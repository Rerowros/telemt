use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;

use super::WebSession;
use crate::web::telemetry::WebSessionLifecycleObservation;

/// Maximum fragment count accepted for one GET uplink operation.
pub(super) use crate::config::GET_MAX_PARTS;
/// Conservative index and allocator overhead charged per retained fragment slot.
const GET_PART_META_BYTES: usize = 64;

/// Incomplete operation whose parts share one upfront body-byte reservation.
struct GetAssembly {
    confirmed: Option<u64>,
    total_parts: u32,
    received: u32,
    bytes_received: usize,
    parts: Vec<Option<Bytes>>,
    last_part_at: Instant,
    budget: Arc<OwnedSemaphorePermit>,
}

/// Retained complete body used for exact fragment replays without reapplication.
struct GetCompleted {
    confirmed: Option<u64>,
    body: Bytes,
    /// Per-part `(offset, length)` proving a replay carries the original bytes.
    part_meta: Box<[(u32, u32)]>,
    total_parts: u32,
    last_part_at: Instant,
    budget: Arc<OwnedSemaphorePermit>,
}

/// Session-bounded GET uplink reassembly keyed by lane and sequence.
#[derive(Default)]
pub(super) struct GetFragments {
    pending: HashMap<(Option<u32>, u64), GetAssembly>,
    completed: HashMap<(Option<u32>, u64), GetCompleted>,
    pending_bytes: usize,
}

/// Outcome of one admitted GET uplink physical request.
pub(crate) enum GetUpOffer {
    /// The part was stored or exactly replayed without completing the operation.
    Pending(u32),
    /// The logical body assembled and its shared lease transfers to canonical handling.
    Complete {
        body: Bytes,
        budget: Arc<OwnedSemaphorePermit>,
    },
}

/// Rejection classes mapped onto existing transport responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetUpReject {
    /// Malformed or protocol-inconsistent input keeps decoy semantics.
    Decoy,
    /// Shared body-budget capacity is exhausted for this physical request.
    Busy,
}

impl GetFragments {
    /// Detaches expired assemblies and completed records for post-lock release.
    pub(super) fn expire(&mut self, now: Instant, ttl: Duration) -> GetFragments {
        let mut retired = GetFragments::default();
        for (key, assembly) in self
            .pending
            .extract_if(|_, assembly| now.duration_since(assembly.last_part_at) >= ttl)
        {
            self.pending_bytes = self.pending_bytes.saturating_sub(assembly.bytes_received);
            retired.pending.insert(key, assembly);
        }
        for (key, completed) in self
            .completed
            .extract_if(|_, completed| now.duration_since(completed.last_part_at) >= ttl)
        {
            retired.completed.insert(key, completed);
        }
        retired
    }

    /// Counts retained records owned by one lane or by the shared channel.
    fn lane_records(&self, lane: Option<u32>) -> usize {
        self.pending
            .keys()
            .chain(self.completed.keys())
            .filter(|(owner, _)| owner == &lane)
            .count()
    }

    /// Detaches completed records the client already confirmed for one lane.
    fn confirm_lane(&mut self, lane: Option<u32>, confirmed: u64, retired: &mut GetFragments) {
        for (key, completed) in self
            .completed
            .extract_if(|(owner, sequence), _| owner == &lane && *sequence <= confirmed)
        {
            retired.completed.insert(key, completed);
        }
    }

    /// Detaches completed records superseded by the next legacy sequence.
    fn retire_below(&mut self, lane: Option<u32>, sequence: u64, retired: &mut GetFragments) {
        for (key, completed) in self
            .completed
            .extract_if(|(owner, retained), _| owner == &lane && *retained < sequence)
        {
            retired.completed.insert(key, completed);
        }
    }

    /// Detaches oldest completed records until one lane fits its conveyor window.
    fn evict_lane_overflow(
        &mut self,
        lane: Option<u32>,
        window: usize,
        retired: &mut GetFragments,
    ) {
        while self.lane_records(lane) > window {
            let Some(victim) = self
                .completed
                .keys()
                .filter(|(owner, _)| owner == &lane)
                .min_by_key(|(_, sequence)| *sequence)
                .copied()
            else {
                break;
            };
            if let Some(completed) = self.completed.remove(&victim) {
                retired.completed.insert(victim, completed);
            }
        }
    }
}

impl WebSession {
    /// Admits one GET uplink fragment without claiming conveyor sequence turns.
    /// All fallible checks precede state mutation, and every retired record or
    /// unused lease detaches so its permit drops only after the session lock.
    pub(crate) fn offer_get_up(
        self: &Arc<Self>,
        lane_id: Option<u32>,
        sequence: u64,
        confirmed: Option<u64>,
        part: u32,
        total: u32,
        data: Bytes,
    ) -> Result<GetUpOffer, GetUpReject> {
        if sequence == 0
            || total == 0
            || total > GET_MAX_PARTS
            || part >= total
            || data.is_empty()
            || data.len() > self.limits.get_url_bytes
            || data.len() > self.limits.max_body_bytes
        {
            return Err(GetUpReject::Decoy);
        }
        let Some(manager) = self.manager.upgrade() else {
            return Err(GetUpReject::Busy);
        };
        // The reader permit covers only this physical request; decoded bytes
        // are charged once to the assembly reservation instead of every part.
        let Some(reader) = manager.try_body_reader() else {
            return Err(GetUpReject::Busy);
        };
        let _reader = reader;
        let key = (lane_id, sequence);
        let now = Instant::now();
        let max_body = self.limits.max_body_bytes;
        let get_url_bytes = self.limits.get_url_bytes;
        // Declared before the guard so unused leases and retired records
        // release their permits strictly after the session lock drops.
        let mut retired = GetFragments::default();
        let mut lease: Option<Arc<OwnedSemaphorePermit>> = None;
        let mut state = self.state.lock();
        loop {
            if state.closed || self.ensure_carrier_active_locked(&state).is_err() {
                return Err(GetUpReject::Decoy);
            }
            if let Some(completed) = state.get_fragments.completed.get(&key) {
                if completed.total_parts != total {
                    return Err(GetUpReject::Decoy);
                }
                match (completed.confirmed, confirmed) {
                    (None, None) => {}
                    // A later retry may confirm more than the original request.
                    (Some(stored), Some(hint)) if hint >= stored => {}
                    _ => return Err(GetUpReject::Decoy),
                }
                let (_, committed) = state.conveyor.sequence_floor(lane_id);
                if confirmed.is_some_and(|hint| hint > committed) {
                    return Err(GetUpReject::Decoy);
                }
                let Some(&(offset, len)) = completed.part_meta.get(part as usize) else {
                    return Err(GetUpReject::Decoy);
                };
                if data.len() != len as usize
                    || completed
                        .body
                        .get(offset as usize..offset as usize + len as usize)
                        != Some(&data[..])
                {
                    return Err(GetUpReject::Decoy);
                }
                return if part == total - 1 {
                    Ok(GetUpOffer::Complete {
                        body: completed.body.clone(),
                        budget: Arc::clone(&completed.budget),
                    })
                } else {
                    Ok(GetUpOffer::Pending(part))
                };
            }
            let pending_bytes = state.get_fragments.pending_bytes;
            if let Some(assembly) = state.get_fragments.pending.get_mut(&key) {
                if assembly.confirmed != confirmed || assembly.total_parts != total {
                    return Err(GetUpReject::Decoy);
                }
                if part < assembly.received {
                    return match &assembly.parts[part as usize] {
                        Some(existing) if *existing == data => Ok(GetUpOffer::Pending(part)),
                        _ => Err(GetUpReject::Decoy),
                    };
                }
                if part > assembly.received {
                    return Err(GetUpReject::Decoy);
                }
                // The session-wide raw-byte bound applies to every new part.
                if pending_bytes.saturating_add(data.len()) > max_body {
                    return Err(GetUpReject::Decoy);
                }
                let data_len = data.len();
                assembly.bytes_received = assembly.bytes_received.saturating_add(data_len);
                assembly.parts[part as usize] = Some(data);
                assembly.received += 1;
                assembly.last_part_at = now;
                let just_completed = assembly.received == assembly.total_parts;
                state.get_fragments.pending_bytes = pending_bytes.saturating_add(data_len);
                if !just_completed {
                    self.touch_peer_locked(
                        &mut state,
                        now,
                        WebSessionLifecycleObservation::HttpActivityAfterGap,
                    );
                    return Ok(GetUpOffer::Pending(part));
                }
                break;
            }
            // Only the first part of an operation may open a reassembly record.
            if part != 0 {
                return Err(GetUpReject::Decoy);
            }
            // Mode consistency: hints belong to negotiated conveyor sessions
            // and a partial part never freezes the ordering mode itself.
            if confirmed.is_some() && state.conveyor.offer_window() <= 1 {
                return Err(GetUpReject::Decoy);
            }
            if state
                .conveyor
                .frozen_mode()
                .is_some_and(|mode| mode != confirmed.is_some())
            {
                return Err(GetUpReject::Decoy);
            }
            let (channel_confirmed, committed) = state.conveyor.sequence_floor(lane_id);
            if confirmed.is_some_and(|hint| hint > committed) {
                return Err(GetUpReject::Decoy);
            }
            if let Some(hint) = confirmed {
                // Confirmed-conveyor admission mirrors claim_conveyor's floor.
                let floor = channel_confirmed.max(hint);
                let window = state.conveyor.offer_window() as u64;
                if sequence <= floor || sequence > floor.saturating_add(window) {
                    return Err(GetUpReject::Decoy);
                }
            } else {
                // Legacy ordering admits exactly the next sequence; anything
                // older without a retained replay record is unverifiable.
                let last_up = match lane_id {
                    Some(lane) => state
                        .carrier_lanes
                        .get(&lane)
                        .map_or(0, |lane| lane.last_up_sequence),
                    None => state.last_up_sequence,
                };
                if sequence != last_up.saturating_add(1) {
                    return Err(GetUpReject::Decoy);
                }
            }
            // A valid new sequence retires the completed records it
            // supersedes before the per-lane record bound applies.
            if let Some(hint) = confirmed {
                state
                    .get_fragments
                    .confirm_lane(lane_id, hint, &mut retired);
            } else {
                state
                    .get_fragments
                    .retire_below(lane_id, sequence, &mut retired);
            }
            if state.get_fragments.pending_bytes.saturating_add(data.len()) > max_body {
                return Err(GetUpReject::Decoy);
            }
            if state.get_fragments.lane_records(lane_id) >= state.conveyor.offer_window() {
                return Err(GetUpReject::Decoy);
            }
            if let Some(lane) = lane_id
                && !state.carrier_lanes.contains_key(&lane)
            {
                let bound = self
                    .profile
                    .max_streams_per_session
                    .saturating_add(self.limits.max_tombstones_per_session)
                    .saturating_add(1);
                let provisional = state
                    .get_fragments
                    .pending
                    .keys()
                    .chain(state.get_fragments.completed.keys())
                    .filter(|(owner, _)| {
                        owner.is_some_and(|owner| !state.carrier_lanes.contains_key(&owner))
                    })
                    .count();
                if state.carrier_lanes.len().saturating_add(provisional) >= bound
                    || state.closed_streams.contains(&lane)
                {
                    return Err(GetUpReject::Decoy);
                }
            }
            if lease.is_none() {
                // Single-part bodies stay whole; staged assemblies reserve the
                // retained parts plus the contiguous completion copy upfront.
                let reserve = if total == 1 {
                    data.len().saturating_add(GET_PART_META_BYTES)
                } else {
                    max_body
                        .min((total as usize).saturating_mul(get_url_bytes))
                        .saturating_mul(2)
                        .saturating_add((total as usize).saturating_mul(GET_PART_META_BYTES))
                };
                // The lease is acquired without the session lock; insertion
                // revalidates every check on the next loop iteration.
                drop(state);
                let Some(permit) = manager.try_body_bytes(reserve) else {
                    return Err(GetUpReject::Busy);
                };
                lease = Some(Arc::new(permit));
                state = self.state.lock();
                continue;
            }
            let Some(budget) = lease.take() else {
                return Err(GetUpReject::Busy);
            };
            if total == 1 {
                // A single-part body moves into replay retention without a copy.
                let part_meta = Box::new([(0u32, data.len() as u32)]);
                let offer = GetUpOffer::Complete {
                    body: data.clone(),
                    budget: Arc::clone(&budget),
                };
                state.get_fragments.completed.insert(
                    key,
                    GetCompleted {
                        confirmed,
                        body: data,
                        part_meta,
                        total_parts: total,
                        last_part_at: now,
                        budget,
                    },
                );
                let window = state.conveyor.offer_window();
                state
                    .get_fragments
                    .evict_lane_overflow(lane_id, window, &mut retired);
                self.touch_peer_locked(
                    &mut state,
                    now,
                    WebSessionLifecycleObservation::HttpActivityAfterGap,
                );
                return Ok(offer);
            }
            let data_len = data.len();
            let mut parts = Vec::new();
            parts.resize_with(total as usize, || None);
            parts[0] = Some(data);
            state.get_fragments.pending_bytes =
                state.get_fragments.pending_bytes.saturating_add(data_len);
            state.get_fragments.pending.insert(
                key,
                GetAssembly {
                    confirmed,
                    total_parts: total,
                    received: 1,
                    bytes_received: data_len,
                    parts,
                    last_part_at: now,
                    budget,
                },
            );
            self.touch_peer_locked(
                &mut state,
                now,
                WebSessionLifecycleObservation::HttpActivityAfterGap,
            );
            return Ok(GetUpOffer::Pending(part));
        }
        // The final part assembles the retained body under the upfront lease.
        let Some(assembly) = state.get_fragments.pending.remove(&key) else {
            return Err(GetUpReject::Decoy);
        };
        state.get_fragments.pending_bytes = state
            .get_fragments
            .pending_bytes
            .saturating_sub(assembly.bytes_received);
        let GetAssembly {
            confirmed,
            total_parts,
            bytes_received,
            parts,
            budget,
            ..
        } = assembly;
        let mut part_meta = Vec::with_capacity(parts.len());
        let mut body = Vec::with_capacity(bytes_received);
        for chunk in parts.into_iter().flatten() {
            part_meta.push((body.len() as u32, chunk.len() as u32));
            body.extend_from_slice(&chunk);
        }
        let completed = GetCompleted {
            confirmed,
            body: Bytes::from(body),
            part_meta: part_meta.into_boxed_slice(),
            total_parts,
            last_part_at: now,
            budget,
        };
        let offer = GetUpOffer::Complete {
            body: completed.body.clone(),
            budget: Arc::clone(&completed.budget),
        };
        state.get_fragments.completed.insert(key, completed);
        let window = state.conveyor.offer_window();
        state
            .get_fragments
            .evict_lane_overflow(lane_id, window, &mut retired);
        self.touch_peer_locked(
            &mut state,
            now,
            WebSessionLifecycleObservation::HttpActivityAfterGap,
        );
        Ok(offer)
    }
}
