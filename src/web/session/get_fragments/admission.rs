use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;

use super::*;
use crate::web::session::{SessionState, WebSession};
use crate::web::telemetry::WebSessionLifecycleObservation;

/// Applied sequence bound for one lane or the shared channel: the conveyor
/// channel's committed floor in negotiated mode, the last uplink otherwise.
fn lane_committed(state: &SessionState, lane: Option<u32>, conveyor: bool) -> u64 {
    if conveyor {
        return state.conveyor.sequence_floor(lane).1;
    }
    match lane {
        Some(lane) => state
            .carrier_lanes
            .get(&lane)
            .map_or(0, |lane| lane.last_up_sequence),
        None => state.last_up_sequence,
    }
}

/// Detaches the oldest completed record whose sequence already applied, so a
/// later replay still resolves through the idempotent duplicate path. Returns
/// false once nothing safely evictable remains.
fn evict_committed_completed(state: &mut SessionState, retired: &mut GetFragments) -> bool {
    let victim = state
        .get_fragments
        .completed
        .iter()
        .filter(|((owner, retained), record)| {
            *retained <= lane_committed(state, *owner, record.confirmed.is_some())
        })
        .min_by_key(|((_, retained), _)| *retained)
        .map(|(key, _)| *key);
    let Some(victim) = victim else {
        return false;
    };
    if let Some(record) = state.get_fragments.completed.remove(&victim) {
        state.get_fragments.committed_bytes = state
            .get_fragments
            .committed_bytes
            .saturating_sub(record.commitment);
        retired.completed.insert(victim, record);
        true
    } else {
        false
    }
}

impl WebSession {
    /// Admits one GET uplink fragment without claiming conveyor sequence turns.
    /// All fallible checks precede state mutation, and every retired record or
    /// unused lease detaches so its permit drops only after the session lock.
    /// Capacity exhaustion answers Busy while protocol violations stay Decoy.
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
        // Declared before the guard so unused leases and retired records
        // release their permits strictly after the session lock drops.
        let mut retired = GetFragments::default();
        let mut spare: Vec<OwnedSemaphorePermit> = Vec::new();
        // Global byte leases acquired without the session lock: `lease` charges
        // the bytes this request adds to a record, `staging` covers the final
        // part's contiguous assembly copy while the parts vector is consumed.
        let mut lease: Option<(usize, OwnedSemaphorePermit)> = None;
        let mut staging: Option<(usize, OwnedSemaphorePermit)> = None;
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
            if let Some(assembly) = state.get_fragments.pending.get(&key) {
                if assembly.confirmed != confirmed || assembly.total_parts != total {
                    return Err(GetUpReject::Decoy);
                }
                // An occupied slot accepts only the exact byte-for-byte replay.
                if let Some(existing) = &assembly.parts[part as usize] {
                    return if *existing == data {
                        Ok(GetUpOffer::Pending(part))
                    } else {
                        Err(GetUpReject::Decoy)
                    };
                }
                if part == total - 1 {
                    // The final part closes the operation, so it is accepted
                    // only once every earlier part is stored; the real bridge
                    // sends it last and parallel clients never race it in.
                    if assembly.received != total - 1 || data.len() > assembly.chunk {
                        return Err(GetUpReject::Decoy);
                    }
                } else if data.len() != assembly.chunk {
                    // Non-final parts share the chunk length fixed by the
                    // first stored part; deviation never comes from the bridge.
                    return Err(GetUpReject::Decoy);
                }
                // One assembly still cannot exceed the canonical body size.
                if assembly.bytes_received.saturating_add(data.len()) > max_body {
                    return Err(GetUpReject::Decoy);
                }
                // With every earlier part stored the final part can no longer
                // grow the set, so the copy reservation size is stable here.
                let stage = if part == total - 1 {
                    assembly.bytes_received
                } else {
                    0
                };
                // The accepted bytes join the global body budget; a failed
                // acquire leaves the assembly intact so the retry lands later.
                if lease.as_ref().is_none_or(|(size, _)| *size != data.len()) {
                    if let Some((_, permit)) = lease.take() {
                        spare.push(permit);
                    }
                    drop(state);
                    let Some(permit) = manager.try_body_bytes(data.len()) else {
                        return Err(GetUpReject::Busy);
                    };
                    lease = Some((data.len(), permit));
                    state = self.state.lock();
                    continue;
                }
                if stage > 0 && staging.as_ref().is_none_or(|(size, _)| *size != stage) {
                    if let Some((_, permit)) = staging.take() {
                        spare.push(permit);
                    }
                    drop(state);
                    let Some(permit) = manager.try_body_bytes(stage) else {
                        return Err(GetUpReject::Busy);
                    };
                    staging = Some((stage, permit));
                    state = self.state.lock();
                    continue;
                }
                let Some((_, permit)) = lease.take() else {
                    return Err(GetUpReject::Busy);
                };
                let Some(assembly) = state.get_fragments.pending.get_mut(&key) else {
                    return Err(GetUpReject::Decoy);
                };
                let data_len = data.len();
                assembly.bytes_received = assembly.bytes_received.saturating_add(data_len);
                assembly.parts[part as usize] = Some(data);
                assembly.received += 1;
                assembly.last_part_at = now;
                assembly.budget.merge(permit);
                let just_completed = assembly.received == assembly.total_parts;
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
            let last_up = match lane_id {
                Some(lane) => state
                    .carrier_lanes
                    .get(&lane)
                    .map_or(0, |lane| lane.last_up_sequence),
                None => state.last_up_sequence,
            };
            if let Some(hint) = confirmed {
                // Confirmed-conveyor admission mirrors claim_conveyor's floor.
                let floor = channel_confirmed.max(hint);
                let window = state.conveyor.offer_window() as u64;
                if sequence <= floor || sequence > floor.saturating_add(window) {
                    return Err(GetUpReject::Decoy);
                }
                if sequence <= committed {
                    // The record is gone but the sequence already applied:
                    // acknowledge idempotently instead of reapplying frames.
                    self.touch_peer_locked(
                        &mut state,
                        now,
                        WebSessionLifecycleObservation::HttpActivityAfterGap,
                    );
                    return Ok(GetUpOffer::Duplicate);
                }
            } else {
                // Legacy ordering admits exactly the next sequence; the last
                // applied sequence may replay idempotently after retirement.
                if sequence == last_up && last_up != 0 {
                    self.touch_peer_locked(
                        &mut state,
                        now,
                        WebSessionLifecycleObservation::HttpActivityAfterGap,
                    );
                    return Ok(GetUpOffer::Duplicate);
                }
                if sequence != last_up.saturating_add(1) {
                    return Err(GetUpReject::Decoy);
                }
            }
            // Any non-final part may open a reassembly record; the final part
            // cannot because it is only admitted once all others are stored.
            // Already-applied sequences answered above stay idempotent.
            if total > 1 && part == total - 1 {
                return Err(GetUpReject::Decoy);
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
            // Session commitment: every open operation reserves its worst-case
            // body plus slot metadata against a window-scaled bound. The lane
            // head always has room for one more operation while non-head
            // operations share the rest, so the head can never deadlock.
            let commitment = max_body
                .min((total as usize).saturating_mul(data.len()))
                .saturating_add((total as usize).saturating_mul(GET_PART_META_BYTES));
            let ceiling = max_body.saturating_add(GET_MAX_PARTS as usize * GET_PART_META_BYTES);
            let window = state.conveyor.offer_window();
            let lane_bound = lane_committed(&state, lane_id, confirmed.is_some());
            let head = sequence == lane_bound.saturating_add(1);
            let bound = ceiling.saturating_mul(if head {
                window
            } else {
                window.saturating_sub(1)
            });
            while state
                .get_fragments
                .committed_bytes
                .saturating_add(commitment)
                > bound
            {
                if !evict_committed_completed(&mut state, &mut retired) {
                    return Err(GetUpReject::Busy);
                }
            }
            // The open charge covers the declared slot metadata plus this
            // part's bytes; later parts merge their own accepted bytes.
            let open = (total as usize)
                .saturating_mul(GET_PART_META_BYTES)
                .saturating_add(data.len());
            if lease.as_ref().is_none_or(|(size, _)| *size != open) {
                if let Some((_, permit)) = lease.take() {
                    spare.push(permit);
                }
                drop(state);
                let Some(permit) = manager.try_body_bytes(open) else {
                    return Err(GetUpReject::Busy);
                };
                lease = Some((open, permit));
                state = self.state.lock();
                continue;
            }
            let Some((_, permit)) = lease.take() else {
                return Err(GetUpReject::Busy);
            };
            let data_len = data.len();
            if total == 1 {
                // A single-part body moves into replay retention without a copy.
                let part_meta = Box::new([(0u32, data.len() as u32)]);
                let budget = Arc::new(permit);
                let offer = GetUpOffer::Complete {
                    body: data.clone(),
                    budget: Arc::clone(&budget),
                };
                state.get_fragments.committed_bytes = state
                    .get_fragments
                    .committed_bytes
                    .saturating_add(commitment);
                state.get_fragments.completed.insert(
                    key,
                    GetCompleted {
                        confirmed,
                        body: data,
                        part_meta,
                        total_parts: total,
                        last_part_at: now,
                        commitment,
                        budget,
                    },
                );
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
            let mut parts = Vec::new();
            parts.resize_with(total as usize, || None);
            parts[part as usize] = Some(data);
            state.get_fragments.committed_bytes = state
                .get_fragments
                .committed_bytes
                .saturating_add(commitment);
            state.get_fragments.pending.insert(
                key,
                GetAssembly {
                    confirmed,
                    total_parts: total,
                    received: 1,
                    bytes_received: data_len,
                    chunk: data_len,
                    parts,
                    last_part_at: now,
                    commitment,
                    budget: permit,
                },
            );
            self.touch_peer_locked(
                &mut state,
                now,
                WebSessionLifecycleObservation::HttpActivityAfterGap,
            );
            return Ok(GetUpOffer::Pending(part));
        }
        // The final part assembles the retained body; the merged part leases
        // move into the completed record while the staging lease releases the
        // transient copy space after the session lock drops.
        let Some(assembly) = state.get_fragments.pending.remove(&key) else {
            return Err(GetUpReject::Decoy);
        };
        state.get_fragments.committed_bytes = state
            .get_fragments
            .committed_bytes
            .saturating_sub(assembly.commitment);
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
        let commitment = body
            .len()
            .saturating_add((total_parts as usize).saturating_mul(GET_PART_META_BYTES));
        state.get_fragments.committed_bytes = state
            .get_fragments
            .committed_bytes
            .saturating_add(commitment);
        let completed = GetCompleted {
            confirmed,
            body: Bytes::from(body),
            part_meta: part_meta.into_boxed_slice(),
            total_parts,
            last_part_at: now,
            commitment,
            budget: Arc::new(budget),
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
