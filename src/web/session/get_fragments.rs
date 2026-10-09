use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::web::manager::GetBodyLease;
use bytes::Bytes;

/// Maximum fragment count accepted for one GET uplink operation.
pub(super) use crate::config::GET_MAX_PARTS;
/// Conservative index and allocator overhead charged per retained fragment slot.
const GET_PART_META_BYTES: usize = 64;

mod admission;

/// Incomplete operation whose global byte lease grows as parts are accepted.
struct GetAssembly {
    confirmed: Option<u64>,
    total_parts: u32,
    received: u32,
    bytes_received: usize,
    /// Fixed length of every non-final part, bound by the first one stored.
    chunk: usize,
    parts: Vec<Option<Bytes>>,
    last_part_at: Instant,
    /// Upper bound committed against the session budget at open time.
    commitment: usize,
    /// Global body-byte lease covering exactly the accepted bytes and slots.
    budget: GetBodyLease,
    /// Copy space for the final assembly, reserved when the record opens so
    /// an operation whose parts were all accepted can always complete.
    staging: Option<GetBodyLease>,
}

/// Retained complete body used for exact fragment replays without reapplication.
struct GetCompleted {
    confirmed: Option<u64>,
    body: Bytes,
    /// Per-part `(offset, length)` proving a replay carries the original bytes.
    part_meta: Box<[(u32, u32)]>,
    total_parts: u32,
    last_part_at: Instant,
    /// Session charge released when the record retires.
    commitment: usize,
    budget: Arc<GetBodyLease>,
}

/// Session-bounded GET uplink reassembly keyed by lane and sequence.
#[derive(Default)]
pub(super) struct GetFragments {
    pending: HashMap<(Option<u32>, u64), GetAssembly>,
    completed: HashMap<(Option<u32>, u64), GetCompleted>,
    /// Committed upper bounds across pending and completed records; the real
    /// bytes additionally sit on the manager's global body-byte semaphore.
    committed_bytes: usize,
}

/// Outcome of one admitted GET uplink physical request.
pub(crate) enum GetUpOffer {
    /// The part was stored or exactly replayed without completing the operation.
    Pending(u32),
    /// The logical body assembled and its shared lease transfers to canonical handling.
    Complete {
        body: Bytes,
        budget: Arc<GetBodyLease>,
    },
}

/// Rejection classes mapped onto existing transport responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetUpReject {
    /// Malformed or protocol-inconsistent input keeps decoy semantics.
    Decoy,
    /// Shared capacity is exhausted for this physical request; retry is safe.
    Busy,
    /// A lone final part of an applied sequence whose record retired cannot
    /// be verified; it answers like a stale POST replay.
    Stale,
}

impl GetFragments {
    /// Reports the session commitment charged across retained records.
    #[cfg(test)]
    pub(super) fn committed_bytes(&self) -> usize {
        self.committed_bytes
    }

    /// Detaches expired assemblies and completed records for post-lock release.
    pub(super) fn expire(&mut self, now: Instant, ttl: Duration) -> GetFragments {
        let mut retired = GetFragments::default();
        for (key, assembly) in self
            .pending
            .extract_if(|_, assembly| now.duration_since(assembly.last_part_at) >= ttl)
        {
            self.committed_bytes = self.committed_bytes.saturating_sub(assembly.commitment);
            retired.pending.insert(key, assembly);
        }
        for (key, completed) in self
            .completed
            .extract_if(|_, completed| now.duration_since(completed.last_part_at) >= ttl)
        {
            self.committed_bytes = self.committed_bytes.saturating_sub(completed.commitment);
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

    /// Detaches every record the client already confirmed for one lane: the
    /// completed ones and replay assemblies of applied sequences, which no
    /// client will finish once it confirmed them.
    fn confirm_lane(&mut self, lane: Option<u32>, confirmed: u64, retired: &mut GetFragments) {
        for (key, assembly) in self
            .pending
            .extract_if(|(owner, sequence), _| owner == &lane && *sequence <= confirmed)
        {
            self.committed_bytes = self.committed_bytes.saturating_sub(assembly.commitment);
            retired.pending.insert(key, assembly);
        }
        for (key, completed) in self
            .completed
            .extract_if(|(owner, sequence), _| owner == &lane && *sequence <= confirmed)
        {
            self.committed_bytes = self.committed_bytes.saturating_sub(completed.commitment);
            retired.completed.insert(key, completed);
        }
    }

    /// Detaches completed records superseded by the next legacy sequence.
    fn retire_below(&mut self, lane: Option<u32>, sequence: u64, retired: &mut GetFragments) {
        for (key, completed) in self
            .completed
            .extract_if(|(owner, retained), _| owner == &lane && *retained < sequence)
        {
            self.committed_bytes = self.committed_bytes.saturating_sub(completed.commitment);
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
                self.committed_bytes = self.committed_bytes.saturating_sub(completed.commitment);
                retired.completed.insert(victim, completed);
            }
        }
    }
}

#[cfg(test)]
#[path = "get_fragments_tests.rs"]
mod get_fragments_tests;
