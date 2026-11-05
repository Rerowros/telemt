//! Wire-shaping helpers that keep carrier traffic from having a fixed cadence and fixed sizes.
//!
//! Values are derived from the session identity and a per-session counter, so every session
//! shapes its own traffic while nothing predictable leaks into the protocol.

use crate::web::frame::DATA_CHUNK_BYTES;

/// Share of the configured long-poll interval that every poll may release early.
const LONG_POLL_SPREAD_PERCENT: u64 = 30;
/// Smallest data chunk emitted, as a percentage of the fixed chunk size.
const DATA_CHUNK_MIN_PERCENT: usize = 60;

/// Mixes one session seed and counter into a stable bounded value.
pub(crate) fn mix(seed: u64, counter: u64, salt: u64) -> u64 {
    let mut mixed = salt
        ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_shl(1);
    mixed ^= mixed >> 30;
    mixed = mixed.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed ^= mixed >> 27;
    mixed = mixed.wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// Returns the hold time for one long poll: between 70% and 100% of the configured interval.
pub(crate) fn long_poll_secs(base: u64, value: u64) -> u64 {
    if base <= 1 {
        return base;
    }
    let spread = base * LONG_POLL_SPREAD_PERCENT / 100;
    base - spread + value % (spread + 1)
}

/// Returns the data chunk ceiling for one write: between 60% and 100% of the fixed chunk size.
pub(crate) fn data_chunk_bytes(value: u64) -> usize {
    let percent = DATA_CHUNK_MIN_PERCENT + value as usize % (101 - DATA_CHUNK_MIN_PERCENT);
    (DATA_CHUNK_BYTES / 100 * percent).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_poll_stays_inside_the_configured_window() {
        let base = 25;
        let spread = base * LONG_POLL_SPREAD_PERCENT / 100;
        for value in 0..512 {
            let hold = long_poll_secs(base, value);
            assert!((base - spread..=base).contains(&hold), "{hold} outside window");
        }
        assert_eq!(long_poll_secs(1, 7), 1);
        assert_eq!(long_poll_secs(0, 7), 0);
    }

    #[test]
    fn long_poll_values_are_not_constant() {
        let values: std::collections::BTreeSet<_> = (0..256).map(|v| long_poll_secs(25, v)).collect();
        assert!(values.len() > 1);
    }

    #[test]
    fn data_chunks_vary_within_bounds() {
        let min = DATA_CHUNK_BYTES / 100 * DATA_CHUNK_MIN_PERCENT;
        let mut seen = std::collections::BTreeSet::new();
        for value in 0..512 {
            let chunk = data_chunk_bytes(value);
            assert!((min..=DATA_CHUNK_BYTES).contains(&chunk), "{chunk} outside bounds");
            seen.insert(chunk);
        }
        assert!(seen.len() > 1);
    }

    #[test]
    fn mix_changes_with_counter_and_seed() {
        assert_ne!(mix(1, 0, 0x2002), mix(1, 1, 0x2002));
        assert_ne!(mix(1, 0, 0x2002), mix(2, 0, 0x2002));
        assert_eq!(mix(1, 0, 0x2002), mix(1, 0, 0x2002));
    }
}
