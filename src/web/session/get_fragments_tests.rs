use super::*;

use std::net::SocketAddr;

use arc_swap::ArcSwap;

use crate::config::{
    ProxyConfig, WebCarrier, WebLimitsConfig, WebRuntimeProfile, WebSecretMode, WebTimeoutsConfig,
};
use crate::maestro::generation::test_runtime_generation;
use crate::web::frame::{self, FrameType};
use crate::web::manager::WebProcessRuntime;
use crate::web::session::WebSession;

/// Minimal live session with default limits and timeouts for fragment tests.
fn session() -> (Arc<WebSession>, Arc<WebProcessRuntime>) {
    let generation = test_runtime_generation(1, ProxyConfig::default());
    let manager = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation)));
    let profile = Arc::new(WebRuntimeProfile {
        host: "proxy.example.com".to_string(),
        public_addr: SocketAddr::from(([203, 0, 113, 10], 443)),
        user: "alice".to_string(),
        secret_mode: WebSecretMode::Plain,
        carrier: WebCarrier::Https,
        carrier_negotiation_enabled: false,
        carrier_learning: false,
        carriers: Arc::from([WebCarrier::Https]),
        carrier_negotiation_deadlines_secs: [3, 5, 8, 12],
        capability: [0; 32],
        credential_id: [0; 16],
        key_fingerprint: "0000000000000000".to_string(),
        max_sessions: 1,
        max_streams: 1,
        max_streams_per_session: 1,
    });
    let timeouts = WebTimeoutsConfig {
        long_poll_secs: 1,
        ..WebTimeoutsConfig::default()
    };
    let session = WebSession::new(
        Arc::downgrade(&manager),
        [1; 32],
        "192.0.2.10".parse().unwrap(),
        1,
        profile,
        [2; 32],
        WebCarrier::Https,
        1,
        [3; 32],
        None,
        crate::web::manager::CarrierClientClass::Legacy,
        None,
        false,
        false,
        WebLimitsConfig::default(),
        timeouts,
        None,
    );
    (session, manager)
}

/// Keeps the peer fresh at the synthetic sweep instant so only fragment TTL
/// decides whether the session stays open.
fn touch_peer_at(session: &WebSession, now: Instant) {
    session.state.lock().activity.touch_peer(now);
}

#[tokio::test]
async fn pending_assembly_survives_request_deadline_but_not_retry_budget() {
    let (session, manager) = session();
    // Defaults: bridge_request_secs = 10, bridge_retry_secs = 90.
    assert_eq!(session.timeouts().bridge_request_secs, 10);
    assert_eq!(session.timeouts().bridge_retry_secs, 90);

    let part = Bytes::from_static(&[9u8; 32]);
    let offer = session.offer_get_up(None, 1, None, 0, 3, part.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(0))));

    let opened = Instant::now();
    // A retried fragment arriving past the per-request deadline must still
    // land: the retry budget, not the request timeout, bounds the assembly.
    let within_retry = opened + Duration::from_secs(30);
    touch_peer_at(&session, within_retry);
    assert!(!session.close_if_due(within_retry));
    let offer = session.offer_get_up(None, 1, None, 1, 3, part.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(1))));

    // Past the whole-operation retry budget the sweep reclaims the record.
    let expired = Instant::now() + Duration::from_secs(95);
    touch_peer_at(&session, expired);
    assert!(!session.close_if_due(expired));
    assert!(matches!(
        session.offer_get_up(None, 1, None, 2, 3, part.clone()),
        Err(GetUpReject::Decoy)
    ));

    // The released lease and freed slot let a fresh operation run to the end.
    let offer = session.offer_get_up(None, 1, None, 0, 2, part.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(0))));
    let offer = session.offer_get_up(None, 1, None, 1, 2, Bytes::from_static(&[1u8; 8]));
    assert!(matches!(offer, Ok(GetUpOffer::Complete { .. })));

    manager.shutdown().await;
}

/// Reads the session commitment counter under the state lock.
fn committed_bytes(session: &WebSession) -> usize {
    session.state.lock().get_fragments.committed_bytes()
}

/// Global body-byte leases grow only by the bytes actually accepted.
#[tokio::test]
async fn accounting_is_incremental_and_head_ops_keep_window_budget() {
    let (session, manager) = session();
    session.configure_conveyor(4);
    let free = manager.body_bytes_available();
    let max_body = session.limits().max_body_bytes;
    let part = Bytes::from_static(&[9u8; 500]);
    let total = GET_MAX_PARTS;
    let commitment =
        max_body.min(total as usize * part.len()) + total as usize * GET_PART_META_BYTES;
    let open_lease = total as usize * GET_PART_META_BYTES + part.len();

    // Three non-head openings saturate the (W-1)*C commitment share.
    for sequence in 2..=4u64 {
        let offer = session.offer_get_up(None, sequence, Some(0), 0, total, part.clone());
        assert!(
            matches!(offer, Ok(GetUpOffer::Pending(0))),
            "seq={sequence}"
        );
    }
    assert_eq!(committed_bytes(&session), 3 * commitment);
    assert_eq!(free - manager.body_bytes_available(), 3 * open_lease);

    // Each further accepted fragment charges only its own bytes globally
    // while the operation's committed ceiling stays unchanged.
    let offer = session.offer_get_up(None, 2, Some(0), 1, total, part.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(1))));
    assert_eq!(free - manager.body_bytes_available(), 3 * open_lease + 500);
    assert_eq!(committed_bytes(&session), 3 * commitment);

    // The lane head still admits a fresh operation inside its full W*C
    // budget even with the non-head share completely exhausted.
    let head = Bytes::from_static(&[9u8; 32]);
    let offer = session.offer_get_up(None, 1, Some(0), 0, 2, head.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(0))));
    let head_commit = 2 * GET_PART_META_BYTES + 64;
    assert_eq!(committed_bytes(&session), 3 * commitment + head_commit);

    manager.shutdown().await;
}

/// Cap exhaustion answers Busy without touching state; expiry frees it.
#[tokio::test]
async fn commitment_cap_is_busy_until_expiry_frees_capacity() {
    let (session, manager) = session();
    session.configure_conveyor(4);
    let free = manager.body_bytes_available();
    let part = Bytes::from_static(&[9u8; 500]);

    for sequence in 1..=3u64 {
        let offer = session.offer_get_up(None, sequence, Some(0), 0, GET_MAX_PARTS, part.clone());
        assert!(
            matches!(offer, Ok(GetUpOffer::Pending(0))),
            "seq={sequence}"
        );
    }
    // A fourth non-head opening exceeds (W-1)*C and parks nothing.
    let blocked = session.offer_get_up(None, 4, Some(0), 0, GET_MAX_PARTS, part.clone());
    assert!(matches!(blocked, Err(GetUpReject::Busy)));
    let replay = session.offer_get_up(None, 2, Some(0), 0, GET_MAX_PARTS, part.clone());
    assert!(matches!(replay, Ok(GetUpOffer::Pending(0))));

    // Past the whole-operation retry budget the sweep frees every lease.
    let expired = Instant::now() + Duration::from_secs(95);
    touch_peer_at(&session, expired);
    assert!(!session.close_if_due(expired));
    assert_eq!(committed_bytes(&session), 0);
    assert_eq!(manager.body_bytes_available(), free);

    // The retried operation now opens cleanly.
    let offer = session.offer_get_up(None, 4, Some(0), 0, GET_MAX_PARTS, part.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(0))));

    manager.shutdown().await;
}

/// Replays of an expired completed record resolve from the applied sequence.
#[tokio::test]
async fn completed_record_expiry_makes_replays_idempotent() {
    let (session, manager) = session();
    session.configure_conveyor(4);
    let free = manager.body_bytes_available();
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let head = pong.iter().copied().cycle().take(32).collect::<Bytes>();

    let offer = session.offer_get_up(None, 1, Some(0), 0, 2, head.clone());
    assert!(matches!(offer, Ok(GetUpOffer::Pending(0))));
    let Ok(GetUpOffer::Complete { body, .. }) =
        session.offer_get_up(None, 1, Some(0), 1, 2, pong.clone())
    else {
        panic!("final part completes the two-part operation");
    };
    // The canonical claim applies the sequence so the record becomes committed.
    let claim = session
        .claim_conveyor(None, 1, Some(0))
        .unwrap()
        .expect("conveyor mode admits the claim");
    assert_eq!(claim.process(&body).await, Ok(1));

    // While the record is retained an exact final replay resolves through it;
    // the returned lease is released explicitly so expiry sees only the map.
    let replay = session.offer_get_up(None, 1, Some(0), 1, 2, pong.clone());
    assert!(matches!(&replay, Ok(GetUpOffer::Complete { .. })));
    drop(replay);

    // Past the retry budget the record is swept and every lease releases.
    let expired = Instant::now() + Duration::from_secs(95);
    touch_peer_at(&session, expired);
    assert!(!session.close_if_due(expired));
    assert_eq!(committed_bytes(&session), 0);
    assert_eq!(manager.body_bytes_available(), free);

    // Replays of the expired record acknowledge idempotently: the applied
    // sequence answers without re-storing or re-verifying fragment bytes.
    let final_replay = session.offer_get_up(None, 1, Some(0), 1, 2, pong.clone());
    assert!(matches!(final_replay, Ok(GetUpOffer::Duplicate)));
    let part_replay = session.offer_get_up(None, 1, Some(0), 0, 2, head.clone());
    assert!(matches!(part_replay, Ok(GetUpOffer::Duplicate)));

    manager.shutdown().await;
}
