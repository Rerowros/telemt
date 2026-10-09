use super::*;

use std::net::SocketAddr;

use arc_swap::ArcSwap;

use crate::config::{
    ProxyConfig, WebCarrier, WebLimitsConfig, WebRuntimeProfile, WebSecretMode, WebTimeoutsConfig,
};
use crate::maestro::generation::test_runtime_generation;
use crate::web::frame::{self, FrameType};
use crate::web::manager::{ManagerError, WebProcessRuntime};
use crate::web::session::{ConveyorError, SessionCloseReason, WebSession};

/// Minimal live session with default limits and timeouts for fragment tests.
fn session() -> (Arc<WebSession>, Arc<WebProcessRuntime>) {
    session_with(false)
}

/// Same session, optionally created by automatic carrier negotiation.
fn session_with(automatic: bool) -> (Arc<WebSession>, Arc<WebProcessRuntime>) {
    let generation = test_runtime_generation(1, ProxyConfig::default());
    let manager = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation)));
    let session = session_in(&manager, WebLimitsConfig::default(), automatic, 1);
    (session, manager)
}

/// One more session of the given manager; `id` keeps token hashes distinct.
fn session_in(
    manager: &Arc<WebProcessRuntime>,
    limits: WebLimitsConfig,
    automatic: bool,
    id: u8,
) -> Arc<WebSession> {
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
    WebSession::new(
        Arc::downgrade(manager),
        [id; 32],
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
        automatic,
        false,
        limits,
        timeouts,
        None,
    )
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

/// Replays of an expired completed record are verified like POST replays.
#[tokio::test]
async fn completed_record_expiry_replays_verify_like_post() {
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

    // A lone final part of the expired record cannot be verified, so it is
    // stale instead of acknowledged.
    let final_replay = session.offer_get_up(None, 1, Some(0), 1, 2, pong.clone());
    assert!(matches!(final_replay, Err(GetUpReject::Stale)));
    // A full replay reassembles and the canonical claim checks its digest
    // against the applied body without reapplying frames.
    let part_replay = session.offer_get_up(None, 1, Some(0), 0, 2, head.clone());
    assert!(matches!(part_replay, Ok(GetUpOffer::Pending(0))));
    let Ok(GetUpOffer::Complete { body, .. }) =
        session.offer_get_up(None, 1, Some(0), 1, 2, pong.clone())
    else {
        panic!("a full replay completes");
    };
    let claim = session
        .claim_conveyor(None, 1, Some(0))
        .unwrap()
        .expect("conveyor mode admits the replay claim");
    assert_eq!(claim.process(&body).await, Ok(1));
    assert!(!session.state.lock().closed);

    manager.shutdown().await;
}

/// Applies sequence 1 as one two-part GET operation, then expires its record.
async fn applied_then_expired(session: &Arc<WebSession>) {
    session.configure_conveyor(4);
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let head = pong.iter().copied().cycle().take(32).collect::<Bytes>();
    assert!(matches!(
        session.offer_get_up(None, 1, Some(0), 0, 2, head),
        Ok(GetUpOffer::Pending(0))
    ));
    let Ok(GetUpOffer::Complete { body, .. }) = session.offer_get_up(None, 1, Some(0), 1, 2, pong)
    else {
        panic!("final part completes the two-part operation");
    };
    let claim = session.claim_conveyor(None, 1, Some(0)).unwrap().unwrap();
    // An automatic carrier answers 503 for a body without stream progress
    // until it commits, but the sequence is applied either way.
    let applied = claim.process(&body).await;
    assert!(matches!(
        applied,
        Ok(1) | Err(ConveyorError::Manager(ManagerError::Backpressure))
    ));
    assert_eq!(session.state.lock().conveyor.sequence_floor(None).1, 1);
    let expired = Instant::now() + Duration::from_secs(95);
    touch_peer_at(session, expired);
    assert!(!session.close_if_due(expired));
}

/// A replay of a retired applied sequence with other bytes closes the
/// session exactly like a POST body with a different digest.
#[tokio::test]
async fn retired_replay_with_another_digest_closes_the_session() {
    let (session, manager) = session();
    applied_then_expired(&session).await;

    let other = frame::encode(FrameType::Pong, 0, &[]);
    let Ok(GetUpOffer::Complete { body, .. }) =
        session.offer_get_up(None, 1, Some(0), 0, 1, other.into())
    else {
        panic!("a single-part replay reassembles");
    };
    let claim = session.claim_conveyor(None, 1, Some(0)).unwrap().unwrap();
    assert_eq!(
        claim.process(&body).await,
        Err(ConveyorError::Manager(ManagerError::Protocol))
    );
    assert!(session.state.lock().closed);

    manager.shutdown().await;
}

/// While the carrier is still negotiated a verified replay answers 503,
/// matching the canonical POST duplicate.
#[tokio::test]
async fn retired_replay_during_negotiation_is_busy() {
    let (session, manager) = session_with(true);
    applied_then_expired(&session).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let head = pong.iter().copied().cycle().take(32).collect::<Bytes>();
    assert!(matches!(
        session.offer_get_up(None, 1, Some(0), 0, 2, head),
        Ok(GetUpOffer::Pending(0))
    ));
    let Ok(GetUpOffer::Complete { body, .. }) = session.offer_get_up(None, 1, Some(0), 1, 2, pong)
    else {
        panic!("a full replay completes");
    };
    let claim = session.claim_conveyor(None, 1, Some(0)).unwrap().unwrap();
    assert_eq!(
        claim.process(&body).await,
        Err(ConveyorError::Manager(ManagerError::Backpressure))
    );
    assert!(!session.state.lock().closed);

    manager.shutdown().await;
}

/// Retained GET records of many sessions stop at the GET share of the
/// global body budget, so a POST body always finds room.
#[tokio::test]
async fn get_sessions_together_leave_the_post_reserve() {
    let mut config = ProxyConfig::default();
    config.web.limits.max_body_bytes = 64 * 1024;
    config.web.limits.max_body_bytes_global = 1024 * 1024;
    let limits = config.web.limits.clone();
    let generation = test_runtime_generation(1, config);
    let manager = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation)));
    let global = manager.body_bytes_available();
    assert_eq!(global, 1024 * 1024);

    // Four sessions each open a full conveyor window of 64 KiB operations
    // and park every non-final part: together they would hold ~1 MiB.
    let part = Bytes::from(vec![9u8; 4096]);
    let (mut stored, mut busy) = (0usize, 0usize);
    let mut sessions = Vec::new();
    for id in 1..=4u8 {
        let session = session_in(&manager, limits.clone(), false, id);
        session.configure_conveyor(4);
        for sequence in 1..=4u64 {
            for index in 0..15u32 {
                match session.offer_get_up(None, sequence, Some(0), index, 16, part.clone()) {
                    Ok(GetUpOffer::Pending(_)) => stored += 1,
                    Err(GetUpReject::Busy) => busy += 1,
                    _ => panic!("unexpected offer outcome"),
                }
            }
        }
        sessions.push(session);
    }
    assert!(stored > 0 && busy > 0, "stored={stored} busy={busy}");
    // GET holds at most half of the global budget; the rest stays free for
    // POST bodies of every vhost.
    let held = global - manager.body_bytes_available();
    assert!(held <= global / 2, "held={held}");
    assert!(manager.body_bytes_available() >= global / 2);
    assert!(manager.body_bytes_available() >= limits.max_body_bytes);
    assert_eq!(
        global / 2 - manager.get_body_bytes_available(),
        held,
        "both charges move together"
    );

    // Closing the sessions returns both budgets in full.
    for session in &sessions {
        session.close(SessionCloseReason::Protocol);
    }
    assert_eq!(manager.body_bytes_available(), global);
    assert_eq!(manager.get_body_bytes_available(), global / 2);

    manager.shutdown().await;
}
