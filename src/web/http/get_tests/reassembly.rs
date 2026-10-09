use super::*;

#[tokio::test]
async fn get_carrier_reassembles_ordered_parts_and_replays_final() {
    let capability = [23u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let mid = pong.len() / 2;
    let (head, tail) = pong.split_at(mid);
    // Part 0 stores without claiming the conveyor sequence.
    let part0 = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={}&p=0&pn=2", b64(head)),
        "",
    )
    .await;
    let (part0_headers, _) = split_response(&part0);
    assert!(part0_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(part0_headers, "x-up-part"), "0");
    assert!(!has_header(part0_headers, "x-up-ack"));

    // An exact duplicate of part 0 is a pending replay, not an error.
    let part0_retry = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=3&s=1&d={}&p=0&pn=2", b64(head)),
        "",
    )
    .await;
    let (part0_retry_headers, _) = split_response(&part0_retry);
    assert!(part0_retry_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(part0_retry_headers, "x-up-part"), "0");

    // A changed duplicate of part 0 decoys without advancing the assembly.
    let changed = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=4&s=1&d={}&p=0&pn=2", b64(tail)),
        "",
    )
    .await;
    assert_decoy(&changed);

    // Out-of-order part numbers cannot skip ahead of the assembly.
    let skipped = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=5&s=1&d={}&p=1&pn=3", b64(tail)),
        "",
    )
    .await;
    assert_decoy(&skipped);

    // The final part applies the reassembled body through the canonical path.
    let part1 = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=6&s=1&d={}&p=1&pn=2", b64(tail)),
        "",
    )
    .await;
    let (part1_headers, _) = split_response(&part1);
    assert!(part1_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(part1_headers, "x-up-ack"), "1");
    assert_eq!(response_header(part1_headers, "x-up-part"), "1");

    // A retained completed record replays the final part without reapplying.
    let part1_retry = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=7&s=1&d={}&p=1&pn=2", b64(tail)),
        "",
    )
    .await;
    let (part1_retry_headers, _) = split_response(&part1_retry);
    assert!(part1_retry_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(part1_retry_headers, "x-up-ack"), "1");

    // The next sequence continues on the canonical conveyor.
    let next = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=8&s=2&d={}", b64(&pong)),
        "",
    )
    .await;
    let (next_headers, _) = split_response(&next);
    assert!(next_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(next_headers, "x-up-ack"), "2");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_sequences_and_lanes_assemble_independently() {
    let capability = [25u8; 32];
    let generation =
        test_runtime_generation(1, get_runtime_config(capability, WebCarrier::HttpsLanes));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    // Two lanes hold independent assemblies for the same sequence number.
    // Lane bodies carry frames whose stream id equals the lane id.
    let bodies = [
        frame::encode(FrameType::Pong, 0, &[]),
        frame::encode(FrameType::Open, 1, &[]),
    ];
    for (index, lane) in ["&l=0", "&l=1"].iter().enumerate() {
        let head = &bodies[index][..bodies[index].len() / 2];
        let part0 = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n=2&s=1{lane}&d={}&p=0&pn=2",
                b64(head)
            ),
            "",
        )
        .await;
        assert!(split_response(&part0).0.starts_with(b"HTTP/1.1 204"));
    }
    for (index, lane) in ["&l=0", "&l=1"].iter().enumerate() {
        let tail = &bodies[index][bodies[index].len() / 2..];
        let part1 = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n=4&s=1{lane}&d={}&p=1&pn=2",
                b64(tail)
            ),
            "",
        )
        .await;
        let (headers, _) = split_response(&part1);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(headers, "x-up-ack"), "1");
    }

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_rejects_nonzero_initial_parts() {
    let capability = [32u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    // A non-zero first part cannot open an assembly: the head of the
    // logical body would be silently truncated.
    for part in [1u32, 2] {
        let total = part + 1;
        let rejected = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s=1&d={}&p={part}&pn={total}",
                part + 1,
                b64(&pong)
            ),
            "",
        )
        .await;
        assert_decoy(&rejected);
    }
    // Nothing was consumed: a proper two-part operation still completes.
    let mid = pong.len() / 2;
    for (part, chunk) in [pong[..mid].to_vec(), pong[mid..].to_vec()]
        .iter()
        .enumerate()
    {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s=1&d={}&p={part}&pn=2",
                part + 10,
                b64(chunk)
            ),
            "",
        )
        .await;
        let (headers, _) = split_response(&response);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(headers, "x-up-part"), part.to_string());
    }

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_completed_replay_rejects_changed_parts() {
    let capability = [33u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let mid = pong.len() / 2;
    let (head, tail) = pong.split_at(mid);
    for (part, chunk) in [head, tail].iter().enumerate() {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s=1&d={}&p={part}&pn=2",
                part + 2,
                b64(chunk)
            ),
            "",
        )
        .await;
        assert!(split_response(&response).0.starts_with(b"HTTP/1.1 204"));
    }

    // A mutated final fragment of the same length cannot claim the ack.
    let mut tampered = tail.to_vec();
    tampered[0] ^= 0x40;
    for target in [
        // Changed final part bytes.
        format!(
            "/api/v1/up?t={session}&n=10&s=1&d={}&p=1&pn=2",
            b64(&tampered)
        ),
        // Changed non-final part bytes.
        format!(
            "/api/v1/up?t={session}&n=11&s=1&d={}&p=0&pn=2",
            b64(&tampered[..mid])
        ),
        // Changed part count.
        format!("/api/v1/up?t={session}&n=12&s=1&d={}&p=1&pn=3", b64(tail)),
        // Changed part length.
        format!("/api/v1/up?t={session}&n=13&s=1&d={}&p=1&pn=2", b64(&pong)),
    ] {
        assert_decoy(&get_request(&listener, &runtime, &target, "").await);
    }

    // The exact retained replay still returns the original ack.
    let replay = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=14&s=1&d={}&p=1&pn=2", b64(tail)),
        "",
    )
    .await;
    let (headers, _) = split_response(&replay);
    assert!(headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(headers, "x-up-ack"), "1");
    // An exact non-final replay stays an idempotent part acknowledgment.
    let replay0 = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=15&s=1&d={}&p=0&pn=2", b64(head)),
        "",
    )
    .await;
    let (headers0, _) = split_response(&replay0);
    assert!(headers0.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(headers0, "x-up-part"), "0");
    assert!(!has_header(headers0, "x-up-ack"));

    // The next sequence still advances on the canonical conveyor.
    let next = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=16&s=2&d={}", b64(&pong)),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&next).0, "x-up-ack"), "2");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_commitment_cap_returns_busy_until_capacity_frees() {
    let capability = [34u8; 32];
    let config = get_runtime_config(capability, WebCarrier::Https);
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    // A negotiated window admits parallel sequences on the shared channel.
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&w=4&d={}", b64(&hello)),
        "",
    )
    .await;
    let session = response_header(split_response(&create).0, "x-session-token").to_string();

    // Completing bodies must parse as frames: every retained part is a run
    // of canonical Pong frames so the final operation applies cleanly.
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let part: Vec<u8> = pong.iter().copied().cycle().take(5000).collect();
    let open = |nonce: u64, sequence: u64, total: u32| {
        format!(
            "/api/v1/up?t={session}&n={nonce}&s={sequence}&u=0&d={}&p=0&pn={total}",
            b64(&part)
        )
    };
    // Sequence 1 stays pending while two pn=4096 fillers each commit the
    // per-operation ceiling; a third non-head opening exceeds (W-1)*C.
    let first = get_request(&listener, &runtime, &open(2, 1, 2), "").await;
    assert!(split_response(&first).0.starts_with(b"HTTP/1.1 204"));
    for (nonce, sequence) in [(3u64, 2u64), (4, 3)] {
        let response = get_request(&listener, &runtime, &open(nonce, sequence, 4096), "").await;
        assert!(split_response(&response).0.starts_with(b"HTTP/1.1 204"));
    }
    let blocked = get_request(&listener, &runtime, &open(5, 4, 4096), "").await;
    assert!(
        split_response(&blocked).0.starts_with(b"HTTP/1.1 503"),
        "session cap exhaustion answers busy so the bridge retries"
    );
    // Busy parked no record: replaying an accepted filler part still works.
    let replay = get_request(&listener, &runtime, &open(3, 2, 4096), "").await;
    assert!(split_response(&replay).0.starts_with(b"HTTP/1.1 204"));

    // Completing sequence 1 applies it; its committed completed record is then
    // evictable under pressure, which frees exactly the slot sequence 4 needs.
    let tail = frame::encode(FrameType::Pong, 0, &[]);
    let finish = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=6&s=1&u=0&d={}&p=1&pn=2",
            b64(&tail)
        ),
        "",
    )
    .await;
    let finish_head = split_response(&finish).0;
    assert!(finish_head.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(finish_head, "x-up-ack"), "1");
    let retried = get_request(&listener, &runtime, &open(7, 4, 4096), "").await;
    assert!(
        split_response(&retried).0.starts_with(b"HTTP/1.1 204"),
        "the evicted committed record frees the retried opening"
    );

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_reassembles_with_a_single_body_reader() {
    let capability = [35u8; 32];
    let mut config = get_runtime_config(capability, WebCarrier::Https);
    config.web.limits.max_body_readers = 1;
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap_token = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap_token, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let mid = pong.len() / 2;
    for (part, chunk) in [pong[..mid].to_vec(), pong[mid..].to_vec()]
        .iter()
        .enumerate()
    {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s=1&d={}&p={part}&pn=2",
                part + 2,
                b64(chunk)
            ),
            "",
        )
        .await;
        let (headers, _) = split_response(&response);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
    }
    // Reader permits are returned: a second session still opens and uploads.
    let bootstrap_next = bootstrap(&listener, &runtime, capability).await;
    let session2 = get_session_token(&listener, &runtime, &bootstrap_next, 20).await;
    let up = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session2}&n=21&s=1&d={}", b64(&pong)),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&up).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_close_clears_pending_fragments() {
    let capability = [28u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let head = &pong[..pong.len() / 2];
    let part0 = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={}&p=0&pn=2", b64(head)),
        "",
    )
    .await;
    assert!(split_response(&part0).0.starts_with(b"HTTP/1.1 204"));
    let close = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?op=close&t={session}&n=3"),
        "",
    )
    .await;
    assert!(close.starts_with(b"HTTP/1.1 204"));
    // Late fragments on a closed session decoy instead of resurrecting state.
    let tail = &pong[pong.len() / 2..];
    let late = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=4&s=1&d={}&p=1&pn=2", b64(tail)),
        "",
    )
    .await;
    assert_decoy(&late);

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
