use super::*;

#[tokio::test]
async fn get_carrier_conveyor_sequences_complete_out_of_order() {
    let capability = [36u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    // Negotiating an uplink window opts the session into confirmed ordering.
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&w=4&d={}", b64(&hello)),
        "",
    )
    .await;
    let (create_headers, _) = split_response(&create);
    assert!(create_headers.starts_with(b"HTTP/1.1 200"));
    assert_eq!(response_header(create_headers, "x-telemt-up-window"), "4");
    let session = response_header(create_headers, "x-session-token").to_string();

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let mid = pong.len() / 2;
    // Four in-window sequences hold simultaneous partial assemblies.
    for (index, sequence) in [1u64, 2, 3, 4].iter().enumerate() {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s={sequence}&u=0&d={}&p=0&pn=2",
                index + 2,
                b64(&pong[..mid])
            ),
            "",
        )
        .await;
        let (headers, _) = split_response(&response);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert!(!has_header(headers, "x-up-ack"));
    }
    // Finals arrive out of order; each resolves only after predecessors commit.
    let tail = b64(&pong[mid..]);
    let target = |sequence: u64, nonce: u64| {
        format!("/api/v1/up?t={session}&n={nonce}&s={sequence}&u=0&d={tail}&p=1&pn=2")
    };
    let t3 = target(3, 10);
    let t1 = target(1, 11);
    let t4 = target(4, 12);
    let t2 = target(2, 13);
    let (r3, r1, r4, r2) = tokio::join!(
        get_request(&listener, &runtime, &t3, ""),
        get_request(&listener, &runtime, &t1, ""),
        get_request(&listener, &runtime, &t4, ""),
        get_request(&listener, &runtime, &t2, ""),
    );
    for (response, sequence) in [(&r3, "3"), (&r1, "1"), (&r4, "4"), (&r2, "2")] {
        let (headers, _) = split_response(response);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(headers, "x-up-ack"), sequence);
        assert_eq!(response_header(headers, "x-up-part"), "1");
    }

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_bounds_conveyor_floor_window_and_confirmation() {
    let capability = [37u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&w=4&d={}", b64(&hello)),
        "",
    )
    .await;
    let session = response_header(split_response(&create).0, "x-session-token").to_string();
    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));

    // Confirmation cannot run ahead of the applied sequence.
    let over_confirmed = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&u=1&d={pong}&p=0&pn=2"),
        "",
    )
    .await;
    assert_decoy(&over_confirmed);
    // A sequence beyond the confirmed floor plus the negotiated window fails.
    let future = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=3&s=6&u=0&d={pong}&p=0&pn=2"),
        "",
    )
    .await;
    assert_decoy(&future);
    // None of the rejections parked a record: the in-window send still works.
    let valid = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=4&s=1&u=0&d={pong}"),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&valid).0, "x-up-ack"), "1");
    // After commit, the same sequence cannot open a new assembly.
    let stale = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=5&s=1&u=0&d={pong}&p=0&pn=3"),
        "",
    )
    .await;
    assert_decoy(&stale);

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_rejects_confirmation_on_legacy_sessions() {
    let capability = [38u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));

    // A legacy-mode session never negotiated confirmations.
    let hinted = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&u=0&d={pong}&p=0&pn=2"),
        "",
    )
    .await;
    assert_decoy(&hinted);
    // A far-future sequence cannot open a record either.
    let future = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=3&s=9&d={pong}&p=0&pn=2"),
        "",
    )
    .await;
    assert_decoy(&future);
    let valid = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=4&s=1&d={pong}"),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&valid).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
