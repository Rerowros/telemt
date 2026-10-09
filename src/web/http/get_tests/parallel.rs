use super::*;

// Parallel-uplink reassembly: any non-final part may open an assembly, the
// first stored part fixes the chunk length, and the final part completes
// only once every earlier part is stored.

#[tokio::test]
async fn get_carrier_reassembles_out_of_order_nonfinal_parts() {
    let capability = [41u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    // Equal non-final chunks share the fixed chunk length; the tail is shorter.
    let third = pong.len().div_ceil(3);
    let chunks = [&pong[..third], &pong[third..2 * third], &pong[2 * third..]];
    assert_eq!(chunks[0].len(), chunks[1].len());
    assert!(chunks[2].len() <= chunks[0].len() && !chunks[2].is_empty());

    // The middle part opens the assembly before the head arrives.
    let middle = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=2&s=1&d={}&p=1&pn=3",
            b64(chunks[1])
        ),
        "",
    )
    .await;
    let (middle_headers, _) = split_response(&middle);
    assert!(middle_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(middle_headers, "x-up-part"), "1");
    assert!(!has_header(middle_headers, "x-up-ack"));

    // An exact out-of-order replay is idempotent; a changed one decoys.
    let replay = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=3&s=1&d={}&p=1&pn=3",
            b64(chunks[1])
        ),
        "",
    )
    .await;
    assert!(split_response(&replay).0.starts_with(b"HTTP/1.1 204"));
    let mut tampered = chunks[1].to_vec();
    tampered[0] ^= 0x40;
    let changed = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=4&s=1&d={}&p=1&pn=3",
            b64(&tampered)
        ),
        "",
    )
    .await;
    assert_decoy(&changed);

    // The head part stores out of order behind the earlier opener.
    let head = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5&s=1&d={}&p=0&pn=3",
            b64(chunks[0])
        ),
        "",
    )
    .await;
    assert!(split_response(&head).0.starts_with(b"HTTP/1.1 204"));

    // The final part applies the index-ordered body through the canonical
    // path; a scrambled reassembly could not decode as the Pong frame.
    let tail = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=6&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    let (tail_headers, _) = split_response(&tail);
    assert!(tail_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(tail_headers, "x-up-ack"), "1");
    assert_eq!(response_header(tail_headers, "x-up-part"), "2");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_fixes_chunk_length_from_the_first_nonfinal() {
    let capability = [42u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let third = pong.len().div_ceil(3);
    let chunks = [&pong[..third], &pong[third..2 * third], &pong[2 * third..]];

    // Part 1 opens the assembly and fixes the chunk length.
    let opener = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=2&s=1&d={}&p=1&pn=3",
            b64(chunks[1])
        ),
        "",
    )
    .await;
    assert!(split_response(&opener).0.starts_with(b"HTTP/1.1 204"));

    // A shorter non-final part deviates from the fixed chunk and decoys.
    let short = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=3&s=1&d={}&p=0&pn=3",
            b64(&chunks[0][..third - 1])
        ),
        "",
    )
    .await;
    assert_decoy(&short);
    // A longer non-final part decoys the same way.
    let long = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=4&s=1&d={}&p=0&pn=3",
            b64(&pong[..third + 1])
        ),
        "",
    )
    .await;
    assert_decoy(&long);

    // The correctly-sized head completes the non-final set.
    let head = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5&s=1&d={}&p=0&pn=3",
            b64(chunks[0])
        ),
        "",
    )
    .await;
    assert!(split_response(&head).0.starts_with(b"HTTP/1.1 204"));
    let tail = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=6&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&tail).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_rejects_early_final_parts_without_state_change() {
    let capability = [43u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let third = pong.len().div_ceil(3);
    let chunks = [&pong[..third], &pong[third..2 * third], &pong[2 * third..]];

    // A final part can never open an assembly.
    let stray_final = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=2&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    assert_decoy(&stray_final);

    // With only part 0 stored the final still arrives too early.
    let head = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=3&s=1&d={}&p=0&pn=3",
            b64(chunks[0])
        ),
        "",
    )
    .await;
    assert!(split_response(&head).0.starts_with(b"HTTP/1.1 204"));
    let early = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=4&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    assert_decoy(&early);
    // The decoyed final parked nothing: the middle part still stores cleanly.
    let middle = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5&s=1&d={}&p=1&pn=3",
            b64(chunks[1])
        ),
        "",
    )
    .await;
    assert!(split_response(&middle).0.starts_with(b"HTTP/1.1 204"));
    // The retried final now completes the operation.
    let tail = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=6&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&tail).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_bounds_the_final_part_by_the_fixed_chunk() {
    let capability = [44u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let third = pong.len().div_ceil(3);
    let chunks = [&pong[..third], &pong[third..2 * third], &pong[2 * third..]];

    for (nonce, part, chunk) in [(2u64, 0u32, chunks[0]), (3, 1, chunks[1])] {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={nonce}&s=1&d={}&p={part}&pn=3",
                b64(chunk)
            ),
            "",
        )
        .await;
        assert!(split_response(&response).0.starts_with(b"HTTP/1.1 204"));
    }
    // A final part longer than the fixed chunk decoys without mutation.
    let oversized = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=4&s=1&d={}&p=2&pn=3",
            b64(&pong[third..])
        ),
        "",
    )
    .await;
    assert_decoy(&oversized);
    // The correctly-sized final completes; an empty payload was already
    // rejected at admission, so only the length bound itself is exercised.
    let tail = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5&s=1&d={}&p=2&pn=3",
            b64(chunks[2])
        ),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&tail).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_interleaves_parallel_operations_per_sequence() {
    let capability = [45u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    // A negotiated window admits parallel confirmed sequences.
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&w=4&d={}", b64(&hello)),
        "",
    )
    .await;
    let session = response_header(split_response(&create).0, "x-session-token").to_string();

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let third = pong.len().div_ceil(3);
    let thirds = [&pong[..third], &pong[third..2 * third], &pong[2 * third..]];
    let mid = pong.len() / 2;
    let (head, tail) = pong.split_at(mid);
    // Two operations open on different part indexes and interleave freely;
    // sequence 1 keeps a missing middle slot while sequence 2 progresses.
    for (nonce, seq, total, part, chunk) in [
        (2u64, 1u64, 3u32, 1u32, thirds[1]),
        (3, 2, 2, 0, head),
        (4, 1, 3, 0, thirds[0]),
    ] {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={nonce}&s={seq}&u=0&d={}&p={part}&pn={total}",
                b64(chunk)
            ),
            "",
        )
        .await;
        let (headers, _) = split_response(&response);
        assert!(
            headers.starts_with(b"HTTP/1.1 204"),
            "seq={seq} part={part}"
        );
        assert_eq!(response_header(headers, "x-up-part"), part.to_string());
    }
    // Each final completes only its own sequence; the sequence-2 reply
    // resolves once its predecessor commits, so both finals run together.
    let second_final = format!(
        "/api/v1/up?t={session}&n=5&s=2&u=0&d={}&p=1&pn=2",
        b64(tail)
    );
    let first_final = format!(
        "/api/v1/up?t={session}&n=6&s=1&u=0&d={}&p=2&pn=3",
        b64(thirds[2])
    );
    let (r2, r1) = tokio::join!(
        get_request(&listener, &runtime, &second_final, ""),
        get_request(&listener, &runtime, &first_final, ""),
    );
    assert_eq!(response_header(split_response(&r2).0, "x-up-ack"), "2");
    assert_eq!(response_header(split_response(&r1).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
