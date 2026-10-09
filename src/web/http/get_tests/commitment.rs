use super::*;

// Session commitment accounting: window-scaled caps answer Busy, committed
// completed records are evictable under pressure, and a flooded GET session
// never starves a neighbouring POST vhost sharing the process.

#[tokio::test]
async fn get_carrier_pending_cap_scales_with_the_conveyor_window() {
    let capability = [48u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    // The uplink window negotiation opts the session into confirmed ordering.
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&w=4&d={}", b64(&hello)),
        "",
    )
    .await;
    let create_headers = split_response(&create).0;
    assert!(create_headers.starts_with(b"HTTP/1.1 200"));
    assert_eq!(response_header(create_headers, "x-telemt-up-window"), "4");
    let session = response_header(create_headers, "x-session-token").to_string();

    // Defaults: max_body_bytes = 2 MiB, window = 4. Each ~2 MiB operation
    // commits about C = max_body + GET_MAX_PARTS*meta, so three non-head
    // operations saturate the (W-1)*C share while the head stays unblocked.
    // Every retained part is a complete valid frame batch: one OPEN then one
    // DATA per first part and DATA afterwards, on a per-sequence stream so
    // applied bodies pass client-shape validation and never decoy.
    let total = 400u32;
    let chunk_len = 5120usize;
    let part_bytes = |sequence: u64, part: u32| {
        let stream = 10 + sequence as u32;
        if part == 0 {
            let mut batch = frame::encode(FrameType::Open, stream, &[]).to_vec();
            batch.extend_from_slice(&frame::encode(
                FrameType::Data,
                stream,
                &vec![9u8; chunk_len - 16],
            ));
            batch
        } else {
            frame::encode(FrameType::Data, stream, &vec![9u8; chunk_len - 8]).to_vec()
        }
    };
    for part in 0..total - 1 {
        for sequence in 1..=3u64 {
            let nonce = 100 + sequence * 1000 + part as u64;
            let response = get_request(
                &listener,
                &runtime,
                &format!(
                    "/api/v1/up?t={session}&n={nonce}&s={sequence}&u=0&d={}&p={part}&pn={total}",
                    b64(&part_bytes(sequence, part))
                ),
                "",
            )
            .await;
            assert!(
                split_response(&response).0.starts_with(b"HTTP/1.1 204"),
                "seq={sequence} part={part}"
            );
        }
    }

    // A fourth non-head operation exceeds the (W-1)*C commitment share: the
    // session answers 503 instead of a decoy so the bridge retries it.
    let fourth = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5000&s=4&u=0&d={}&p=0&pn={total}",
            b64(&part_bytes(4, 0))
        ),
        "",
    )
    .await;
    assert!(
        split_response(&fourth).0.starts_with(b"HTTP/1.1 503"),
        "cap exhaustion must be busy, not decoy"
    );

    // Completing the head applies sequence 1; its committed completed record
    // is then evictable under pressure, which frees the fourth operation's slot.
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let first_final = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=5001&s=1&u=0&d={}&p={}&pn={total}",
            b64(&pong),
            total - 1
        ),
        "",
    )
    .await;
    assert_eq!(
        response_header(split_response(&first_final).0, "x-up-ack"),
        "1"
    );
    for part in 0..total - 1 {
        let response = get_request(
            &listener,
            &runtime,
            &format!(
                "/api/v1/up?t={session}&n={}&s=4&u=0&d={}&p={part}&pn={total}",
                6000 + part as u64,
                b64(&part_bytes(4, part))
            ),
            "",
        )
        .await;
        assert!(
            split_response(&response).0.starts_with(b"HTTP/1.1 204"),
            "seq=4 part={part} retries after the head completes"
        );
    }

    // Remaining finals complete out of order; every reply carries its own ack.
    let finals: Vec<(u64, _)> = [4u64, 3, 2]
        .into_iter()
        .map(|sequence| {
            (
                sequence,
                format!(
                    "/api/v1/up?t={session}&n={}&s={sequence}&u=0&d={}&p={}&pn={total}",
                    7000 + sequence,
                    b64(&pong),
                    total - 1
                ),
            )
        })
        .collect();
    let (r4, r3, r2) = tokio::join!(
        get_request(&listener, &runtime, &finals[0].1, ""),
        get_request(&listener, &runtime, &finals[1].1, ""),
        get_request(&listener, &runtime, &finals[2].1, ""),
    );
    for (response, sequence) in [(&r4, "4"), (&r3, "3"), (&r2, "2")] {
        let headers = split_response(response).0;
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(headers, "x-up-ack"), sequence);
        assert_eq!(
            response_header(headers, "x-up-part"),
            (total - 1).to_string()
        );
    }

    // Completed records retire once the client confirms them, and the released
    // leases admit a fresh in-window operation.
    let next = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=8000&s=5&u=4&d={}&p=0&pn=2",
            b64(&[
                frame::encode(FrameType::Open, 15, &[]).to_vec(),
                frame::encode(FrameType::Data, 15, b"pad").to_vec(),
            ]
            .concat())
        ),
        "",
    )
    .await;
    assert!(split_response(&next).0.starts_with(b"HTTP/1.1 204"));
    let last = get_request(
        &listener,
        &runtime,
        &format!(
            "/api/v1/up?t={session}&n=8001&s=5&u=4&d={}&p=1&pn=2",
            b64(&frame::encode(FrameType::Data, 15, b"tail"))
        ),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&last).0, "x-up-ack"), "5");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_garbage_flood_never_starves_post_neighbor() {
    let capability = [49u8; 32];
    let post_capability = [50u8; 32];
    let mut config = get_runtime_config(capability, WebCarrier::HttpsLanes);
    {
        // The neighbouring vhost keeps the canonical POST carrier while the
        // GET host floods its own reassembly budget.
        let runtime_config = Arc::get_mut(config.web.runtime.as_mut().unwrap()).unwrap();
        let post_profile = Arc::new(crate::config::WebRuntimeProfile {
            host: "other.example.com".to_string(),
            public_addr: "203.0.113.20:443".parse().unwrap(),
            user: "bob".to_string(),
            secret_mode: crate::config::WebSecretMode::Plain,
            carrier: WebCarrier::Https,
            carrier_negotiation_enabled: false,
            carrier_learning: false,
            carriers: Arc::from([WebCarrier::Https]),
            carrier_negotiation_deadlines_secs: [3, 5, 8, 12],
            capability: post_capability,
            credential_id: [1; 16],
            key_fingerprint: "1111111111111111".to_string(),
            max_sessions: 4,
            max_streams: 16,
            max_streams_per_session: 4,
        });
        let vhost =
            Arc::get_mut(runtime_config.vhosts.get_mut("other.example.com").unwrap()).unwrap();
        vhost.carrier_method = Some(WebCarrierMethod::Post);
        vhost.capabilities = vec![post_capability].into_boxed_slice();
        vhost.profiles = vec![Arc::clone(&post_profile)];
        // Issuance re-resolves the matched profile against the runtime table.
        runtime_config.profiles.push(post_profile);
        // Two bootstraps (one per vhost) share the forwarded test address.
        config.web.limits.max_bootstraps_per_ip = 4;
    }
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let free_at_start = runtime.body_bytes_available();

    let bootstrap_of = |host: &str, capability: [u8; 32]| {
        let encoded = b64(&capability);
        format!(
            "GET /?bridge={encoded} HTTP/1.1\r\nHost: {host}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
        )
        .into_bytes()
    };
    let get_bootstrap = {
        let response = request(&listener, &runtime, bootstrap_of(HOST, capability)).await;
        let (_, body) = split_response(&response);
        std::str::from_utf8(body)
            .unwrap()
            .split_once("bootstrap=\"")
            .and_then(|(_, suffix)| suffix.split_once('"'))
            .map(|(token, _)| token.to_string())
            .unwrap()
    };
    let post_bootstrap = {
        let response = request(
            &listener,
            &runtime,
            bootstrap_of("other.example.com", post_capability),
        )
        .await;
        let (_, body) = split_response(&response);
        std::str::from_utf8(body)
            .unwrap()
            .split_once("bootstrap=\"")
            .and_then(|(_, suffix)| suffix.split_once('"'))
            .map(|(token, _)| token.to_string())
            .unwrap()
    };

    // A lane-capable GET session negotiates the four-deep conveyor window.
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = format!(
        "GET /api/v1/session?t={get_bootstrap}&n=1&w=4&d={} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n",
        b64(&hello)
    )
    .into_bytes();
    let create = request(&listener, &runtime, create).await;
    let session = response_header(split_response(&create).0, "x-session-token").to_string();

    // Garbage pn=4096 openings on independent lane heads each commit the
    // per-operation ceiling; the session cap stops the fifth at 4*C.
    let junk = vec![9u8; 500];
    for lane in 1..=4u32 {
        let response = request(
            &listener,
            &runtime,
            format!(
                "GET /api/v1/up?t={session}&n={}&l={lane}&s=1&u=0&d={}&p=0&pn=4096 HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n",
                100 + lane,
                b64(&junk)
            )
            .into_bytes(),
        )
        .await;
        assert!(
            split_response(&response).0.starts_with(b"HTTP/1.1 204"),
            "lane={lane}"
        );
    }
    let overflow = request(
        &listener,
        &runtime,
        format!(
            "GET /api/v1/up?t={session}&n=200&l=5&s=1&u=0&d={}&p=0&pn=4096 HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n",
            b64(&junk)
        )
        .into_bytes(),
    )
    .await;
    assert!(
        split_response(&overflow).0.starts_with(b"HTTP/1.1 503"),
        "session cap exhaustion answers busy, not decoy"
    );

    // The session's global lease stays below W*C: metadata plus the bytes
    // actually accepted, nothing like the committed worst-case sum.
    let ceiling = crate::config::WebLimitsConfig::default().max_body_bytes
        + crate::config::GET_MAX_PARTS as usize * 64;
    assert!(
        free_at_start - runtime.body_bytes_available() <= 4 * ceiling,
        "global charge must stay inside W*C"
    );

    // The POST neighbour on the same process still admits ordinary traffic.
    let mut post_create = format!(
        "POST /api/v1/session HTTP/1.1\r\nHost: other.example.com\r\nX-Forwarded-For: 192.0.2.10\r\nAuthorization: Bearer {post_bootstrap}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        hello.len()
    )
    .into_bytes();
    post_create.extend_from_slice(&hello);
    let post_create = request(&listener, &runtime, post_create).await;
    let post_headers = split_response(&post_create).0;
    assert!(post_headers.starts_with(b"HTTP/1.1 200"));
    let post_session = response_header(post_headers, "x-session-token").to_string();
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let mut post_up = format!(
        "POST /api/v1/up HTTP/1.1\r\nHost: other.example.com\r\nX-Forwarded-For: 192.0.2.10\r\nAuthorization: Bearer {post_session}\r\nContent-Type: application/octet-stream\r\nX-Up-Seq: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        pong.len()
    )
    .into_bytes();
    post_up.extend_from_slice(&pong);
    let post_up = request(&listener, &runtime, post_up).await;
    let post_up_headers = split_response(&post_up).0;
    assert!(post_up_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(post_up_headers, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
