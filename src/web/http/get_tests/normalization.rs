use super::*;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RECOVERY_TYPE: &str = "application/vnd.telemt.web-recovery+json";

#[tokio::test]
async fn get_carrier_rejects_noncanonical_and_inconsistent_queries() {
    let capability = [24u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));

    // Every malformed shape decoys instead of reaching carrier handlers.
    for target in [
        // Unknown key.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&x=1"),
        // Repeated key.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&s=2"),
        // Negative value.
        format!("/api/v1/up?t={session}&n=2&s=-1&d={pong}"),
        // Leading zeros.
        format!("/api/v1/up?t={session}&n=2&s=01&d={pong}"),
        // Missing required d.
        format!("/api/v1/up?t={session}&n=2&s=1"),
        // Invalid base64.
        format!("/api/v1/up?t={session}&n=2&s=1&d=%%%"),
        // Padded base64 alias is noncanonical.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}%3D"),
        // Part index without total.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&p=0"),
        // Zero part total.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&p=0&pn=0"),
        // Part total above the bound.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&p=0&pn=4097"),
        // Part index outside the total.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&p=2&pn=2"),
        // Missing n.
        format!("/api/v1/up?t={session}&s=1&d={pong}"),
        // Zero nonce.
        format!("/api/v1/up?t={session}&n=0&s=1&d={pong}"),
        // Lane key is query-only on the shared channel.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&l=0"),
        // Uppercase keys are not query keys.
        format!("/api/v1/up?T={session}&n=2&s=1&d={pong}"),
        // Bare segments without '=' are not key/value pairs.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&flag"),
    ] {
        assert_decoy(&get_request(&listener, &runtime, &target, "").await);
    }

    // Mirrored headers must match exactly.
    let mismatch = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={pong}"),
        "X-Up-Seq: 2\r\n",
    )
    .await;
    assert_decoy(&mismatch);
    // An unexpected mirrored header without its query key also decoys.
    let stray = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={pong}"),
        "X-Lane-ID: 0\r\n",
    )
    .await;
    assert_decoy(&stray);
    // Transfer coding is never accepted on the GET carrier.
    let chunked = format!(
        "GET /api/v1/up?t={session}&n=2&s=1&d={pong} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n"
    )
    .into_bytes();
    assert_decoy(&request(&listener, &runtime, chunked).await);
    // A nonzero content length is rejected.
    let length = format!(
        "GET /api/v1/up?t={session}&n=2&s=1&d={pong} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx"
    )
    .into_bytes();
    assert_decoy(&request(&listener, &runtime, length).await);
    // Routes without a canonical content type reject any content type.
    let typed = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?op=close&t={session}&n=2"),
        "Content-Type: text/plain\r\n",
    )
    .await;
    assert_decoy(&typed);
    // An incompatible content type on a data route also decoys.
    let typed = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={pong}"),
        "Content-Type: text/plain\r\n",
    )
    .await;
    assert_decoy(&typed);
    // Credentials outside the canonical query never authenticate.
    let stray_token = get_request(
        &listener,
        &runtime,
        "/api/v1/up?t=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&n=2&s=1&d=AQ",
        "",
    )
    .await;
    assert_decoy(&stray_token);

    // None of the rejections consumed the sequence: the canonical send works.
    let up = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=9&s=1&d={pong}"),
        "",
    )
    .await;
    let (up_headers, _) = split_response(&up);
    assert!(up_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(up_headers, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_accepts_stripped_content_type() {
    // A CDN front may strip headers; query-only requests carry no
    // Content-Type and the canonical value is injected internally.
    let capability = [39u8; 32];
    let mut config = get_runtime_config(capability, WebCarrier::Https);
    config.web.debug.enabled = true;
    config.web.debug.sideband = true;
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = format!(
        "GET /api/v1/session?t={bootstrap}&n=1&d={} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n",
        b64(&hello)
    )
    .into_bytes();
    let create_response = request(&listener, &runtime, create).await;
    let (create_headers, _) = split_response(&create_response);
    assert!(create_headers.starts_with(b"HTTP/1.1 200"));
    let session = response_header(create_headers, "x-session-token").to_string();

    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));
    let up = format!(
        "GET /api/v1/up?t={session}&n=2&s=1&d={pong} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let up_response = request(&listener, &runtime, up).await;
    let (up_headers, _) = split_response(&up_response);
    assert!(up_headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(up_headers, "x-up-ack"), "1");

    // A headerless diagnostic report uses the bootstrap credential scope.
    let report = b64(br#"{"v":1,"event":"runtime_started"}"#);
    let diagnostic = format!(
        "GET /api/v1/diagnostic?t={bootstrap}&n=3&d={report} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let diagnostic_response = request(&listener, &runtime, diagnostic).await;
    assert!(diagnostic_response.starts_with(b"HTTP/1.1 204"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_misscoped_credentials_stay_decoy() {
    let capability = [41u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));
    let hello = b64(&frame::encode(FrameType::Hello, 0, &[1]));

    for target in [
        // A session token cannot mint another session.
        format!("/api/v1/session?t={session}&n=2&d={hello}"),
        // A bootstrap token cannot drive session traffic.
        format!("/api/v1/up?t={bootstrap}&n=3&s=1&d={pong}"),
        format!("/api/v1/down?t={bootstrap}&n=4&c=0"),
        // A syntactically valid but unknown token authenticates nothing.
        format!("/api/v1/up?t={}&n=5&s=1&d={pong}", "B".repeat(43)),
        format!("/api/v1/session?op=close&t={}&n=6", "B".repeat(43)),
    ] {
        assert_decoy(&get_request(&listener, &runtime, &target, "").await);
    }
    // The real session continues unharmed after the probing storm.
    let up = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=7&s=1&d={pong}"),
        "",
    )
    .await;
    assert_eq!(response_header(split_response(&up).0, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

/// Captures exactly one request the HTTP decoy upstream receives.
async fn capture_upstream(origin: &TcpListener) -> Vec<u8> {
    let (mut stream, _) = origin.accept().await.unwrap();
    let mut captured = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = stream.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "decoy origin closed before request completed");
        captured.extend_from_slice(&buffer[..read]);
        let Some(header_end) = captured.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&captured[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        if captured.len() >= header_end + 4 + content_length {
            break;
        }
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\norigin")
        .await
        .unwrap();
    captured
}

#[tokio::test]
async fn get_carrier_malformed_query_never_reaches_http_upstream() {
    let capability = [42u8; 32];
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = get_runtime_config(capability, WebCarrier::Https);
    let runtime_config = Arc::get_mut(config.web.runtime.as_mut().unwrap()).unwrap();
    let vhost = Arc::get_mut(runtime_config.vhosts.get_mut(HOST).unwrap()).unwrap();
    vhost.decoy = WebRuntimeDecoy::HttpUpstream {
        addr: origin.local_addr().unwrap(),
        authority: "decoy.example".to_string(),
    };
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));

    for target in [
        // Authentic token with an unknown key still cannot escape upstream.
        format!("/api/v1/up?t={session}&n=2&s=1&d={pong}&x=1"),
        // Authentic token with corrupt payload material.
        format!("/api/v1/up?t={session}&n=3&s=1&d=%%%"),
        // An authentic-looking but unknown token decoys the same way.
        format!("/api/v1/up?t={}&n=4&s=1&d={pong}&pn=0", "B".repeat(43)),
        // A real bootstrap credential on a wrong route stays fenced as well.
        format!("/api/v1/up?t={bootstrap}&n=5&s=1&d={pong}"),
    ] {
        let bytes = format!(
            "GET {target} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        let (response, captured) = tokio::join!(request(&listener, &runtime, bytes), async {
            // Recognized credentials are fenced before forwarding; only an
            // unrecognized token reaches the upstream, already sanitized.
            tokio::time::timeout(std::time::Duration::from_secs(1), capture_upstream(&origin))
                .await
                .ok()
        });
        if let Some(captured) = captured {
            let forwarded = std::str::from_utf8(&captured).unwrap();
            assert!(
                forwarded.starts_with("GET /api/v1/up HTTP/1.1"),
                "decoy upstream must see only the sanitized path, got: {forwarded:?}"
            );
            for leaked in [
                session.as_str(),
                bootstrap.as_str(),
                "t=",
                "d=",
                "pn=",
                pong.as_str(),
            ] {
                assert!(
                    !forwarded.contains(leaked),
                    "carrier material leaked to the decoy upstream: {leaked}"
                );
            }
            assert!(!forwarded.contains("Authorization"));
        } else {
            // The secrets fence answered locally with a private 404.
            assert!(response.starts_with(b"HTTP/1.1 404"));
        }
    }

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_failed_injection_never_leaks_headers() {
    let capability = [44u8; 32];
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = get_runtime_config(capability, WebCarrier::Https);
    let runtime_config = Arc::get_mut(config.web.runtime.as_mut().unwrap()).unwrap();
    let vhost = Arc::get_mut(runtime_config.vhosts.get_mut(HOST).unwrap()).unwrap();
    vhost.decoy = WebRuntimeDecoy::HttpUpstream {
        addr: origin.local_addr().unwrap(),
        authority: "decoy.example".to_string(),
    };
    // Two bridge pages are issued: the decoyed probe and the valid retry.
    config.web.limits.max_bootstraps_per_ip = 2;
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token = bootstrap(&listener, &runtime, capability).await;
    let hello = b64(&frame::encode(FrameType::Hello, 0, &[1]));

    // A header-safe capability value precedes one that cannot become a
    // header: normalization must fail before any carrier header is inserted.
    let broken = format!(
        "GET /api/v1/session?t={token}&n=1&k=https&f=%0a&d={hello} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let (response, captured) = tokio::join!(request(&listener, &runtime, broken), async {
        tokio::time::timeout(std::time::Duration::from_secs(1), capture_upstream(&origin))
            .await
            .ok()
    });
    assert!(response.starts_with(b"HTTP/1.1 404"));
    if let Some(captured) = captured {
        let forwarded = std::str::from_utf8(&captured).unwrap();
        for leaked in [
            "authorization",
            "x-carrier-",
            "x-up-seq",
            "x-telemt-up-window",
        ] {
            assert!(
                !forwarded.to_lowercase().contains(leaked),
                "injected carrier header leaked to the decoy upstream: {leaked}"
            );
        }
    }

    // The same route with every value header-safe normalizes cleanly.
    let second = bootstrap(&listener, &runtime, capability).await;
    let valid = format!(
        "GET /api/v1/session?t={second}&n=2&d={hello} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let response = request(&listener, &runtime, valid).await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "got: {}",
        String::from_utf8_lossy(&response)
    );

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_recovery_nonce_is_unique_and_scoped() {
    let capability = [43u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let encoded = b64(&capability);
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    // Recovery authenticates the retired bearer plus one canonical nonce.
    let recover = |bearer: &str, suffix: &str, accept: bool| {
        let accept = accept
            .then_some(format!("Accept: {RECOVERY_TYPE}\r\n"))
            .unwrap_or_default();
        format!(
            "GET /?bridge={encoded}{suffix} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\n{accept}Authorization: Bearer {bearer}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes()
    };
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
    let response = request(&listener, &runtime, recover(&session, "&n=5", true)).await;
    let (headers, body) = split_response(&response);
    assert!(
        headers.starts_with(b"HTTP/1.1 200"),
        "got: {:?}",
        String::from_utf8_lossy(&response[..response.len().min(300)])
    );
    assert_eq!(response_header(headers, "content-type"), RECOVERY_TYPE);
    // The issued recovery bootstrap mints the replacement session.
    let recovered_bootstrap = std::str::from_utf8(body)
        .unwrap()
        .split_once("\"bootstrap\":\"")
        .and_then(|(_, suffix)| suffix.split_once('"'))
        .map(|(token, _)| token.to_string())
        .unwrap();
    let session2 = get_session_token(&listener, &runtime, &recovered_bootstrap, 2).await;
    // A second recovery carries a different nonce for the new bearer.
    let response = request(&listener, &runtime, recover(&session2, "&n=6", true)).await;
    let (_, body2) = split_response(&response);
    assert!(response.starts_with(b"HTTP/1.1 200"));
    let recovered_bootstrap2 = std::str::from_utf8(body2)
        .unwrap()
        .split_once("\"bootstrap\":\"")
        .and_then(|(_, suffix)| suffix.split_once('"'))
        .map(|(token, _)| token.to_string())
        .unwrap();
    let session3 = get_session_token(&listener, &runtime, &recovered_bootstrap2, 3).await;

    // Malformed nonces decoy without touching the live bearer.
    for suffix in ["&n=0", "&n=01", "&n=x", "&n=5&x=1", "&n=5&n=6"] {
        let response = request(&listener, &runtime, recover(&session3, suffix, true)).await;
        assert_decoy(&response);
    }
    // The nonce suffix is never accepted on the ordinary bridge grammar.
    let response = request(&listener, &runtime, recover(&session3, "&n=7", false)).await;
    assert_decoy(&response);
    // And the pre-GET nonceless recovery contract still works on a live bearer.
    let response = request(&listener, &runtime, recover(&session3, "", true)).await;
    assert!(response.starts_with(b"HTTP/1.1 200"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
