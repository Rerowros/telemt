use super::*;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Header payload: a GET uplink fragment may carry base64url payload in
// indexed `x-up-data-N` headers after the URL `d` value. The server joins
// them in index order, enforces the page-owned budget frozen with the
// session, and treats any malformed family member exactly like a malformed
// query (decoy).

const LINE: usize = crate::config::GET_HEADER_CHUNK_CHARS;

/// GET runtime with header payload enabled at `bytes` and a request-head
/// limit that fits it.
fn header_runtime_config(capability: [u8; 32], carrier: WebCarrier, bytes: usize) -> ProxyConfig {
    let mut config = get_runtime_config(capability, carrier);
    config.web.limits.max_header_bytes = 65536;
    config.web.limits.get_header_bytes = bytes;
    config
}

/// One applicable uplink body of exactly `len` bytes: OPEN plus one DATA
/// frame on `stream`.
fn open_data_body(stream: u32, len: usize) -> Vec<u8> {
    let mut body = frame::encode(FrameType::Open, stream, &[]).to_vec();
    body.extend_from_slice(&frame::encode(
        FrameType::Data,
        stream,
        &vec![5u8; len - 16],
    ));
    body
}

/// Splits encoded data into the URL `d` value and payload header lines.
fn split_encoded(encoded: &str, url_chars: usize, line: usize) -> (String, Vec<String>) {
    let (data, rest) = encoded.split_at(url_chars.min(encoded.len()));
    let lines = rest
        .as_bytes()
        .chunks(line)
        .map(|chunk| String::from_utf8(chunk.to_vec()).unwrap())
        .collect();
    (data.to_string(), lines)
}

/// Renders payload header lines with explicit names, in the given order.
fn header_lines(lines: &[(String, String)]) -> String {
    lines
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect()
}

/// Indexed payload header lines in index order.
fn indexed(lines: &[String]) -> Vec<(String, String)> {
    lines
        .iter()
        .enumerate()
        .map(|(index, value)| (format!("X-Up-Data-{index}"), value.clone()))
        .collect()
}

fn status(response: &[u8]) -> &[u8] {
    &response[..response.len().min(12)]
}

#[tokio::test]
async fn get_header_payload_joins_url_data_and_lines_in_index_order() {
    for carrier in [WebCarrier::Https, WebCarrier::HttpsLanes] {
        let capability = [71u8; 32];
        let generation =
            test_runtime_generation(1, header_runtime_config(capability, carrier, 40000));
        let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = bootstrap(&listener, &runtime, capability).await;
        let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
        let (lane, stream) = if carrier == WebCarrier::HttpsLanes {
            ("&l=1", 1)
        } else {
            ("", 1)
        };

        // One 30000-byte body cannot fit the 7168-byte URL budget: the URL
        // carries 4000 characters and five payload lines carry the rest.
        let body = open_data_body(stream, 30000);
        let (data, lines) = split_encoded(&b64(&body), 4000, LINE);
        assert_eq!(lines.len(), 5);
        // Wire order is irrelevant: the lines are sent in reverse.
        let mut reversed = indexed(&lines);
        reversed.reverse();
        let response = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/up?t={session}&n=2&s=1{lane}&d={data}"),
            &header_lines(&reversed),
        )
        .await;
        let (headers, _) = split_response(&response);
        assert!(
            headers.starts_with(b"HTTP/1.1 204"),
            "carrier={carrier:?} got {}",
            String::from_utf8_lossy(status(&response))
        );
        assert_eq!(response_header(headers, "x-up-ack"), "1");

        runtime.shutdown().await;
        generation.stop_sessions().await;
        generation.stop_background_tasks().await;
    }
}

#[tokio::test]
async fn get_header_payload_rejects_gaps_duplicates_and_bad_lines() {
    let capability = [72u8; 32];
    let generation = test_runtime_generation(
        1,
        header_runtime_config(capability, WebCarrier::Https, 40000),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let body = open_data_body(1, 20000);
    let (data, lines) = split_encoded(&b64(&body), 4000, LINE);
    assert_eq!(lines.len(), 3);
    let named = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    };
    let oversized = "A".repeat(LINE + 1);
    let cases: Vec<(&str, Vec<(String, String)>)> = vec![
        // Index 1 is missing: the joined payload would silently lose a line.
        (
            "gap",
            named(&[("X-Up-Data-0", &lines[0]), ("X-Up-Data-2", &lines[2])]),
        ),
        // A line that does not start at index 0.
        (
            "no head",
            named(&[("X-Up-Data-1", &lines[0]), ("X-Up-Data-2", &lines[1])]),
        ),
        // The same index twice, even with the order the bridge would use.
        (
            "duplicate",
            named(&[
                ("X-Up-Data-0", &lines[0]),
                ("X-Up-Data-0", &lines[1]),
                ("X-Up-Data-1", &lines[2]),
            ]),
        ),
        // Non-canonical and out-of-range indexes.
        (
            "leading zero",
            named(&[
                ("X-Up-Data-0", &lines[0]),
                ("X-Up-Data-01", &lines[1]),
                ("X-Up-Data-2", &lines[2]),
            ]),
        ),
        ("out of range", named(&[("X-Up-Data-7", &lines[0])])),
        ("not a number", named(&[("X-Up-Data-x", &lines[0])])),
        // A line above the fixed chunk length or with a non-alphabet byte.
        ("oversized line", named(&[("X-Up-Data-0", &oversized)])),
        ("bad alphabet", named(&[("X-Up-Data-0", "AAA+")])),
        // A line boundary that breaks the joined canonical encoding.
        (
            "non-canonical join",
            named(&[("X-Up-Data-0", &lines[0]), ("X-Up-Data-1", "A")]),
        ),
    ];
    for (nonce, (name, case)) in cases.iter().enumerate() {
        let response = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/up?t={session}&n={}&s=1&d={data}", nonce + 2),
            &header_lines(case),
        )
        .await;
        assert!(
            response.starts_with(b"HTTP/1.1 404"),
            "case {name} was not decoyed: {}",
            String::from_utf8_lossy(status(&response))
        );
    }

    // One line four characters above the chunk length still joins into a
    // canonical, applicable body: only the per-line bound rejects it.
    let long_body = open_data_body(1, (4000 + LINE + 4) / 4 * 3);
    let (long_data, long_lines) = split_encoded(&b64(&long_body), 4000, LINE + 4);
    assert_eq!(long_lines.len(), 1);
    assert_eq!(long_lines[0].len(), LINE + 4);
    let long = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=39&s=1&d={long_data}"),
        &format!("X-Up-Data-0: {}\r\n", long_lines[0]),
    )
    .await;
    assert_decoy(&long);

    // An empty payload line is malformed as well.
    let empty = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=40&s=1&d={data}"),
        "X-Up-Data-0: \r\n",
    )
    .await;
    assert_decoy(&empty);

    // Nothing above consumed the sequence: the well-formed request applies.
    let valid = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=41&s=1&d={data}"),
        &header_lines(&indexed(&lines)),
    )
    .await;
    let (headers, _) = split_response(&valid);
    assert!(headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(headers, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_header_payload_is_capped_by_the_frozen_budget() {
    let capability = [73u8; 32];
    let generation = test_runtime_generation(
        1,
        header_runtime_config(capability, WebCarrier::Https, 8000),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    // 4000 URL characters plus 8004 header characters: four over budget.
    let over = open_data_body(1, 9003);
    let (data, lines) = split_encoded(&b64(&over), 4000, LINE);
    assert_eq!(lines.iter().map(String::len).sum::<usize>(), 8004);
    let response = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={data}"),
        &header_lines(&indexed(&lines)),
    )
    .await;
    assert_decoy(&response);

    // Exactly at the budget the same sequence applies.
    let fits = open_data_body(1, 9000);
    let (data, lines) = split_encoded(&b64(&fits), 4000, LINE);
    assert_eq!(lines.iter().map(String::len).sum::<usize>(), 8000);
    let response = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=3&s=1&d={data}"),
        &header_lines(&indexed(&lines)),
    )
    .await;
    let (headers, _) = split_response(&response);
    assert!(headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(headers, "x-up-ack"), "1");

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_header_payload_is_not_accepted_when_the_feature_is_off() {
    let capability = [74u8; 32];
    // Default limits: header payload off, default request-head limit.
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let pong = b64(&frame::encode(FrameType::Pong, 0, &[]));
    let (data, tail) = pong.split_at(4);
    // A small payload split into a header line is still header payload.
    let response = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={data}"),
        &format!("X-Up-Data-0: {tail}\r\n"),
    )
    .await;
    assert_decoy(&response);
    // Even a header that would not change the payload is refused.
    let response = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=3&s=1&d={pong}"),
        "X-Up-Data-0: AAAA\r\n",
    )
    .await;
    assert_decoy(&response);
    // The URL-only form of the same sequence still applies.
    let response = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=4&s=1&d={pong}"),
        "",
    )
    .await;
    assert!(split_response(&response).0.starts_with(b"HTTP/1.1 204"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_header_payload_is_uplink_only() {
    let capability = [75u8; 32];
    let generation = test_runtime_generation(
        1,
        header_runtime_config(capability, WebCarrier::Https, 40000),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    let hello = b64(&frame::encode(FrameType::Hello, 0, &[1]));
    let (data, tail) = hello.split_at(4);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&d={data}"),
        &format!("X-Up-Data-0: {tail}\r\n"),
    )
    .await;
    assert_decoy(&create);
    let session = get_session_token(&listener, &runtime, &bootstrap, 2).await;
    let down = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/down?t={session}&n=3&c=0"),
        "X-Up-Data-0: AAAA\r\n",
    )
    .await;
    assert_decoy(&down);

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_header_fragments_replay_and_verify_like_url_fragments() {
    let capability = [76u8; 32];
    let generation = test_runtime_generation(
        1,
        header_runtime_config(capability, WebCarrier::Https, 16000),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    // Two equal 15000-byte non-final parts and a short final, each over the
    // URL-only fragment size.
    let mut body = open_data_body(1, 15000);
    body.extend_from_slice(&frame::encode(FrameType::Data, 1, &vec![7u8; 15000 - 8]));
    body.extend_from_slice(&frame::encode(FrameType::Data, 1, &vec![8u8; 9000 - 8]));
    let chunks = [&body[..15000], &body[15000..30000], &body[30000..]];
    let send = |nonce: u64, part: usize, url_chars: usize, bytes: Vec<u8>| {
        let listener = &listener;
        let runtime = &runtime;
        let session = session.clone();
        async move {
            let (data, lines) = split_encoded(&b64(&bytes), url_chars, LINE);
            get_request(
                listener,
                runtime,
                &format!("/api/v1/up?t={session}&n={nonce}&s=1&d={data}&p={part}&pn=3"),
                &header_lines(&indexed(&lines)),
            )
            .await
        }
    };
    for (part, chunk) in chunks.iter().enumerate().take(2) {
        let response = send(10 + part as u64, part, 4000, chunk.to_vec()).await;
        let (headers, _) = split_response(&response);
        assert!(headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(headers, "x-up-part"), part.to_string());
    }
    // A replay split at another URL/header boundary carries the same bytes.
    let replay = send(20, 1, 6000, chunks[1].to_vec()).await;
    assert!(split_response(&replay).0.starts_with(b"HTTP/1.1 204"));
    // A changed replay byte in a header line decoys.
    let mut tampered = chunks[1].to_vec();
    tampered[14000] ^= 1;
    assert_decoy(&send(21, 1, 4000, tampered.clone()).await);
    let final_part = send(22, 2, 4000, chunks[2].to_vec()).await;
    let (headers, _) = split_response(&final_part);
    assert!(headers.starts_with(b"HTTP/1.1 204"));
    assert_eq!(response_header(headers, "x-up-ack"), "1");
    // The retained completed record verifies header-carried replays too.
    let replay = send(23, 0, 4000, chunks[0].to_vec()).await;
    assert!(split_response(&replay).0.starts_with(b"HTTP/1.1 204"));
    assert_decoy(&send(24, 1, 4000, tampered).await);
    let final_replay = send(25, 2, 1000, chunks[2].to_vec()).await;
    assert_eq!(
        response_header(split_response(&final_replay).0, "x-up-ack"),
        "1"
    );

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

/// Captures exactly one request the HTTP decoy upstream receives.
async fn capture_upstream(origin: &TcpListener) -> Vec<u8> {
    let (mut stream, _) = origin.accept().await.unwrap();
    let mut captured = Vec::new();
    let mut buffer = [0u8; 16384];
    loop {
        let read = stream.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "decoy origin closed before request completed");
        captured.extend_from_slice(&buffer[..read]);
        if captured.windows(4).any(|window| window == b"\r\n\r\n") {
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
async fn get_header_payload_never_reaches_the_decoy_upstream() {
    let capability = [77u8; 32];
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = header_runtime_config(capability, WebCarrier::Https, 40000);
    let runtime_config = Arc::get_mut(config.web.runtime.as_mut().unwrap()).unwrap();
    let vhost = Arc::get_mut(runtime_config.vhosts.get_mut(HOST).unwrap()).unwrap();
    vhost.decoy = WebRuntimeDecoy::HttpUpstream {
        addr: origin.local_addr().unwrap(),
        authority: "decoy.example".to_string(),
    };
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let marker = "QUJDREVGR0hJSktM".repeat(8);

    // An unknown token with header payload: the probe gets the same decoy
    // as any other invalid carrier request, without the payload lines.
    let target = format!("/api/v1/up?t={}&n=2&s=1&d=AAAA", "B".repeat(43));
    let bytes = format!(
        "GET {target} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nX-Up-Data-0: {marker}\r\nX-Up-Data-1: {marker}\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let (response, captured) = tokio::join!(request(&listener, &runtime, bytes), async {
        tokio::time::timeout(std::time::Duration::from_secs(1), capture_upstream(&origin))
            .await
            .ok()
    });
    let captured = captured.expect("an unknown token reaches the decoy origin");
    let forwarded = std::str::from_utf8(&captured).unwrap();
    assert!(forwarded.starts_with("GET /api/v1/up HTTP/1.1"));
    assert!(!forwarded.to_lowercase().contains("x-up-data"));
    assert!(!forwarded.contains(&marker));
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert!(response.ends_with(b"origin"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

/// Fetches one bridge page and returns its body with the bootstrap token.
async fn bridge_page(
    listener: &TcpListener,
    runtime: &Arc<WebProcessRuntime>,
    capability: [u8; 32],
) -> (String, String) {
    let encoded = b64(&capability);
    let root = format!(
        "GET /?bridge={encoded} HTTP/1.1
Host: {HOST}
X-Forwarded-For: 192.0.2.10
Connection: close

"
    )
    .into_bytes();
    let response = request(listener, runtime, root).await;
    let body = String::from_utf8_lossy(split_response(&response).1).into_owned();
    let token = body
        .split_once("bootstrap=\"")
        .and_then(|(_, suffix)| suffix.split_once('"'))
        .map(|(token, _)| token.to_string())
        .unwrap();
    (body, token)
}

#[tokio::test]
async fn get_header_budget_is_frozen_per_page_across_reload() {
    let capability = [78u8; 32];
    let mut config = header_runtime_config(capability, WebCarrier::Https, 16000);
    config.web.limits.max_bootstraps_per_ip = 4;
    let generation = test_runtime_generation(1, config);
    let active = Arc::new(ArcSwap::from(Arc::clone(&generation)));
    let runtime = WebProcessRuntime::start(Arc::clone(&active));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (enabled_page, enabled_bootstrap) = bridge_page(&listener, &runtime, capability).await;
    assert!(enabled_page.contains(&format!(
        "const getHeaderBytes=16000,getHeaderChunk={LINE};"
    )));

    // A hot reload turns header payload off through the vhost override.
    let mut config = header_runtime_config(capability, WebCarrier::Https, 16000);
    config.web.limits.max_bootstraps_per_ip = 4;
    let runtime_config = Arc::get_mut(config.web.runtime.as_mut().unwrap()).unwrap();
    Arc::get_mut(runtime_config.vhosts.get_mut(HOST).unwrap())
        .unwrap()
        .get_header_bytes = Some(0);
    let reloaded = test_runtime_generation(2, config);
    active.store(Arc::clone(&reloaded));
    let (disabled_page, disabled_bootstrap) = bridge_page(&listener, &runtime, capability).await;
    assert!(disabled_page.contains("const getHeaderBytes=0,"));

    // A session created from the earlier page keeps its page budget; the
    // new page's session keeps URL-only grammar.
    let body = open_data_body(1, 12000);
    let (data, lines) = split_encoded(&b64(&body), 4000, LINE);
    for (bootstrap, accepted) in [(enabled_bootstrap, true), (disabled_bootstrap, false)] {
        let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
        let response = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/up?t={session}&n=2&s=1&d={data}"),
            &header_lines(&indexed(&lines)),
        )
        .await;
        assert_eq!(
            response.starts_with(b"HTTP/1.1 204"),
            accepted,
            "got {}",
            String::from_utf8_lossy(status(&response))
        );
    }

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
    reloaded.stop_sessions().await;
    reloaded.stop_background_tasks().await;
}

#[tokio::test]
async fn get_payload_headers_skip_the_credential_scan_only_on_the_uplink_route() {
    let capability = [80u8; 32];
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = header_runtime_config(capability, WebCarrier::Https, 40000);
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

    // On the uplink route payload lines are not HMAC-scanned for
    // credentials (a fragment would cost one HMAC pair per base64url
    // position); they are stripped before the decoy forwards anything.
    let target = format!("/api/v1/up?t={}&n=2&s=1&d=AAAA", "B".repeat(43));
    let bytes = format!(
        "GET {target} HTTP/1.1
Host: {HOST}
X-Forwarded-For: 192.0.2.10
X-Up-Data-0: {session}
Connection: close

"
    )
    .into_bytes();
    let (response, captured) = tokio::join!(request(&listener, &runtime, bytes), async {
        tokio::time::timeout(std::time::Duration::from_secs(1), capture_upstream(&origin))
            .await
            .ok()
    });
    let captured = captured.expect("the sanitized probe reaches the decoy origin");
    let forwarded = std::str::from_utf8(&captured).unwrap();
    assert!(!forwarded.contains(&session));
    assert!(!forwarded.to_lowercase().contains("x-up-data"));
    assert!(response.ends_with(b"origin"));

    // Any other route keeps the full fence: a credential in a payload-named
    // header is answered locally and never forwarded.
    let bytes = format!(
        "GET /index.html HTTP/1.1
Host: {HOST}
X-Forwarded-For: 192.0.2.10
X-Up-Data-0: {session}
Connection: close

"
    )
    .into_bytes();
    let (response, captured) = tokio::join!(request(&listener, &runtime, bytes), async {
        tokio::time::timeout(std::time::Duration::from_secs(1), capture_upstream(&origin))
            .await
            .ok()
    });
    assert!(
        captured.is_none(),
        "a fenced credential reached the decoy origin"
    );
    assert!(response.starts_with(b"HTTP/1.1 404"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
