// GET-only carrier coverage: strict query normalization, fragment
// reassembly, and decoy behavior share one raw-request harness.
// Submodules:
// - reassembly: fragment admission, replay, and sequencing bounds
// - conveyor: confirmed-window floors and out-of-order completion
// - normalization: canonical query shape, credentials, and leak fences
// - parallel: out-of-order non-final parts and fixed-chunk final gating
// - commitment: window-scaled session caps, eviction, and POST isolation

use std::sync::Arc;

use arc_swap::ArcSwap;
use base64::Engine as _;
use tokio::net::TcpListener;

use super::{request, response_header, runtime_config, split_response};
use crate::config::{ProxyConfig, WebCarrier, WebCarrierMethod, WebRuntimeDecoy};
use crate::maestro::generation::test_runtime_generation;
use crate::web::frame::{self, FrameType};
use crate::web::manager::WebProcessRuntime;

#[path = "get_tests/commitment.rs"]
mod commitment;
#[path = "get_tests/conveyor.rs"]
mod conveyor;
#[path = "get_tests/normalization.rs"]
mod normalization;
#[path = "get_tests/parallel.rs"]
mod parallel;
#[path = "get_tests/reassembly.rs"]
mod reassembly;

const HOST: &str = "proxy.example.com";

/// Builds the default test runtime with an effective GET carrier method.
fn get_runtime_config(capability: [u8; 32], carrier: WebCarrier) -> ProxyConfig {
    let mut config = runtime_config(capability, carrier);
    config.web.carrier_method = WebCarrierMethod::Get;
    config
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Issues one fresh bootstrap token through the ordinary bridge root.
async fn bootstrap(
    listener: &TcpListener,
    runtime: &Arc<WebProcessRuntime>,
    capability: [u8; 32],
) -> String {
    let encoded = b64(&capability);
    let root = format!(
        "GET /?bridge={encoded} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let response = request(listener, runtime, root).await;
    let (_, body) = split_response(&response);
    let body = std::str::from_utf8(body).unwrap();
    body.split_once("bootstrap=\"")
        .and_then(|(_, suffix)| suffix.split_once('"'))
        .map(|(token, _)| token.to_string())
        .unwrap()
}

/// Creates one GET session and returns the issued session token.
async fn get_session_token(
    listener: &TcpListener,
    runtime: &Arc<WebProcessRuntime>,
    bootstrap: &str,
    nonce: u64,
) -> String {
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = format!(
        "GET /api/v1/session?t={bootstrap}&n={nonce}&d={} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
        b64(&hello)
    )
    .into_bytes();
    let response = request(listener, runtime, create).await;
    let (headers, body) = split_response(&response);
    assert!(headers.starts_with(b"HTTP/1.1 200"));
    assert_eq!(body, frame::encode(FrameType::Welcome, 0, &[]));
    response_header(headers, "x-session-token").to_string()
}

/// Sends one raw GET carrier request on the canonical API path.
/// `up` requests carry the mirrored content type the bridge emits.
async fn get_request(
    listener: &TcpListener,
    runtime: &Arc<WebProcessRuntime>,
    target: &str,
    extra_headers: &str,
) -> Vec<u8> {
    let content_type = if target.contains("d=") && !extra_headers.contains("Content-Type") {
        "Content-Type: application/octet-stream\r\n"
    } else {
        ""
    };
    let bytes = format!(
        "GET {target} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\n{extra_headers}{content_type}Connection: close\r\n\r\n"
    )
    .into_bytes();
    request(listener, runtime, bytes).await
}

/// Asserts one invalid GET request decoys without touching carrier state.
fn assert_decoy(response: &[u8]) {
    assert!(response.starts_with(b"HTTP/1.1 404"));
}

/// Returns whether the raw response head carries the named header.
fn has_header(headers: &[u8], name: &str) -> bool {
    std::str::from_utf8(headers)
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(header, _)| header.eq_ignore_ascii_case(name))
}

#[tokio::test]
async fn get_carrier_roundtrips_session_up_down_and_close() {
    for carrier in [WebCarrier::Https, WebCarrier::HttpsLanes] {
        let capability = [21u8; 32];
        let generation = test_runtime_generation(1, get_runtime_config(capability, carrier));
        let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = bootstrap(&listener, &runtime, capability).await;
        let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;
        let lane = if carrier == WebCarrier::HttpsLanes {
            "&l=0"
        } else {
            ""
        };

        let pong = frame::encode(FrameType::Pong, 0, &[]);
        let up = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/up?t={session}&n=2&s=1{lane}&d={}", b64(&pong)),
            "",
        )
        .await;
        let (up_headers, _) = split_response(&up);
        assert!(up_headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(up_headers, "x-up-ack"), "1");
        assert_eq!(response_header(up_headers, "x-up-part"), "0");

        // The exact final retry replays the retained body without doubling counters.
        let up_retry = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/up?t={session}&n=3&s=1{lane}&d={}", b64(&pong)),
            "",
        )
        .await;
        let (up_retry_headers, _) = split_response(&up_retry);
        assert!(up_retry_headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(up_retry_headers, "x-up-ack"), "1");

        let down = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/down?t={session}&n=4&c=0{lane}"),
            "",
        )
        .await;
        let (down_headers, _) = split_response(&down);
        assert!(down_headers.starts_with(b"HTTP/1.1 204"));
        assert_eq!(response_header(down_headers, "x-down-cursor"), "0");

        let close = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/session?op=close&t={session}&n=5"),
            "",
        )
        .await;
        assert!(close.starts_with(b"HTTP/1.1 204"));
        let close_retry = get_request(
            &listener,
            &runtime,
            &format!("/api/v1/session?op=close&t={session}&n=6"),
            "",
        )
        .await;
        assert!(close_retry.starts_with(b"HTTP/1.1 204"));

        runtime.shutdown().await;
        generation.stop_sessions().await;
        generation.stop_background_tasks().await;
    }
}

#[tokio::test]
async fn get_carrier_accepts_exact_mirrored_headers() {
    let capability = [22u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;

    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/session?t={bootstrap}&n=1&d={}", b64(&hello)),
        &format!(
            "Authorization: Bearer {bootstrap}\r\nContent-Type: application/octet-stream\r\nContent-Length: 0\r\n"
        ),
    )
    .await;
    let (create_headers, _) = split_response(&create);
    assert!(create_headers.starts_with(b"HTTP/1.1 200"));
    let session = response_header(create_headers, "x-session-token").to_string();

    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let up = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={}", b64(&pong)),
        &format!(
            "Authorization: Bearer {session}\r\nX-Up-Seq: 1\r\nContent-Type: application/octet-stream\r\n"
        ),
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
async fn post_host_still_rejects_get_carrier_requests() {
    // The default POST runtime never normalizes GET carrier traffic.
    let capability = [26u8; 32];
    let generation = test_runtime_generation(1, runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let hello = frame::encode(FrameType::Hello, 0, &[1]);
    // The legacy POST path still works on the same host.
    let session = {
        let mut create = format!(
            "POST /api/v1/session HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nAuthorization: Bearer {bootstrap}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            hello.len()
        )
        .into_bytes();
        create.extend_from_slice(&hello);
        let response = request(&listener, &runtime, create).await;
        let (headers, _) = split_response(&response);
        assert!(headers.starts_with(b"HTTP/1.1 200"));
        response_header(headers, "x-session-token").to_string()
    };
    // GET carrier traffic on the POST host decoys even with a live session.
    let pong = frame::encode(FrameType::Pong, 0, &[]);
    let create = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={}", b64(&pong)),
        "",
    )
    .await;
    assert_decoy(&create);
    // And the live POST session still accepts its canonical uplink.
    let mut up = format!(
        "POST /api/v1/up HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nAuthorization: Bearer {session}\r\nContent-Type: application/octet-stream\r\nX-Up-Seq: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        pong.len()
    )
    .into_bytes();
    up.extend_from_slice(&pong);
    let up_response = request(&listener, &runtime, up).await;
    assert!(up_response.starts_with(b"HTTP/1.1 204"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_rejects_oversized_urls() {
    let capability = [27u8; 32];
    let mut config = get_runtime_config(capability, WebCarrier::Https);
    config.web.limits.get_url_bytes = 1024;
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = bootstrap(&listener, &runtime, capability).await;
    let session = get_session_token(&listener, &runtime, &bootstrap, 1).await;

    let body = vec![7u8; 900];
    let oversized = get_request(
        &listener,
        &runtime,
        &format!("/api/v1/up?t={session}&n=2&s=1&d={}", b64(&body)),
        "",
    )
    .await;
    assert_decoy(&oversized);

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn get_carrier_bootstrap_uses_the_get_bridge_page() {
    let capability = [29u8; 32];
    let generation = test_runtime_generation(1, get_runtime_config(capability, WebCarrier::Https));
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let encoded = b64(&capability);
    let root = format!(
        "GET /?bridge={encoded} HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-For: 192.0.2.10\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let response = request(&listener, &runtime, root).await;
    let (_, body) = split_response(&response);
    let body = std::str::from_utf8(body).unwrap();
    assert!(body.contains("const carrierMethod='GET'"));
    assert!(body.contains("const getUrlBytes=7168"));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
