// External browser fixture for tools/web-get-e2e: serves the real WEB HTTP
// stack behind a plain TCP listener that the GET-only nginx container proxies.
// The test is ignored by default; the runner launches it explicitly with
// TELEMT_WEB_E2E_FIXTURE=1 and reports through stdout markers.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use base64::Engine as _;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::runtime_config;
use crate::config::{WebCarrier, WebCarrierMethod, WebClientIpSource};
use crate::maestro::generation::test_runtime_generation;
use crate::web::http::serve_connection;
use crate::web::manager::WebProcessRuntime;

/// Runs the GET-carrier fixture endpoint until the fixture TTL elapses.
/// TELEMT_WEB_GET_E2E_ECHO=1 additionally switches the session backend to the
/// marker-dispatched fixture loop so the browser roundtrip does not need a DC
/// upstream; TELEMT_WEB_E2E_METHOD=post selects the canonical body carrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "fixture for tools/web-get-e2e; run via run.sh"]
async fn get_carrier_browser_fixture() {
    assert!(
        std::env::var_os("TELEMT_WEB_E2E_FIXTURE").is_some(),
        "browser fixture requires TELEMT_WEB_E2E_FIXTURE=1 (tools/web-get-e2e/run.sh)"
    );
    let carrier = match std::env::var("TELEMT_WEB_E2E_CARRIER").as_deref() {
        Ok("https-lanes") => WebCarrier::HttpsLanes,
        _ => WebCarrier::Https,
    };
    let port: u16 = std::env::var("TELEMT_WEB_E2E_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(18081);
    let ttl_secs: u64 = std::env::var("TELEMT_WEB_E2E_TTL_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(600);

    let capability = [7u8; 32];
    let mut config = runtime_config(capability, carrier);
    // TELEMT_WEB_E2E_METHOD=post keeps the canonical body carrier for the
    // baseline bench cells that must not traverse the GET-only edge.
    config.web.carrier_method = match std::env::var("TELEMT_WEB_E2E_METHOD").as_deref() {
        Ok("post") => WebCarrierMethod::Post,
        _ => WebCarrierMethod::Get,
    };
    config.web.timeouts.long_poll_secs = 30;
    config.web.limits.max_bootstraps_per_ip = 64;
    let generation = test_runtime_generation(1, config);
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(generation.clone())));
    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(capability);
    eprintln!("E2E_READY port={port} carrier={carrier:?} capability={encoded}");

    let cancel = CancellationToken::new();
    let server = {
        let cancel = cancel.clone();
        let runtime = Arc::clone(&runtime);
        async move {
            loop {
                let Ok((socket, peer)) = listener.accept().await else {
                    break;
                };
                let Ok(permit) = runtime.try_http_connection() else {
                    continue;
                };
                tokio::spawn(serve_connection(
                    socket,
                    peer,
                    WebClientIpSource::XForwardedFor,
                    Arc::from(["0.0.0.0/0".parse().unwrap()]),
                    Arc::clone(&runtime),
                    cancel.clone(),
                    permit,
                ));
            }
        }
    };
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(ttl_secs)) => {}
        _ = server => {}
    }
    cancel.cancel();
    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
