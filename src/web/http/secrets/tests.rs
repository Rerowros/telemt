use std::sync::Arc;

use arc_swap::ArcSwap;
use base64::Engine as _;

use super::*;
use crate::config::WebCarrier;
use crate::maestro::generation::test_runtime_generation;
use crate::web::http::tests::runtime_config_with_base;

fn percent_encode_byte(value: u8, lowercase: bool) -> String {
    if lowercase {
        format!("%{value:02x}")
    } else {
        format!("%{value:02X}")
    }
}

fn found(text: &[u8], config: &WebRuntimeConfig, runtime: &WebProcessRuntime) -> bool {
    let mut checks = CREDENTIAL_CHECKS_PER_REQUEST;
    contains_secret(text, &mut checks, config, runtime) == Scan::Secret
}

#[tokio::test]
async fn scanner_recognizes_exact_credentials_across_encoding_boundaries() {
    let capability = [71u8; 32];
    let generation = test_runtime_generation(
        1,
        runtime_config_with_base(capability, WebCarrier::Https, "/relay/"),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(Arc::clone(&generation))));
    let generation_config = generation.config();
    let config = generation_config.web.runtime.as_ref().unwrap();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(capability);

    assert!(found(encoded.as_bytes(), config, &runtime));
    assert!(found(
        format!("prefix{encoded}suffix").as_bytes(),
        config,
        &runtime,
    ));
    for index in 0..encoded.len() {
        let mut escaped = String::with_capacity(encoded.len() + 2);
        escaped.push_str(&encoded[..index]);
        escaped.push_str(&percent_encode_byte(
            encoded.as_bytes()[index],
            index % 2 == 0,
        ));
        escaped.push_str(&encoded[index + 1..]);
        assert!(
            found(escaped.as_bytes(), config, &runtime),
            "percent-encoded byte {index} was not recognized"
        );
    }

    let profile = config.profiles[0].clone();
    let bootstrap = runtime
        .issue_bootstrap(profile, "192.0.2.10".parse().unwrap())
        .unwrap()
        .token;
    let escaped_bootstrap = bootstrap
        .bytes()
        .enumerate()
        .map(|(index, byte)| percent_encode_byte(byte, index % 2 == 0))
        .collect::<String>();
    assert!(found(bootstrap.as_bytes(), config, &runtime));
    assert!(found(escaped_bootstrap.as_bytes(), config, &runtime,));

    let split = format!("{}%zz{}", &encoded[..20], &encoded[20..]);
    assert!(!found(split.as_bytes(), config, &runtime));
    let inactive = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([72u8; 32]);
    assert!(!found(inactive.as_bytes(), config, &runtime));

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn scanner_spends_a_bounded_number_of_keyed_checks() {
    let capability = [73u8; 32];
    let generation = test_runtime_generation(
        1,
        runtime_config_with_base(capability, WebCarrier::Https, "/relay/"),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(Arc::clone(&generation))));
    let generation_config = generation.config();
    let config = generation_config.web.runtime.as_ref().unwrap();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(capability);

    // Every 43-character window of a run of `A` is a canonical credential,
    // the worst case for keyed checks; 60 KB of it stops at the budget.
    let mut checks = CREDENTIAL_CHECKS_PER_REQUEST;
    let junk = "A".repeat(60_000);
    assert!(contains_secret(junk.as_bytes(), &mut checks, config, &runtime) == Scan::Exhausted);
    assert_eq!(checks, 0);

    // A credential the budget still reaches is found; one past it is not
    // claimed as found, only reported as unscanned.
    let within = format!(
        "{}.{encoded}",
        "A".repeat(42 + CREDENTIAL_CHECKS_PER_REQUEST - 1)
    );
    let mut checks = CREDENTIAL_CHECKS_PER_REQUEST;
    assert!(contains_secret(within.as_bytes(), &mut checks, config, &runtime) == Scan::Secret);
    assert_eq!(checks, 0);
    let past = format!(
        "{}.{encoded}",
        "A".repeat(42 + CREDENTIAL_CHECKS_PER_REQUEST)
    );
    let mut checks = CREDENTIAL_CHECKS_PER_REQUEST;
    assert!(contains_secret(past.as_bytes(), &mut checks, config, &runtime) == Scan::Exhausted);

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}

#[tokio::test]
async fn exhausted_scan_fences_the_uri_and_strips_unscanned_headers() {
    let capability = [74u8; 32];
    let generation = test_runtime_generation(
        1,
        runtime_config_with_base(capability, WebCarrier::Https, "/relay/"),
    );
    let runtime = WebProcessRuntime::start(Arc::new(ArcSwap::from(Arc::clone(&generation))));
    let generation_config = generation.config();
    let config = generation_config.web.runtime.as_ref().unwrap();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(capability);

    let mut request = Request::builder()
        .uri(format!("/{}", "A".repeat(2_000)))
        .body(())
        .unwrap();
    mark_internal_credential(&mut request, config, &runtime, false);
    assert!(has_internal_credential(&request));

    let mut request = Request::builder()
        .uri("/")
        .header("x-small", "kept")
        .header("x-junk", "A".repeat(60_000))
        .header("cookie", format!("opaque={encoded}"))
        .body(())
        .unwrap();
    mark_internal_credential(&mut request, config, &runtime, false);
    assert!(!has_internal_credential(&request));
    remove_unscanned_headers(&mut request);
    assert_eq!(request.headers().get("x-small").unwrap(), "kept");
    assert!(request.headers().get("x-junk").is_none());
    assert!(request.headers().get("cookie").is_none());

    runtime.shutdown().await;
    generation.stop_sessions().await;
    generation.stop_background_tasks().await;
}
