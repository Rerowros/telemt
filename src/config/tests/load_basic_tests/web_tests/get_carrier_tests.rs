use super::*;

#[test]
fn get_carrier_method_loads_and_roundtrips() {
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"\ncarrier_method = \"get\"",
    );
    let config = load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert_eq!(config.web.carrier_method, WebCarrierMethod::Get);
    assert!(
        config
            .web
            .effective_carrier_method("proxy.example.com")
            .is_get()
    );
    let json = serde_json::to_value(&config.web).unwrap();
    assert_eq!(json["carrier_method"], "get");
    let decoded: WebConfig = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.carrier_method, WebCarrierMethod::Get);
    let serialized = toml::to_string(&config.web).unwrap();
    let decoded: WebConfig = toml::from_str(&serialized).unwrap();
    assert_eq!(decoded.carrier_method, WebCarrierMethod::Get);
}

#[test]
fn get_carrier_method_rejects_unknown_tokens() {
    for value in ["\"GET\"", "\"head\"", "\"patch\""] {
        let source = WEB_CONFIG.replace(
            "carrier = \"https-lanes\"",
            &format!("carrier = \"https-lanes\"\ncarrier_method = {value}"),
        );
        assert!(load_config_error_from_temp_toml(&source).contains("carrier_method"));
    }
}

#[test]
fn get_carrier_method_vhost_override_resolves_per_host() {
    let source = r#"
[access.users]
alice = "000102030405060708090a0b0c0d0e0f"

[[server.listeners]]
ip = "127.0.0.1"
port = 18080
transport = "web"
proxy_protocol = false
web_client_ip_source = "x_forwarded_for"
web_trusted_proxy_cidrs = ["127.0.0.1/32"]

[web]
enabled = true
carrier = "https"
carrier_method = "get"

[[web.vhosts]]
host = "proxy.example.com"
public_addr = "203.0.113.10:443"
carrier_method = "post"

[web.vhosts.decoy]
mode = "http_upstream"
upstream = "http://127.0.0.1:18081"

[[web.vhosts.profiles]]
user = "alice"
secret_mode = "dd"

[[web.vhosts]]
host = "get.example.com"
public_addr = "203.0.113.11:443"

[web.vhosts.decoy]
mode = "http_upstream"
upstream = "http://127.0.0.1:18081"

[[web.vhosts.profiles]]
user = "alice"
secret_mode = "dd"
"#;
    let config = load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(
        config
            .web
            .effective_carrier_method("get.example.com")
            .is_get()
    );
    assert_eq!(
        config.web.effective_carrier_method("proxy.example.com"),
        WebCarrierMethod::Post
    );
    assert!(config.web.get_carrier_fits_limits());
}

#[test]
fn get_carrier_method_rejects_websocket_candidates() {
    // A global GET method with no explicit vhosts drives every host.
    for carriers in ["websocket", "websocket-lanes"] {
        let source = format!(
            "[general]\nconfig_strict = true\n{}",
            WEB_CONFIG.replace(
                "carrier = \"https-lanes\"",
                &format!("carrier = \"{carriers}\"\ncarrier_method = \"get\"")
            )
        );
        let error = load_config_error_from_temp_toml(&source);
        assert!(
            error.contains("WebSocket"),
            "carrier={carriers} must be rejected, got: {error}"
        );
    }
    // WebSocket fallback candidates are rejected while any host is GET.
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"\ncarriers = [\"https\", \"websocket\"]\ncarrier_method = \"get\"",
    );
    let error =
        load_config_error_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(error.contains("WebSocket"), "got: {error}");
    // A per-host GET override rejects WebSocket candidates as well.
    let source = WEB_CONFIG
        .replace(
            "carrier = \"https-lanes\"",
            "carrier = \"https-lanes\"\ncarriers = [\"https\", \"websocket\"]",
        )
        .replace(
            "public_addr = \"203.0.113.10:443\"",
            "public_addr = \"203.0.113.10:443\"\ncarrier_method = \"get\"",
        );
    let error =
        load_config_error_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(error.contains("WebSocket"), "got: {error}");
}

#[test]
fn get_carrier_method_allows_websocket_when_no_effective_get_host() {
    // Global GET with an explicit POST override on the only vhost leaves no
    // effective GET host, so WebSocket candidates stay usable.
    let source = WEB_CONFIG
        .replace(
            "carrier = \"https-lanes\"",
            "carrier = \"https-lanes\"\ncarriers = [\"https\", \"websocket\"]\ncarrier_method = \"get\"",
        )
        .replace(
            "public_addr = \"203.0.113.10:443\"",
            "public_addr = \"203.0.113.10:443\"\ncarrier_method = \"post\"",
        );
    let config = load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(
        !config
            .web
            .effective_carrier_method("proxy.example.com")
            .is_get()
    );
    assert!(config.web.get_carrier_fits_limits());
}

#[test]
fn get_url_bytes_bounds_are_enforced() {
    for (value, expect_ok) in [
        (1023usize, false),
        (1024, true),
        (7500, true),
        (7501, false),
    ] {
        // Keep the carrier batch small enough to fit the minimal URL budget.
        let source = WEB_CONFIG.replace(
            "carrier = \"https-lanes\"",
            &format!(
                "carrier = \"https-lanes\"\ncarrier_method = \"get\"\n\n[web.limits]\nget_url_bytes = {value}\ncarrier_batch_bytes = 4096\nmax_frame_payload_bytes = 512"
            ),
        );
        if expect_ok {
            let config =
                load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
            assert_eq!(config.web.limits.get_url_bytes, value);
        } else {
            let error = load_config_error_from_temp_toml(&format!(
                "[general]\nconfig_strict = true\n{source}"
            ));
            assert!(
                error.contains("get_url_bytes"),
                "value={value} got: {error}"
            );
        }
    }
}

#[test]
fn get_url_budget_must_fit_the_configured_batch() {
    // The default 2 MiB batch cannot travel inside the minimal URL budget.
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"\ncarrier_method = \"get\"\n\n[web.limits]\nget_url_bytes = 1024",
    );
    let error =
        load_config_error_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(error.contains("get_url_bytes"), "got: {error}");

    // Host and base path consume URL budget before payload room; a maximal
    // valid hostname on the minimal budget still leaves no GET capacity.
    let long_host = format!(
        "{}.{}.{}.{}.example.com",
        "a".repeat(55),
        "b".repeat(55),
        "c".repeat(55),
        "d".repeat(55)
    );
    let source = WEB_CONFIG
        .replace("proxy.example.com", &long_host)
        .replace("Proxy.Example.COM", &long_host)
        .replace(
            "carrier = \"https-lanes\"",
            "carrier = \"https-lanes\"\ncarrier_method = \"get\"\n\n[web.limits]\nget_url_bytes = 1024",
        );
    let error =
        load_config_error_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(error.contains("get_url_bytes"), "got: {error}");
}

#[test]
fn get_parallel_parts_defaults_and_bounds() {
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"\ncarrier_method = \"get\"",
    );
    let config = load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert_eq!(config.web.limits.get_parallel_parts, 6);

    for (value, expect_ok) in [(0usize, false), (1, true), (16, true), (17, false)] {
        let source = WEB_CONFIG.replace(
            "carrier = \"https-lanes\"",
            &format!(
                "carrier = \"https-lanes\"\ncarrier_method = \"get\"\n\n[web.limits]\nget_parallel_parts = {value}"
            ),
        );
        if expect_ok {
            let config =
                load_config_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
            assert_eq!(config.web.limits.get_parallel_parts, value);
        } else {
            let error = load_config_error_from_temp_toml(&format!(
                "[general]\nconfig_strict = true\n{source}"
            ));
            assert!(
                error.contains("get_parallel_parts"),
                "value={value} got: {error}"
            );
        }
    }
}

#[test]
fn get_parallel_parts_is_independent_of_body_readers() {
    // A GET fragment holds its reader permit only for the synchronous
    // admission step, so the page-side parallelism never needs a reader
    // each: a reader-limited config must keep loading for every vhost.
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"

[web.limits]
max_body_readers = 4",
    );
    let config = load_config_from_temp_toml(&format!(
        "[general]
config_strict = true
{source}"
    ));
    assert_eq!(config.web.limits.max_body_readers, 4);
    assert_eq!(config.web.limits.get_parallel_parts, 6);

    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        "carrier = \"https-lanes\"
carrier_method = \"get\"

[web.limits]
get_parallel_parts = 16
max_body_readers = 4",
    );
    let config = load_config_from_temp_toml(&format!(
        "[general]
config_strict = true
{source}"
    ));
    assert_eq!(config.web.limits.get_parallel_parts, 16);
}

/// Envelope that absorbs 64 KiB request heads for the default connection
/// count (1024 x 64 KiB = 64 MiB instead of 16 MiB).
const RAISED_ENVELOPE: &str = "memory_envelope_bytes = 1610612736";

/// GET-method WEB config with extra `[web.limits]` lines.
fn get_config_with_limits(limits: &str) -> String {
    let source = WEB_CONFIG.replace(
        "carrier = \"https-lanes\"",
        &format!("carrier = \"https-lanes\"\ncarrier_method = \"get\"\n\n[web.limits]\n{limits}"),
    );
    format!("[general]\nconfig_strict = true\n{source}")
}

#[test]
fn get_header_bytes_defaults_off_and_validates_bounds() {
    let config = load_config_from_temp_toml(&get_config_with_limits(""));
    assert_eq!(config.web.limits.get_header_bytes, 0);
    assert_eq!(
        config.web.effective_get_header_bytes("proxy.example.com"),
        0
    );

    // Fits the default 16 KiB request head: 7168 URL + 1024 + 32 + 4096.
    for (limits, expect_ok) in [
        ("get_header_bytes = 1024", true),
        ("get_header_bytes = 1023", false),
        ("get_header_bytes = 1", false),
        // 7168 + 5120 + 32 + 4096 = 16416 > 16384.
        ("get_header_bytes = 5120", false),
        ("get_header_bytes = 5088", true),
        ("max_header_bytes = 65536\nget_header_bytes = 53760", true),
        ("max_header_bytes = 65536\nget_header_bytes = 53761", false),
        // 7168 + 24000 + 4 * 32 + 4096 = 35392 > 32768; a shorter URL budget
        // leaves that room to the header payload.
        (
            "max_header_bytes = 32768\nget_url_bytes = 4096\nget_header_bytes = 24000",
            true,
        ),
        ("max_header_bytes = 32768\nget_header_bytes = 24000", false),
    ] {
        let source = get_config_with_limits(&format!("{limits}\n{RAISED_ENVELOPE}"));
        if expect_ok {
            let config = load_config_from_temp_toml(&source);
            assert!(config.web.get_carrier_fits_limits(), "{limits}");
        } else {
            let error = load_config_error_from_temp_toml(&source);
            assert!(
                error.contains("get_header_bytes") && error.contains("max_header_bytes"),
                "{limits} got: {error}"
            );
        }
    }
}

#[test]
fn get_header_bytes_prod_profile_fits_with_a_raised_envelope() {
    // A production-like process: 4096 connections with 64 KiB request heads
    // reserve 256 MiB for headers, which the default 1280 MiB envelope with
    // its default pending and body reservations cannot absorb.
    let limits = "max_http_connections = 4096\nmax_header_bytes = 65536\nget_header_bytes = 40000";
    let error = load_config_error_from_temp_toml(&get_config_with_limits(limits));
    assert!(error.contains("memory_envelope_bytes"), "got: {error}");
    let config = load_config_from_temp_toml(&get_config_with_limits(&format!(
        "{limits}\n{RAISED_ENVELOPE}"
    )));
    assert_eq!(config.web.limits.get_header_bytes, 40000);
    assert_eq!(
        config.web.effective_get_header_bytes("proxy.example.com"),
        40000
    );
}

#[test]
fn get_header_bytes_vhost_override_resolves_and_is_guarded() {
    let with_override = |method: &str, bytes: usize, limits: &str| {
        let source = WEB_CONFIG
            .replace(
                "carrier = \"https-lanes\"",
                &format!("carrier = \"https-lanes\"\ncarrier_method = \"{method}\"\n\n[web.limits]\n{limits}"),
            )
            .replace(
                "public_addr = \"203.0.113.10:443\"",
                &format!("public_addr = \"203.0.113.10:443\"\ncarrier_method = \"get\"\nget_header_bytes = {bytes}"),
            );
        format!("[general]\nconfig_strict = true\n{source}")
    };
    // A per-host budget on a GET host overrides the global limit both ways.
    let raised = format!("max_header_bytes = 65536\n{RAISED_ENVELOPE}");
    let config = load_config_from_temp_toml(&with_override("post", 40000, &raised));
    assert_eq!(config.web.limits.get_header_bytes, 0);
    assert_eq!(
        config.web.effective_get_header_bytes("proxy.example.com"),
        40000
    );
    let config = load_config_from_temp_toml(&with_override(
        "get",
        0,
        &format!("{raised}\nget_header_bytes = 40000"),
    ));
    assert_eq!(
        config.web.effective_get_header_bytes("proxy.example.com"),
        0
    );
    // The override must fit the request-head limit like the global value.
    let error = load_config_error_from_temp_toml(&with_override("get", 40000, ""));
    assert!(
        error.contains("web.vhosts[0].get_header_bytes"),
        "got: {error}"
    );

    // A non-zero override on a host whose effective method is not GET.
    let source = WEB_CONFIG.replace(
        "public_addr = \"203.0.113.10:443\"",
        "public_addr = \"203.0.113.10:443\"\nget_header_bytes = 4096",
    );
    let error =
        load_config_error_from_temp_toml(&format!("[general]\nconfig_strict = true\n{source}"));
    assert!(error.contains("carrier_method"), "got: {error}");

    // Hot reload keeps process limits: a reloaded override that no longer
    // fits the frozen request-head limit fails the reload guard.
    let mut config = load_config_from_temp_toml(&with_override("get", 4096, ""));
    assert!(config.web.get_carrier_fits_limits());
    config.web.vhosts[0].get_header_bytes = Some(40000);
    assert!(!config.web.get_carrier_fits_limits());
}
