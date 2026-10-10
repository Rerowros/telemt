use hyper::Request;
use hyper::header::HeaderName;

use super::capability::{canonical_credential, scan_capabilities};
use crate::config::WebRuntimeConfig;
use crate::web::manager::WebProcessRuntime;

/// Keyed credential checks (capability table plus token HMAC pair) one
/// request may spend before routing. A legitimate request carries a few
/// credentials, each one 43-character window; the cap bounds what junk
/// metadata can cost, independent of `max_header_bytes`.
const CREDENTIAL_CHECKS_PER_REQUEST: usize = 1024;

/// Request extension proving that metadata contains an authentic internal credential.
#[derive(Clone, Copy)]
struct InternalCredential;

/// Request extension naming headers left unscanned after the check budget ran out.
#[derive(Clone)]
struct UnscannedHeaders(Vec<HeaderName>);

/// Outcome of scanning one metadata field.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scan {
    Clean,
    Secret,
    Exhausted,
}

/// Marks requests whose metadata contains a capability or process token.
/// `uplink_route` skips the GET payload header lines: on the uplink route
/// they are stripped before any decoy forwarding, and scanning tens of
/// kilobytes of payload costs one HMAC pair per base64url position.
///
/// The scan spends at most `CREDENTIAL_CHECKS_PER_REQUEST` keyed checks.
/// A URI that exhausts the budget is fenced like a credential; headers
/// reached after exhaustion are recorded and removed before the decoy sees
/// the request, so unscanned metadata is never forwarded.
pub(super) fn mark_internal_credential<B>(
    request: &mut Request<B>,
    config: &WebRuntimeConfig,
    runtime: &WebProcessRuntime,
    uplink_route: bool,
) {
    let mut checks = CREDENTIAL_CHECKS_PER_REQUEST;
    let uri = request.uri();
    let mut uri_scan = uri.authority().map_or(Scan::Clean, |authority| {
        contains_secret(authority.as_str().as_bytes(), &mut checks, config, runtime)
    });
    if uri_scan == Scan::Clean {
        uri_scan = uri.path_and_query().map_or(Scan::Clean, |path| {
            contains_secret(path.as_str().as_bytes(), &mut checks, config, runtime)
        });
    }
    if uri_scan != Scan::Clean {
        request.extensions_mut().insert(InternalCredential);
        return;
    }
    let mut unscanned = Vec::new();
    let mut secret = false;
    for (name, value) in request
        .headers()
        .iter()
        .filter(|(name, _)| !(uplink_route && super::get::is_payload_header(name)))
    {
        if checks == 0 {
            unscanned.push(name.clone());
            continue;
        }
        let mut scan = contains_secret(name.as_str().as_bytes(), &mut checks, config, runtime);
        if scan == Scan::Clean {
            scan = contains_secret(value.as_bytes(), &mut checks, config, runtime);
        }
        match scan {
            Scan::Clean => {}
            Scan::Secret => {
                secret = true;
                break;
            }
            Scan::Exhausted => unscanned.push(name.clone()),
        }
    }
    if secret {
        request.extensions_mut().insert(InternalCredential);
    } else if !unscanned.is_empty() {
        request.extensions_mut().insert(UnscannedHeaders(unscanned));
    }
}

/// Returns whether request metadata was authenticated before routing.
pub(super) fn has_internal_credential<B>(request: &Request<B>) -> bool {
    request.extensions().get::<InternalCredential>().is_some()
}

/// Removes every header the bounded credential scan did not reach.
pub(super) fn remove_unscanned_headers<B>(request: &mut Request<B>) {
    if let Some(UnscannedHeaders(names)) = request.extensions_mut().remove::<UnscannedHeaders>() {
        for name in names {
            request.headers_mut().remove(name);
        }
    }
}

fn contains_secret(
    text: &[u8],
    checks: &mut usize,
    config: &WebRuntimeConfig,
    runtime: &WebProcessRuntime,
) -> Scan {
    let mut window = [0; 43];
    let mut run_len = 0usize;
    let mut offset = 0usize;
    while offset < text.len() {
        let (byte, consumed) = if text[offset] == b'%' && offset + 2 < text.len() {
            match (hex_value(text[offset + 1]), hex_value(text[offset + 2])) {
                (Some(high), Some(low)) => ((high << 4) | low, 3),
                _ => (text[offset], 1),
            }
        } else {
            (text[offset], 1)
        };
        offset += consumed;
        if base64url_byte(byte) {
            if run_len < window.len() {
                window[run_len] = byte;
                run_len += 1;
            } else {
                window.copy_within(1.., 0);
                window[42] = byte;
            }
            if run_len >= window.len()
                && let Some(candidate) = canonical_credential(&window)
            {
                let Some(left) = checks.checked_sub(1) else {
                    return Scan::Exhausted;
                };
                *checks = left;
                if bool::from(scan_capabilities(&config.capabilities, &candidate).matched)
                    || runtime.authentic_token(&candidate)
                {
                    return Scan::Secret;
                }
            }
        } else {
            run_len = 0;
        }
    }
    Scan::Clean
}

fn base64url_byte(value: u8) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_')
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[path = "secrets/tests.rs"]
mod tests;
