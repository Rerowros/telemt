use base64::Engine as _;
use bytes::Bytes;
use hyper::body::Body;
use hyper::header::{self, HeaderName, HeaderValue};
use hyper::{Method, Request};
use sha2::{Digest, Sha256};

use super::body::RequestBody;
use super::capability::canonical_credential;
use crate::config::WebRuntimeVhost;
use crate::web::manager::GetTokenScope;
use crate::web::manager::WebProcessRuntime;

/// Maximum decoded session-creation payload shared with the canonical handler.
const CREATE_BODY_LIMIT: usize = 64;
/// Maximum decoded diagnostic payload shared with the canonical handler.
const DIAGNOSTIC_BODY_LIMIT: usize = 64;

// Query keys mirror the canonical carrier headers one-to-one; `n`, `op`, `d`,
// `p`, and `pn` have no header counterparts and stay query-only.
const KEY_TOKEN: &str = "t";
const KEY_NONCE: &str = "n";
const KEY_UP_SEQ: &str = "s";
const KEY_DOWN_CURSOR: &str = "c";
const KEY_LANE: &str = "l";
const KEY_CAPABILITIES: &str = "k";
const KEY_ATTEMPT: &str = "a";
const KEY_FAILURE: &str = "f";
const KEY_UP_WINDOW: &str = "w";
const KEY_UP_CONFIRMED: &str = "u";
const KEY_DATA: &str = "d";
const KEY_PART: &str = "p";
const KEY_PARTS: &str = "pn";
const KEY_OPERATION: &str = "op";

const HEADER_UP_SEQ: HeaderName = HeaderName::from_static("x-up-seq");
const HEADER_DOWN_CURSOR: HeaderName = HeaderName::from_static("x-down-cursor");
const HEADER_LANE: HeaderName = HeaderName::from_static("x-lane-id");
const HEADER_CAPABILITIES: HeaderName = HeaderName::from_static("x-carrier-capabilities");
const HEADER_ATTEMPT: HeaderName = HeaderName::from_static("x-carrier-attempt");
const HEADER_FAILURE: HeaderName = HeaderName::from_static("x-carrier-failure");
const HEADER_UP_WINDOW: HeaderName = HeaderName::from_static("x-telemt-up-window");
const HEADER_UP_CONFIRMED: HeaderName = HeaderName::from_static("x-telemt-up-confirmed");
const HEADER_UP_PART: HeaderName = HeaderName::from_static("x-up-part");

/// Response header reporting the stored GET fragment index.
pub(super) fn up_part_header_name() -> HeaderName {
    HEADER_UP_PART
}

/// Decoded GET body injected into the canonical request body path.
#[derive(Clone)]
pub(super) enum GetRequestBody {
    /// Decoded query payload still requiring a fresh body-byte lease.
    Raw(Bytes),
}

/// Fragment descriptor for one GET uplink physical request.
#[derive(Clone)]
pub(super) struct GetUpParts {
    /// Zero-based index of this fragment.
    pub(super) part: u32,
    /// Total fragment count for the logical operation.
    pub(super) total: u32,
    /// Decoded payload bytes carried by this fragment.
    pub(super) data: Bytes,
}

/// Validated logical-operation coordinates shared by every fragment of one
/// GET uplink operation.
pub(super) struct GetUpOperation {
    /// Optional lane binding accepted only for lane carriers.
    pub(super) lane_id: Option<u32>,
    /// Logical uplink sequence claimed by this operation.
    pub(super) sequence: u64,
    /// Optional conveyor-confirmed floor bound across the operation.
    pub(super) confirmed: Option<u64>,
}

/// Headers accepted only as exact duplicates of a query key.
const CARRIER_QUERY_HEADERS: [HeaderName; 9] = [
    header::AUTHORIZATION,
    HEADER_UP_SEQ,
    HEADER_DOWN_CURSOR,
    HEADER_LANE,
    HEADER_CAPABILITIES,
    HEADER_ATTEMPT,
    HEADER_FAILURE,
    HEADER_UP_WINDOW,
    HEADER_UP_CONFIRMED,
];

/// Decodes one query value emitted by URLSearchParams canonical encoding.
fn decode_value(raw: &[u8]) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        match raw[index] {
            b'%' => {
                let (Some(high), Some(low)) = (
                    raw.get(index + 1).copied().and_then(hex_upper),
                    raw.get(index + 2).copied().and_then(hex_upper),
                ) else {
                    return None;
                };
                let byte = (high << 4) | low;
                if is_unreserved(byte) {
                    return None;
                }
                decoded.push(byte);
                index += 3;
            }
            byte if is_unreserved(byte) => {
                decoded.push(byte);
                index += 1;
            }
            _ => return None,
        }
    }
    Some(decoded)
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_')
}

fn hex_upper(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Parses one canonical unsigned decimal equal to `canonical_u64_header` rules.
fn parse_decimal(value: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(value).ok()?;
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let parsed = text.parse::<u64>().ok()?;
    (parsed.to_string() == text).then_some(parsed)
}

fn parse_u32(value: &[u8]) -> Option<u32> {
    u32::try_from(parse_decimal(value)?).ok()
}

/// Decodes canonical base64url data without padding or aliases.
fn decode_data(value: &[u8], limit: usize) -> Option<Bytes> {
    if value.is_empty() || value.len() > 4 * limit.div_ceil(3) {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .ok()?;
    if bytes.is_empty()
        || bytes.len() > limit
        || base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(&bytes)
            .as_bytes()
            != value
    {
        return None;
    }
    Some(Bytes::from(bytes))
}

/// Rewrites one strict GET carrier request into its canonical method form.
/// Returns `Err` for any deviation so the caller keeps decoy semantics.
pub(super) fn normalize(
    request: &mut Request<RequestBody>,
    runtime: &WebProcessRuntime,
    vhost: &WebRuntimeVhost,
) -> Result<(), ()> {
    let generation = runtime.active_generation();
    let web = &generation.config().web;
    let Some(path_and_query) = request.uri().path_and_query() else {
        return Err(());
    };
    // The configured budget covers the full https URL, not just the target.
    let url_bytes = "https://"
        .len()
        .saturating_add(vhost.host.len())
        .saturating_add(path_and_query.as_str().len());
    if url_bytes > web.limits.get_url_bytes {
        return Err(());
    }
    let Some(query) = request.uri().query() else {
        return Err(());
    };
    if request.headers().contains_key(header::TRANSFER_ENCODING) {
        return Err(());
    }
    {
        let mut lengths = request.headers().get_all(header::CONTENT_LENGTH).iter();
        match lengths.next() {
            None => {}
            Some(value) if lengths.next().is_none() && value.as_bytes() == b"0" => {}
            _ => return Err(()),
        }
    }
    if !request.body().is_end_stream() {
        return Err(());
    }
    let suffix = request.uri().path().strip_prefix(&vhost.base).ok_or(())?;
    let route = match suffix {
        "api/v1/session" => Route::Session,
        "api/v1/up" => Route::Up,
        "api/v1/down" => Route::Down,
        "api/v1/diagnostic" => Route::Diagnostic,
        _ => return Err(()),
    };
    let mut pairs: Vec<(&str, Vec<u8>)> = Vec::new();
    for segment in query.split('&') {
        let (key, value) = segment.split_once('=').ok_or(())?;
        if key.is_empty()
            || !key.bytes().all(|byte| byte.is_ascii_lowercase())
            || pairs.iter().any(|(existing, _)| *existing == key)
        {
            return Err(());
        }
        pairs.push((key, decode_value(value.as_bytes()).ok_or(())?));
    }
    let value_of = |key: &str| -> Option<&[u8]> {
        pairs
            .iter()
            .find(|(existing, _)| *existing == key)
            .map(|(_, value)| value.as_slice())
    };
    let has = |key: &str| value_of(key).is_some();
    let operation = match route {
        Route::Session if has(KEY_OPERATION) => {
            if value_of(KEY_OPERATION) != Some(b"close") {
                return Err(());
            }
            Operation::Close
        }
        Route::Session => Operation::Create,
        Route::Up => Operation::Up,
        Route::Down => Operation::Down,
        Route::Diagnostic => Operation::Diagnostic,
    };
    const CREATE_KEYS: &[&str] = &[
        KEY_TOKEN,
        KEY_NONCE,
        KEY_DATA,
        KEY_CAPABILITIES,
        KEY_ATTEMPT,
        KEY_FAILURE,
        KEY_UP_WINDOW,
    ];
    const CLOSE_KEYS: &[&str] = &[KEY_OPERATION, KEY_TOKEN, KEY_NONCE, KEY_FAILURE];
    const UP_KEYS: &[&str] = &[
        KEY_TOKEN,
        KEY_NONCE,
        KEY_UP_SEQ,
        KEY_LANE,
        KEY_UP_CONFIRMED,
        KEY_DATA,
        KEY_PART,
        KEY_PARTS,
    ];
    const DOWN_KEYS: &[&str] = &[KEY_TOKEN, KEY_NONCE, KEY_DOWN_CURSOR, KEY_LANE];
    const DIAGNOSTIC_KEYS: &[&str] = &[KEY_TOKEN, KEY_NONCE, KEY_DATA];
    let (allowed, required): (&[&str], &[&str]) = match operation {
        Operation::Create => (CREATE_KEYS, &[KEY_TOKEN, KEY_NONCE, KEY_DATA][..]),
        Operation::Close => (CLOSE_KEYS, &[KEY_OPERATION, KEY_TOKEN, KEY_NONCE][..]),
        Operation::Up => (UP_KEYS, &[KEY_TOKEN, KEY_NONCE, KEY_UP_SEQ, KEY_DATA][..]),
        Operation::Down => (DOWN_KEYS, &[KEY_TOKEN, KEY_NONCE, KEY_DOWN_CURSOR][..]),
        Operation::Diagnostic => (DIAGNOSTIC_KEYS, &[KEY_TOKEN, KEY_NONCE, KEY_DATA][..]),
    };
    if pairs.iter().any(|(key, _)| !allowed.contains(key)) || required.iter().any(|key| !has(key)) {
        return Err(());
    }
    if parse_decimal(value_of(KEY_NONCE).unwrap_or_default()).is_none_or(|nonce| nonce == 0) {
        return Err(());
    }
    // Authenticate the host-bound token against the store matching this
    // operation before any memory reservation; unknown tokens stay decoy.
    let scope = match operation {
        Operation::Create | Operation::Diagnostic => GetTokenScope::Bootstrap,
        Operation::Up | Operation::Down => GetTokenScope::Session,
        Operation::Close => GetTokenScope::Close,
    };
    let token = value_of(KEY_TOKEN).unwrap_or_default();
    let Some(raw_token) = canonical_credential(token) else {
        return Err(());
    };
    if !runtime.get_carrier_allowed(Sha256::digest(raw_token).into(), &vhost.host, scope) {
        return Err(());
    }
    let mut injected: Vec<(HeaderName, String)> = Vec::new();
    let text_field = |key: &str| -> Result<String, ()> {
        let value = value_of(key).unwrap_or_default();
        let text = std::str::from_utf8(value).map_err(|_| ())?;
        Ok(text.to_string())
    };
    let decimal_field = |key: &str| -> Result<String, ()> {
        let value = value_of(key).unwrap_or_default();
        parse_decimal(value)
            .map(|_| std::str::from_utf8(value).unwrap_or_default().to_string())
            .ok_or(())
    };
    match operation {
        Operation::Up => {
            injected.push((HEADER_UP_SEQ, decimal_field(KEY_UP_SEQ)?));
            if has(KEY_LANE) {
                injected.push((HEADER_LANE, decimal_field(KEY_LANE)?));
            }
            if has(KEY_UP_CONFIRMED) {
                injected.push((HEADER_UP_CONFIRMED, decimal_field(KEY_UP_CONFIRMED)?));
            }
        }
        Operation::Down => {
            injected.push((HEADER_DOWN_CURSOR, decimal_field(KEY_DOWN_CURSOR)?));
            if has(KEY_LANE) {
                injected.push((HEADER_LANE, decimal_field(KEY_LANE)?));
            }
        }
        Operation::Create => {
            if has(KEY_CAPABILITIES) {
                injected.push((HEADER_CAPABILITIES, text_field(KEY_CAPABILITIES)?));
            }
            if has(KEY_ATTEMPT) {
                injected.push((HEADER_ATTEMPT, decimal_field(KEY_ATTEMPT)?));
            }
            if has(KEY_FAILURE) {
                injected.push((HEADER_FAILURE, text_field(KEY_FAILURE)?));
            }
            if has(KEY_UP_WINDOW) {
                injected.push((HEADER_UP_WINDOW, decimal_field(KEY_UP_WINDOW)?));
            }
        }
        Operation::Close => {
            if has(KEY_FAILURE) {
                injected.push((HEADER_FAILURE, text_field(KEY_FAILURE)?));
            }
        }
        Operation::Diagnostic => {}
    }
    let data_limit = match operation {
        Operation::Create => CREATE_BODY_LIMIT,
        Operation::Diagnostic => DIAGNOSTIC_BODY_LIMIT,
        Operation::Up => web.limits.max_body_bytes,
        _ => 0,
    };
    let mut parts_extension = None;
    let mut body_extension = None;
    match operation {
        Operation::Up => {
            let part = match (has(KEY_PART), has(KEY_PARTS)) {
                (true, true) => parse_u32(value_of(KEY_PART).unwrap_or_default()).ok_or(())?,
                (false, false) => 0,
                _ => return Err(()),
            };
            let total = match has(KEY_PARTS) {
                true => parse_u32(value_of(KEY_PARTS).unwrap_or_default()).ok_or(())?,
                false => 1,
            };
            if total == 0 || total > crate::config::GET_MAX_PARTS || part >= total {
                return Err(());
            }
            let data = decode_data(value_of(KEY_DATA).unwrap_or_default(), data_limit).ok_or(())?;
            parts_extension = Some(GetUpParts { part, total, data });
        }
        Operation::Create | Operation::Diagnostic => {
            let data = decode_data(value_of(KEY_DATA).unwrap_or_default(), data_limit).ok_or(())?;
            body_extension = Some(GetRequestBody::Raw(data));
        }
        _ => {}
    }
    let content_type = match operation {
        Operation::Create | Operation::Up => Some("application/octet-stream"),
        Operation::Diagnostic => Some("application/json"),
        _ => None,
    };
    {
        let mut types = request.headers().get_all(header::CONTENT_TYPE).iter();
        match (types.next(), content_type) {
            (Some(value), Some(expected)) => {
                if types.next().is_some()
                    || !value
                        .to_str()
                        .is_ok_and(|value| value.eq_ignore_ascii_case(expected))
                {
                    return Err(());
                }
            }
            // A stripped Content-Type is tolerated: the canonical value is
            // injected below, but an unexpected one stays rejected.
            (Some(_), None) => return Err(()),
            (None, _) => {}
        }
    }
    let authorization_header = format!("Bearer {}", std::str::from_utf8(token).map_err(|_| ())?);
    // Mirrored headers are accepted only as exact single-value duplicates.
    for name in CARRIER_QUERY_HEADERS {
        let expected = if name == header::AUTHORIZATION {
            Some(authorization_header.as_str())
        } else {
            injected
                .iter()
                .find(|(injected_name, _)| *injected_name == name)
                .map(|(_, value)| value.as_str())
        };
        let mut values = request.headers().get_all(&name).iter();
        match (values.next(), expected) {
            (Some(value), Some(expected)) => {
                if values.next().is_some() || value.as_bytes() != expected.as_bytes() {
                    return Err(());
                }
            }
            (Some(_), None) => return Err(()),
            (None, _) => {}
        }
    }
    injected.push((header::AUTHORIZATION, authorization_header));
    if let Some(content_type) = content_type
        && !request.headers().contains_key(header::CONTENT_TYPE)
    {
        injected.push((header::CONTENT_TYPE, content_type.to_string()));
    }
    for (name, value) in injected {
        if request.headers().contains_key(&name) {
            continue;
        }
        let Ok(value) = HeaderValue::from_str(&value) else {
            return Err(());
        };
        request.headers_mut().insert(name, value);
    }
    if let Some(parts) = parts_extension {
        request.extensions_mut().insert(parts);
    }
    if let Some(body) = body_extension {
        request.extensions_mut().insert(body);
    }
    *request.method_mut() = match operation {
        Operation::Close => Method::DELETE,
        _ => Method::POST,
    };
    let Ok(uri) = request.uri().path().parse() else {
        return Err(());
    };
    *request.uri_mut() = uri;
    Ok(())
}

enum Route {
    Session,
    Up,
    Down,
    Diagnostic,
}

enum Operation {
    Create,
    Close,
    Up,
    Down,
    Diagnostic,
}
