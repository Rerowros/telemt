use super::*;

/// Precomputed WEB configuration consumed by listener hot paths.
#[derive(Debug)]
pub(crate) struct WebRuntimeConfig {
    /// Canonical host lookup used by HTTP request routing.
    pub(crate) vhosts: BTreeMap<String, Arc<WebRuntimeVhost>>,
    /// Flat profile inventory used by startup link emission.
    pub(crate) profiles: Vec<Arc<WebRuntimeProfile>>,
    /// Complete active capability table used to contain misplaced credentials.
    pub(crate) capabilities: CapabilitySet,
}

/// Active capabilities in configuration order, plus a hashed index for the
/// pre-routing credential scan. A lookup hashes the candidate with the
/// per-process random SipHash key of `RandomState`, so its cost does not grow
/// with the table and, unlike an ordered search, its timing carries nothing
/// an outsider can steer toward a capability.
#[derive(Clone, Default)]
pub(crate) struct CapabilitySet {
    table: Box<[[u8; 32]]>,
    index: std::collections::HashSet<[u8; 32]>,
}

impl CapabilitySet {
    /// Returns whether `candidate` is an active capability.
    pub(crate) fn contains(&self, candidate: &[u8; 32]) -> bool {
        self.index.contains(candidate)
    }
}

impl std::ops::Deref for CapabilitySet {
    type Target = [[u8; 32]];

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl PartialEq for CapabilitySet {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table
    }
}

impl std::fmt::Debug for CapabilitySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CapabilitySet").field(&self.table).finish()
    }
}

impl FromIterator<[u8; 32]> for CapabilitySet {
    fn from_iter<I: IntoIterator<Item = [u8; 32]>>(iter: I) -> Self {
        let table: Box<[[u8; 32]]> = iter.into_iter().collect();
        let index = table.iter().copied().collect();
        Self { table, index }
    }
}

/// Precomputed immutable virtual-host data.
#[derive(Debug)]
pub(crate) struct WebRuntimeVhost {
    /// Canonical lowercase ACE hostname.
    pub(crate) host: String,
    /// Exact slash-delimited endpoint base, including the trailing slash.
    pub(crate) base: String,
    /// Optional HTTPS carrier method override for this hostname.
    pub(crate) carrier_method: Option<crate::config::WebCarrierMethod>,
    /// Optional GET uplink header payload override for this hostname.
    pub(crate) get_header_bytes: Option<usize>,
    /// Restart-frozen decoy capability-scan policy.
    pub(crate) decoy_fasttrack_mode: WebDecoyFastTrackMode,
    /// Immutable ordinary-site fallback snapshot.
    pub(crate) decoy: WebRuntimeDecoy,
    /// Upstream connect and response-head deadline.
    pub(crate) decoy_header_secs: u64,
    /// Exact capability profiles accepted by this host.
    pub(crate) profiles: Vec<Arc<WebRuntimeProfile>>,
    /// Contiguous capability table aligned one-to-one with `profiles`.
    pub(crate) capabilities: Box<[[u8; 32]]>,
}

/// Precomputed exact-user capability entry.
#[derive(Debug)]
pub(crate) struct WebRuntimeProfile {
    /// Canonical host that owns this profile.
    pub(crate) host: String,
    /// Stable public destination tuple supplied to relay routing.
    pub(crate) public_addr: SocketAddr,
    /// Exact access user authenticated by logical streams.
    pub(crate) user: String,
    /// Stable credential identity used by process-wide admission fencing.
    pub(crate) credential_id: [u8; 16],
    /// Client secret representation and inner protocol policy.
    pub(crate) secret_mode: WebSecretMode,
    /// Sole carrier or final fallback frozen into the issued bridge policy.
    pub(crate) carrier: WebCarrier,
    /// Whether an explicit carrier list enabled automatic negotiation.
    pub(crate) carrier_negotiation_enabled: bool,
    /// Whether automatic outcomes consult and update process-local evidence.
    pub(crate) carrier_learning: bool,
    /// Ordered negotiation candidates including the fallback carrier exactly once.
    pub(crate) carriers: Arc<[WebCarrier]>,
    /// Cumulative carrier-attempt deadlines frozen when the bridge is issued.
    pub(crate) carrier_negotiation_deadlines_secs: [u64; 4],
    /// HMAC-derived bridge capability.
    pub(crate) capability: [u8; 32],
    /// Non-secret domain-separated client-secret fingerprint for debugging.
    pub(crate) key_fingerprint: String,
    /// Per-profile live session ceiling.
    pub(crate) max_sessions: usize,
    /// Per-profile live logical-stream ceiling.
    pub(crate) max_streams: usize,
    /// Per-session live relay-task ceiling.
    pub(crate) max_streams_per_session: usize,
}

/// Runtime-ready ordinary-site fallback.
#[derive(Debug)]
pub(crate) enum WebRuntimeDecoy {
    HttpUpstream { addr: SocketAddr, authority: String },
    StaticDirectory(Arc<WebStaticSite>),
}

/// Immutable bounded static-site snapshot.
#[derive(Debug)]
pub(crate) struct WebStaticSite {
    /// Canonical URL-path to immutable response asset mapping.
    pub(crate) assets: BTreeMap<String, WebStaticAsset>,
    /// Configured root index file name.
    pub(crate) index: String,
}

/// One immutable static response body and metadata.
#[derive(Debug)]
pub(crate) struct WebStaticAsset {
    /// Immutable response body retained by the runtime snapshot.
    pub(crate) body: Bytes,
    /// Extension-derived static content type.
    pub(crate) content_type: &'static str,
    /// Strong SHA-256 entity tag.
    pub(crate) etag: String,
}
