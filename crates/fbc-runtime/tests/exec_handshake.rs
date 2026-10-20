//! FBC-j8a4's done line (decision 0116): an order-entry connection whose planned endpoint names
//! a credential header ([`HandshakeHeader`]) carries it on the opening handshake of its first
//! connection and of every reconnect, on each planned endpoint, directly and through a SOCKS5
//! proxy, its value taken from the session's [`Secrets`] as it is (Hibachi's raw
//! `Authorization: <key>`, no `Bearer`), while a market-data socket's handshake carries none. The
//! synthetic key, and the synthetic account id the URL's query carries, are in none of the
//! journal's bytes, the log lines or the errors of those handshakes, a first attempt refused
//! with a 401 that echoes the key included. A credential the plan names but the consumer did
//! not hand over, or a value no header can carry, refuses the session by the key's name; a
//! header the runtime sets itself, or one named twice, refuses it as a venue defect.
//!
//! Every server is local (tests/common); nothing reaches the internet. The key and the account
//! id are synthetic, assembled at run time.

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::collections::BTreeSet;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::tls::TestCa;
use common::toy::{self, ToyVenue};
use common::{Answer, ScriptedWs, Socks5Stub, Upgrade};
use exec_toy::{EXEC_STREAM, OWN_NS, ToyExec, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint, FieldSpec,
    HandshakeHeader, HttpPlan, InstrumentSpecDraft, MdCodec, MdTransport, NonceBlock, NonceSource,
    Secret, Secrets, SpecTable, StreamId, Subscription, SymbolError, VenueCaps, VenueConfig,
    VenueError, VenueFactory, WireUrl,
};
use fbc_journal::{JournalWriter, RedactionKey, SinkConfig, journal_queue};
use fbc_runtime::{
    Connector, ExecSession, ExecSessionConfig, ExecSessionError, IngestClock, Journal, Liveness,
    MdSession, MdSessionConfig, ProxyConfig, RateLimiter, ReconnectPacing, SafetyReserve,
    WriteStall,
};
use log::{LevelFilter, Log, Metadata, Record};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------------------------
// Synthetic values.
// ---------------------------------------------------------------------------------------------

/// The `Secrets` key the planned endpoints name for their header.
const KEY: &str = "authed.key";

/// A synthetic API key, assembled at run time: no funded account uses it.
fn synthetic_key() -> String {
    format!("SYNTH{}-handshake-{}", "apikey", "j8a4-0123456789")
}

/// A synthetic account id, as Hibachi's URL carries it: private account data (0009).
fn synthetic_account() -> String {
    format!("{}{}", "7777", "424242")
}

// ---------------------------------------------------------------------------------------------
// The venue: the conformance toy's order entry on two endpoints, each naming the header.
// ---------------------------------------------------------------------------------------------

const TRADE: &str = "authed.trade";
const ACCOUNT_URL: &str = "authed.account";
/// Which header names the endpoints use: `reserved` names one the runtime sets itself, `twice`
/// names the header twice on one endpoint, otherwise `Authorization` once.
const NAMES: &str = "authed.names";
/// The account stream, as Hibachi's `/ws/account`.
const ACCOUNT: StreamId = StreamId(2);

/// Whether the venue's `exec_codec` was handed the key the runtime also sends: the runtime
/// reads the value, it does not take it from the codec.
#[derive(Default)]
struct Authed {
    codec_had_key: AtomicBool,
}

impl Authed {
    fn leak() -> &'static Authed {
        Box::leak(Box::default())
    }
}

fn header(name: &'static str) -> HandshakeHeader {
    HandshakeHeader { name, secret: KEY }
}

impl VenueFactory for Authed {
    fn id(&self) -> &'static str {
        "TOY-AUTHED"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(exec_toy::caps())
    }

    fn parse_fbc_common_symbol(&self, _: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NotCommonForm)
    }

    fn discover(&self, _: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    fn plan_md(
        &self,
        _: &VenueConfig,
        _: &SpecTable,
        _: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        Ok(Vec::new())
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        ToyVenue::leak().md_codec(cfg, ep)
    }

    /// The order connection, then the account stream, each with the account id in its query,
    /// marked as a span of its URL, and the header named.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = |key| {
            let base = cfg.get(key).unwrap();
            let text = format!("{base}?accountId={}&hibachiClient=toy", synthetic_account());
            let at = base.len() + "?accountId=".len();
            let span = at as u32..(at + synthetic_account().len()) as u32;
            WireUrl::redacted(text, vec![span]).unwrap()
        };
        let handshake = match cfg.get(NAMES) {
            Some("reserved") => vec![header("Sec-WebSocket-Key")],
            Some("twice") => vec![header("Authorization"), header("authorization")],
            _ => vec![header("Authorization")],
        };
        Ok(vec![
            ExecEndpoint {
                stream: EXEC_STREAM,
                url: url(TRADE),
                handshake: handshake.clone(),
            },
            ExecEndpoint {
                stream: ACCOUNT,
                url: url(ACCOUNT_URL),
                handshake,
            },
        ])
    }

    fn exec_codec(
        &self,
        _: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let had = creds
            .get(KEY)
            .is_some_and(|v| v.expose() == synthetic_key());
        self.codec_had_key.store(had, Ordering::SeqCst);
        Some(Ok(Box::new(ToyExec::new(Box::new(ToySigner)))))
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const CONN: u16 = 4;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A quick floor, so a drop reconnects at once in real time.
fn quick() -> ReconnectPacing {
    ReconnectPacing::new(ms(10), ms(10), 100, Duration::from_secs(60), ms(5_000)).unwrap()
}

/// Credentials holding `value` under [`KEY`], or none.
fn creds(value: Option<String>) -> Secrets {
    let mut creds = Secrets::new();
    if let Some(value) = value {
        creds.insert(KEY, Secret::new(value));
    }
    creds
}

/// A session of `venue` with its order connection at `trade` and its account stream at
/// `account`, through `connector`, with `creds` and the extra configuration `extra`.
fn setup(
    venue: &'static Authed,
    trade: &str,
    account: &str,
    connector: Connector,
    creds: Secrets,
    extra: &[(&'static str, &str)],
) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(TRADE, trade);
    cfg.insert(ACCOUNT_URL, account);
    for (key, value) in extra {
        cfg.insert(key, value);
    }
    let limits = exec_toy::caps().limits;
    ExecSessionConfig {
        venue,
        cfg,
        creds,
        acct: fbc_core::AccountKey::new(1),
        rpc_ids: fbc_runtime::RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector,
        pacing: quick(),
        clock: IngestClock::new(),
        nonces: Box::new(Counting(0)),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    }
}

/// Nonces counted up from 0.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

fn direct() -> Connector {
    Connector::new(ProxyConfig::Direct)
}

/// What the handler hears is not this ticket's: dropped.
fn ignore(_: fbc_core::Envelope<fbc_core::ExecEvent>) {}

/// Asserts `upgrade` carries the synthetic key as its one `Authorization` line, as it is, and
/// the account id in its query.
fn assert_authed(upgrade: &Upgrade, what: &str) {
    assert_eq!(
        upgrade.values("authorization"),
        [synthetic_key().as_str()],
        "{what}: {upgrade:?}"
    );
    let query = format!("accountId={}&hibachiClient=toy", synthetic_account());
    assert!(upgrade.target.ends_with(&query), "{what}: {upgrade:?}");
}

// ---------------------------------------------------------------------------------------------
// The done line: the header on every upgrade of a planned endpoint.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn each_planned_endpoint_sends_the_header_on_its_first_connection_and_on_a_reconnect() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Authed::leak();
    let config = setup(
        venue,
        &trade.url(),
        &account.url(),
        direct(),
        creds(Some(synthetic_key())),
        &[],
    );
    let (mut session, control) = ExecSession::new(config, ignore).unwrap();
    let script = async move {
        let (mut t, mut a) = (trade.accept().await, account.accept().await);
        let firsts = (t.upgrade().await, a.upgrade().await);
        // Each endpoint reconnects alone, and its new connection carries the header again.
        t.drop_conn();
        let mut t2 = trade.accept().await;
        let t_again = t2.upgrade().await;
        a.drop_conn();
        let mut a2 = account.accept().await;
        let a_again = a2.upgrade().await;
        drop(control);
        (firsts, t_again, a_again)
    };
    let (run, ((t_first, a_first), t_again, a_again)) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_authed(&t_first, "the order connection's first upgrade");
    assert_authed(&a_first, "the account stream's first upgrade");
    assert_authed(&t_again, "the order connection's reconnect");
    assert_authed(&a_again, "the account stream's reconnect");
    // The codec was still handed the key: the runtime reads it, it does not take it.
    assert!(venue.codec_had_key.load(Ordering::SeqCst));
}

#[tokio::test]
async fn the_header_goes_through_a_socks5_proxy_on_the_first_connection_and_a_reconnect() {
    // Over TLS, by name: a credential header crosses the network only inside wss://.
    let ca = TestCa::new();
    let (mut trade, mut account) = (
        ScriptedWs::start_tls(ca.server(&["trade.invalid"])).await,
        ScriptedWs::start_tls(ca.server(&["account.invalid"])).await,
    );
    let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let names = [("trade.invalid", local), ("account.invalid", local)];
    let proxy = Socks5Stub::start(&names, Answer::Relay).await;
    let mut socks = Connector::new(ProxyConfig::Socks5 {
        host: proxy.addr.ip().to_string(),
        port: proxy.addr.port(),
    });
    socks.add_trust_anchor(&ca.der()).unwrap();
    let config = setup(
        Authed::leak(),
        &trade.wss_url("trade.invalid"),
        &account.wss_url("account.invalid"),
        socks,
        creds(Some(synthetic_key())),
        &[],
    );
    let (mut session, control) = ExecSession::new(config, ignore).unwrap();
    let script = async move {
        let (mut t, mut a) = (trade.accept().await, account.accept().await);
        let firsts = (t.upgrade().await, a.upgrade().await);
        t.drop_conn();
        let mut t2 = trade.accept().await;
        let again = t2.upgrade().await;
        drop(control);
        (firsts, again)
    };
    let (run, ((t_first, a_first), again)) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_authed(&t_first, "the order connection through the proxy");
    assert_authed(&a_first, "the account stream through the proxy");
    assert_authed(&again, "the order connection's reconnect through the proxy");
    // Every connection went through the proxy, by name.
    let hosts: Vec<String> = proxy.targets().into_iter().map(|t| t.host).collect();
    assert_eq!(hosts.iter().filter(|h| *h == "trade.invalid").count(), 2);
    assert!(hosts.iter().all(|h| h.ends_with(".invalid")), "{hosts:?}");
}

#[tokio::test]
async fn a_market_data_socket_sends_no_credential_header() {
    let mut server = ScriptedWs::start().await;
    let venue = ToyVenue::leak();
    let config = MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.url()),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        connector: direct(),
        pacing: quick(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let first = peer.upgrade().await;
        peer.drop_conn();
        let again = server.accept().await.upgrade().await;
        drop(control);
        (first, again)
    };
    let (run, (first, again)) = tokio::join!(session.run(), script);
    run.unwrap();
    for upgrade in [first, again] {
        let mut names: Vec<&str> = upgrade.headers.iter().map(|(n, _)| n.as_str()).collect();
        names.sort_unstable();
        // Only the handshake's own headers: no credential, whatever the session's venue.
        let own = [
            "connection",
            "host",
            "sec-websocket-key",
            "sec-websocket-version",
            "upgrade",
        ];
        assert_eq!(names, own, "{upgrade:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// The done line: the key in no journal byte, log line or error.
// ---------------------------------------------------------------------------------------------

/// Every log record any test in this binary emitted, at every level the build keeps.
static LOGGED: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        let line = format!("{} {} {}", record.level(), record.target(), record.args());
        LOGGED.lock().unwrap_or_else(|e| e.into_inner()).push(line);
    }

    fn flush(&self) {}
}

/// Installs [`Capture`] once for this binary, at the most verbose level the facade allows.
fn capture_logs() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        log::set_logger(&Capture).expect("the only logger in this test binary");
        log::set_max_level(LevelFilter::Trace);
    });
}

fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(
        RedactionKey::new(format!("SYNTH-{}-journal-redaction-key-0123456789", "j8a4").as_bytes())
            .unwrap(),
    )
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Every byte under `dir`, compressed segments decompressed.
fn written_bytes(dir: &Path) -> Vec<u8> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            out.extend(written_bytes(&path));
        } else if path.extension().is_some_and(|e| e == "zst") {
            out.extend(zstd::decode_all(fs::File::open(&path).unwrap()).unwrap());
        } else {
            out.extend(fs::read(&path).unwrap());
        }
    }
    out
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

/// A server at a fixed address whose first connection's upgrade is refused with a 401 whose
/// body and a header echo the request's `Authorization` value, as a venue's error page may;
/// every later connection is relayed to `to`. Reports each refused request's head.
async fn refuse_first_then_relay(to: std::net::SocketAddr) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let url = format!("ws://{}/md", listener.local_addr().unwrap());
    let refused = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&refused);
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            first.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        let echoed = head
            .lines()
            .find_map(|l| l.strip_prefix("Authorization: "))
            .unwrap_or("")
            .to_owned();
        log.lock().unwrap().push(head);
        let body = format!("refused key {echoed}");
        let reply = format!(
            "HTTP/1.1 401 Unauthorized\r\nX-Echo: {echoed}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        first.write_all(reply.as_bytes()).await.unwrap();
        drop(first);
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect(to).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    (url, refused)
}

#[tokio::test]
async fn the_key_and_the_account_id_are_in_no_journal_byte_log_line_or_error() {
    capture_logs();
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let trade_addr: std::net::SocketAddr = trade.url()["ws://".len()..]
        .trim_end_matches("/md")
        .parse()
        .unwrap();
    let (trade_url, refused) = refuse_first_then_relay(trade_addr).await;
    let config = setup(
        Authed::leak(),
        &trade_url,
        &account.url(),
        direct(),
        creds(Some(synthetic_key())),
        &[],
    );

    let root = fresh_dir("exec_handshake_journal");
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 2, redaction_key()).unwrap())
        .unwrap();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(queue));

    let (mut session, control) = ExecSession::new(config, ignore).unwrap();
    session.set_journal(Journal::new(queue));
    let script = async move {
        // The first attempt is refused; the session reconnects, and the second is upgraded.
        let (mut t, mut a) = (trade.accept().await, account.accept().await);
        let firsts = (t.upgrade().await, a.upgrade().await);
        a.drop_conn();
        let again = account.accept().await.upgrade().await;
        drop(control);
        (firsts, again)
    };
    let (run, ((t_first, a_first), again)) = tokio::join!(session.run(), script);
    // A run's own error, had there been one, would be the session's error to show.
    if let Err(err) = &run {
        let shown = format!("{err} {err:?}");
        assert!(!shown.contains(&synthetic_key()), "{shown}");
    }
    run.unwrap();
    assert_authed(&t_first, "the order connection after its refused attempt");
    assert_authed(&a_first, "the account stream's first upgrade");
    assert_authed(&again, "the account stream's reconnect");
    // The refused attempt carried the header too, and was refused.
    let refused = refused.lock().unwrap().clone();
    assert_eq!(refused.len(), 1);
    assert!(refused[0].contains(&format!("Authorization: {}\r\n", synthetic_key())));
    assert!(session.counters().failed_attempts >= 1);
    drop(session);
    writer.close().unwrap();

    let journal = written_bytes(&root);
    fs::remove_dir_all(&root).unwrap();
    assert!(!journal.is_empty());
    assert!(
        !contains(&journal, &synthetic_key()),
        "the key is in the journal"
    );
    assert!(
        !contains(&journal, &synthetic_account()),
        "the account id is in the journal"
    );
    let logged = LOGGED.lock().unwrap_or_else(|e| e.into_inner()).clone();
    // The logger is live: tungstenite's handshakes are heard at DEBUG.
    assert!(!logged.is_empty());
    for line in &logged {
        assert!(!line.contains(&synthetic_key()), "logged: {line}");
        assert!(!line.contains(&synthetic_account()), "logged: {line}");
    }
}

#[tokio::test]
async fn a_credential_the_plan_names_but_the_session_lacks_refuses_it_by_the_key_s_name() {
    let config = setup(
        Authed::leak(),
        "ws://127.0.0.1:9/a",
        "ws://127.0.0.1:9/b",
        direct(),
        creds(None),
        &[],
    );
    let err = ExecSession::new(config, ignore).err().unwrap();
    assert_eq!(
        err,
        ExecSessionError::Venue(VenueError::Config(ConfigError::Missing(KEY)))
    );
}

#[tokio::test]
async fn a_value_no_header_can_carry_refuses_the_session_without_showing_it() {
    capture_logs();
    let bad = format!("{}\r\nX-Smuggled: 1", synthetic_key());
    let config = setup(
        Authed::leak(),
        "ws://127.0.0.1:9/a",
        "ws://127.0.0.1:9/b",
        direct(),
        creds(Some(bad)),
        &[],
    );
    let err = ExecSession::new(config, ignore).err().unwrap();
    assert!(
        matches!(
            err,
            ExecSessionError::Venue(VenueError::Config(ConfigError::Invalid { key: KEY, .. }))
        ),
        "{err:?}"
    );
    let shown = format!("{err} {err:?}");
    assert!(!shown.contains(&synthetic_key()), "{shown}");
}

#[tokio::test]
async fn a_header_the_runtime_sets_itself_or_one_named_twice_is_refused_as_a_venue_defect() {
    for (names, header) in [
        ("reserved", "Sec-WebSocket-Key"),
        ("twice", "authorization"),
    ] {
        let config = setup(
            Authed::leak(),
            "ws://127.0.0.1:9/a",
            "ws://127.0.0.1:9/b",
            direct(),
            creds(Some(synthetic_key())),
            &[(NAMES, names)],
        );
        let err = ExecSession::new(config, ignore).err().unwrap();
        assert_eq!(
            err,
            ExecSessionError::HandshakeHeader {
                stream: EXEC_STREAM,
                name: header,
            },
            "{names}"
        );
        assert!(!format!("{err} {err:?}").contains(&synthetic_key()));
    }
}

#[tokio::test]
async fn a_credential_header_over_plain_ws_to_a_host_not_loopback_is_refused_before_anything_opens()
{
    let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let proxy = Socks5Stub::start(&[("trade.invalid", local)], Answer::Relay).await;
    let socks = || {
        Connector::new(ProxyConfig::Socks5 {
            host: proxy.addr.ip().to_string(),
            port: proxy.addr.port(),
        })
    };
    // Either endpoint, directly or through the proxy: the key would cross the wire in the clear.
    for (trade, account, connector, stream) in [
        (
            "ws://trade.invalid:9/a",
            "ws://127.0.0.1:9/b",
            direct(),
            EXEC_STREAM,
        ),
        ("ws://127.0.0.1:9/a", "ws://10.0.0.1:9/b", direct(), ACCOUNT),
        (
            "ws://trade.invalid:9/a",
            "wss://account.invalid:9/b",
            socks(),
            EXEC_STREAM,
        ),
    ] {
        let config = setup(
            Authed::leak(),
            trade,
            account,
            connector,
            creds(Some(synthetic_key())),
            &[],
        );
        let err = ExecSession::new(config, ignore).err().unwrap();
        assert_eq!(
            err,
            ExecSessionError::CleartextHandshake { stream },
            "{trade} {account}"
        );
        let shown = format!("{err} {err:?}");
        assert!(!shown.contains(&synthetic_key()), "{shown}");
        assert!(
            !shown.contains(".invalid") && !shown.contains(&synthetic_account()),
            "{shown}"
        );
    }
    assert_eq!(proxy.connections(), 0, "nothing was opened");
}
