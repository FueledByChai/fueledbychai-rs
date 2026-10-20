//! What the Paradex TESTNET samples share (`testnet_trade`, FBC-x69b; `testnet_quote`, FBC-elg7):
//! the testnet guard and the command-line pieces it rests on, and the wiring of one market's
//! order-entry session. Included by path by each sample and its test, beside [`link`](crate::link),
//! and not a module of this crate's library target, which stays empty with no dependency.
//!
//! - **The guard** ([`testnet_guard`]): the REST and WebSocket URLs must be exactly Paradex's
//!   testnet ones ([`TESTNET_REST`], [`TESTNET_WS`]) or both a loopback stub's at `/v1`, and the
//!   chain id (`PARADEX_CHAIN_ID` when set) the testnet's ([`TESTNET_CHAIN`]), checked before
//!   anything connects. A refusal never shows what was typed: it may be a pasted secret.
//! - **The market** ([`specs`]): one Paradex perpetual, its tick, size step and minimum size of
//!   one lot from the command line (no instrument discovery yet, FBC-l5o), read over REST
//!   ([`touch`], `GET /orderbook`).
//! - **The session** ([`config`], [`session_config`]): order entry on the WebSocket, the REST
//!   base for the login and the resync, the testnet chain id, reconnects paced, and the account's
//!   one nonce source.
//! - **What both keep**: the client-id high-water mark in the lease directory ([`HighWater`]),
//!   blocking work off the runtime's thread ([`off_thread`]), and the output on a thread of its
//!   own ([`detached`]).
//!
//! The lease directory's default ([`default_lease_dir`]) is the one `testnet_trade` has always
//! used, `$HOME/.fueledbychai/testnet_trade`, for both samples: their market, account and
//! namespace leases then exclude each other, and they keep one high-water mark per namespace.

use std::cell::RefCell;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use fbc_core::{
    AccountKey, AssetSym, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, Lots,
    Namespace, NonceBlock, NonceSource, PriceGrid, Secrets, SizeStep, SpecTable, TradingStatus,
    UnderlyingId, VenueConfig, VenueFactory, VenueId, WallNs, dispatch_market_data,
};
use fbc_journal::NonceSourceId;
use fbc_oms::{PositionCheck, ResyncReport, ResyncSnapshot};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::{
    Connector, ExecSession, ExecSessionConfig, IngestClock, ProxyConfig, RateLimiter,
    ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_paradex::auth::{CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, TIMEOUT};
use fbc_venue_paradex::factory::{EXEC_MODE, EXEC_URL, RPC_TIMEOUT};
use fbc_venue_paradex::md::rest::{OrderbookSnapshot, decode_orderbook, orderbook_path};
use rust_decimal::Decimal;

use crate::link::Note;

/// The testnet's REST base and order-entry WebSocket: the only non-loopback URLs the guard
/// admits, exactly.
pub const TESTNET_REST: &str = "https://api.testnet.paradex.trade/v1";
pub const TESTNET_WS: &str = "wss://ws.api.testnet.paradex.trade/v1";

/// The testnet's Starknet chain id, the only one the guard admits and the one the login signs
/// for: the `starknet_chain_id` that GET https://api.testnet.paradex.trade/v1/system/config
/// reports (2026-10-09). The Java library's `PRIVATE_SN_POTC_SEPOLIA` default is stale:
/// signing for it fails every login with HTTP 401 `STARKNET_SIGNATURE_VERIFICATION_FAILED`
/// (the owner's first runs). Reading it from the venue at start is FBC-g544.
pub const TESTNET_CHAIN: &str = "PRIVATE_SN_PARACLEAR_TESTNET";

/// [`TESTNET_CHAIN`]'s felt (its ASCII bytes as one big-endian number) in decimal, as the Java
/// library writes `PARADEX_CHAIN_ID`, and in hex, lower case without `0x`.
pub const TESTNET_CHAIN_DECIMAL: &str =
    "8458834024819506728615521019831122032732688838300959446835911345492";
pub const TESTNET_CHAIN_HEX: &str = "505249564154455f534e5f50415241434c4541525f544553544e4554";

/// Whether `chain` names the testnet's chain: [`TESTNET_CHAIN`] by name, or its felt in
/// decimal or in `0x` hex (either case). Nothing else, not even another spelling of the felt.
pub fn is_testnet_chain(chain: &str) -> bool {
    let hex = chain
        .strip_prefix("0x")
        .or_else(|| chain.strip_prefix("0X"))
        .map(str::to_ascii_lowercase);
    chain == TESTNET_CHAIN
        || chain == TESTNET_CHAIN_DECIMAL
        || hex.as_deref() == Some(TESTNET_CHAIN_HEX)
}

/// Ends a refusal of a value: what was typed is never shown, as it may be a pasted secret.
pub const UNSHOWN: &str = " (the value is not shown: it may be a secret)";

/// The environment variable of the chain id (optional).
pub const CHAIN_VAR: &str = "PARADEX_CHAIN_ID";

/// The longest argument [`flag_name`] names: every flag of this sample is shorter, and a pasted
/// key longer.
pub const FLAG_MAX: usize = 24;

/// `arg`'s part before any '=' when it is shaped like a flag: '-' or '--', a lower-case letter,
/// then lower-case letters, digits and '-', at most [`FLAG_MAX`] long. Anything else (a key
/// pasted after a '-', say) is not a flag and is never named.
pub fn flag_name(arg: &str) -> Option<&str> {
    let name = arg.split_once('=').map_or(arg, |(name, _)| name);
    let bare = name.strip_prefix("--").or_else(|| name.strip_prefix('-'))?;
    let shaped = name.len() <= FLAG_MAX
        && bare.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && bare
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    shaped.then_some(name)
}

/// `$HOME/.fueledbychai/testnet_trade` ([`lease_dir_under`]).
pub fn default_lease_dir() -> Result<PathBuf, String> {
    lease_dir_under(std::env::var_os("HOME"))
}

/// `<home>/.fueledbychai/testnet_trade` ([`lease_dir_named`]).
pub fn lease_dir_under(home: Option<std::ffi::OsString>) -> Result<PathBuf, String> {
    lease_dir_named(home, "testnet_trade")
}

/// `<home>/.fueledbychai/<name>`: durable, so the client-id high-water mark kept there
/// outlives a reboot or a temporary-directory cleanup. Refused when `home` is unset, empty or
/// relative: a relative one would put the leases under the directory the run starts in, so two
/// runs started in different directories would not exclude each other (Codex r4226246468 on
/// PR #114). Its value is never shown. `mainnet_smoke` keeps its own (`mainnet_smoke`), so a
/// mainnet run never shares a testnet sample's high-water mark.
pub fn lease_dir_named(home: Option<std::ffi::OsString>, name: &str) -> Result<PathBuf, String> {
    match home.map(PathBuf::from) {
        Some(home) if home.is_absolute() => Ok(home.join(".fueledbychai").join(name)),
        Some(home) if !home.as_os_str().is_empty() => Err(format!(
            "HOME is not an absolute path{UNSHOWN}: give --lease-dir, a directory kept across \
             reboots"
        )),
        _ => Err("HOME is not set: give --lease-dir, a directory kept across reboots".to_owned()),
    }
}

/// What the testnet guard admitted: whether the URLs are a loopback test stub.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Paradex's testnet hosts.
    Testnet,
    /// A loopback host: a test stub on this machine, not Paradex.
    LoopbackStub,
}

/// The testnet guard, run before anything connects: the REST base must be exactly
/// [`TESTNET_REST`] and the WebSocket exactly [`TESTNET_WS`], or both a loopback stub's (a test
/// stub on this machine, its path exactly `/v1`), and the chain id (`PARADEX_CHAIN_ID` when
/// set, else the testnet's) must be [`TESTNET_CHAIN`], by name or as its felt in decimal or hex.
/// Refused naming what is not testnet.
pub fn testnet_guard(rest: &str, ws: &str, chain: Option<&str>) -> Result<Target, String> {
    let target = testnet_urls(rest, ws)?;
    match chain {
        None => {}
        Some(chain) if is_testnet_chain(chain) => {}
        // The value is never shown: a private key pasted there by mistake would be printed.
        Some(_) => {
            return Err(format!(
                "{CHAIN_VAR}: not the testnet chain id {TESTNET_CHAIN} (the value is not shown: \
                 it may be a secret); the sample never signs for another chain (mainnet is \
                 PRIVATE_SN_PARACLEAR_MAINNET)"
            ));
        }
    }
    Ok(target)
}

/// Refuses a SOCKS5 proxy with loopback stub URLs: the proxy would connect to its own host's
/// loopback, not this machine's, and hand it the login and the signed orders.
pub fn direct_to_stub(target: Target, proxy: &ProxyConfig) -> Result<(), String> {
    match (target, proxy) {
        (Target::LoopbackStub, ProxyConfig::Socks5 { .. }) => Err(
            "--socks5 with loopback stub URLs: the proxy would reach its own host's loopback, \
             not this machine's; connect to a stub directly"
                .to_owned(),
        ),
        _ => Ok(()),
    }
}

/// The hosts of the REST and WebSocket URLs, all of them a run prints: the guard admits only
/// the testnet's exact URLs or a loopback stub's, so each is a testnet host or a loopback one.
pub fn hosts(rest: &str, ws: &str) -> Result<(String, String), String> {
    Ok((
        split(rest, ["https://", "http://"], "--rest-url")?.host,
        split(ws, ["wss://", "ws://"], "--ws-url")?.host,
    ))
}

/// The URL half of [`testnet_guard`]: both URLs exactly the testnet's ([`TESTNET_REST`] and
/// [`TESTNET_WS`]), or both a loopback stub's with the path exactly `/v1`. Whatever sits in a
/// URL's path is sent to the venue (the REST requests' paths, the WebSocket upgrade), so no
/// other path is taken. A refusal names the flag and what is expected, never what was typed.
pub fn testnet_urls(rest: &str, ws: &str) -> Result<Target, String> {
    Ok(
        match exact_or_stub(rest, ws, "testnet", TESTNET_REST, TESTNET_WS)? {
            true => Target::LoopbackStub,
            false => Target::Testnet,
        },
    )
}

/// The URL guard of one Paradex network `net` (`testnet`, `mainnet`): both URLs exactly its
/// REST base `want_rest` and WebSocket `want_ws`, or both a loopback stub's with the path
/// exactly `/v1` (true). Whatever sits in a URL's path is sent to the venue (the REST requests'
/// paths, the WebSocket upgrade), so no other path is taken. A refusal names the flag and what
/// is expected, never what was typed.
pub fn exact_or_stub(
    rest: &str,
    ws: &str,
    net: &str,
    want_rest: &str,
    want_ws: &str,
) -> Result<bool, String> {
    let rest_url = split(rest, ["https://", "http://"], "--rest-url")?;
    let ws_url = split(ws, ["wss://", "ws://"], "--ws-url")?;
    match (rest_url.loopback(), ws_url.loopback()) {
        (true, true) => {
            rest_url.stub("--rest-url")?;
            ws_url.stub("--ws-url")?;
            Ok(true)
        }
        (false, false) => {
            if rest != want_rest {
                return Err(format!(
                    "--rest-url: not Paradex's {net} REST base, exactly {want_rest}, or a \
                     loopback stub's; the sample is {net} only{SHOWN_NOT}"
                ));
            }
            if ws != want_ws {
                return Err(format!(
                    "--ws-url: not Paradex's {net} WebSocket, exactly {want_ws}, or a \
                     loopback stub's; the sample is {net} only{SHOWN_NOT}"
                ));
            }
            Ok(false)
        }
        _ => Err(format!(
            "--rest-url and --ws-url: one is a loopback stub and the other is not; give both \
             {net} URLs or both a stub's"
        )),
    }
}

/// Ends a refusal of a URL: what was typed is never shown.
pub const SHOWN_NOT: &str = " (the URL is not shown: it may carry a secret)";

/// A URL [`split`] took apart.
struct Url<'a> {
    /// The lowercased host, a host name's shape ([`proxy_host`]).
    host: String,
    /// The port, when one was given.
    port: Option<&'a str>,
    /// Everything after the host and port: empty, or from its '/'.
    path: &'a str,
}

impl Url<'_> {
    /// Whether the host is a loopback address: a test stub on this machine. An address only,
    /// in 127.0.0.0/8 or `[::1]`: a name (`localhost`) is resolved when the connector connects,
    /// and a resolver may map it to another machine (Codex r4217903067).
    fn loopback(&self) -> bool {
        self.host == "[::1]"
            || self
                .host
                .parse::<std::net::Ipv4Addr>()
                .is_ok_and(|ip| ip.is_loopback())
    }

    /// Refuses a loopback stub's URL unless its path is exactly `/v1` and its port, if any, a
    /// port number.
    fn stub(&self, flag: &str) -> Result<(), String> {
        if self
            .port
            .is_some_and(|p| !p.parse::<u16>().is_ok_and(|p| p > 0))
        {
            return Err(format!(
                "{flag}: a loopback stub's port is not a port{SHOWN_NOT}"
            ));
        }
        if self.path != "/v1" {
            return Err(format!(
                "{flag}: a loopback stub's path must be exactly /v1{SHOWN_NOT}"
            ));
        }
        Ok(())
    }
}

/// `url` taken apart: it must start with one of `schemes`, name no user, query or fragment, and
/// its host must be shaped like a host name ([`proxy_host`]). A refusal names the flag, never
/// the URL nor its host: what was typed may carry a secret (a password, a token, a pasted key).
fn split<'a>(url: &'a str, schemes: [&str; 2], flag: &str) -> Result<Url<'a>, String> {
    let bad = |why: &str| format!("{flag}: {why}{SHOWN_NOT}");
    let rest = schemes
        .iter()
        .find_map(|s| url.strip_prefix(s))
        .ok_or_else(|| bad(&format!("not a {} URL", schemes.join(" or "))))?;
    if rest.contains(['?', '#']) {
        return Err(bad("a URL with a query or a fragment is refused"));
    }
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    if authority.contains('@') {
        return Err(bad("a URL with a user is refused"));
    }
    let (host, port) = match authority.strip_prefix('[') {
        Some(v6) => {
            let close = v6.find(']').ok_or_else(|| bad("no closing ]"))?;
            (&authority[..close + 2], &v6[close + 1..])
        }
        None => authority.split_at(authority.find(':').unwrap_or(authority.len())),
    };
    let port = match port.strip_prefix(':') {
        Some(port) => Some(port),
        None if port.is_empty() => None,
        None => return Err(bad("not a host and port")),
    };
    if !proxy_host(host) {
        return Err(bad("not a host name"));
    }
    Ok(Url {
        host: host.to_ascii_lowercase(),
        port,
        path,
    })
}

/// The longest part of a market name between its '-'s: Paradex's are short (`BTC`, `USD`,
/// `kBONK`), and a longer one is more likely a pasted secret than a market.
const MARKET_PART_MAX: usize = 12;
/// The longest whole market name taken (Codex r4226495325 on PR #114).
const MARKET_MAX: usize = 32;

/// A Paradex perpetual as Paradex spells it: `BASE-QUOTE-PERP`, the base and quote ASCII
/// letters and digits, each at most [`MARKET_PART_MAX`] long, at most [`MARKET_MAX`] in all
/// (`BTC-USD-PERP`, `kBONK-USD-PERP`: the letters as md_watch's Paradex spelling takes them,
/// either case, since some markets carry a lower-case prefix). The spec is built as a
/// perpetual's ([`crate::trade::specs`]), and Paradex's discovery takes perpetuals only, so an
/// option, a future or a spot market is refused (Codex r4226570445 on PR #114). Anything else is
/// refused before it is put in a request path, where a pasted private key would be sent to the
/// venue; the refusal never shows the value.
pub fn symbol(value: &str) -> Result<String, String> {
    let parts: Vec<&str> = value.split('-').collect();
    let shaped = parts.len() == 3
        && parts[2] == "PERP"
        && value.len() <= MARKET_MAX
        && parts.iter().all(|p| {
            (1..=MARKET_PART_MAX).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_alphanumeric())
        });
    if shaped {
        Ok(value.to_owned())
    } else {
        Err(format!(
            "--market: not a Paradex perpetual as Paradex spells it (BASE-QUOTE-PERP in letters \
             and digits, e.g. BTC-USD-PERP or kBONK-USD-PERP; options, futures and spot are not \
             modelled here){UNSHOWN}"
        ))
    }
}

/// A whole number from `lo` to `hi`.
pub fn bounded(flag: &str, value: &str, lo: u64, hi: u64) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(n) if (lo..=hi).contains(&n) => Ok(n),
        _ => Err(format!(
            "{flag}: not a whole number from {lo} to {hi}{UNSHOWN}"
        )),
    }
}

/// `host:port`, the port from 1 to 65535.
pub fn socks5(value: &str) -> Result<ProxyConfig, String> {
    // Never echoed: a proxy typed with a user and password would print them.
    let bad =
        || "--socks5: not host:port (the value is not shown: it may carry a secret)".to_owned();
    let (host, port) = value.rsplit_once(':').ok_or_else(bad)?;
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(bad)?;
    if !proxy_host(host) {
        return Err(bad());
    }
    // A bracketed IPv6 address without its brackets: the connector resolves (host, port),
    // which takes `::1` but not `[::1]` (Codex r4217903060).
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    Ok(ProxyConfig::Socks5 {
        host: host.to_owned(),
        port,
    })
}

/// Whether `host` is an IPv4 address, a bracketed IPv6 one, or a DNS name (labels of 1 to 63
/// ASCII letters, digits and '-', none starting or ending with '-', 253 characters at most).
/// A pasted key or token (a label longer than DNS allows, or other characters) is refused
/// before it reaches a resolver.
fn proxy_host(host: &str) -> bool {
    if let Some(v6) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return v6.parse::<std::net::Ipv6Addr>().is_ok();
    }
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    host.len() <= 253
        && host.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// `--lease-dir`'s value, which must be an absolute path: a relative one is refused, so a
/// value pasted there by mistake (a key) never becomes a directory in the working directory
/// nor appears in a later error. The refusal never shows the value.
pub fn absolute(value: &str) -> Result<PathBuf, String> {
    let dir = PathBuf::from(value);
    if dir.is_absolute() {
        Ok(dir)
    } else {
        Err(format!("--lease-dir: not an absolute path{UNSHOWN}"))
    }
}

/// A positive decimal.
pub fn positive(flag: &str, value: &str) -> Result<Decimal, String> {
    match value.parse::<Decimal>() {
        Ok(d) if d > Decimal::ZERO => Ok(d),
        _ => Err(format!("{flag}: not a positive decimal{UNSHOWN}")),
    }
}

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// How long after an order was sent a resync's watermark must be before the order's absence from
/// it counts toward Lost (the ladder's `settle`): Paradex's snapshot is trustworthy (decision
/// 0094), so an absence can end an order, and FBC-8xr's runs saw `GET /orders` lag the stream by
/// up to 3480 ms. Well above that, so a lagging snapshot never ends an order still resting.
pub const SNAPSHOT_SETTLE: std::time::Duration = std::time::Duration::from_secs(10);

/// The one market's instrument id and the account's number.
pub const INST: InstrumentId = InstrumentId::new(1);
pub const ACCT: AccountKey = AccountKey::new(1);

/// The venue's id as the lease files name it, and the account's name in them: a label, never
/// the account's address.
pub const VENUE: &str = "PARADEX";
pub const ACCOUNT_LABEL: &str = "testnet";

/// How long an order request awaits its reply before it is `Unknown`.
pub const RPC_WAIT: &str = "5000ms";

/// How many logins refused in a row stop the run: the venue answered, and a key, an address or
/// an account it refuses is not mended by reconnecting.
pub const LOGIN_REFUSALS: usize = 3;

/// The usual causes of a refused login, printed with the refusal (and in `--help`).
pub const REFUSAL_CAUSES: &str = "the usual causes: a mainnet key on testnet (Paradex derives the \
     account's L2 key per network), an Ethereum address instead of the Paradex (Starknet) one in \
     PARADEX_ACCOUNT_ADDRESS, or an account not onboarded on the testnet, or unfunded";

/// A writer that never blocks its caller: each write is handed to a thread of its own that
/// writes it to the sink, in order, and flushes. Dropping it (every clone of the [`Out`] holding
/// it) ends the thread once it has written everything; join the handle to wait for that.
pub struct Detached(mpsc::Sender<Vec<u8>>);

/// A [`Detached`] writer over `sink`, and its thread.
pub fn detached<W: Write + Send + 'static>(mut sink: W) -> (Detached, std::thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let thread = std::thread::spawn(move || {
        for chunk in rx {
            // A sink that fails loses the line; the run goes on to its Stop regardless.
            let _ = sink.write_all(&chunk).and_then(|()| sink.flush());
        }
    });
    (Detached(tx), thread)
}

impl Write for Detached {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // The channel is unbounded, so this never waits; it fails only once the thread ended.
        self.0
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Lines with the milliseconds since the start.
#[derive(Clone)]
pub struct Lines {
    pub out: Out,
    pub t0: Instant,
}

impl Lines {
    pub fn line(&self, text: std::fmt::Arguments<'_>) {
        // A line that cannot be written is lost; the run goes on to its Stop regardless.
        let _ = writeln!(self.out.borrow_mut(), "{text}");
    }

    pub fn step(&self, step: &str, text: std::fmt::Arguments<'_>) {
        let ms = self.t0.elapsed().as_millis();
        self.line(format_args!("STEP {step:<10} +{ms}ms  {text}"));
    }
}

/// Runs the session beside `driver` until the driver is done (it drops the session's control,
/// so the session then stops); the session's end first is an error.
pub async fn drive_with<H: fbc_runtime::ExecHandler, T>(
    session: &mut ExecSession<H>,
    driver: impl Future<Output = T>,
) -> Result<T, String> {
    let running = session.run();
    tokio::pin!(running);
    tokio::pin!(driver);
    tokio::select! {
        ended = &mut running => Err(match ended {
            Ok(()) => "stopped".to_owned(),
            Err(e) => e.to_string(),
        }),
        ok = &mut driver => {
            // The control is dropped: the session closes its socket and returns.
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut running).await;
            Ok(ok)
        }
    }
}

/// The wall clock now.
pub fn wall_now() -> WallNs {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    WallNs(i64::try_from(ns).unwrap_or(i64::MAX))
}

/// The Paradex configuration: order entry on the WebSocket, the REST base for the login and
/// the resync, the testnet chain id, and the timings.
pub fn config(rest_url: &str, ws_url: &str) -> VenueConfig {
    config_on(rest_url, ws_url, TESTNET_CHAIN)
}

/// As [`config`], the login signing for `chain` (`mainnet_smoke`: the mainnet's).
pub fn config_on(rest_url: &str, ws_url: &str, chain: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (EXEC_URL, ws_url),
        (EXEC_MODE, "orders"),
        (RPC_TIMEOUT, RPC_WAIT),
        (REST_URL, rest_url),
        (CHAIN_ID, chain),
        (SIGNATURE_LIFETIME, "600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// The one market's spec, from the command line: its tick, size step and minimum size of one
/// lot. The minimum notional is checked by [`price`], not here.
pub fn specs(
    venue: &dyn VenueFactory,
    cfg: &VenueConfig,
    market: &str,
    tick: Decimal,
    step: Decimal,
) -> Result<SpecTable, String> {
    let caps = venue.caps(cfg).map_err(|e| e.to_string())?;
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(market))
        // Neither the value nor the venue's error (which may quote it) is shown.
        .map_err(|_| "--market: not a symbol the Paradex adapter takes (the value is not shown)")?;
    let price_grid = PriceGrid::fixed(tick).map_err(|e| format!("--tick: {e:?}"))?;
    let size_step = SizeStep::new(step).ok_or("--step: not a valid size step")?;
    let usd = AssetSym::new("USD").expect("an asset symbol");
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        id: INST,
        venue: VenueId::new(1),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(INST.get()),
        kind: InstrumentKind::Perpetual,
        price_grid,
        quote_grid: None,
        size_step,
        min_size: Lots::new(1).expect("one lot"),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    });
    Ok(table)
}

/// The market's REST order book (`GET /orderbook`), read before the order is priced and again
/// just before it is placed.
pub async fn touch(
    rest_url: &str,
    market: &str,
    proxy: &ProxyConfig,
    wait: Duration,
    specs: &SpecTable,
) -> Result<OrderbookSnapshot, String> {
    let body = rest_get(
        rest_url,
        &orderbook_path(market),
        "GET /orderbook",
        proxy,
        wait,
    )
    .await?;
    decode_orderbook(&body, specs).map_err(|e| format!("GET /orderbook: {e:?}"))
}

/// The body of a successful unauthenticated `GET` of `path` (from `/v1`, with any query) on the
/// REST base `rest_url`, within `wait`: refused naming `what` (`GET /orderbook`) on no answer in
/// time, a transport error, or a status other than 2xx.
pub async fn rest_get(
    rest_url: &str,
    path: &str,
    what: &str,
    proxy: &ProxyConfig,
    wait: Duration,
) -> Result<Bytes, String> {
    let origin = rest_url
        .trim_end_matches('/')
        .strip_suffix("/v1")
        .ok_or("--rest-url: not a base ending in /v1")?;
    let url = format!("{origin}{path}");
    let request = Request::builder()
        .method(Method::GET)
        .uri(&url)
        .body(Bytes::new())
        .map_err(|e| format!("{what}: {e}"))?;
    let connector = Connector::new(proxy.clone());
    let response = tokio::time::timeout(wait, connector.http(request, 1 << 20))
        .await
        .map_err(|_| format!("{what}: no answer in time"))?
        .map_err(|e| format!("{what}: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("{what}: status {}", response.status()));
    }
    Ok(response.into_body())
}

/// The path (from `/v1`) of `market`'s `GET /markets/summary`, whose `mark_price` is where
/// Paradex judges an order's minimum notional ([`decode_mark`]).
pub fn mark_path(market: &str) -> String {
    format!("/v1/markets/summary?market={market}")
}

/// The mark price of `market` in a `GET /markets/summary?market=` answer
/// (`{"results":[{"symbol", "mark_price", ..}]}`): a positive decimal, or refused.
pub fn decode_mark(body: &[u8], market: &str) -> Result<Decimal, String> {
    let summary: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "GET /markets/summary: not a JSON answer".to_owned())?;
    let mark = summary["results"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["symbol"].as_str() == Some(market))
        .ok_or("GET /markets/summary: no summary of the market")?["mark_price"]
        .as_str()
        .and_then(|m| m.parse::<Decimal>().ok())
        .ok_or("GET /markets/summary: the market's summary has no decimal mark_price")?;
    if mark <= Decimal::ZERO {
        return Err("GET /markets/summary: the market's mark price is not positive".to_owned());
    }
    Ok(mark)
}

/// The connection key an HTTP request is charged under: Paradex declares no per-connection
/// limit, and an HTTP request is on no connection.
const NO_CONN: fbc_core::ConnKey = fbc_core::ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// Charges `reads` unauthenticated REST reads (`GET /orderbook`, `GET /markets/summary`,
/// `GET /system/config`) to `limiter`, the run's order-entry session's, as normal traffic: each
/// as `Rest` (Paradex's per-IP request buckets, as discovery's public `GET /markets` charges)
/// with the connection it opens (`Connect`, the per-IP connection buckets), all together or
/// none. Waits until the buckets have room, but never past `by`: refused naming `what` when
/// they would have none by then. So these reads and the session's traffic share one count, and
/// a read never spends the session's safety reserve (Codex r4289363370 on PR #149).
pub async fn admit_reads(
    limiter: &RateLimiter,
    reads: usize,
    by: tokio::time::Instant,
    what: &str,
) -> Result<(), String> {
    let ops: Vec<fbc_core::OpKind> = (0..reads)
        .flat_map(|_| [fbc_core::OpKind::Rest, fbc_core::OpKind::Connect])
        .collect();
    admit_ops(limiter, &ops, by, what).await
}

/// Charges `ops` (one request each, normal traffic, over HTTP) to `limiter` together, all or
/// none, waiting until the buckets have room but never past `by`: refused naming `what` when
/// they would have none by then ([`admit_reads`]; the position check's connections, Codex
/// r4306449950 on PR #158).
pub async fn admit_ops(
    limiter: &RateLimiter,
    ops: &[fbc_core::OpKind],
    by: tokio::time::Instant,
    what: &str,
) -> Result<(), String> {
    let requests: Vec<fbc_runtime::Request> = ops
        .iter()
        .map(|op| fbc_runtime::Request {
            charge: fbc_core::RateCharge::one(*op, None),
            via: fbc_core::Via::Http,
            class: fbc_core::TrafficClass::Normal,
        })
        .collect();
    loop {
        match limiter.charge(tokio::time::Instant::now(), NO_CONN, &requests) {
            Ok(_) => return Ok(()),
            Err(refused) => match refused.ready_at {
                Some(at) if at <= by => tokio::time::sleep_until(at).await,
                _ => {
                    return Err(format!(
                        "{what}: not sent, the venue's rate limit has no room for it before the \
                         step timeout passes"
                    ));
                }
            },
        }
    }
}

/// The market's touch (`GET /orderbook`, [`touch`]) and mark price (`GET /markets/summary`,
/// [`decode_mark`]), read together within `within`, both charged to `limiter` first
/// ([`admit_reads`]).
pub async fn touch_and_mark(
    rest_url: &str,
    market: &str,
    proxy: &ProxyConfig,
    within: Duration,
    specs: &SpecTable,
    limiter: &RateLimiter,
) -> Result<(OrderbookSnapshot, Decimal), String> {
    let by = tokio::time::Instant::now() + within;
    admit_reads(limiter, 2, by, "GET /orderbook and GET /markets/summary").await?;
    let left = by.saturating_duration_since(tokio::time::Instant::now());
    let book = touch(rest_url, market, proxy, left, specs);
    let mark = async {
        let path = mark_path(market);
        let body = rest_get(rest_url, &path, "GET /markets/summary", proxy, left).await?;
        decode_mark(&body, market)
    };
    tokio::try_join!(book, mark)
}

/// A price, or `none` for a side with nothing resting.
pub fn px_or_none(px: Option<Decimal>) -> String {
    px.map_or_else(|| "none".to_owned(), |px| px.to_string())
}

/// The price the inventory cap is converted at: the highest of the order's price `px` and the
/// touch's sides that rest, so a position is never worth more than the cap at the market. A
/// buy's own price is below the bid, so on a book with no asks the bid is the market's price
/// (Codex r4226246459 on PR #114).
pub fn inventory_price(px: Decimal, bid: Option<Decimal>, ask: Option<Decimal>) -> Decimal {
    [bid, ask].into_iter().flatten().fold(px, Decimal::max)
}

/// A cap of `usd` in lots of `step` at `price`, floored. The caps are the operator's own, with
/// no ceiling (FBC-thks, decision 0113), so a cap past what a lot count holds (or a decimal
/// holds, once divided) is the largest lot count, never zero and never a panic (Codex
/// r4320205456 on PR #184); fbc-oms's checked arithmetic then bounds every order as at any
/// large cap. A lot whose own cost does not fit a decimal costs more than any cap, so it gives
/// no lot (Codex r4320317907), and a re-check at such a price never admits an order; so does a
/// price, a step or a cap that is not positive.
pub fn cap_lots(usd: Decimal, price: Decimal, step: Decimal) -> Lots {
    use rust_decimal::prelude::ToPrimitive;
    let per_lot = match price.checked_mul(step) {
        Some(per) if per > Decimal::ZERO => per,
        _ => return Lots::ZERO,
    };
    if usd <= Decimal::ZERO {
        return Lots::ZERO;
    }
    let lots = usd
        .checked_div(per_lot)
        .and_then(|n| n.floor().to_i64())
        .unwrap_or(i64::MAX);
    Lots::new(lots).unwrap_or(Lots::ZERO)
}

/// Nonces counted up from 1: Paradex asks for none, but the session reserves one per item and
/// per arm as the contract says.
pub struct Counting(pub u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).expect("a block of nonces");
        self.0 += u64::from(len);
        block
    }
}

/// The order-entry session's configuration: reconnects paced from 250 ms to 5 s, at most 5
/// attempts a minute, each given 10 s; normal traffic stopping at 90% of each declared limit; a
/// write a peer stopped reading ends its connection after 10 s; 16 commands waiting for the
/// connection and 16 requests in flight for places, amends and queries, each with 16 more for
/// cancels and reducing orders (decision 0105).
pub fn session_config(
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    creds: Secrets,
    specs: SpecTable,
    proxy: &ProxyConfig,
    namespace: u16,
) -> Result<ExecSessionConfig, String> {
    let caps = venue.caps(&cfg).map_err(|e| e.to_string())?;
    let limiter = RateLimiter::new(
        &caps.limits,
        SafetyReserve::percent(10).expect("a valid reserve"),
    )
    .map_err(|e| format!("{e:?}"))?;
    Ok(ExecSessionConfig {
        venue,
        cfg,
        creds,
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: Namespace::new(namespace),
        specs,
        connector: Connector::new(proxy.clone()),
        pacing: ReconnectPacing::new(
            Duration::from_millis(250),
            Duration::from_secs(5),
            5,
            Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .expect("valid pacing"),
        clock: IngestClock::new(),
        nonces: Box::new(Counting(1)),
        // One source for the account: Paradex's scope is `NonceScope::None`, so the one
        // `Counting` this run reserves from is the account's, journaled under its number (0078).
        nonce_source: NonceSourceId(u32::from(ACCT.get())),
        conn: 0,
        limiter,
        write_stall: WriteStall::new(Duration::from_secs(10)).expect("a non-zero window"),
        http_max_body: 1 << 20,
        outbound: fbc_runtime::ReservedBound::new(16, 16).expect("a valid bound"),
        in_flight: fbc_runtime::InFlight::new(
            fbc_runtime::ReservedBound::new(16, 16).expect("a valid bound"),
        ),
    })
}

/// An applied resync: its report and the snapshot it applied.
pub type Applied<'a> = (&'a ResyncReport, &'a ResyncSnapshot);

/// The latest resync, applied or refused (why): once the session takes places, the one whose
/// end opened the current epoch's gate, never an earlier epoch's. A refused one means the gate
/// is open with no resync applied behind it.
pub fn latest_resync(notes: &[Note]) -> Option<Result<Applied<'_>, &str>> {
    notes.iter().rev().find_map(|n| match n {
        Note::Resynced { report, snapshot } => Some(Ok((report, snapshot))),
        Note::ResyncRefused(why) => Some(Err(why.as_str())),
        _ => None,
    })
}

/// As [`off_thread`], holding `held` until `work` ends, even past the timeout: a lease `work`
/// relies on stays held while it runs. Work that ends in time hands `held` back with its
/// result, for the caller to keep; past the timeout it is dropped on the work's thread once
/// the work ends.
pub async fn off_thread_holding<H: Send + 'static, T: Send + 'static>(
    timeout: Duration,
    held: H,
    work: impl FnOnce() -> T + Send + 'static,
) -> Option<(T, H)> {
    off_thread(timeout, move || (work(), held)).await
}

/// Runs `work` on a thread of its own and waits up to `timeout` for its result: `None` when it
/// did not finish in time (or panicked). The thread is detached, never a runtime's blocking
/// pool, so work stalled past the timeout (a `sync_all` on a stalled disk) never holds up the
/// runtime's end, and so the sample's exit.
pub async fn off_thread<T: Send + 'static>(
    timeout: Duration,
    work: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        // The receiver is gone when the wait timed out: the result is then dropped.
        let _ = tx.send(work());
    });
    tokio::time::timeout(timeout, rx).await.ok()?.ok()
}

/// The highest sequence of our namespace's client ids among the open orders `snapshot` shows, 0
/// when it shows none.
pub fn snapshot_max(snapshot: &ResyncSnapshot) -> u64 {
    snapshot
        .orders
        .iter()
        .filter_map(|o| match o.cid {
            Some(fbc_core::CidMatch::Ours(cid)) => Some(cid.seq()),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// What the resyncs after the first (a reconnect's) found wrong with the positions the
/// registry holds: a market whose venue position differs from its inventory (`Desync`), or
/// could not be compared (`Stale`, `Unsettled`), or a fill the resync could not place. A later
/// resync never overwrites the inventory, so the run cannot go on from it.
pub fn resync_disagreements(notes: &[Note]) -> Vec<String> {
    notes
        .iter()
        .filter_map(|n| match n {
            Note::Resynced { report, .. } => Some(report),
            _ => None,
        })
        .flat_map(|report| {
            let checks = report
                .checks
                .iter()
                .filter(|c| !matches!(c, PositionCheck::Agrees { .. }))
                .map(|c| format!("{c:?}"));
            let unsettled = report
                .unsettled
                .iter()
                .map(|(inst, _)| format!("Unsettled fill on instrument {}", inst.get()));
            checks.chain(unsettled).collect::<Vec<_>>()
        })
        .collect()
}

/// The client-id mint's high-water mark, kept in a file of the lease directory across runs, so
/// a run whose wall clock stepped back never mints an id an earlier run used (the namespace
/// lease keeps two minters apart, not two runs).
#[derive(Clone)]
pub struct HighWater {
    path: std::path::PathBuf,
}

impl HighWater {
    /// The mark of namespace `ns` kept in `dir`.
    pub fn at(dir: &std::path::Path, ns: Namespace) -> HighWater {
        let name = format!("cid-high-water-account-{}-ns-{}", ACCT.get(), ns.get());
        HighWater {
            path: dir.join(name),
        }
    }

    /// The mark kept, 0 when none was.
    pub fn read(&self) -> Result<u64, String> {
        match fs::read_to_string(&self.path) {
            Ok(text) => text
                .trim()
                .parse()
                .map_err(|_| format!("{}: not a high-water mark", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(format!("{}: {e}", self.path.display())),
        }
    }

    /// Keeps `mark`: written whole to a file beside it and synced, renamed over the mark, and
    /// the directory synced, so the mark is on the disk when this returns.
    pub fn write(&self, mark: u64) -> Result<(), String> {
        use std::io::Write as _;
        let tmp = self.path.with_extension("tmp");
        #[cfg(test)]
        held_writes::wait(&self.path);
        let durable = || -> std::io::Result<()> {
            // The new mark reaches the disk before it replaces the old, and the rename does
            // before the order goes out: a power loss leaves the old mark or the new, never none.
            let mut file = fs::File::create(&tmp)?;
            file.write_all(mark.to_string().as_bytes())?;
            file.sync_all()?;
            fs::rename(&tmp, &self.path)?;
            let dir = self.path.parent().unwrap_or(std::path::Path::new("."));
            fs::File::open(dir)?.sync_all()
        };
        durable().map_err(|e| {
            format!(
                "{}: cannot keep the client-id high-water mark: {e}",
                self.path.display()
            )
        })
    }
}

/// Test-only: holds the next high-water mark written in a lease directory until the test
/// releases it, so a test can end a connection while the write is in flight, an event and not
/// a time (Codex r4244206466 on PR #149). Keyed by the directory, so tests running at once each
/// hold only their own.
#[cfg(test)]
pub mod held_writes {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};

    #[derive(Default)]
    struct Gate {
        released: Mutex<bool>,
        open: Condvar,
        entered: tokio::sync::Notify,
    }

    fn held() -> &'static Mutex<HashMap<PathBuf, Arc<Gate>>> {
        static HELD: OnceLock<Mutex<HashMap<PathBuf, Arc<Gate>>>> = OnceLock::new();
        HELD.get_or_init(Mutex::default)
    }

    /// The hold of the next write in a directory.
    pub struct Hold(Arc<Gate>);

    /// Holds the next mark written in `dir` (only that one) until [`Hold::release`].
    pub fn hold(dir: &Path) -> Hold {
        let gate = Arc::new(Gate::default());
        held()
            .lock()
            .unwrap()
            .insert(dir.to_path_buf(), Arc::clone(&gate));
        Hold(gate)
    }

    impl Hold {
        /// Completes once the held write has started, and waits there.
        pub async fn entered(&self) {
            self.0.entered.notified().await;
        }

        /// Lets the held write go on.
        pub fn release(&self) {
            *self.0.released.lock().unwrap() = true;
            self.0.open.notify_all();
        }
    }

    /// Called by [`super::HighWater::write`] on its thread: waits while the write to `path` is
    /// held.
    pub(super) fn wait(path: &Path) {
        let Some(gate) = path
            .parent()
            .and_then(|dir| held().lock().unwrap().remove(dir))
        else {
            return;
        };
        gate.entered.notify_one();
        let mut released = gate.released.lock().unwrap();
        while !*released {
            released = gate.open.wait(released).unwrap();
        }
    }
}
