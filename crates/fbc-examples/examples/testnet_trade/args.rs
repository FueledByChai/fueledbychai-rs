//! testnet_trade's command line, parsed by hand (no argument-parsing dependency), and the
//! testnet guard: the REST and WebSocket URLs and the chain id must name Paradex's testnet (or,
//! for the URLs, a loopback test stub), checked before anything connects.

use std::path::PathBuf;

use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

/// The help text `--help` prints.
pub const USAGE: &str = "\
testnet_trade: places ONE post-only limit order of the size you give, well away from the touch
on a Paradex TESTNET market through fbc-oms, waits for its acknowledgement, cancels it, waits
for the cancel's acknowledgement, then Stops (kill switch + cancel all) and exits. TESTNET ONLY: a
mainnet URL or chain id is refused before anything connects. An owner-assisted testnet run:
the market's position is seeded by hand from the venue's own REST position (decision 0067).

Usage: cargo run -p fbc-examples --example testnet_trade -- [OPTIONS]

Environment (read once at start; no value is ever printed):
  PARADEX_ACCOUNT_ADDRESS   the testnet account's Starknet address (0x and hex digits)
  PARADEX_PRIVATE_KEY       the account's own Stark private key (0x and hex digits): the main
                            key, not a trading subkey
  PARADEX_CHAIN_ID          optional; when set it must be the testnet's,
                            PRIVATE_SN_POTC_SEPOLIA, by name or as its felt in decimal (as the
                            Java library writes it) or 0x hex

Required confirmation and namespace:
  --sole-trader             you confirm that no other process, Java or Rust, trades this
                            account (the lease files guard only against another testnet_trade
                            using the same --lease-dir); refused without it
  --namespace <N>           the client-id namespace your configuration allocates this sample
                            on the account, 1 to 65535: its orders are minted, recognised as
                            ours, leased and kept under it (no default; an id of another
                            namespace on the market refuses the run)

The market (required; read them from GET https://api.testnet.paradex.trade/v1/markets):
  --market <MARKET>         the Paradex market as Paradex spells it, e.g. BTC-USD-PERP
  --tick <DEC>              its price_tick_size, e.g. 0.1
  --step <DEC>              its order_size_increment, e.g. 0.001
  --min-notional <USD>      its min_notional, e.g. 10 (0 when it states none)

The caps (required; your own limits, in USD, no defaults):
  --resting-cap-usd <USD>   the resting cap per side
  --inventory-cap-usd <USD> the inventory cap

The order (required; no defaults):
  --order-usd <USD>         the order's size in USD at its price, floored onto the size step;
                            at most --resting-cap-usd
  --side <buy|sell>         the order's side (buy: below the best bid; sell: above the best ask)
  --away-bps <N>            how far from the touch the order rests, in basis points, from 100
                            to 2000 (300: 3% below the best bid for a buy)

Options:
  --hold <SECONDS>          how long the acknowledged order rests before the cancel, 0 to 60
                            (default 0)
  --step-timeout <SECONDS>  how long each awaited step may take before the sample gives up and
                            Stops, 1 to 120 (default 15)
  --rest-url <URL>          REST base (default https://api.testnet.paradex.trade/v1)
  --ws-url <URL>            order-entry WebSocket, without a query
                            (default wss://ws.api.testnet.paradex.trade/v1)
  --socks5 <HOST:PORT>      connect through this SOCKS5 proxy (default: directly); refused
                            with loopback stub URLs, which the proxy would resolve on its host
  --lease-dir <DIR>         where the market, account and client-id leases are taken, and the
                            client-id high-water mark is kept across runs
                            (default: $HOME/.fueledbychai/testnet_trade; kept across reboots,
                            unlike a temporary directory)
  -h, --help                print this help

The URLs must name Paradex's testnet hosts (api.testnet.paradex.trade and
ws.api.testnet.paradex.trade) or a loopback host (127.0.0.0/8, ::1, localhost: a test stub);
anything else, mainnet included, is refused, as is any chain id but PRIVATE_SN_POTC_SEPOLIA.

Lines (each step with the milliseconds since the start):
  TESTNET ...        the hosts the guard accepted (never the URLs: a path may carry a token)
  OWNER-ASSISTED ... this is a declared owner-assisted testnet run (decision 0067)
  BBO ...            the touch read from GET /orderbook before the order is priced
  BBO again ...      the touch read again just before the place: the order must still be
                     at least --away-bps behind it, or nothing is placed
  ORDER ...          the order to be placed: side, size, price, notional, distance from the touch
  STEP login         the login answered and the order socket is authenticated
  STEP arm           Paradex accepted cancel-on-disconnect for the socket
  STEP resync        the REST resync of open orders and positions ended
  SEED ...           the position seeded by hand from the resync's REST position
  STEP start         the market was Started under its caps
  STEP place         the order was handed to the session
  STEP ack           Paradex answered the place (provisional: queued for its risk check)
  STEP cancel        the cancel was handed to the session
  STEP cancel-ack    Paradex answered the cancel (provisional: queued for cancellation)
  STEP closed        the order event reporting the order cancelled, nothing of it filled
  STEP stop          kill switch on, cancel all sent for what is still open, session closed
  TIMEOUT <step>     the step did not happen in time; the sample Stops
  NOTE ...           something worth knowing (an unexpected event, a refusal)
  DONE ok|failed     the outcome; ok only when every step happened, every order of ours
                     on the market ended cancelled with nothing filled during the run, every
                     Stop cancel was sent and accepted, the inventory did not move, no fill
                     not of our orders or position change came in, and no order not ours
                     came into view; the exit status is 0 only for ok

Ctrl-C aborts at once: the socket closes and Paradex's cancel-on-disconnect cancels the order.
";

/// The testnet's REST base and order-entry WebSocket.
pub const TESTNET_REST: &str = "https://api.testnet.paradex.trade/v1";
pub const TESTNET_WS: &str = "wss://ws.api.testnet.paradex.trade/v1";

/// The testnet's hosts: the only non-loopback hosts the guard admits.
const TESTNET_REST_HOST: &str = "api.testnet.paradex.trade";
const TESTNET_WS_HOST: &str = "ws.api.testnet.paradex.trade";

/// The testnet's Starknet chain id, the only one the guard admits.
pub const TESTNET_CHAIN: &str = "PRIVATE_SN_POTC_SEPOLIA";

/// [`TESTNET_CHAIN`]'s felt (its ASCII bytes as one big-endian number) in decimal, as the Java
/// library writes `PARADEX_CHAIN_ID`, and in hex, lower case without `0x`.
const TESTNET_CHAIN_DECIMAL: &str = "7693264728749915528729180568779831130134670232771119425";
const TESTNET_CHAIN_HEX: &str = "505249564154455f534e5f504f54435f5345504f4c4941";

/// Whether `chain` names the testnet's chain: [`TESTNET_CHAIN`] by name, or its felt in
/// decimal or in `0x` hex (either case). Nothing else, not even another spelling of the felt.
fn is_testnet_chain(chain: &str) -> bool {
    let hex = chain
        .strip_prefix("0x")
        .or_else(|| chain.strip_prefix("0X"))
        .map(str::to_ascii_lowercase);
    chain == TESTNET_CHAIN
        || chain == TESTNET_CHAIN_DECIMAL
        || hex.as_deref() == Some(TESTNET_CHAIN_HEX)
}

/// Ends a refusal of a value: what was typed is never shown, as it may be a pasted secret.
const UNSHOWN: &str = " (the value is not shown: it may be a secret)";

/// The environment variable of the chain id (optional).
pub const CHAIN_VAR: &str = "PARADEX_CHAIN_ID";

/// The order's side.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// What to trade and how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub market: String,
    pub tick: Decimal,
    pub step: Decimal,
    pub min_notional: Decimal,
    /// The resting cap per side, in USD.
    pub resting_cap_usd: Decimal,
    /// The inventory cap, in USD.
    pub inventory_cap_usd: Decimal,
    /// The order's size in USD at its price, at most the resting cap.
    pub order_usd: Decimal,
    /// The client-id namespace the consumer allocates this sample on the account.
    pub namespace: u16,
    pub side: OrderSide,
    pub away_bps: u32,
    pub hold_secs: u64,
    pub step_timeout_secs: u64,
    pub rest_url: String,
    pub ws_url: String,
    pub proxy: ProxyConfig,
    pub lease_dir: PathBuf,
    /// The owner's confirmation that no other process trades the account.
    pub sole_trader: bool,
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Trade(Box<Options>),
}

/// Parses the arguments after the program name; an error says which argument is wrong. The
/// URLs are checked against the testnet guard here as well as before connecting.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    let mut market = None;
    let (mut tick, mut step, mut min_notional) = (None, None, None);
    let (mut resting_cap_usd, mut inventory_cap_usd) = (None, None);
    let (mut side, mut away_bps) = (None, None);
    let (mut order_usd, mut namespace) = (None, None);
    let mut hold_secs = 0;
    let mut step_timeout_secs = 15;
    let mut rest_url = TESTNET_REST.to_owned();
    let mut ws_url = TESTNET_WS.to_owned();
    let mut proxy = ProxyConfig::Direct;
    let mut lease_dir = None;
    let mut sole_trader = false;
    let mut args = args.into_iter();
    // The position of the argument read last, from 1: a refusal names it, not what was typed.
    let mut at = 0usize;
    while let Some(flag) = args.next() {
        at += 1;
        if flag == "-h" || flag == "--help" {
            return Ok(Parsed::Help);
        }
        // A value that starts with `--` is the next flag: the value was left out.
        let mut value = || {
            at += 1;
            args.next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        if flag == "--sole-trader" {
            sole_trader = true;
            continue;
        }
        match flag.as_str() {
            "--market" => market = Some(symbol(&value()?)?),
            "--tick" => tick = Some(positive(&flag, &value()?)?),
            "--step" => step = Some(positive(&flag, &value()?)?),
            "--min-notional" => {
                let v = value()?;
                match v.parse::<Decimal>() {
                    Ok(d) if d >= Decimal::ZERO => min_notional = Some(d),
                    _ => {
                        return Err(format!(
                            "--min-notional: not a decimal of 0 or more{UNSHOWN}"
                        ));
                    }
                }
            }
            "--resting-cap-usd" => resting_cap_usd = Some(positive(&flag, &value()?)?),
            "--inventory-cap-usd" => inventory_cap_usd = Some(positive(&flag, &value()?)?),
            "--order-usd" => order_usd = Some(positive(&flag, &value()?)?),
            "--namespace" => namespace = Some(bounded(&flag, &value()?, 1, 65_535)? as u16),
            "--side" => {
                side = Some(match value()?.as_str() {
                    "buy" => OrderSide::Buy,
                    "sell" => OrderSide::Sell,
                    _ => return Err(format!("--side: not buy or sell{UNSHOWN}")),
                })
            }
            "--away-bps" => away_bps = Some(bounded(&flag, &value()?, 100, 2000)? as u32),
            "--hold" => hold_secs = bounded(&flag, &value()?, 0, 60)?,
            "--step-timeout" => step_timeout_secs = bounded(&flag, &value()?, 1, 120)?,
            "--rest-url" => rest_url = value()?,
            "--ws-url" => ws_url = value()?,
            "--socks5" => proxy = socks5(&value()?)?,
            "--lease-dir" => lease_dir = Some(PathBuf::from(value()?)),
            // A flag (it starts with '-') is named; anything else may be a pasted secret.
            _ if flag.starts_with('-') => {
                return Err(format!("unknown argument {flag}; --help lists them"));
            }
            _ => {
                return Err(format!(
                    "argument {at}: not a flag (the argument is not shown: it may be a secret); \
                     --help lists them"
                ));
            }
        }
    }
    let need = |name: &str| format!("{name} is required; --help says where to read it");
    let opts = Options {
        market: market.ok_or_else(|| need("--market"))?,
        tick: tick.ok_or_else(|| need("--tick"))?,
        step: step.ok_or_else(|| need("--step"))?,
        min_notional: min_notional.ok_or_else(|| need("--min-notional"))?,
        resting_cap_usd: resting_cap_usd.ok_or_else(|| need("--resting-cap-usd"))?,
        inventory_cap_usd: inventory_cap_usd.ok_or_else(|| need("--inventory-cap-usd"))?,
        order_usd: order_usd.ok_or_else(|| need("--order-usd"))?,
        namespace: namespace.ok_or_else(|| need("--namespace"))?,
        side: side.ok_or_else(|| need("--side"))?,
        away_bps: away_bps.ok_or_else(|| need("--away-bps"))?,
        hold_secs,
        step_timeout_secs,
        rest_url,
        ws_url,
        proxy,
        lease_dir: match lease_dir {
            Some(dir) => dir,
            None => default_lease_dir()?,
        },
        sole_trader,
    };
    let target = testnet_urls(&opts.rest_url, &opts.ws_url)?;
    direct_to_stub(target, &opts.proxy)?;
    sole(&opts)?;
    within_cap(&opts)?;
    Ok(Parsed::Trade(Box::new(opts)))
}

/// `$HOME/.fueledbychai/testnet_trade`: durable, so the client-id high-water mark kept there
/// outlives a reboot or a temporary-directory cleanup.
fn default_lease_dir() -> Result<PathBuf, String> {
    match std::env::var_os("HOME") {
        Some(home) if !home.is_empty() => Ok(PathBuf::from(home)
            .join(".fueledbychai")
            .join("testnet_trade")),
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

/// The testnet guard, run before anything connects: the REST base must be `https://` to
/// Paradex's testnet REST host and the WebSocket `wss://` to its testnet WebSocket host, or both
/// a loopback host (a test stub), and the chain id (`PARADEX_CHAIN_ID` when set, else the
/// testnet's) must be [`TESTNET_CHAIN`], by name or as its felt in decimal or hex. Refused
/// naming what is not testnet.
pub fn testnet_guard(rest: &str, ws: &str, chain: Option<&str>) -> Result<Target, String> {
    let target = testnet_urls(rest, ws)?;
    match chain {
        None => {}
        Some(chain) if is_testnet_chain(chain) => {}
        // The value is never shown: a private key pasted there by mistake would be printed.
        Some(_) => {
            return Err(format!(
                "{CHAIN_VAR}: not the testnet chain id {TESTNET_CHAIN} (the value is not shown: \
                 it may be a secret); testnet_trade never signs for another chain (mainnet is \
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

/// Refuses a run the owner has not confirmed is the account's only trader (`--sole-trader`):
/// one quoter per market and account, and the lease files see only another testnet_trade
/// sharing the lease directory, never a Java or Rust process trading the account elsewhere.
pub fn sole(opts: &Options) -> Result<(), String> {
    if opts.sole_trader {
        Ok(())
    } else {
        Err(
            "--sole-trader is required: confirm that no other process, Java or Rust, trades \
             this account (the leases cannot see one that does not share --lease-dir)"
                .to_owned(),
        )
    }
}

/// Refuses an order larger than the resting cap: `--order-usd` above `--resting-cap-usd`.
pub fn within_cap(opts: &Options) -> Result<(), String> {
    if opts.order_usd <= opts.resting_cap_usd {
        Ok(())
    } else {
        Err(format!(
            "--order-usd {} is above --resting-cap-usd {}: the cap bounds the order",
            opts.order_usd, opts.resting_cap_usd
        ))
    }
}

/// The hosts of the REST and WebSocket URLs, all of them a run prints: an accepted URL's path
/// may still carry a token.
pub fn hosts(rest: &str, ws: &str) -> Result<(String, String), String> {
    Ok((
        host_of(rest, &["https://", "http://"], "--rest-url")?,
        host_of(ws, &["wss://", "ws://"], "--ws-url")?,
    ))
}

/// The URL half of [`testnet_guard`].
fn testnet_urls(rest: &str, ws: &str) -> Result<Target, String> {
    let (rest_host, ws_host) = hosts(rest, ws)?;
    let loopback = |host: &str| {
        host == "localhost"
            || host == "[::1]"
            || host
                .parse::<std::net::Ipv4Addr>()
                .is_ok_and(|ip| ip.is_loopback())
    };
    let plain = |url: &str| url.starts_with("http://") || url.starts_with("ws://");
    match (loopback(&rest_host), loopback(&ws_host)) {
        (true, true) => Ok(Target::LoopbackStub),
        (false, false) => {
            if rest_host != TESTNET_REST_HOST || plain(rest) {
                return Err(format!(
                    "--rest-url (host {rest_host}): not Paradex's testnet REST base (https://{TESTNET_REST_HOST}/v1) \
                     or a loopback stub; testnet_trade is testnet only"
                ));
            }
            if ws_host != TESTNET_WS_HOST || plain(ws) {
                return Err(format!(
                    "--ws-url (host {ws_host}): not Paradex's testnet WebSocket (wss://{TESTNET_WS_HOST}/v1) \
                     or a loopback stub; testnet_trade is testnet only"
                ));
            }
            Ok(Target::Testnet)
        }
        _ => Err(
            "--rest-url and --ws-url: one is a loopback stub and the other is not; give both \
             testnet URLs or both a stub's"
                .to_owned(),
        ),
    }
}

/// The lowercased host of `url` (without port), which must start with one of `schemes` and name
/// no user, query or fragment. A refusal names the flag, never the URL: what was typed may carry
/// a secret (a password, a token).
fn host_of(url: &str, schemes: &[&str], flag: &str) -> Result<String, String> {
    let bad = |why: &str| format!("{flag}: {why} (the URL is not shown: it may carry a secret)");
    let rest = schemes
        .iter()
        .find_map(|s| url.strip_prefix(s))
        .ok_or_else(|| bad(&format!("not a {} URL", schemes.join(" or "))))?;
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    if authority.contains('@') {
        return Err(bad("a URL with a user is refused"));
    }
    if rest.contains(['?', '#']) {
        return Err(bad("a URL with a query or a fragment is refused"));
    }
    let host = match authority.strip_prefix('[') {
        Some(v6) => format!(
            "[{}]",
            &v6[..v6.find(']').ok_or_else(|| bad("no closing ]"))?]
        ),
        None => authority[..authority.find(':').unwrap_or(authority.len())].to_owned(),
    };
    if host.is_empty() {
        return Err(bad("no host"));
    }
    // Printed in a later refusal: a host name's characters only.
    if !host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-.[]:".contains(&b))
    {
        return Err(bad("not a host name"));
    }
    Ok(host.to_ascii_lowercase())
}

/// The longest part of a market name between its '-'s: Paradex's are short (`BTC`, `PERP`,
/// `27JUN25`, `100000`), and a longer one is more likely a pasted secret than a market.
const MARKET_PART_MAX: usize = 12;

/// A Paradex market as Paradex spells it: three or more parts of upper-case ASCII letters and
/// digits, each at most [`MARKET_PART_MAX`] long, joined by '-' (`BTC-USD-PERP`). Anything else
/// is refused before it is put in a request path, where a pasted private key would be sent to
/// the venue; the refusal never shows the value.
fn symbol(value: &str) -> Result<String, String> {
    let parts: Vec<&str> = value.split('-').collect();
    let shaped = parts.len() >= 3
        && parts.iter().all(|p| {
            (1..=MARKET_PART_MAX).contains(&p.len())
                && p.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        });
    if shaped {
        Ok(value.to_owned())
    } else {
        Err(format!(
            "--market: not a market as Paradex spells it (upper-case letters and digits in three \
             or more parts joined by '-', e.g. BTC-USD-PERP){UNSHOWN}"
        ))
    }
}

/// A whole number from `lo` to `hi`.
fn bounded(flag: &str, value: &str, lo: u64, hi: u64) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(n) if (lo..=hi).contains(&n) => Ok(n),
        _ => Err(format!(
            "{flag}: not a whole number from {lo} to {hi}{UNSHOWN}"
        )),
    }
}

/// `host:port`, the port from 1 to 65535.
fn socks5(value: &str) -> Result<ProxyConfig, String> {
    // Never echoed: a proxy typed with a user and password would print them.
    let bad =
        || "--socks5: not host:port (the value is not shown: it may carry a secret)".to_owned();
    let (host, port) = value.rsplit_once(':').ok_or_else(bad)?;
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(bad)?;
    if host.is_empty() {
        return Err(bad());
    }
    Ok(ProxyConfig::Socks5 {
        host: host.to_owned(),
        port,
    })
}

/// A positive decimal.
fn positive(flag: &str, value: &str) -> Result<Decimal, String> {
    match value.parse::<Decimal>() {
        Ok(d) if d > Decimal::ZERO => Ok(d),
        _ => Err(format!("{flag}: not a positive decimal{UNSHOWN}")),
    }
}
