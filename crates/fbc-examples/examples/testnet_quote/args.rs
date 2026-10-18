//! testnet_quote's command line, parsed by hand (no argument-parsing dependency). The testnet
//! guard it checks (the REST and WebSocket URLs and the chain id must name Paradex's testnet or,
//! for the URLs, a loopback test stub, checked before anything connects) and the value parsers
//! are the samples' shared ones ([`crate::testnet`]).

use std::path::PathBuf;

use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

pub use crate::testnet::{
    CHAIN_VAR, TESTNET_REST, TESTNET_WS, Target, direct_to_stub, hosts, testnet_guard,
};
use crate::testnet::{
    UNSHOWN, absolute, bounded, default_lease_dir, flag_name, positive, socks5, symbol,
    testnet_urls,
};

/// The help text `--help` prints.
pub const USAGE: &str = "\
testnet_quote: a PLUMBING TEST, NOT A STRATEGY. On one Paradex TESTNET market it keeps one
post-only limit order of a fixed size on each side, a fixed number of basis points behind the
touch, and requotes them as the touch moves: Paradex's touch (GET /orderbook) -> a two-sided
desired book -> fbc-oms's ExecutionPlanner -> fbc-oms's caps and authorizations -> the
order-entry session -> Paradex testnet. Every change is a cancel and then a new place once the
old order ended (cancel-and-replace, never an amend). It prices nothing from any model: the
offsets are the flags'. TESTNET ONLY: a mainnet URL or chain id is refused before anything
connects. An owner-assisted testnet run (decision 0067): the first resync seeds the market's
position (Paradex's snapshot is trustworthy, decision 0094), or, when it left it unknown, it is
seeded by hand from the venue's own REST position; a restart seeds again.

Usage: cargo run -p fbc-examples --example testnet_quote -- [OPTIONS]

Environment (read once at start; no value is ever printed):
  PARADEX_ACCOUNT_ADDRESS   the testnet account's Starknet address (0x and hex digits)
  PARADEX_PRIVATE_KEY       the account's own Stark private key (0x and hex digits): the main
                            key, not a trading subkey
  PARADEX_CHAIN_ID          optional; when set it must be the testnet's,
                            PRIVATE_SN_PARACLEAR_TESTNET (the starknet_chain_id of GET
                            https://api.testnet.paradex.trade/v1/system/config), by name or as
                            its felt in decimal (as the Java library writes it) or 0x hex

Required confirmation and namespace:
  --sole-trader             you confirm that no other process, Java or Rust, trades this
                            account (the lease files guard only against another sample using
                            the same --lease-dir); refused without it
  --namespace <N>           the client-id namespace your configuration allocates this sample
                            on the account, 1 to 65535 (no default; an id of another namespace
                            on the market refuses the run)

The market (required; read them from GET https://api.testnet.paradex.trade/v1/markets):
  --market <MARKET>         the Paradex perpetual as Paradex spells it, e.g. BTC-USD-PERP
  --tick <DEC>              its price_tick_size, e.g. 0.1
  --step <DEC>              its order_size_increment, e.g. 0.001
  --min-notional <USD>      its min_notional, e.g. 10 (0 when it states none)

The caps (required; your own limits, in USD, no defaults):
  --resting-cap-usd <USD>   the resting cap per side (the owner's is 11)
  --inventory-cap-usd <USD> the inventory cap (the owner's is 50)

The quotes (required; no defaults):
  --order-usd <USD>         each quote's size in USD, at the highest quote price of the first
                            touch, floored onto the size step: a fixed number of lots for the
                            whole run; at most --resting-cap-usd
  --bid-bps <N>             how far below the best bid the bid rests, 1 to 2000 basis points
  --ask-bps <N>             how far above the best ask the ask rests, 1 to 2000 basis points
  --duration <SECONDS>      how long the loop runs before it Stops, 1 to 86400

Options:
  --cycles <N>              Stop after N requote cycles, 1 to 1000000 (default: none, the
                            duration alone ends the run)
  --interval-ms <MS>        the pause between requote cycles, 100 to 60000 (default 1000)
  --step-timeout <SECONDS>  how long each awaited step (the login, the arm, the resync, a
                            book settling, a Stop cancel) may take before the run Stops, 1 to
                            120 (default 15)
  --rest-url <URL>          REST base: https://api.testnet.paradex.trade/v1 (the default)
                            or a loopback stub's
  --ws-url <URL>            order-entry WebSocket: wss://ws.api.testnet.paradex.trade/v1
                            (the default) or a loopback stub's
  --socks5 <HOST:PORT>      connect through this SOCKS5 proxy (default: directly); refused
                            with loopback stub URLs, which the proxy would resolve on its host
  --lease-dir <DIR>         an absolute path where the market, account and client-id leases
                            are taken and the client-id high-water mark is kept (default:
                            $HOME/.fueledbychai/testnet_trade, shared with testnet_trade so the
                            two samples never trade one market or namespace at once)
  -h, --help                print this help

The URLs must be exactly Paradex's testnet ones (above), or both a loopback stub's (host an
address in 127.0.0.0/8 or [::1], never a name; any port; the path exactly /v1). Anything else,
mainnet included, is refused, as is any chain id but PRIVATE_SN_PARACLEAR_TESTNET. A flag's
value goes in the next argument, never after '='.

Each cycle reads the touch, and for each side whose touch rests prices the quote behind it
(floored for the bid, ceiled for the ask, onto the tick; refused unless strictly behind the
touch). A side is left unquoted (and its order cancelled) when its book side is empty, when
the quote's notional at its price would exceed --resting-cap-usd or fall below
--min-notional, or when a fill of it would take the position past --inventory-cap-usd at the
highest of the quote prices and the touch. fbc-oms checks the same caps in lots, converted at
the first touch, on every place.

It Stops (kill switch on, every order of ours on the market cancelled, the session closed) on
Ctrl-C, at --duration or --cycles, on a refused login, on a resync whose position disagrees
with the inventory or that the registry refuses, on any order of ours going Unknown (it does
not drive the Unknown ladder: no Paradex order query is wired here), on an order or fill not
ours, an amend of ours, any problem the order socket reports, a book that does not settle
within the step timeout, and when the order-entry session ends. After every reconnect, once
cancel-on-disconnect is armed again and the resync has ended, every order of ours still open
on the market is cancelled before anything is placed (Paradex's arm does not say it covers
orders already open). A replace's cancel that races a fill may come back Unknown (Paradex
answers ALREADY_CLOSED, FBC-rht4), which Stops the run: the conservative end.

Lines (each with the milliseconds since the start):
  TESTNET ...        the hosts the guard accepted (never the URLs)
  OWNER-ASSISTED ... this is a declared owner-assisted testnet run (decision 0067)
  PLUMBING TEST ...  a reminder: fixed offsets, no model
  QUOTE ...          the quote size in lots and the caps in lots, from the first touch
  STEP login|arm|resync|start   as testnet_trade prints them
  SEED ...           the position the first resync seeded (or seeded by hand, if it did not)
  CYCLE n ...        a requote cycle that changed the book: the touch and the quotes
  SKIP side ...      a side left unquoted this cycle, and why
  PLACE side ...     a place handed to the session: its client id sequence, size and price
  CANCEL side ...    a cancel handed to the session (a replace, a side pulled, a reconnect's
                     cancel, or Stop's)
  ACK / CANCEL-ACK   Paradex answered the place / the cancel
  CLOSED ...         the order event ending an order of ours, and what of it filled
  FILL ...           a fill of an order of ours
  RECONNECT ...      a new connection: every order of ours still open is cancelled first
  NOTE ...           something worth knowing
  STOP <why>         why the loop ended
  STEP stop          kill switch on, cancel all sent for what is still open, session closing
  POSITION after the first fill: ...  GET /positions read before the next plan after the
                     first fill, the market's size compared with the registry's position in
                     lots (with the time since the start and the fills applied)
  POSITION re-read ...  on a mismatch, GET /positions read once more after --step-timeout:
                     agreeing, the loop goes on (GET /positions lagged the fills); still
                     differing, the loop Stops with nothing placed after and fails:
                     check the position by hand
  POSITION at Stop: ...  the same comparison once Stop's cancels are answered, on every run;
                     a mismatch the re-read confirms fails the run
  SUMMARY ...        cycles, orders placed, cancels sent, orders cancelled, fills, inventory
  DONE ok|failed     ok only when the loop ended at --duration, --cycles or Ctrl-C, every
                     Stop cancel was accepted, every order of ours ended and GET /positions
                     agreed with the registry (POSITION); the exit status is 0 only for ok
";

/// What to quote and how.
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
    /// Each quote's size in USD at the first touch, at most the resting cap.
    pub order_usd: Decimal,
    /// The client-id namespace the consumer allocates this sample on the account.
    pub namespace: u16,
    /// How far behind the best bid and ask the quotes rest, in basis points.
    pub bid_bps: u32,
    pub ask_bps: u32,
    pub duration_secs: u64,
    pub cycles: Option<u64>,
    pub interval_ms: u64,
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
    Quote(Box<Options>),
}

/// Parses the arguments after the program name; an error says which argument is wrong, never
/// what was typed. The URLs are checked against the testnet guard here as well as before
/// connecting.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    let mut market = None;
    let (mut tick, mut step, mut min_notional) = (None, None, None);
    let (mut resting_cap_usd, mut inventory_cap_usd) = (None, None);
    let (mut order_usd, mut namespace) = (None, None);
    let (mut bid_bps, mut ask_bps, mut duration_secs) = (None, None, None);
    let mut cycles = None;
    let mut interval_ms = 1_000;
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
        if flag == "--sole-trader" {
            sole_trader = true;
            continue;
        }
        // A value that starts with `--` is the next flag: the value was left out.
        let mut value = || {
            at += 1;
            args.next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--market" => market = Some(symbol(&value()?)?),
            "--tick" => tick = Some(positive(&flag, &value()?)?),
            "--step" => step = Some(positive(&flag, &value()?)?),
            "--min-notional" => match value()?.parse::<Decimal>() {
                Ok(d) if d >= Decimal::ZERO => min_notional = Some(d),
                _ => {
                    return Err(format!(
                        "--min-notional: not a decimal of 0 or more{UNSHOWN}"
                    ));
                }
            },
            "--resting-cap-usd" => resting_cap_usd = Some(positive(&flag, &value()?)?),
            "--inventory-cap-usd" => inventory_cap_usd = Some(positive(&flag, &value()?)?),
            "--order-usd" => order_usd = Some(positive(&flag, &value()?)?),
            "--namespace" => namespace = Some(bounded(&flag, &value()?, 1, 65_535)? as u16),
            "--bid-bps" => bid_bps = Some(bounded(&flag, &value()?, 1, 2_000)? as u32),
            "--ask-bps" => ask_bps = Some(bounded(&flag, &value()?, 1, 2_000)? as u32),
            "--duration" => duration_secs = Some(bounded(&flag, &value()?, 1, 86_400)?),
            "--cycles" => cycles = Some(bounded(&flag, &value()?, 1, 1_000_000)?),
            "--interval-ms" => interval_ms = bounded(&flag, &value()?, 100, 60_000)?,
            "--step-timeout" => step_timeout_secs = bounded(&flag, &value()?, 1, 120)?,
            "--rest-url" => rest_url = value()?,
            "--ws-url" => ws_url = value()?,
            "--socks5" => proxy = socks5(&value()?)?,
            "--lease-dir" => lease_dir = Some(absolute(&value()?)?),
            // A flag (shaped like one) is named, by its part before any '=' only: what follows
            // may be a pasted secret, as may anything else, which is named by its position.
            _ if flag_name(&flag).is_some() => {
                return Err(match flag.split_once('=') {
                    Some((name, _)) => format!(
                        "unknown argument {name} typed with '=' (the value is not shown: it may \
                         be a secret); give a flag's value as the next argument; --help lists \
                         them"
                    ),
                    None => format!("unknown argument {flag}; --help lists them"),
                });
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
        bid_bps: bid_bps.ok_or_else(|| need("--bid-bps"))?,
        ask_bps: ask_bps.ok_or_else(|| need("--ask-bps"))?,
        duration_secs: duration_secs.ok_or_else(|| need("--duration"))?,
        cycles,
        interval_ms,
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
    Ok(Parsed::Quote(Box::new(opts)))
}

/// Refuses a run the owner has not confirmed is the account's only trader (`--sole-trader`).
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

/// Refuses a quote larger than the resting cap: `--order-usd` above `--resting-cap-usd`.
pub fn within_cap(opts: &Options) -> Result<(), String> {
    if opts.order_usd <= opts.resting_cap_usd {
        Ok(())
    } else {
        Err(format!(
            "--order-usd {} is above --resting-cap-usd {}: the cap bounds each quote",
            opts.order_usd, opts.resting_cap_usd
        ))
    }
}
