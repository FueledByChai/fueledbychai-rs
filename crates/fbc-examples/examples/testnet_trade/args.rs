//! testnet_trade's command line, parsed by hand (no argument-parsing dependency). The testnet
//! guard it checks (the REST and WebSocket URLs and the chain id must name Paradex's testnet or,
//! for the URLs, a loopback test stub, checked before anything connects) and the value parsers
//! are the samples' shared ones ([`crate::testnet`]).

use std::path::PathBuf;

use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

pub use crate::testnet::{
    CHAIN_VAR, TESTNET_CHAIN, TESTNET_REST, TESTNET_WS, Target, direct_to_stub, hosts,
    testnet_guard,
};
use crate::testnet::{
    UNSHOWN, absolute, bounded, default_lease_dir, flag_name, positive, socks5, symbol,
    testnet_urls,
};

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
                            PRIVATE_SN_PARACLEAR_TESTNET (the starknet_chain_id of GET
                            https://api.testnet.paradex.trade/v1/system/config), by name or as
                            its felt in decimal (as the Java library writes it) or 0x hex

Required confirmation and namespace:
  --sole-trader             you confirm that no other process, Java or Rust, trades this
                            account (the lease files guard only against another testnet_trade
                            using the same --lease-dir); refused without it
  --namespace <N>           the client-id namespace your configuration allocates this sample
                            on the account, 1 to 65535: its orders are minted, recognised as
                            ours, leased and kept under it (no default; an id of another
                            namespace on the market refuses the run)

The market (required; read them from GET https://api.testnet.paradex.trade/v1/markets):
  --market <MARKET>         the Paradex perpetual as Paradex spells it, e.g. BTC-USD-PERP or
                            kBONK-USD-PERP
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
  --rest-url <URL>          REST base: https://api.testnet.paradex.trade/v1 (the default)
                            or a loopback stub's
  --ws-url <URL>            order-entry WebSocket: wss://ws.api.testnet.paradex.trade/v1
                            (the default) or a loopback stub's
  --socks5 <HOST:PORT>      connect through this SOCKS5 proxy (default: directly); refused
                            with loopback stub URLs, which the proxy would resolve on its host
  --lease-dir <DIR>         an absolute path where the market, account and client-id leases
                            are taken, and the client-id high-water mark is kept across runs
                            (default: $HOME/.fueledbychai/testnet_trade; kept across reboots,
                            unlike a temporary directory)
  -h, --help                print this help

The URLs must be exactly Paradex's testnet ones (above), or both a loopback stub's (host
an address in 127.0.0.0/8 or [::1], never a name such as localhost, which a resolver may map
anywhere; any port; the path exactly /v1): whatever sits in a path is sent to the venue, so no
other path is taken. Anything else, mainnet included, is refused, as is any chain id but
PRIVATE_SN_PARACLEAR_TESTNET. A flag's value goes in the next argument, never after '='.

A refused login: each refusal ends the order socket's connection (a NOTE says why: Paradex's
HTTP status, error code and message), and after 3 in a row the run stops at once with
NOTE login refused 3 times. The usual causes:
  - a mainnet key on testnet: Paradex derives the account's L2 (Starknet) key per network, so
    the testnet account's key differs from the mainnet one, as does its address;
  - an Ethereum address in PARADEX_ACCOUNT_ADDRESS instead of the Paradex (Starknet) account
    address the Paradex app shows;
  - an account not onboarded on the testnet (log in to the testnet app once), or unfunded.

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
  NOTE ...           something worth knowing (an unexpected event, a refusal, why a
                     connection of the order socket ended)
  NOTE login refused N times: ...  the last refusal; the run stops
  DONE ok|failed     the outcome; ok only when every step happened, every order of ours
                     on the market ended cancelled with nothing filled during the run, every
                     Stop cancel was sent and accepted, the inventory did not move, no fill
                     not of our orders or position change came in, and no order not ours
                     came into view; the exit status is 0 only for ok

Ctrl-C aborts at once: the socket closes and Paradex's cancel-on-disconnect cancels the order.
";

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
