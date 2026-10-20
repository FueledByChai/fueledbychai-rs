//! mainnet_smoke's command line, parsed by hand (no argument-parsing dependency) into
//! testnet_trade's [`Options`] (the round trip is testnet_trade's, [`crate::trade`]) and the
//! `--mainnet` confirmation. The value parsers are the samples' shared ones
//! ([`crate::testnet`]); the URL guard is mainnet's ([`crate::smoke::mainnet_guard`]).

use fbc_runtime::ProxyConfig;

pub use crate::args::{Options, OrderSide};
use crate::mainnet::{LEASE_DIR_NAME, MAINNET_REST, MAINNET_WS, mainnet_caps, mainnet_urls};
use crate::testnet::{
    Target, UNSHOWN, absolute, bounded, direct_to_stub, flag_name, lease_dir_named, positive,
    socks5, symbol,
};

/// The help text `--help` prints.
pub const USAGE: &str = "\
mainnet_smoke: the owner's first Paradex MAINNET order. Places ONE post-only limit order of
about $11, well away from the touch, through fbc-oms, waits for its acknowledgement, cancels
it, waits for the cancel's acknowledgement, then Stops (kill switch + cancel all) and exits.
MAINNET ONLY, and REAL MONEY: refused without both --mainnet and --sole-trader.

Before you run it:
  - a SEPARATE Paradex mainnet account from the one the Java quoter trades (one quoter per
    account), funded small, with no open orders and no position on any market;
  - its Paradex (Starknet) address and its main L2 key, exported as below;
  - nothing else: Paradex's snapshot source is trustworthy (decision 0094, accepted by the
    owner), so the resync seeds the market; a mainnet market is armed only after a trustworthy
    resync and never seeded by hand (decision 0067 allows that only for an owner-assisted
    testnet run), and a venue declaring its snapshot otherwise is refused before anything
    connects.

Usage: cargo run -p fbc-examples --example mainnet_smoke -- --mainnet --sole-trader [OPTIONS]

Environment (read once at start; no value is ever printed):
  PARADEX_MAINNET_ACCOUNT_ADDRESS  the mainnet account's Paradex (Starknet) address
  PARADEX_MAINNET_PRIVATE_KEY      its own Stark private key, the main key (not a subkey)
  PARADEX_CHAIN_ID                 optional; when set it must be the mainnet's,
                                   PRIVATE_SN_PARACLEAR_MAINNET, by name or as its felt in
                                   decimal or 0x hex
  The testnet samples' PARADEX_ACCOUNT_ADDRESS and PARADEX_PRIVATE_KEY must NOT be set: either
  one refuses the run, so a testnet key never reaches a mainnet login (unset them first).

Required confirmations and namespace:
  --mainnet                 you confirm this run trades Paradex MAINNET with real money
  --sole-trader             you confirm that no other process, Java or Rust, trades this
                            account
  --namespace <N>           the client-id namespace your configuration allocates this sample
                            on the account, 1 to 65535 (no default)

The market (required; read them from GET https://api.prod.paradex.trade/v1/markets):
  --market <MARKET>         the Paradex perpetual as Paradex spells it, e.g. BTC-USD-PERP
  --tick <DEC>              its price_tick_size
  --step <DEC>              its order_size_increment
  --min-notional <USD>      its min_notional (Paradex judges it at the MARK price)

The caps (required; yours, in USD, positive, no defaults and no ceiling):
  --resting-cap-usd <USD>   the resting cap per side, judged at the order's own price (the
                            notional a fill costs), not at the mark
  --inventory-cap-usd <USD> the inventory cap

The order (required; no defaults):
  --order-usd <USD>         the order's size in USD at its price, floored onto the size step;
                            at most --resting-cap-usd, and at the mark price at least
                            --min-notional, or nothing is sent
  --side <buy|sell>         buy: below the best bid; sell: above the best ask
  --away-bps <N>            how far behind the touch it rests, 100 to 2000 basis points

Options:
  --hold <SECONDS>          how long the acknowledged order rests before the cancel, 0 to 60
                            (default 0)
  --step-timeout <SECONDS>  how long each awaited step may take before the run gives up and
                            Stops, 1 to 120 (default 15)
  --rest-url <URL>          REST base: https://api.prod.paradex.trade/v1 (the default), or
                            a loopback stub's (tests)
  --ws-url <URL>            order-entry WebSocket: wss://ws.api.prod.paradex.trade/v1 (the
                            default), or a loopback stub's (tests)
  --socks5 <HOST:PORT>      connect through this SOCKS5 proxy (default: directly)
  --lease-dir <DIR>         an absolute path for the leases and the client-id high-water mark
                            (default: $HOME/.fueledbychai/mainnet_smoke)
  -h, --help                print this help

The URLs must be exactly Paradex's mainnet ones, or both a loopback stub's (an address in
127.0.0.0/8 or [::1], the path exactly /v1). Anything else, testnet included, is refused, as
is a chain id other than PRIVATE_SN_PARACLEAR_MAINNET. A flag's value goes in the next
argument, never after '='.

Lines (each step with the milliseconds since the start):
  MAINNET ...        the hosts the guard accepted (never the URLs)
  CAPS ...           the caps the run takes, printed before anything connects
  STEP chain         GET /system/config names PRIVATE_SN_PARACLEAR_MAINNET (else refused)
  STEP mark          the market's mark price from GET /markets/summary
  BBO ...            the touch read from GET /orderbook before the order is priced
  ORDER ...          the order: side, size, price, notional at its price and at the mark
  STEP login         the login answered and the order socket is authenticated
  STEP arm           Paradex accepted cancel-on-disconnect for the socket
  STEP resync        the REST resync ended; it must be trustworthy and the account flat
  SEED ...           the position the trustworthy resync seeded (never seeded by hand)
  STEP start         the market was Started under its caps
  BBO again ...      the touch read again just before the place
  STEP place         the order was handed to the session
  STEP ack           Paradex answered the place (provisional: queued for its risk check)
  STEP cancel        the cancel was handed to the session
  STEP cancel-ack    Paradex answered the cancel
  STEP closed        the order event reporting the order cancelled, nothing of it filled
  STEP stop          kill switch on, cancel all sent (an instrument cancel-all of the market),
                     session closed
  POSITION after the first fill: ...  GET /positions read after the first fill the session
                     sees, the market's size compared with the registry's position in lots
                     (with the time since the start and the fills applied)
  POSITION re-read ...  on a mismatch, GET /positions read once more after --step-timeout:
                     agreeing, the run goes on (GET /positions lagged the fills); still
                     differing, the run Stops at once and fails: check the position by hand
  POSITION at Stop: ...  the same comparison once Stop's cancels are answered, on every run,
                     before STEP stop (the market still leased); a mismatch the re-read
                     confirms fails the run
  TIMEOUT <step>     the step did not happen in time; the run Stops
  HALT <step>        the step was not waited for: the position check after the first fill
                     failed (POSITION re-read ... STILL DIFFER); the run Stops
  WARN ...           a cancel of the order that this program did not send ended it (Paradex
                     does not say who sent a user cancel): another program on the account, or
                     you in the Paradex UI; one quoter per account (FBC-aoh5)
  NOTE ...           something worth knowing (a refusal with Paradex's status, code and
                     message, an unexpected event, why a connection ended)
  DONE ok|failed     the outcome; the exit status is 0 only for ok

Ctrl-C aborts at once: the socket closes and Paradex's cancel-on-disconnect cancels the order.
";

/// What to trade, and the owner's mainnet confirmation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Smoke {
    /// The round trip's options (testnet_trade's), its URLs mainnet's or a stub's.
    pub trade: Options,
    /// `--mainnet`: the owner's confirmation that the run trades mainnet.
    pub mainnet: bool,
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Smoke(Box<Smoke>),
}

/// Parses the arguments after the program name; an error says which argument is wrong, never
/// what was typed. The URLs are checked against mainnet's guard, and the confirmations and caps
/// ([`confirmed`]), here as well as by the run.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    let mut market = None;
    let (mut tick, mut step, mut min_notional) = (None, None, None);
    let (mut resting_cap_usd, mut inventory_cap_usd) = (None, None);
    let (mut side, mut away_bps) = (None, None);
    let (mut order_usd, mut namespace) = (None, None);
    let mut hold_secs = 0;
    let mut step_timeout_secs = 15;
    let mut rest_url = MAINNET_REST.to_owned();
    let mut ws_url = MAINNET_WS.to_owned();
    let mut proxy = ProxyConfig::Direct;
    let mut lease_dir = None;
    let (mut sole_trader, mut mainnet) = (false, false);
    let mut args = args.into_iter();
    // The position of the argument read last, from 1: a refusal names it, not what was typed.
    let mut at = 0usize;
    while let Some(flag) = args.next() {
        at += 1;
        match flag.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--sole-trader" => {
                sole_trader = true;
                continue;
            }
            "--mainnet" => {
                mainnet = true;
                continue;
            }
            _ => {}
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
            "--min-notional" => min_notional = Some(positive(&flag, &value()?)?),
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
    let trade = Options {
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
            None => lease_dir_named(std::env::var_os("HOME"), LEASE_DIR_NAME)?,
        },
        sole_trader,
    };
    let smoke = Smoke { trade, mainnet };
    if mainnet_urls(&smoke.trade.rest_url, &smoke.trade.ws_url)? {
        direct_to_stub(Target::LoopbackStub, &smoke.trade.proxy)?;
    }
    confirmed(&smoke)?;
    Ok(Parsed::Smoke(Box::new(smoke)))
}

/// Refuses a run the owner has not confirmed with both `--mainnet` and `--sole-trader`, or
/// whose caps are not positive ([`mainnet_caps`]: the caps are yours, with no default and no
/// ceiling, owner 2026-10-19, FBC-thks), or whose order exceeds its resting cap.
pub fn confirmed(smoke: &Smoke) -> Result<(), String> {
    let opts = &smoke.trade;
    if !smoke.mainnet {
        return Err(
            "--mainnet is required: confirm that this run trades Paradex MAINNET with real money"
                .to_owned(),
        );
    }
    crate::args::sole(opts)?;
    mainnet_caps(opts.resting_cap_usd, opts.inventory_cap_usd)?;
    crate::args::within_cap(opts)
}
