//! paradex_testnet_acceptance's command line: testnet_trade's flags (`args.rs`, which also holds
//! the testnet guard) and four of its own, parsed by hand (no argument-parsing dependency).

use std::path::PathBuf;

use crate::args::{self, Options, Parsed};

/// The help text `--help` prints.
pub const USAGE: &str = "\
paradex_testnet_acceptance: FBC-8xr's acceptance run on Paradex TESTNET, through fbc-oms, one run
of each command type on tiny post-only orders well away from the touch (with --allow-fill, also
two small orders that take liquidity, each flattened at once), writing an evidence
report the snapshot-trust decision (FBC-2f7w) cites. TESTNET ONLY: a mainnet URL or chain id is
refused before anything connects. An owner-assisted testnet run: each phase's market position is
seeded by hand from the venue's own REST position (decision 0067).

The steps, each sent once (a step whose order could not be built is reported, not retried):
  place          one post-only GTC limit order on --side, --away-bps behind the touch
  amend          that order, once: further from the touch and, where the market's minimum
                 notional allows, one size step smaller (order.modify's size is the total)
  cancel         that order
  batch          one batch of two post-only orders, a buy below the bid and a sell above the
                 ask (skipped when either side of the book is empty)
  cancel-many    those two orders in one cancel-many
  ioc            one IOC limit order on --side priced behind the touch, which can fill nothing:
                 Paradex expires it, and the reason it states is recorded
  fill           --allow-fill only, on a book with both sides resting: one IOC of about
                 --order-usd at the touch, which takes resting liquidity (never past the
                 inventory cap), GET /positions compared with the fills, then a reduce-only IOC
                 on the other side that flattens it, compared again
  fill-partial   --allow-fill only, when the touch's level on --side's far side is smaller than
                 the order: a GTC limit (not post-only) at that level fills in part and rests;
                 it is amended once (its size sent as the total) to record whether Paradex reads
                 order.modify's size as the total or the remainder (FBC-b0z9), then cancelled
                 and what filled flattened with a reduce-only IOC; otherwise not exercised
  cod-dropped    one post-only order, then the order socket DROPPED with no close frame; the
                 account's open orders are read over REST until the order is gone or --cod-wait
                 passes (a reconnect then Safety-cancels it, decision 0080)
  cod-graceful   the same, the socket CLOSED gracefully (a close frame)
  stop           a last connection: kill switch, cancel everything of ours, a last comparison
After every step the account's GET /orders and GET /positions are read and compared with what the
order stream showed (each order present while the stream shows it open, absent once ended; the
position the fills imply), polled until they agree or --rest-wait passes; each new order's
GET /orders-history?client_id= is read until it lists the order.

Usage: cargo run -p fbc-examples --example paradex_testnet_acceptance -- [OPTIONS]

Its own options:
  --evidence-dir <DIR>      required: an absolute path where the run writes report.md,
                            evidence.json, its journal (journal/) and the scrubbed fixtures
                            (fixtures/, to copy into fixtures/paradex/exec-recorded/); it must
                            be empty or not exist
  --cod-wait <SECONDS>      how long to read REST for an order cancel-on-disconnect should
                            have cancelled, 1 to 300 (default 30)
  --rest-wait <SECONDS>     how long each REST comparison polls before it records that REST
                            never agreed with the stream, 1 to 120 (default 15)
  --allow-fill              run the fill and fill-partial steps: orders that take resting
                            liquidity on TESTNET (about --order-usd each, under the caps), each
                            flattened with a reduce-only IOC; without it neither runs

Every testnet_trade option, with the same meaning (--side, --away-bps and --order-usd shape each
order; --hold is unused):
";

/// What to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptOptions {
    /// testnet_trade's options: the market, its grid, the caps, the order's side, size and
    /// distance, the URLs, the proxy and the lease directory.
    pub trade: Options,
    /// Where the evidence is written.
    pub evidence_dir: PathBuf,
    /// How long, in seconds, REST is read for an order cancel-on-disconnect should cancel.
    pub cod_wait_secs: u64,
    /// How long, in seconds, a comparison polls REST for agreement with the stream.
    pub rest_wait_secs: u64,
    /// Whether the fill steps run (`--allow-fill`, FBC-6fnc): orders that take liquidity.
    pub allow_fill: bool,
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptParsed {
    Help,
    Run(Box<AcceptOptions>),
}

/// Parses the arguments after the program name: this program's own flags, then testnet_trade's
/// from what is left ([`args::parse`], which runs the testnet guard on the URLs).
pub fn parse(argv: impl IntoIterator<Item = String>) -> Result<AcceptParsed, String> {
    let mut evidence_dir = None;
    let mut cod_wait_secs = 30;
    let mut rest_wait_secs = 15;
    let mut allow_fill = false;
    let mut rest = Vec::new();
    let mut argv = argv.into_iter();
    while let Some(arg) = argv.next() {
        let mut value = || {
            argv.next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Ok(AcceptParsed::Help),
            "--evidence-dir" => {
                let dir = PathBuf::from(value()?);
                if !dir.is_absolute() {
                    return Err(
                        "--evidence-dir: not an absolute path (the value is not shown: it may be \
                         a secret)"
                            .to_owned(),
                    );
                }
                evidence_dir = Some(dir);
            }
            "--cod-wait" => cod_wait_secs = seconds("--cod-wait", &value()?, 300)?,
            "--rest-wait" => rest_wait_secs = seconds("--rest-wait", &value()?, 120)?,
            "--allow-fill" => allow_fill = true,
            _ => rest.push(arg),
        }
    }
    let trade = match args::parse(rest)? {
        Parsed::Help => return Ok(AcceptParsed::Help),
        Parsed::Trade(opts) => *opts,
    };
    let evidence_dir = evidence_dir.ok_or("--evidence-dir is required")?;
    Ok(AcceptParsed::Run(Box::new(AcceptOptions {
        trade,
        evidence_dir,
        cod_wait_secs,
        rest_wait_secs,
        allow_fill,
    })))
}

/// A whole number of seconds from 1 to `hi`.
fn seconds(flag: &str, value: &str, hi: u64) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(n) if (1..=hi).contains(&n) => Ok(n),
        _ => Err(format!(
            "{flag}: not a whole number from 1 to {hi} (the value is not shown: it may be a \
             secret)"
        )),
    }
}
