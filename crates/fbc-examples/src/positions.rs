//! The position check of the samples that can fill (FBC-hpg6, the owner's condition in decision
//! 0094: positions follow fills). Included by path by `testnet_trade` (whose round trip
//! `mainnet_smoke` runs) and `testnet_quote`, and by their tests, beside
//! [`link`](crate::link) and [`testnet`](crate::testnet); it is not a module of this crate's
//! library target, which stays empty.
//!
//! A session resyncs once per connection, so a session with fills and no reconnect never reads
//! `GET /positions` after a fill. The samples therefore read it themselves, through
//! [`PositionCheck`]: after the first fill the session sees, and at Stop once Stop's cancels
//! are answered (every run, fills or not: one REST read). Each read is the Paradex adapter's own
//! `positions_request` sent by the REST reader of `src/auth_rest.rs` (its own login, the adapter's
//! signed `POST /auth`; a review path, used here unchanged), charged to the same rate limiter
//! the run's order-entry session charges, and decoded by the adapter's own resync decoder
//! (`decode_resync`: signed size, LONG/SHORT, a CLOSED position at size 0), so the size read is
//! the one a resync would seed. The market's size, in lots, is compared exactly with the
//! registry's position for it, read as the answer lands.
//!
//! **A mismatch** (or a read that failed) is read once more after the step timeout, and both
//! readings are printed with their times since the start, the venue's size, the registry's and
//! the fills applied, so `GET /positions` lagging the fill stream (FBC-8xr's runs saw the REST
//! snapshot trail the stream by up to 3.5 s) is told apart from a real disagreement (RB155-2 on
//! PR #155). A re-read that agrees is printed and the run goes on ([`Verdict::Cleared`]); one
//! that still disagrees ([`Verdict::Persists`]) Stops the run (kill switch + cancel all,
//! nothing placed after) and fails it, saying to check the position by hand ([`HAND_CHECK`]).
//! What Stops it is each sample's: this module reads, compares and prints.
//!
//! **Other markets** ([`PositionCheck::elsewhere`], `testnet_quote --mainnet`, FBC-tyft): before
//! the order socket connects, one `GET /positions` read lists the account's open positions on
//! every market but the run's ([`other_markets`]); one there breaks `--sole-trader`'s premise,
//! and the run is refused (Codex r4277631493 on PR #149). The adapter's decoder is not used for
//! it: it knows only the run's market, and refuses an answer naming another.
//!
//! Nothing here prints, logs or keeps a credential: the reader's errors name the request and
//! its status only, and an answer's body is decoded, never shown.

use std::time::{Duration, Instant};

use fbc_core::{
    ExecEvent, Secrets, SignedLots, SpecTable, VenueCaps, VenueConfig, WallNs, dispatch_market_data,
};
use fbc_runtime::{Connector, RateLimiter};
use fbc_venue_paradex::exec::{decode_resync, positions_request};

use crate::auth_rest::{RestAnswer, RestReader};
use crate::link::Note;
use crate::testnet::{INST, Lines};

/// What a run that Stops on a persisting mismatch says to do.
pub const HAND_CHECK: &str = "check the position by hand";

/// One reading: when (milliseconds since the start), what `GET /positions` said of the market
/// (in lots, or why it could not be read), and what the registry held as the answer landed:
/// its position and how many fills of ours it had applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reading {
    pub at_ms: u128,
    pub venue: Result<SignedLots, String>,
    pub registry: SignedLots,
    pub fills: usize,
}

impl Reading {
    /// Whether the venue's size was read and is exactly the registry's.
    pub fn agrees(&self) -> bool {
        self.venue.as_ref().is_ok_and(|v| *v == self.registry)
    }

    /// The reading as the run prints it.
    pub fn shown(&self) -> String {
        let venue = match &self.venue {
            Ok(lots) => format!("GET /positions {} lots", lots.0),
            Err(why) => format!("GET /positions unread ({why})"),
        };
        format!(
            "+{}ms {venue}, the registry {} lots ({} fills applied)",
            self.at_ms, self.registry.0, self.fills
        )
    }
}

/// What a check came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The first reading agreed.
    Agrees(Reading),
    /// The first reading disagreed (or was unread) and the re-read after the step timeout
    /// agreed: `GET /positions` lagged.
    Cleared(Reading, Reading),
    /// Both readings disagreed (or were unread): the run Stops and fails.
    Persists(Reading, Reading),
}

impl Verdict {
    /// Whether the run must Stop and fail.
    pub fn persists(&self) -> bool {
        matches!(self, Verdict::Persists(..))
    }

    /// Why the run Stops, naming both readings, when it must.
    pub fn failure(&self, when: &str) -> Option<String> {
        match self {
            Verdict::Persists(first, second) => Some(format!(
                "GET /positions disagreed with the registry's position {when} and again on the \
                 re-read after the step timeout (first {}; re-read {}): {HAND_CHECK}",
                first.shown(),
                second.shown()
            )),
            _ => None,
        }
    }
}

/// How many fills of ours `notes` show applied to the inventory.
pub fn fills_in(notes: &[Note]) -> usize {
    notes
        .iter()
        .filter(|n| matches!(n, Note::Fill { ours: true, .. }))
        .count()
}

/// Whether `notes` hold a fill of any kind: the session saw one.
pub fn fill_seen(notes: &[Note]) -> bool {
    notes.iter().any(|n| matches!(n, Note::Fill { .. }))
}

/// The market's position as a `GET /positions` answer `body` states it, decoded by the Paradex
/// adapter's resync decoder: the size of [`INST`]'s position in lots (0 when it lists none, or
/// a CLOSED one), or why the answer is refused (naming the part, never the body).
pub fn market_position(
    body: &[u8],
    caps: &VenueCaps,
    specs: &SpecTable,
) -> Result<SignedLots, String> {
    let decoded = dispatch_market_data(caps, |scope| {
        decode_resync(WallNs(0), br#"{"results":[]}"#, body, scope, specs)
    })
    .map_err(|e| format!("its answer: {e}"))?;
    Ok(decoded
        .events()
        .iter()
        .find_map(|(_, ev)| match ev {
            ExecEvent::ResyncPosition { inst, qty, .. } if *inst == INST => Some(*qty),
            _ => None,
        })
        .unwrap_or(SignedLots(0)))
}

/// The open positions of a nonzero size on markets other than `market` that a `GET /positions`
/// answer `body` lists, each as `<market> <size>` (the market named only when shaped like a
/// Paradex perpetual, [`crate::testnet::symbol`], the size a decimal), or why the answer is
/// refused (naming the part, never the body).
pub fn other_markets(body: &[u8], market: &str) -> Result<Vec<String>, String> {
    let answer: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "GET /positions: not a JSON answer".to_owned())?;
    let results = answer["results"]
        .as_array()
        .ok_or("GET /positions: no results list")?;
    let mut held = Vec::new();
    for position in results {
        let name = position["market"]
            .as_str()
            .ok_or("GET /positions: a position with no market")?;
        if name == market {
            continue;
        }
        let size = position["size"]
            .as_str()
            .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
            .ok_or("GET /positions: a position with no decimal size")?;
        if size.is_zero() {
            continue;
        }
        let shown = if crate::testnet::symbol(name).is_ok() {
            name
        } else {
            "(a market not shown: not shaped like a Paradex perpetual)"
        };
        held.push(format!("{shown} {size}"));
    }
    Ok(held)
}

/// Reads `GET /positions` and compares the market's size with the registry's (module
/// documentation).
pub struct PositionCheck {
    rest: RestReader,
    /// The limiter the reader charges each request to, for the connections they open
    /// ([`PositionCheck::admit_connections`]).
    limiter: RateLimiter,
    rest_url: String,
    specs: SpecTable,
    caps: VenueCaps,
    /// The step timeout: how long a read may take, and how long after a mismatch the re-read
    /// comes.
    wait: Duration,
}

impl PositionCheck {
    /// A check of the market in `specs` on the account `creds` hold, under `cfg` (the session's
    /// Paradex configuration: its REST base, chain id and timings) and the venue's `caps`,
    /// reading through `connector` and charging `limiter` (the session's), each read bounded by
    /// `wait` and a mismatch re-read after it. Refused naming a missing or invalid key, never a
    /// value.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &VenueConfig,
        creds: Secrets,
        connector: Connector,
        limiter: RateLimiter,
        rest_url: &str,
        specs: SpecTable,
        caps: VenueCaps,
        wait: Duration,
    ) -> Result<PositionCheck, String> {
        let rest = RestReader::new(cfg, creds, connector, limiter.clone())
            .map_err(|e| format!("the position check: {e}"))?;
        Ok(PositionCheck {
            rest,
            limiter,
            rest_url: rest_url.to_owned(),
            specs,
            caps,
            wait,
        })
    }

    /// The account's open positions on every market but the check's own, as `GET /positions`
    /// lists them now ([`other_markets`]), or why they could not be read.
    pub async fn elsewhere(&mut self) -> Result<Vec<String>, String> {
        let by = Instant::now() + self.wait;
        self.admit_connections(by).await?;
        let answer = self.rest.get(positions_request(&self.rest_url), by).await?;
        let market = self
            .specs
            .get(INST)
            .ok_or("the market is not in the spec table")?
            .venue_symbol
            .as_wire()
            .to_owned();
        match answer {
            RestAnswer {
                status: 200, body, ..
            } => other_markets(&body, &market),
            RestAnswer { status, .. } => Err(format!("GET /positions: status {status}")),
        }
    }

    /// The market's size as `GET /positions` states it now, or why it could not be read.
    pub async fn venue_position(&mut self) -> Result<SignedLots, String> {
        let by = Instant::now() + self.wait;
        self.admit_connections(by).await?;
        let answer = self.rest.get(positions_request(&self.rest_url), by).await?;
        match answer {
            RestAnswer {
                status: 200, body, ..
            } => market_position(&body, &self.caps, &self.specs),
            RestAnswer { status, .. } => Err(format!("status {status}")),
        }
    }

    /// Charges the connections a read opens to the limiter (`Connect`), before the reader sends
    /// it: each of the reader's requests (its login, when its token is stale, and the GET) opens
    /// one, and the reader charges only the request, so both are charged here, the login's
    /// whether or not it comes (Codex r4306449950 on PR #158). `src/auth_rest.rs`, a review
    /// path, is used unchanged.
    async fn admit_connections(&self, by: Instant) -> Result<(), String> {
        let by = tokio::time::Instant::from_std(by);
        let connects = [fbc_core::OpKind::Connect, fbc_core::OpKind::Connect];
        crate::testnet::admit_ops(&self.limiter, &connects, by, "GET /positions").await
    }

    /// One reading: `GET /positions`, then `registry` (the registry's position and the fills
    /// of ours applied) as the answer landed.
    async fn reading(
        &mut self,
        lines: &Lines,
        registry: &impl Fn() -> (SignedLots, usize),
    ) -> Reading {
        let venue = self.venue_position().await;
        let (lots, fills) = registry();
        Reading {
            at_ms: lines.t0.elapsed().as_millis(),
            venue,
            registry: lots,
            fills,
        }
    }

    /// The check `when` (`after the first fill`, `at Stop`): a reading, and on a mismatch or an
    /// unread answer a second one after the step timeout, each printed on a `POSITION` line.
    pub async fn check(
        &mut self,
        when: &str,
        lines: &Lines,
        registry: impl Fn() -> (SignedLots, usize),
    ) -> Verdict {
        let first = self.reading(lines, &registry).await;
        if first.agrees() {
            lines.line(format_args!(
                "POSITION {when}: {}: they agree",
                first.shown()
            ));
            return Verdict::Agrees(first);
        }
        lines.line(format_args!(
            "POSITION {when}: {}: they DIFFER; GET /positions is read once more in {} s (it may \
             lag the fills)",
            first.shown(),
            self.wait.as_secs()
        ));
        tokio::time::sleep(self.wait).await;
        let second = self.reading(lines, &registry).await;
        if second.agrees() {
            lines.line(format_args!(
                "POSITION re-read {when}: {}: they agree now (GET /positions lagged the fills; \
                 first {}); the run goes on",
                second.shown(),
                first.shown()
            ));
            Verdict::Cleared(first, second)
        } else {
            lines.line(format_args!(
                "POSITION re-read {when}: {}: they STILL DIFFER (first {}): the run fails; \
                 {HAND_CHECK}",
                second.shown(),
                first.shown()
            ));
            Verdict::Persists(first, second)
        }
    }
}
