//! The place stages' preflight (FBC-c6kw, the first slice of FBC-9sk2; decision 0148): what a
//! place-stage run reads and checks before any account connects for a place stage. Nothing here
//! authenticates, places or reads an account.
//!
//! - **The spec** ([`discover_spec`]): the traded instrument comes from this run's discovery,
//!   the adapter's `Qfex::discover` on the configuration the probe's guard admitted (decision
//!   0148 item 4), its `qfex.rest_timeout` from `--read-wait-ms` ([`market_config`]): one public
//!   `GET /refdata` on the guarded REST origin, charged to the run's shared limiter and sent
//!   through the run's connector within its timeout, as md_watch reads Hibachi's contracts. The
//!   draft `--symbol` names is resolved into the run's one [`SpecTable`]. A symbol the answer
//!   does not list, one QFEX does not list as trading, and one whose order shapes do not list both
//!   a post-only GTC and a limit IOC, the two shapes decision 0148 item 4 lets a place take, are
//!   refused, and no refusal names the symbol (it is never written to the report or the
//!   evidence).
//! - **The count bound** ([`count_bound`]): `--ledger-max-entries` above each place registry's
//!   fill ceiling (`ledger::PlaceBounds`), which only the instrument's minimum size gives, so it
//!   is checked after the `/refdata` read and before any account connects (FBC-fqv5 item 3). Its
//!   refusal shows no ceiling, since a ceiling counts `--flatten-attempts` (FBC-fqv5 item 10).
//! - **The touch** ([`Touch`]): fbc-runtime's `MdVenue` on the guarded configuration's MDS
//!   origin with the discovered spec table, subscribed to the instrument's `bbo` alone, with a
//!   rate limiter of its own (QFEX's market data declares no limit), one connection attempt (as
//!   each session's) and a silence window of its own that no touch's age sets. [`Touch::now`]
//!   refuses a touch from before the MDS connection's current epoch, one with no bid or no ask, a
//!   crossed or locked one, and one older than `--touch-max-age-ms` ([`TouchState::judge`]): its
//!   age is the longer of the time since it arrived and the time since QFEX stamped it, so a
//!   touch QFEX stamped long ago is old however lately it arrived, and a clock that runs ahead of
//!   QFEX's only refuses more.
//! - **The prices** ([`rest_price`], [`far_touch`], [`flatten_price`]), pure: a resting order
//!   `--away-bps` behind the touch on `--side`; the crossing post-only and the taker IOC at the
//!   far touch; a flatten IOC at most `--flatten-cap-bps` through the touch it crosses (the bid
//!   for a sell, the ask for a buy). Each is moved in whole ticks, exactly, and snapped to the
//!   grid on its safe side as `InstrumentSpec::quantize` snaps, flooring a buy and ceiling a
//!   sell, so a resting order is never nearer the touch, and a flatten never further through it,
//!   than asked, however many ticks the price spans. Only the resting price is taken now, for
//!   the caps; the driver prices the others (FBC-23ch).
//! - **The caps** ([`place_caps`]): each of `--resting-cap-usd` and `--inventory-cap-usd` in lots
//!   with `testnet::cap_lots`, at `testnet::inventory_price` of the resting price and the touch,
//!   the highest price any order of the stages takes, and refused below three of the
//!   instrument's minimum size: stage three rests three (decision 0148 item 7), each at the
//!   minimum (item 4).
//!
//! The report and the evidence say which of these the run passed ([`Preflight`]), never a value:
//! no cap, distance, flatten cap or attempt count, and no symbol (FBC-fqv5 item 10).

use std::cell::RefCell;
use std::fmt::Write as _;
use std::rc::Rc;
use std::time::Duration;

use fbc_core::{
    AliasTable, ConnKey, Effect, Envelope, ExchNs, Feed, HttpAnswer, HttpMethod, HttpPlan,
    HttpResponse, HttpTag, InstrumentId, InstrumentResolver, InstrumentSpec, InstrumentSpecDraft,
    Listing, Lots, Lvl, MdEvent, OrderKindTag, Side, SpecTable, Subscription, Ticks, TifTag,
    TradingStatus, UnderlyingId, VenueCaps, VenueConfig, VenueFactory, VenueId, WallNs,
    dispatch_market_data,
};
use fbc_oms::{MarketCapsConfig, PreTradeCaps};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdHandler, MdVenue, MdVenueConfig, MdVenueControl,
    RateLimiter, ReconnectPacing, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::{KEY_REST_TIMEOUT, Qfex, TOUCH_BBO};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ledger::{Bounds, PlaceBounds, PlaceLists};
use crate::probe_args::Places;
use crate::testnet;

/// The venue, for the touch's `&'static dyn VenueFactory`.
static QFEX: Qfex = Qfex;

/// The venue id the run's spec table is under: the program's own numbering.
pub const VENUE: VenueId = VenueId::new(1);

/// The traded instrument's id in the run's spec table: the program's own numbering, one market
/// per run.
pub const INST: InstrumentId = InstrumentId::new(1);

/// The connection a discovery request is charged under: QFEX's buckets are per user, and an
/// HTTP request is on no connection.
const NO_CONN: ConnKey = ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// The touch's connection number: one, apart from every session's (numbered from 0 up).
pub const MDS_CONN: u16 = u16::MAX - 1;

/// The longest the touch's connection may hear nothing before the runtime calls it silent: its
/// liveness alone, never a touch's age (`--touch-max-age-ms`).
pub const MDS_SILENCE: Duration = Duration::from_secs(60);

/// The reconnect pacing's floor, ceiling and window: one connection attempt a day, as each
/// session's, so a touch connection that ends leaves no touch current.
const DAY: Duration = Duration::from_secs(86_400);

/// A `/refdata` answer lists every QFEX instrument: 1 MiB is far above it.
const MAX_BODY: usize = 1 << 20;

/// What every refusal of the spec's read starts with.
const WHAT: &str = "GET /refdata";

/// What a request of the spec's read was answered with: its tag, the status and the body.
pub type Answer = (HttpTag, u16, Bytes);

/// The traded instrument, as this run's discovery gave it.
#[derive(Clone, Debug)]
pub struct Discovered {
    /// The run's spec table, holding the instrument alone, which every session's takes.
    pub specs: SpecTable,
    /// Its id there ([`INST`]).
    pub inst: InstrumentId,
    pub spec: InstrumentSpec,
    /// Its minimum size, in size steps: the size of every place (decision 0148 item 4).
    pub min_size: Lots,
    /// Its finest price step, which a price in ticks is a count of.
    pub tick: Decimal,
}

impl Discovered {
    /// The price `ticks` names.
    pub fn px(&self, ticks: Ticks) -> Decimal {
        Decimal::from(ticks.0) * self.tick
    }
}

/// `cfg`, which the probe's guard admitted, with its public REST read's timeout set from
/// `--read-wait-ms`: the configuration discovery and the touch take.
pub fn market_config(cfg: &VenueConfig, read_wait: Duration) -> VenueConfig {
    let mut cfg = cfg.clone();
    cfg.insert(KEY_REST_TIMEOUT, &format!("{}ms", read_wait.as_millis()));
    cfg
}

/// The instrument `symbol` names, from this run's `GET /refdata` (the adapter's discovery plan on
/// `cfg`), each request charged to `limiter` and sent through `connector` ([`fetch`]), its
/// answer resolved by [`resolve`].
pub async fn discover_spec(
    cfg: &VenueConfig,
    symbol: &str,
    connector: &Connector,
    limiter: &RateLimiter,
) -> Result<Discovered, String> {
    let plan = QFEX.discover(cfg).map_err(|e| format!("{WHAT}: {e}"))?;
    let caps = QFEX.caps(cfg).map_err(|e| format!("{WHAT}: {e}"))?;
    let answers = fetch(plan.requests(), connector, limiter).await?;
    resolve(plan, &caps, &answers, symbol)
}

/// Makes a discovery plan's `requests`, in order, each charged to `limiter` and then sent through
/// `connector` within its timeout. Only a bare public GET is made: a request with a header, a body
/// or another method is refused, as is one `limiter` has no room for, before it is charged or
/// sent.
pub async fn fetch(
    requests: &[Effect],
    connector: &Connector,
    limiter: &RateLimiter,
) -> Result<Vec<Answer>, String> {
    let mut answers = Vec::new();
    for effect in requests {
        let Effect::Http {
            tag, req, timeout, ..
        } = effect
        else {
            return Err(format!("{WHAT}: the plan asks for something not HTTP"));
        };
        let public = req.method == HttpMethod::Get && req.headers.is_empty();
        if !public || !req.body.bytes().is_empty() {
            return Err(format!(
                "{WHAT}: the plan asks for a request the probe does not make"
            ));
        }
        let charge = fbc_runtime::Request::of(effect).expect("an HTTP request is charged");
        let now = Instant::now();
        let charged = limiter.charge(now, NO_CONN, &[charge]);
        charged.map_err(|_| format!("{WHAT}: not sent: the rate limit has no room"))?;
        let request = Request::builder()
            .method(Method::GET)
            .uri(req.url.as_str())
            .body(Bytes::new())
            .map_err(|e| format!("{WHAT}: {e}"))?;
        let response = tokio::time::timeout(*timeout, connector.http(request, MAX_BODY))
            .await
            .map_err(|_| format!("{WHAT}: no answer within {} ms", timeout.as_millis()))?
            .map_err(|e| format!("{WHAT}: {e}"))?;
        answers.push((*tag, response.status().as_u16(), response.into_body()));
    }
    Ok(answers)
}

/// The instrument `symbol` names in `plan`'s `answers`: parsed in a decode scope of `caps`, and
/// its draft resolved into [`INST`]'s spec in a spec table of its own. Refused, never naming the
/// symbol, when an answer is refused, the symbol is not listed, QFEX does not list it as
/// trading, or its order shapes do not list both a post-only GTC and a limit IOC (decision 0148
/// item 4).
pub fn resolve(
    plan: HttpPlan<Vec<InstrumentSpecDraft>>,
    caps: &VenueCaps,
    answers: &[Answer],
    symbol: &str,
) -> Result<Discovered, String> {
    let answers: Vec<HttpAnswer<'_>> = (answers.iter())
        .map(|(tag, status, body)| {
            let response = HttpResponse {
                status: *status,
                headers: &[],
                body,
            };
            (*tag, Ok(response))
        })
        .collect();
    let drafts = dispatch_market_data(caps, |scope| plan.parse(&answers, scope))
        .map_err(|e| format!("{WHAT}: {e}"))?
        .done()
        .ok_or_else(|| format!("{WHAT}: the answer asked for another round"))?;
    let draft = (drafts.into_iter())
        .find(|d| d.venue_symbol.as_wire() == symbol)
        .ok_or_else(|| {
            format!(
                "{WHAT}: the instrument --symbol names is not listed in this run's answer (the \
                 symbol is not shown)"
            )
        })?;
    if draft.status != TradingStatus::Trading {
        return Err(format!(
            "{WHAT}: the instrument --symbol names is listed {:?}, not trading (the symbol is \
             not shown)",
            draft.status
        ));
    }
    let both = draft.order_shapes.is_some_and(|shapes| {
        shapes.lists(OrderKindTag::Limit, true, TifTag::Gtc)
            && shapes.lists(OrderKindTag::Limit, false, TifTag::Ioc)
    });
    if !both {
        return Err(format!(
            "{WHAT}: the instrument --symbol names does not list both a post-only GTC and a \
             limit IOC, the two shapes a place stage's place takes (decision 0148 item 4; the \
             symbol is not shown)"
        ));
    }
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let listing = Listing {
        id: INST,
        underlying: UnderlyingId::new(INST.get()),
    };
    let unresolved = |_| format!("{WHAT}: the instrument --symbol names could not be resolved");
    resolver
        .list(VENUE, draft.asset, listing)
        .map_err(unresolved)?;
    let spec = (resolver.spec(VENUE, draft, 1, testnet::wall_now())).map_err(unresolved)?;
    let mut specs = SpecTable::new();
    specs.insert(spec.clone());
    Ok(Discovered {
        specs,
        inst: INST,
        min_size: spec.min_size,
        tick: spec.price_grid.finest(),
        spec,
    })
}

/// The place registries' ledger bounds: the command line's `parsed`, its count bound held above
/// each place registry's fill ceiling, every order of its list at `found`'s minimum size with
/// `--flatten-attempts` of `places` (`ledger::PlaceBounds::accept`; FBC-fqv5 item 3). Refused
/// naming `--ledger-max-entries` and showing no ceiling, since a ceiling counts the attempts
/// (FBC-fqv5 item 10).
pub fn count_bound(
    parsed: &Bounds,
    found: &Discovered,
    places: &Places,
) -> Result<PlaceBounds, String> {
    let lists = PlaceLists::new(found.min_size, places.flatten_attempts)?;
    PlaceBounds::accept(parsed, &lists).map_err(|_| {
        "--ledger-max-entries must be above each place registry's fill ceiling, every order its \
         list may place counted at the instrument's minimum size in size steps (decision 0132 \
         item 5, FBC-fqv5 item 3; no value is shown)"
            .to_owned()
    })
}

/// A touch, both sides present and not crossed or locked.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Quote {
    pub bid: Ticks,
    pub ask: Ticks,
}

/// The last touch the MDS connection gave.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Seen {
    bid: Option<Lvl>,
    ask: Option<Lvl>,
    /// When it arrived, on the runtime's clock.
    received: Instant,
    /// When QFEX stamped it, when it did.
    stamped: Option<ExchNs>,
    /// The connection epoch it arrived on.
    conn: ConnKey,
}

/// What the touch's connection gave: its last touch, and the epoch now running, if one is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TouchState {
    last: Option<Seen>,
    live: Option<ConnKey>,
}

/// Why no price is taken from the touch.
pub const NO_TOUCH: &str = "the MDS touch: none arrived on the MDS connection";
pub const BEFORE_EPOCH: &str =
    "the MDS touch: the last one is from before the MDS connection's current epoch";
pub const ONE_SIDED: &str = "the MDS touch: it has no bid or no ask";
pub const CROSSED: &str = "the MDS touch: it is crossed or locked";
pub const STALE: &str =
    "the MDS touch: it is older than --touch-max-age-ms (the value is not shown)";

impl TouchState {
    /// Records `env`, when it is a `bbo` touch of `inst`, as the last touch, which arrived `now`
    /// on the epoch now running: `true` when it is one.
    pub fn heard(&mut self, env: &Envelope<MdEvent>, inst: InstrumentId, now: Instant) -> bool {
        let MdEvent::Touch {
            inst: of,
            bid,
            ask,
            source,
        } = env.body
        else {
            return false;
        };
        let ours = of == inst && source == TOUCH_BBO;
        if ours {
            self.live = Some(env.stamp.conn);
            self.last = Some(Seen {
                bid,
                ask,
                received: now,
                stamped: env.exch_ts,
                conn: env.stamp.conn,
            });
        }
        ours
    }

    /// The epoch `key` ended: no touch is current until the next epoch's first.
    pub fn ended(&mut self, key: ConnKey) {
        self.live = self.live.filter(|live| *live != key);
    }

    /// Whether a touch of the epoch now running has arrived.
    pub fn current(&self) -> bool {
        self.last.is_some_and(|seen| Some(seen.conn) == self.live)
    }

    /// The touch to price from at `now` on the runtime's clock, `wall` on the local wall clock:
    /// refused when none arrived, when the last is from before the current epoch, when it has no
    /// bid or no ask, when it is crossed or locked, and when it is older than `max_age`, by the
    /// longer of the time since it arrived and the time since QFEX stamped it.
    pub fn judge(&self, now: Instant, wall: WallNs, max_age: Duration) -> Result<Quote, String> {
        let seen = self.last.ok_or(NO_TOUCH)?;
        if Some(seen.conn) != self.live {
            return Err(BEFORE_EPOCH.to_owned());
        }
        let (Some(bid), Some(ask)) = (seen.bid, seen.ask) else {
            return Err(ONE_SIDED.to_owned());
        };
        if bid.px >= ask.px {
            return Err(CROSSED.to_owned());
        }
        let held = now.saturating_duration_since(seen.received);
        let since = |at: ExchNs| u64::try_from(wall.0.saturating_sub(at.0)).unwrap_or(0);
        let stamped = Duration::from_nanos(seen.stamped.map_or(0, since));
        if held.max(stamped) > max_age {
            return Err(STALE.to_owned());
        }
        Ok(Quote {
            bid: bid.px,
            ask: ask.px,
        })
    }
}

/// The touch's handler: each touch of the instrument into the shared [`TouchState`], each waking
/// [`Touch::first`].
pub struct TouchWatch {
    pub inst: InstrumentId,
    pub state: Rc<RefCell<TouchState>>,
    pub wake: Rc<Notify>,
}

impl MdHandler for TouchWatch {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        if self
            .state
            .borrow_mut()
            .heard(&env, self.inst, Instant::now())
        {
            self.wake.notify_one();
        }
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.state.borrow_mut().ended(key);
        self.wake.notify_one();
    }
}

/// The MDS touch of the traded instrument ([`Touch::open`]).
pub struct Touch {
    venue: MdVenue<TouchWatch>,
    /// Kept so the venue runs: dropping it stops the touch's connection.
    _control: MdVenueControl,
    state: Rc<RefCell<TouchState>>,
    wake: Rc<Notify>,
}

impl Touch {
    /// The touch of `found`'s instrument on `cfg`'s MDS origin, which the probe's guard admitted
    /// (the adapter's `plan_md`), through `connector`, its events stamped on the run's `clock`.
    /// It opens its connection, within `connect_wait`, once it is driven ([`Touch::first`]).
    pub fn open(
        cfg: &VenueConfig,
        found: &Discovered,
        connector: &Connector,
        clock: &IngestClock,
        connect_wait: Duration,
    ) -> Result<Touch, String> {
        let fail = |e: &dyn std::fmt::Display| format!("the MDS touch: {e}");
        let caps = QFEX.caps(cfg).map_err(|e| fail(&e))?;
        let reserve = SafetyReserve::percent(10).expect("10% is a reserve");
        let limiter = RateLimiter::new(&caps.limits, reserve).map_err(|e| fail(&e))?;
        let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, connect_wait);
        let liveness = Liveness::new(MDS_SILENCE, Duration::from_secs(1));
        let config = MdVenueConfig {
            venue: &QFEX,
            cfg: cfg.clone(),
            specs: found.specs.clone(),
            connector: connector.clone(),
            pacing: pacing.expect("one attempt a day, within --connect-wait-ms"),
            clock: clock.clone(),
            http_max_body: MAX_BODY,
            conns: MDS_CONN..MDS_CONN + 1,
            limiter,
            liveness: liveness.expect("a silence window and a rotation margin"),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
        };
        let state = Rc::new(RefCell::new(TouchState::default()));
        let wake = Rc::new(Notify::new());
        let watch = TouchWatch {
            inst: found.inst,
            state: Rc::clone(&state),
            wake: Rc::clone(&wake),
        };
        let (venue, control) = MdVenue::new(config, watch).map_err(|e| fail(&e))?;
        let bbo = Subscription {
            inst: found.inst,
            feed: Feed::Touch(TOUCH_BBO),
        };
        control.set_desired([bbo]).map_err(|e| fail(&e))?;
        Ok(Touch {
            venue,
            _control: control,
            state,
            wake,
        })
    }

    /// Drives the touch's connection until a touch of its current epoch has arrived, or for
    /// `within` at most, then judges the touch as [`Touch::now`] does.
    pub async fn first(&mut self, within: Duration, max_age: Duration) -> Result<Quote, String> {
        let deadline = Instant::now() + within;
        let (state, wake) = (&self.state, &self.wake);
        let heard = async {
            while !state.borrow().current() {
                wake.notified().await;
            }
        };
        // The venue runs until its control drops, which the touch keeps: it ends only with an
        // error, after which no touch arrives and the judgement below says so.
        tokio::select! {
            _ = self.venue.run() => {}
            () = heard => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
        self.now(max_age)
    }

    /// The touch to price from now ([`TouchState::judge`]).
    pub fn now(&self, max_age: Duration) -> Result<Quote, String> {
        let state = self.state.borrow();
        state.judge(Instant::now(), testnet::wall_now(), max_age)
    }
}

/// Basis points in one.
const BPS: i128 = 10_000;

/// `ticks` moved `bps` up (`up`) or down, in whole ticks of the finest grid on `side`'s safe
/// side: ceiled for a sell and floored for a buy, exactly, in `i128`, so no float rounds a
/// fraction of a tick away however many ticks the price spans.
fn moved(ticks: Ticks, bps: u32, up: bool, side: Side) -> i128 {
    let factor = match up {
        true => BPS + i128::from(bps),
        false => BPS - i128::from(bps),
    };
    let scaled = i128::from(ticks.0) * factor;
    match side {
        Side::Buy => scaled.div_euclid(BPS),
        Side::Sell => -(-scaled).div_euclid(BPS),
    }
}

/// Why no price is taken: none positive on the grid on the side's safe side, within an `i64`.
const NO_PRICE: &str = "the price: no positive price on the grid on its safe side";

/// `ticks`, a count of the finest grid's ticks, snapped onto `spec`'s grid on `side`'s safe side
/// as `InstrumentSpec::quantize` snaps (a buy floors and a sell ceils, onto the grid valid at
/// the result), refused when no positive price on the grid is there.
fn snapped(spec: &InstrumentSpec, side: Side, ticks: i128) -> Result<Ticks, String> {
    let ticks = Ticks(i64::try_from(ticks).map_err(|_| NO_PRICE.to_owned())?);
    let grid = &spec.price_grid;
    let on = match side {
        Side::Buy => grid.floor_valid(ticks),
        Side::Sell => grid.ceil_valid(ticks),
    };
    on.filter(|on| on.0 > 0).ok_or_else(|| NO_PRICE.to_owned())
}

/// A resting order's price on `side`, `away_bps` behind `quote` (below the bid for a buy, above
/// the ask for a sell), snapped further from the touch.
pub fn rest_price(
    spec: &InstrumentSpec,
    side: Side,
    quote: Quote,
    away_bps: u32,
) -> Result<Ticks, String> {
    match side {
        Side::Buy => snapped(spec, side, moved(quote.bid, away_bps, false, side)),
        Side::Sell => snapped(spec, side, moved(quote.ask, away_bps, true, side)),
    }
}

/// A crossing order's price on `side`, the crossing post-only's and the taker IOC's: the far
/// touch (the ask for a buy, the bid for a sell).
#[allow(dead_code)] // the driver prices the crossing post-only and the taker IOC (FBC-23ch)
pub fn far_touch(spec: &InstrumentSpec, side: Side, quote: Quote) -> Result<Ticks, String> {
    match side {
        Side::Buy => snapped(spec, side, i128::from(quote.ask.0)),
        Side::Sell => snapped(spec, side, i128::from(quote.bid.0)),
    }
}

/// A flatten IOC's price on `side`, the other of `--side`'s: at most `cap_bps` through the touch
/// it crosses, measured from that touch (the bid for a sell, the ask for a buy), and snapped
/// back toward it, so it is never further through than the cap.
#[allow(dead_code)] // the driver prices each flatten IOC (FBC-23ch)
pub fn flatten_price(
    spec: &InstrumentSpec,
    side: Side,
    quote: Quote,
    cap_bps: u32,
) -> Result<Ticks, String> {
    match side {
        Side::Sell => snapped(spec, side, moved(quote.bid, cap_bps, false, side)),
        Side::Buy => snapped(spec, side, moved(quote.ask, cap_bps, true, side)),
    }
}

/// The two caps in lots.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PlaceCaps {
    pub resting: Lots,
    pub inventory: Lots,
}

/// `places`' two caps in lots of `found`'s size step (`testnet::cap_lots`), each at
/// `testnet::inventory_price` of the resting price on `--side` and `quote`'s two sides, the
/// highest price any order of the stages takes. Refused, naming the flag and never a value, when
/// either is below three of the instrument's minimum size: stage three rests three, each at the
/// minimum (decision 0148 items 4 and 7).
pub fn place_caps(places: &Places, found: &Discovered, quote: Quote) -> Result<PlaceCaps, String> {
    let rest = rest_price(&found.spec, places.side, quote, places.away_bps)?;
    let (bid, ask) = (Some(found.px(quote.bid)), Some(found.px(quote.ask)));
    let at = testnet::inventory_price(found.px(rest), bid, ask);
    let step = found.spec.size_step.get();
    let three = found.min_size.get().checked_mul(3).and_then(Lots::new);
    let lots = |usd| testnet::cap_lots(usd, at, step);
    let (resting, inventory) = (lots(places.resting_cap_usd), lots(places.inventory_cap_usd));
    for (flag, cap) in [
        ("--resting-cap-usd", resting),
        ("--inventory-cap-usd", inventory),
    ] {
        if three.is_none_or(|three| cap < three) {
            return Err(format!(
                "{flag} admits fewer than three of the instrument's minimum size at the touch: \
                 stage three rests three (decision 0148 item 7; the values are not shown)"
            ));
        }
    }
    Ok(PlaceCaps { resting, inventory })
}

/// The pre-trade caps of a place registry on `inst`: `caps` both ways.
#[allow(dead_code)] // the driver builds each place registry with them (FBC-23ch)
pub fn pre_trade_caps(inst: InstrumentId, caps: PlaceCaps) -> PreTradeCaps {
    let config = MarketCapsConfig {
        inventory: Some(caps.inventory),
        resting: Some(caps.resting),
    };
    let built = PreTradeCaps::new().with_market(inst, config);
    built.expect("both caps are stated")
}

/// The spec came from this run's discovery: one of [`Preflight::passed`].
pub const DISCOVERED: &str = "the instrument's spec from this run's GET /refdata on the guarded \
     REST origin: listed, trading, and listing both a post-only GTC and a limit IOC (decision \
     0148 item 4)";
/// The count bound cleared each place registry's fill ceiling.
pub const COUNTED: &str = "--ledger-max-entries above each place registry's fill ceiling \
     (FBC-fqv5 item 3)";
/// A touch of the MDS connection's current epoch, fresh and not crossed.
pub const TOUCHED: &str = "a touch from the guarded MDS origin, of the connection's current \
     epoch, with both sides, not crossed or locked, and no older than --touch-max-age-ms";
/// Each cap admits three minimum sizes at the touch.
pub const CAPPED: &str = "each cap at least three of the instrument's minimum size at the touch \
     (decisions 0113 and 0148 item 7)";

/// What the place stages' preflight passed, in order, for the report and the evidence: never a
/// value (FBC-fqv5 item 10).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Preflight {
    pub passed: Vec<&'static str>,
}

/// The fences the command line met before anything connected, as the report names them: met,
/// never a value (FBC-fqv5 item 10).
const FENCES: &str = "--production; --sole-trader (decision 0148 item 7); --resting-cap-usd and \
     --inventory-cap-usd set and positive (decision 0113); --away-bps at least 20; \
     --flatten-cap-bps at least 1 and below --away-bps; --flatten-attempts 1 to 3; \
     --registry-span at most 3600 s and above the drop test's floor; at most 10 reads per \
     comparison; and every origin a loopback test stub's, until FBC-4plj";

/// What the place stages do not do yet, as the report says it.
const NOT_BUILT: &str = "The place stages after the preflight are not built yet (FBC-23ch and \
     the slices after it): nothing authenticated connected for a place stage, no account was read \
     for one, and nothing was placed";

/// The report's section on the place stages' preflight.
pub fn markdown(s: &mut String, pre: &Preflight) {
    let passed = match pre.passed.is_empty() {
        true => "none".to_owned(),
        false => pre.passed.join("; "),
    };
    let _ = writeln!(
        s,
        "## The place stages' preflight (FBC-c6kw)\n\n\
         Decision 0148: before any account connects for a place stage. Fences met before \
         anything connected, each as a fence and never a value: {FENCES}. The symbol traded \
         is not shown.\n\n\
         Passed, in order: {passed}.\n\n\
         {NOT_BUILT}.\n"
    );
}

/// The evidence's entry for the place stages' preflight.
pub fn to_json(pre: &Preflight) -> Value {
    json!({"fences_met": FENCES, "passed": pre.passed, "not_built": NOT_BUILT})
}
