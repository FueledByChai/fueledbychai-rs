//! What the status poll's tests share (FBC-1632, decision 0130): the spec table discovery reads
//! from `fixtures/hibachi/rest/status/live.json` (ETH/USDT-P, EUR/USDT-P and WIF/USDT-P), the
//! hand-built answers of that directory, the poll endpoint `plan_md` plans, and [`Sim`], which
//! runs the poll's codec as the runtime does (each timer at its due instant, each request held
//! until the test answers it) on a wall clock the test moves.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use fbc_core::{
    AliasTable, DecodeError, Effect, Effects, EndpointPlan, Feed, HttpFailure, HttpResponse,
    HttpTag, InstrumentId, InstrumentResolver, Listing, MdCodec, MdEvent, MdSink, MdTransport,
    MonoNs, SpecTable, Subscription, TimerTag, TradingStatus, UnderlyingId, VenueCaps, VenueConfig,
    VenueFactory, VenueId, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_hibachi::discover::EXCHANGE_INFO_TAG;
use fbc_venue_hibachi::{BOOK_ORDERBOOK, Hibachi, HibachiStatus, STATUS_STREAM};

pub const ETH: InstrumentId = InstrumentId::new(1);
pub const EUR: InstrumentId = InstrumentId::new(2);
pub const WIF: InstrumentId = InstrumentId::new(3);
pub const MARKETS: [InstrumentId; 3] = [ETH, EUR, WIF];

/// Nanoseconds per second.
pub const NS: i64 = 1_000_000_000;

/// 2026-10-21T12:00:00Z, a Wednesday: the transitions' and the closes' timeline.
pub const WEDNESDAY: i64 = 1_792_584_000;
/// 2026-10-23T20:50:00Z: ten minutes before the FX weekend closes (Friday 17:00 New York, EDT).
pub const FRIDAY: i64 = 1_792_788_600;
/// The FX weekend's close that Friday, and its reopening that Sunday (17:00 EDT).
pub const FX_CLOSE: i64 = 1_792_789_200;
pub const FX_REOPEN: i64 = 1_792_962_000;

/// The status interval and the window after a reopening the tests' configuration states
/// (`super::CONFIG`).
pub const INTERVAL: i64 = 60;
pub const AFTER: i64 = 120;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../fixtures/hibachi/rest/status")
}

/// The answer `fixtures/hibachi/rest/status/<name>.json`.
pub fn answer(name: &str) -> Vec<u8> {
    let path = fixtures().join(format!("{name}.json"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"))
}

/// The same, as JSON to change.
pub fn answer_json(name: &str) -> serde_json::Value {
    serde_json::from_slice(&answer(name)).unwrap()
}

/// ETH/USDT-P, EUR/USDT-P and WIF/USDT-P as discovery reads them from `live.json`.
pub fn specs() -> SpecTable {
    let body = answer("live");
    let cfg = super::config();
    let plan = Hibachi.discover(&cfg).unwrap();
    let caps = Hibachi.caps(&cfg).unwrap();
    let response = HttpResponse {
        status: 200,
        headers: &[],
        body: &body,
    };
    let drafts = dispatch_market_data(&caps, |scope| {
        plan.parse(&[(EXCHANGE_INFO_TAG, Ok(response))], scope)
    })
    .unwrap()
    .done()
    .unwrap();
    let venue = VenueId::new(6);
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let mut table = SpecTable::new();
    for (id, symbol) in [
        (ETH, "ETH/USDT-P"),
        (EUR, "EUR/USDT-P"),
        (WIF, "WIF/USDT-P"),
    ] {
        let draft = drafts
            .iter()
            .find(|d| d.venue_symbol.as_wire() == symbol)
            .unwrap()
            .clone();
        let listing = Listing {
            id,
            underlying: UnderlyingId::new(id.get()),
        };
        resolver.list(venue, draft.asset, listing).unwrap();
        table.insert(resolver.spec(venue, draft, 1, WallNs(0)).unwrap());
    }
    table
}

/// ETH's trades and orderbook, EUR's trades and WIF's mark: every market subscribed, ETH twice.
pub fn subs() -> BTreeSet<Subscription> {
    let sub = |inst, feed| Subscription { inst, feed };
    BTreeSet::from([
        sub(ETH, Feed::Trades),
        sub(ETH, Feed::Book(BOOK_ORDERBOOK)),
        sub(EUR, Feed::Trades),
        sub(WIF, Feed::Mark),
    ])
}

/// The status poll's endpoint as `plan_md` plans it for `subs` under `cfg`.
pub fn poll_plan(cfg: &VenueConfig, subs: &BTreeSet<Subscription>) -> EndpointPlan {
    let plans = Hibachi.plan_md(cfg, &specs(), subs).unwrap();
    let poll = plans
        .into_iter()
        .find(|p| p.stream == STATUS_STREAM)
        .unwrap();
    assert!(matches!(poll.transport, MdTransport::Poll { .. }));
    poll
}

/// What a codec reported, at the wall time it reported it.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Reported {
    pub at: i64,
    pub inst: InstrumentId,
    pub status: TradingStatus,
}

struct Kept<'a> {
    at: i64,
    out: &'a mut Vec<Reported>,
}

impl MdSink for Kept<'_> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        assert_eq!(meta, VenueMeta::NONE);
        let MdEvent::Status { inst, status } = ev else {
            panic!("the status poll pushed {ev:?}")
        };
        self.out.push(Reported {
            at: self.at,
            inst,
            status,
        });
    }
}

/// The poll's codec, run as the runtime runs it: every timer it asks for fires at its due
/// instant, in order (one asked for at once fires before the clock moves), and every request
/// waits for the test's answer. The wall clock starts at `start` and moves only by
/// [`Sim::advance_to`].
pub struct Sim {
    pub codec: HibachiStatus,
    pub specs: SpecTable,
    caps: VenueCaps,
    start: i64,
    /// The wall time now, in nanoseconds.
    pub now: i64,
    timers: BTreeMap<(i64, u64), TimerTag>,
    seq: u64,
    /// The requests asked for and not yet answered, oldest first, with when each was asked.
    pub requests: Vec<(HttpTag, i64)>,
    /// Every request asked for, with its URL.
    pub urls: Vec<String>,
    /// Every status it reported.
    pub reported: Vec<Reported>,
    /// The timers asked for, as (due, tag).
    pub armed: Vec<(i64, TimerTag)>,
}

impl Sim {
    /// The poll's codec under the tests' configuration, subscribed to [`subs`], at `start`
    /// (Unix seconds): `on_open`, the subscribe call, and the timers due at once.
    pub fn new(start: i64) -> Sim {
        Sim::with(&super::config(), &subs(), start)
    }

    pub fn with(cfg: &VenueConfig, subs: &BTreeSet<Subscription>, start: i64) -> Sim {
        let plan = poll_plan(cfg, subs);
        let specs = specs();
        let mut sim = Sim {
            codec: HibachiStatus::new(plan.stream, cfg),
            specs,
            caps: Hibachi.caps(cfg).unwrap(),
            start: start * NS,
            now: start * NS,
            timers: BTreeMap::new(),
            seq: 0,
            requests: Vec::new(),
            urls: Vec::new(),
            reported: Vec::new(),
            armed: Vec::new(),
        };
        let mut fx = Effects::new();
        sim.codec.on_open(&mut fx);
        sim.codec
            .subscribe(&plan.subs, &[], &sim.specs, &mut fx)
            .unwrap();
        sim.take(fx);
        sim.fire_due();
        sim
    }

    fn take(&mut self, mut fx: Effects) {
        for effect in fx.take() {
            match effect {
                Effect::Timer { tag, after } => {
                    let due = self.now + i64::try_from(after.as_nanos()).unwrap();
                    self.seq += 1;
                    self.timers.insert((due, self.seq), tag);
                    self.armed.push((due, tag));
                }
                Effect::Http { tag, req, .. } => {
                    self.requests.push((tag, self.now));
                    self.urls.push(req.url.as_str().to_owned());
                }
                other => panic!("the status poll asked for {other:?}"),
            }
        }
    }

    /// Fires every timer due by now, in order.
    fn fire_due(&mut self) {
        while let Some((&(due, seq), &tag)) = self.timers.iter().next() {
            if due > self.now {
                break;
            }
            self.timers.remove(&(due, seq));
            self.fire(tag);
        }
    }

    fn fire(&mut self, tag: TimerTag) {
        let mut fx = Effects::new();
        let mono = MonoNs(u64::try_from(self.now - self.start).unwrap());
        let mut sink = Kept {
            at: self.now,
            out: &mut self.reported,
        };
        self.codec
            .on_timer(tag, mono, WallNs(self.now), &mut sink, &mut fx);
        self.take(fx);
    }

    /// Moves the clock to `secs` (Unix seconds, a fraction allowed), firing each timer due by
    /// then at its due instant.
    pub fn advance_to(&mut self, secs: f64) {
        let target = (secs * NS as f64).round() as i64;
        self.advance_to_ns(target);
    }

    pub fn advance_to_ns(&mut self, target: i64) {
        assert!(target >= self.now, "the clock goes back");
        while let Some((&(due, seq), &tag)) = self.timers.iter().next() {
            if due > target {
                break;
            }
            self.timers.remove(&(due, seq));
            self.now = due;
            self.fire(tag);
        }
        self.now = target;
    }

    /// The oldest request not yet answered.
    pub fn next_read(&mut self) -> HttpTag {
        assert!(!self.requests.is_empty(), "no read in flight");
        self.requests.remove(0).0
    }

    /// Hands `tag` its result now, then fires every timer due now (the judging one): what the
    /// codec's `on_http` returned.
    pub fn respond(
        &mut self,
        tag: HttpTag,
        result: Result<HttpResponse<'_>, HttpFailure>,
    ) -> Result<(), DecodeError> {
        let mut fx = Effects::new();
        let mut sink = Kept {
            at: self.now,
            out: &mut self.reported,
        };
        let codec = &mut self.codec;
        let specs = &self.specs;
        let returned = dispatch_market_data(&self.caps, |scope| {
            codec.on_http(tag, result, scope, specs, &mut sink, &mut fx)
        });
        self.take(fx);
        self.fire_due();
        returned
    }

    /// Answers `tag` 200 with `body`.
    pub fn answer(&mut self, tag: HttpTag, body: &[u8]) -> Result<(), DecodeError> {
        let response = HttpResponse {
            status: 200,
            headers: &[],
            body,
        };
        self.respond(tag, Ok(response))
    }

    /// Answers the oldest read in flight 200 with fixture `name`, which must be taken.
    pub fn answer_next(&mut self, name: &str) {
        let tag = self.next_read();
        let body = answer(name);
        self.answer(tag, &body)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }

    /// Moves to the next read's instant, `INTERVAL` after `start` times `k` (seconds), and
    /// answers it at once with fixture `name`.
    pub fn read_at(&mut self, secs: i64, name: &str) {
        self.advance_to(secs as f64);
        self.answer_next(name);
    }

    /// What it reported since position `from`, as (seconds after `origin`, market, status).
    pub fn since(&self, from: usize, origin: i64) -> Vec<(f64, InstrumentId, TradingStatus)> {
        self.reported[from..]
            .iter()
            .map(|r| ((r.at - origin * NS) as f64 / NS as f64, r.inst, r.status))
            .collect()
    }

    /// The status last reported for `inst`.
    pub fn last(&self, inst: InstrumentId) -> Option<TradingStatus> {
        self.reported
            .iter()
            .rev()
            .find(|r| r.inst == inst)
            .map(|r| r.status)
    }
}

/// `status` reported for each of `markets`, in market order, at `at` seconds after the origin.
pub fn each(
    at: f64,
    markets: &[InstrumentId],
    status: TradingStatus,
) -> Vec<(f64, InstrumentId, TradingStatus)> {
    markets.iter().map(|&inst| (at, inst, status)).collect()
}
