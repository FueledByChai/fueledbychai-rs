//! FBC-1632's guarantee, checked at random against a venue the test makes up (decision 0130;
//! Verifier VE5-1 and VE5-2 on PR #214). The oracle is written from the status module's docs, not
//! from its code: what the venue does is decided first, as a timeline of halts, and every answer
//! a read gets is a true snapshot of that timeline at some instant between the read's send and
//! its answer. The poll's codec runs as the runtime runs it (`common::status::Sim`), and every
//! report it makes is held to what the docs promise:
//!
//! - **The guarantee.** A read that reports a market trading has a span, from its send to the
//!   instant its answer is judged, that meets no halt of the market and no window
//!   ([`AFTER`], `hibachi.auction_halt`) after one ends, except a halt of the one kind the docs
//!   leave out: one nothing announces (no published window, no calendar, no close or open an
//!   answer advertised), that no answer judged by then was sampled inside, and that no read
//!   failure or refusal, no answer leaving the market out, and no subscription of the market once
//!   the poll had sent a read, came after its begin and by then. A calendar's reopening and a
//!   published window's end hold the window after them whatever the venue does. Nothing reports
//!   a market trading but a read's answer.
//! - **The halts the docs promise, by when.** Every subscribed market halted at once when a read
//!   fails or is refused; a market a judged answer shows halted (its status or the exchange's, left
//!   out, sent before an open it advertises, judged at or after a close it advertises, its span
//!   met by a window it publishes or by the calendar's closure and the window after it) halted by
//!   that answer; a market subscribed once the poll has sent a read halted at once; and, between
//!   reads, a market halted at a calendar's close, at a published window's begin, at a close an
//!   answer advertised and before an open one advertised, until each ends.
//! - **Trading again, no later than the docs say.** Once the venue is quiet again, every
//!   subscribed market whose calendar is open trades from the read sent the window after the
//!   first read sent after that.
//!
//! Each run draws its venue and its traffic from a seed: halts by a contract's status and by the
//! exchange's, published maintenance windows (some withdrawn), advertised closes and opens past
//! and future (added, moved, withdrawn, kept or dropped after they pass), an FX pair's weekend and
//! its overrun, answers sampled anywhere in their spans and answered in or out of order, failed
//! and refused reads, answers leaving a market out, and subscriptions added, removed and taken
//! back, the empty set included, on a Wednesday and across the FX close and the FX reopening. The
//! instants the venue changes what it advertises cluster just after reads are sent, and answers
//! sample their spans at either end as often as between, where reads in flight together see the
//! venue in either order. A third of the runs also set up, around two consecutive reads, a pattern
//! that random draws line up too rarely ([`Probe`]): a read held across a market's first
//! subscription during a pause (Verifier VE5-1), and two reads answered in send order whose
//! samples straddle a close's or a window's publication (VE5-2). The seeds are fixed, so every
//! run is the same. `STATUS_ORACLE_SEED=<n>` runs seed `n` alone and prints what happened;
//! `STATUS_ORACLE_RUNS=<n>` runs seeds 0 to `n` instead of the fixed ones.

mod common;

use std::collections::BTreeSet;
use std::fmt::Write as _;

use common::status::{AFTER, ETH, EUR, FX_CLOSE, FX_REOPEN, INTERVAL, NS, Sim, WEDNESDAY, WIF};
use fbc_core::{
    Feed, HttpFailure, HttpResponse, HttpTag, InstrumentId, Subscription, TradingStatus,
};
use fbc_venue_hibachi::BOOK_ORDERBOOK;
use serde_json::{Value, json};

const TRADING: TradingStatus = TradingStatus::Trading;
const HALTED: TradingStatus = TradingStatus::Halted;

/// The three markets, in the index order every table below uses, and their symbols.
const MARKETS: [InstrumentId; 3] = [ETH, EUR, WIF];
const SYMBOLS: [&str; 3] = ["ETH/USDT-P", "EUR/USDT-P", "WIF/USDT-P"];
const EUR_AT: usize = 1;

/// Seconds of traffic drawn at random in each run; then the venue is quiet.
const RANDOM_PHASE: i64 = 1_200;
/// No halt or advertisement drawn lasts past the random phase.
const QUIET_MARGIN: i64 = 150;
/// The longest a read waits for its answer.
const LONGEST_HOLD: i64 = 250;
/// A halt that never ends within a run.
const NEVER: i64 = i64::MAX / 4;

/// The seeds run, each in the regime its index picks.
const SEEDS: std::ops::Range<u64> = 0..240;

/// SplitMix64: a small, fixed generator, so the runs need no dependency and never change.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// From `lo` to `hi`, both included.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(lo <= hi, "{lo} > {hi}");
        lo + i64::try_from(self.below(u64::try_from(hi - lo).unwrap() + 1)).unwrap()
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[usize::try_from(self.below(items.len() as u64)).unwrap()]
    }
}

/// An instant from `lo` to `hi`, both included; half the time a few seconds after a read is sent
/// (every [`INTERVAL`] from `t0`), where two reads in flight may see it in either order.
fn instant(rng: &mut Rng, t0: i64, lo: i64, hi: i64) -> i64 {
    let at = rng.range(lo, hi);
    if rng.chance(50) {
        let read = t0 + (at - t0).div_euclid(INTERVAL) * INTERVAL;
        let near = read + rng.range(1, 25);
        if (lo..=hi).contains(&near) {
            return near;
        }
    }
    at
}

/// What a halt is announced by, as the docs list the schedule: nothing, the contract's calendar,
/// a published maintenance window, an advertised close (its instant) or an advertised open.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Cause {
    Unannounced,
    Calendar,
    Window(usize),
    Close(i64),
    Open(i64),
}

/// A halt of the venue: closed from `b` until `e`.
#[derive(Clone, Copy, Debug)]
struct Halt {
    cause: Cause,
    b: i64,
    e: i64,
}

impl Halt {
    fn covers(&self, x: i64) -> bool {
        self.b <= x && x < self.e
    }

    /// Whether it meets the span from `from` to `to`, both included.
    fn meets(&self, from: i64, to: i64) -> bool {
        self.b <= to && from < self.e
    }
}

/// An advertised instant: `value`, in every answer sampled from `from` until `until`.
#[derive(Clone, Copy, Debug)]
struct Advert {
    from: i64,
    until: i64,
    value: i64,
}

fn advertised(adverts: &[Advert], x: i64) -> Option<i64> {
    adverts
        .iter()
        .find(|a| a.from <= x && x < a.until)
        .map(|a| a.value)
}

/// One contract's timeline.
#[derive(Clone, Default, Debug)]
struct Contract {
    halts: Vec<Halt>,
    closes: Vec<Advert>,
    opens: Vec<Advert>,
}

/// A maintenance window the venue publishes from `from` until `until`; `real` when it runs.
#[derive(Clone, Copy, Debug)]
struct Published {
    id: usize,
    from: i64,
    until: i64,
    b: i64,
    e: i64,
    real: bool,
    /// Whether the exchange's status shows it while it runs.
    status_shown: bool,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Regime {
    Wednesday,
    FxClose,
    FxReopen,
}

/// A pattern a run sets up on purpose around two consecutive reads, sent at `earlier` and
/// `later`, where a codec has been wrong before and which random draws line up too rarely.
#[derive(Clone, Copy, Debug)]
enum Probe {
    /// Something is published just after the later read is sent: a close of every contract or a
    /// window, coming before the read after it is sent. The earlier read, held, sees it and is
    /// answered first; the later, seeing the venue before it, is answered after, before it comes
    /// (Verifier VE5-2).
    Overlap {
        earlier: i64,
        later: i64,
        earlier_answer: i64,
        later_answer: i64,
    },
    /// Market `m`, not subscribed, pauses just after the earlier read is sent, which sees it
    /// trading; it is subscribed during the pause, while that read is in flight; the pause ends
    /// just after the later read is sent, which sees the market trading again (Verifier VE5-1).
    Held {
        m: usize,
        earlier: i64,
        subscribed: i64,
        earlier_answer: i64,
        later: i64,
        later_answer: i64,
    },
}

/// The venue: what each contract and the exchange do, decided before any read.
#[derive(Clone, Debug)]
struct Venue {
    regime: Regime,
    contracts: [Contract; 3],
    /// The exchange's halts: unannounced maintenances and the windows that run.
    exchange: Vec<Halt>,
    windows: Vec<Published>,
    probe: Option<Probe>,
}

/// Where the halts in `halts` end, the market reopening: the end of each run of them that
/// overlap or touch.
fn reopenings(halts: impl Iterator<Item = Halt>) -> Vec<i64> {
    let mut spans: Vec<(i64, i64)> = halts.map(|h| (h.b, h.e)).collect();
    spans.sort_unstable();
    let mut ends = Vec::new();
    let mut current: Option<(i64, i64)> = None;
    for (b, e) in spans {
        current = match current {
            Some((cb, ce)) if b <= ce => Some((cb, ce.max(e))),
            Some((_, ce)) => {
                ends.push(ce);
                Some((b, e))
            }
            None => Some((b, e)),
        };
    }
    ends.extend(current.map(|(_, e)| e));
    ends.retain(|e| *e < NEVER);
    ends
}

fn in_auction(ends: &[i64], x: i64) -> bool {
    ends.iter().any(|e| *e <= x && x < e + AFTER)
}

impl Venue {
    /// Every halt of market `i`: its contract's and the exchange's.
    fn halts(&self, i: usize) -> impl Iterator<Item = Halt> + '_ {
        (self.contracts[i].halts.iter().copied()).chain(self.exchange.iter().copied())
    }

    /// The contract's status in an answer sampled at `x`. Inside a halt it shows the halt,
    /// unless an advertised close or open already does (then anything); in the window after a
    /// reopening it may show either (no source says which).
    fn contract_status(&self, i: usize, x: i64, rng: &mut Rng) -> &'static str {
        let c = &self.contracts[i];
        let close = advertised(&c.closes, x);
        let open = advertised(&c.opens, x);
        let mut covered = false;
        let mut forced = None;
        for h in c.halts.iter().filter(|h| h.covers(x)) {
            covered = true;
            let shown_by_schedule = match h.cause {
                Cause::Close(_) => close.is_some_and(|c| c <= x),
                Cause::Open(_) => open.is_some_and(|o| o > x),
                _ => false,
            };
            if !shown_by_schedule {
                forced = Some(match h.cause {
                    Cause::Close(_) => "CLOSED",
                    Cause::Open(_) => rng.pick(&["PRE_OPEN", "LISTED"]),
                    _ => rng.pick(&["PAUSED", "PAUSED", "CLOSED"]),
                });
            }
        }
        if let Some(status) = forced {
            return status;
        }
        if covered {
            return rng.pick(&["LIVE", "LIVE", "CLOSED", "PRE_OPEN"]);
        }
        if in_auction(&reopenings(c.halts.iter().copied()), x) && rng.chance(25) {
            return "PAUSED";
        }
        "LIVE"
    }

    /// The exchange's status in an answer sampled at `x`.
    fn exchange_status(&self, x: i64, rng: &mut Rng) -> &'static str {
        let unannounced = |h: &&Halt| h.cause == Cause::Unannounced && h.covers(x);
        if self.exchange.iter().any(|h| unannounced(&h)) {
            return "UNSCHEDULED_MAINTENANCE";
        }
        for w in self
            .windows
            .iter()
            .filter(|w| w.real && w.b <= x && x < w.e)
        {
            let published = w.from <= x && x < w.until;
            if w.status_shown || !published {
                return "SCHEDULED_MAINTENANCE";
            }
        }
        if in_auction(&reopenings(self.exchange.iter().copied()), x) && rng.chance(20) {
            return rng.pick(&["SCHEDULED_MAINTENANCE", "UNSCHEDULED_MAINTENANCE"]);
        }
        "NORMAL"
    }
}

/// What one judged answer showed, as the oracle reads it.
#[derive(Clone, Debug)]
struct Facts {
    sample: i64,
    exchange_normal: bool,
    listed: [bool; 3],
    live: [bool; 3],
    opens: [Option<i64>; 3],
    closes: [Option<i64>; 3],
    /// The windows it publishes: id, begin, end.
    windows: Vec<(usize, i64, i64)>,
}

/// The answer the venue gives a read sampled at `x`, leaving out the contracts `omit` names.
fn answer_at(
    venue: &Venue,
    template: &Value,
    x: i64,
    omit: [bool; 3],
    rng: &mut Rng,
) -> (Vec<u8>, Facts) {
    let mut doc = template.clone();
    let exchange = venue.exchange_status(x, rng);
    doc["status"] = json!(exchange);
    let published: Vec<&Published> = (venue.windows.iter())
        .filter(|w| w.from <= x && x < w.until)
        .collect();
    let as_string = rng.chance(50);
    doc["maintenanceWindow"] = Value::Array(
        published
            .iter()
            .map(|w| {
                if as_string {
                    json!({"begin": w.b.to_string(), "end": w.e.to_string(), "note": "Synthetic"})
                } else {
                    json!({"begin": w.b, "end": w.e, "note": "Synthetic"})
                }
            })
            .collect(),
    );
    let mut facts = Facts {
        sample: x,
        exchange_normal: exchange == "NORMAL",
        listed: [false; 3],
        live: [false; 3],
        opens: [None; 3],
        closes: [None; 3],
        windows: published.iter().map(|w| (w.id, w.b, w.e)).collect(),
    };
    let contracts = doc["futureContracts"].as_array_mut().unwrap();
    contracts.retain(|c| {
        let symbol = c["symbol"].as_str().unwrap();
        SYMBOLS
            .iter()
            .position(|s| *s == symbol)
            .is_none_or(|i| !omit[i])
    });
    for contract in contracts.iter_mut() {
        let symbol = contract["symbol"].as_str().unwrap().to_owned();
        let Some(i) = SYMBOLS.iter().position(|s| *s == symbol) else {
            continue;
        };
        let status = venue.contract_status(i, x, rng);
        let open = advertised(&venue.contracts[i].opens, x);
        let close = advertised(&venue.contracts[i].closes, x);
        contract["status"] = json!(status);
        contract["marketOpenTimestamp"] = open.map_or(Value::Null, |o| json!(o.to_string()));
        contract["marketCloseTimestamp"] = close.map_or(Value::Null, |c| json!(c.to_string()));
        facts.listed[i] = true;
        facts.live[i] = status == "LIVE";
        facts.opens[i] = open;
        facts.closes[i] = close;
    }
    (doc.to_string().into_bytes(), facts)
}

/// The venue of one run, starting at `t0`.
fn draw_venue(regime: Regime, t0: i64, rng: &mut Rng) -> Venue {
    let quiet = t0 + RANDOM_PHASE - QUIET_MARGIN;
    let last_advert = t0 + RANDOM_PHASE;
    let mut contracts: [Contract; 3] = Default::default();
    let mut windows = Vec::new();
    let mut exchange = Vec::new();
    let probe = match rng.below(100) {
        0..=34 => {
            // Late enough to follow some history, early enough that the relisting after its
            // close, and the window after that, end before the venue is quiet.
            let later = t0 + rng.range(2, 11) * INTERVAL;
            let publish = later + rng.range(1, 8);
            let at = later + rng.range(30, 50);
            let earlier_answer = rng.range(publish + 1, at - 8);
            let later_answer = rng.range(earlier_answer + 1, at - 2);
            if rng.chance(50) {
                let e = at + rng.range(10, 100);
                let id = 99;
                windows.push(Published {
                    id,
                    from: publish,
                    until: e + rng.range(0, 100),
                    b: at,
                    e,
                    real: true,
                    status_shown: rng.chance(50),
                });
                exchange.push(Halt {
                    cause: Cause::Window(id),
                    b: at,
                    e,
                });
            } else {
                for c in &mut contracts {
                    closed(c, publish, at, last_advert, rng);
                }
            }
            Some(Probe::Overlap {
                earlier: later - INTERVAL,
                later,
                earlier_answer,
                later_answer,
            })
        }
        35..=64 => {
            let earlier = t0 + rng.range(1, 13) * INTERVAL;
            let later = earlier + INTERVAL;
            let m = usize::try_from(rng.below(3)).unwrap();
            let b = earlier + rng.range(2, 4);
            let subscribed = b + rng.range(1, 3);
            let e = later + rng.range(1, 3);
            contracts[m].halts.push(unannounced(b, e));
            Some(Probe::Held {
                m,
                earlier,
                subscribed,
                earlier_answer: subscribed + rng.range(1, 5),
                later,
                later_answer: e + rng.range(1, 3),
            })
        }
        _ => None,
    };
    for (i, c) in contracts.iter_mut().enumerate() {
        if i == EUR_AT {
            // The FX weekend, which the codec knows from EUR's calendar, and the halts around it
            // that nothing announces: EUR paused before the close, or still paused after the
            // reopening.
            match regime {
                Regime::Wednesday => {}
                Regime::FxClose | Regime::FxReopen => {
                    c.halts.push(Halt {
                        cause: Cause::Calendar,
                        b: FX_CLOSE,
                        e: FX_REOPEN,
                    });
                }
            }
            if regime == Regime::FxClose && rng.chance(30) {
                let b = FX_CLOSE - rng.range(5, 150);
                c.halts.push(unannounced(b, FX_CLOSE));
            }
            if regime == Regime::FxReopen && rng.chance(40) {
                let e = FX_REOPEN + rng.range(5, 200);
                c.halts.push(unannounced(FX_REOPEN, e));
            }
        }
        let roll = rng.below(100);
        let adverts_free = c.closes.is_empty() && c.opens.is_empty();
        if adverts_free && roll < 18 {
            draw_open(c, t0, last_advert, rng);
        } else if adverts_free && roll < 55 {
            draw_close(c, t0, last_advert, rng);
        }
        for _ in 0..rng.pick(&[0, 0, 1, 1, 2]) {
            let b = rng.range(t0 - 50, quiet - 200);
            let len = if rng.chance(50) {
                rng.range(1, 30)
            } else {
                rng.range(31, 200)
            };
            c.halts.push(unannounced(b, b + len));
        }
    }
    if rng.chance(25) {
        let b = rng.range(t0, quiet - 200);
        exchange.push(unannounced(b, b + rng.range(5, 150)));
    }
    for id in 0..rng.pick(&[0, 0, 1, 1, 2]) {
        let b = rng.range(t0 + 30, quiet - 160);
        let e = b + rng.range(10, 150);
        let real = rng.chance(75);
        if real {
            let from = instant(rng, t0, b - 300, b + 30);
            let overrun = rng.range(0, 200);
            let until = (e + rng.pick(&[0, overrun])).min(last_advert);
            let status_shown = from > b || rng.chance(50);
            windows.push(Published {
                id,
                from,
                until,
                b,
                e,
                real,
                status_shown,
            });
            exchange.push(Halt {
                cause: Cause::Window(id),
                b,
                e,
            });
        } else {
            // Withdrawn before it begins.
            let from = instant(rng, t0, b - 300, b - 2);
            let until = instant(rng, t0, from + 1, b - 1);
            windows.push(Published {
                id,
                from,
                until,
                b,
                e,
                real,
                status_shown: false,
            });
        }
    }
    Venue {
        regime,
        contracts,
        exchange,
        windows,
        probe,
    }
}

fn unannounced(b: i64, e: i64) -> Halt {
    Halt {
        cause: Cause::Unannounced,
        b,
        e,
    }
}

/// A contract not yet open: its open advertised (perhaps moved before it comes, perhaps dropped
/// before or after it), past or future. An open moved later leaves the stretch from the first to
/// the second a halt of its own, announced only by an answer advertising the second: one that
/// saw only the first knows of no halt past it.
fn draw_open(c: &mut Contract, t0: i64, last_advert: i64, rng: &mut Rng) {
    let start = t0 - 1_000;
    let open = rng.range(t0 - 200, t0 + 500);
    let mut from = start;
    let mut b = start;
    if rng.chance(30) {
        let first = rng.range(t0 - 100, t0 + 600);
        let lo = open.min(first);
        if lo - 1 > t0 - 60 {
            let moved = instant(rng, t0, t0 - 60, lo - 1);
            c.opens.push(Advert {
                from,
                until: moved,
                value: first,
            });
            from = moved;
            if first < open {
                c.halts.push(Halt {
                    cause: Cause::Open(first),
                    b: start,
                    e: first,
                });
                b = first;
            }
        }
    }
    let until = match rng.below(10) {
        0 => (open - rng.range(1, 60)).max(from + 1),
        1..=3 => last_advert,
        _ => (open + rng.range(0, 300)).min(last_advert),
    };
    if from < until {
        c.opens.push(Advert {
            from,
            until,
            value: open,
        });
    }
    c.halts.push(Halt {
        cause: Cause::Open(open),
        b,
        e: open,
    });
}

/// A temp market's close: advertised ahead of it or first after it passed, kept or dropped once
/// the market is listed again (perhaps with an open), or moved, or withdrawn before it comes.
fn draw_close(c: &mut Contract, t0: i64, last_advert: i64, rng: &mut Rng) {
    let close = instant(rng, t0, t0 + 20, t0 + RANDOM_PHASE - 400);
    match rng.below(10) {
        0 | 1 => {
            let from = instant(rng, t0, t0 - 100, close - 2);
            let until = instant(rng, t0, from + 1, close - 1);
            c.closes.push(Advert {
                from,
                until,
                value: close,
            });
        }
        2 | 3 => {
            let other = rng.range(t0 + 20, t0 + RANDOM_PHASE - 400);
            let lo = close.min(other);
            if lo - 2 < t0 - 100 {
                return;
            }
            let from = instant(rng, t0, t0 - 100, lo - 2);
            let moved = instant(rng, t0, from + 1, lo - 1);
            c.closes.push(Advert {
                from,
                until: moved,
                value: other,
            });
            closed(c, moved, close, last_advert, rng);
        }
        _ => {
            let from = instant(rng, t0, t0 - 100, close + 30);
            closed(c, from, close, last_advert, rng);
        }
    }
}

/// The close at `close`, advertised from `from`, the market listed again after it.
fn closed(c: &mut Contract, from: i64, close: i64, last_advert: i64, rng: &mut Rng) {
    let relisted = close + rng.range(5, 300);
    let kept = rng.range(0, 200);
    let until = (relisted + rng.pick(&[0, kept])).min(last_advert);
    if from < until {
        c.closes.push(Advert {
            from,
            until,
            value: close,
        });
    }
    if rng.chance(50) {
        let from = relisted - rng.range(0, 100);
        let until = (relisted + rng.range(0, 250)).min(last_advert);
        if from < until {
            c.opens.push(Advert {
                from,
                until,
                value: relisted,
            });
        }
    }
    c.halts.push(Halt {
        cause: Cause::Close(close),
        b: close,
        e: relisted,
    });
}

/// The subscriptions a run draws from.
fn pool() -> [Subscription; 6] {
    let sub = |inst, feed| Subscription { inst, feed };
    [
        sub(ETH, Feed::Trades),
        sub(ETH, Feed::Book(BOOK_ORDERBOOK)),
        sub(EUR, Feed::Trades),
        sub(EUR, Feed::Mark),
        sub(WIF, Feed::Mark),
        sub(WIF, Feed::Trades),
    ]
}

fn draw_subs(rng: &mut Rng) -> BTreeSet<Subscription> {
    if rng.chance(12) {
        return BTreeSet::new();
    }
    pool().into_iter().filter(|_| rng.chance(45)).collect()
}

fn markets_of(subs: &BTreeSet<Subscription>) -> [bool; 3] {
    let mut on = [false; 3];
    for sub in subs {
        on[MARKETS.iter().position(|m| *m == sub.inst).unwrap()] = true;
    }
    on
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Answer,
    TimedOut,
    Unavailable,
    Refused,
}

/// Where in its span an answer samples the venue.
#[derive(Clone, Copy, Debug)]
enum Sample {
    Send,
    Answer,
    Anywhere,
}

/// A read in flight and what the run will do with it.
#[derive(Clone, Copy, Debug)]
struct Planned {
    tag: HttpTag,
    sent: i64,
    answer_at: i64,
    outcome: Outcome,
    omit: [bool; 3],
    sample: Sample,
}

/// Something that happened in a run, in the order the codec saw it.
#[derive(Clone, Debug)]
enum Event {
    Judged {
        at: i64,
        facts: Facts,
    },
    /// A read that failed or whose answer was refused.
    Failed {
        at: i64,
    },
    /// A market subscribed when it was not, and whether the poll had sent a read by then.
    Subscribed {
        at: i64,
        market: usize,
        begun: bool,
    },
}

/// One run: the venue, what happened and what the codec reported.
struct Run {
    seed: u64,
    venue: Venue,
    events: Vec<Event>,
    /// The codec's status of each market as reported since it was last subscribed.
    view: [Option<TradingStatus>; 3],
    violations: Vec<(Kind, String)>,
    trace: Option<String>,
}

/// What a violation breaks.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
enum Kind {
    /// A market reported trading where the guarantee forbids it.
    Guarantee,
    /// A halt the docs promise not reported by when they say.
    Halt,
    /// A market not trading again by when the docs say.
    Trading,
}

impl Run {
    fn violation(&mut self, kind: Kind, at: i64, what: String) {
        let line = format!(
            "seed {} ({:?}) at t{at:+}: {what}",
            self.seed, self.venue.regime
        );
        self.violations.push((kind, line));
    }

    fn tracing(&self) -> bool {
        self.trace.is_some()
    }

    fn note(&mut self, line: String) {
        if let Some(trace) = &mut self.trace {
            let _ = writeln!(trace, "{line}");
        }
    }

    /// Whether the halt `h` of market `m` is the one kind the docs leave out, as the codec stood
    /// after the events so far: nothing announces it, no answer judged was sampled inside it, and
    /// no failure, refusal, answer leaving `m` out or subscription of `m` once the poll had sent a
    /// read came at or after its begin.
    fn excepted(&self, m: usize, h: &Halt) -> bool {
        let announced = match h.cause {
            Cause::Unannounced => false,
            Cause::Calendar => true,
            Cause::Window(id) => self.events.iter().any(|ev| {
                matches!(ev, Event::Judged { facts, .. } if facts.windows.iter().any(|w| w.0 == id))
            }),
            Cause::Close(close) => self.events.iter().any(
                |ev| matches!(ev, Event::Judged { facts, .. } if facts.closes[m] == Some(close)),
            ),
            Cause::Open(open) => self.events.iter().any(
                |ev| matches!(ev, Event::Judged { facts, .. } if facts.opens[m] == Some(open)),
            ),
        };
        let shown_or_bracketed = self.events.iter().any(|ev| match ev {
            Event::Judged { at, facts, .. } => {
                h.covers(facts.sample) || (!facts.listed[m] && *at >= h.b)
            }
            Event::Failed { at } => *at >= h.b,
            Event::Subscribed {
                at, market, begun, ..
            } => *market == m && *begun && *at >= h.b,
        });
        !announced && !shown_or_bracketed
    }

    /// The guarantee, for a read sent at `sent` and judged at `at` that reported market `m`
    /// trading.
    fn check_trading(&mut self, m: usize, sent: i64, at: i64) {
        let halts: Vec<Halt> = self.venue.halts(m).collect();
        for h in &halts {
            if h.meets(sent, at) && !self.excepted(m, h) {
                let what = format!(
                    "{} reported trading by the read sent at t{:+} and judged at t{:+}, inside {h:?}",
                    SYMBOLS[m],
                    sent - self.t0(),
                    at - self.t0()
                );
                self.violation(Kind::Guarantee, at - self.t0(), what);
            }
        }
        // The window after each reopening the venue made: one known if any halt ending there is.
        for e in reopenings(halts.iter().copied()) {
            let window = Halt {
                cause: Cause::Unannounced,
                b: e,
                e: e + AFTER,
            };
            let known = halts.iter().any(|h| h.e == e && !self.excepted(m, h));
            if window.meets(sent, at) && known {
                let what = format!(
                    "{} reported trading by the read sent at t{:+} and judged at t{:+}, inside the \
                     window after the reopening at t{:+}",
                    SYMBOLS[m],
                    sent - self.t0(),
                    at - self.t0(),
                    e - self.t0()
                );
                self.violation(Kind::Guarantee, at - self.t0(), what);
            }
        }
        // The window after the calendar's reopening and after each window published, whatever
        // the venue did.
        let mut scheduled = Vec::new();
        if m == EUR_AT && self.venue.regime != Regime::Wednesday {
            scheduled.push(FX_REOPEN);
        }
        for ev in &self.events {
            if let Event::Judged { facts, .. } = ev {
                let real = |id: usize| self.venue.windows.iter().any(|w| w.id == id && w.real);
                scheduled.extend(facts.windows.iter().filter(|w| real(w.0)).map(|w| w.2));
            }
        }
        for e in scheduled {
            if sent < e + AFTER && e <= at {
                let what = format!(
                    "{} reported trading by the read sent at t{:+} and judged at t{:+}, inside the \
                     window after the scheduled reopening at t{:+}",
                    SYMBOLS[m],
                    sent - self.t0(),
                    at - self.t0(),
                    e - self.t0()
                );
                self.violation(Kind::Guarantee, at - self.t0(), what);
            }
        }
    }

    fn t0(&self) -> i64 {
        start_of(self.venue.regime)
    }

    /// Takes the reports made since `from`: a trading one only from a read judged (`judged`:
    /// the read's send and judged instants), each held to the guarantee.
    fn take_reports(&mut self, sim: &Sim, from: usize, judged: Option<(i64, i64)>) {
        for &r in &sim.reported[from..] {
            let m = MARKETS.iter().position(|i| *i == r.inst).unwrap();
            let at = r.at / NS;
            if self.tracing() {
                let line = format!(
                    "  report t{:+} {} {:?}",
                    at - self.t0(),
                    SYMBOLS[m],
                    r.status
                );
                self.note(line);
            }
            self.view[m] = Some(r.status);
            if r.status == TRADING {
                match judged {
                    Some((sent, judged_at)) => self.check_trading(m, sent, judged_at),
                    None => self.violation(
                        Kind::Guarantee,
                        at - self.t0(),
                        format!("{} reported trading by no read", SYMBOLS[m]),
                    ),
                }
            }
        }
    }

    fn expect_halted(&mut self, now: i64, on: [bool; 3], which: [bool; 3], why: &str) {
        for m in 0..3 {
            if on[m] && which[m] && self.view[m] != Some(HALTED) {
                let what = format!("{} not halted {why} (view {:?})", SYMBOLS[m], self.view[m]);
                self.violation(Kind::Halt, now - self.t0(), what);
            }
        }
    }

    /// The halts the docs promise between reads, at `now`, for the markets subscribed (`on`).
    fn check_scheduled(&mut self, now: i64, on: [bool; 3]) {
        let mut which = [false; 3];
        let mut why = String::new();
        let judged = |ev: &Event| match ev {
            Event::Judged { facts, .. } => Some(facts.clone()),
            _ => None,
        };
        let seen: Vec<Facts> = self.events.iter().filter_map(judged).collect();
        if on[EUR_AT]
            && self.venue.regime != Regime::Wednesday
            && (FX_CLOSE..FX_REOPEN).contains(&now)
        {
            which[EUR_AT] = true;
            why.push_str("in its calendar's closure; ");
        }
        for w in self
            .venue
            .windows
            .iter()
            .filter(|w| w.real && w.b <= now && now < w.e)
        {
            if seen.iter().any(|f| f.windows.iter().any(|p| p.0 == w.id)) {
                which = [true; 3];
                let _ = write!(why, "in the published window {}; ", w.id);
            }
        }
        for m in 0..3 {
            for h in self.venue.contracts[m].halts.clone() {
                let known = match h.cause {
                    Cause::Close(close) => seen.iter().any(|f| f.closes[m] == Some(close)),
                    Cause::Open(open) => seen.iter().any(|f| f.opens[m] == Some(open)),
                    _ => false,
                };
                if known && h.covers(now) {
                    which[m] = true;
                    let _ = write!(why, "{} in {h:?} an answer advertised; ", SYMBOLS[m]);
                }
            }
        }
        self.expect_halted(now, on, which, &format!("between reads: {why}"));
    }
}

fn start_of(regime: Regime) -> i64 {
    match regime {
        Regime::Wednesday => WEDNESDAY,
        Regime::FxClose => FX_CLOSE - 300,
        Regime::FxReopen => FX_REOPEN - 300,
    }
}

/// What a judged answer shows halted, by itself: the markets the docs say any read reports so.
fn shown_halted(venue: &Venue, sent: i64, at: i64, facts: &Facts) -> [bool; 3] {
    let mut halted = [false; 3];
    for (m, h) in halted.iter_mut().enumerate() {
        let calendar = m == EUR_AT
            && venue.regime != Regime::Wednesday
            && FX_CLOSE <= at
            && sent < FX_REOPEN + AFTER;
        let window = (facts.windows.iter()).any(|&(_, b, e)| b <= at && sent < e + AFTER);
        *h = !facts.exchange_normal
            || !facts.listed[m]
            || !facts.live[m]
            || facts.closes[m].is_some_and(|c| c <= at)
            || facts.opens[m].is_some_and(|o| sent < o + AFTER)
            || window
            || calendar;
    }
    halted
}

/// The reads the codec has asked for since the last call, each given what the run will do with
/// it: when it is answered, and how. While `random`, a read may wait up to [`LONGEST_HOLD`], fail,
/// be refused or leave a market out; afterwards each is answered at once.
struct Reads {
    planned: Vec<Planned>,
    known: BTreeSet<HttpTag>,
    probe: Option<Probe>,
    /// Whether the poll has sent a read.
    begun: bool,
    /// When the last read drawn at random is settled: the venue is quiet after then.
    quiet_from: i64,
    /// The first read sent once the venue is quiet: after that instant, so after every read
    /// drawn at random is settled, one settled at that instant included.
    first_quiet: Option<i64>,
}

impl Reads {
    fn take(&mut self, sim: &Sim, random: bool, rng: &mut Rng) {
        for &(tag, at) in &sim.requests {
            if !self.known.insert(tag) {
                continue;
            }
            let sent = at / NS;
            self.begun = true;
            let hold = if !random {
                0
            } else {
                match rng.below(100) {
                    0..=54 => rng.range(0, 3),
                    55..=79 => rng.range(4, 50),
                    80..=94 => rng.range(51, 130),
                    _ => rng.range(131, LONGEST_HOLD),
                }
            };
            let outcome = if !random {
                Outcome::Answer
            } else {
                match rng.below(100) {
                    0..=3 => Outcome::TimedOut,
                    4..=5 => Outcome::Unavailable,
                    6..=9 => Outcome::Refused,
                    _ => Outcome::Answer,
                }
            };
            let omit = [0; 3].map(|_| random && rng.chance(6));
            let sample = match rng.below(100) {
                0..=34 => Sample::Send,
                35..=69 => Sample::Answer,
                _ => Sample::Anywhere,
            };
            let mut plan = Planned {
                tag,
                sent,
                answer_at: sent + hold,
                outcome,
                omit,
                sample,
            };
            // The probe's two reads, and the Held probe's next four answered clean and at once.
            let clean = |answer_at: i64, sample: Sample| Planned {
                tag,
                sent,
                answer_at,
                outcome: Outcome::Answer,
                omit: [false; 3],
                sample,
            };
            match self.probe {
                Some(Probe::Overlap {
                    earlier,
                    later,
                    earlier_answer,
                    later_answer,
                }) => {
                    if sent == earlier {
                        plan = clean(earlier_answer, Sample::Answer);
                    } else if sent == later {
                        plan = clean(later_answer, Sample::Send);
                    }
                }
                Some(Probe::Held {
                    earlier,
                    earlier_answer,
                    later,
                    later_answer,
                    ..
                }) => {
                    if sent == earlier {
                        plan = clean(earlier_answer, Sample::Send);
                    } else if sent == later {
                        plan = clean(later_answer, Sample::Answer);
                    } else if later < sent && sent <= later + 4 * INTERVAL {
                        plan = clean(sent + rng.range(0, 2), Sample::Anywhere);
                    }
                }
                None => {}
            }
            let hold = plan.answer_at - sent;
            self.planned.push(plan);
            if random {
                self.quiet_from = self.quiet_from.max(sent + hold);
            } else if self.first_quiet.is_none() && sent > self.quiet_from {
                self.first_quiet = Some(sent);
            }
        }
    }
}

/// A change of subscriptions drawn at random: one added, one removed, none, or a new set.
fn draw_change(subs: &BTreeSet<Subscription>, rng: &mut Rng) -> BTreeSet<Subscription> {
    let mut next = subs.clone();
    match rng.below(10) {
        0..=3 => {
            next.insert(rng.pick(&pool()));
        }
        4..=6 => {
            let n = next.len() as u64;
            if n > 0 {
                let gone = *next
                    .iter()
                    .nth(usize::try_from(rng.below(n)).unwrap())
                    .unwrap();
                next.remove(&gone);
            }
        }
        7 => next.clear(),
        _ => next = draw_subs(rng),
    }
    next
}

/// Whether `probe` runs at `now`: no change of subscriptions is drawn then.
fn probe_runs(probe: Probe, now: i64) -> bool {
    match probe {
        Probe::Overlap { earlier, later, .. } => earlier - 30 <= now && now <= later + INTERVAL,
        Probe::Held { earlier, later, .. } => earlier - 20 <= now && now <= later + 4 * INTERVAL,
    }
}

/// The change of subscriptions `probe` makes at `now`, if any: some market subscribed before
/// its reads, so they are sent; for Held, its market's subscriptions removed first, and one of
/// them taken during the pause.
fn probe_change(
    probe: Option<Probe>,
    now: i64,
    subs: &BTreeSet<Subscription>,
    rng: &mut Rng,
) -> Option<BTreeSet<Subscription>> {
    let of = |m: usize| pool().into_iter().filter(move |s| s.inst == MARKETS[m]);
    match probe? {
        Probe::Overlap { earlier, .. } if now == earlier - 30 && subs.is_empty() => {
            Some(BTreeSet::from([rng.pick(&pool())]))
        }
        Probe::Held { m, earlier, .. } if now == earlier - 20 => {
            let mut next: BTreeSet<Subscription> = subs
                .iter()
                .copied()
                .filter(|s| s.inst != MARKETS[m])
                .collect();
            if next.is_empty() {
                let others: Vec<Subscription> = pool()
                    .into_iter()
                    .filter(|s| s.inst != MARKETS[m])
                    .collect();
                next.insert(rng.pick(&others));
            }
            Some(next)
        }
        Probe::Held { m, subscribed, .. } if now == subscribed => {
            let mut next = subs.clone();
            let mine: Vec<Subscription> = of(m).collect();
            next.insert(rng.pick(&mine));
            Some(next)
        }
        _ => None,
    }
}

fn run(seed: u64, trace: bool) -> Run {
    let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x1632);
    let regime = [Regime::Wednesday, Regime::FxClose, Regime::FxReopen][(seed % 3) as usize];
    let t0 = start_of(regime);
    let venue = draw_venue(regime, t0, &mut rng);
    let template: Value = common::status::answer_json("live");
    let mut run = Run {
        seed,
        venue,
        events: Vec::new(),
        view: [None; 3],
        violations: Vec::new(),
        trace: trace.then(String::new),
    };
    if run.tracing() {
        let line = format!("{:#?}", run.venue);
        run.note(line);
    }
    let mut subs = draw_subs(&mut rng);
    if run.tracing() {
        run.note(format!("t+0 subscribed {:?}", markets_of(&subs)));
    }
    let mut sim = Sim::with(&common::config(), &subs, t0);
    // Changes of subscriptions drawn just after some halts begin, where a market subscribed then
    // may be lifted by a read that cannot know of the halt.
    let begins: Vec<i64> = (run.venue.contracts.iter())
        .flat_map(|c| c.halts.iter())
        .chain(&run.venue.exchange)
        .map(|h| h.b)
        .collect();
    let mut nudges = BTreeSet::new();
    for b in begins {
        if rng.chance(50) {
            nudges.insert(b + rng.range(0, 15));
        }
    }
    let mut reads = Reads {
        planned: Vec::new(),
        known: BTreeSet::new(),
        probe: run.venue.probe,
        begun: false,
        quiet_from: t0 + RANDOM_PHASE,
        first_quiet: None,
    };
    run.take_reports(&sim, 0, None);
    reads.take(&sim, true, &mut rng);
    let end_by = t0 + RANDOM_PHASE + LONGEST_HOLD + 4 * INTERVAL + AFTER;
    for now in t0..=end_by {
        let random = now < t0 + RANDOM_PHASE;
        let from = sim.reported.len();
        sim.advance_to(now as f64);
        run.take_reports(&sim, from, None);
        reads.take(&sim, random, &mut rng);
        // The answers due now, in any order.
        let mut due: Vec<Planned> = (reads.planned.iter())
            .filter(|p| p.answer_at <= now)
            .copied()
            .collect();
        reads.planned.retain(|p| p.answer_at > now);
        while !due.is_empty() {
            let p = due.remove(usize::try_from(rng.below(due.len() as u64)).unwrap());
            sim.requests.retain(|(tag, _)| *tag != p.tag);
            let on = markets_of(&subs);
            let from = sim.reported.len();
            match p.outcome {
                Outcome::Answer => {
                    // Anywhere in its span, often at either end.
                    let x = match p.sample {
                        Sample::Send => p.sent,
                        Sample::Answer => now,
                        Sample::Anywhere => rng.range(p.sent, now),
                    };
                    let (body, facts) = answer_at(&run.venue, &template, x, p.omit, &mut rng);
                    if run.tracing() {
                        run.note(format!(
                            "t{:+} answer to the read sent at t{:+}, sampled at t{:+}: {facts:?}",
                            now - t0,
                            p.sent - t0,
                            x - t0
                        ));
                    }
                    sim.answer(p.tag, &body).unwrap();
                    let halted = shown_halted(&run.venue, p.sent, now, &facts);
                    run.events.push(Event::Judged { at: now, facts });
                    run.take_reports(&sim, from, Some((p.sent, now)));
                    run.expect_halted(now, on, halted, "by an answer showing it halted");
                }
                outcome => {
                    if run.tracing() {
                        run.note(format!(
                            "t{:+} {outcome:?}: the read sent at t{:+}",
                            now - t0,
                            p.sent - t0
                        ));
                    }
                    let result = match outcome {
                        Outcome::TimedOut => sim.respond(p.tag, Err(HttpFailure::TimedOut)),
                        Outcome::Unavailable => sim.respond(
                            p.tag,
                            Ok(HttpResponse {
                                status: 503,
                                headers: &[],
                                body: b"",
                            }),
                        ),
                        _ => sim.answer(p.tag, b"{\"status\": \"NORMAL\""),
                    };
                    assert_eq!(result.is_err(), matches!(outcome, Outcome::Refused));
                    run.events.push(Event::Failed { at: now });
                    run.take_reports(&sim, from, None);
                    run.expect_halted(now, on, [true; 3], "after a read failed or was refused");
                }
            }
            reads.take(&sim, random, &mut rng);
        }
        // A change of subscriptions: one a probe makes, or one drawn while no probe runs.
        let forced = probe_change(run.venue.probe, now, &subs, &mut rng);
        let probing = run.venue.probe.is_some_and(|p| probe_runs(p, now));
        let drawn = random && !probing && (rng.chance(3) || nudges.contains(&now));
        if let Some(next) = forced.or_else(|| drawn.then(|| draw_change(&subs, &mut rng))) {
            let before = core::mem::replace(&mut subs, next);
            let (was, is) = (markets_of(&before), markets_of(&subs));
            if run.tracing() {
                run.note(format!("t{:+} subscribed {is:?} (was {was:?})", now - t0));
            }
            let add: Vec<Subscription> = subs.difference(&before).copied().collect();
            let remove: Vec<Subscription> = before.difference(&subs).copied().collect();
            let from = sim.reported.len();
            sim.subscribe(&add, &remove);
            sim.advance_to(now as f64);
            for m in 0..3 {
                if was[m] && !is[m] {
                    run.view[m] = None;
                }
                if !was[m] && is[m] {
                    run.events.push(Event::Subscribed {
                        at: now,
                        market: m,
                        begun: reads.begun,
                    });
                }
            }
            run.take_reports(&sim, from, None);
            reads.take(&sim, random, &mut rng);
            let again = [0, 1, 2].map(|m| !was[m] && is[m] && reads.begun);
            let why = "when subscribed once the poll had sent a read";
            run.expect_halted(now, is, again, why);
        }
        run.check_scheduled(now, markets_of(&subs));
        // Trading again, no later than the docs say: the window after the first read sent once
        // the venue is quiet.
        if let Some(first) = reads.first_quiet
            && now == first + AFTER
        {
            let on = markets_of(&subs);
            for m in 0..3 {
                let closed = m == EUR_AT && run.venue.regime == Regime::FxClose;
                if on[m] && !closed && run.view[m] != Some(TRADING) {
                    let what = format!(
                        "{} not trading the window after the first read sent once the venue was \
                         quiet (sent at t{:+}; view {:?})",
                        SYMBOLS[m],
                        first - t0,
                        run.view[m]
                    );
                    run.violation(Kind::Trading, now - t0, what);
                }
            }
        }
    }
    run
}

#[test]
fn no_market_is_reported_trading_while_an_auction_the_docs_cover_may_run_and_every_promised_report_comes_in_time()
 {
    let only: Option<u64> = std::env::var("STATUS_ORACLE_SEED")
        .ok()
        .map(|s| s.parse().unwrap());
    let runs: Option<u64> = std::env::var("STATUS_ORACLE_RUNS")
        .ok()
        .map(|s| s.parse().unwrap());
    let seeds: Vec<u64> = match (only, runs) {
        (Some(seed), _) => vec![seed],
        (None, Some(runs)) => (0..runs).collect(),
        (None, None) => SEEDS.collect(),
    };
    let mut violations: Vec<(Kind, String)> = Vec::new();
    for seed in seeds {
        let run = run(seed, only.is_some());
        if let Some(trace) = &run.trace {
            println!("{trace}");
        }
        violations.extend(run.violations);
    }
    let mut summary = String::new();
    for kind in [Kind::Guarantee, Kind::Halt, Kind::Trading] {
        let of: Vec<&String> = (violations.iter())
            .filter(|(k, _)| *k == kind)
            .map(|(_, line)| line)
            .collect();
        if !of.is_empty() {
            let _ = writeln!(summary, "{kind:?}: {} violations, the first:", of.len());
            for line in of.iter().take(6) {
                let _ = writeln!(summary, "  {line}");
            }
        }
    }
    assert!(violations.is_empty(), "{summary}");
}
