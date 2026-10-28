//! The status poll: each subscribed market's trading status, re-read after discovery from
//! `GET /market/exchange-info` and reported as [`MdEvent::Status`] (FBC-1632; decisions 0002,
//! 0027 and 0130).
//!
//! Discovery's status is a snapshot of one answer, and `/ws/market` carries no status topic (the
//! SDK's `WebSocketSubscriptionTopic`), so [`plan_md`](crate::Hibachi) plans this poll endpoint
//! ([`STATUS_STREAM`](crate::STATUS_STREAM), `MdTransport::Poll` on the market-data REST origin)
//! beside the `/ws/market` connection, carrying the same subscriptions: every instrument one of
//! them names is a market it reports. It opens no connection; its codec, [`HibachiStatus`],
//! asks the runtime for each read and each timer (decision 0002).
//!
//! # Reads
//!
//! A codec learns the wall time only from [`MdCodec::on_timer`] (`on_open` and `on_http` carry
//! none), so every read is sent from a timer: the first from one it asks for in `on_open`, each
//! later one [`KEY_STATUS_INTERVAL`](crate::KEY_STATUS_INTERVAL) after the one before, whether
//! or not the one before has been answered. Each is one public `GET` with no header, the REST
//! timeout, one request against the IP's REST limit (`caps.rs`), as discovery's. An answer that
//! arrives is read at once and judged at the wall time of a timer the codec asks for then, after
//! no delay: a read shows the venue at some instant between its send and its answer, and only a
//! timer says when the answer came.
//!
//! An answer is read as discovery reads it ([`discover`]): the exchange-level
//! `status`, and each contract's `status` mapped by the one function discovery maps it with (only
//! `LIVE` while the exchange is `NORMAL` is trading), an absent, `null` or undocumented status
//! refusing the answer; each contract's `symbol`, and its `marketOpenTimestamp` and
//! `marketCloseTimestamp` read as discovery reads them (decimal Unix seconds in a string, none
//! after 2100-01-01 UTC). It also reads `maintenanceWindow`, a list of `{ begin, end, note }`
//! (the SDK's `MaintenanceWindow`; its README: seconds since the Unix epoch): each bound a decimal
//! count of Unix seconds in a JSON number or a string (the Market API spec types both as strings,
//! the SDK and its test data as numbers), by the same rule and bound, the begin rounded down and
//! the end up to whole nanoseconds, the end after the begin; an absent or `null` list publishes
//! none. A JSON number with a fraction is read at the precision `serde_json` holds it in, an
//! `f64`'s, under a microsecond at these magnitudes. Anything else refuses the answer, naming the
//! part, and so does a symbol listed twice. No other field of a contract is read here.
//!
//! # What is reported
//!
//! Each market's [`TradingStatus`] is reported each time it differs from the status last
//! reported for it, and only then: the first report of a market is the first read judged, read
//! failed, or scheduled halt after it is subscribed. It is reported only through
//! [`MdEvent::Status`], to the consumer's handler; nothing else here reports it. A market is
//! [`TradingStatus::Trading`] after a read only if it was trading throughout the read's span,
//! from its send to the timer its answer is judged at:
//!
//! - the read lists its contract `LIVE` and the exchange `NORMAL`;
//! - it advertises no open later than the read's send, less the reopening window below, and no
//!   close at or before the answer's judged time;
//! - no scheduled closure below, nor the reopening window after one, meets the span, and the
//!   read was sent no earlier than the reopening window after the latest reopening its reads
//!   showed ends (below);
//! - and no halt of that market came after the read's send, whether or not it changed the status
//!   reported: on a schedule, by another read's answer, or by a read that failed or was refused.
//!   So a read in flight across a halt never lifts it; only a read sent after it can.
//!
//! A read that meets every condition but the last shows the market trading no later than a halt
//! after its send, so it changes nothing: it neither lifts that halt nor is one, and a read sent
//! after the halt still lifts it however many reads are in flight.
//!
//! Any other read reports it [`TradingStatus::Halted`]: a read listing it in any other status
//! (`PAUSED` for an FX weekend, the exchange in either maintenance), a read sent before its
//! advertised open or judged at or after its advertised close, whatever its status, and a read
//! that no longer lists it. A read that fails (no response, or a status other than 2xx) or whose
//! answer is refused reports every market halted at once, until a read sent after it succeeds.
//! Nothing makes a market trading but a read: never a schedule alone.
//!
//! # Halts on a schedule
//!
//! Some halts are known before they start, and polling alone would report each up to one
//! interval late, so between reads the codec also halts a market at its time, on a timer it asks
//! the runtime for at the next of them, and judges every read's span against them:
//!
//! - the begin of each maintenance window the schedule below holds (kept, once begun, until its
//!   end and the reopening window after it have passed every read still in flight);
//! - the end of a session in the contract's calendar (its spec's [`SessionCalendar`]: an FX
//!   pair's New York weekend, the final close and first open discovery read; decision 0125),
//!   found by [`SessionCalendar::next_closed`];
//! - and the close (`marketCloseTimestamp`) the schedule holds for it, so the earlier of that and
//!   the calendar's end.
//!
//! A market a scheduled closure holds stays halted until a read sent after it, and after the
//! window that follows its reopening, shows it trading.
//!
//! The schedule is the last read's: each market's advertised open and close, and the published
//! windows, of the latest-sent read judged. A read's answer replaces the schedule of every read
//! sent before it. Reads overlap when answers take longer than the interval, and the runtime hands
//! each answer over as it comes, so a read sent earlier can be answered after one sent later.
//! Which of the two shows the venue later is then not known, so the earlier read's answer removes
//! nothing from the schedule and adds what it publishes: the earlier of its close and the one
//! standing, and its windows. Its own answer, and the windows standing, judge its span.
//!
//! # The window after a reopening
//!
//! Hibachi runs a call auction when trading resumes after a scheduled maintenance and when FX
//! markets reopen after the weekend (docs.hibachi.xyz, Trading, Call auctions; during it the book
//! may cross and nothing trades), and no source says which contract or exchange status it shows.
//! Until FBC-vg2n records that from evidence, a market is halted for the window
//! [`KEY_AUCTION_HALT`](crate::KEY_AUCTION_HALT) configures after each reopening the codec knows
//! of, a superset of the two the docs name:
//!
//! - a scheduled one: a published maintenance window's end, a reopening of its calendar (the FX
//!   weekend's end, and its first open), and an advertised open, each at its instant;
//! - and one its reads show: once a read has shown the market halted by its contract's status or
//!   the exchange's (a maintenance with or without a published window, one that overruns it, an
//!   FX pair still `PAUSED` past its calendar's reopening), the first read that can lift it
//!   (sent after every halt of it) and shows it trading again shows the venue reopened it at some
//!   instant no later than that read's judged time, so the window is counted from that time.
//!   A read sent before that halt and answered after it is not that read: it may show the venue
//!   before the halt.
//!
//! What is guaranteed, then: no read whose span may meet such a reopening or the window after it
//! reports the market trading, so a market is never reported trading while an auction after one
//! may still run. What is not: a halt and a reopening that no schedule announces and no read
//! shows, both falling between two reads, are never reported and start no window, and neither
//! does a reopening before the poll's first read.
//!
//! # Limits
//!
//! A change no schedule announces (a pair paused early, an unscheduled maintenance) reaches the
//! consumer at the next read, up to one interval late. What reads the status in this library is
//! `fbc-oms`'s execution planner, from the `PlanVenue::status` the consumer passes it (decision
//! 0111): a command the consumer authorizes through `fbc-oms`'s registry directly is not checked
//! against status.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    ConfigError, DecodeError, DecodeScope, Effect, Effects, HttpFailure, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentId, Keepalive, MdCodec, MdEvent, MdSink, MonoNs, PlanError,
    RawFrame, SessionCalendar, SpecTable, StreamId, Subscription, TimerTag, TradingStatus,
    VenueConfig, VenueError, VenueMeta, WallNs, WireUrl,
};
use serde_json::{Map, Value};

use crate::config::Settings;
use crate::discover::{
    self, EXCHANGE_INFO_PATH, Round, advertised, answer_doc, contract_status, exchange_normal,
    future_contracts, malformed, text, unix_seconds,
};
use crate::md;

/// The name a refused maintenance window is refused under.
const MAINTENANCE_WINDOW: &str = "maintenanceWindow";

/// What the status poll counted.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct HibachiStatusCounts {
    /// Reads sent.
    pub reads: u64,
    /// Reads that failed: no response, or one with a status other than 2xx.
    pub failed_reads: u64,
    /// Answers refused as not documented.
    pub refused_answers: u64,
    /// Answers judged.
    pub judged: u64,
    /// Markets a timer found in a scheduled closure while not reported halted, so halted then.
    pub scheduled_halts: u64,
    /// Timers that act on nothing: a schedule check superseded by a later one.
    pub stale_timers: u64,
}

/// A published maintenance window: closed from `begin` until `end`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Window {
    begin: WallNs,
    end: WallNs,
}

/// What one answer lists of a contract.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Listed {
    /// Its status, mapped as discovery maps it.
    status: TradingStatus,
    /// The open and close it advertises.
    open: Option<WallNs>,
    close: Option<WallNs>,
}

/// One answer, read.
#[derive(Clone, Eq, PartialEq, Debug)]
struct Answer {
    /// Each contract it lists, by symbol.
    contracts: BTreeMap<String, Listed>,
    windows: Vec<Window>,
}

/// One read sent.
struct Read {
    /// The wall time of the timer it was sent from.
    sent: WallNs,
    /// The halts counted when it was sent: a market halted after it is not lifted by it.
    halts: u64,
    /// Its answer, read, and the timer it is judged at.
    answer: Option<(Answer, TimerTag)>,
}

/// One subscribed market.
struct Market {
    /// Its venue symbol, as the answer lists it.
    symbol: String,
    /// Its calendar, from its spec.
    sessions: Option<SessionCalendar>,
    /// The status last reported, `None` before the first.
    reported: Option<TradingStatus>,
    /// The halt count at its last halt; 0 for none.
    last_halt: u64,
    /// The open and close of its schedule (the module docs).
    open: Option<WallNs>,
    close: Option<WallNs>,
    /// The read (its `HttpTag`'s number) whose open and close those are, or the latest-sent of
    /// the reads they were gathered from; 0 for none.
    schedule_of: u64,
    /// Whether the last read to show its status, by its contract's or the exchange's, showed it
    /// halted, and no read that can lift it has shown it trading since.
    status_halted: bool,
    /// The judged time of the latest read that showed it reopened: the first read that could
    /// lift it to show it trading after one showed it halted by status (the module docs).
    reopened: Option<WallNs>,
}

/// The codec of the status poll endpoint: one epoch, which the endpoint's life is.
pub struct HibachiStatus {
    settings: Result<Settings, ConfigError>,
    /// The subscriptions taken on the endpoint; every instrument they name is a market.
    subs: BTreeSet<Subscription>,
    markets: BTreeMap<InstrumentId, Market>,
    /// The reads sent and not yet judged, failed or refused.
    reads: BTreeMap<HttpTag, Read>,
    /// The maintenance windows of the schedule (the module docs), and every earlier one that
    /// has begun while its end and the reopening window may still meet a read in flight.
    windows: Vec<Window>,
    /// The read whose windows those are, or the latest-sent of the reads they were gathered
    /// from; 0 for none.
    windows_of: u64,
    /// Halts counted so far, each a market's.
    halts: u64,
    last_timer: u64,
    last_read: u64,
    /// The timer the next read is sent from.
    read_timer: Option<TimerTag>,
    /// The schedule check armed, and the instant it was armed for (`None`: at once).
    schedule: Option<(Option<WallNs>, TimerTag)>,
    /// Whether a timer has fired: the first read's timer checks the schedule of every market
    /// subscribed before it.
    started: bool,
    counts: HibachiStatusCounts,
}

impl HibachiStatus {
    /// The codec of the poll endpoint on `stream` under `cfg`. A configuration the factory would
    /// refuse gives a codec that refuses every subscription with the same error and reads
    /// nothing.
    pub fn new(_stream: StreamId, cfg: &VenueConfig) -> HibachiStatus {
        HibachiStatus {
            settings: Settings::read(cfg),
            subs: BTreeSet::new(),
            markets: BTreeMap::new(),
            reads: BTreeMap::new(),
            windows: Vec::new(),
            windows_of: 0,
            halts: 0,
            last_timer: 0,
            last_read: 0,
            read_timer: None,
            schedule: None,
            started: false,
            counts: HibachiStatusCounts::default(),
        }
    }

    /// What it counted so far.
    pub fn counts(&self) -> HibachiStatusCounts {
        self.counts
    }

    /// A fresh timer, `after` from now.
    fn arm(&mut self, after: Duration, fx: &mut Effects) -> TimerTag {
        self.last_timer += 1;
        let tag = TimerTag(self.last_timer);
        fx.push(Effect::Timer { tag, after });
        tag
    }

    /// The window after a reopening, in nanoseconds.
    fn reopening(&self) -> i64 {
        self.settings.as_ref().map_or(0, |s| nanos(s.auction_halt))
    }

    /// Sends a read at `at`, if any market is subscribed, and arms the next one. The read timer
    /// is armed only under a configuration read (`on_open`).
    fn send_read(&mut self, at: WallNs, fx: &mut Effects) {
        let read = self.settings.as_ref().map(|s| {
            let url = format!("{}{EXCHANGE_INFO_PATH}", s.rest_origin);
            (url, s.rest_timeout, s.status_interval)
        });
        if let Ok((url, timeout, interval)) = read {
            self.read(at, url, timeout, interval, fx);
        }
    }

    /// Sends a read of `url` at `at`, if any market is subscribed, and arms the next one
    /// `interval` later.
    fn read(
        &mut self,
        at: WallNs,
        url: String,
        timeout: Duration,
        interval: Duration,
        fx: &mut Effects,
    ) {
        if !self.markets.is_empty() {
            self.last_read += 1;
            let tag = HttpTag(self.last_read);
            fx.push(discover::request(tag, WireUrl::plain(url), timeout));
            let read = Read {
                sent: at,
                halts: self.halts,
                answer: None,
            };
            self.reads.insert(tag, read);
            self.counts.reads += 1;
        }
        self.read_timer = Some(self.arm(interval, fx));
    }

    /// Every market halted: a read failed or its answer was refused.
    fn halt_all(&mut self, sink: &mut dyn MdSink) {
        let halts = &mut self.halts;
        for (&inst, market) in &mut self.markets {
            halt(halts, inst, market, sink);
        }
    }

    /// The answer of read `read`, sent at `sent` when `at_send` halts had been counted, judged
    /// at `at`, the wall time of the timer asked for when it came.
    fn judge(
        &mut self,
        read: u64,
        (sent, at_send, answer): (WallNs, u64, Answer),
        at: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.counts.judged += 1;
        let after = self.reopening();
        let mut known = self.windows.clone();
        known.extend(answer.windows.iter().copied());
        let span = Span {
            from: sent,
            to: at,
            after,
        };
        let halts = &mut self.halts;
        for (&inst, market) in &mut self.markets {
            let listed = answer.contracts.get(&market.symbol);
            // Sent after every halt of the market: a read that can lift it.
            let lifts = market.last_halt <= at_send;
            let trading = listed.map(|l| l.status == TradingStatus::Trading);
            match trading {
                // Its contract's or the exchange's status halts it ...
                Some(false) => market.status_halted = true,
                // ... and the first read that can lift it to show it trading again shows the
                // venue reopened it by the time it is judged, no later (RA214-2).
                Some(true) if lifts && market.status_halted => {
                    market.status_halted = false;
                    market.reopened = Some(at);
                }
                _ => {}
            }
            let (open, close) = listed.map_or((None, None), |l| (l.open, l.close));
            // The window after the reopening the reads showed, as after a scheduled one.
            let auction = market.reopened.is_some_and(|r| sent < later(r, after));
            let shown = trading == Some(true)
                && !auction
                && !span.meets(market.sessions.as_ref(), open, close, &known);
            if read > market.schedule_of {
                (market.open, market.close) = (open, close);
                market.schedule_of = read;
            } else {
                // A read sent before the one whose schedule stands: the earlier close stands,
                // and nothing is removed. (An open it advertises matters only while its send
                // precedes that open and the window after it; it has then just halted the
                // market, and that halt holds every read sent before now.)
                market.close = earlier(market.close, close);
            }
            if !shown {
                halt(halts, inst, market, sink);
            } else if lifts {
                report(market, inst, TradingStatus::Trading, sink);
            }
            // Otherwise the read shows the market trading but was sent before a halt of it: it
            // neither lifts that halt nor is one, so a read sent after that halt still can.
        }
        let windows: Vec<Window> = if read > self.windows_of {
            self.windows_of = read;
            // The answer's windows, and every earlier one begun whose end and the window after
            // it may still meet a read in flight (sent no later than `horizon`).
            let horizon = self.reads.values().map(|r| r.sent).fold(at, WallNs::min);
            (self.windows.iter().copied())
                .filter(|w| w.begin <= at && horizon < later(w.end, after))
                .chain(answer.windows)
                .collect()
        } else {
            // A read sent before the one whose windows stand: its windows are added.
            known
        };
        self.windows = windows;
        self.windows.sort_unstable();
        self.windows.dedup();
        self.check_schedule(at, sink, fx);
    }

    /// Halts every market a scheduled closure holds at `at`, and arms the check of the next
    /// instant one starts.
    fn check_schedule(&mut self, at: WallNs, sink: &mut dyn MdSink, fx: &mut Effects) {
        let after = self.reopening();
        let span = Span {
            from: at,
            to: at,
            after,
        };
        let (halts, windows) = (&mut self.halts, &self.windows);
        for (&inst, market) in &mut self.markets {
            if span.meets(market.sessions.as_ref(), market.open, market.close, windows) {
                if market.reported != Some(TradingStatus::Halted) {
                    self.counts.scheduled_halts += 1;
                }
                halt(halts, inst, market, sink);
            }
        }
        let next = (self.markets.values())
            .filter_map(|m| next_halt(m, &self.windows, at))
            .min();
        let armed = self.schedule.map(|(when, _)| when);
        if let Some(next) = next.filter(|next| armed != Some(Some(*next))) {
            let wait = u64::try_from(next.0.saturating_sub(at.0)).unwrap_or(0);
            let tag = self.arm(Duration::from_nanos(wait), fx);
            self.schedule = Some((Some(next), tag));
        }
    }
}

/// Reports `status` for `inst` when it differs from the status last reported.
fn report(market: &mut Market, inst: InstrumentId, status: TradingStatus, sink: &mut dyn MdSink) {
    if market.reported != Some(status) {
        market.reported = Some(status);
        sink.push(VenueMeta::NONE, MdEvent::Status { inst, status });
    }
}

/// Counts a halt of `inst` and reports it halted when it was not.
fn halt(halts: &mut u64, inst: InstrumentId, market: &mut Market, sink: &mut dyn MdSink) {
    *halts += 1;
    market.last_halt = *halts;
    report(market, inst, TradingStatus::Halted, sink);
}

/// From `from` to `to`, with the window after a reopening (`after`, in nanoseconds).
#[derive(Copy, Clone)]
struct Span {
    from: WallNs,
    to: WallNs,
    after: i64,
}

impl Span {
    /// Whether a scheduled closure of a market, or the window after it reopens, meets the span:
    /// its calendar, the open and close advertised, and the published maintenance windows.
    fn meets(
        self,
        sessions: Option<&SessionCalendar>,
        open: Option<WallNs>,
        close: Option<WallNs>,
        windows: &[Window],
    ) -> bool {
        let Span { from, to, after } = self;
        // Closed at some instant from `after` before the span until its end.
        let calendar = sessions.is_some_and(|cal| {
            let closed = cal.next_closed(WallNs(from.0.saturating_sub(after)));
            closed.is_some_and(|closed| closed <= to)
        });
        let closes = close.is_some_and(|close| close <= to);
        let opens = open.is_some_and(|open| from < later(open, after));
        let maintained = windows
            .iter()
            .any(|w| w.begin <= to && from < later(w.end, after));
        calendar || closes || opens || maintained
    }
}

/// The earlier of two closes, either one when the other is not known.
fn earlier(a: Option<WallNs>, b: Option<WallNs>) -> Option<WallNs> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// `at` moved on by `by` nanoseconds, saturating.
fn later(at: WallNs, by: i64) -> WallNs {
    WallNs(at.0.saturating_add(by))
}

/// The first instant after `at` a scheduled closure of `market` starts, while it is not held by
/// one already: its calendar's next close, its advertised close, a window's begin.
fn next_halt(market: &Market, windows: &[Window], at: WallNs) -> Option<WallNs> {
    let calendar = (market.sessions.as_ref())
        .filter(|cal| cal.is_open(at))
        .and_then(|cal| cal.next_closed(at));
    let close = market.close.filter(|close| *close > at);
    let window = windows.iter().map(|w| w.begin).filter(|b| *b > at).min();
    [calendar, close, window].into_iter().flatten().min()
}

/// A duration in whole nanoseconds, saturating.
fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// An exchange-info answer as the poll reads it (the module docs).
fn decode(body: &[u8]) -> Result<Answer, PlanError> {
    let doc = answer_doc(body)?;
    let normal = exchange_normal(&doc)?;
    let mut contracts = BTreeMap::new();
    for contract in future_contracts(&doc)? {
        let listed = Listed {
            status: contract_status(contract, normal)?,
            open: advertised(contract, "marketOpenTimestamp", Round::Up)?,
            close: advertised(contract, "marketCloseTimestamp", Round::Down)?,
        };
        let symbol = text(contract, "symbol")?.to_owned();
        if contracts.insert(symbol, listed).is_some() {
            return Err(malformed("symbol listed twice").into());
        }
    }
    let windows = maintenance_windows(&doc)?;
    Ok(Answer { contracts, windows })
}

/// `maintenanceWindow`: none when absent or `null`, otherwise a list of windows each with a
/// `begin` and an `end` in decimal Unix seconds, a number or a string, the end after the begin.
fn maintenance_windows(doc: &Map<String, Value>) -> Result<Vec<Window>, PlanError> {
    let list = match doc.get(MAINTENANCE_WINDOW) {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(list)) => list,
        Some(_) => return Err(malformed(MAINTENANCE_WINDOW).into()),
    };
    let bad = || PlanError::from(malformed(MAINTENANCE_WINDOW));
    list.iter()
        .map(|window| {
            let window = window.as_object().ok_or_else(bad)?;
            let begin = bound(window.get("begin"), Round::Down).ok_or_else(bad)?;
            let end = bound(window.get("end"), Round::Up).ok_or_else(bad)?;
            (end > begin)
                .then_some(Window { begin, end })
                .ok_or_else(bad)
        })
        .collect()
}

/// A window's bound: decimal Unix seconds in a JSON number, as it is written, or a string.
fn bound(value: Option<&Value>, round: Round) -> Option<WallNs> {
    match value? {
        Value::String(text) => unix_seconds(text, round),
        Value::Number(n) => unix_seconds(&n.to_string(), round),
        _ => None,
    }
}

/// The decode error an answer is refused with, naming the part.
fn refusal(err: PlanError) -> DecodeError {
    match err {
        PlanError::Decode(err) => err,
        PlanError::Missing(part) => DecodeError::Malformed(part),
        // The parser raises no other.
        _ => DecodeError::Malformed("exchange-info answer"),
    }
}

impl MdCodec for HibachiStatus {
    /// Asks for the timer the first read is sent from, at once.
    fn on_open(&mut self, fx: &mut Effects) {
        if self.settings.is_ok() {
            self.read_timer = Some(self.arm(Duration::ZERO, fx));
        }
    }

    /// Takes the difference: every instrument a subscription names is a market, refused as
    /// [`plan_md`](crate::Hibachi) refuses it. Nothing is sent; a market subscribed once the
    /// reads have begun has its schedule checked at once, its status reported from the next
    /// read judged.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        let settings = self
            .settings
            .as_ref()
            .map_err(|err| VenueError::Config(*err))?;
        for sub in add.iter().chain(remove) {
            md::plannable(settings, specs, *sub)?;
        }
        for sub in remove {
            self.subs.remove(sub);
        }
        self.subs.extend(add.iter().copied());
        let wanted: BTreeSet<InstrumentId> = self.subs.iter().map(|s| s.inst).collect();
        self.markets.retain(|inst, _| wanted.contains(inst));
        let mut added = false;
        for inst in wanted {
            if self.markets.contains_key(&inst) {
                continue;
            }
            // Checked above: every subscription's instrument is in `specs`.
            if let Some(spec) = specs.get(inst) {
                let market = Market {
                    symbol: spec.venue_symbol.as_wire().to_owned(),
                    sessions: spec.sessions.clone(),
                    reported: None,
                    last_halt: 0,
                    open: None,
                    close: None,
                    schedule_of: 0,
                    status_halted: false,
                    reopened: None,
                };
                self.markets.insert(inst, market);
                added = true;
            }
        }
        if added && self.started {
            let tag = self.arm(Duration::ZERO, fx);
            self.schedule = Some((None, tag));
        }
        Ok(())
    }

    /// Refused: a poll endpoint receives no frame.
    fn on_frame(
        &mut self,
        _f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("the status poll receives no frame"))
    }

    /// A read's answer: read whole and judged at the timer it asks for at once. A failure, a
    /// status other than 2xx or a refused answer reports every market halted now; a refused
    /// answer is also the error.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let pending = self.reads.get(&tag).is_some_and(|r| r.answer.is_none());
        if !pending {
            return Err(DecodeError::Malformed(
                "a response to no exchange-info read",
            ));
        }
        let body = match resp {
            Ok(resp) if (200..300).contains(&resp.status) => resp.body,
            _ => {
                self.reads.remove(&tag);
                self.counts.failed_reads += 1;
                self.halt_all(sink);
                return Ok(());
            }
        };
        match decode(body) {
            Ok(answer) => {
                let timer = self.arm(Duration::ZERO, fx);
                if let Some(read) = self.reads.get_mut(&tag) {
                    read.answer = Some((answer, timer));
                }
                Ok(())
            }
            Err(err) => {
                self.reads.remove(&tag);
                self.counts.refused_answers += 1;
                self.halt_all(sink);
                Err(refusal(err))
            }
        }
    }

    /// The read timer sends a read; an answer's timer judges it; a schedule check halts the
    /// markets a scheduled closure holds. Each then arms the check of the next scheduled halt.
    fn on_timer(
        &mut self,
        tag: TimerTag,
        _now: MonoNs,
        wall: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.started = true;
        if self.read_timer == Some(tag) {
            self.check_schedule(wall, sink, fx);
            self.send_read(wall, fx);
            return;
        }
        let judged = self.reads.iter().find_map(|(&http, read)| {
            let (_, timer) = read.answer.as_ref()?;
            (*timer == tag).then_some(http)
        });
        let read = judged.and_then(|http| Some((http, self.reads.remove(&http)?)));
        if let Some((
            http,
            Read {
                sent,
                halts,
                answer: Some((answer, _)),
            },
        )) = read
        {
            self.judge(http.0, (sent, halts, answer), wall, sink, fx);
            return;
        }
        if self.schedule.is_some_and(|(_, armed)| armed == tag) {
            self.schedule = None;
            self.check_schedule(wall, sink, fx);
            return;
        }
        self.counts.stale_timers += 1;
    }

    /// None: a poll endpoint has no keepalive.
    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    /// The exchange information is public: no credential.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_bound_reads_alike_as_a_number_and_a_string() {
        let as_number = bound(Some(&serde_json::json!(1_830_000_000)), Round::Down);
        let as_float = bound(Some(&serde_json::json!(1_830_000_000.0)), Round::Down);
        let as_text = bound(Some(&serde_json::json!("1830000000")), Round::Down);
        let at = Some(WallNs(1_830_000_000 * 1_000_000_000));
        assert_eq!((as_number, as_float, as_text), (at, at, at));
        // Milliseconds, a negative count, an exponent, a boolean: none is a bound.
        for refused in [
            serde_json::json!(1_830_000_000_000_u64),
            serde_json::json!(-5),
            serde_json::json!(1e21),
            serde_json::json!(true),
            serde_json::json!("1830000000000"),
        ] {
            assert_eq!(bound(Some(&refused), Round::Down), None, "{refused}");
        }
        assert_eq!(bound(None, Round::Up), None);
    }

    #[test]
    fn a_refusal_names_the_part() {
        let missing = refusal(PlanError::Missing("status"));
        assert_eq!(missing, DecodeError::Malformed("status"));
        let other = refusal(PlanError::Answers);
        assert_eq!(other, DecodeError::Malformed("exchange-info answer"));
    }
}
