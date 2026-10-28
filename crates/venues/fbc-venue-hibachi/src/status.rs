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
//! It is planned for every set of subscriptions, alone and carrying none for the empty set, on
//! the same stream and transport, and `fbc-runtime`'s `MdVenue` keeps an endpoint planned again
//! on the same stream and transport, so the endpoint stays open while the venue's market data
//! runs (Verifier VE3-1); a poll endpoint's one epoch ends only when its session stops. One codec
//! thus serves every subscription taken meanwhile, and what it knows (that the reads have begun,
//! and the schedule, below) outlives the removal of every subscription. With no market subscribed
//! it sends no read and keeps its read timer. A codec of a newly opened endpoint (the venue's
//! market data started again) knows nothing an earlier one read or held.
//!
//! # Reads
//!
//! A codec learns the wall time only from [`MdCodec::on_timer`] (`on_open` and `on_http` carry
//! none), so every read is sent from a timer, its read timer: the first it asks for in `on_open`,
//! each later one [`KEY_STATUS_INTERVAL`](crate::KEY_STATUS_INTERVAL) after the one before,
//! whether or not the read sent from that one has been answered. A read timer sends a read only
//! while some market is subscribed. Each read is one public `GET` with no header, the REST
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
//! failed, or scheduled halt after it is subscribed, and a market subscribed once the reads have
//! begun (below) is reported halted at once. It is reported only through
//! [`MdEvent::Status`], to the consumer's handler; nothing else here reports it. A market is
//! [`TradingStatus::Trading`] after a read only if it was trading throughout the read's span,
//! from its send to the timer its answer is judged at:
//!
//! - the read lists its contract `LIVE` and the exchange `NORMAL`;
//! - it advertises no open later than the read's send, less the reopening window below, and no
//!   close at or before the answer's judged time;
//! - no scheduled closure below, nor the reopening window after one, meets the span, and the
//!   read was sent no earlier than the end of the reopening window counted from the latest
//!   reopening a read's judged time marks (below);
//! - and the read was sent after the market was subscribed, and no halt of that market came after
//!   the read's send, whether or not it changed the status reported: on a schedule, by another
//!   read's answer, or by a read that failed or was refused. So a read in flight across a halt, or
//!   across the market's subscription, never lifts it; only a read sent after them can (Verifier
//!   VE5-1).
//!
//! A read that meets every condition but the last shows the market trading at an instant that may
//! come before that halt or that subscription, so it changes nothing: it neither lifts the market
//! nor halts it, and a read sent after them still lifts it however many reads are in flight.
//!
//! Any other read reports it [`TradingStatus::Halted`]: a read listing it in any other status
//! (`PAUSED` for an FX weekend, the exchange in either maintenance), a read sent before its
//! advertised open or judged at or after its advertised close, whatever its status, and a read
//! that no longer lists it. A read that fails (no response, or a status other than 2xx) or whose
//! answer is refused reports every market halted at once, and only a read sent after it can lift
//! one, after the window that follows a reopening (below). The reads begin with the first read
//! the endpoint sends, for whichever markets are subscribed then, not with its first timer: a
//! read timer that fires while no market is subscribed sends no read (above) and begins nothing.
//! A market subscribed once the reads have begun, for the first time on the endpoint or again,
//! however long no market was subscribed meanwhile (the endpoint stays open, above), is held the
//! same way: the reads judged before showed its contract while no market of it was subscribed, so
//! none was judged for it, and the venue may have halted and reopened it meanwhile (Verifier
//! VE5-1). It is reported halted at the check asked for when it is subscribed, and only a read
//! sent after that can lift it, after that window. A market subscribed before the reads began,
//! however many timers had fired, is subscribed afresh, its status reported from the first read
//! judged: no read had been sent, so none showed anything of it. Nothing makes a market trading
//! but a read: never a schedule alone.
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
//! window that follows its reopening, shows it trading. A halt by an advertised close, or before
//! an advertised open, ends on the schedule only through an open the venue advertises (below);
//! one that ends otherwise is then a halt no schedule announces, and its window is counted as
//! one's (below).
//!
//! The schedule is what the judged answers advertise: each contract's open and close, and the
//! published windows. It is the endpoint's, kept whichever markets are subscribed, so a market
//! subscribed later is checked against it at once. An answer shows the venue at some instant
//! between its read's send and its judged time, so a read sent no earlier than an answer was
//! judged shows the venue no earlier than that answer does, and its answer replaces that one's.
//! Reads overlap when answers take longer than the interval, and the runtime hands each answer
//! over as it comes. Of two answers whose spans overlap (the later read sent before the earlier
//! answer was judged), in whichever order they come, which shows the venue later is not known, so
//! the schedule keeps both (Verifier VE5-2): for each contract the earliest close any answer in it
//! advertises and the latest open (each holding every span another holds; below), and every
//! window any publishes. Each read's own answer, and the windows standing, judge its span.
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
//! - and one after a halt no schedule announces: once a read has shown the market halted by its
//!   contract's status or the exchange's (a maintenance with or without a published window, one
//!   that overruns it, an FX pair still `PAUSED` past its calendar's reopening), a read has left
//!   it out, a read has failed or been refused (which shows nothing of the venue, so it may have
//!   halted and reopened the market meanwhile), a read has ended a halt by an advertised close
//!   or open other than through an open the venue advertises (below), or the market has been
//!   subscribed once the reads had begun (above; likewise), the first read that can lift it (sent
//!   after every halt of it and after its subscription) and shows it trading again shows the
//!   venue reopened it, if it halted it, at some instant no later than that read's judged time,
//!   so the window is counted from that time. A read sent before that halt and answered after it
//!   is not that read: it may show the venue before the halt. Each of these starts that window
//!   whatever already halts the market (a window, a calendar, a close or an open, the window
//!   after an earlier reopening).
//!
//! An advertised close holds a read's span when it is at or before the read's judged time, and an
//! advertised open when the window after it ends after the read's send. A close or an open by
//! which a read halts a market, whichever read advertised it, stays in the schedule, or one
//! holding every span it holds does, until an answer replaces the one that advertised it (above).
//! Each such halt also shows the market halted at an instant that any reopening after it follows:
//! a close the schedule holds the market by, at that close; a read whose own close holds its span,
//! at its judged time (it may show the venue as late as that); a read whose own open holds its
//! span, at that open or, if earlier, its judged time. The codec keeps the latest of these since a
//! read last showed the market reopened, and a halt by an advertised close or open ends in one of
//! two ways:
//!
//! - on the schedule, through an open: an answer replacing one of the schedule's advertises an
//!   open that holds its read's span, no earlier than that latest instant, and no close that holds
//!   it. That open is the venue's reopening (an open the venue moves stays one while it is no
//!   earlier), and a read sent once the window after it has ended, held by no other close or open,
//!   can lift the market whether or not it still advertises that open;
//! - or otherwise, at an answer replacing one of the schedule's that advertises no close holding
//!   its read's span and, while the schedule before it held the span by a close or an open, no
//!   open holding it either (no close or open, a close after its judged time, an open whose window
//!   ended no later than its send), or an open holding it earlier than that latest instant. The
//!   codec cannot tell a close or an open the venue withdrew from one it passed and then reopened
//!   the market after, and an open earlier than an instant the market was shown halted is not the
//!   reopening after that halt, so that halt ends as one no schedule announces.
//!
//! The answer's own close and open judge this, not the schedule after it: an answer of the
//! schedule it does not replace may show the venue before it does, so what that one still
//! advertises may be gone. A read advertising a close that holds its span keeps the market halted,
//! by that close, and an answer that replaces none of the schedule's (its read was sent before
//! every answer in it was judged, as one answered after a later-sent read's is) ends no halt. A
//! close or an open an answer stops advertising when it no longer holds, or never held, its read's
//! span changes nothing.
//!
//! What is guaranteed, then: no read whose span may meet a reopening the codec knows of, or the
//! window after it, reports the market trading, so a market is never reported trading while an
//! auction after one may still run. The codec knows what its own endpoint's reads showed and its
//! schedule held, and that endpoint stays open while the venue's market data runs (above). What
//! is not guaranteed is one reopening the codec may not know of: one after a halt that no
//! schedule announces and no read shows, when no read leaves the market out, fails or is
//! refused, and the market is not subscribed once the reads have begun (above), from the halt's
//! begin until a read reports the market trading again. Such a halt begins and ends between what
//! two reads show, or ends before what the first read judged shows of a market subscribed before
//! the reads began (above); it is never reported and may start no window.
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

/// What one judged answer advertised, as the schedule keeps it (the module docs).
struct Advertised {
    /// The wall time it was judged at: a read sent no earlier shows the venue no earlier.
    judged: WallNs,
    /// Each contract's advertised open and close, by symbol.
    contracts: BTreeMap<String, (Option<WallNs>, Option<WallNs>)>,
    /// Its published maintenance windows.
    windows: Vec<Window>,
}

/// The open and close the answers in `schedule` advertise for `symbol`: the latest open and the
/// earliest close, each holding every span any other holds (`None` orders before every open, and
/// a close is kept over none).
fn advertised_of<'a>(
    schedule: impl IntoIterator<Item = &'a Advertised>,
    symbol: &str,
) -> (Option<WallNs>, Option<WallNs>) {
    let mut kept = (None, None);
    for (open, close) in schedule.into_iter().filter_map(|a| a.contracts.get(symbol)) {
        kept = (kept.0.max(*open), earlier(kept.1, *close));
    }
    kept
}

/// One subscribed market.
struct Market {
    /// Its venue symbol, as the answer lists it.
    symbol: String,
    /// Its calendar, from its spec.
    sessions: Option<SessionCalendar>,
    /// The status last reported, `None` before the first.
    reported: Option<TradingStatus>,
    /// The halt count at its last halt, a halt counted when it was subscribed included: only a
    /// read sent after that can lift it.
    last_halt: u64,
    /// Whether a halt no schedule announces may have held it since a read that can lift it last
    /// showed it trading: a read showed it halted by its contract's status or the exchange's,
    /// left it out, or failed or was refused, a read ended a halt by an advertised close or open
    /// other than through an open no earlier than `shown_halted`, or it was subscribed once the
    /// reads had begun (the module docs).
    unannounced: bool,
    /// The judged time of the latest read that showed it reopened: the first read that could
    /// lift it to show it trading after such a halt (the module docs).
    reopened: Option<WallNs>,
    /// The latest instant an advertised close or open showed it halted since a read last showed
    /// it reopened (the module docs): any reopening after that halt is no earlier, so an open
    /// before it is not one. `None` for none. A read showing it trading leaves it as it is: it is
    /// then no later than that read's send less the window after a reopening, so before every
    /// open that can hold a later read.
    shown_halted: Option<WallNs>,
    /// Subscribed once the reads had begun, and not yet reported halted for it.
    held: bool,
}

/// The codec of the status poll endpoint: one epoch, which the endpoint's life is. The endpoint
/// is planned for every set of subscriptions, the empty one included, so it lives while the
/// venue's market data runs (the module docs).
pub struct HibachiStatus {
    settings: Result<Settings, ConfigError>,
    /// The subscriptions taken on the endpoint; every instrument they name is a market.
    subs: BTreeSet<Subscription>,
    markets: BTreeMap<InstrumentId, Market>,
    /// The reads sent and not yet judged, failed or refused.
    reads: BTreeMap<HttpTag, Read>,
    /// The schedule (the module docs): every judged answer not yet replaced by the answer of a
    /// read sent no earlier than its judged time, whichever markets were subscribed.
    schedule: Vec<Advertised>,
    /// The windows of answers replaced that had begun, kept while their end and the reopening
    /// window after it may still meet a read in flight.
    begun: Vec<Window>,
    /// Halts counted so far, each a market's.
    halts: u64,
    last_timer: u64,
    last_read: u64,
    /// The timer the next read is sent from.
    read_timer: Option<TimerTag>,
    /// The schedule check armed, and the instant it was armed for (`None`: at once).
    check: Option<(Option<WallNs>, TimerTag)>,
    /// Whether a timer has fired: a market subscribed before the first has its schedule checked
    /// at that timer, one subscribed after at once.
    started: bool,
    /// Whether a read has been sent, for whichever markets: the reads have begun (the module
    /// docs). A timer that fires with no market subscribed sends none, so begins nothing
    /// (Verifier VE4-1).
    read_sent: bool,
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
            schedule: Vec::new(),
            begun: Vec::new(),
            halts: 0,
            last_timer: 0,
            last_read: 0,
            read_timer: None,
            check: None,
            started: false,
            read_sent: false,
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
            self.read_sent = true;
        }
        self.read_timer = Some(self.arm(interval, fx));
    }

    /// Every market halted: a read failed or its answer was refused. It shows nothing of the
    /// venue, which may have halted and reopened any market meanwhile, so the first read that
    /// can lift one and shows it trading starts the window after a reopening (Verifier V-1).
    fn halt_all(&mut self, sink: &mut dyn MdSink) {
        let halts = &mut self.halts;
        for (&inst, market) in &mut self.markets {
            market.unannounced = true;
            halt(halts, inst, market, sink);
        }
    }

    /// The answer of a read sent at `sent` when `at_send` halts had been counted, judged at `at`,
    /// the wall time of the timer asked for when it came.
    fn judge(
        &mut self,
        (sent, at_send, answer): (WallNs, u64, Answer),
        at: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.counts.judged += 1;
        let after = self.reopening();
        let span = Span {
            from: sent,
            to: at,
            after,
        };
        // The windows standing judge the span too, those of the answers it replaces included.
        let mut known = self.schedule_windows();
        known.extend(answer.windows.iter().copied());
        // This answer replaces every answer of the schedule judged no later than its send: those
        // show the venue no later than it does. The others were judged after its send, so which
        // of the two shows the venue later is not known: it joins them (Verifier VE5-2).
        let advertised = Advertised {
            judged: at,
            contracts: (answer.contracts.iter())
                .map(|(symbol, l)| (symbol.clone(), (l.open, l.close)))
                .collect(),
            windows: answer.windows.clone(),
        };
        let (replaced, standing): (Vec<Advertised>, Vec<Advertised>) =
            core::mem::take(&mut self.schedule)
                .into_iter()
                .partition(|a| a.judged <= sent);
        let halts = &mut self.halts;
        for (&inst, market) in &mut self.markets {
            let listed = answer.contracts.get(&market.symbol);
            // Sent after every halt of the market, its subscription's included: a read that can
            // lift it.
            let lifts = market.last_halt <= at_send;
            let trading = listed.map(|l| l.status == TradingStatus::Trading);
            let (open, close) = listed.map_or((None, None), |l| (l.open, l.close));
            let (opens, closes) = (span.opens(open), span.closes(close));
            // An answer that replaces one or more of the schedule's (its read was sent once each
            // was judged), while its own answer holds its span by no close, ends a halt by an
            // advertised close or open as one no schedule announces when:
            // - the schedule before it held the span by a close or an open and its answer by no
            //   open either: it no longer advertises that close or open, and the codec cannot
            //   tell one the venue withdrew from one it passed and reopened the market after
            //   (V1C-2);
            // - or its answer holds the span by an open earlier than an instant a close or an
            //   open showed the market halted: no reopening after that halt, so not the venue's
            //   (Verifier VE2-1, VE2-2).
            // Its own answer judges this, not the schedule after it: an answer of the schedule it
            // does not replace may show the venue before it does, so what that one still
            // advertises may be gone (Verifier VE5-2). An answer that replaces none was sent
            // before every answer of the schedule was judged, so may show the venue before them
            // all: it ends no halt, and adds what it advertises (VE-1, VE2-2).
            if !replaced.is_empty() && !closes {
                let was = advertised_of(replaced.iter().chain(&standing), &market.symbol);
                let dropped = !opens && span.advertised(was.0, was.1);
                let early = opens && open.zip(market.shown_halted).is_some_and(|(o, h)| o < h);
                if dropped || early {
                    market.unannounced = true;
                }
            }
            match trading {
                // Its contract's or the exchange's status halts it, or the read leaves it out
                // (Verifier V-1) ...
                Some(false) | None => market.unannounced = true,
                // ... and the first read that can lift it to show it trading again shows the
                // venue reopened it, if it halted it, by the time it is judged, no later
                // (RA214-2): the window after that covers every halt shown before it.
                Some(true) if lifts && market.unannounced => {
                    market.unannounced = false;
                    market.reopened = Some(at);
                    market.shown_halted = None;
                }
                Some(true) => {}
            }
            // What its own answer shows: the market halted by a close at its judged time, and by
            // an open until that open or, if earlier, its judged time (it may show the venue as
            // late as that).
            if closes {
                market.shown_halted = market.shown_halted.max(Some(at));
            }
            if opens {
                market.shown_halted = market.shown_halted.max(open.map(|o| o.min(at)));
            }
            // The window after the reopening the reads showed, as after a scheduled one.
            let auction = market.reopened.is_some_and(|r| sent < later(r, after));
            let shown = trading == Some(true)
                && !auction
                && !span.meets(market.sessions.as_ref(), open, close, &known);
            if !shown {
                halt(halts, inst, market, sink);
            } else if lifts {
                report(market, inst, TradingStatus::Trading, sink);
            }
            // Otherwise the read shows the market trading but was sent before a halt of it: it
            // neither lifts that halt nor is one, so a read sent after that halt still can.
        }
        // The windows of the answers replaced that have begun stay while their end and the
        // window after it may still meet a read in flight (sent no later than `horizon`) or one
        // sent later; so do those kept before.
        let horizon = self.reads.values().map(|r| r.sent).fold(at, WallNs::min);
        let begun = (self.begun.iter())
            .chain(replaced.iter().flat_map(|a| a.windows.iter()))
            .copied()
            .filter(|w| w.begin <= at && horizon < later(w.end, after));
        self.begun = begun.collect();
        self.begun.sort_unstable();
        self.begun.dedup();
        self.schedule = standing;
        self.schedule.push(advertised);
        self.check_schedule(at, sink, fx);
    }

    /// The published windows the schedule holds: every answer's in it, and those kept once begun.
    fn schedule_windows(&self) -> Vec<Window> {
        let mut windows = self.begun.clone();
        windows.extend(self.schedule.iter().flat_map(|a| a.windows.iter().copied()));
        windows.sort_unstable();
        windows.dedup();
        windows
    }

    /// Reports halted every market subscribed once the reads had begun and not yet reported so,
    /// halts every market a scheduled closure holds at `at`, and arms the check of the next
    /// instant one starts.
    fn check_schedule(&mut self, at: WallNs, sink: &mut dyn MdSink, fx: &mut Effects) {
        let after = self.reopening();
        let span = Span {
            from: at,
            to: at,
            after,
        };
        let windows = self.schedule_windows();
        let (halts, schedule) = (&mut self.halts, &self.schedule);
        let mut next: Option<WallNs> = None;
        for (&inst, market) in &mut self.markets {
            // Its halt was counted when it was subscribed (`subscribe`).
            if core::mem::take(&mut market.held) {
                report(market, inst, TradingStatus::Halted, sink);
            }
            let (open, close) = advertised_of(schedule, &market.symbol);
            // A close the schedule holds it by shows it halted from that close's instant.
            if span.closes(close) {
                market.shown_halted = market.shown_halted.max(close);
            }
            if span.meets(market.sessions.as_ref(), open, close, &windows) {
                if market.reported != Some(TradingStatus::Halted) {
                    self.counts.scheduled_halts += 1;
                }
                halt(halts, inst, market, sink);
            }
            next = [
                next,
                next_halt(market.sessions.as_ref(), close, &windows, at),
            ]
            .into_iter()
            .flatten()
            .min();
        }
        let armed = self.check.map(|(when, _)| when);
        if let Some(next) = next.filter(|next| armed != Some(Some(*next))) {
            let wait = u64::try_from(next.0.saturating_sub(at.0)).unwrap_or(0);
            let tag = self.arm(Duration::from_nanos(wait), fx);
            self.check = Some((Some(next), tag));
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
        let maintained = windows
            .iter()
            .any(|w| w.begin <= to && from < later(w.end, after));
        calendar || self.advertised(open, close) || maintained
    }

    /// Whether an advertised close or open holds the market over the span.
    fn advertised(self, open: Option<WallNs>, close: Option<WallNs>) -> bool {
        self.opens(open) || self.closes(close)
    }

    /// Whether an advertised close holds the market over the span: it is at or before its end.
    fn closes(self, close: Option<WallNs>) -> bool {
        close.is_some_and(|close| close <= self.to)
    }

    /// Whether an advertised open holds the market over the span: the window after it ends
    /// after its start.
    fn opens(self, open: Option<WallNs>) -> bool {
        open.is_some_and(|open| self.from < later(open, self.after))
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

/// The first instant after `at` a scheduled closure of a market starts, while it is not held by
/// one already: its calendar's next close, its advertised close, a window's begin.
fn next_halt(
    sessions: Option<&SessionCalendar>,
    close: Option<WallNs>,
    windows: &[Window],
    at: WallNs,
) -> Option<WallNs> {
    let calendar = sessions
        .filter(|cal| cal.is_open(at))
        .and_then(|cal| cal.next_closed(at));
    let close = close.filter(|close| *close > at);
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
    /// [`plan_md`](crate::Hibachi) refuses it. Nothing is sent; a market subscribed once a timer
    /// has fired has its schedule checked at once (one subscribed before, at the first timer),
    /// and only a read sent after it is subscribed can lift it. One subscribed before the reads
    /// began has its status reported from the first read judged; one subscribed once they had
    /// begun (a read was sent), for the first time or again, is reported halted at that check and
    /// held as across a failed read (the module docs).
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
                // A halt is counted now, so only a read sent after this can lift the market or
                // report it trading: one sent before may show the venue before anything the
                // reads judged since showed of it (Verifier VE5-1).
                self.halts += 1;
                // Subscribed once the reads had begun (a read was sent), for the first time or
                // again: what the reads judged before showed of its contract was judged for no
                // market of it, so the venue may have halted and reopened it meanwhile, as across
                // a failed read. It is marked so and reported halted at the check asked for
                // below (Verifier VE2-4, VE5-1). Before the reads began, nothing was read, and it
                // is subscribed afresh (VE3-2, VE4-1).
                let held = self.read_sent;
                let market = Market {
                    symbol: spec.venue_symbol.as_wire().to_owned(),
                    sessions: spec.sessions.clone(),
                    reported: None,
                    last_halt: self.halts,
                    unannounced: held,
                    reopened: None,
                    shown_halted: None,
                    held,
                };
                self.markets.insert(inst, market);
                added = true;
            }
        }
        if added && self.started {
            let tag = self.arm(Duration::ZERO, fx);
            self.check = Some((None, tag));
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
        let read = judged.and_then(|http| self.reads.remove(&http));
        if let Some(Read {
            sent,
            halts,
            answer: Some((answer, _)),
        }) = read
        {
            self.judge((sent, halts, answer), wall, sink, fx);
            return;
        }
        if self.check.is_some_and(|(_, armed)| armed == tag) {
            self.check = None;
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
