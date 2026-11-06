//! FBC-vmw's done line, its second half: `continuity` fails against a toy variant that ignores a
//! sequence break, naming every break it missed. Around it, every other way the four
//! market-data checks fail or are skipped: a toy that reports a gap in sequence, stamps a
//! timestamp its frame lacks, puts a book's frames on another book or declares channels its
//! frames do not show, refuses a subscription; caps that declare no book, or only unchained
//! ones, or binary market data and its longer-block sub-case; and fixture
//! directories missing a directory or a case, holding a stray case, a malformed line, a refused
//! frame or a case that states nothing.
//!
//! FBC-n35m (decision 0124): `continuity` holds a prune or a clear of one side of a broken book as
//! it holds a level, and counts either as the codec taking the sequence; `book_channels` requires
//! both on the channel's own book, and reads neither as showing liquidity.
//!
//! FBC-fhk4's done line, its second half: the toy's `rpi_book`, anchored on REST, is driven from
//! case files whose `http` lines answer its anchors, and `continuity` fails a toy variant that
//! ignores that channel's sequence breaks. Around it: the twists hold what the toy pushes from an
//! anchor's answer as from a frame; an answer is no frame for a case's sequence taken or a
//! longer block; an `http` line with no request to answer is a breach of its case; and
//! `book_channels` names the lines showing a channel `rpi_book` is declared without.
//!
//! Codex r4489870341: `no_exch_ts_synthesized` lets an anchor's answer without a timestamp
//! release a stamped delta the toy held with its own frame's timestamp, which the answer names
//! with `held=`, and fails a toy that dates the answer's snapshot, a health it reports or an
//! unstamped held delta by it, a case naming a frame that held nothing or was released already,
//! and a case whose only events judged are released ones.

mod toy_setup;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_conformance::suite::{self, Failure, Subject, Verdict};
use fbc_conformance::toy::{ANCHORED_BOOK, INST_A, INST_B, ToyFactory};
use fbc_core::{
    AccountSummary, AssetKey, BookId, BookSide, Channel, ConfigError, ConnTopology, Continuity,
    DecodeError, DecodeScope, Effect, Effects, Encoding, EndpointPlan, ExchNs, ExecCodec,
    ExecEndpoint, Feed, FeedHealth, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentId, InstrumentSpecDraft, Keepalive, Lots, MdCodec, MdEvent,
    MdSink, MonoNs, OpKind, RateCharge, RawFrame, Secrets, SpecTable, StreamId, Subscription,
    SymbolError, TagSet, Ticks, TimerTag, TrafficClass, VenueCaps, VenueConfig, VenueError,
    VenueFactory, VenueMeta, WallNs, WireSlice,
};
use toy_setup::{FIXTURES, assumed};

/// The four market-data checks, by name.
type Check = fn(&Subject<'_>) -> Result<Verdict, Failure>;
const CHECKS: [(&str, Check); 4] = [
    ("continuity", suite::continuity),
    ("subscriptions_idempotent", suite::subscriptions_idempotent),
    ("no_exch_ts_synthesized", suite::no_exch_ts_synthesized),
    ("book_channels", suite::book_channels),
];

// ---------------------------------------------------------------------------------------------
// The variants.
// ---------------------------------------------------------------------------------------------

/// How a variant's market-data codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Twist {
    /// It drops every gap it would report: a sequence break is ignored.
    IgnoresBreaks,
    /// It drops every gap it would report on the anchored book (`rpi_book`), and only there:
    /// that channel's sequence breaks are ignored (FBC-fhk4).
    IgnoresAnchoredBreaks,
    /// Every frame it decodes also reports a gap on the book.
    GapsAlways,
    /// Every event without the venue's timestamp gets one of the codec's own.
    StampsTime,
    /// Each book snapshot an anchor's HTTP answer pushes without the venue's timestamp is dated
    /// by the last timestamp the answer's events carry, a stamped delta's it held and flushes
    /// after the snapshot (Codex r4489870341).
    DatesSnapshotByHeld,
    /// After each anchor's HTTP answer pushing a snapshot, it reports the book live, dated by the
    /// last timestamp the answer's events carry, a stamped delta's it held.
    LiveByHeld,
    /// Each event an anchor's HTTP answer pushes after its snapshot without the venue's
    /// timestamp is dated by the last timestamp the answer's events carry: an unstamped delta it
    /// held is flushed with a stamped one's.
    DatesFlushedByHeld,
    /// Each event an anchor's HTTP answer none of whose events carries the venue's timestamp
    /// pushes after its snapshot is dated by the last timestamp the codec pushed before the
    /// answer, from any frame or answer.
    DatesFlushedByLast,
    /// It pushes only the events that carry the venue's timestamp.
    OnlyStamped,
    /// Every book event is put on the anchored book instead.
    OtherBook,
    /// It refuses every subscription.
    RefusesSubscribe,
    /// Every gap it reports names the other instrument (TOYA's for TOYB's, and the reverse).
    WrongInstrument,
    /// Every gap it reports on a book is also reported on the same instrument's trades.
    GapsTradesToo,
    /// It keeps a broken book live: each gap it reports on a book is preceded by a level of
    /// that book, and every later frame pushes one too until the book's next snapshot begins.
    KeepsApplying,
    /// As [`Twist::KeepsApplying`], with a prune of one side of the book for the level.
    KeepsPruning,
    /// As [`Twist::KeepsApplying`], with a clear of one side of the book for the level.
    KeepsClearing,
    /// As [`Twist::KeepsApplying`], with a both-sides window of the book for the level.
    KeepsWindowing,
    /// After each level it pushes, a prune of the level's side to its price and a clear of that
    /// side, both on the anchored book instead.
    SidesOnOtherBook,
    /// After each level it pushes, a prune of the level's side to its price and a clear of that
    /// side, both on the level's own book.
    SidesOnOwnBook,
    /// It pushes the gaps it reports and, for each level, a clear of that level's side, and
    /// nothing else.
    OnlyGapsAndClears,
    /// It pushes the gaps it reports and, for each level, a prune of that level's side to the
    /// level's price, and nothing else.
    OnlyGapsAndPrunes,
    /// Each gap it reports on a book is followed by `Live` on that book, and so is each
    /// snapshot's begin: it tells the consumer to trust a book still broken.
    LiveTooSoon,
    /// Each gap it reports on a book is put on the anchored book instead.
    GapOtherBook,
    /// Opening, it sends the subscribe frame for the toy's two instruments on `book`, which
    /// its subscribe then sends again.
    OpensWithSubscribe,
    /// Each gap it reports on a book comes after a snapshot of that book begins, and is followed
    /// by a level, the snapshot's end and `Live`: the gap discards that snapshot.
    SnapshotBeforeGap,
    /// It pushes the gaps it reports and nothing else.
    OnlyGaps,
    /// It takes every subscription and sends nothing for it.
    SubscribesSilently,
    /// It reads a binary frame's bytes as the toy's text.
    BinaryText,
    /// Each codec its factory builds after the first also asks for a timer when it opens.
    OpenDrifts,
    /// It refuses a subscribe call naming more than one instrument, as a venue with one
    /// connection per instrument may.
    OneInstrument,
    /// Each codec its factory builds after the first sends its subscribe frame twice, in one
    /// effect, as one that remembered the last connection's subscriptions in its payload would.
    StalePayload,
    /// Each codec its factory builds subscribes once more for every codec built before it, as
    /// one that remembered the last connection's subscriptions would.
    PilesUp,
}

/// The toy with its caps edited by `caps` and its market-data codec twisted.
struct Variant {
    caps: fn(&mut VenueCaps),
    twist: Option<Twist>,
    /// How many market-data codecs it has built.
    builds: AtomicU64,
}

impl Variant {
    /// The toy, its caps edited.
    fn declaring(caps: fn(&mut VenueCaps)) -> Variant {
        Variant {
            caps,
            twist: None,
            builds: AtomicU64::new(0),
        }
    }

    /// The toy, its market-data codec twisted.
    fn twisted(twist: Twist) -> Variant {
        Variant {
            twist: Some(twist),
            ..Variant::declaring(|_| {})
        }
    }

    /// `check` run against `fixtures`.
    fn run_in(&self, check: Check, fixtures: &Path) -> Result<Verdict, Failure> {
        check(&Subject::new(self, fixtures, assumed).unwrap())
    }

    /// `check` run against the toy's fixtures.
    fn run(&self, check: Check) -> Result<Verdict, Failure> {
        self.run_in(check, Path::new(FIXTURES))
    }
}

impl VenueFactory for Variant {
    fn id(&self) -> &'static str {
        "TOY-MD-VARIANT"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        ToyFactory.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = ToyFactory.caps(cfg)?;
        (self.caps)(&mut caps);
        Ok(caps)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        ToyFactory.parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        ToyFactory.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        ToyFactory.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        let inner = ToyFactory.md_codec(cfg, ep);
        match self.twist {
            None => inner,
            Some(twist) => Box::new(Twisted {
                inner,
                twist,
                built: self.builds.fetch_add(1, Ordering::Relaxed),
                broken: BTreeSet::new(),
                last_ts: None,
            }),
        }
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        ToyFactory.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        ToyFactory.exec_codec(cfg, creds)
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The toy's market-data codec, twisted: what it pushes from a frame or an anchor's HTTP answer
/// alike, except where a twist names frames.
struct Twisted {
    inner: Box<dyn MdCodec>,
    twist: Twist,
    /// How many codecs its factory built before it.
    built: u64,
    /// The books it reported a gap on and has had no snapshot of since ([`Twist::KeepsApplying`]).
    broken: BTreeSet<(InstrumentId, BookId)>,
    /// The last timestamp an event it pushed carried ([`Twist::DatesFlushedByLast`]).
    last_ts: Option<ExchNs>,
}

/// The events a twisted codec's toy pushed from one call.
struct Collect(Vec<(VenueMeta, MdEvent)>);

impl MdSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

/// A level of `book` on `inst`, as a codec that keeps a broken book live would push.
fn stray_level(inst: InstrumentId, book: BookId) -> MdEvent {
    MdEvent::Level {
        inst,
        book,
        side: BookSide::Bid,
        px: Ticks(1),
        qty: Lots::new(1).unwrap(),
    }
}

/// A prune of `book`'s bids on `inst`, as a codec that keeps a broken book live would push.
fn stray_prune(inst: InstrumentId, book: BookId) -> MdEvent {
    MdEvent::SidePrune {
        inst,
        book,
        side: BookSide::Bid,
        lo: Ticks(1),
        hi: Ticks(2),
    }
}

/// A clear of `book`'s asks on `inst`, as a codec that keeps a broken book live would push.
fn stray_clear(inst: InstrumentId, book: BookId) -> MdEvent {
    MdEvent::SideClear {
        inst,
        book,
        side: BookSide::Ask,
    }
}

/// A both-sides window of `book` on `inst`, as a codec that keeps a broken book live would push.
fn stray_window(inst: InstrumentId, book: BookId) -> MdEvent {
    MdEvent::Window {
        inst,
        book,
        lo: Ticks(1),
        hi: Ticks(2),
    }
}

impl Twisted {
    /// The event [`Twist::KeepsApplying`] and its kin push of a broken book.
    fn stray(&self) -> Option<fn(InstrumentId, BookId) -> MdEvent> {
        match self.twist {
            Twist::KeepsApplying => Some(stray_level),
            Twist::KeepsPruning => Some(stray_prune),
            Twist::KeepsClearing => Some(stray_clear),
            Twist::KeepsWindowing => Some(stray_window),
            _ => None,
        }
    }

    /// [`Twist::KeepsApplying`]'s frame or HTTP answer, and its kin's: the events the toy pushes
    /// as `decode` hands it the input, a `stray` event of the book before each gap on it, and one
    /// of every book still broken from an earlier input.
    fn keep_applying(
        &mut self,
        sink: &mut dyn MdSink,
        stray: fn(InstrumentId, BookId) -> MdEvent,
        decode: impl FnOnce(&mut dyn MdCodec, &mut dyn MdSink) -> Result<(), DecodeError>,
    ) -> Result<(), DecodeError> {
        let mut toy = Collect(Vec::new());
        let outcome = decode(&mut *self.inner, &mut toy);
        let before = self.broken.clone();
        for (meta, ev) in toy.0 {
            match ev {
                MdEvent::Health {
                    inst,
                    feed: Feed::Book(book),
                    h: FeedHealth::Gap,
                } => {
                    sink.push(meta, stray(inst, book));
                    self.broken.insert((inst, book));
                }
                MdEvent::BookSnapshotBegin { inst, book, .. } => {
                    self.broken.remove(&(inst, book));
                }
                _ => {}
            }
            sink.push(meta, ev);
        }
        for &(inst, book) in before.intersection(&self.broken) {
            sink.push(VenueMeta::NONE, stray(inst, book));
        }
        outcome
    }
}

/// A sink that rewrites what a twisted codec pushes.
struct Rewrite<'a> {
    sink: &'a mut dyn MdSink,
    twist: Twist,
}

impl MdSink for Rewrite<'_> {
    fn push(&mut self, mut meta: VenueMeta, mut ev: MdEvent) {
        let gap = matches!(
            ev,
            MdEvent::Health {
                h: FeedHealth::Gap,
                ..
            }
        );
        if self.twist == Twist::OnlyGaps && !gap {
            return;
        }
        if self.twist == Twist::OnlyStamped && meta.exch_ts.is_none() {
            return;
        }
        if matches!(
            self.twist,
            Twist::OnlyGapsAndClears | Twist::OnlyGapsAndPrunes
        ) && !gap
        {
            if let MdEvent::Level {
                inst,
                book,
                side,
                px,
                ..
            } = ev
            {
                let ev = match self.twist {
                    Twist::OnlyGapsAndClears => MdEvent::SideClear { inst, book, side },
                    _ => MdEvent::SidePrune {
                        inst,
                        book,
                        side,
                        lo: px,
                        hi: px,
                    },
                };
                self.sink.push(meta, ev);
            }
            return;
        }
        if let (
            Twist::SnapshotBeforeGap,
            MdEvent::Health {
                inst,
                feed: Feed::Book(book),
                h: FeedHealth::Gap,
            },
        ) = (self.twist, ev)
        {
            let epoch = 99;
            self.sink
                .push(meta, MdEvent::BookSnapshotBegin { inst, book, epoch });
            self.sink.push(meta, ev);
            self.sink.push(meta, stray_level(inst, book));
            self.sink
                .push(meta, MdEvent::BookSnapshotEnd { inst, book });
            let feed = Feed::Book(book);
            let h = FeedHealth::Live;
            self.sink.push(meta, MdEvent::Health { inst, feed, h });
            return;
        }
        match (self.twist, &mut ev) {
            (
                Twist::IgnoresBreaks,
                MdEvent::Health {
                    h: FeedHealth::Gap, ..
                },
            ) => return,
            (
                Twist::IgnoresAnchoredBreaks,
                MdEvent::Health {
                    feed: Feed::Book(ANCHORED_BOOK),
                    h: FeedHealth::Gap,
                    ..
                },
            ) => return,
            (Twist::StampsTime, _) if meta.exch_ts.is_none() => meta.exch_ts = Some(ExchNs(7)),
            (
                Twist::WrongInstrument,
                MdEvent::Health {
                    inst,
                    h: FeedHealth::Gap,
                    ..
                },
            ) => *inst = if *inst == INST_A { INST_B } else { INST_A },
            (
                Twist::GapOtherBook,
                MdEvent::Health {
                    feed,
                    h: FeedHealth::Gap,
                    ..
                },
            ) => *feed = Feed::Book(ANCHORED_BOOK),
            (
                Twist::OtherBook,
                MdEvent::Level { book, .. }
                | MdEvent::BookSnapshotBegin { book, .. }
                | MdEvent::BookSnapshotEnd { book, .. },
            ) => *book = ANCHORED_BOOK,
            _ => {}
        }
        self.sink.push(meta, ev);
        if let (
            Twist::SidesOnOtherBook | Twist::SidesOnOwnBook,
            MdEvent::Level {
                inst,
                book,
                side,
                px,
                ..
            },
        ) = (self.twist, ev)
        {
            let book = match self.twist {
                Twist::SidesOnOtherBook => ANCHORED_BOOK,
                _ => book,
            };
            let (lo, hi) = (px, px);
            self.sink.push(
                meta,
                MdEvent::SidePrune {
                    inst,
                    book,
                    side,
                    lo,
                    hi,
                },
            );
            self.sink
                .push(meta, MdEvent::SideClear { inst, book, side });
        }
        if let (
            Twist::LiveTooSoon,
            MdEvent::Health {
                inst,
                feed: Feed::Book(book),
                h: FeedHealth::Gap,
            }
            | MdEvent::BookSnapshotBegin { inst, book, .. },
        ) = (self.twist, ev)
        {
            let feed = Feed::Book(book);
            let h = FeedHealth::Live;
            self.sink.push(meta, MdEvent::Health { inst, feed, h });
        }
        if let (
            Twist::GapsTradesToo,
            MdEvent::Health {
                inst,
                h: FeedHealth::Gap,
                ..
            },
        ) = (self.twist, ev)
        {
            let feed = Feed::Trades;
            let h = FeedHealth::Gap;
            self.sink.push(meta, MdEvent::Health { inst, feed, h });
        }
    }
}

impl MdCodec for Twisted {
    fn on_open(&mut self, fx: &mut Effects) {
        if self.twist == Twist::OpensWithSubscribe {
            fx.push(Effect::Send {
                stream: StreamId(0),
                frame: WireSlice::plain(b"sub|add=TOYA-PERP@0,TOYB-PERP@0".to_vec()),
                rpc: None,
                class: TrafficClass::Normal,
                charge: RateCharge::one(OpKind::Subscribe, None),
            });
        }
        if self.twist == Twist::OpenDrifts && self.built > 0 {
            let tag = TimerTag(self.built);
            let after = core::time::Duration::from_secs(1);
            fx.push(Effect::Timer { tag, after });
        }
        self.inner.on_open(fx);
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        if self.twist == Twist::RefusesSubscribe {
            return Err(VenueError::UnsupportedFeed(add[0]));
        }
        if self.twist == Twist::OneInstrument && add.len() > 1 {
            return Err(VenueError::UnsupportedFeed(add[1]));
        }
        if self.twist == Twist::StalePayload && self.built > 0 {
            let mut once = Effects::new();
            self.inner.subscribe(add, remove, specs, &mut once)?;
            for effect in once.take() {
                let Effect::Send {
                    stream,
                    frame,
                    rpc,
                    class,
                    charge,
                } = effect
                else {
                    fx.push(effect);
                    continue;
                };
                let mut twice = frame.bytes().to_vec();
                twice.extend_from_slice(frame.bytes());
                let frame = WireSlice::plain(twice);
                fx.push(Effect::Send {
                    stream,
                    frame,
                    rpc,
                    class,
                    charge,
                });
            }
            return Ok(());
        }
        if self.twist == Twist::SubscribesSilently {
            return self
                .inner
                .subscribe(add, remove, specs, &mut Effects::new());
        }
        if self.twist == Twist::PilesUp {
            for _ in 0..self.built {
                self.inner.subscribe(add, remove, specs, fx)?;
            }
        }
        self.inner.subscribe(add, remove, specs, fx)
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let twist = self.twist;
        if let Some(stray) = self.stray() {
            return self.keep_applying(sink, stray, |toy, sink| {
                toy.on_frame(f, scope, specs, sink, fx)
            });
        }
        if twist == Twist::DatesFlushedByLast {
            let mut toy = Collect(Vec::new());
            let outcome = self.inner.on_frame(f, scope, specs, &mut toy, fx);
            for (meta, ev) in toy.0 {
                self.last_ts = meta.exch_ts.or(self.last_ts);
                sink.push(meta, ev);
            }
            return outcome;
        }
        let text;
        let f = match (twist, f) {
            (Twist::BinaryText, RawFrame::Binary(bytes)) => {
                text = String::from_utf8(bytes.to_vec()).unwrap();
                RawFrame::Text(&text)
            }
            (_, f) => f,
        };
        let rewrite = &mut Rewrite { sink, twist };
        self.inner.on_frame(f, scope, specs, rewrite, fx)?;
        if twist == Twist::GapsAlways {
            let inst = specs.iter().next().unwrap().id;
            let feed = Feed::Book(BookId(0));
            let gap = MdEvent::Health {
                inst,
                feed,
                h: FeedHealth::Gap,
            };
            rewrite.sink.push(VenueMeta::NONE, gap);
        }
        Ok(())
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let twist = self.twist;
        if matches!(
            twist,
            Twist::DatesSnapshotByHeld
                | Twist::LiveByHeld
                | Twist::DatesFlushedByHeld
                | Twist::DatesFlushedByLast
        ) {
            let mut toy = Collect(Vec::new());
            let outcome = self.inner.on_http(tag, resp, scope, specs, &mut toy, fx);
            // The last timestamp the answer's events carry, and the last one the codec pushed
            // before the answer.
            let held = toy.0.iter().rev().find_map(|(meta, _)| meta.exch_ts);
            let before = self.last_ts;
            let (mut in_snapshot, mut after_snapshot, mut snapshot) = (false, false, None);
            for (mut meta, ev) in toy.0 {
                let of_snapshot = match ev {
                    MdEvent::BookSnapshotBegin { inst, book, .. } => {
                        in_snapshot = true;
                        snapshot = Some((inst, book));
                        true
                    }
                    MdEvent::BookSnapshotEnd { .. } => {
                        in_snapshot = false;
                        after_snapshot = true;
                        true
                    }
                    _ => in_snapshot,
                };
                let flushed = after_snapshot && !of_snapshot;
                let date = match twist {
                    Twist::DatesSnapshotByHeld if of_snapshot => held,
                    Twist::DatesFlushedByHeld if flushed => held,
                    Twist::DatesFlushedByLast if flushed && held.is_none() => before,
                    _ => None,
                };
                meta.exch_ts = meta.exch_ts.or(date);
                self.last_ts = meta.exch_ts.or(self.last_ts);
                sink.push(meta, ev);
            }
            if let (Twist::LiveByHeld, Some((inst, book))) = (twist, snapshot) {
                let meta = VenueMeta {
                    exch_ts: held,
                    ..VenueMeta::NONE
                };
                let (feed, h) = (Feed::Book(book), FeedHealth::Live);
                sink.push(meta, MdEvent::Health { inst, feed, h });
            }
            return outcome;
        }
        // What the toy pushes from an anchor's answer is twisted as from a frame (FBC-fhk4).
        if let Some(stray) = self.stray() {
            return self.keep_applying(sink, stray, |toy, sink| {
                toy.on_http(tag, resp, scope, specs, sink, fx)
            });
        }
        let rewrite = &mut Rewrite {
            sink,
            twist: self.twist,
        };
        self.inner.on_http(tag, resp, scope, specs, rewrite, fx)
    }

    fn on_timer(
        &mut self,
        tag: TimerTag,
        now: MonoNs,
        wall: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.inner.on_timer(tag, now, wall, sink, fx);
    }

    fn keepalive(&self) -> Option<Keepalive> {
        self.inner.keepalive()
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture directories made for a test.
// ---------------------------------------------------------------------------------------------

/// The subdirectories of the toy's fixtures that hold market-data cases.
const CASE_DIRS: [&str; 3] = ["continuity", "no_exch_ts_synthesized", "book_channels"];

/// A fixture directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    /// An empty fixture directory named after `test`.
    fn empty(test: &str) -> Scratch {
        let dir =
            std::env::temp_dir().join(format!("fbc-conformance-md-{}-{test}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    /// A copy of the toy's market-data cases named after `test`.
    fn toy(test: &str) -> Scratch {
        let scratch = Scratch::empty(test);
        for sub in CASE_DIRS {
            let to = scratch.0.join(sub);
            fs::create_dir_all(&to).unwrap();
            for entry in fs::read_dir(Path::new(FIXTURES).join(sub)).unwrap() {
                let from = entry.unwrap().path();
                fs::copy(&from, to.join(from.file_name().unwrap())).unwrap();
            }
        }
        scratch
    }

    /// The path of `file` inside it.
    fn at(&self, file: &str) -> PathBuf {
        self.0.join(file)
    }

    /// Writes `text` to `file` inside it, creating its directory.
    fn write(&self, file: &str, text: &str) {
        let path = self.at(file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    /// `check` run on the toy against it.
    fn run(&self, check: Check) -> Result<Verdict, Failure> {
        Variant::declaring(|_| {}).run_in(check, &self.0)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The failure `outcome` must be.
fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    match outcome {
        Err(failure) => failure,
        Ok(verdict) => panic!("expected a failure, got {verdict:?}"),
    }
}

/// What every breach naming `capability` says, in order.
fn said<'a>(failure: &'a Failure, capability: &str) -> Vec<&'a str> {
    let said: Vec<&str> = failure
        .breaches
        .iter()
        .filter(|b| b.capability == capability)
        .map(|b| b.what.as_str())
        .collect();
    assert!(!said.is_empty(), "{capability} not named in {failure}");
    said
}

/// The capabilities `failure` names, in order.
fn named(failure: &Failure) -> Vec<&str> {
    failure
        .breaches
        .iter()
        .map(|b| b.capability.as_str())
        .collect()
}

/// What a passing `outcome` probed.
fn probed(outcome: Result<Verdict, Failure>) -> Vec<String> {
    match outcome {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("expected a pass, got {other:?}"),
    }
}

const PLUS_ONE: &str = "BookCaps.continuity is PlusOne";

/// A `continuity` case of the toy's anchored `rpi_book` on TOYA alone, as a connection carrying
/// one instrument sees it: its one anchor answered, a delta in sequence, then a break.
const RPI_TOYA_ONLY: &str = "http 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n\
                             text delta|sym=TOYA-PERP|book=1|seq=11|bid=100:4|ask=\n\
                             gap=TOYA-PERP text delta|sym=TOYA-PERP|book=1|seq=13|bid=100:4|ask=\n";

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn continuity_fails_a_toy_variant_that_ignores_a_sequence_break_naming_every_break() {
    let failure = failed(Variant::twisted(Twist::IgnoresBreaks).run(suite::continuity));
    assert_eq!(failure.check, "continuity");
    // The anchored rpi_book's breaks too (FBC-fhk4).
    assert_eq!(named(&failure), [PLUS_ONE; 6]);
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/book.frames line 16 breaks TOYA-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/book.frames line 17 breaks TOYB-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/rpi_book.frames line 14 breaks TOYA-PERP's sequence, yet no gap on \
             rpi_book was reported for it",
            "continuity/rpi_book.frames line 23 breaks TOYA-PERP's sequence, yet no gap on \
             rpi_book was reported for it",
            "continuity/rpi_book.frames line 24 breaks TOYB-PERP's sequence, yet no gap on \
             rpi_book was reported for it",
        ]
    );
}

/// FBC-fhk4's done line, its second half: `continuity` drives the toy's anchored `rpi_book` from
/// its case's frames and anchors' HTTP answers, and fails a toy variant that ignores that
/// channel's sequence breaks, naming each break on `rpi_book` and nothing on `book`.
#[test]
fn continuity_fails_the_anchored_rpi_book_of_a_toy_variant_that_ignores_its_sequence_break() {
    let failure = failed(Variant::twisted(Twist::IgnoresAnchoredBreaks).run(suite::continuity));
    assert_eq!(failure.check, "continuity");
    assert_eq!(named(&failure), [PLUS_ONE; 3]);
    let missed = |line: u32, sym: &str| {
        format!(
            "continuity/rpi_book.frames line {line} breaks {sym}'s sequence, yet no gap on \
             rpi_book was reported for it"
        )
    };
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            missed(14, "TOYA-PERP"),
            missed(23, "TOYA-PERP"),
            missed(24, "TOYB-PERP"),
        ]
    );
    // The toy itself passes it.
    probed(Variant::declaring(|_| {}).run(suite::continuity));
}

#[test]
#[should_panic(expected = "BookCaps.continuity is PlusOne")]
fn the_suite_test_for_continuity_panics_naming_the_declared_continuity() {
    suite::run(
        suite::continuity,
        &Variant::twisted(Twist::IgnoresBreaks),
        FIXTURES,
        assumed,
    );
}

// ---------------------------------------------------------------------------------------------
// Other venues that break a check.
// ---------------------------------------------------------------------------------------------

#[test]
fn continuity_fails_a_toy_that_reports_a_gap_in_sequence() {
    let failure = failed(Variant::twisted(Twist::GapsAlways).run(suite::continuity));
    // On book, every frame not marked (eleven frames, three of them breaks), and TOYB's break,
    // which also reports a gap on TOYA. On rpi_book (FBC-fhk4), every frame not marked (eight
    // frames, three of them breaks; its HTTP answers are no frames), and each break, which also
    // reports a gap on TOYA's book.
    assert_eq!(named(&failure), [PLUS_ONE; 17]);
    let said = said(&failure, PLUS_ONE);
    assert_eq!(
        said[0],
        "continuity/book.frames line 3 is in sequence, yet a gap was reported"
    );
    assert_eq!(
        said[9..],
        [
            "continuity/rpi_book.frames line 7 is in sequence, yet a gap was reported",
            "continuity/rpi_book.frames line 11 is in sequence, yet a gap was reported",
            "continuity/rpi_book.frames line 12 is in sequence, yet a gap was reported",
            "continuity/rpi_book.frames line 14 breaks TOYA-PERP's sequence, yet a gap was \
             reported on TOYA-PERP's Book(BookId(0))",
            "continuity/rpi_book.frames line 16 is in sequence, yet a gap was reported",
            "continuity/rpi_book.frames line 21 is in sequence, yet a gap was reported",
            "continuity/rpi_book.frames line 23 breaks TOYA-PERP's sequence, yet a gap was \
             reported on TOYA-PERP's Book(BookId(0))",
            "continuity/rpi_book.frames line 24 breaks TOYB-PERP's sequence, yet a gap was \
             reported on TOYA-PERP's Book(BookId(0))",
        ]
    );
}

#[test]
fn no_exch_ts_synthesized_fails_a_toy_that_stamps_a_time_its_frame_lacks() {
    let failure = failed(Variant::twisted(Twist::StampsTime).run(suite::no_exch_ts_synthesized));
    assert_eq!(failure.check, "no_exch_ts_synthesized");
    // Six events on book, and eleven on rpi_book, nine of them from an anchor's HTTP answer
    // (FBC-fhk4): the last answer's snapshot and the unstamped delta it releases, but not the
    // stamped one, which keeps its own frame's timestamp (Codex r4489870341).
    assert_eq!(named(&failure), ["VenueMeta.exch_ts"; 17]);
    let said = said(&failure, "VenueMeta.exch_ts");
    assert_eq!(
        said[0],
        "no_exch_ts_synthesized/book.frames line 4: the frame carries no timestamp, yet \
         Level { inst: InstrumentId(7), book: BookId(0), side: Bid, px: Ticks(100), qty: \
         Lots(4) } carries exch_ts 7"
    );
    for s in &said[7..11] {
        assert!(
            s.starts_with(
                "no_exch_ts_synthesized/rpi_book.frames line 6: the HTTP answer carries no \
                 timestamp, yet "
            ) && s.ends_with(" carries exch_ts 7"),
            "{s}"
        );
    }
    for s in &said[12..] {
        assert!(
            s.starts_with(
                "no_exch_ts_synthesized/rpi_book.frames line 16: the HTTP answer carries no \
                 timestamp and releases no held frame stamped 7, yet "
            ),
            "{s}"
        );
    }
    // The held delta the answer flushes, named.
    assert_eq!(
        said[16],
        "no_exch_ts_synthesized/rpi_book.frames line 16: the HTTP answer carries no timestamp \
         and releases no held frame stamped 7, yet Level { inst: InstrumentId(7), book: \
         BookId(1), side: Bid, px: Ticks(99), qty: Lots(1) } carries exch_ts 7"
    );
}

/// Codex r4489870341: an anchor's answer without a timestamp that releases a stamped delta the
/// toy held may carry that delta's timestamp on the delta's events only: a toy that dates the
/// answer's snapshot by it fails, naming each of the snapshot's events, and nothing else.
#[test]
fn no_exch_ts_synthesized_fails_a_toy_dating_an_anchors_snapshot_by_the_delta_it_held() {
    let twisted = Variant::twisted(Twist::DatesSnapshotByHeld);
    let failure = failed(twisted.run(suite::no_exch_ts_synthesized));
    assert_eq!(failure.check, "no_exch_ts_synthesized");
    assert_eq!(named(&failure), ["VenueMeta.exch_ts"; 4]);
    let dated = |ev: &str| {
        format!(
            "no_exch_ts_synthesized/rpi_book.frames line 16: the HTTP answer carries no \
             timestamp, yet {ev}, of a book snapshot it pushes, carries exch_ts \
             1759363200000000900, line 12's: a snapshot is the line's own, never released from \
             a held frame"
        )
    };
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts"),
        [
            dated("BookSnapshotBegin { inst: InstrumentId(7), book: BookId(1), epoch: 2 }"),
            dated(
                "Level { inst: InstrumentId(7), book: BookId(1), side: Bid, px: Ticks(100), \
                 qty: Lots(1) }"
            ),
            dated(
                "Level { inst: InstrumentId(7), book: BookId(1), side: Ask, px: Ticks(101), \
                 qty: Lots(1) }"
            ),
            dated("BookSnapshotEnd { inst: InstrumentId(7), book: BookId(1) }"),
        ]
    );
}

/// The toy's `rpi_book` case, by name.
const RPI_TS: &str = "no_exch_ts_synthesized/rpi_book.frames";

/// The toy's `rpi_book` case, with `more` after its last line, 16.
fn rpi_ts_and(more: &str) -> String {
    let toy = fs::read_to_string(Path::new(FIXTURES).join(RPI_TS)).unwrap();
    assert_eq!(toy.lines().count(), 16);
    format!("{toy}{more}")
}

/// Codex r4489870341, the skeptic's first P3: an anchor's answer without a timestamp that
/// releases a stamped delta the toy held may carry that delta's timestamp on book deltas only: a
/// toy that dates the health it reports after the answer by it fails, naming the health, where
/// the rule before let it through.
#[test]
fn no_exch_ts_synthesized_fails_a_toy_dating_a_health_by_the_delta_it_held() {
    let failure = failed(Variant::twisted(Twist::LiveByHeld).run(suite::no_exch_ts_synthesized));
    assert_eq!(named(&failure), ["VenueMeta.exch_ts"]);
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts"),
        [
            "no_exch_ts_synthesized/rpi_book.frames line 16: the HTTP answer carries no \
             timestamp, yet Health { inst: InstrumentId(7), feed: Book(BookId(1)), h: Live }, no \
             book delta, carries exch_ts 1759363200000000900, line 12's: only a book delta is \
             released from a held frame"
        ]
    );
}

/// Codex r4489870341, the skeptic's second P3: the toy's case states with `held=<ns>:1` that
/// one event of the answer carries the held delta's timestamp, so a toy that flushes the
/// unstamped delta it held with it too fails, naming both, where the rule before let it
/// through. Without the count, as before, the case cannot tell them apart.
#[test]
fn no_exch_ts_synthesized_fails_a_toy_flushing_an_unstamped_held_delta_with_a_stamped_ones_time() {
    let twisted = Variant::twisted(Twist::DatesFlushedByHeld);
    let failure = failed(twisted.run(suite::no_exch_ts_synthesized));
    assert_eq!(named(&failure), ["VenueMeta.exch_ts"]);
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts"),
        [
            "no_exch_ts_synthesized/rpi_book.frames line 16: the HTTP answer carries no \
             timestamp and releases line 12's held frame with `held=1759363200000000900:1`, yet \
             2 of its book deltas carry exch_ts 1759363200000000900: Level { inst: \
             InstrumentId(7), book: BookId(1), side: Bid, px: Ticks(99), qty: Lots(1) }, Level \
             { inst: InstrumentId(7), book: BookId(1), side: Bid, px: Ticks(99), qty: Lots(2) }"
        ]
    );

    let scratch = Scratch::toy("held-uncounted");
    let uncounted =
        rpi_ts_and("").replace("held=1759363200000000900:1 ", "held=1759363200000000900 ");
    scratch.write(RPI_TS, &uncounted);
    probed(twisted.run_in(suite::no_exch_ts_synthesized, &scratch.0));
}

/// Codex r4489870341, the skeptic's third P3: a toy that dates the unstamped delta an answer
/// flushes by the last timestamp it pushed passes only a case that lets the answer name that
/// timestamp's frame with `held=`. A `held=` naming a frame that pushed its timestamp at once,
/// so held nothing, or one an earlier answer released, is the case's error, and the dated delta
/// breaches, where the rule before took both cases and passed the toy.
#[test]
fn no_exch_ts_synthesized_fails_a_case_releasing_a_frame_that_held_nothing_or_was_released() {
    let twisted = Variant::twisted(Twist::DatesFlushedByLast);
    // The toy's own case: the one answer it dates by an earlier time flushes nothing.
    probed(twisted.run(suite::no_exch_ts_synthesized));
    let scratch = Scratch::toy("held-twice");
    let dated = |line: u32, px: u32, ns: &str| {
        format!(
            "no_exch_ts_synthesized/rpi_book.frames line {line}: the HTTP answer carries no \
             timestamp and releases no held frame stamped {ns}, yet Level {{ inst: \
             InstrumentId(7), book: BookId(1), side: Bid, px: Ticks({px}), qty: Lots(1) }} \
             carries exch_ts {ns}"
        )
    };

    // Line 17, stamped, is pushed at once; 18 breaks the sequence and is held; the answer at 19
    // flushes it dated by 17's timestamp, and names 17's frame.
    scratch.write(
        RPI_TS,
        &rpi_ts_and(
            "ts=1759363200000001000 text \
             delta|sym=TOYA-PERP|book=1|seq=15|ts=1759363200000001000|bid=98:2|ask=\n\
             text delta|sym=TOYA-PERP|book=1|seq=17|bid=97:1|ask=\n\
             held=1759363200000001000 http 200 text \
             anchor|sym=TOYA-PERP|seq=16|bid=100:1|ask=101:1\n",
        ),
    );
    let failure = failed(twisted.run_in(suite::no_exch_ts_synthesized, &scratch.0));
    assert_eq!(named(&failure), [RPI_TS, "VenueMeta.exch_ts"]);
    assert_eq!(
        said(&failure, RPI_TS),
        [
            "line 19: `held=1759363200000001000` names line 17's frame, which pushed an event \
             carrying exch_ts 1759363200000001000 at once: it held none stamped so"
        ]
    );
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts"),
        [dated(19, 97, "1759363200000001000")]
    );

    // Line 17 breaks the sequence and is held; the answer at 18 flushes it dated by line 12's
    // timestamp, and names line 12's frame, which line 16 released.
    scratch.write(
        RPI_TS,
        &rpi_ts_and(
            "text delta|sym=TOYA-PERP|book=1|seq=16|bid=98:1|ask=\n\
             held=1759363200000000900 http 200 text \
             anchor|sym=TOYA-PERP|seq=15|bid=100:1|ask=101:1\n",
        ),
    );
    let failure = failed(twisted.run_in(suite::no_exch_ts_synthesized, &scratch.0));
    assert_eq!(named(&failure), [RPI_TS, "VenueMeta.exch_ts"]);
    assert_eq!(
        said(&failure, RPI_TS),
        [
            "line 18: `held=1759363200000000900` names line 12's frame, which line 16 released: \
             a held frame is released once"
        ]
    );
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts"),
        [dated(18, 98, "1759363200000000900")]
    );
}

/// Codex r4489870341, the skeptic's fifth P3: a toy that pushes only the events carrying the
/// venue's timestamp leaves `rpi_book`'s case judging only the delta its last answer releases
/// with its held frame's timestamp, which proves nothing, where the rule before counted it.
#[test]
fn no_exch_ts_synthesized_fails_a_case_whose_only_events_judged_are_released() {
    let failure = failed(Variant::twisted(Twist::OnlyStamped).run(suite::no_exch_ts_synthesized));
    let book = "no_exch_ts_synthesized/book.frames";
    assert_eq!(named(&failure), [book, RPI_TS]);
    assert_eq!(
        said(&failure, book),
        [
            "no event is decoded from a line without a timestamp: a case that states none proves \
             nothing"
        ]
    );
    assert_eq!(
        said(&failure, RPI_TS),
        [
            "every event decoded from a line without a timestamp is released from a held frame \
             with its timestamp: a case that states none of its own proves nothing"
        ]
    );
}

#[test]
fn book_channels_fails_a_toy_that_puts_a_books_frames_on_another_book() {
    let failure = failed(Variant::twisted(Twist::OtherBook).run(suite::book_channels));
    assert_eq!(failure.check, "book_channels");
    // The two snapshots' begin, levels and end, and the delta's level.
    assert_eq!(named(&failure), ["BookCaps.channel"; 10]);
    assert_eq!(
        said(&failure, "BookCaps.channel")[0],
        "book_channels/book.frames line 3, a frame of book (book 0), decoded onto book 1"
    );
}

#[test]
fn book_channels_fails_a_declared_channel_no_frame_shows_and_a_shown_one_not_declared() {
    let declares_rpi = Variant::declaring(|caps| {
        caps.md.books[0].includes_channels = TagSet::of(&[Channel::Public, Channel::Rpi]);
    });
    let failure = failed(declares_rpi.run(suite::book_channels));
    assert_eq!(named(&failure), ["BookCaps.includes_channels has Rpi"]);
    assert_eq!(
        said(&failure, "BookCaps.includes_channels has Rpi"),
        ["no frame of book_channels/book.frames shows it"]
    );

    let scratch = Scratch::toy("shows-rpi");
    scratch.write(
        "book_channels/book.frames",
        "public rpi text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n",
    );
    let failure = failed(scratch.run(suite::book_channels));
    assert_eq!(
        said(&failure, "BookCaps.includes_channels lacks Rpi"),
        ["book_channels/book.frames line 1 shows Rpi liquidity"]
    );
}

/// FBC-fhk4: the toy's anchored `rpi_book` declares public and RPI liquidity
/// (`md.books[1].includes_channels`), and its case shows each, in its anchors' HTTP answers and
/// its deltas: declared without either, `book_channels` names every line that shows it.
#[test]
fn book_channels_fails_the_anchored_rpi_book_declared_without_public_or_rpi() {
    let public_only = Variant::declaring(|caps| {
        caps.md.books[usize::from(ANCHORED_BOOK.0)].includes_channels =
            TagSet::of(&[Channel::Public]);
    });
    let failure = failed(public_only.run(suite::book_channels));
    assert_eq!(named(&failure), ["BookCaps.includes_channels lacks Rpi"; 2]);
    assert_eq!(
        said(&failure, "BookCaps.includes_channels lacks Rpi"),
        [
            "book_channels/rpi_book.frames line 4 shows Rpi liquidity",
            "book_channels/rpi_book.frames line 6 shows Rpi liquidity",
        ]
    );
    let rpi_only = Variant::declaring(|caps| {
        caps.md.books[usize::from(ANCHORED_BOOK.0)].includes_channels = TagSet::of(&[Channel::Rpi]);
    });
    let failure = failed(rpi_only.run(suite::book_channels));
    assert_eq!(
        named(&failure),
        ["BookCaps.includes_channels lacks Public"; 2]
    );
    assert_eq!(
        said(&failure, "BookCaps.includes_channels lacks Public"),
        [
            "book_channels/rpi_book.frames line 4 shows Public liquidity",
            "book_channels/rpi_book.frames line 5 shows Public liquidity",
        ]
    );
}

#[test]
fn book_channels_fails_a_frame_that_pushes_levels_without_stating_its_channels() {
    let scratch = Scratch::toy("unstated");
    scratch.write(
        "book_channels/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         public text delta|sym=TOYA-PERP|book=0|seq=11|bid=100:4|ask=\n",
    );
    let failure = failed(scratch.run(suite::book_channels));
    assert_eq!(
        said(&failure, "book_channels/book.frames"),
        ["line 1 pushes levels, yet states no order channel (public, rpi)"]
    );
}

#[test]
fn book_channels_fails_a_channel_stated_on_a_frame_that_pushes_no_level() {
    // Codex r4217682460: a tag shows a channel only on a frame that shows liquidity.
    let declares_rpi = || {
        Variant::declaring(|caps| {
            caps.md.books[0].includes_channels = TagSet::of(&[Channel::Public, Channel::Rpi]);
        })
    };
    let scratch = Scratch::toy("rpi-on-nothing");
    scratch.write(
        "book_channels/book.frames",
        "public text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         rpi text delta|sym=TOYA-PERP|book=0|seq=11|bid=|ask=\n",
    );
    let failure = failed(declares_rpi().run_in(suite::book_channels, &scratch.0));
    assert_eq!(
        said(&failure, "book_channels/book.frames"),
        ["line 2 states order channels, yet pushes no level"]
    );
    assert_eq!(
        said(&failure, "BookCaps.includes_channels has Rpi"),
        ["no frame of book_channels/book.frames shows it"]
    );
}

#[test]
fn subscriptions_idempotent_fails_a_codec_that_refuses_the_set() {
    let variant = Variant::twisted(Twist::RefusesSubscribe);
    let failure = failed(variant.run(suite::subscriptions_idempotent));
    assert_eq!(failure.check, "subscriptions_idempotent");
    let refused = said(&failure, "MdCodec::subscribe");
    assert_eq!(refused.len(), 2);
    for (refused, channel) in refused.iter().zip(["book", "rpi_book"]) {
        assert!(
            refused.starts_with(&format!(
                "refused {channel} on every instrument in epoch 0: "
            )),
            "{refused}"
        );
    }
    // The checks that read cases fail on the refusal before any frame.
    for (name, check) in [CHECKS[0], CHECKS[2], CHECKS[3]] {
        let failure = failed(variant.run(check));
        assert_eq!(failure.check, name);
        assert!(said(&failure, "MdCodec::subscribe")[0].starts_with("refused book on every"));
    }
}

#[test]
fn continuity_fails_a_toy_that_reports_a_break_on_the_wrong_instrument() {
    // Codex r4217682431: a gap must name the instrument whose sequence broke.
    let failure = failed(Variant::twisted(Twist::WrongInstrument).run(suite::continuity));
    let said = said(&failure, PLUS_ONE);
    // Six on book and six on rpi_book (FBC-fhk4).
    assert_eq!(said.len(), 12, "{said:#?}");
    assert_eq!(
        said[6..8],
        [
            "continuity/rpi_book.frames line 14 breaks TOYA-PERP's sequence, yet no gap on \
             rpi_book was reported for it",
            "continuity/rpi_book.frames line 14 breaks TOYA-PERP's sequence, yet a gap was \
             reported on TOYB-PERP",
        ]
    );
    assert_eq!(
        said[..2],
        [
            "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet a gap was reported \
             on TOYB-PERP",
        ]
    );
}

#[test]
fn continuity_fails_a_toy_that_reports_a_break_on_another_feed_too() {
    // Codex r4217839174: the instrument's trades are not broken by its book's gap.
    let failure = failed(Variant::twisted(Twist::GapsTradesToo).run(suite::continuity));
    let said = said(&failure, PLUS_ONE);
    // Each break on book and on rpi_book (FBC-fhk4).
    assert_eq!(said.len(), 6, "{said:#?}");
    assert_eq!(
        said[3],
        "continuity/rpi_book.frames line 14 breaks TOYA-PERP's sequence, yet a gap was reported \
         on TOYA-PERP's Trades"
    );
    assert_eq!(
        said[0],
        "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet a gap was reported on \
         TOYA-PERP's Trades"
    );
}

#[test]
fn continuity_fails_a_toy_that_keeps_a_broken_book_live_until_its_snapshot() {
    // Codex r4218167354: a level of a broken book, from the frame that breaks it (before its
    // gap) or from any later frame before its next snapshot begins, is applied by a consumer
    // that trusts it.
    let failure = failed(Variant::twisted(Twist::KeepsApplying).run(suite::continuity));
    assert_eq!(named(&failure), [PLUS_ONE; 11]);
    let broken = |file: &str, line: u32, sym: &str| {
        format!(
            "continuity/{file}.frames line {line} pushes a level of {sym}'s book while its \
             sequence is broken: a broken book takes nothing until its next snapshot"
        )
    };
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            broken("book", 10, "TOYA-PERP"),
            broken("book", 12, "TOYA-PERP"),
            broken("book", 16, "TOYA-PERP"),
            broken("book", 17, "TOYB-PERP"),
            broken("book", 17, "TOYA-PERP"),
            // FBC-fhk4: on rpi_book, line 18 is an anchor's answer that mends nothing, held to
            // the broken book as a frame is; line 20's anchor begins its snapshot.
            broken("rpi_book", 14, "TOYA-PERP"),
            broken("rpi_book", 16, "TOYA-PERP"),
            broken("rpi_book", 18, "TOYA-PERP"),
            broken("rpi_book", 23, "TOYA-PERP"),
            broken("rpi_book", 24, "TOYB-PERP"),
            broken("rpi_book", 24, "TOYA-PERP"),
        ]
    );
}

#[test]
fn continuity_fails_a_toy_that_prunes_clears_or_windows_a_broken_book_until_its_snapshot() {
    // FBC-n35m (0124): a prune or a clear of one side of a broken book changes it, as a level
    // does, so a consumer that applies it trusts a broken book; a window, held the same way
    // before, is named as one.
    for (twist, kind) in [
        (Twist::KeepsPruning, "side prune"),
        (Twist::KeepsClearing, "side clear"),
        (Twist::KeepsWindowing, "window"),
    ] {
        let failure = failed(Variant::twisted(twist).run(suite::continuity));
        assert_eq!(named(&failure), [PLUS_ONE; 11], "{twist:?}");
        let broken = |file: &str, line: u32, sym: &str| {
            format!(
                "continuity/{file}.frames line {line} pushes a {kind} of {sym}'s book while its \
                 sequence is broken: a broken book takes nothing until its next snapshot"
            )
        };
        assert_eq!(
            said(&failure, PLUS_ONE),
            [
                broken("book", 10, "TOYA-PERP"),
                broken("book", 12, "TOYA-PERP"),
                broken("book", 16, "TOYA-PERP"),
                broken("book", 17, "TOYB-PERP"),
                broken("book", 17, "TOYA-PERP"),
                broken("rpi_book", 14, "TOYA-PERP"),
                broken("rpi_book", 16, "TOYA-PERP"),
                broken("rpi_book", 18, "TOYA-PERP"),
                broken("rpi_book", 23, "TOYA-PERP"),
                broken("rpi_book", 24, "TOYB-PERP"),
                broken("rpi_book", 24, "TOYA-PERP"),
            ],
            "{twist:?}"
        );
    }
}

#[test]
fn continuity_counts_a_side_prune_or_clear_as_the_codec_taking_the_sequence() {
    // FBC-n35m (0124): a frame in sequence whose only events of the book are prunes, or clears,
    // of one side shows the codec taking the sequence, as one pushing a level does.
    let scratch = Scratch::toy("clears-in-sequence");
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    scratch.write("continuity/rpi_book.frames", RPI_TOYA_ONLY);
    let only_gaps = Variant::twisted(Twist::OnlyGaps);
    let failure = failed(only_gaps.run_in(suite::continuity, &scratch.0));
    for channel in ["book", "rpi_book"] {
        assert_eq!(
            said(&failure, &format!("continuity/{channel}.frames")),
            [format!(
                "no frame in sequence pushes an event of {channel}: a case must show the codec \
                 taking the sequence, not only breaking it"
            )]
        );
    }
    for twist in [Twist::OnlyGapsAndClears, Twist::OnlyGapsAndPrunes] {
        let read = probed(Variant::twisted(twist).run_in(suite::continuity, &scratch.0));
        assert_eq!(
            read[..2],
            [
                "continuity/book.frames: 1 of 2 frames break the sequence",
                "continuity/rpi_book.frames: 1 of 3 lines (2 frames, 1 HTTP answer) break the \
                 sequence",
            ],
            "{twist:?}"
        );
    }
}

#[test]
fn book_channels_requires_a_side_prune_or_clear_on_the_books_own_channel() {
    // FBC-n35m (0124): the runtime routes a prune or a clear by its book, as it routes a level.
    let failure = failed(Variant::twisted(Twist::SidesOnOtherBook).run(suite::book_channels));
    // Two for each of the six levels the case's frames push.
    assert_eq!(named(&failure), ["BookCaps.channel"; 12]);
    let said = said(&failure, "BookCaps.channel");
    assert_eq!(
        said[..2],
        [
            "book_channels/book.frames line 3, a frame of book (book 0), decoded onto book 1",
            "book_channels/book.frames line 3, a frame of book (book 0), decoded onto book 1",
        ]
    );
    // On the book's own channel they pass.
    probed(Variant::twisted(Twist::SidesOnOwnBook).run(suite::book_channels));
}

#[test]
fn book_channels_reads_a_side_prune_or_clear_as_showing_no_liquidity() {
    // FBC-n35m (0124): only a level shows liquidity, so a frame whose only book events are
    // prunes, or clears, of one side needs no order channel tag. These frames state none, and
    // the twists push no level: the case shows no channel the book declares, and nothing else
    // is wrong with it.
    let scratch = Scratch::toy("sides-show-nothing");
    scratch.write(
        "book_channels/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3,98:5|ask=101:2\n\
         text delta|sym=TOYA-PERP|book=0|seq=11|bid=100:4|ask=\n",
    );
    // The anchored channel's snapshot, from its anchor's HTTP answer, alike (FBC-fhk4).
    scratch.write(
        "book_channels/rpi_book.frames",
        "http 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3,98:5|ask=101:2\n\
         text delta|sym=TOYA-PERP|book=1|seq=11|bid=100:4|ask=\n",
    );
    for twist in [Twist::OnlyGapsAndClears, Twist::OnlyGapsAndPrunes] {
        let failure = failed(Variant::twisted(twist).run_in(suite::book_channels, &scratch.0));
        assert_eq!(
            named(&failure),
            [
                "BookCaps.includes_channels has Public",
                "BookCaps.includes_channels has Public",
                "BookCaps.includes_channels has Rpi",
            ],
            "{twist:?}: {failure}"
        );
    }
}

#[test]
fn continuity_fails_a_toy_that_reports_a_broken_book_live_before_its_snapshot_ends() {
    // Codex r4218492690: Live says no gap since the last snapshot, so a broken book is Live only
    // once its next snapshot is complete.
    let failure = failed(Variant::twisted(Twist::LiveTooSoon).run(suite::continuity));
    let live = |file: &str, line: u32, sym: &str| {
        format!(
            "continuity/{file}.frames line {line} reports {sym}'s book Live while its sequence \
             is broken: it is Live again only once its next snapshot ends"
        )
    };
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            live("book", 10, "TOYA-PERP"),
            live("book", 13, "TOYA-PERP"),
            live("book", 16, "TOYA-PERP"),
            live("book", 17, "TOYB-PERP"),
            // FBC-fhk4: line 20 is the anchor's HTTP answer whose snapshot mends the book, Live
            // as soon as it begins; lines 8 and 10 anchor books never broken.
            live("rpi_book", 14, "TOYA-PERP"),
            live("rpi_book", 20, "TOYA-PERP"),
            live("rpi_book", 23, "TOYA-PERP"),
            live("rpi_book", 24, "TOYB-PERP"),
        ]
    );
}

#[test]
fn book_channels_fails_a_toy_that_reports_a_books_health_on_another_book() {
    // Codex r4218492678: a book's health is routed by its book too.
    let scratch = Scratch::toy("gap-other-book");
    scratch.write(
        "book_channels/book.frames",
        "public text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    probed(scratch.run(suite::book_channels));
    let failure =
        failed(Variant::twisted(Twist::GapOtherBook).run_in(suite::book_channels, &scratch.0));
    assert_eq!(
        said(&failure, "BookCaps.channel"),
        ["book_channels/book.frames line 2, a frame of book (book 0), decoded onto book 1"]
    );
}

#[test]
fn subscriptions_idempotent_fails_a_codec_that_sends_its_subscribe_opening_and_subscribing() {
    // Codex r4218492650: within one epoch, opening and subscribing send a frame once.
    let failure =
        failed(Variant::twisted(Twist::OpensWithSubscribe).run(suite::subscriptions_idempotent));
    assert_eq!(
        said(&failure, "MdCodec::subscribe"),
        [
            "book: epoch 0 sends the same frame twice, opening and subscribing",
            "book: epoch 1 sends the same frame twice, opening and subscribing",
        ]
    );
}

#[test]
fn continuity_reads_the_longer_blocks_of_a_binary_venues_unchained_books() {
    // Codex r4218492670: the longer-block sub-case is no sequence's, so an unchained book of a
    // binary venue has one too.
    let unsequenced = || Variant {
        caps: |caps| {
            caps.md.encoding = Encoding::Sbe;
            for book in &mut caps.md.books {
                book.continuity = Continuity::Unsequenced;
            }
        },
        ..Variant::twisted(Twist::BinaryText)
    };
    let scratch = Scratch::empty("unchained-longer");
    let file = "continuity/longer_block/book.frames";
    let snap = "snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2";
    scratch.write(file, &hex(snap));
    // The anchored channel's longer blocks, after its anchor's answer (FBC-fhk4).
    scratch.write(
        "continuity/longer_block/rpi_book.frames",
        &format!(
            "http 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n{}",
            hex("delta|sym=TOYA-PERP|book=1|seq=11|bid=100:4|ask=")
        ),
    );
    let read = probed(unsequenced().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        read,
        [
            "book: skipped: BookCaps.continuity is Unsequenced: nothing chains its frames",
            "rpi_book: skipped: BookCaps.continuity is Unsequenced: nothing chains its frames",
            "continuity/longer_block/book.frames: 1 frames, 0 breaking the sequence",
            "continuity/longer_block/rpi_book.frames: 1 frames, 0 breaking the sequence, 1 HTTP \
             answer",
        ]
    );
    // A breach in it is reported, not discarded.
    scratch.write(file, &format!("text {snap}\n"));
    let failure = failed(unsequenced().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, file),
        ["line 1 is a text frame: a longer block is binary"]
    );
}

#[test]
fn continuity_holds_a_book_broken_after_a_snapshot_began_in_the_same_frame() {
    // Codex r4218744557: a gap discards the snapshot begun before it, so only a snapshot begun
    // after the gap mends the book.
    let failure = failed(Variant::twisted(Twist::SnapshotBeforeGap).run(suite::continuity));
    let level = |file: &str, line: u32, sym: &str| {
        format!(
            "continuity/{file}.frames line {line} pushes a level of {sym}'s book while its \
             sequence is broken: a broken book takes nothing until its next snapshot"
        )
    };
    let live = |file: &str, line: u32, sym: &str| {
        format!(
            "continuity/{file}.frames line {line} reports {sym}'s book Live while its sequence \
             is broken: it is Live again only once its next snapshot ends"
        )
    };
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            level("book", 10, "TOYA-PERP"),
            live("book", 10, "TOYA-PERP"),
            level("book", 16, "TOYA-PERP"),
            live("book", 16, "TOYA-PERP"),
            level("book", 17, "TOYB-PERP"),
            live("book", 17, "TOYB-PERP"),
            level("rpi_book", 14, "TOYA-PERP"),
            live("rpi_book", 14, "TOYA-PERP"),
            level("rpi_book", 23, "TOYA-PERP"),
            live("rpi_book", 23, "TOYA-PERP"),
            level("rpi_book", 24, "TOYB-PERP"),
            live("rpi_book", 24, "TOYB-PERP"),
        ]
    );
}

#[test]
fn continuity_fails_a_case_whose_frames_in_sequence_push_nothing_of_the_book() {
    // Codex r4218744571: a case proves the codec takes the sequence as well as breaks in it.
    let failure = failed(Variant::twisted(Twist::OnlyGaps).run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        [
            "no frame in sequence pushes an event of book: a case must show the codec taking \
             the sequence, not only breaking it"
        ]
    );
    // A case whose every frame is marked proves no more.
    let scratch = Scratch::toy("all-marked");
    scratch.write(
        "continuity/book.frames",
        "gap=TOYA-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        [
            "no frame in sequence pushes an event of book: a case must show the codec taking \
             the sequence, not only breaking it"
        ]
    );
}

#[test]
fn subscriptions_idempotent_fails_a_codec_that_sends_no_subscribe_request() {
    // Codex r4218744576: a set taken and never sent is no subscription.
    let failure =
        failed(Variant::twisted(Twist::SubscribesSilently).run(suite::subscriptions_idempotent));
    assert_eq!(
        said(&failure, "MdCodec::subscribe"),
        [
            "book: epoch 0 sends no subscribe request (a Send charged OpKind::Subscribe), \
             opening or subscribing",
            "book: epoch 1 sends no subscribe request (a Send charged OpKind::Subscribe), \
             opening or subscribing",
            "rpi_book: epoch 0 sends no subscribe request (a Send charged OpKind::Subscribe), \
             opening or subscribing",
            "rpi_book: epoch 1 sends no subscribe request (a Send charged OpKind::Subscribe), \
             opening or subscribing",
        ]
    );
}

#[test]
fn the_checks_drive_one_connection_as_the_topology_allows() {
    // Codex r4217682420: a venue with a connection per instrument is subscribed one instrument
    // per codec, and its cases name that instrument only.
    let per_instrument = || {
        let mut v = Variant::twisted(Twist::OneInstrument);
        v.caps = |caps| caps.md.topology = ConnTopology::PerInstrument;
        v
    };
    let scratch = Scratch::toy("per-instrument");
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    // The anchored channel too: one anchor asked for, on its one instrument (FBC-fhk4).
    scratch.write("continuity/rpi_book.frames", RPI_TOYA_ONLY);
    let read = probed(per_instrument().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        read[1],
        "continuity/rpi_book.frames: 1 of 3 lines (2 frames, 1 HTTP answer) break the sequence"
    );
    let sent = probed(per_instrument().run(suite::subscriptions_idempotent));
    assert!(sent[0].starts_with("book: 1 subscriptions"), "{sent:?}");
    assert!(
        sent[1].starts_with("rpi_book: 1 subscriptions sent once in each of 2 epochs ([2, 2]"),
        "{sent:?}"
    );
    // A capped shared connection takes as many as its cap.
    let capped = Variant::declaring(|caps| {
        caps.md.topology = ConnTopology::Shared {
            max_subscriptions: Some(1),
        };
    });
    let sent = probed(capped.run(suite::subscriptions_idempotent));
    assert!(sent[0].starts_with("book: 1 subscriptions"), "{sent:?}");
    assert!(sent[1].starts_with("rpi_book: 1 subscriptions"), "{sent:?}");
    // The toy, its two instruments on one connection, fails a codec that takes one only.
    let failure = failed(Variant::twisted(Twist::OneInstrument).run(suite::continuity));
    assert!(said(&failure, "MdCodec::subscribe")[0].starts_with("refused book"));
}

#[test]
fn subscriptions_idempotent_fails_a_codec_whose_reconnect_opens_differently() {
    // Codex r4217991947: what opening asks for is compared too.
    let failure = failed(Variant::twisted(Twist::OpenDrifts).run(suite::subscriptions_idempotent));
    assert_eq!(
        said(&failure, "MdCodec::on_open"),
        [
            "book: the reconnect's on_open asked for other effects than the first epoch's",
            "rpi_book: the reconnect's on_open asked for other effects than the first epoch's",
        ]
    );
}

#[test]
fn a_case_on_an_instrument_its_connection_did_not_subscribe_fails() {
    // Codex r4217991939: one instrument per connection, and a case of the other's frames.
    let mut per_instrument = Variant::declaring(|_| {});
    per_instrument.caps = |caps| caps.md.topology = ConnTopology::PerInstrument;
    let scratch = Scratch::toy("unsubscribed");
    scratch.write("continuity/rpi_book.frames", RPI_TOYA_ONLY);
    // A tag naming the instrument the connection did not subscribe.
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP gap=TOYB-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    let failure = failed(per_instrument.run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        ["line 2: `gap=TOYB-PERP` names an instrument this connection did not subscribe"]
    );
    // An event on it: the toy drops frames of a channel not subscribed, so a toy that puts its
    // gap on the other instrument stands in for a codec decoding them.
    let mut gaps_b = Variant::twisted(Twist::WrongInstrument);
    gaps_b.caps = |caps| caps.md.topology = ConnTopology::PerInstrument;
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    let failure = failed(gaps_b.run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        [format!(
            "line 2: an event on {INST_B:?}, which this connection did not subscribe"
        )]
    );
}

#[test]
fn continuity_reads_every_gap_tag_of_a_frame() {
    // Codex r4217991957: a frame may break several instruments' sequences; each tag counts.
    let scratch = Scratch::toy("two-gaps");
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         text snap|sym=TOYB-PERP|book=0|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP gap=TOYB-PERP text delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            "continuity/book.frames line 3 breaks TOYB-PERP's sequence, yet no gap on book was \
          reported for it"
        ]
    );
}

#[test]
fn subscriptions_idempotent_fails_a_codec_whose_reconnect_sends_other_subscriptions() {
    // Codex r4217682449: the same number of effects, but the payload doubled.
    let failure =
        failed(Variant::twisted(Twist::StalePayload).run(suite::subscriptions_idempotent));
    assert_eq!(
        said(&failure, "MdCodec::subscribe"),
        ["book: the reconnect's subscribe asked for other effects than the first epoch's"]
    );
}

#[test]
fn subscriptions_idempotent_fails_a_codec_that_sends_the_last_connections_subscriptions_again() {
    let failure = failed(Variant::twisted(Twist::PilesUp).run(suite::subscriptions_idempotent));
    // Its reconnect differs from the first epoch, and sends its frame twice within the epoch
    // (Codex r4218492650). rpi_book's codecs are the third and fourth it builds, so each of its
    // epochs sends its frame more than once (FBC-fhk4).
    assert_eq!(
        said(&failure, "MdCodec::subscribe"),
        [
            "book: the reconnect's subscribe asked for other effects than the first epoch's",
            "book: epoch 1 sends the same frame twice, opening and subscribing",
            "rpi_book: epoch 0 sends the same frame twice, opening and subscribing",
            "rpi_book: the reconnect's subscribe asked for other effects than the first epoch's",
            "rpi_book: epoch 1 sends the same frame twice, opening and subscribing",
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Caps that leave a check nothing, or something else, to check.
// ---------------------------------------------------------------------------------------------

#[test]
fn every_check_is_skipped_for_a_venue_with_no_book_channel_and_drives_every_anchored_one() {
    let none = Variant::declaring(|caps| caps.md.books.clear());
    for (name, check) in CHECKS {
        match none.run(check) {
            Ok(Verdict::Skipped { check, why }) => {
                assert_eq!(check, name);
                assert!(why.starts_with("the caps declare no book channel"), "{why}");
            }
            other => panic!("{name}: {other:?}"),
        }
    }
    // FBC-fhk4: a channel anchored on REST is driven, never skipped by name. Every book the toy
    // declares anchored, each check drives both.
    let anchored = Variant::declaring(|caps| caps.md.books[0].rest_anchor = true);
    for (name, check) in CHECKS {
        let probed = probed(anchored.run(check));
        assert!(
            probed.iter().all(|p| !p.contains("skipped: BookCaps")),
            "{name}: {probed:#?}"
        );
        assert!(
            probed
                .iter()
                .any(|p| p.starts_with("rpi_book") || p.contains("/rpi_book.frames")),
            "{name}: {probed:#?}"
        );
    }
}

/// FBC-fhk4: an `http` line answers a request the codec asked for and no line answered; one
/// with none to answer is a breach of its case, which is judged no further, while the other
/// channels' cases are still judged.
#[test]
fn an_http_line_with_no_request_to_answer_is_a_breach_of_its_case_and_others_are_judged() {
    let scratch = Scratch::toy("unasked");
    // A tag the toy never gave a request.
    scratch.write(
        "continuity/rpi_book.frames",
        "http tag=9 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n\
         gap=TOYA-PERP text delta|sym=TOYA-PERP|book=1|seq=12|bid=100:4|ask=\n",
    );
    let failure =
        failed(Variant::twisted(Twist::IgnoresBreaks).run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, "continuity/rpi_book.frames"),
        ["line 1 answers HTTP request 9, which the codec has not asked for or a line has answered"]
    );
    // book's breaks are still reported; rpi_book's case is judged no further.
    assert_eq!(said(&failure, PLUS_ONE).len(), 3, "{failure}");
    assert!(
        said(&failure, PLUS_ONE)
            .iter()
            .all(|b| b.starts_with("continuity/book.frames"))
    );

    // One more answer than requests: the toy asked for two anchors.
    scratch.write(
        "continuity/rpi_book.frames",
        "http 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n\
         http 200 text anchor|sym=TOYB-PERP|seq=500|bid=40:1|ask=42:1\n\
         http 200 text anchor|sym=TOYB-PERP|seq=500|bid=40:1|ask=42:1\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/rpi_book.frames"),
        ["line 3 answers an HTTP request, but the codec has asked for none it still waits on"]
    );

    // A request answered twice by its tag.
    scratch.write(
        "continuity/rpi_book.frames",
        "http tag=1 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n\
         http tag=1 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/rpi_book.frames"),
        ["line 2 answers HTTP request 1, which the codec has not asked for or a line has answered"]
    );

    // An answer the codec refuses is a refused line, as a frame is.
    scratch.write(
        "no_exch_ts_synthesized/rpi_book.frames",
        "http 200 text nonsense\n\
         http 200 text anchor|sym=TOYB-PERP|seq=500|bid=40:1|ask=42:1\n",
    );
    let failure = failed(scratch.run(suite::no_exch_ts_synthesized));
    let refused = said(&failure, "no_exch_ts_synthesized/rpi_book.frames");
    assert_eq!(refused.len(), 1, "{failure}");
    assert!(refused[0].starts_with("line 1 refused: "), "{failure}");
}

#[test]
fn continuity_skips_a_channel_whose_frames_nothing_chains() {
    let windowed = Variant::declaring(|caps| {
        for book in &mut caps.md.books {
            book.continuity = Continuity::Windowed;
        }
    });
    match windowed.run_in(suite::continuity, &Scratch::empty("windowed").0) {
        Ok(Verdict::Skipped { why, .. }) => assert!(why.contains("Windowed or Unsequenced")),
        other => panic!("{other:?}"),
    }
    // The other checks still drive them.
    let driven = probed(windowed.run(suite::book_channels));
    assert_eq!(driven[0], "book_channels/book.frames: shows [Public]");

    // With a second, chained channel, the unchained one is named.
    let mixed = Variant::declaring(|caps| {
        let mut book = caps.md.books[0];
        book.channel = "flat";
        book.continuity = Continuity::Unsequenced;
        caps.md.books.push(book);
    });
    let named = probed(mixed.run(suite::continuity));
    assert_eq!(
        named[0],
        "flat: skipped: BookCaps.continuity is Unsequenced: nothing chains its frames"
    );
}

/// The toy declaring its market data binary, its codec reading a binary frame's bytes as the
/// toy's text: a stand-in for a binary venue whose blocks run longer.
fn binary() -> Variant {
    Variant {
        caps: |caps| caps.md.encoding = Encoding::Sbe,
        ..Variant::twisted(Twist::BinaryText)
    }
}

/// A `hex` line carrying `text`'s bytes.
fn hex(text: &str) -> String {
    let bytes: Vec<String> = text.bytes().map(|b| format!("{b:02x}")).collect();
    format!("hex {}\n", bytes.join(" "))
}

#[test]
fn continuity_reads_a_binary_venues_longer_blocks() {
    let scratch = Scratch::toy("longer");
    let failure = failed(binary().run_in(suite::continuity, &scratch.0));
    assert!(said(&failure, "continuity")[0].contains("holds no continuity/longer_block/"));

    // Binary frames, decoded, and gapped only where marked.
    let snap = "snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2";
    let delta = "delta|sym=TOYA-PERP|book=0|seq=11|bid=100:4|ask=";
    let empty_delta = "delta|sym=TOYA-PERP|book=0|seq=11|bid=|ask=";
    let file = "continuity/longer_block/book.frames";
    scratch.write(file, &(hex(snap) + &hex(delta)));
    // FBC-fhk4: the anchored channel's longer blocks follow its anchors' HTTP answers, which are
    // its REST bodies, no binary block: text, and one (a refusal it asks again after) pushing
    // nothing.
    let rpi = "continuity/longer_block/rpi_book.frames";
    let anchors = "http tag=2 503 text busy\n\
                   http 200 text anchor|sym=TOYA-PERP|seq=10|bid=100:3|ask=101:2\n";
    let rpi_delta = hex("delta|sym=TOYA-PERP|book=1|seq=11|bid=100:4|ask=");
    scratch.write(rpi, &format!("{anchors}{rpi_delta}"));
    let read = probed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        read[2..],
        [
            "continuity/longer_block/book.frames: 2 frames, 0 breaking the sequence",
            "continuity/longer_block/rpi_book.frames: 1 frames, 0 breaking the sequence, 2 HTTP \
             answers",
        ]
    );
    // Answers alone hand the codec no frame.
    scratch.write(rpi, anchors);
    let failure = failed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, rpi),
        ["hands the codec no frame: a case that decodes nothing proves nothing"]
    );
    scratch.write(rpi, &format!("{anchors}{rpi_delta}"));

    // Codex r4217839165: an empty longer-block case fails.
    scratch.write(file, "# nothing\n");
    let failure = failed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, file),
        ["hands the codec no frame: a case that decodes nothing proves nothing"]
    );

    // Codex r4217682441: every frame must push something, not just one.
    scratch.write(file, &(hex(snap) + &hex(empty_delta)));
    let failure = failed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, file),
        ["line 2 decodes to no event: a longer block must be read, not dropped"]
    );

    // Codex r4217991970: a text frame is no longer block.
    scratch.write(file, &format!("{}text {delta}\n", hex(snap)));
    let failure = failed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        said(&failure, file),
        ["line 2 is a text frame: a longer block is binary"]
    );
}

#[test]
fn continuity_fails_a_text_venue_whose_fixtures_hold_longer_blocks() {
    let scratch = Scratch::toy("text-longer");
    scratch.write("continuity/longer_block/book.frames", "# none\n");
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(named(&failure), ["continuity/longer_block/"]);
    assert!(said(&failure, "continuity/longer_block/")[0].contains("is never read"));
}

// ---------------------------------------------------------------------------------------------
// The fixture directory.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_check_fails_a_fixture_directory_without_its_subdirectory_or_unreadable() {
    let scratch = Scratch::empty("no-dirs");
    for (name, check) in [CHECKS[0], CHECKS[2], CHECKS[3]] {
        let failure = failed(scratch.run(check));
        assert!(said(&failure, name)[0].starts_with("missing:"), "{failure}");
    }
    // subscriptions_idempotent reads no file.
    assert_eq!(
        probed(scratch.run(suite::subscriptions_idempotent)).len(),
        2
    );

    scratch.write("continuity", "a file, not a directory");
    let failure = failed(scratch.run(suite::continuity));
    assert!(said(&failure, "continuity")[0].starts_with("cannot read"));
}

#[test]
fn a_check_fails_a_missing_case_a_malformed_one_and_a_case_it_never_reads() {
    let scratch = Scratch::toy("stray");
    fs::remove_file(scratch.at("book_channels/book.frames")).unwrap();
    scratch.write(
        "book_channels/flat_book.frames",
        "# no such channel: never driven\n",
    );
    let failure = failed(scratch.run(suite::book_channels));
    assert_eq!(
        named(&failure),
        [
            "book_channels/book.frames",
            "book_channels/flat_book.frames"
        ]
    );
    assert!(said(&failure, "book_channels/book.frames")[0].starts_with("cannot read"));
    assert_eq!(
        said(&failure, "book_channels/flat_book.frames"),
        ["book_channels drives no book channel of that name, so it is never checked"]
    );

    scratch.write("no_exch_ts_synthesized/book.frames", "tz text snap\n");
    let failure = failed(scratch.run(suite::no_exch_ts_synthesized));
    assert_eq!(
        said(&failure, "no_exch_ts_synthesized/book.frames"),
        [
            "line 1: `tz` is neither a frame nor a tag (gap=<symbol>, ts, ts=<ns>, \
             held=<ns>[:<count>], public, rpi)"
        ]
    );
}

#[test]
fn a_check_fails_a_refused_frame_and_a_case_that_states_nothing() {
    let scratch = Scratch::toy("states-nothing");
    scratch.write(
        "continuity/book.frames",
        "text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\ntext nonsense\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        [
            "line 2 refused: malformed frame: sym",
            "marks no frame `gap=<symbol>`: a case that breaks no sequence proves nothing",
        ]
    );

    scratch.write(
        "continuity/book.frames",
        "gap=TOYC-PERP text snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101:2\n",
    );
    let failure = failed(scratch.run(suite::continuity));
    assert_eq!(
        said(&failure, "continuity/book.frames"),
        [
            "line 1: `gap=TOYC-PERP` names no instrument of the setup",
            // Its one frame is marked (Codex r4218744571).
            "no frame in sequence pushes an event of book: a case must show the codec taking \
             the sequence, not only breaking it",
        ]
    );

    scratch.write(
        "no_exch_ts_synthesized/book.frames",
        "ts text snap|sym=TOYA-PERP|book=0|seq=10|ts=5|bid=100:3|ask=101:2\n",
    );
    let failure = failed(scratch.run(suite::no_exch_ts_synthesized));
    assert!(said(&failure, "no_exch_ts_synthesized/book.frames")[0].contains("proves nothing"));
}

#[test]
fn a_sub_case_skipped_by_name_is_printed_by_the_suite_test_which_passes() {
    // `expect` prints it; the test is that it does not panic.
    suite::run(suite::continuity, &ToyFactory, FIXTURES, assumed);
}
