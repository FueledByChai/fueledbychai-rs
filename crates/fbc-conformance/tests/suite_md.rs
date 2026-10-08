//! FBC-vmw's done line, its second half: `continuity` fails against a toy variant that ignores a
//! sequence break, naming every break it missed. Around it, every other way the four
//! market-data checks fail or are skipped: a toy that reports a gap in sequence, stamps a
//! timestamp its frame lacks, puts a book's frames on another book or declares channels its
//! frames do not show, refuses a subscription; caps that declare no book, only anchored or
//! unchained ones, or binary market data and its longer-block sub-case; and fixture
//! directories missing a directory or a case, holding a stray case, a malformed line, a refused
//! frame or a case that states nothing.

mod toy_setup;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_conformance::suite::{self, Failure, Subject, Verdict};
use fbc_conformance::toy::{ANCHORED_BOOK, INST_A, INST_B, ToyFactory};
use fbc_core::{
    AccountSummary, AssetKey, BookId, Channel, ConfigError, ConnTopology, Continuity, DecodeError,
    DecodeScope, Effect, Effects, Encoding, EndpointPlan, ExchNs, ExecCodec, ExecEndpoint, Feed,
    FeedHealth, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentSpecDraft, Keepalive, MdCodec, MdEvent, MdSink, MonoNs, RawFrame, Secrets, SpecTable,
    Subscription, SymbolError, TagSet, TimerTag, VenueCaps, VenueConfig, VenueError, VenueFactory,
    VenueMeta, WallNs, WireSlice,
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
    /// Every frame it decodes also reports a gap on the book.
    GapsAlways,
    /// Every event without the venue's timestamp gets one of the codec's own.
    StampsTime,
    /// Every book event is put on the anchored book instead.
    OtherBook,
    /// It refuses every subscription.
    RefusesSubscribe,
    /// Every gap it reports names the other instrument (TOYA's for TOYB's, and the reverse).
    WrongInstrument,
    /// Every gap it reports on a book is also reported on the same instrument's trades.
    GapsTradesToo,
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

/// The toy's market-data codec, twisted.
struct Twisted {
    inner: Box<dyn MdCodec>,
    twist: Twist,
    /// How many codecs its factory built before it.
    built: u64,
}

/// A sink that rewrites what a twisted codec pushes.
struct Rewrite<'a> {
    sink: &'a mut dyn MdSink,
    twist: Twist,
}

impl MdSink for Rewrite<'_> {
    fn push(&mut self, mut meta: VenueMeta, mut ev: MdEvent) {
        match (self.twist, &mut ev) {
            (
                Twist::IgnoresBreaks,
                MdEvent::Health {
                    h: FeedHealth::Gap, ..
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
                Twist::OtherBook,
                MdEvent::Level { book, .. }
                | MdEvent::BookSnapshotBegin { book, .. }
                | MdEvent::BookSnapshotEnd { book, .. },
            ) => *book = ANCHORED_BOOK,
            _ => {}
        }
        self.sink.push(meta, ev);
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
        self.inner.on_http(tag, resp, scope, specs, sink, fx)
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

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn continuity_fails_a_toy_variant_that_ignores_a_sequence_break_naming_every_break() {
    let failure = failed(Variant::twisted(Twist::IgnoresBreaks).run(suite::continuity));
    assert_eq!(failure.check, "continuity");
    assert_eq!(named(&failure), [PLUS_ONE; 3]);
    assert_eq!(
        said(&failure, PLUS_ONE),
        [
            "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/book.frames line 16 breaks TOYA-PERP's sequence, yet no gap on book was \
             reported for it",
            "continuity/book.frames line 17 breaks TOYB-PERP's sequence, yet no gap on book was \
             reported for it",
        ]
    );
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
    // Every frame not marked (eleven frames, three of them breaks), and TOYB's break, which
    // also reports a gap on TOYA.
    assert_eq!(named(&failure), [PLUS_ONE; 9]);
    assert_eq!(
        said(&failure, PLUS_ONE)[0],
        "continuity/book.frames line 3 is in sequence, yet a gap was reported"
    );
}

#[test]
fn no_exch_ts_synthesized_fails_a_toy_that_stamps_a_time_its_frame_lacks() {
    let failure = failed(Variant::twisted(Twist::StampsTime).run(suite::no_exch_ts_synthesized));
    assert_eq!(failure.check, "no_exch_ts_synthesized");
    assert_eq!(named(&failure), ["VenueMeta.exch_ts"; 6]);
    assert_eq!(
        said(&failure, "VenueMeta.exch_ts")[0],
        "no_exch_ts_synthesized/book.frames line 4: the frame carries no timestamp, yet an \
         event carries exch_ts 7"
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
    assert_eq!(refused.len(), 1);
    assert!(
        refused[0].starts_with("refused book on every instrument in epoch 0: "),
        "{}",
        refused[0]
    );
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
    assert_eq!(said.len(), 6, "{said:#?}");
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
    assert_eq!(said.len(), 3, "{said:#?}");
    assert_eq!(
        said[0],
        "continuity/book.frames line 10 breaks TOYA-PERP's sequence, yet a gap was reported on \
         TOYA-PERP's Trades"
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
    probed(per_instrument().run_in(suite::continuity, &scratch.0));
    let sent = probed(per_instrument().run(suite::subscriptions_idempotent));
    assert!(sent[1].starts_with("book: 1 subscriptions"), "{sent:?}");
    // A capped shared connection takes as many as its cap.
    let capped = Variant::declaring(|caps| {
        caps.md.topology = ConnTopology::Shared {
            max_subscriptions: Some(1),
        };
    });
    let sent = probed(capped.run(suite::subscriptions_idempotent));
    assert!(sent[1].starts_with("book: 1 subscriptions"), "{sent:?}");
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
        ["book: the reconnect's on_open asked for other effects than the first epoch's"]
    );
}

#[test]
fn a_case_on_an_instrument_its_connection_did_not_subscribe_fails() {
    // Codex r4217991939: one instrument per connection, and a case of the other's frames.
    let mut per_instrument = Variant::declaring(|_| {});
    per_instrument.caps = |caps| caps.md.topology = ConnTopology::PerInstrument;
    let scratch = Scratch::toy("unsubscribed");
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
    assert_eq!(
        said(&failure, "MdCodec::subscribe"),
        ["book: the reconnect's subscribe asked for other effects than the first epoch's"]
    );
}

// ---------------------------------------------------------------------------------------------
// Caps that leave a check nothing, or something else, to check.
// ---------------------------------------------------------------------------------------------

#[test]
fn every_check_is_skipped_for_a_venue_with_no_book_channel_or_only_anchored_ones() {
    let none = Variant::declaring(|caps| caps.md.books.clear());
    let anchored = Variant::declaring(|caps| caps.md.books[0].rest_anchor = true);
    for variant in [none, anchored] {
        for (name, check) in CHECKS {
            match variant.run(check) {
                Ok(Verdict::Skipped { check, why }) => {
                    assert_eq!(check, name);
                    assert!(why.contains("BookCaps.rest_anchor"), "{why}");
                }
                other => panic!("{name}: {other:?}"),
            }
        }
    }
}

#[test]
fn continuity_skips_a_channel_whose_frames_nothing_chains() {
    let windowed = Variant::declaring(|caps| {
        caps.md.books[0].continuity = Continuity::Windowed;
    });
    match windowed.run_in(suite::continuity, &Scratch::empty("windowed").0) {
        Ok(Verdict::Skipped { why, .. }) => assert!(why.contains("Windowed or Unsequenced")),
        other => panic!("{other:?}"),
    }
    // The other checks still drive it.
    let driven = probed(windowed.run(suite::book_channels));
    assert_eq!(driven[1], "book_channels/book.frames: shows [Public]");

    // With a second, chained channel, the unchained one is named.
    let mixed = Variant::declaring(|caps| {
        let mut book = caps.md.books[0];
        book.channel = "flat";
        book.continuity = Continuity::Unsequenced;
        caps.md.books.push(book);
    });
    let named = probed(mixed.run(suite::continuity));
    assert_eq!(
        named[1],
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
    let read = probed(binary().run_in(suite::continuity, &scratch.0));
    assert_eq!(
        read[2..],
        ["continuity/longer_block/book.frames: 2 frames, 0 breaking the sequence"]
    );

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
        "book_channels/rpi_book.frames",
        "# anchored: never driven\n",
    );
    let failure = failed(scratch.run(suite::book_channels));
    assert_eq!(
        named(&failure),
        ["book_channels/book.frames", "book_channels/rpi_book.frames"]
    );
    assert!(said(&failure, "book_channels/book.frames")[0].starts_with("cannot read"));
    assert_eq!(
        said(&failure, "book_channels/rpi_book.frames"),
        ["book_channels drives no book channel of that name, so it is never checked"]
    );

    scratch.write("no_exch_ts_synthesized/book.frames", "tz text snap\n");
    let failure = failed(scratch.run(suite::no_exch_ts_synthesized));
    assert_eq!(
        said(&failure, "no_exch_ts_synthesized/book.frames"),
        ["line 1: `tz` is neither a frame nor a tag (gap=<symbol>, ts, public, rpi)"]
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
        ["line 1: `gap=TOYC-PERP` names no instrument of the setup"]
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
