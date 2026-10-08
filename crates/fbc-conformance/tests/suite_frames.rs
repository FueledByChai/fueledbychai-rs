//! FBC-whw's done line, its second half: `fee_sign` fails against a toy variant that flips a
//! fee's sign, and `decoder_deterministic` against one that decodes nondeterministically, each
//! naming what broke. Around them, every other way the four checks that read fixture frames
//! fail or are skipped: a toy that swaps a fill's liquidity or a position's sign, one whose
//! effects or results differ between runs, no order entry, fills without a liquidity flag, a
//! codec that is not built, and fixture directories missing a directory or a case, holding a
//! stray case, a malformed line, a refused frame or a case that states nothing.

mod toy_setup;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_conformance::suite::{self, Failure, Subject, Verdict};
use fbc_conformance::toy::{ToyExec, ToyFactory, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, CtxCall, DecodeError, DecodeScope, Effect, Effects,
    EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint, ExecEvent, ExecSink,
    FieldSpec, FillSource, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentSpecDraft, Liquidity3, MdCodec, NotSentReason, PathStamps, RawFrame, RpcId, Secrets,
    SignedLots, SpecTable, StreamId, Subscription, SymbolError, TimerTag, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, VenueFeeSign, VenueMeta,
};
use toy_setup::{FIXTURES, assumed};

/// The four checks, by name.
type Check = fn(&Subject<'_>) -> Result<Verdict, Failure>;
const CHECKS: [(&str, Check); 4] = [
    ("fee_sign", suite::fee_sign),
    ("liquidity_reported", suite::liquidity_reported),
    ("position_signed", suite::position_signed),
    ("decoder_deterministic", suite::decoder_deterministic),
];

// ---------------------------------------------------------------------------------------------
// The variants.
// ---------------------------------------------------------------------------------------------

/// How a variant's codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Twist {
    /// Every event it pushes carries the number of the codec's run as its sequence.
    SeqDrifts,
    /// Every frame and every resync also asks for a timer tagged with the codec's run.
    TimerDrifts,
    /// A codec built an odd number of times refuses every frame.
    RefusesOnOddRuns,
    /// Every fill reports the other liquidity.
    SwapsLiquidity,
    /// Every position is reported with the opposite sign.
    NegatesPositions,
}

/// The toy with its caps edited by `caps` and its codec twisted, or not built at all.
struct Variant {
    caps: fn(&mut VenueCaps),
    codec: Option<Option<Twist>>,
    /// How many codecs it has built: each codec's run.
    runs: AtomicU64,
}

impl Variant {
    /// The toy, its caps edited.
    fn declaring(caps: fn(&mut VenueCaps)) -> Variant {
        Variant {
            caps,
            codec: Some(None),
            runs: AtomicU64::new(0),
        }
    }

    /// The toy, its codec twisted.
    fn twisted(twist: Twist) -> Variant {
        Variant {
            codec: Some(Some(twist)),
            ..Variant::declaring(|_| {})
        }
    }

    /// The toy, run against `fixtures`.
    fn subject(&self, fixtures: &Path) -> Subject<'_> {
        Subject::new(self, fixtures, assumed).unwrap()
    }

    /// `check` run against the toy's fixtures.
    fn run(&self, check: Check) -> Result<Verdict, Failure> {
        check(&self.subject(Path::new(FIXTURES)))
    }
}

impl VenueFactory for Variant {
    fn id(&self) -> &'static str {
        "TOY-VARIANT"
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
        ToyFactory.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        ToyFactory.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let twist = self.codec?;
        let Some(twist) = twist else {
            return ToyFactory.exec_codec(cfg, creds);
        };
        let codec: Box<dyn ExecCodec> = Box::new(Twisted {
            inner: ToyExec::new(Box::new(ToySigner)),
            twist,
            run: self.runs.fetch_add(1, Ordering::Relaxed),
        });
        Some(Ok(codec))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The toy's codec, twisted, built as its variant's `run`th.
struct Twisted {
    inner: ToyExec,
    twist: Twist,
    run: u64,
}

impl Twisted {
    /// The timer [`Twist::TimerDrifts`] asks for.
    fn timer(&self) -> Effect {
        Effect::Timer {
            tag: TimerTag(self.run),
            after: core::time::Duration::from_secs(1),
        }
    }
}

/// A sink that rewrites what a twisted codec pushes.
struct Rewrite<'a> {
    sink: &'a mut dyn ExecSink,
    twist: Twist,
    run: u64,
}

impl ExecSink for Rewrite<'_> {
    fn push(&mut self, mut meta: VenueMeta, mut ev: ExecEvent) {
        match (self.twist, &mut ev) {
            (Twist::SeqDrifts, _) => meta.venue_seq = Some(self.run),
            (Twist::SwapsLiquidity, ExecEvent::Fill(fill)) => {
                fill.liquidity = match fill.liquidity {
                    Liquidity3::Maker => Liquidity3::Taker,
                    _ => Liquidity3::Maker,
                };
            }
            (
                Twist::NegatesPositions,
                ExecEvent::Position { qty, .. } | ExecEvent::ResyncPosition { qty, .. },
            ) => *qty = SignedLots(-qty.0),
            _ => {}
        }
        self.sink.push(meta, ev);
    }
}

impl ExecCodec for Twisted {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_open(stream, ctx, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        self.inner.encode(cmd, rpc, specs, ctx, t, fx)
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if self.twist == Twist::RefusesOnOddRuns && !self.run.is_multiple_of(2) {
            return Err(DecodeError::Malformed("an odd run"));
        }
        if self.twist == Twist::TimerDrifts {
            fx.push(self.timer());
        }
        let (twist, run) = (self.twist, self.run);
        let sink = &mut Rewrite { sink, twist, run };
        self.inner.on_frame(stream, f, scope, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.inner.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        if self.twist == Twist::TimerDrifts {
            fx.push(self.timer());
        }
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture directories made for a test.
// ---------------------------------------------------------------------------------------------

/// The subdirectories of the toy's fixtures that hold case files.
const CASE_DIRS: [&str; 4] = [
    "fee_sign",
    "liquidity_reported",
    "position_signed",
    "decoder_deterministic",
];

/// A fixture directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    /// An empty fixture directory named after `test`.
    fn empty(test: &str) -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "fbc-conformance-frames-{}-{test}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    /// A copy of the toy's case files named after `test`.
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

    /// Writes `text` to `file` inside it.
    fn write(&self, file: &str, text: &str) {
        fs::write(self.at(file), text).unwrap();
    }

    /// `check` run on the toy against it.
    fn run(&self, check: Check) -> Result<Verdict, Failure> {
        check(&Subject::new(&ToyFactory, &self.0, assumed).unwrap())
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

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// The toy declaring that a positive fee is a rebate, while its frames, like its declaration
/// before, report a positive fee as a cost: every fee decodes with its sign flipped.
fn fee_flipped() -> Variant {
    Variant::declaring(|caps| {
        caps.exec.as_mut().unwrap().fills.fee_sign = VenueFeeSign::PositiveIsRebate;
    })
}

#[test]
fn fee_sign_fails_a_toy_variant_that_flips_a_fees_sign_naming_every_fill() {
    let failure = failed(fee_flipped().run(suite::fee_sign));
    assert_eq!(failure.check, "fee_sign");
    assert_eq!(named(&failure), ["FillCaps.fee_sign"; 3]);
    assert_eq!(
        said(&failure, "FillCaps.fee_sign"),
        [
            "fee_sign/rebate.frames line 4: a fill's fee decoded as a cost of 5234600 nanos USDC, \
             not below zero (a rebate)",
            "fee_sign/rebate.frames line 5: a fill's fee decoded as a cost of 32717500 nanos \
             USDC, not below zero (a rebate)",
            "fee_sign/paid.frames line 3: a fill's fee decoded as a cost of -58887000 nanos USDC, \
             not above zero (paid)",
        ]
    );
}

#[test]
#[should_panic(expected = "FillCaps.fee_sign")]
fn the_suite_test_for_fee_sign_panics_naming_the_fee_sign() {
    suite::run(suite::fee_sign, &fee_flipped(), FIXTURES, assumed);
}

#[test]
fn decoder_deterministic_fails_a_toy_variant_that_decodes_nondeterministically() {
    let variant = Variant::twisted(Twist::SeqDrifts);
    let failure = failed(variant.run(suite::decoder_deterministic));
    assert_eq!(failure.check, "decoder_deterministic");
    // Every line that pushes an event differs, each named (Codex r4216777885): two in the
    // rebate and maker cases, four in the orders case (its last frame is refused alike), one
    // in every other case; the position cases only at the resync's end, which pushes what the
    // toy held, so their resync line and the frames before differ in nothing.
    assert_eq!(named(&failure), ["ExecCodec::on_frame"; 12]);
    let said = said(&failure, "ExecCodec::on_frame");
    let lines: Vec<&str> = said
        .iter()
        .map(|what| what.split(" decoded").next().unwrap())
        .collect();
    assert_eq!(
        lines,
        [
            "fee_sign/paid.frames line 3",
            "fee_sign/rebate.frames line 4",
            "fee_sign/rebate.frames line 5",
            "liquidity_reported/maker.frames line 2",
            "liquidity_reported/maker.frames line 3",
            "liquidity_reported/taker.frames line 2",
            "position_signed/long.frames line 8",
            "position_signed/short.frames line 5",
            "decoder_deterministic/orders.frames line 4",
            "decoder_deterministic/orders.frames line 5",
            "decoder_deterministic/orders.frames line 6",
            "decoder_deterministic/orders.frames line 7",
        ]
    );
    assert_eq!(
        said[0],
        "fee_sign/paid.frames line 3 decoded differently on two runs: the events pushed"
    );
    // The frame's bytes are never shown: a frame can carry a credential.
    assert!(!failure.to_string().contains("fid="), "{failure}");
}

#[test]
#[should_panic(expected = "decoded differently on two runs")]
fn the_suite_test_for_decoder_deterministic_panics_naming_the_line() {
    let variant = Variant::twisted(Twist::SeqDrifts);
    suite::run(suite::decoder_deterministic, &variant, FIXTURES, assumed);
}

// ---------------------------------------------------------------------------------------------
// The other variants.
// ---------------------------------------------------------------------------------------------

#[test]
fn decoder_deterministic_names_effects_and_results_that_differ_and_the_call_that_made_them() {
    let failure = failed(Variant::twisted(Twist::TimerDrifts).run(suite::decoder_deterministic));
    // The position cases differ first at their resync line.
    assert_eq!(said(&failure, "ExecCodec::resync").len(), 2);
    assert_eq!(
        said(&failure, "ExecCodec::on_frame")[0],
        "fee_sign/paid.frames line 3 decoded differently on two runs: the effects asked for"
    );

    let failure =
        failed(Variant::twisted(Twist::RefusesOnOddRuns).run(suite::decoder_deterministic));
    assert_eq!(
        said(&failure, "ExecCodec::on_frame")[0],
        "fee_sign/paid.frames line 3 decoded differently on two runs: its result, the events \
         pushed"
    );
}

#[test]
fn the_toys_variants_that_flip_or_drift_still_pass_the_checks_they_do_not_break() {
    let flipped = fee_flipped();
    for check in [
        suite::liquidity_reported,
        suite::position_signed,
        suite::decoder_deterministic,
    ] {
        assert!(matches!(flipped.run(check), Ok(Verdict::Passed { .. })));
    }
    let drifting = Variant::twisted(Twist::SeqDrifts);
    for check in [
        suite::fee_sign,
        suite::liquidity_reported,
        suite::position_signed,
    ] {
        assert!(matches!(drifting.run(check), Ok(Verdict::Passed { .. })));
    }
}

#[test]
fn liquidity_reported_fails_a_toy_that_swaps_a_fills_liquidity() {
    let failure = failed(Variant::twisted(Twist::SwapsLiquidity).run(suite::liquidity_reported));
    assert_eq!(named(&failure), ["FillCaps.liquidity_flag"; 3]);
    assert_eq!(
        said(&failure, "FillCaps.liquidity_flag")[2],
        "liquidity_reported/taker.frames line 2: a fill decoded as Maker, not Taker"
    );
}

#[test]
fn position_signed_fails_a_toy_that_reports_positions_with_the_opposite_sign() {
    let failure = failed(Variant::twisted(Twist::NegatesPositions).run(suite::position_signed));
    let capability = "SignedLots: positive is long";
    assert_eq!(named(&failure), [capability; 3]);
    assert_eq!(
        said(&failure, capability),
        [
            "position_signed/long.frames line 8: a position decoded as -25 lots, not above zero \
             (long)",
            "position_signed/long.frames line 8: a position decoded as -3 lots, not above zero \
             (long)",
            "position_signed/short.frames line 5: a position decoded as 12 lots, not below zero \
             (short)",
        ]
    );
}

#[test]
fn every_check_is_skipped_for_a_venue_without_order_entry() {
    let variant = Variant {
        codec: None,
        ..Variant::declaring(|caps| caps.exec = None)
    };
    for (name, check) in CHECKS {
        match variant.run(check) {
            Ok(Verdict::Skipped { check, why }) => {
                assert_eq!(check, name);
                assert!(why.contains("VenueCaps.exec is None"), "{why}");
            }
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[test]
fn liquidity_reported_is_skipped_for_fills_without_a_liquidity_flag() {
    let variant = Variant::declaring(|caps| {
        caps.exec.as_mut().unwrap().fills.liquidity_flag = false;
    });
    let verdict = variant.run(suite::liquidity_reported).unwrap();
    assert!(
        matches!(verdict, Verdict::Skipped { why, .. } if why.contains("liquidity_flag is false")),
        "{verdict:?}"
    );
}

#[test]
fn every_check_fails_a_venue_declaring_order_entry_that_builds_no_codec() {
    let variant = Variant {
        codec: None,
        ..Variant::declaring(|_| {})
    };
    for (name, check) in CHECKS {
        let failure = failed(variant.run(check));
        assert_eq!(failure.check, name);
        assert!(said(&failure, "VenueCaps.exec")[0].contains("builds no codec"));
    }
}

// ---------------------------------------------------------------------------------------------
// The fixture directory.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_check_fails_a_fixture_directory_without_its_subdirectory() {
    let scratch = Scratch::empty("no-dirs");
    for (name, check) in &CHECKS[..3] {
        let failure = failed(scratch.run(*check));
        assert!(said(&failure, name)[0].starts_with("missing:"), "{failure}");
    }
    let failure = failed(scratch.run(suite::decoder_deterministic));
    assert_eq!(named(&failure), ["fixtures"]);
    assert!(said(&failure, "fixtures")[0].contains("nothing was decoded"));
}

#[test]
fn a_check_fails_a_subdirectory_it_cannot_read() {
    let scratch = Scratch::empty("unreadable");
    scratch.write("fee_sign", "a file, not a directory");
    let failure = failed(scratch.run(suite::fee_sign));
    assert!(said(&failure, "fee_sign")[0].starts_with("cannot read"));
    let failure = failed(scratch.run(suite::decoder_deterministic));
    assert_eq!(named(&failure), ["fee_sign"]);
}

#[test]
fn a_check_fails_a_missing_case_and_a_case_it_never_reads() {
    let scratch = Scratch::toy("stray");
    fs::remove_file(scratch.at("fee_sign/paid.frames")).unwrap();
    scratch.write("fee_sign/zero.frames", "# no fill\n");
    scratch.write("fee_sign/notes.txt", "not a case: ignored");
    let failure = failed(scratch.run(suite::fee_sign));
    assert_eq!(
        named(&failure),
        ["fee_sign/paid.frames", "fee_sign/zero.frames"]
    );
    assert!(said(&failure, "fee_sign/paid.frames")[0].starts_with("cannot read"));
    assert!(said(&failure, "fee_sign/zero.frames")[0].contains("never checked"));
}

#[test]
fn a_check_fails_a_malformed_case_naming_its_line() {
    let scratch = Scratch::toy("malformed");
    scratch.write("position_signed/short.frames", "resync\nframe rsbegin\n");
    let failure = failed(scratch.run(suite::position_signed));
    assert_eq!(
        said(&failure, "position_signed/short.frames"),
        ["line 2: starts with neither `text `, `hex ` nor `resync`"]
    );
    let failure = failed(scratch.run(suite::decoder_deterministic));
    assert_eq!(named(&failure), ["position_signed/short.frames"]);
}

#[test]
fn a_check_fails_a_refused_frame_and_a_case_that_states_nothing() {
    let scratch = Scratch::toy("refused");
    // A binary frame the toy refuses, and nothing else: no fill is judged either.
    scratch.write("liquidity_reported/taker.frames", "hex 66 69 6c 6c\n");
    let failure = failed(scratch.run(suite::liquidity_reported));
    assert_eq!(
        said(&failure, "liquidity_reported/taker.frames"),
        [
            "line 1 refused: malformed frame: binary frame",
            "decodes to no fill: a case that states none proves nothing",
        ]
    );
    // Both runs refuse it alike: no breach of determinism.
    assert!(matches!(
        scratch.run(suite::decoder_deterministic),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
fn a_position_case_whose_resync_is_never_answered_states_no_position() {
    let scratch = Scratch::toy("unanswered");
    scratch.write("position_signed/long.frames", "resync\n");
    let failure = failed(scratch.run(suite::position_signed));
    assert_eq!(
        said(&failure, "position_signed/long.frames"),
        ["decodes to no position: a case that states none proves nothing"]
    );
}

#[test]
fn events_of_another_kind_in_a_case_are_not_judged() {
    let scratch = Scratch::toy("other-kinds");
    let paid = fs::read_to_string(scratch.at("fee_sign/paid.frames")).unwrap();
    let open = "text order|cid=c-3|vid=toy-7003|sym=TOYA-PERP|side=S|st=open|cum=0|px=130860|\
                qty=10|po=0|ro=0|seq=12\n";
    scratch.write("fee_sign/paid.frames", &format!("{open}{paid}"));
    let verdict = scratch.run(suite::fee_sign).unwrap();
    assert!(
        matches!(&verdict, Verdict::Passed { probed, .. } if probed[1] == "fee_sign/paid.frames: 1 fill"),
        "{verdict:?}"
    );
}

#[test]
fn fill_checks_are_skipped_for_fills_derived_from_order_status() {
    // Codex r4216777891: such a venue's codec pushes order updates, never a fill event, so the
    // checks that judge fill events have nothing to judge; positions are still judged.
    let variant = Variant::declaring(|caps| {
        caps.exec.as_mut().unwrap().fills.source = FillSource::DerivedFromOrderStatus;
    });
    for check in [suite::fee_sign, suite::liquidity_reported] {
        let verdict = variant.run(check).unwrap();
        assert!(
            matches!(verdict, Verdict::Skipped { why, .. } if why.contains("DerivedFromOrderStatus")),
            "{verdict:?}"
        );
    }
    assert!(matches!(
        variant.run(suite::position_signed),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
fn decoder_deterministic_fails_a_case_that_hands_the_codec_nothing() {
    // Codex r4216777870: a case of comments alone decodes nothing, so it proves nothing.
    let scratch = Scratch::empty("empty-case");
    fs::create_dir_all(scratch.at("decoder_deterministic")).unwrap();
    scratch.write("decoder_deterministic/empty.frames", "# nothing\n\n");
    let failure = failed(scratch.run(suite::decoder_deterministic));
    assert_eq!(named(&failure), ["decoder_deterministic/empty.frames"]);
    assert_eq!(
        said(&failure, "decoder_deterministic/empty.frames"),
        ["hands the codec nothing: a case that decodes nothing proves nothing"]
    );
}
