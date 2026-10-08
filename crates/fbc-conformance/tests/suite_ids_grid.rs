//! FBC-2re's done line, its second half: `encode_deterministic` fails against a toy variant
//! whose encode reads the real time, naming every command it encoded differently. Around it,
//! every other way `encode_deterministic`, `ids_roundtrip`, `restart_cid` and `price_grid`
//! fail or are skipped: a client-id format too small for our ids, Java-era ids missing or read
//! as ours, a codec that loses our ids from a resync, a resync case that shows nothing of ours
//! resting or refuses a frame, a codec that rounds prices, refuses them or sends them without
//! their request, a banded grid with nothing below it, no post-only order, no order entry, and
//! no codec built.

mod toy_setup;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fbc_conformance::suite::{self, Failure, Setup, Subject, Verdict};
use fbc_conformance::toy::{self, ToyExec, ToyFactory, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, CidMatch, ClientIdFormat, ConfigError, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint,
    ExecEvent, ExecSink, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound,
    InboundSpans, InstrumentSpecDraft, MdCodec, NotSentReason, OrderKind, PathStamps, PriceGrid,
    RawFrame, RpcId, Secrets, SpecTable, StreamId, Subscription, SymbolError, TagSet, Ticks,
    TimerTag, VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta, WallNs,
};
use rust_decimal::Decimal;
use toy_setup::{FIXTURES, assumed};

/// The four checks, by name.
type Check = fn(&Subject<'_>) -> Result<Verdict, Failure>;
const CHECKS: [(&str, Check); 4] = [
    ("encode_deterministic", suite::encode_deterministic),
    ("ids_roundtrip", suite::ids_roundtrip),
    ("restart_cid", suite::restart_cid),
    ("price_grid", suite::price_grid),
];

// ---------------------------------------------------------------------------------------------
// The variants.
// ---------------------------------------------------------------------------------------------

/// How a variant's codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Twist {
    /// Every command is encoded at the real time, not its context's.
    ReadsTheClock,
    /// Every client id it decodes reads as not ours.
    LosesClientIds,
    /// Every order's price is rounded down to a multiple of ten ticks.
    RoundsPrices,
    /// Every placement is refused `Unsupported`.
    RefusesPlacements,
    /// A placement's frames name no rpc.
    UnlabelledPlacements,
}

/// The toy with its caps edited, its codec twisted or not built, under a setup.
struct Variant {
    caps: fn(&mut VenueCaps),
    codec: Option<Option<Twist>>,
    setup: fn() -> Setup,
}

impl Variant {
    /// The toy, its caps edited.
    fn declaring(caps: fn(&mut VenueCaps)) -> Variant {
        Variant {
            caps,
            codec: Some(None),
            setup: assumed,
        }
    }

    /// The toy, its codec twisted.
    fn twisted(twist: Twist) -> Variant {
        Variant {
            codec: Some(Some(twist)),
            ..Variant::declaring(|_| {})
        }
    }

    /// `check` run against `fixtures`.
    fn run_in(&self, fixtures: &Path, check: Check) -> Result<Verdict, Failure> {
        check(&Subject::new(self, fixtures, self.setup).unwrap())
    }

    /// `check` run against the toy's fixtures.
    fn run(&self, check: Check) -> Result<Verdict, Failure> {
        self.run_in(Path::new(FIXTURES), check)
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
        let Some(twist) = self.codec? else {
            return ToyFactory.exec_codec(cfg, creds);
        };
        let codec: Box<dyn ExecCodec> = Box::new(Twisted {
            inner: ToyExec::new(Box::new(ToySigner)),
            twist,
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

/// The toy's codec, twisted.
struct Twisted {
    inner: ToyExec,
    twist: Twist,
}

/// A sink that reads every client id it is pushed as not ours.
struct LoseIds<'a>(&'a mut dyn ExecSink);

impl ExecSink for LoseIds<'_> {
    fn push(&mut self, meta: VenueMeta, mut ev: ExecEvent) {
        let cid = match &mut ev {
            ExecEvent::ResyncOrder(snap) => Some(&mut snap.cid),
            ExecEvent::Order(update) => Some(&mut update.cid),
            ExecEvent::Fill(fill) => Some(&mut fill.cid),
            _ => None,
        };
        if let Some(cid) = cid {
            *cid = Some(CidMatch::Unparseable);
        }
        self.0.push(meta, ev);
    }
}

/// `cmd` with every order's price rounded down to a multiple of ten ticks.
fn rounded(cmd: &VenueCommand) -> VenueCommand {
    let round = |px: Ticks| Ticks(px.0 / 10 * 10);
    match cmd {
        VenueCommand::Place(o) => {
            let mut o = o.clone();
            if let OrderKind::Limit { px } = o.kind {
                o.kind = OrderKind::Limit { px: round(px) };
            }
            VenueCommand::Place(o)
        }
        other => other.clone(),
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
        let placing = matches!(cmd, VenueCommand::Place(_));
        match self.twist {
            Twist::ReadsTheClock => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let ctx = EncodeCtx {
                    wall: WallNs(i64::try_from(now.as_nanos()).unwrap()),
                    ..ctx.clone()
                };
                self.inner.encode(cmd, rpc, specs, &ctx, t, fx)
            }
            Twist::RoundsPrices => self.inner.encode(&rounded(cmd), rpc, specs, ctx, t, fx),
            Twist::RefusesPlacements if placing => Err(NotSentReason::Unsupported),
            Twist::UnlabelledPlacements if placing => {
                let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, fx)?;
                for effect in fx.take() {
                    fx.push(match effect {
                        Effect::Send {
                            stream,
                            frame,
                            class,
                            charge,
                            ..
                        } => Effect::Send {
                            stream,
                            frame,
                            rpc: None,
                            class,
                            charge,
                        },
                        other => other,
                    });
                }
                Ok(receipt)
            }
            _ => self.inner.encode(cmd, rpc, specs, ctx, t, fx),
        }
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
        if self.twist == Twist::LosesClientIds {
            return self
                .inner
                .on_frame(stream, f, scope, specs, &mut LoseIds(sink), fx);
        }
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
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// Setups and fixture directories made for a test.
// ---------------------------------------------------------------------------------------------

/// The toy's setup with no golden command.
fn no_goldens() -> Setup {
    Setup {
        goldens: Vec::new(),
        ..assumed()
    }
}

/// The toy's setup with its first instrument on a banded grid: 0.5 from 100.0 and 1.5 from
/// 200.0, so a bid below 100.0 has no valid price.
fn banded() -> Setup {
    let mut specs = toy::specs();
    let mut a = specs.get(toy::INST_A).unwrap().clone();
    let bands = [
        (Decimal::new(1000, 1), Decimal::new(5, 1)),
        (Decimal::new(2000, 1), Decimal::new(15, 1)),
    ];
    a.price_grid = PriceGrid::banded(&bands).unwrap();
    specs.insert(a);
    Setup { specs, ..assumed() }
}

/// A fixture directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    /// A fixture directory named after `test` holding `files`, each a path and its text.
    fn with(test: &str, files: &[(&str, &str)]) -> Scratch {
        let dir =
            std::env::temp_dir().join(format!("fbc-conformance-ids-{}-{test}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        for (file, text) in files {
            let path = dir.join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        }
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    /// `check` run on `variant` against it.
    fn run(&self, variant: &Variant, check: Check) -> Result<Verdict, Failure> {
        variant.run_in(&self.0, check)
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

/// What a passing `check` probed.
fn probed(outcome: Result<Verdict, Failure>) -> Vec<String> {
    match outcome {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("expected a pass, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn encode_deterministic_fails_a_toy_variant_whose_encode_reads_the_real_time() {
    let failure = failed(Variant::twisted(Twist::ReadsTheClock).run(suite::encode_deterministic));
    assert_eq!(failure.check, "encode_deterministic");
    // The toy signs the time into every place, amend and cancel it writes, so each of those
    // differs; its queries carry no time and do not.
    assert_eq!(
        named(&failure),
        [
            "golden place",
            "golden place-batch",
            "golden amend",
            "golden cancel",
            "golden cancel-batch",
            "the plainest order",
            "AmendCaps.refs has Venue: an amend",
            "OrderCaps.cancel_refs has Venue: a cancel",
            "OrderCaps.cancel_refs has Client: a cancel",
            "CancelBatch.refs has Venue: a batch cancel",
            "CancelBatch.refs has PlacementNonce: a batch cancel",
        ]
    );
    assert_eq!(
        said(&failure, "golden place"),
        [
            "encoded twice under one context by freshly built codecs, 5 ms apart: the effects \
             asked for differ"
        ]
    );
    // The frames' bytes are never shown: a frame can carry a credential.
    assert!(!failure.to_string().contains("ts="), "{failure}");
}

#[test]
#[should_panic(expected = "encoded twice under one context")]
fn the_suite_test_for_encode_deterministic_panics_naming_the_command() {
    let variant = Variant::twisted(Twist::ReadsTheClock);
    suite::run(suite::encode_deterministic, &variant, FIXTURES, assumed);
}

#[test]
fn a_variant_refusing_alike_on_both_encodings_passes_encode_deterministic() {
    // A refusal is no breach as long as both encodings refuse alike.
    let probed =
        probed(Variant::twisted(Twist::RefusesPlacements).run(suite::encode_deterministic));
    assert!(
        probed.iter().any(|p| p == "the plainest order"),
        "{probed:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// encode_deterministic, otherwise.
// ---------------------------------------------------------------------------------------------

#[test]
fn encode_deterministic_builds_its_own_commands_when_the_setup_lists_no_golden() {
    let variant = Variant {
        setup: no_goldens,
        ..Variant::declaring(|_| {})
    };
    let probed = probed(variant.run(suite::encode_deterministic));
    assert_eq!(probed[0], "the plainest order");
    assert_eq!(probed.len(), 8);
}

#[test]
fn encode_deterministic_fails_a_venue_with_nothing_to_encode() {
    // No golden, no order the caps allow, and no reference to name one by.
    let variant = Variant {
        setup: no_goldens,
        ..Variant::declaring(|caps| {
            let o = &mut caps.exec.as_mut().unwrap().order;
            o.kinds = TagSet::of(&[]);
            o.amend = None;
            o.cancel_refs = TagSet::of(&[]);
            o.batch_cancel = None;
            o.query_refs = TagSet::of(&[]);
        })
    };
    let failure = failed(variant.run(suite::encode_deterministic));
    assert_eq!(
        said(&failure, "Setup.goldens"),
        ["lists no command, and the caps allow none to build: nothing to encode"]
    );
}

// ---------------------------------------------------------------------------------------------
// ids_roundtrip.
// ---------------------------------------------------------------------------------------------

#[test]
fn ids_roundtrip_fails_a_client_id_format_too_small_for_our_ids() {
    // Twelve decimal digits cannot carry the tag, the namespace, the sequence and the checks.
    let variant = Variant::declaring(|caps| {
        caps.exec.as_mut().unwrap().order.client_id = ClientIdFormat::Numeric { max_digits: 12 };
    });
    let failure = failed(variant.run(suite::ids_roundtrip));
    let capability = "OrderCaps.client_id is Numeric { max_digits: 12 }";
    let said = said(&failure, capability);
    // Every minted id, then the Java-era ids, which a numeric venue still reads as not ours.
    assert_eq!(said.len(), 9, "{failure}");
    assert_eq!(
        said[0],
        "our id of sequence number 1 does not round-trip: read back as Err(DoesNotFit { \
         needed: 13, max: 12 })"
    );
}

#[test]
fn ids_roundtrip_fails_a_java_era_id_read_as_ours_or_another_engines() {
    // The suite's own id 1, and the same payload in namespace 2, in the toy's spelling.
    let java = "# ours, then another engine's\n00SKjrSqeFPTfbl3hrG0S\n1759363199123\n";
    let scratch = Scratch::with("ours", &[("ids_roundtrip/java_era.txt", java)]);
    let failure = failed(scratch.run(&Variant::declaring(|_| {}), suite::ids_roundtrip));
    let capability = "OrderCaps.client_id is Alnum { max_len: 32, charset: Alphanumeric }";
    let said = said(&failure, capability);
    assert_eq!(said.len(), 1, "{failure}");
    assert!(
        said[0].starts_with("reads the Java-era id 00SKjrSqeFPTfbl3hrG0S as Ours("),
        "{}",
        said[0]
    );
}

#[test]
fn ids_roundtrip_fails_a_java_era_file_missing_or_listing_nothing() {
    let toy = Variant::declaring(|_| {});
    let missing = Scratch::with("missing", &[]);
    let failure = failed(missing.run(&toy, suite::ids_roundtrip));
    assert!(said(&failure, "ids_roundtrip/java_era.txt")[0].starts_with("cannot read"));

    let empty = Scratch::with("empty", &[("ids_roundtrip/java_era.txt", "# none\n\n")]);
    let failure = failed(empty.run(&toy, suite::ids_roundtrip));
    assert_eq!(
        said(&failure, "ids_roundtrip/java_era.txt"),
        ["lists no id"]
    );
}

// ---------------------------------------------------------------------------------------------
// restart_cid.
// ---------------------------------------------------------------------------------------------

#[test]
fn restart_cid_fails_a_codec_that_loses_our_ids_from_a_resync() {
    let variant = Variant::twisted(Twist::LosesClientIds);
    let failure = failed(variant.run(suite::restart_cid));
    // It loses every id, so it shows nothing of ours resting.
    assert_eq!(
        said(&failure, "restart_cid/resting.frames"),
        [
            "shows no order of ours resting in a resync's answer: a restart with nothing \
             resting proves nothing"
        ]
    );
}

#[test]
fn restart_cid_fails_a_resync_that_hides_the_newest_id_naming_each_reissued() {
    // Only our id 5 rests, and ids 6 to 8 are shown nowhere: the restarted mint issues them
    // again, and id 1 to 5 never.
    let case = "resync\n\
                text rsbegin|wm=1759363200000000000\n\
                text rsorder|cid=00SKjrSqeFPTfblNYkHZx|vid=toy-7305|sym=TOYB-PERP|side=S|st=open|\
                px=130875|qty=3|cum=1|po=1|ro=0\n\
                text rsend\n";
    let scratch = Scratch::with("hidden", &[("restart_cid/resting.frames", case)]);
    let failure = failed(scratch.run(&Variant::declaring(|_| {}), suite::restart_cid));
    let said = said(&failure, "ExecCodec::resync");
    assert_eq!(said.len(), 3, "{failure}");
    assert_eq!(
        said[0],
        "a mint restarted from restart_cid/resting.frames issues our id of sequence number 6 \
         again: the highest of ours the frames show is 5"
    );
}

#[test]
fn restart_cid_fails_a_refused_frame_and_a_missing_case() {
    let toy = Variant::declaring(|_| {});
    let scratch = Scratch::with(
        "refused",
        &[("restart_cid/resting.frames", "hex 66 69 6c 6c\n")],
    );
    let failure = failed(scratch.run(&toy, suite::restart_cid));
    assert_eq!(
        said(&failure, "restart_cid/resting.frames"),
        [
            "line 1 refused: malformed frame: binary frame",
            "shows no order of ours resting in a resync's answer: a restart with nothing \
             resting proves nothing",
        ]
    );

    let missing = Scratch::with("no-case", &[]);
    let failure = failed(missing.run(&toy, suite::restart_cid));
    assert!(said(&failure, "restart_cid/resting.frames")[0].starts_with("cannot read"));
}

// ---------------------------------------------------------------------------------------------
// price_grid.
// ---------------------------------------------------------------------------------------------

#[test]
fn price_grid_fails_a_codec_that_rounds_the_prices_it_is_given() {
    let failure = failed(Variant::twisted(Twist::RoundsPrices).run(suite::price_grid));
    let first = &failure.breaches[0];
    // Around ten ticks, asks go out at ticks 9, 10 and 11, and the variant sends 10 and 11
    // both as 10.
    assert_eq!(first.capability, "a post-only ask on TOYA-PERP at tick 11");
    assert_eq!(
        first.what,
        "encoded exactly as a post-only ask at tick 10: the codec does not send the price it is \
         given"
    );
}

#[test]
fn price_grid_fails_a_codec_that_refuses_a_valid_price_or_sends_it_without_its_request() {
    let failure = failed(Variant::twisted(Twist::RefusesPlacements).run(suite::price_grid));
    assert_eq!(failure.breaches.len(), 2 * 54, "{failure}");
    assert_eq!(
        said(&failure, "a post-only bid on TOYA-PERP at tick 9"),
        ["refused as NotSent(Unsupported) by a freshly built codec, though the price is valid"]
    );

    let failure = failed(Variant::twisted(Twist::UnlabelledPlacements).run(suite::price_grid));
    assert_eq!(
        said(&failure, "a post-only ask on TOYB-PERP at tick 1000000001"),
        ["encoded, but its effects do not carry request 1"]
    );
}

#[test]
fn price_grid_passes_a_banded_grid_with_no_bid_below_its_first_band() {
    let variant = Variant {
        setup: banded,
        ..Variant::declaring(|_| {})
    };
    let probed = probed(variant.run(suite::price_grid));
    assert_eq!(
        probed[0],
        "TOYA-PERP: 55 model prices quantized to 28 order prices, 28 post-only orders sent"
    );
}

#[test]
fn price_grid_quantizes_without_sending_where_the_caps_allow_no_post_only_order() {
    let variant = Variant::declaring(|caps| {
        caps.exec.as_mut().unwrap().order.post_only = false;
    });
    let probed = probed(variant.run(suite::price_grid));
    assert_eq!(
        probed[0],
        "TOYA-PERP: 45 model prices quantized to 54 order prices, no post-only limit order the \
         caps allow to send"
    );
}

// ---------------------------------------------------------------------------------------------
// Every check.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_three_checks_of_order_entry_are_skipped_without_it_and_price_grid_still_quantizes() {
    let variant = Variant {
        codec: None,
        ..Variant::declaring(|caps| caps.exec = None)
    };
    for (name, check) in &CHECKS[..3] {
        match variant.run(*check) {
            Ok(Verdict::Skipped { check, why }) => {
                assert_eq!(check, *name);
                assert!(why.contains("VenueCaps.exec is None"), "{why}");
            }
            other => panic!("{name}: {other:?}"),
        }
    }
    let probed = probed(variant.run(suite::price_grid));
    assert!(probed[0].ends_with("no post-only limit order the caps allow to send"));
}

#[test]
fn every_check_but_ids_roundtrip_fails_a_venue_declaring_order_entry_that_builds_no_codec() {
    let variant = Variant {
        codec: None,
        ..Variant::declaring(|_| {})
    };
    for (name, check) in CHECKS {
        if name == "ids_roundtrip" {
            // It builds no codec: the core's codec and decode scope are all it reads.
            assert!(matches!(variant.run(check), Ok(Verdict::Passed { .. })));
            continue;
        }
        let failure = failed(variant.run(check));
        assert_eq!(failure.check, name);
        assert!(said(&failure, "VenueCaps.exec")[0].contains("builds no codec"));
    }
}
