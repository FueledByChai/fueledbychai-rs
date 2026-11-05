//! FBC-8ew's done line, its second half: `caps_truthful` run against a deliberately broken toy,
//! whose caps declare post-only orders absent while its codec still sends them, fails and
//! names that capability (BT-502). Around it, every other way the two checks can fail: a caps
//! declaration the toy's codec does not keep, a refusal with the wrong reason or with effects,
//! a control refused or sent without its request, an instrument cancel-all charged wider than
//! its instrument, a codec that encodes differently once it has seen the order, and a setup
//! the factory refuses; and where a check passes or is skipped with nothing to check.
//!
//! FBC-fuis's done line too: both checks pass a toy whose codec refuses an order below its
//! instrument's `min_notional` or above its `max_order_size`, on a setup where the instrument's
//! `min_size` at the probe's price is below `min_notional`; `caps_truthful` passes a toy whose
//! identical cancel-all frames go to each instrument's own stream, and accepts `Unsupported`
//! for a flag-conflict amend where `AmendCaps.refs` names no reference an amend can carry.
//!
//! And FBC-7y5e's: `price_grid` passes that codec on a setup whose `min_size` at the prices it
//! probes is below `min_notional`, and fails, naming the spec field, a setup where no size within
//! `max_order_size` reaches `min_notional` at a price it probes, sending nothing on it; and it
//! sends no bid at a price of zero, on a banded grid whose first band starts there.
//!
//! FBC-6rfe's done line: both checks pass a toy whose cancels, batch cancels, amends and queries
//! refuse a venue order id that is not a UUID's 8-4-4-4-12 hex form, as QFEX's do, on a setup
//! whose venue ids are UUIDs (`suite::uuid_venue_order_id`); the same toy fails them on the
//! toy's own ids, and a setup whose venue ids the suite cannot use fails, naming
//! `Setup.venue_order_ids`.

use std::collections::BTreeSet;

use fbc_conformance::suite::{self, Failure, Setup, Subject, Verdict};
use fbc_conformance::toy::{self, ToyExec, ToyFactory, ToySigner};
use fbc_core::{
    AccountSummary, AmendOrder, AssetKey, Batch, CancelBatch, CancelOnDisconnect, CancelScope,
    Channel, ConfigError, CtxCall, DecodeError, Effect, Effects, EncodeCtx, EncodeReceipt,
    EndpointPlan, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, Feature, FieldSpec, HttpFailure,
    HttpPlan, HttpTag, Inbound, InboundSpans, InstrumentId, InstrumentSpec, InstrumentSpecDraft,
    Lots, MdCodec, Money, MonoNs, NonceBlock, NotSentReason, OrderCaps, OrderKindTag, OrderRef,
    PathStamps, RawFrame, RefKind, RpcId, Secrets, Side, SpecTable, StreamId, Subscription,
    Support, SymbolError, TagSet, Ticks, TifTag, TimerTag, VenueCaps, VenueCommand, VenueConfig,
    VenueError, VenueFactory, VenueMeta, VenueOrderId, WallNs,
};

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/conformance-toy"
);

/// The toy's assumed setup.
fn assumed() -> Setup {
    Setup {
        specs: toy::specs(),
        cfg: VenueConfig::new(),
        creds: Secrets::new(),
        goldens: Vec::new(),
        exec_stream: toy::EXEC_STREAM,
        order_entry: None,
        bootstrap: None,
        venue_order_ids: |i| format!("toy-{i}"),
    }
}

/// A setup with the toy's first instrument only.
fn one_instrument() -> Setup {
    let mut specs = SpecTable::new();
    specs.insert(toy::specs().get(toy::INST_A).unwrap().clone());
    Setup { specs, ..assumed() }
}

/// The toy's setup with its first instrument on a banded grid that starts at 100.0 in steps
/// of 0.5 (tick 200 of the finest 0.5), so tick 100 is off the grid.
fn banded() -> Setup {
    let mut specs = toy::specs();
    let mut a = specs.get(toy::INST_A).unwrap().clone();
    let bands = [
        (
            rust_decimal::Decimal::new(1000, 1),
            rust_decimal::Decimal::new(5, 1),
        ),
        (
            rust_decimal::Decimal::new(2000, 1),
            rust_decimal::Decimal::new(15, 1),
        ),
    ];
    a.price_grid = fbc_core::PriceGrid::banded(&bands).unwrap();
    specs.insert(a);
    Setup { specs, ..assumed() }
}

/// A setup with no instrument.
fn no_instruments() -> Setup {
    Setup {
        specs: SpecTable::new(),
        ..assumed()
    }
}

/// How a broken toy's codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Twist {
    /// The toy's codec as it is.
    None,
    /// A refusal still asks for a timer.
    Leaky,
    /// `Unsupported` is reported as `Unencodable`.
    Mislabelled,
    /// Every command is refused `Unsupported`.
    RefusesAll,
    /// An account-wide protection arm is refused despite the declared cap.
    RefusesArm,
    /// Every disarm, `ArmCancelOnDisconnect(false)`, is refused `Unsupported` with no effect,
    /// as a codec for a venue offering none refuses it (decision 0141); the arm is still sent.
    RefusesDisarm,
    /// Every placement is refused `Unsupported`.
    RefusesPlacements,
    /// A placement's frames name no rpc.
    UnlabelledPlacements,
    /// An instrument cancel-all is written as an account-wide one, still charged to its
    /// instrument.
    AccountWide,
    /// A cancel-all goes as an HTTP request whose body is the frame it would have been.
    OverHttp,
    /// The first instrument's cancel-all is written as an account-wide one; the second's is
    /// refused.
    AccountWideRefusingOther,
    /// An order or amend priced off its instrument's grid is refused `Unencodable`.
    ChecksGrid,
    /// A sent request's frames name no rpc.
    Unlabelled,
    /// Every charge names no instrument.
    Widened,
    /// Once it has sent a placement, every later request also asks for a timer.
    Stateful,
    /// An order or amend whose notional at its price is below its instrument's `min_notional`,
    /// or whose size is above its `max_order_size`, is refused `Unencodable`.
    ChecksSize,
    /// An instrument cancel-all goes on a stream of that instrument's own, written as a
    /// cancel-all of everything on its connection: the same bytes for every instrument.
    PerInstrumentStreams,
    /// Every amend is refused `Unsupported` before anything else is read: its amends name an
    /// order by its placement nonce alone, which an amend never carries.
    AmendsUnnamed,
    /// A cancel, batch cancel, amend or query naming a venue order id that is not a UUID's
    /// 8-4-4-4-12 hex form is refused `Unencodable`, as QFEX's encoder refuses it (FBC-s8wq).
    UuidVenueIds,
}

/// What a broken toy's factory builds for order entry.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Builds {
    /// The toy's codec, twisted.
    Codec(Twist),
    /// No codec.
    Nothing,
    /// A refusal.
    Refusal,
}

/// The toy with its caps edited by `caps` (refused when `None`) and its order entry `builds`.
struct Broken {
    caps: Option<fn(&mut VenueCaps)>,
    builds: Builds,
}

impl Broken {
    /// The toy, its caps edited and its codec unchanged.
    const fn declaring(caps: fn(&mut VenueCaps)) -> Broken {
        Broken {
            caps: Some(caps),
            builds: Builds::Codec(Twist::None),
        }
    }

    /// The toy with its caps unchanged and its codec twisted.
    const fn twisted(twist: Twist) -> Broken {
        Broken {
            caps: Some(|_| {}),
            builds: Builds::Codec(twist),
        }
    }

    fn subject(&self) -> Subject<'_> {
        Subject::new(self, FIXTURES, assumed).unwrap()
    }

    fn caps_truthful(&self) -> Result<Verdict, Failure> {
        suite::caps_truthful(&self.subject())
    }

    fn commands_selfcontained(&self) -> Result<Verdict, Failure> {
        suite::commands_selfcontained(&self.subject())
    }
}

/// The toy's order caps, edited by `f`.
fn order(caps: &mut VenueCaps) -> &mut OrderCaps {
    &mut caps.exec.as_mut().unwrap().order
}

impl VenueFactory for Broken {
    fn id(&self) -> &'static str {
        "BROKEN-TOY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        ToyFactory.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let edit = self.caps.ok_or(ConfigError::Missing("broken.caps"))?;
        let mut caps = ToyFactory.caps(cfg)?;
        edit(&mut caps);
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
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        match self.builds {
            Builds::Codec(twist) => Some(Ok(Box::new(Twisted::new(twist)))),
            Builds::Nothing => None,
            Builds::Refusal => Some(Err(VenueError::Config(ConfigError::Missing("broken.key")))),
        }
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The toy's codec with a twist.
struct Twisted {
    inner: ToyExec,
    twist: Twist,
    placed: bool,
}

impl Twisted {
    fn new(twist: Twist) -> Twisted {
        Twisted {
            inner: ToyExec::new(Box::new(ToySigner)),
            twist,
            placed: false,
        }
    }
}

fn timer() -> Effect {
    Effect::Timer {
        tag: TimerTag(9),
        after: core::time::Duration::from_secs(1),
    }
}

/// The venue order ids `cmd` names its orders by.
fn venue_ids(cmd: &VenueCommand) -> impl Iterator<Item = &VenueOrderId> {
    let targets: Vec<&OrderRef> = match cmd {
        VenueCommand::Amend(a) => vec![&a.target],
        VenueCommand::Cancel(c) => vec![&c.target],
        VenueCommand::CancelMany(cs) => cs.iter().map(|c| &c.target).collect(),
        VenueCommand::Query(q) => vec![&q.target],
        _ => Vec::new(),
    };
    targets.into_iter().filter_map(OrderRef::venue)
}

/// Whether `id` is in a UUID's 8-4-4-4-12 form of hex digits, either case.
fn uuid_form(id: &str) -> bool {
    let groups: Vec<&str> = id.split('-').collect();
    let hex = |g: &&str| g.bytes().all(|b| b.is_ascii_hexdigit());
    groups.iter().map(|g| g.len()).eq([8, 4, 4, 4, 12]) && groups.iter().all(hex)
}

/// Whether an order of `qty` at `px` (none for a market order) is outside what `spec` takes:
/// above its largest order, or, priced, below its minimum notional.
fn unfit(spec: &InstrumentSpec, px: Option<Ticks>, qty: Lots) -> bool {
    let over = spec.max_order_size.is_some_and(|max| qty > max);
    let under = match (spec.min_notional, px) {
        (Some(min), Some(px)) => spec.notional(px, qty).is_none_or(|n| n.nanos < min.nanos),
        _ => false,
    };
    over || under
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
        let result = self.inner.encode(cmd, rpc, specs, ctx, t, fx);
        match (self.twist, result) {
            (Twist::Leaky, Err(why)) => {
                fx.push(timer());
                Err(why)
            }
            (Twist::Mislabelled, Err(NotSentReason::Unsupported)) => {
                Err(NotSentReason::Unencodable)
            }
            (Twist::RefusesPlacements, _) if matches!(cmd, VenueCommand::Place(_)) => {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::RefusesAll, _) => {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::RefusesArm, _) if matches!(cmd, VenueCommand::ArmCancelOnDisconnect(_)) => {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::RefusesDisarm, _) if *cmd == VenueCommand::ArmCancelOnDisconnect(false) => {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::UnlabelledPlacements, Ok(receipt)) if matches!(cmd, VenueCommand::Place(_)) => {
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
            (Twist::AccountWideRefusingOther, Ok(_))
                if *cmd == VenueCommand::CancelAll(CancelScope::Instrument(toy::INST_B)) =>
            {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::ChecksGrid, result) => {
                let priced = match cmd {
                    VenueCommand::Place(o) => vec![(o.inst, o.kind.limit_px())],
                    VenueCommand::PlaceBatch(os) => {
                        os.iter().map(|o| (o.inst, o.kind.limit_px())).collect()
                    }
                    VenueCommand::Amend(a) => vec![(a.inst, Some(a.px))],
                    _ => Vec::new(),
                };
                let off = |(inst, px): &(fbc_core::InstrumentId, Option<fbc_core::Ticks>)| {
                    px.is_some_and(|px| !specs.get(*inst).unwrap().price_grid.valid_at(px))
                };
                if priced.iter().any(off) {
                    fx.take();
                    return Err(NotSentReason::Unencodable);
                }
                result
            }
            (Twist::AccountWide | Twist::AccountWideRefusingOther, Ok(receipt))
                if matches!(cmd, VenueCommand::CancelAll(_)) =>
            {
                for effect in fx.take() {
                    fx.push(match effect {
                        Effect::Send {
                            stream,
                            rpc,
                            class,
                            charge,
                            ..
                        } => Effect::Send {
                            stream,
                            frame: fbc_core::WireSlice::plain(
                                format!("cancelall|rpc={}", rpc.unwrap().id.0).into_bytes(),
                            ),
                            rpc,
                            class,
                            charge,
                        },
                        other => other,
                    });
                }
                Ok(receipt)
            }
            (Twist::OverHttp, Ok(receipt)) if matches!(cmd, VenueCommand::CancelAll(_)) => {
                for effect in fx.take() {
                    let Effect::Send {
                        frame,
                        rpc,
                        class,
                        charge,
                        ..
                    } = effect
                    else {
                        unreachable!("the toy's cancel-all is one frame");
                    };
                    fx.push(Effect::Http {
                        tag: HttpTag(1),
                        req: fbc_core::HttpRequest {
                            method: fbc_core::HttpMethod::Delete,
                            url: fbc_core::WireUrl::plain("https://toy.invalid/orders"),
                            headers: Vec::new(),
                            body: frame,
                        },
                        rpc: rpc.map(|call| call.id),
                        timeout: core::time::Duration::from_secs(5),
                        class,
                        charge,
                    });
                }
                Ok(receipt)
            }
            (Twist::Unlabelled | Twist::Widened, Ok(receipt)) => {
                for effect in fx.take() {
                    fx.push(match effect {
                        Effect::Send {
                            stream,
                            frame,
                            rpc,
                            class,
                            mut charge,
                        } => {
                            let rpc = rpc.filter(|_| self.twist != Twist::Unlabelled);
                            charge.inst = charge.inst.filter(|_| self.twist != Twist::Widened);
                            Effect::Send {
                                stream,
                                frame,
                                rpc,
                                class,
                                charge,
                            }
                        }
                        other => other,
                    });
                }
                Ok(receipt)
            }
            (Twist::Stateful, Ok(receipt)) => {
                if matches!(cmd, VenueCommand::Place(_)) {
                    self.placed = true;
                } else if self.placed {
                    fx.push(timer());
                }
                Ok(receipt)
            }
            (Twist::ChecksSize, result) => {
                let sized = match cmd {
                    VenueCommand::Place(o) => vec![(o.inst, o.kind.limit_px(), o.qty)],
                    VenueCommand::PlaceBatch(os) => os
                        .iter()
                        .map(|o| (o.inst, o.kind.limit_px(), o.qty))
                        .collect(),
                    VenueCommand::Amend(a) => vec![(a.inst, Some(a.px), a.qty)],
                    _ => Vec::new(),
                };
                let unfit = |&(inst, px, qty): &(InstrumentId, Option<Ticks>, Lots)| {
                    unfit(specs.get(inst).unwrap(), px, qty)
                };
                if sized.iter().any(unfit) {
                    fx.take();
                    return Err(NotSentReason::Unencodable);
                }
                result
            }
            (Twist::PerInstrumentStreams, Ok(receipt)) => {
                let VenueCommand::CancelAll(CancelScope::Instrument(inst)) = cmd else {
                    return Ok(receipt);
                };
                let stream = StreamId(100 + u16::try_from(inst.get()).unwrap());
                for effect in fx.take() {
                    fx.push(match effect {
                        Effect::Send {
                            rpc, class, charge, ..
                        } => Effect::Send {
                            stream,
                            frame: fbc_core::WireSlice::plain(
                                format!("cancelall|rpc={}", rpc.unwrap().id.0).into_bytes(),
                            ),
                            rpc,
                            class,
                            charge,
                        },
                        other => other,
                    });
                }
                Ok(receipt)
            }
            (Twist::AmendsUnnamed, _) if matches!(cmd, VenueCommand::Amend(_)) => {
                fx.take();
                Err(NotSentReason::Unsupported)
            }
            (Twist::UuidVenueIds, _) if !venue_ids(cmd).all(|vid| uuid_form(vid.as_str())) => {
                fx.take();
                Err(NotSentReason::Unencodable)
            }
            (_, result) => result,
        }
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &fbc_core::DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.inner.on_frame(stream, f, scope, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<fbc_core::HttpResponse<'_>, HttpFailure>,
        scope: &fbc_core::DecodeScope<'_>,
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

/// The failure `outcome` must be, its breaches' capabilities listed.
fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    match outcome {
        Err(failure) => failure,
        Ok(verdict) => panic!("expected a failure, got {verdict:?}"),
    }
}

/// The breach naming `capability` in `failure`, by what it says.
fn said<'a>(failure: &'a Failure, capability: &str) -> &'a str {
    let breach = failure.breaches.iter().find(|b| b.capability == capability);
    let breach = breach.unwrap_or_else(|| panic!("{capability} not named in {failure}"));
    &breach.what
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// The deliberately broken toy: its caps declare post-only orders absent, and leave out the
/// pair that involves them, but its codec still sends them.
const POST_ONLY_SENT: Broken = Broken::declaring(|caps| {
    let o = order(caps);
    o.post_only = false;
    o.flag_conflicts = Vec::new();
});

#[test]
fn caps_truthful_fails_a_toy_that_sends_a_post_only_order_its_caps_declare_absent() {
    let failure = failed(POST_ONLY_SENT.caps_truthful());
    assert_eq!(failure.check, "caps_truthful");
    // Placed, as a batch item and as an amend: each is sent, and each breach names post-only.
    for capability in [
        "OrderCaps.post_only is false",
        "OrderCaps.post_only is false (a batch item)",
        "OrderCaps.post_only is false (an amend)",
    ] {
        let what = said(&failure, capability);
        assert!(
            what.starts_with("sent (1 effect(s))"),
            "{capability}: {what}"
        );
        assert!(
            what.contains("NotSent(Unsupported)"),
            "{capability}: {what}"
        );
    }
    assert_eq!(failure.breaches.len(), 3, "{failure}");
    assert!(failure.names("OrderCaps.post_only is false"));
    let shown = failure.to_string();
    assert!(shown.starts_with("caps_truthful failed:\n  OrderCaps.post_only is false: sent"));
    // The same toy's amends, cancels and queries are still self-contained.
    assert!(matches!(
        POST_ONLY_SENT.commands_selfcontained(),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
#[should_panic(expected = "OrderCaps.post_only is false: sent")]
fn the_suite_test_for_caps_truthful_panics_naming_the_broken_capability() {
    suite::run(suite::caps_truthful, &POST_ONLY_SENT, FIXTURES, assumed);
}

// ---------------------------------------------------------------------------------------------
// Every absence the caps can declare, against a codec that offers them all.
// ---------------------------------------------------------------------------------------------

#[test]
fn caps_truthful_names_every_absent_capability_the_toy_still_sends() {
    let broken = Broken::declaring(|caps| {
        let o = order(caps);
        o.tifs = TagSet::of(&[TifTag::Gtc]);
        o.reduce_only = false;
        o.post_only = false;
        o.flag_conflicts = vec![(Feature::PostOnly, Feature::ReduceOnly)];
        o.amend = None;
        o.batch_place = None;
        o.batch_cancel = None;
        o.cancel_refs = TagSet::of(&[RefKind::Client]);
        o.query_refs = TagSet::none();
        o.cancel_all_instrument = Support::Unsupported;
        o.cancel_on_disconnect = CancelOnDisconnect::None;
    });
    let failure = failed(broken.caps_truthful());
    for capability in [
        "OrderCaps.tifs lacks Ioc",
        "OrderCaps.reduce_only is false",
        "OrderCaps.post_only is false",
        "OrderCaps.flag_conflicts has (PostOnly, ReduceOnly)",
        "OrderCaps.amend is None",
        "OrderCaps.batch_place is None",
        "OrderCaps.batch_cancel is None",
        "OrderCaps.cancel_refs lacks [Venue, PlacementNonce]",
        "OrderCaps.query_refs is empty",
        "OrderCaps.cancel_all_instrument is Unsupported",
        "OrderCaps.cancel_on_disconnect has no protection (arm)",
        "OrderCaps.cancel_on_disconnect has no protection (disarm)",
    ] {
        assert!(said(&failure, capability).starts_with("sent"), "{failure}");
    }
    // What the toy really leaves out is still refused, and its controls are still sent: it has
    // no dead-man timer either (its protection is per connection).
    let refresh = "OrderCaps.cancel_on_disconnect has no dead-man timer (refresh)";
    assert!(!failure.names(refresh), "{failure}");
    for kept in [
        "OrderCaps.kinds lacks Market",
        "OrderCaps.channels lacks Rpi",
    ] {
        assert!(!failure.names(kept), "{failure}");
    }
    assert!(
        !failure
            .breaches
            .iter()
            .any(|b| b.capability.starts_with("control"))
    );
}

#[test]
fn caps_truthful_names_limits_and_references_narrower_than_the_toy_takes() {
    let broken = Broken::declaring(|caps| {
        let o = order(caps);
        o.batch_place = Some(Batch { max_items: 2 });
        let refs = TagSet::of(&[RefKind::Venue]);
        o.batch_cancel = Some(CancelBatch { max_items: 2, refs });
        o.amend.as_mut().unwrap().refs = TagSet::of(&[RefKind::Client]);
        o.query_refs = TagSet::of(&[RefKind::Venue]);
        o.cancel_on_disconnect = CancelOnDisconnect::PerConnection {
            rearm_on_reconnect: true,
            covers_open_orders: false,
            offers_disarm: true,
        };
        // No one order is both IOC and FOK: the pair is not probed. An RPI FOK order is
        // refused for its undeclared parts before its conflict.
        o.flag_conflicts.push((Feature::Ioc, Feature::Fok));
        o.flag_conflicts.push((Feature::Rpi, Feature::Fok));
    });
    let failure = failed(broken.caps_truthful());
    for capability in [
        "Batch.max_items is 2",
        "CancelBatch.max_items is 2",
        "CancelBatch.refs lacks [Client, PlacementNonce]",
        "AmendCaps.refs lacks [Venue]",
        "OrderCaps.query_refs lacks [Client, PlacementNonce]",
    ] {
        assert!(said(&failure, capability).starts_with("sent"), "{failure}");
    }
    assert!(!failure.names("OrderCaps.cancel_on_disconnect has no protection (arm)"));
    assert!(!failure.names("OrderCaps.cancel_on_disconnect has no dead-man timer (refresh)"));
    assert!(
        !failure
            .breaches
            .iter()
            .any(|b| b.capability.contains("(Ioc, Fok)"))
    );
    assert!(
        !failure.names("OrderCaps.flag_conflicts has (Rpi, Fok)"),
        "{failure}"
    );
}

#[test]
fn commands_selfcontained_names_a_reference_the_toy_declares_but_does_not_take() {
    // Every reference declared for batch cancels and queries: neither is ever named by its
    // placement nonce, and the toy's batch cancels and queries never take our id.
    let broken = Broken::declaring(|caps| {
        let o = order(caps);
        let all = TagSet::of(&[RefKind::Venue, RefKind::Client, RefKind::PlacementNonce]);
        o.batch_cancel.as_mut().unwrap().refs = all;
        o.query_refs = all;
    });
    let failure = failed(broken.commands_selfcontained());
    for capability in [
        "CancelBatch.refs has Client: a batch cancel",
        "OrderCaps.query_refs has Client: a query",
    ] {
        let what = said(&failure, capability);
        assert_eq!(
            what,
            "refused as NotSent(Unsupported) by a freshly built codec"
        );
    }
    assert_eq!(failure.breaches.len(), 2, "{failure}");
    assert!(
        !failure
            .breaches
            .iter()
            .any(|b| b.capability.contains("PlacementNonce"))
    );
}

#[test]
fn caps_truthful_probes_a_declaration_that_takes_no_order_or_reference_at_all() {
    // Nothing an order can be: the first order named is refused, and every absence is still
    // probed on it, in a placement, a batch and an amend (Codex r4188991858); no control is.
    let no_kinds = Broken::declaring(|caps| {
        order(caps).kinds = TagSet::none();
        order(caps).post_only = false;
    });
    let failure = failed(no_kinds.caps_truthful());
    for capability in [
        "OrderCaps allows no order: the first it names",
        "OrderCaps.post_only is false (a batch item)",
        "OrderCaps allows no limit order to amend",
        "OrderCaps.post_only is false (an amend)",
    ] {
        assert!(said(&failure, capability).starts_with("sent"), "{failure}");
    }
    let controls = [
        "control: a placement",
        "control: a batch placement",
        "control: an amend",
    ];
    assert!(!controls.iter().any(|c| failure.names(c)), "{failure}");
    // Where the toy refuses, for the probe's reason or another that applies, there is no
    // breach: a post-only IOC order of no declared kind may be refused as either.
    assert!(!failure.names("OrderCaps.tifs lacks Fok"), "{failure}");
    assert!(
        !failure.names("OrderCaps.flag_conflicts has (PostOnly, Ioc)"),
        "{failure}"
    );

    // Market orders only: nothing to amend, and the toy refuses the market control.
    let market_only = Broken::declaring(|caps| {
        order(caps).kinds = TagSet::of(&[OrderKindTag::Market]);
        order(caps).amend.as_mut().unwrap().refs = TagSet::of(&[RefKind::PlacementNonce]);
    });
    let failure = failed(market_only.caps_truthful());
    assert!(said(&failure, "control: a placement").starts_with("refused as NotSent(Unsupported)"));
    let amend = said(&failure, "OrderCaps allows no limit order to amend");
    assert!(amend.starts_with("sent"), "{failure}");
    assert!(!failure.names("control: an amend"), "{failure}");

    // An amend no reference names, empty reference sets, batches of no item.
    let unnamed = Broken::declaring(|caps| {
        let o = order(caps);
        o.amend.as_mut().unwrap().refs = TagSet::of(&[RefKind::PlacementNonce]);
        o.cancel_refs = TagSet::none();
        o.batch_cancel = Some(CancelBatch {
            max_items: 4,
            refs: TagSet::none(),
        });
        o.batch_place = Some(Batch { max_items: 0 });
    });
    let failure = failed(unnamed.caps_truthful());
    for capability in [
        "AmendCaps.refs lacks [Venue, Client]",
        "OrderCaps.cancel_refs is empty",
        "CancelBatch.refs is empty",
    ] {
        assert!(said(&failure, capability).starts_with("sent"), "{failure}");
    }
    assert!(!failure.names("control: an amend"), "{failure}");
    // A batch of no item: one item is past its limit, and nothing else is probed in it.
    assert!(
        said(&failure, "Batch.max_items is 0").starts_with("sent"),
        "{failure}"
    );
    let in_batch = |b: &suite::Breach| b.capability.contains("(a batch item)");
    assert!(!failure.breaches.iter().any(in_batch), "{failure}");

    let no_batch_items = Broken::declaring(|caps| {
        let o = order(caps);
        o.batch_place = Some(Batch { max_items: 1 });
        o.batch_cancel = Some(CancelBatch {
            max_items: 0,
            refs: TagSet::of(&[RefKind::Venue]),
        });
    });
    let failure = failed(no_batch_items.caps_truthful());
    for capability in ["Batch.max_items is 1", "CancelBatch.max_items is 0"] {
        assert!(said(&failure, capability).starts_with("sent"), "{failure}");
    }
    assert_eq!(failure.breaches.len(), 2, "{failure}");
    // A batch cancel of no item names no reference to encode.
    let probed = match no_batch_items.commands_selfcontained() {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("{other:?}"),
    };
    assert!(
        !probed.iter().any(|p| p.contains("batch cancel")),
        "{probed:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Codecs that keep the caps' word but not the contract around it.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_refusal_that_asks_for_an_effect_is_a_breach() {
    let failure = failed(Broken::twisted(Twist::Leaky).caps_truthful());
    let what = said(&failure, "OrderCaps.kinds lacks Market");
    assert_eq!(
        what,
        "refused as NotSent(Unsupported) but asked for 1 effect(s): a refusal asks for none"
    );
}

#[test]
fn a_refusal_with_the_wrong_reason_is_a_breach() {
    let failure = failed(Broken::twisted(Twist::Mislabelled).caps_truthful());
    let what = said(&failure, "OrderCaps.tifs lacks Fok");
    assert_eq!(
        what,
        "refused as NotSent(Unencodable), not NotSent(Unsupported)"
    );
    // The flag conflict keeps its own reason.
    assert!(!failure.names("OrderCaps.flag_conflicts has (PostOnly, Ioc)"));
}

#[test]
fn a_codec_that_refuses_everything_fails_every_control() {
    let failure = failed(Broken::twisted(Twist::RefusesAll).caps_truthful());
    for control in [
        "control: a placement",
        "control: a batch placement",
        "control: an amend",
        "control: a cancel",
        "control: a batch cancel",
        "control: a query",
        "control: protection (arm)",
        "control: protection (disarm)",
        "OrderCaps.cancel_all_instrument is Native: never widened",
    ] {
        assert!(
            said(&failure, control)
                .starts_with("refused as NotSent(Unsupported) though the caps declare it")
        );
    }
    // Its flag conflict is refused for the wrong reason, too.
    assert!(failure.names("OrderCaps.flag_conflicts has (PostOnly, Ioc)"));

    let failure = failed(Broken::twisted(Twist::RefusesAll).commands_selfcontained());
    let what = said(&failure, "AmendCaps.refs has Venue: an amend");
    assert_eq!(
        what,
        "refused as NotSent(Unsupported) by a freshly built codec"
    );
    assert_eq!(failure.breaches.len(), 7, "{failure}");
}

#[test]
fn a_request_sent_without_its_rpc_is_a_breach() {
    let failure = failed(Broken::twisted(Twist::Unlabelled).caps_truthful());
    let what = said(&failure, "control: a cancel");
    assert_eq!(
        what,
        "sent, but its effects do not carry request 1 as Safety traffic"
    );

    let failure = failed(Broken::twisted(Twist::Unlabelled).commands_selfcontained());
    let what = said(&failure, "OrderCaps.query_refs has Venue: a query");
    assert!(
        what.contains("its effects do not carry request 1 as Safety traffic"),
        "{what}"
    );
}

#[test]
fn an_instrument_cancel_all_charged_to_no_instrument_is_widened() {
    let failure = failed(Broken::twisted(Twist::Widened).caps_truthful());
    let what = said(
        &failure,
        "OrderCaps.cancel_all_instrument is Native: never widened",
    );
    assert_eq!(
        what,
        "sent charged as CancelAll on None, not as a cancel-all of instrument 7: widened beyond \
         its instrument"
    );
    // Only the cancel-all is held to its instrument.
    assert_eq!(failure.breaches.len(), 1, "{failure}");
}

#[test]
fn an_instrument_cancel_all_written_as_the_accounts_is_widened_whatever_its_charge() {
    // Codex r4188991848: charged to its instrument, but the request names none.
    let failure = failed(Broken::twisted(Twist::AccountWide).caps_truthful());
    let what = said(
        &failure,
        "OrderCaps.cancel_all_instrument is Native: never widened",
    );
    assert_eq!(
        what,
        "written exactly as the cancel-all of another instrument: it does not name instrument \
         7, so it cancels beyond it"
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");

    // With one instrument there is no second to compare it with: the setup must list two.
    let subject = Subject::new(&ToyFactory, FIXTURES, one_instrument).unwrap();
    let failure = failed(suite::caps_truthful(&subject));
    let what = said(
        &failure,
        "OrderCaps.cancel_all_instrument is Native: never widened",
    );
    assert!(what.ends_with("names its own: list two"), "{what}");
}

#[test]
fn an_instrument_cancel_all_over_http_is_compared_by_its_request() {
    // Each request names its instrument in its body, so neither is widened.
    assert!(matches!(
        Broken::twisted(Twist::OverHttp).caps_truthful(),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
fn every_reference_declared_and_the_longest_batches_leave_nothing_to_probe_past() {
    // Every reference an operation can take is declared, and no batch can be longer than its
    // limit: none of those absences is probed, and the toy's controls still pass.
    let broken = Broken::declaring(|caps| {
        let o = order(caps);
        let all = TagSet::of(&[RefKind::Venue, RefKind::Client, RefKind::PlacementNonce]);
        o.amend.as_mut().unwrap().refs = TagSet::of(&[RefKind::Venue, RefKind::Client]);
        o.query_refs = all;
        o.batch_cancel = Some(CancelBatch {
            max_items: u16::MAX,
            refs: all,
        });
        o.batch_place = Some(Batch {
            max_items: u16::MAX,
        });
    });
    let Ok(Verdict::Passed { probed, .. }) = broken.caps_truthful() else {
        panic!("the toy keeps every control");
    };
    let past = |p: &&String| p.contains("lacks [") || p.contains("max_items");
    assert!(!probed.iter().any(|p| past(&p)), "{probed:?}");
}

#[test]
fn a_cancel_all_compared_with_one_never_sent_proves_nothing() {
    // Codex r4189256916: the second instrument's cancel-all refused, the first's account-wide.
    let failure = failed(Broken::twisted(Twist::AccountWideRefusingOther).caps_truthful());
    let what = said(
        &failure,
        "OrderCaps.cancel_all_instrument is Native: never widened",
    );
    assert_eq!(
        what,
        "the cancel-all of instrument 8 was not sent as one, so nothing shows this one names \
         instrument 7"
    );
}

#[test]
fn orders_are_priced_on_the_instruments_grid() {
    // Codex r4189256906: a codec that refuses an off-grid price still passes both checks on a
    // banded grid where tick 100 is below the first band.
    let grid_checked = Broken::twisted(Twist::ChecksGrid);
    for check in [suite::caps_truthful, suite::commands_selfcontained] {
        let subject = Subject::new(&grid_checked, FIXTURES, banded).unwrap();
        assert!(matches!(check(&subject), Ok(Verdict::Passed { .. })));
    }
    // And on the toy's own grid, where every tick is valid.
    assert!(matches!(
        grid_checked.caps_truthful(),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
fn a_codec_that_encodes_differently_once_it_saw_the_placement_is_not_self_contained() {
    let failure = failed(Broken::twisted(Twist::Stateful).commands_selfcontained());
    let what = said(&failure, "OrderCaps.cancel_refs has Client: a cancel");
    assert!(what.starts_with("encoded differently by a codec that first saw the order placed"));
    assert_eq!(failure.breaches.len(), 7, "{failure}");
    // caps_truthful builds a codec per probe, so the twist never shows there.
    assert!(matches!(
        Broken::twisted(Twist::Stateful).caps_truthful(),
        Ok(Verdict::Passed { .. })
    ));
}

#[test]
fn operations_the_caps_declare_are_controls_the_codec_must_send() {
    // An account cancel-all and a dead-man timer declared, which the toy does not send: its
    // protection is per connection, with no timer to refresh.
    let declared = Broken::declaring(|caps| {
        let o = order(caps);
        o.cancel_all_account = Support::Native;
        o.cancel_on_disconnect = CancelOnDisconnect::DeadMan {
            max_ttl: std::time::Duration::from_secs(10),
            offers_disarm: true,
        };
    });
    let failure = failed(declared.caps_truthful());
    let refused = "refused as NotSent(Unsupported) though the caps declare it";
    let account = said(&failure, "control: an account cancel-all");
    assert!(account.starts_with(refused), "{failure}");
    let refresh = said(&failure, "control: dead-man timer (refresh)");
    assert!(refresh.starts_with(refused), "{failure}");
    // The toy arms and disarms its protection, and refuses nothing else declared.
    assert_eq!(failure.breaches.len(), 2, "{failure}");
}

#[test]
fn account_wide_cod_declaration_requires_an_arm_and_disarm_the_codec_sends() {
    let broken = Broken {
        caps: Some(|caps| {
            order(caps).cancel_on_disconnect = CancelOnDisconnect::AccountWide {
                offers_disarm: true,
            };
        }),
        builds: Builds::Codec(Twist::RefusesArm),
    };
    let failure = failed(broken.caps_truthful());
    let refused = "refused as NotSent(Unsupported) though the caps declare it";
    assert!(said(&failure, "control: protection (arm)").starts_with(refused));
    assert!(said(&failure, "control: protection (disarm)").starts_with(refused));
}

// ---------------------------------------------------------------------------------------------
// FBC-3ymj's done line: a disarm held to `offers_disarm` (decision 0141).
// ---------------------------------------------------------------------------------------------

/// The probe a disarm meets where the caps take an arm and offer no disarm.
const NO_DISARM: &str = "OrderCaps.cancel_on_disconnect offers no disarm";

/// The toy's per-connection protection, offering no disarm.
fn per_connection_without_disarm(caps: &mut VenueCaps) {
    order(caps).cancel_on_disconnect = CancelOnDisconnect::PerConnection {
        rearm_on_reconnect: true,
        covers_open_orders: false,
        offers_disarm: false,
    };
}

/// Account-wide protection offering no disarm, as the QFEX probe declares (decision 0132).
fn account_wide_without_disarm(caps: &mut VenueCaps) {
    order(caps).cancel_on_disconnect = CancelOnDisconnect::AccountWide {
        offers_disarm: false,
    };
}

#[test]
fn the_toy_declaring_a_disarm_sends_it_as_a_control_and_passes() {
    // The toy as it is: per-connection protection offering a disarm, which its codec writes.
    let caps = ToyFactory.caps(&VenueConfig::new()).unwrap();
    let cod = caps.exec.unwrap().order.cancel_on_disconnect;
    assert!(cod.offers_disarm(), "{cod:?}");
    let probed = probed(Broken::twisted(Twist::None).caps_truthful());
    for name in ["control: protection (arm)", "control: protection (disarm)"] {
        assert!(probed.iter().any(|p| p == name), "{name}: {probed:?}");
    }
    assert!(!probed.iter().any(|p| p == NO_DISARM), "{probed:?}");
}

#[test]
fn a_toy_declaring_no_disarm_yet_sending_one_fails_naming_offers_disarm() {
    for caps in [per_connection_without_disarm, account_wide_without_disarm] {
        let broken = Broken::declaring(caps);
        let failure = failed(broken.caps_truthful());
        assert_eq!(
            said(&failure, NO_DISARM),
            "sent (1 effect(s)) where the caps make it NotSent(Unsupported)",
            "{failure}"
        );
        // Only the disarm: the arm is still a control the codec sends.
        assert_eq!(failure.breaches.len(), 1, "{failure}");
        assert!(!failure.names("control: protection (arm)"), "{failure}");
    }
}

#[test]
fn a_toy_declaring_no_disarm_and_refusing_it_passes_its_arm_still_a_control() {
    for caps in [per_connection_without_disarm, account_wide_without_disarm] {
        let refusing = Broken {
            caps: Some(caps),
            builds: Builds::Codec(Twist::RefusesDisarm),
        };
        let probed = probed(refusing.caps_truthful());
        for name in ["control: protection (arm)", NO_DISARM] {
            assert!(probed.iter().any(|p| p == name), "{name}: {probed:?}");
        }
        assert!(
            !probed.iter().any(|p| p == "control: protection (disarm)"),
            "{probed:?}"
        );
    }
}

#[test]
fn a_toy_declaring_a_disarm_yet_refusing_it_fails_its_control() {
    let failure = failed(Broken::twisted(Twist::RefusesDisarm).caps_truthful());
    let what = said(&failure, "control: protection (disarm)");
    assert!(
        what.starts_with("refused as NotSent(Unsupported) though the caps declare it"),
        "{failure}"
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");
}

#[test]
fn a_dead_man_timer_offering_no_disarm_has_its_disarm_probed_as_an_absence() {
    // The toy refreshes no timer, so the refresh control fails too; the disarm the toy still
    // writes is named as the absence the caps declare, and the arm is a control it sends.
    let broken = Broken::declaring(|caps| {
        order(caps).cancel_on_disconnect = CancelOnDisconnect::DeadMan {
            max_ttl: std::time::Duration::from_secs(10),
            offers_disarm: false,
        };
    });
    let failure = failed(broken.caps_truthful());
    let sent = "sent (1 effect(s)) where the caps make it NotSent(Unsupported)";
    assert_eq!(said(&failure, NO_DISARM), sent, "{failure}");
    assert!(
        failure.names("control: dead-man timer (refresh)"),
        "{failure}"
    );
    assert!(!failure.names("control: protection (arm)"), "{failure}");
    assert_eq!(failure.breaches.len(), 2, "{failure}");
    // `None` takes no arm, so it offers no disarm either.
    assert!(!CancelOnDisconnect::None.offers_disarm());
}

#[test]
fn the_control_order_is_the_first_the_caps_allow_not_the_first_they_name() {
    // An IOC order conflicts with itself here, so the plainest allowed order is FOK, which
    // the toy does not take: its control fails, rather than the caps reading as allowing none.
    let ioc_refused = Broken::declaring(|caps| {
        let o = order(caps);
        o.tifs = TagSet::of(&[TifTag::Ioc, TifTag::Fok]);
        o.flag_conflicts = vec![(Feature::Ioc, Feature::Ioc)];
    });
    let failure = failed(ioc_refused.caps_truthful());
    let control = said(&failure, "control: a placement");
    assert!(control.starts_with("refused as NotSent(Unsupported) though the caps declare it"));
    assert!(!failure.names("OrderCaps allows no order: the first it names"));
    // The IOC order is probed as the conflict it is, and the toy sends it.
    let ioc = said(&failure, "OrderCaps.flag_conflicts has (Ioc, Ioc)");
    assert_eq!(
        ioc,
        "sent (1 effect(s)) where the caps make it NotSent(FlagConflict)"
    );
}

#[test]
fn commands_selfcontained_needs_its_warm_placement_sent_and_skips_amends_of_no_limit_order() {
    // A codec whose placements are refused never sees one, so nothing it encodes after is
    // compared, and each command says so (Codex r4188991867).
    let failure = failed(Broken::twisted(Twist::RefusesPlacements).commands_selfcontained());
    let what = said(&failure, "OrderCaps.cancel_refs has Client: a cancel");
    assert!(what.starts_with("not compared: the placement a codec was to see first was refused"));
    assert_eq!(failure.breaches.len(), 7, "{failure}");
    // So does one whose placements carry no request.
    let failure = failed(Broken::twisted(Twist::UnlabelledPlacements).commands_selfcontained());
    let what = said(&failure, "OrderCaps.query_refs has Venue: a query");
    assert!(
        what.ends_with("sent without its request, though the caps allow it"),
        "{what}"
    );

    // No order allowed: no warm codec and no amend, and the cancels and queries still encode.
    let no_orders = Broken::declaring(|caps| order(caps).kinds = TagSet::none());
    let Ok(Verdict::Passed { probed, .. }) = no_orders.commands_selfcontained() else {
        panic!("cancels and queries are self-contained");
    };
    assert!(
        !probed
            .iter()
            .any(|p| p.contains("amend") || p.contains("warm")),
        "{probed:?}"
    );
    assert_eq!(probed.len(), 6, "{probed:?}");
}

// ---------------------------------------------------------------------------------------------
// Setups, skips and the suite's own failures.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_venue_without_order_entry_passes_caps_truthful_only_when_it_builds_no_codec() {
    let md_only = Broken {
        caps: Some(|caps| caps.exec = None),
        builds: Builds::Nothing,
    };
    let Ok(Verdict::Passed { check, probed }) = md_only.caps_truthful() else {
        panic!("a market-data-only venue building no codec is truthful");
    };
    assert_eq!(check, "caps_truthful");
    assert_eq!(probed, ["VenueCaps.exec is None: no order-entry codec"]);
    let skipped = md_only.commands_selfcontained();
    let Ok(Verdict::Skipped { check, why }) = skipped else {
        panic!("{skipped:?}");
    };
    assert_eq!(check, "commands_selfcontained");
    assert!(why.contains("VenueCaps.exec is None"), "{why}");
    suite::expect(Ok(Verdict::Skipped { check, why }));

    let lying = Broken::declaring(|caps| caps.exec = None);
    let failure = failed(lying.caps_truthful());
    let what = said(&failure, "VenueCaps.exec");
    assert_eq!(
        what,
        "declares no order entry, yet exec_codec builds a codec"
    );
}

#[test]
fn a_venue_declaring_order_entry_must_build_a_codec() {
    let none = Broken {
        caps: Some(|_| {}),
        builds: Builds::Nothing,
    };
    for outcome in [none.caps_truthful(), none.commands_selfcontained()] {
        let failure = failed(outcome);
        let what = said(&failure, "VenueCaps.exec");
        assert_eq!(what, "declares order entry, yet exec_codec builds no codec");
    }

    let refused = Broken {
        caps: Some(|_| {}),
        builds: Builds::Refusal,
    };
    for outcome in [refused.caps_truthful(), refused.commands_selfcontained()] {
        let failure = failed(outcome);
        let what = said(&failure, "VenueFactory::exec_codec");
        assert_eq!(
            what,
            "refused the setup: venue configuration refused: missing configuration key broken.key"
        );
    }
}

#[test]
fn a_setup_the_factory_refuses_or_with_no_instrument_fails_every_check() {
    let refused = Broken {
        caps: None,
        builds: Builds::Codec(Twist::None),
    };
    for outcome in [refused.caps_truthful(), refused.commands_selfcontained()] {
        let failure = failed(outcome);
        let what = said(&failure, "VenueFactory::caps");
        assert_eq!(
            what,
            "refused the setup: missing configuration key broken.caps"
        );
    }

    let empty = Subject::new(&ToyFactory, FIXTURES, no_instruments).unwrap();
    for outcome in [
        suite::caps_truthful(&empty),
        suite::commands_selfcontained(&empty),
    ] {
        let failure = failed(outcome);
        assert_eq!(
            said(&failure, "setup"),
            "the spec table lists no instrument"
        );
    }
}

#[test]
fn commands_selfcontained_is_skipped_when_no_reference_names_an_order() {
    let unnamed = Broken::declaring(|caps| {
        let o = order(caps);
        o.amend = None;
        o.cancel_refs = TagSet::none();
        o.batch_cancel = None;
        o.query_refs = TagSet::none();
    });
    let Ok(Verdict::Skipped { why, .. }) = unnamed.commands_selfcontained() else {
        panic!("nothing to encode");
    };
    assert_eq!(
        why,
        "the caps declare no reference an amend, cancel or query can name an order by"
    );
}

#[test]
fn a_fixture_directory_that_is_not_one_is_refused() {
    let missing = concat!(env!("CARGO_MANIFEST_DIR"), "/no-such-fixtures");
    let failure = Subject::new(&ToyFactory, missing, assumed).unwrap_err();
    assert_eq!(failure.check, "suite");
    assert!(said(&failure, "fixture directory").ends_with("no-such-fixtures is not a directory"));
}

#[test]
fn a_subject_shows_its_factory_and_fixtures() {
    let subject = Subject::new(&ToyFactory, FIXTURES, assumed).unwrap();
    assert!(subject.fixtures().ends_with("fixtures/conformance-toy"));
    assert_eq!(subject.factory().id(), "TOY-CONFORMANCE");
    let shown = format!("{subject:?}");
    assert!(
        shown.starts_with("Subject { factory: \"TOY-CONFORMANCE\", fixtures:"),
        "{shown}"
    );
}

// ---------------------------------------------------------------------------------------------
// FBC-fuis: probe orders inside the instrument's size limits, cancel-alls compared with the
// stream they go to, and amends no declared reference can name.
// ---------------------------------------------------------------------------------------------

/// The toy's setup with its first instrument's orders at least 10 USDC of notional and at most
/// `max` lots: its least order, one lot (0.001) at the probe's tick 100 (50.0 USDC), is 0.05
/// USDC.
fn floored(max: i64) -> Setup {
    let mut specs = toy::specs();
    let mut a = specs.get(toy::INST_A).unwrap().clone();
    a.min_notional = Some(Money::new(10_000_000_000, a.quote_ccy));
    a.max_order_size = Some(Lots::new(max).unwrap());
    specs.insert(a);
    Setup { specs, ..assumed() }
}

/// An amend of the toy's first instrument to `qty` lots at tick `px`, naming it by the venue's
/// id.
fn amend_to(qty: i64, px: i64) -> VenueCommand {
    let vid = toy::with_scope(|scope| scope.venue_order_id("conformance-0")).unwrap();
    VenueCommand::Amend(AmendOrder {
        target: OrderRef::Venue(vid),
        inst: toy::INST_A,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
        cum_filled: Lots::ZERO,
    })
}

#[test]
fn both_checks_pass_a_codec_refusing_orders_outside_the_instruments_size_limits() {
    // Codex r4189398089: the instrument's least order at the probe's price is below its minimum
    // notional, and the codec refuses it, as it refuses one above the largest order.
    let size_checked = Broken::twisted(Twist::ChecksSize);
    let setups: [fn() -> Setup; 2] = [
        // 200 lots at tick 100 reach 10 USDC, within 500.
        || floored(500),
        // No order of at most 100 lots reaches it at tick 100; at tick 200 (100.0 USDC) 100 do.
        || floored(100),
    ];
    for setup in setups {
        let subject = Subject::new(&size_checked, FIXTURES, setup).unwrap();
        for check in [suite::caps_truthful, suite::commands_selfcontained] {
            let outcome = check(&subject);
            assert!(matches!(outcome, Ok(Verdict::Passed { .. })), "{outcome:?}");
        }
    }
    // The codec does refuse what the limits refuse, and sends what they take.
    let specs = floored(100).specs;
    let encode = |cmd: &VenueCommand| {
        let ctx = EncodeCtx {
            wall: WallNs(1),
            mono: MonoNs(1),
            nonces: NonceBlock::new(vec![1]),
        };
        let mut codec = Twisted::new(Twist::ChecksSize);
        let (stamps, mut fx) = (&mut PathStamps::off(), Effects::new());
        let result = codec.encode(cmd, RpcId(1), &specs, &ctx, stamps, &mut fx);
        (result.err(), fx.len())
    };
    let refused = (Some(NotSentReason::Unencodable), 0);
    // One lot at tick 100, 0.05 USDC; 99 lots at tick 200, 9.90 USDC; 101 lots, past 100.
    for (qty, px) in [(1, 100), (99, 200), (101, 200)] {
        assert_eq!(
            encode(&amend_to(qty, px)),
            refused,
            "{qty} lots at tick {px}"
        );
    }
    assert_eq!(encode(&amend_to(100, 200)), (None, 1));
}

#[test]
fn a_setup_whose_instrument_takes_no_order_is_refused_naming_its_spec_field() {
    /// Its least order, ten lots, is more than its largest, five.
    fn inverted() -> Setup {
        let mut specs = toy::specs();
        let mut a = specs.get(toy::INST_A).unwrap().clone();
        a.min_size = Lots::new(10).unwrap();
        a.max_order_size = Some(Lots::new(5).unwrap());
        specs.insert(a);
        Setup { specs, ..assumed() }
    }
    /// No order reaches its minimum notional at any price.
    fn unreachable() -> Setup {
        let mut specs = toy::specs();
        let mut a = specs.get(toy::INST_A).unwrap().clone();
        a.min_notional = Some(Money::new(i128::MAX, a.quote_ccy));
        specs.insert(a);
        Setup { specs, ..assumed() }
    }
    /// No order of at most 500 lots reaches it.
    fn bounded() -> Setup {
        let mut setup = unreachable();
        let mut a = setup.specs.get(toy::INST_A).unwrap().clone();
        a.max_order_size = Some(Lots::new(500).unwrap());
        setup.specs.insert(a);
        setup
    }
    /// A setup, the spec field its refusal names and what it says.
    type Case = (fn() -> Setup, &'static str, &'static str);
    let cases: [Case; 3] = [
        (
            inverted,
            "InstrumentSpec.max_order_size",
            "TOYA-PERP takes orders of at most 5 lots, fewer than its least, 10 lots (its \
             min_size, and one lot at least): no order fits to probe with",
        ),
        (
            unreachable,
            "InstrumentSpec.min_notional",
            "no order of TOYA-PERP reaches its min_notional at a valid price: no order fits to \
             probe with",
        ),
        (
            bounded,
            "InstrumentSpec.min_notional",
            "no order of TOYA-PERP within its max_order_size of 500 lots reaches its \
             min_notional at a valid price: no order fits to probe with",
        ),
    ];
    for (setup, field, what) in cases {
        let subject = Subject::new(&ToyFactory, FIXTURES, setup).unwrap();
        for outcome in [
            suite::caps_truthful(&subject),
            suite::commands_selfcontained(&subject),
            suite::encode_deterministic(&subject),
            suite::amend_ack(&subject),
        ] {
            let failure = failed(outcome);
            assert_eq!(said(&failure, field), what);
            assert_eq!(failure.breaches.len(), 1, "{failure}");
        }
    }
    // A venue taking no orders sizes none, whatever its instrument's limits.
    let md_only = Broken {
        caps: Some(|caps| caps.exec = None),
        builds: Builds::Nothing,
    };
    let subject = Subject::new(&md_only, FIXTURES, inverted).unwrap();
    let outcome = suite::caps_truthful(&subject);
    assert!(matches!(outcome, Ok(Verdict::Passed { .. })), "{outcome:?}");
}

#[test]
fn identical_cancel_alls_each_on_its_instruments_own_stream_are_not_widened() {
    // Codex r4189398084: where each instrument's orders go on a connection of its own, a
    // cancel-all of what its connection carries is that instrument's alone, though every
    // instrument's is written alike.
    let outcome = Broken::twisted(Twist::PerInstrumentStreams).caps_truthful();
    assert!(matches!(outcome, Ok(Verdict::Passed { .. })), "{outcome:?}");
    // The same bytes on the same stream still cancel beyond the instrument.
    let failure = failed(Broken::twisted(Twist::AccountWide).caps_truthful());
    let what = said(
        &failure,
        "OrderCaps.cancel_all_instrument is Native: never widened",
    );
    assert!(what.starts_with("written exactly as the cancel-all of another"));
}

/// The toy declaring that its amends name an order by its placement nonce alone, which no amend
/// carries, and its codec refusing every amend for that first.
const AMENDS_BY_NONCE: Broken = Broken {
    caps: Some(|caps| {
        let amend = order(caps).amend.as_mut().unwrap();
        amend.refs = TagSet::of(&[RefKind::PlacementNonce]);
    }),
    builds: Builds::Codec(Twist::AmendsUnnamed),
};

/// The same, its amends declared to name an order by nothing.
const AMENDS_BY_NOTHING: Broken = Broken {
    caps: Some(|caps| order(caps).amend.as_mut().unwrap().refs = TagSet::none()),
    builds: Builds::Codec(Twist::AmendsUnnamed),
};

#[test]
fn an_amend_no_declared_reference_can_name_may_be_refused_unsupported_before_its_flags() {
    // Codex r4189398073: a codec may read the amend's reference before its flags, and refuse
    // the flag-conflict amend as Unsupported where no reference it carries is declared.
    let conflict = "OrderCaps.flag_conflicts has (PostOnly, Ioc) (an amend)";
    for broken in [AMENDS_BY_NONCE, AMENDS_BY_NOTHING] {
        let probed = match broken.caps_truthful() {
            Ok(Verdict::Passed { probed, .. }) => probed,
            other => panic!("{other:?}"),
        };
        for name in [conflict, "AmendCaps.refs lacks [Venue, Client]"] {
            assert!(probed.iter().any(|p| p == name), "{name}: {probed:#?}");
        }
    }
    // Where the caps declare a reference an amend carries, its flag conflict is still refused
    // for the conflict.
    let failure = failed(Broken::twisted(Twist::AmendsUnnamed).caps_truthful());
    assert_eq!(
        said(&failure, conflict),
        "refused as NotSent(Unsupported), not NotSent(FlagConflict)"
    );
    let control = said(&failure, "control: an amend");
    assert!(control.starts_with("refused as NotSent(Unsupported) though the caps declare it"));
    // No limit order allowed, every one a conflict: the base amend may be refused for its
    // reference too.
    let conflicted = Broken {
        caps: Some(|caps| {
            let o = order(caps);
            o.amend.as_mut().unwrap().refs = TagSet::of(&[RefKind::PlacementNonce]);
            o.tifs = TagSet::of(&[TifTag::Ioc]);
            o.flag_conflicts = vec![(Feature::Ioc, Feature::Ioc)];
        }),
        builds: Builds::Codec(Twist::AmendsUnnamed),
    };
    let failure = failed(conflicted.caps_truthful());
    for amend in [
        "OrderCaps allows no limit order to amend",
        "OrderCaps.flag_conflicts has (Ioc, Ioc) (an amend)",
    ] {
        assert!(!failure.names(amend), "{failure}");
    }
    // The toy's codec sends the IOC placements its caps now refuse.
    assert!(failure.names("OrderCaps allows no order: the first it names"));
}

// ---------------------------------------------------------------------------------------------
// FBC-7y5e: price_grid's orders sized inside each instrument's limits, price by price.
// ---------------------------------------------------------------------------------------------

/// `price_grid` run on `broken` under `setup`.
fn price_grid(broken: &Broken, setup: fn() -> Setup) -> Result<Verdict, Failure> {
    suite::price_grid(&Subject::new(broken, FIXTURES, setup).unwrap())
}

/// What a passing `outcome` probed.
fn probed(outcome: Result<Verdict, Failure>) -> Vec<String> {
    match outcome {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("expected a pass, got {other:?}"),
    }
}

/// What a size-checking codec does with `cmd` under `specs`: why it refused it, and how many
/// effects it left.
fn size_checked(specs: &SpecTable, cmd: &VenueCommand) -> (Option<NotSentReason>, usize) {
    let ctx = EncodeCtx {
        wall: WallNs(1),
        mono: MonoNs(1),
        nonces: NonceBlock::new(vec![1]),
    };
    let mut codec = Twisted::new(Twist::ChecksSize);
    let (stamps, mut fx) = (&mut PathStamps::off(), Effects::new());
    let result = codec.encode(cmd, RpcId(1), specs, &ctx, stamps, &mut fx);
    (result.err(), fx.len())
}

#[test]
fn price_grid_passes_a_codec_refusing_orders_outside_the_instruments_size_limits() {
    // The lowest price price_grid probes on the toy's grid is tick 9 (4.5 USDC), where one lot
    // (0.001) is 0.0045 USDC against the 10 USDC minimum: 2223 lots reach it there, 2000 at
    // tick 10, 203 at tick 99 and one from tick 20000. Within 2223 lots each price takes a size.
    let checks_size = Broken::twisted(Twist::ChecksSize);
    let probed = probed(price_grid(&checks_size, || floored(2223)));
    assert_eq!(
        probed,
        [
            "TOYA-PERP: 45 model prices quantized to 54 order prices, 54 post-only orders sent",
            "TOYB-PERP: 45 model prices quantized to 54 order prices, 54 post-only orders sent",
        ]
    );
    // The codec refuses what the limits refuse at tick 9, and sends what they take.
    let specs = floored(2223).specs;
    let refused = (Some(NotSentReason::Unencodable), 0);
    // One lot and 2222 lots at tick 9, under 10 USDC; 2224 lots at tick 10, past 2223.
    for (qty, px) in [(1, 9), (2222, 9), (2224, 10)] {
        let cmd = amend_to(qty, px);
        assert_eq!(
            size_checked(&specs, &cmd),
            refused,
            "{qty} lots at tick {px}"
        );
    }
    assert_eq!(size_checked(&specs, &amend_to(2223, 9)), (None, 1));
}

#[test]
fn price_grid_refuses_a_setup_where_no_size_within_its_largest_order_reaches_the_minimum() {
    /// A setup and what its refusal says.
    type Case = (fn() -> Setup, &'static str);
    let cases: [Case; 2] = [
        // 2222 lots at tick 9 are 9.999 USDC; at tick 10, 2000 lots reach 10 USDC.
        (
            || floored(2222),
            "no order of TOYA-PERP within its max_order_size of 2222 lots reaches its \
             min_notional at tick 9, a price its grid's boundaries quantize to: no order fits to \
             probe it with",
        ),
        // 500 lots reach it from tick 40 (20.0 USDC) up: not around tick 10.
        (
            || floored(500),
            "no order of TOYA-PERP within its max_order_size of 500 lots reaches its \
             min_notional at ticks 9, 10 and 11, prices its grid's boundaries quantize to: no \
             order fits to probe them with",
        ),
    ];
    for (setup, what) in cases {
        // Whether the codec checks sizes or not, the refusal is the one breach.
        for twist in [Twist::ChecksSize, Twist::None] {
            let failure = failed(price_grid(&Broken::twisted(twist), setup));
            assert_eq!(
                said(&failure, "InstrumentSpec.min_notional of TOYA-PERP"),
                what
            );
            assert_eq!(failure.breaches.len(), 1, "{failure}");
        }
        // Nothing is sent on the refused instrument: a codec refusing every placement breaches
        // for each order sent, all 54 on the other instrument and none on it.
        let failure = failed(price_grid(
            &Broken::twisted(Twist::RefusesPlacements),
            setup,
        ));
        assert_eq!(placed_on(&failure, "TOYA-PERP"), 0, "{failure}");
        assert_eq!(placed_on(&failure, "TOYB-PERP"), 54, "{failure}");
        assert_eq!(failure.breaches.len(), 1 + 54, "{failure}");
    }
    // Where the setup takes a size at every price, that codec breaches for each order sent on
    // the first instrument too.
    let failure = failed(price_grid(
        &Broken::twisted(Twist::RefusesPlacements),
        || floored(2223),
    ));
    assert_eq!(placed_on(&failure, "TOYA-PERP"), 54, "{failure}");
}

/// How many of `failure`'s breaches name a post-only order sent on `symbol`.
fn placed_on(failure: &Failure, symbol: &str) -> usize {
    let on = format!(" on {symbol} at tick ");
    failure
        .breaches
        .iter()
        .filter(|b| b.capability.starts_with("a post-only ") && b.capability.contains(&on))
        .count()
}

/// The toy's setup with its first instrument on a banded grid whose first band starts at zero
/// (steps of 0.5 up to 100.0, of 1.0 from there), its orders at least 10 USDC of notional and at
/// most 20000 lots: the bid at the model price 0.25 floors to a price of zero, where no size
/// reaches the minimum, and 20000 lots (0.001 each) reach it at 0.5, tick 1.
fn from_zero() -> Setup {
    let mut specs = toy::specs();
    let mut a = specs.get(toy::INST_A).unwrap().clone();
    let bands = [
        (
            rust_decimal::Decimal::ZERO,
            rust_decimal::Decimal::new(5, 1),
        ),
        (
            rust_decimal::Decimal::ONE_HUNDRED,
            rust_decimal::Decimal::ONE,
        ),
    ];
    a.price_grid = fbc_core::PriceGrid::banded(&bands).unwrap();
    a.min_notional = Some(Money::new(10_000_000_000, a.quote_ccy));
    a.max_order_size = Some(Lots::new(20_000).unwrap());
    specs.insert(a);
    Setup { specs, ..assumed() }
}

#[test]
fn price_grid_sends_no_bid_at_zero_on_a_banded_grid_starting_there() {
    // The bid at 0.25 is judged, at tick 0, and not sent: sizing it would refuse the setup, as
    // no order at a price of zero reaches a minimum notional.
    for twist in [Twist::ChecksSize, Twist::None] {
        let probed = probed(price_grid(&Broken::twisted(twist), from_zero));
        assert_eq!(
            probed[0],
            "TOYA-PERP: 52 model prices quantized to 47 order prices, 47 post-only orders sent",
            "{twist:?}"
        );
    }
    // The bid at 0.5, tick 1, is sent, at the 20000 lots that reach 10 USDC there.
    let failure = failed(price_grid(
        &Broken::twisted(Twist::RefusesPlacements),
        from_zero,
    ));
    let named = |capability: &str| failure.breaches.iter().any(|b| b.capability == capability);
    assert!(named("a post-only bid on TOYA-PERP at tick 1"), "{failure}");
    assert!(
        !named("a post-only bid on TOYA-PERP at tick 0"),
        "{failure}"
    );
    let specs = from_zero().specs;
    let refused = (Some(NotSentReason::Unencodable), 0);
    assert_eq!(size_checked(&specs, &amend_to(19_999, 1)), refused);
    assert_eq!(size_checked(&specs, &amend_to(20_000, 1)), (None, 1));
}

#[test]
fn price_grid_names_the_field_no_order_fits_and_sizes_nothing_where_it_sends_nothing() {
    /// Its least order, ten lots, is more than its largest, five.
    fn inverted() -> Setup {
        let mut specs = toy::specs();
        let mut a = specs.get(toy::INST_A).unwrap().clone();
        a.min_size = Lots::new(10).unwrap();
        a.max_order_size = Some(Lots::new(5).unwrap());
        specs.insert(a);
        Setup { specs, ..assumed() }
    }
    /// No order reaches its minimum notional at any price.
    fn unreachable() -> Setup {
        let mut specs = toy::specs();
        let mut a = specs.get(toy::INST_A).unwrap().clone();
        a.min_notional = Some(Money::new(i128::MAX, a.quote_ccy));
        specs.insert(a);
        Setup { specs, ..assumed() }
    }
    let toy = Broken::twisted(Twist::None);
    let failure = failed(price_grid(&toy, inverted));
    assert_eq!(
        said(&failure, "InstrumentSpec.max_order_size of TOYA-PERP"),
        "TOYA-PERP takes orders of at most 5 lots, fewer than its least, 10 lots (its min_size, \
         and one lot at least): no order fits to probe with"
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");
    // Every price, with no largest order to name.
    let failure = failed(price_grid(&toy, unreachable));
    let what = said(&failure, "InstrumentSpec.min_notional of TOYA-PERP");
    assert!(
        what.starts_with(
            "no order of TOYA-PERP reaches its min_notional at ticks 9, 10, 11, 99, 100, 101, \
             999,"
        ),
        "{what}"
    );
    assert!(
        what.ends_with(
            " 1000000000 and 1000000001, prices its grid's boundaries quantize to: no order fits \
             to probe them with"
        ),
        "{what}"
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");
    // A venue that sends no post-only order sizes none, whatever its instrument's limits.
    let no_post_only = Broken::declaring(|caps| order(caps).post_only = false);
    let md_only = Broken {
        caps: Some(|caps| caps.exec = None),
        builds: Builds::Nothing,
    };
    for broken in [no_post_only, md_only] {
        for setup in [inverted, unreachable] {
            let probed = probed(price_grid(&broken, setup));
            assert!(probed[0].ends_with("no post-only limit order the caps allow to send"));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The broken toy is the toy but for what it breaks.
// ---------------------------------------------------------------------------------------------

/// A sink that drops what it is handed.
struct Drop;

impl ExecSink for Drop {
    fn push(&mut self, _meta: VenueMeta, _event: ExecEvent) {}
}

#[test]
fn the_broken_toy_otherwise_delegates_to_the_toy() {
    let b = Broken::twisted(Twist::None);
    let cfg = VenueConfig::new();
    assert_eq!(b.config_schema(), ToyFactory.config_schema());
    assert_eq!(
        b.parse_fbc_common_symbol("BTC/USD"),
        Err(SymbolError::Unmapped)
    );
    assert!(matches!(b.discover(&cfg), Err(VenueError::NoDiscovery)));
    assert!(
        matches!(b.plan_md(&cfg, &toy::specs(), &BTreeSet::new()), Ok(plans) if plans.is_empty())
    );
    let ep = EndpointPlan {
        stream: StreamId(3),
        transport: fbc_core::MdTransport::Socket {
            url: fbc_core::WireUrl::plain("ws://127.0.0.1/md"),
        },
        subs: Vec::new(),
    };
    let toys = ToyFactory.md_codec(&cfg, &ep).keepalive();
    assert_eq!(b.md_codec(&cfg, &ep).keepalive(), toys);
    assert!(toys.is_some());
    assert!(b.plan_exec(&cfg).is_err());
    assert!(b.test_connection(&cfg, Secrets::new()).is_none());

    let mut codec = Twisted::new(Twist::None);
    let ctx = EncodeCtx {
        wall: WallNs(1),
        mono: MonoNs(1),
        nonces: NonceBlock::new(Vec::new()),
    };
    let mut fx = Effects::new();
    assert_eq!(codec.nonces_for(CtxCall::Resync), 0);
    codec.on_open(toy::EXEC_STREAM, &ctx, &mut fx);
    codec.on_timer(TimerTag(1), &ctx, &mut fx);
    codec.resync(&ctx, &mut fx);
    assert!(!fx.is_empty());
    codec.on_rpc_timeout(RpcId(5), &mut Drop);
    let frame = RawFrame::Text("nonsense");
    assert_eq!(
        codec.redact_inbound(Inbound::Frame(frame)),
        InboundSpans::NONE
    );
    toy::with_scope(|scope| {
        let specs = toy::specs();
        assert!(
            codec
                .on_frame(toy::EXEC_STREAM, frame, scope, &specs, &mut Drop, &mut fx)
                .is_err()
        );
        let http = codec.on_http(
            HttpTag(1),
            Err(HttpFailure::NotSent),
            scope,
            &specs,
            &mut Drop,
            &mut fx,
        );
        assert!(http.is_err());
    });
}

// ---------------------------------------------------------------------------------------------
// FBC-6rfe: venue order ids in the shape the setup gives.
// ---------------------------------------------------------------------------------------------

/// The toy's setup with its venue order ids written as UUIDs.
fn uuid_ids() -> Setup {
    Setup {
        venue_order_ids: suite::uuid_venue_order_id,
        ..assumed()
    }
}

/// The toy taking only UUIDs for venue order ids.
const UUID_ONLY: Broken = Broken::twisted(Twist::UuidVenueIds);

/// The commands commands_selfcontained names an order by the venue's id with, on the toy's caps.
const BY_VENUE_ID: [&str; 4] = [
    "AmendCaps.refs has Venue: an amend",
    "OrderCaps.cancel_refs has Venue: a cancel",
    "CancelBatch.refs has Venue: a batch cancel",
    "OrderCaps.query_refs has Venue: a query",
];

#[test]
fn both_checks_pass_a_codec_taking_only_uuid_venue_ids_on_a_setup_giving_uuids() {
    let subject = Subject::new(&UUID_ONLY, FIXTURES, uuid_ids).unwrap();
    for check in [
        suite::caps_truthful,
        suite::commands_selfcontained,
        suite::encode_deterministic,
    ] {
        let outcome = check(&subject);
        assert!(matches!(outcome, Ok(Verdict::Passed { .. })), "{outcome:?}");
        // Every command naming an order by the venue's id was probed, and sent.
        if let Ok(Verdict::Passed {
            check: "commands_selfcontained",
            probed,
        }) = outcome
        {
            for label in BY_VENUE_ID {
                assert!(probed.iter().any(|p| p == label), "{label}: {probed:#?}");
            }
        }
    }
}

#[test]
fn both_checks_fail_a_codec_taking_only_uuid_venue_ids_on_the_toys_own_ids() {
    // The suite's ids are the setup's: the toy's, which this codec refuses.
    let failure = failed(UUID_ONLY.commands_selfcontained());
    for label in BY_VENUE_ID {
        assert_eq!(
            said(&failure, label),
            "refused as NotSent(Unencodable) by a freshly built codec"
        );
    }
    assert_eq!(failure.breaches.len(), BY_VENUE_ID.len(), "{failure}");
    let failure = failed(UUID_ONLY.caps_truthful());
    for control in [
        "control: an amend",
        "control: a cancel",
        "control: a batch cancel",
        "control: a query",
    ] {
        let what = said(&failure, control);
        assert!(
            what.starts_with("refused as NotSent(Unencodable) though the caps declare it"),
            "{control}: {what}"
        );
    }
}

#[test]
fn the_uuid_only_codec_refuses_every_other_venue_id_and_sends_a_uuid() {
    let specs = toy::specs();
    let encode = |cmd: &VenueCommand| {
        let ctx = EncodeCtx {
            wall: WallNs(1),
            mono: MonoNs(1),
            nonces: NonceBlock::new(vec![1, 2]),
        };
        let mut codec = Twisted::new(Twist::UuidVenueIds);
        let (stamps, mut fx) = (&mut PathStamps::off(), Effects::new());
        let result = codec.encode(cmd, RpcId(1), &specs, &ctx, stamps, &mut fx);
        (result.err(), fx.len())
    };
    let vid = |wire: &str| toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap();
    let amend_by = |vid: VenueOrderId| {
        let mut amend = amend_to(1, 100);
        if let VenueCommand::Amend(a) = &mut amend {
            a.target = OrderRef::Venue(vid);
        }
        amend
    };
    let cancel = |wire: &str| fbc_core::CancelOrder {
        target: OrderRef::Venue(vid(wire)),
        inst: toy::INST_A,
        side: Side::Buy,
        placement_nonce: None,
    };
    let commands = |wire: &str| {
        [
            VenueCommand::Cancel(cancel(wire)),
            VenueCommand::CancelMany(vec![cancel(wire)]),
            VenueCommand::Query(fbc_core::QueryOrder {
                target: OrderRef::Venue(vid(wire)),
                inst: toy::INST_A,
                placement_nonce: None,
            }),
            amend_by(vid(wire)),
        ]
    };
    let refused = (Some(NotSentReason::Unencodable), 0);
    for wire in [
        "toy-0",
        "1759500000000000001",
        // One hex digit short, one too many, a group out of place, and a digit not hex.
        "00000000-0000-4000-8000-00000000000",
        "00000000-0000-4000-8000-0000000000000",
        "000000000-000-4000-8000-000000000000",
        "00000000-0000-4000-8000-00000000000g",
    ] {
        for cmd in commands(wire) {
            assert_eq!(encode(&cmd), refused, "{wire}: {cmd:?}");
        }
    }
    for wire in [
        "00000000-0000-4000-8000-000000000000",
        "88888888-4444-4444-4444-1234567890AB",
    ] {
        for cmd in commands(wire) {
            assert_eq!(encode(&cmd), (None, 1), "{wire}: {cmd:?}");
        }
    }
}

#[test]
fn the_suites_uuids_are_one_per_order_in_the_uuid_form() {
    let cases: [(usize, &str); 4] = [
        (0, "00000000-0000-4000-8000-000000000000"),
        (10, "00000000-0000-4000-8000-00000000000a"),
        ((1 << 48) + 1, "00000001-0000-4000-8000-000000000001"),
        (usize::MAX, "0000ffff-0000-4000-8000-ffffffffffff"),
    ];
    for (i, want) in cases {
        assert_eq!(suite::uuid_venue_order_id(i), want);
        assert!(uuid_form(want), "{want}");
    }
    let some: BTreeSet<String> = (0..1000).map(suite::uuid_venue_order_id).collect();
    assert_eq!(some.len(), 1000);
}

#[test]
fn a_setup_whose_venue_ids_the_suite_cannot_use_is_refused_naming_its_field() {
    fn empty() -> Setup {
        Setup {
            venue_order_ids: |_| String::new(),
            ..assumed()
        }
    }
    fn too_long() -> Setup {
        Setup {
            venue_order_ids: |i| format!("{i:0>97}"),
            ..assumed()
        }
    }
    fn shared() -> Setup {
        Setup {
            venue_order_ids: |i| format!("toy-{}", i.min(1)),
            ..assumed()
        }
    }
    /// A check.
    type Check = fn(&Subject<'_>) -> Result<Verdict, Failure>;
    /// A setup and what its refusal says.
    type Case = (fn() -> Setup, &'static str);
    let field = "Setup.venue_order_ids";
    // The checks that name an order by the venue's id.
    let checks: [Check; 3] = [
        suite::caps_truthful,
        suite::commands_selfcontained,
        suite::encode_deterministic,
    ];
    let cases: [Case; 2] = [
        (
            empty,
            "order 0's id is not a venue order id: the venue id is empty",
        ),
        (
            too_long,
            "order 0's id is not a venue order id: the venue id is 97 bytes long; at most 96 \
             are accepted",
        ),
    ];
    for (setup, what) in cases {
        let subject = Subject::new(&ToyFactory, FIXTURES, setup).unwrap();
        for check in checks {
            let failure = failed(check(&subject));
            assert_eq!(said(&failure, field), what);
            assert_eq!(failure.breaches.len(), 1, "{failure}");
        }
    }
    // price_grid only places, under our client id: it reads no venue id, so none of these
    // setups fails it, and it still sends its post-only orders.
    for setup in [empty, too_long, shared] {
        let subject = Subject::new(&ToyFactory, FIXTURES, setup).unwrap();
        match suite::price_grid(&subject) {
            Ok(Verdict::Passed { probed, .. }) => assert!(
                probed.iter().all(|p| p.ends_with("post-only orders sent")),
                "{probed:?}"
            ),
            other => panic!("price_grid read the setup's venue ids: {other:?}"),
        }
    }
    // Orders 1 and 2 share an id: the checks naming one order alone use order 0's.
    let subject = Subject::new(&ToyFactory, FIXTURES, shared).unwrap();
    let failure = failed(suite::caps_truthful(&subject));
    assert_eq!(
        said(&failure, field),
        r#"gives orders 1 and 2 the one id "toy-1""#
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");
    for check in &checks[1..] {
        let outcome = check(&subject);
        assert!(matches!(outcome, Ok(Verdict::Passed { .. })), "{outcome:?}");
    }
}
