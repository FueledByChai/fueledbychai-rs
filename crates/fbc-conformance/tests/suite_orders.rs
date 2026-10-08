//! FBC-3il's done line, its second half: `mixed_batch` run against a toy variant that reports
//! every item of a timed-out batch `Accepted` fails, naming the timeout (BT-502). Around it, every
//! other way the three order-entry checks can fail: that variant's unanswered placement reported
//! `Accepted`, a request written again after its deadline, an amend whose reply alone is declared
//! to report it (`RpcReplyOnly`) with no update synthesized, an amended order's new venue id
//! missing, a placement refused or answered wrongly, a setup stating no replies, an opening that
//! never readies the epoch and a reply that refuses its request; and where each is skipped with
//! nothing to check.

mod toy_setup;

use std::collections::BTreeSet;
use std::time::Duration;

use fbc_conformance::Frame;
use fbc_conformance::suite::{self, Failure, Replier, Setup, Subject, Verdict};
use fbc_conformance::toy::{self, EXEC_STREAM, ToyFactory};
use fbc_core::{
    AccountSummary, AckLevel, AmendAck, AssetKey, ConfigError, CtxCall, DecodeError, DecodeScope,
    Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint, ExecEvent,
    ExecSink, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentSpecDraft, MdCodec, NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, RpcId,
    Secrets, SpecTable, StreamId, SubmitOutcome, Subscription, SymbolError, TimerTag, TrafficClass,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta, VenueOrderState,
    WireSlice,
};
use fbc_core::{CancelOnDisconnect, ItemRef, RefKind, TagSet};
use toy_setup::{FIXTURES, assumed, order_entry, replier};

/// How a toy variant's codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Twist {
    /// The toy's codec as it is.
    None,
    /// Every outcome a timed-out request reports is `Accepted`.
    AcceptsOnTimeout,
    /// The last placement it wrote is written again every second, its bytes changed as a
    /// request signed again would be.
    Resends,
    /// An amended order's update names no new venue id.
    NoNewVid,
    /// Every placement is refused `Unsupported`.
    RefusesPlacements,
    /// A timed-out request reports nothing.
    SilentOnTimeout,
    /// Each item a timed-out request reports `Unknown` is reported again for the whole request
    /// and for an index three past its own.
    Garbled,
    /// Every outcome a timed-out request reports is reported twice.
    Doubled,
    /// An amended order's update names, as its new venue id, the one it was placed under.
    SameVid,
    /// An accepted placement is reported amended at once, under another venue id.
    EarlyAmended,
    /// Once a request timed out, the next second asks for a reconnect whose opening frame
    /// never fits the venue's buckets, which ends the session with an error.
    FailsAfterTimeout,
    /// As [`Twist::Resends`], but twenty seconds after the connection opened, long after the
    /// toy's five-second deadline.
    ResendsLate,
    /// Once a request timed out, the next second asks for a reconnect, and the new connection's
    /// opening writes the last placement again, signed again.
    ReplacesOnReconnect,
    /// Each timed-out batch's items 0 and 1 swap indices, each keeping its client id.
    SwapsItems,
    /// A placement's acceptance is reported for the whole request, naming no item.
    WholeAcceptance,
}

/// The toy with its caps edited by `caps` and its codec twisted by `twist`.
struct Variant {
    caps: fn(&mut VenueCaps),
    twist: Twist,
}

static ACCEPTS_ON_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::AcceptsOnTimeout,
};
static RESENDS: Variant = Variant {
    caps: |_| {},
    twist: Twist::Resends,
};
static NO_NEW_VID: Variant = Variant {
    caps: |_| {},
    twist: Twist::NoNewVid,
};
static REFUSES_PLACEMENTS: Variant = Variant {
    caps: |_| {},
    twist: Twist::RefusesPlacements,
};
static DOUBLED: Variant = Variant {
    caps: |_| {},
    twist: Twist::Doubled,
};
static SAME_VID: Variant = Variant {
    caps: |_| {},
    twist: Twist::SameVid,
};
static EARLY_AMENDED: Variant = Variant {
    caps: |_| {},
    twist: Twist::EarlyAmended,
};
static RESENDS_LATE: Variant = Variant {
    caps: |_| {},
    twist: Twist::ResendsLate,
};
static REPLACES_ON_RECONNECT: Variant = Variant {
    caps: |_| {},
    twist: Twist::ReplacesOnReconnect,
};
static SWAPS_ITEMS: Variant = Variant {
    caps: |_| {},
    twist: Twist::SwapsItems,
};
static WHOLE_ACCEPTANCE: Variant = Variant {
    caps: |_| {},
    twist: Twist::WholeAcceptance,
};
/// The toy with a second limit, a minute's window that counts placements and never fills here:
/// a check need not wait it out, and through the toy's 15-second ping.
static MINUTE_WINDOW: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Account,
            ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
            per: Duration::from_secs(60),
            units: 1_000,
        });
    },
    twist: Twist::None,
};
static FAILS_AFTER_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::FailsAfterTimeout,
};
/// The toy with a second limit: one placement or amend a second.
static ONE_ORDER_A_SECOND: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Account,
            ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
            per: Duration::from_secs(1),
            units: 1,
        });
    },
    twist: Twist::None,
};
/// The toy declaring client ids in base 36, which its codec, writing base 62, never sends: the
/// frames carry no id the suite can find, so a request is told by its bytes.
static OTHER_CID_FORMAT: Variant = Variant {
    caps: |c| {
        let order = &mut c.exec.as_mut().unwrap().order;
        order.client_id = fbc_core::ClientIdFormat::Alnum {
            max_len: 32,
            charset: fbc_core::Charset::LowerAlphanumeric,
        };
    },
    twist: Twist::None,
};
static SILENT_ON_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::SilentOnTimeout,
};
static GARBLED: Variant = Variant {
    caps: |_| {},
    twist: Twist::Garbled,
};
/// The toy declaring no cancel-on-disconnect, which no order-entry session takes.
static UNPROTECTED: Variant = Variant {
    caps: |c| c.exec.as_mut().unwrap().order.cancel_on_disconnect = CancelOnDisconnect::None,
    twist: Twist::None,
};
/// The toy declaring a limit that admits nothing, which no rate limiter takes.
static NO_UNITS: Variant = Variant {
    caps: |c| c.limits[0].units = 0,
    twist: Twist::None,
};
/// The toy naming an amend's order by its placement nonce alone, which fbc-oms does not track.
static AMEND_BY_NONCE: Variant = Variant {
    caps: |c| amend(c).refs = TagSet::of(&[RefKind::PlacementNonce]),
    twist: Twist::None,
};
/// The toy declaring no order kind.
static NO_KIND: Variant = Variant {
    caps: |c| c.exec.as_mut().unwrap().order.kinds = TagSet::of(&[]),
    twist: Twist::None,
};
/// The toy declaring that only an amend's reply reports it.
static REPLY_ONLY: Variant = Variant {
    caps: |c| amend(c).ack = AmendAck::RpcReplyOnly,
    twist: Twist::None,
};
static NO_EXEC: Variant = Variant {
    caps: |c| c.exec = None,
    twist: Twist::None,
};
static NO_AMEND: Variant = Variant {
    caps: |c| c.exec.as_mut().unwrap().order.amend = None,
    twist: Twist::None,
};
static NOTHING_AMENDABLE: Variant = Variant {
    caps: |c| {
        let amend = amend(c);
        amend.price = false;
        amend.qty = false;
    },
    twist: Twist::None,
};
/// The toy amending quantities only.
static QTY_ONLY: Variant = Variant {
    caps: |c| amend(c).price = false,
    twist: Twist::None,
};
static NO_BATCH: Variant = Variant {
    caps: |c| c.exec.as_mut().unwrap().order.batch_place = None,
    twist: Twist::None,
};
static SHORT_BATCH: Variant = Variant {
    caps: |c| {
        let batch = c.exec.as_mut().unwrap().order.batch_place.as_mut();
        batch.unwrap().max_items = 2;
    },
    twist: Twist::None,
};

fn amend(caps: &mut VenueCaps) -> &mut fbc_core::AmendCaps {
    let order = &mut caps.exec.as_mut().unwrap().order;
    order.amend.as_mut().unwrap()
}

impl Variant {
    fn subject(&'static self, setup: fn() -> Setup) -> Subject<'static> {
        Subject::new(self, FIXTURES, setup).unwrap()
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
        let twist = self.twist;
        let codec = ToyFactory.exec_codec(cfg, creds)?;
        Some(codec.map(|inner| {
            let (last, failed) = (None, false);
            let twisted = Twisted {
                inner,
                twist,
                last,
                failed,
                reconnected: false,
            };
            Box::new(twisted) as Box<dyn ExecCodec>
        }))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The timer a resending codec writes its last request again on.
const RESEND: TimerTag = TimerTag(77);

/// The toy's codec, as its factory builds it, with a twist.
struct Twisted {
    inner: Box<dyn ExecCodec>,
    twist: Twist,
    /// The last request frame it wrote, for [`Twist::Resends`].
    last: Option<Vec<u8>>,
    /// Whether a request timed out, for [`Twist::FailsAfterTimeout`].
    failed: bool,
    /// Whether it asked for the reconnect, for [`Twist::ReplacesOnReconnect`].
    reconnected: bool,
}

/// A sink handing `inner` the events `f` rewrites each into.
struct Rewrite<'a> {
    inner: &'a mut dyn ExecSink,
    f: fn(ExecEvent) -> Vec<ExecEvent>,
}

impl ExecSink for Rewrite<'_> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        for ev in (self.f)(ev) {
            self.inner.push(meta, ev);
        }
    }
}

/// The event as it came.
fn kept(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![ev]
}

/// Every outcome `Accepted`.
fn accepted(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome { rpc, item, .. } => {
            let ack = AckLevel::Final;
            let outcome = SubmitOutcome::Accepted { ack };
            ExecEvent::Outcome { rpc, item, outcome }
        }
        other => other,
    }]
}

/// Nothing at all.
fn dropped(_: ExecEvent) -> Vec<ExecEvent> {
    Vec::new()
}

/// An item's `Unknown` reported for the whole request and for an index three on, then as it was.
fn garbled(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Outcome {
        rpc,
        item: Some(item),
        outcome: SubmitOutcome::Unknown,
    } = ev
    else {
        return vec![ev];
    };
    let unknown = |item| ExecEvent::Outcome {
        rpc,
        item,
        outcome: SubmitOutcome::Unknown,
    };
    let idx = item.idx + 3;
    let beyond = ItemRef {
        idx,
        ..item.clone()
    };
    vec![unknown(None), unknown(Some(beyond)), unknown(Some(item))]
}

/// Every event twice.
fn doubled(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![ev.clone(), ev]
}

/// An amended order's update naming the venue id it was placed under as its new one.
fn same_vid(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.state = VenueOrderState::Amended {
                new_vid: u.vid.clone(),
            };
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An accepted placement's outcome, then an update reporting that order amended.
fn early_amended(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Outcome {
        item:
            Some(ItemRef {
                cid: Some(cid),
                vid: Some(vid),
                ..
            }),
        outcome: SubmitOutcome::Accepted { .. },
        ..
    } = &ev
    else {
        return vec![ev];
    };
    let new_vid = toy::with_scope(|scope| scope.venue_order_id("toy-early")).ok();
    let update = fbc_core::OrderUpdate {
        cid: Some(fbc_core::CidMatch::Ours(*cid)),
        vid: Some(vid.clone()),
        inst: toy::INST_A,
        side: fbc_core::Side::Buy,
        state: VenueOrderState::Amended { new_vid },
        cum_filled: fbc_core::Lots::ZERO,
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    vec![ev, ExecEvent::Order(update)]
}

/// An amended order's update without its new venue id.
fn no_new_vid(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.state = VenueOrderState::Amended { new_vid: None };
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

fn resend_timer() -> Effect {
    Effect::Timer {
        tag: RESEND,
        after: Duration::from_secs(1),
    }
}

/// A placement frame signed again, as a rebuilt retry would be: other bytes, the same orders
/// (Codex r4222138042).
fn again(frame: &[u8]) -> Effect {
    Effect::Send {
        stream: EXEC_STREAM,
        frame: WireSlice::plain([frame, b"|again"].concat()),
        rpc: None,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Place, None),
    }
}

/// Item 0 reported as item 1 and item 1 as item 0, each keeping its client id.
fn swapped(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item: Some(mut it),
            outcome,
        } if it.idx < 2 => {
            it.idx = 1 - it.idx;
            ExecEvent::Outcome {
                rpc,
                item: Some(it),
                outcome,
            }
        }
        other => other,
    }]
}

/// A placement's acceptance for the whole request.
fn whole_acceptance(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item: Some(it),
            outcome: outcome @ SubmitOutcome::Accepted { .. },
        } if it.cid.is_some() => ExecEvent::Outcome {
            rpc,
            item: None,
            outcome,
        },
        other => other,
    }]
}

impl ExecCodec for Twisted {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_open(stream, ctx, fx);
        if self.failed && self.twist == Twist::FailsAfterTimeout {
            // Heavier than the toy's one bucket ever admits.
            let weight = core::num::NonZeroU32::new(1_000).unwrap();
            fx.push(Effect::Send {
                stream,
                frame: WireSlice::plain(b"heavy".to_vec()),
                rpc: None,
                class: TrafficClass::Safety,
                charge: RateCharge {
                    op: OpKind::Control,
                    inst: None,
                    weight,
                },
            });
        }
        if self.reconnected
            && let Some(frame) = self.last.take()
        {
            fx.push(again(&frame));
        }
        match self.twist {
            Twist::Resends | Twist::FailsAfterTimeout | Twist::ReplacesOnReconnect => {
                fx.push(resend_timer());
            }
            Twist::ResendsLate => fx.push(Effect::Timer {
                tag: RESEND,
                after: Duration::from_secs(20),
            }),
            _ => {}
        }
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
        if self.twist == Twist::RefusesPlacements && matches!(cmd, VenueCommand::Place(_)) {
            return Err(NotSentReason::Unsupported);
        }
        let mut mine = Effects::new();
        let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, &mut mine)?;
        for effect in mine.take() {
            let placing = matches!(cmd, VenueCommand::Place(_) | VenueCommand::PlaceBatch(_));
            if let Effect::Send { frame, .. } = &effect
                && placing
            {
                self.last = Some(frame.bytes().to_vec());
            }
            fx.push(effect);
        }
        Ok(receipt)
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
        let f_ = match self.twist {
            Twist::NoNewVid => no_new_vid,
            Twist::SameVid => same_vid,
            Twist::EarlyAmended => early_amended,
            Twist::WholeAcceptance => whole_acceptance,
            _ => kept,
        };
        let sink = &mut Rewrite { inner: sink, f: f_ };
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
        if tag != RESEND {
            return self.inner.on_timer(tag, ctx, fx);
        }
        if self.failed && !self.reconnected {
            self.reconnected = self.twist == Twist::ReplacesOnReconnect;
            let reason = "a twisted toy reconnects after a timeout";
            return fx.push(Effect::Reconnect {
                stream: EXEC_STREAM,
                reason,
            });
        }
        let resends = matches!(self.twist, Twist::Resends | Twist::ResendsLate);
        if let Some(frame) = self.last.as_ref().filter(|_| resends) {
            fx.push(again(frame));
        }
        fx.push(resend_timer());
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let f = match self.twist {
            Twist::AcceptsOnTimeout => accepted,
            Twist::SilentOnTimeout => dropped,
            Twist::Garbled => garbled,
            Twist::Doubled => doubled,
            Twist::FailsAfterTimeout | Twist::ReplacesOnReconnect => {
                self.failed = true;
                kept
            }
            Twist::SwapsItems => swapped,
            _ => return self.inner.on_rpc_timeout(rpc, sink),
        };
        self.inner
            .on_rpc_timeout(rpc, &mut Rewrite { inner: sink, f });
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

/// The toy's setup with no order-entry stub.
fn no_stub() -> Setup {
    Setup {
        order_entry: None,
        ..assumed()
    }
}

/// The toy's setup whose replies report an amend by its reply alone.
fn reply_only() -> Setup {
    let mut stub = order_entry();
    stub.reply = replier(false);
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose opening answers nothing.
fn mute() -> Setup {
    let mut stub = order_entry();
    stub.opening.clear();
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose replies refuse every request.
fn refusing() -> Setup {
    let mut stub = order_entry();
    stub.reply = Replier::new(|_, _| Err("no request answered here".into()));
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose replies reject every item, whatever was asked.
fn rejecting() -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let rejects = vec![suite::Answer::Reject; answers.len()];
        inner.reply(frame, &rejects)
    });
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose replies accept placements and reject every amend.
fn rejecting_amends() -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let amend = matches!(frame, Frame::Text(t) if t.starts_with("amend|"));
        let rejects = vec![suite::Answer::Reject; answers.len()];
        inner.reply(frame, if amend { &rejects } else { answers })
    });
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    outcome.expect_err("the check failed")
}

/// Whether a breach of `capability` says `what`.
fn says(failure: &Failure, capability: &str, what: &str) -> bool {
    let found = failure.breaches.iter();
    let mut found = found.filter(|b| b.capability == capability);
    found.any(|b| b.what.contains(what))
}

#[test]
fn mixed_batch_fails_a_toy_that_reports_every_item_of_a_timed_out_batch_accepted() {
    let failure = failed(suite::mixed_batch(&ACCEPTS_ON_TIMEOUT.subject(assumed)));
    assert!(failure.names("ExecCodec::on_rpc_timeout"), "{failure}");
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "item 2, which the stub never answered"
        ),
        "{failure}"
    );
    assert!(
        says(
            &failure,
            "OrderCaps.batch_place",
            "item 1, which the stub rejected"
        ),
        "{failure}"
    );
    // The item the stub accepted stays accepted.
    assert!(!failure.to_string().contains("item 0"), "{failure}");
}

#[test]
fn unknown_on_timeout_fails_a_toy_that_reports_a_timed_out_placement_accepted() {
    let failure = failed(suite::unknown_on_timeout(
        &ACCEPTS_ON_TIMEOUT.subject(assumed),
    ));
    assert!(
        says(&failure, "ExecCodec::on_rpc_timeout", "not Unknown once"),
        "{failure}"
    );
}

#[test]
fn unknown_on_timeout_and_mixed_batch_fail_a_toy_that_writes_a_request_again() {
    let failure = failed(suite::unknown_on_timeout(&RESENDS.subject(assumed)));
    assert!(failure.names("ExecCodec: never resent"), "{failure}");
    assert_eq!(failure.breaches.len(), 1, "{failure}");
    let failure = failure_of_batch(&RESENDS);
    assert!(
        says(&failure, "ExecCodec: never resent", "the batch was written"),
        "{failure}"
    );
}

fn failure_of_batch(variant: &'static Variant) -> Failure {
    failed(suite::mixed_batch(&variant.subject(assumed)))
}

#[test]
fn amend_ack_fails_a_toy_declaring_its_amend_reported_by_the_reply_alone_that_synthesizes_none() {
    let failure = failed(suite::amend_ack(&REPLY_ONLY.subject(reply_only)));
    assert!(
        says(
            &failure,
            "AmendCaps.ack is RpcReplyOnly",
            "synthesized no OrderUpdate Amended"
        ),
        "{failure}"
    );
    // With the venue's event, the same declaration passes: the update surfaced.
    let passed = suite::amend_ack(&REPLY_ONLY.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_order_names_no_new_venue_id() {
    let failure = failed(suite::amend_ack(&NO_NEW_VID.subject(assumed)));
    assert!(
        failure.names("AmendCaps.keeps_venue_id is false"),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_when_the_events_reporting_the_replaced_order_never_come() {
    let failure = failed(suite::amend_ack(&ToyFactory.subject_for(reply_only)));
    assert!(
        says(
            &failure,
            "AmendCaps.ack is ReplacedEvent",
            "surfaced no OrderUpdate Amended"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_amends_the_quantity_where_the_price_is_not_amendable() {
    let passed = suite::amend_ack(&QTY_ONLY.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn a_placement_the_codec_refuses_or_the_venue_rejects_fails_each_check() {
    let failure = failed(suite::unknown_on_timeout(
        &REFUSES_PLACEMENTS.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "ExecCodec::encode",
            "was not sent: Some(Err(Unsupported))"
        ),
        "{failure}"
    );
    let failure = failed(suite::amend_ack(&ToyFactory.subject_for(rejecting)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "the placement the stub accepted"
        ),
        "{failure}"
    );
}

#[test]
fn every_order_entry_check_skips_a_venue_that_takes_no_orders() {
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let skipped = check(&NO_EXEC.subject(assumed));
        let why = "VenueCaps.exec is None: the venue takes no orders";
        assert!(
            matches!(skipped, Ok(Verdict::Skipped { why: w, .. }) if w == why),
            "{skipped:?}"
        );
    }
}

#[test]
fn amend_ack_skips_a_venue_that_takes_no_amend_or_amends_neither_price_nor_quantity() {
    let skipped = |variant: &'static Variant| match suite::amend_ack(&variant.subject(assumed)) {
        Ok(Verdict::Skipped { why, .. }) => why,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        skipped(&NO_AMEND),
        "OrderCaps.amend is None: the venue takes no amend"
    );
    assert_eq!(
        skipped(&NOTHING_AMENDABLE),
        "AmendCaps declares neither the price nor the quantity amendable"
    );
}

#[test]
fn mixed_batch_skips_a_venue_that_takes_no_batch_of_three() {
    let skipped = |variant: &'static Variant| match suite::mixed_batch(&variant.subject(assumed)) {
        Ok(Verdict::Skipped { why, .. }) => why,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        skipped(&NO_BATCH),
        "OrderCaps.batch_place is None: the venue takes no batch"
    );
    assert!(skipped(&SHORT_BATCH).starts_with("Batch.max_items is below 3"));
}

#[test]
fn a_venue_taking_orders_with_no_order_entry_stub_fails_every_order_entry_check() {
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let failure = failed(check(&ToyFactory.subject_for(no_stub)));
        assert!(failure.names("Setup.order_entry"), "{failure}");
    }
}

#[test]
fn an_opening_that_never_readies_the_epoch_fails_with_what_the_stub_and_session_ended_with() {
    let failure = failed(suite::unknown_on_timeout(&ToyFactory.subject_for(mute)));
    assert!(failure.names("OrderEntryStub.opening"), "{failure}");
    // The reply read the authentication, which it refuses: the script stopped there, and the
    // reason names the record's kind, never the token it carries.
    assert!(
        says(&failure, "OrderEntryStub", "no rpc in the auth record"),
        "{failure}"
    );
    assert!(!failure.to_string().contains(toy::TOY_TOKEN), "{failure}");
}

#[test]
fn a_reply_that_refuses_its_request_fails_the_check_naming_the_stub() {
    for check in [suite::mixed_batch, suite::unknown_on_timeout] {
        let failure = failed(check(&ToyFactory.subject_for(refusing)));
        assert!(
            says(&failure, "OrderEntryStub", "no request answered here"),
            "{failure}"
        );
    }
}

/// The toy under another setup.
trait SubjectFor {
    fn subject_for(&'static self, setup: fn() -> Setup) -> Subject<'static>;
}

impl SubjectFor for ToyFactory {
    fn subject_for(&'static self, setup: fn() -> Setup) -> Subject<'static> {
        Subject::new(self, FIXTURES, setup).unwrap()
    }
}

#[test]
fn the_frames_the_toys_stub_answers_with_carry_what_the_request_named() {
    // A batch item's answer names its request and index; a reject carries the toy's code.
    let reply = replier(true);
    let batch = Frame::text("batch|rpc=9|n=2\nplace|i=0|cid=a\nplace|i=1|cid=b");
    let answers = [suite::Answer::Accept, suite::Answer::Reject];
    assert_eq!(
        reply.reply(&batch, &answers).unwrap(),
        [
            Frame::text("item|rpc=9|i=0|res=ok|vid=toy-1"),
            Frame::text("item|rpc=9|i=1|res=rej|code=1002|msg=refused"),
        ]
    );
    // A count of answers that is not the request's count of items is refused.
    let err = reply.reply(&batch, &answers[..1]).unwrap_err();
    assert!(err.starts_with("2 items, 1 answers"), "{err}");
    // An amend of an order the stub never accepted is refused.
    let amend = Frame::text("amend|rpc=10|vid=toy-9|sym=S|side=B|px=1|qty=1|po=1|ro=0");
    let err = reply.reply(&amend, &[suite::Answer::Accept]).unwrap_err();
    assert_eq!(err, "no order toy-9 to amend");
    assert!(reply.reply(&Frame::Binary(vec![1]), &[]).is_err());
}

#[test]
fn mixed_batch_fails_a_toy_whose_timeout_reports_nothing_after_waiting_out_the_longest_deadline() {
    let failure = failed(suite::mixed_batch(&SILENT_ON_TIMEOUT.subject(assumed)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "not every item had an outcome 600s"
        ),
        "{failure}"
    );
}

#[test]
fn mixed_batch_fails_a_toy_that_reports_the_whole_batch_and_items_it_does_not_hold() {
    let failure = failed(suite::mixed_batch(&GARBLED.subject(assumed)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "outcomes for the whole batch"
        ),
        "{failure}"
    );
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "items the batch does not hold"
        ),
        "{failure}"
    );
}

#[test]
fn mixed_batch_fails_when_the_item_the_stub_accepted_comes_back_rejected() {
    let failure = failed(suite::mixed_batch(&ToyFactory.subject_for(rejecting)));
    assert!(
        says(
            &failure,
            "OrderCaps.batch_place",
            "item 0, which the stub accepted"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_when_the_amend_the_stub_accepted_comes_back_rejected() {
    let failure = failed(suite::amend_ack(&ToyFactory.subject_for(rejecting_amends)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "the amend the stub accepted"
        ),
        "{failure}"
    );
}

#[test]
fn a_venue_the_session_refuses_or_whose_caps_allow_no_order_fails_naming_why() {
    let failure = failed(suite::unknown_on_timeout(&UNPROTECTED.subject(assumed)));
    assert!(failure.names("ExecSession::new"), "{failure}");
    let failure = failed(suite::unknown_on_timeout(&NO_KIND.subject(assumed)));
    assert!(failure.names("OrderCaps"), "{failure}");
}

#[test]
fn an_order_entry_stub_and_its_replier_show_no_function_in_debug() {
    let setup = format!("{:?}", assumed());
    assert!(
        setup.contains("OrderEntryStub { opening: 3, .. }"),
        "{setup}"
    );
    assert_eq!(format!("{:?}", replier(true)), "Replier(..)");
}

/// The toy's setup with no instrument.
fn no_instruments() -> Setup {
    Setup {
        specs: SpecTable::new(),
        ..assumed()
    }
}

#[test]
fn a_setup_with_no_instrument_or_a_limit_admitting_nothing_fails_before_any_session() {
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let failure = failed(check(&ToyFactory.subject_for(no_instruments)));
        assert!(failure.names("setup"), "{failure}");
    }
    let failure = failed(suite::unknown_on_timeout(&NO_UNITS.subject(assumed)));
    assert!(says(&failure, "VenueCaps.limits", "Empty"), "{failure}");
}

#[test]
fn amend_ack_fails_when_fbc_oms_cannot_name_the_order_by_any_reference_the_amend_takes() {
    let failure = failed(suite::amend_ack(&AMEND_BY_NONCE.subject(assumed)));
    assert!(says(&failure, "fbc-oms", "refused the amend"), "{failure}");
}

#[test]
fn mixed_batch_fails_a_toy_that_reports_each_answered_item_twice() {
    let failure = failed(suite::mixed_batch(&DOUBLED.subject(assumed)));
    for item in [
        "item 0, which the stub accepted",
        "item 1, which the stub rejected",
    ] {
        assert!(says(&failure, "OrderCaps.batch_place", item), "{failure}");
    }
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_order_keeps_the_venue_id_it_declares_replaced() {
    let failure = failed(suite::amend_ack(&SAME_VID.subject(assumed)));
    assert!(
        says(
            &failure,
            "AmendCaps.keeps_venue_id is false",
            "having been placed under"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_takes_no_amended_update_reported_before_the_amend() {
    // The update the placement brought is no answer to the amend, which brings none.
    let failure = failed(suite::amend_ack(&EARLY_AMENDED.subject(reply_only)));
    assert!(failure.names("AmendCaps.ack is ReplacedEvent"), "{failure}");
}

#[test]
fn amend_ack_waits_out_a_limit_of_one_order_a_second_before_amending() {
    let passed = suite::amend_ack(&ONE_ORDER_A_SECOND.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn mixed_batch_passes_on_an_instrument_whose_minimum_size_is_two_million_lots() {
    fn huge() -> Setup {
        let mut specs = toy::specs();
        let mut a = specs.get(toy::INST_A).unwrap().clone();
        a.min_size = fbc_core::Lots::new(2_000_000).unwrap();
        specs.insert(a);
        Setup { specs, ..assumed() }
    }
    let passed = suite::mixed_batch(&ToyFactory.subject_for(huge));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn a_session_that_ends_with_an_error_fails_a_check_whose_scenario_saw_everything() {
    let failure = failed(suite::unknown_on_timeout(
        &FAILS_AFTER_TIMEOUT.subject(assumed),
    ));
    assert!(
        says(&failure, "ExecSession::run", "OpenNeverFits"),
        "{failure}"
    );
    assert_eq!(failure.breaches.len(), 1, "{failure}");
}

#[test]
fn a_venue_whose_frames_carry_no_client_id_the_suite_can_find_is_judged_by_the_frames_bytes() {
    let passed = suite::unknown_on_timeout(&OTHER_CID_FORMAT.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn mixed_batch_fails_a_toy_whose_items_name_each_others_client_ids() {
    let failure = failed(suite::mixed_batch(&SWAPS_ITEMS.subject(assumed)));
    assert!(says(&failure, "ItemRef.cid", "items [1, 0]"), "{failure}");
}

#[test]
fn unknown_on_timeout_sees_a_request_written_again_long_after_its_deadline() {
    let failure = failed(suite::unknown_on_timeout(&RESENDS_LATE.subject(assumed)));
    assert!(failure.names("ExecCodec: never resent"), "{failure}");
}

#[test]
fn unknown_on_timeout_sees_a_request_written_again_on_the_next_connection() {
    let failure = failed(suite::unknown_on_timeout(
        &REPLACES_ON_RECONNECT.subject(assumed),
    ));
    assert!(
        says(&failure, "ExecCodec: never resent", "written 2 times"),
        "{failure}"
    );
}

#[test]
fn amend_ack_takes_an_acceptance_of_the_whole_placement_as_its_one_items() {
    // The toy's amend names the order by its venue id, which an acceptance naming no item
    // does not give: fbc-oms refuses the amend, past the placement.
    let failure = failed(suite::amend_ack(&WHOLE_ACCEPTANCE.subject(assumed)));
    assert!(says(&failure, "fbc-oms", "refused the amend"), "{failure}");
    assert!(
        !failure
            .to_string()
            .contains("the placement the stub accepted"),
        "{failure}"
    );
}

#[test]
fn a_limit_whose_bucket_never_fills_is_not_waited_out() {
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let passed = check(&MINUTE_WINDOW.subject(assumed));
        assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
    }
}
