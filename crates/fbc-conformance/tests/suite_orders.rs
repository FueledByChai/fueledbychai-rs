//! FBC-3il's done line, its second half: `mixed_batch` run against a toy variant that reports
//! every item of a timed-out batch `Accepted` fails, naming the timeout (BT-502). Around it, every
//! other way the three order-entry checks can fail: that variant's unanswered placement reported
//! `Accepted`, a request written again after its deadline, an amend whose reply alone is declared
//! to report it (`RpcReplyOnly`) with no update synthesized, an amended order's new venue id
//! missing, a placement refused or answered wrongly, a setup stating no replies, an opening that
//! never readies the epoch and a reply that refuses its request, an unanswered request reported
//! before its deadline, an `Unknown` or an amended update naming another order; and where each
//! is skipped with nothing to check.
//!
//! FBC-y6y's done line, its second half: `resync_after_reconnect` run against a toy variant that
//! re-places an open order after a reconnect fails, naming the re-placement. Around it: a
//! reconnect that leads to no resync; `two_phase_ack` on a two-phase toy whose provisional
//! acceptance is followed by a final one, by an asynchronous reject, by nothing, or that accepts
//! final at once, and on the single-phase toy reporting a provisional acceptance;
//! `reject_coverage` against a toy misreading every refusal, a stub answering none, and a table
//! that is missing, empty or names no kind.

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
    /// Every acceptance a timed-out request reports is provisional.
    ProvisionalOnTimeout,
    /// Every acceptance a frame brings naming no venue id (the arm's, the amend's) is reported
    /// twice.
    DoubledAck,
    /// A placement's acceptance, naming its venue id, is reported twice.
    DoubledPlacementAck,
    /// A placement's acceptance is provisional, on a venue declaring single-phase acceptances.
    ProvisionalPlacement,
    /// An amended order's update is followed by a refusal of the amend naming the order.
    RefusesAmendToo,
    /// An amended order's update is preceded by an update reporting the order canceled.
    CancelsOnAmend,
    /// An amended order's update states a lot of it filled.
    FilledOnAmend,
    /// An amended order's update is followed by another, a tick above its price.
    AmendedTwice,
    /// An amended order's update reports it post-only.
    PostOnlyOnAmend,
    /// An amended order's update carries no flags.
    NoFlagsOnAmend,
    /// 70 ms after the connection opens, a maintenance frame is written.
    MaintainsAt70Ms,
    /// An amended order's update carries no client id.
    NoCidOnAmend,
    /// The first frame read after an amend is written writes the amend again.
    ResendsAmend,
    /// An amended order's update states a price one tick above the amend's.
    WrongPx,
    /// A frame `early` reports the last request it wrote timed out, at once, long before its
    /// deadline; its deadline then reports nothing more.
    EagerTimeout,
    /// A placement's acceptance names no venue id; an `Open` update of the order, with it,
    /// follows.
    VidOnUpdate,
    /// An amended order's update names it by its venue id with a client id not canonical.
    StrangerAmended,
    /// A timed-out placement's `Unknown` names another order's client id.
    StrangerOnTimeout,
    /// A timed-out request's `Unknown` names its item with a venue id the stub never sent.
    VidOnTimeout,
    /// A placement's acceptance names item 1 of its single command.
    PlacedAsItemOne,
    /// An amended order's update is followed by an `Open` update of the order under its new
    /// venue id, a tick above the amend's price.
    OpenAfterAmend,
    /// An amended order's update is followed by an `Open` update of the order under its new
    /// venue id, stating what the amend asked for.
    OpenAgreesAfterAmend,
    /// An amended order's update is followed by a fill of a lot of the order.
    FillsOnAmend,
    /// A placement's acceptance is followed by a fill of a lot of the order.
    FillsOnPlacement,
    /// An amended order's update is followed by another naming another new venue id.
    RenamedTwice,
    /// Every acceptance naming no venue id (the arm's, the amend's) names a stranger's.
    StrangerVidOnAck,
    /// On every connection after the first, once its resync has ended, the last placement it
    /// wrote is written again, signed again: an open order re-placed after a reconnect.
    ReplacesAfterResync,
    /// Only the first connection asks for a resync.
    ResyncsOnce,
    /// Every refusal names item 1, carries no venue code, and maps to `Other`.
    MisreadsRejects,
    /// A placement's acceptance is provisional, then final.
    ProvisionalThenFinal,
    /// A placement's acceptance is provisional, then refused by the venue's risk check, naming
    /// the order by its venue id.
    ProvisionalThenRiskReject,
    /// A placement's acceptance is provisional, then final, then refused by the venue's risk
    /// check.
    FinalAndRiskReject,
    /// A placement's acceptance is preceded by the venue's risk check refusing it, then reported
    /// provisional (Codex r4225971611).
    RiskRejectFirst,
    /// Every refusal's item names a stranger's client id (Codex r4225971615).
    StrangerOnReject,
    /// A placement's acceptance is followed by a stray resync end, and every resync frame on a
    /// later connection is ignored (Codex r4225971621).
    StrayResyncEnd,
    /// On every connection after the first, the authentication's answer is followed by a stray
    /// resync end, and every resync frame is ignored (Codex r4226060251).
    StrayResyncEndOnReconnect,
    /// As [`Twist::ProvisionalThenRiskReject`], the reject naming the order's venue id beside a
    /// stranger's client id (Codex r4226156929).
    RiskRejectNamingTwoOrders,
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
/// The toy with a second limit, one placement a minute: the opening's frames are no
/// placements, so its bucket is empty when the first placement goes, and waiting its window out
/// would let the toy's 15-second ping reach the stub in that placement's place.
static ONE_PLACEMENT_A_MINUTE: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Account,
            ops: TagSet::of(&[OpKind::Place]),
            per: Duration::from_secs(60),
            units: 1,
        });
    },
    twist: Twist::None,
};
/// The toy with a second limit: one placement or amend a second on the instrument.
static ONE_ORDER_A_SECOND_A_PAIR: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Pair,
            ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
            per: Duration::from_secs(1),
            units: 1,
        });
    },
    twist: Twist::None,
};
/// The toy with a second limit: one placement or amend a second on the connection.
static ONE_ORDER_A_SECOND_A_CONNECTION: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Connection,
            ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
            per: Duration::from_secs(1),
            units: 1,
        });
    },
    twist: Twist::None,
};
static PROVISIONAL_ON_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::ProvisionalOnTimeout,
};
/// The toy declaring two-phase acknowledgements, its timed-out acceptances provisional.
static TWO_PHASE: Variant = Variant {
    caps: |c| {
        let window = Duration::from_secs(1);
        c.exec.as_mut().unwrap().order.ack = fbc_core::AckModel::TwoPhase {
            risk_reject_window: window,
        };
    },
    twist: Twist::ProvisionalOnTimeout,
};
static DOUBLED_AMEND_ACK: Variant = Variant {
    caps: |_| {},
    twist: Twist::DoubledAck,
};
static DOUBLED_PLACEMENT_ACK: Variant = Variant {
    caps: |_| {},
    twist: Twist::DoubledPlacementAck,
};
static PROVISIONAL_PLACEMENT: Variant = Variant {
    caps: |_| {},
    twist: Twist::ProvisionalPlacement,
};
static REFUSES_AMEND_TOO: Variant = Variant {
    caps: |_| {},
    twist: Twist::RefusesAmendToo,
};
static CANCELS_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::CancelsOnAmend,
};
static POST_ONLY_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::PostOnlyOnAmend,
};
static NO_FLAGS_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::NoFlagsOnAmend,
};
/// The toy declaring market orders alone.
static MARKET_ONLY: Variant = Variant {
    caps: |c| {
        let order = &mut c.exec.as_mut().unwrap().order;
        order.kinds = TagSet::of(&[fbc_core::OrderKindTag::Market]);
    },
    twist: Twist::None,
};
/// The toy with a second limit, one placement or amend in 50 ms, writing a maintenance frame
/// 70 ms after the connection opens.
static FIFTY_MS_WINDOW: Variant = Variant {
    caps: |c| {
        c.limits.push(fbc_core::RateLimit {
            scope: fbc_core::LimitScope::Account,
            ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
            per: Duration::from_millis(50),
            units: 1,
        });
    },
    twist: Twist::MaintainsAt70Ms,
};
static NO_CID_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::NoCidOnAmend,
};
static RESENDS_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::ResendsAmend,
};
static FILLED_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::FilledOnAmend,
};
static AMENDED_TWICE: Variant = Variant {
    caps: |_| {},
    twist: Twist::AmendedTwice,
};
static WRONG_PX: Variant = Variant {
    caps: |_| {},
    twist: Twist::WrongPx,
};
static EAGER_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::EagerTimeout,
};
static VID_ON_UPDATE: Variant = Variant {
    caps: |_| {},
    twist: Twist::VidOnUpdate,
};
static STRANGER_AMENDED: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrangerAmended,
};
static STRANGER_ON_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrangerOnTimeout,
};
static VID_ON_TIMEOUT: Variant = Variant {
    caps: |_| {},
    twist: Twist::VidOnTimeout,
};
static PLACED_AS_ITEM_ONE: Variant = Variant {
    caps: |_| {},
    twist: Twist::PlacedAsItemOne,
};
static OPEN_AFTER_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::OpenAfterAmend,
};
static OPEN_AGREES_AFTER_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::OpenAgreesAfterAmend,
};
static STRANGER_VID_ON_ACK: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrangerVidOnAck,
};
/// The toy allowing limit orders only immediate or cancel.
static IOC_LIMITS: Variant = Variant {
    caps: |c| {
        let order = &mut c.exec.as_mut().unwrap().order;
        order.tifs = TagSet::of(&[fbc_core::TifTag::Ioc]);
    },
    twist: Twist::None,
};
static RENAMED_TWICE: Variant = Variant {
    caps: |_| {},
    twist: Twist::RenamedTwice,
};
static FILLS_ON_AMEND: Variant = Variant {
    caps: |_| {},
    twist: Twist::FillsOnAmend,
};
static FILLS_ON_PLACEMENT: Variant = Variant {
    caps: |_| {},
    twist: Twist::FillsOnPlacement,
};
/// The toy declaring that an amended order keeps its venue id, which its events still replace.
static KEEPS_VID: Variant = Variant {
    caps: |c| amend(c).keeps_venue_id = true,
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
/// The toy amending prices only.
static PRICE_ONLY: Variant = Variant {
    caps: |c| amend(c).qty = false,
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

static REPLACES_AFTER_RESYNC: Variant = Variant {
    caps: |_| {},
    twist: Twist::ReplacesAfterResync,
};
static RESYNCS_ONCE: Variant = Variant {
    caps: |_| {},
    twist: Twist::ResyncsOnce,
};
static MISREADS_REJECTS: Variant = Variant {
    caps: |_| {},
    twist: Twist::MisreadsRejects,
};
/// The toy declaring two-phase acknowledgements, a risk window of a second.
fn two_phase(c: &mut VenueCaps) {
    let window = Duration::from_secs(1);
    c.exec.as_mut().unwrap().order.ack = fbc_core::AckModel::TwoPhase {
        risk_reject_window: window,
    };
}
static TWO_PHASE_FINAL: Variant = Variant {
    caps: two_phase,
    twist: Twist::ProvisionalThenFinal,
};
static TWO_PHASE_RISK_REJECT: Variant = Variant {
    caps: two_phase,
    twist: Twist::ProvisionalThenRiskReject,
};
static TWO_PHASE_BOTH: Variant = Variant {
    caps: two_phase,
    twist: Twist::FinalAndRiskReject,
};
static TWO_PHASE_REJECT_FIRST: Variant = Variant {
    caps: two_phase,
    twist: Twist::RiskRejectFirst,
};
static STRANGER_ON_REJECT: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrangerOnReject,
};
static STRAY_RESYNC_END: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrayResyncEnd,
};
static STRAY_RESYNC_END_ON_RECONNECT: Variant = Variant {
    caps: |_| {},
    twist: Twist::StrayResyncEndOnReconnect,
};
static TWO_PHASE_REJECT_TWO_ORDERS: Variant = Variant {
    caps: two_phase,
    twist: Twist::RiskRejectNamingTwoOrders,
};
static TWO_PHASE_DANGLING: Variant = Variant {
    caps: two_phase,
    twist: Twist::ProvisionalPlacement,
};
static TWO_PHASE_AT_ONCE: Variant = Variant {
    caps: two_phase,
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
                rpc: None,
                early: None,
                amend: None,
                opens: 0,
                resyncs: 0,
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
/// The timer of [`Twist::MaintainsAt70Ms`].
const MAINTAIN: TimerTag = TimerTag(78);

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
    /// The last request it wrote, for [`Twist::EagerTimeout`].
    rpc: Option<RpcId>,
    /// The request it reported timed out early, for [`Twist::EagerTimeout`].
    early: Option<RpcId>,
    /// The amend frame it wrote and has not written again, for [`Twist::ResendsAmend`].
    amend: Option<Vec<u8>>,
    /// The connections it opened, for [`Twist::ReplacesAfterResync`].
    opens: usize,
    /// The resyncs it asked for, for [`Twist::ResyncsOnce`].
    resyncs: usize,
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

/// Every acceptance provisional.
fn provisional(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item,
            outcome: SubmitOutcome::Accepted { .. },
        } => ExecEvent::Outcome {
            rpc,
            item,
            outcome: SubmitOutcome::Accepted {
                ack: AckLevel::Provisional,
            },
        },
        other => other,
    }]
}

/// Whether `ev` is an acceptance whose item names a venue id (`named`) or none (`!named`): the
/// toy's placement acceptance names one, its amend's and its arm's none.
fn acceptance(ev: &ExecEvent, named: bool) -> bool {
    let ExecEvent::Outcome {
        item,
        outcome: SubmitOutcome::Accepted { .. },
        ..
    } = ev
    else {
        return false;
    };
    item.as_ref().is_some_and(|it| it.vid.is_some()) == named
}

/// Every acceptance a frame brings naming no venue id, twice.
fn doubled_ack(ev: ExecEvent) -> Vec<ExecEvent> {
    if acceptance(&ev, false) {
        vec![ev.clone(), ev]
    } else {
        vec![ev]
    }
}

/// A placement's acceptance, naming its venue id, twice.
fn doubled_placement_ack(ev: ExecEvent) -> Vec<ExecEvent> {
    if acceptance(&ev, true) {
        vec![ev.clone(), ev]
    } else {
        vec![ev]
    }
}

/// A placement's acceptance, naming its venue id, provisional.
fn provisional_placement(ev: ExecEvent) -> Vec<ExecEvent> {
    if acceptance(&ev, true) {
        provisional(ev)
    } else {
        vec![ev]
    }
}

/// An amended order's update, then a refusal of the amend naming the order by both its ids.
fn refuses_amend_too(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Order(u) = &ev else {
        return vec![ev];
    };
    let (VenueOrderState::Amended { .. }, Some(fbc_core::CidMatch::Ours(cid)), Some(vid)) =
        (&u.state, u.cid, u.vid.clone())
    else {
        return vec![ev];
    };
    let reject = fbc_core::Reject {
        kind: fbc_core::RejectKind::InvalidPrice,
        venue_code: None,
        raw: "refused".into(),
    };
    let refusal = ExecEvent::AsyncReject {
        target: fbc_core::OrderRef::Both(cid, vid),
        op: OpKind::Amend,
        reject,
    };
    vec![ev, refusal]
}

/// An amended order's update, after an update reporting the order canceled under the venue id
/// it was placed under.
fn cancels_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Order(u) = &ev else {
        return vec![ev];
    };
    if !matches!(u.state, VenueOrderState::Amended { .. }) {
        return vec![ev];
    }
    let mut canceled = u.clone();
    canceled.state = VenueOrderState::Canceled(fbc_core::CancelReason::Requested);
    vec![ExecEvent::Order(canceled), ev]
}

/// An amended order's update a tick above its price.
fn wrong_px(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.px = u.px.map(|px| fbc_core::Ticks(px.0 + 1));
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An amended order's update stating a lot of it filled.
fn filled_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.cum_filled = fbc_core::Lots::new(1).unwrap();
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An amended order's update reporting it post-only.
fn post_only_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.post_only = Some(true);
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An amended order's update without its client id.
fn no_cid_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.cid = None;
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An amended order's update without its flags.
fn no_flags_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.post_only = None;
            u.reduce_only = None;
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// An amended order's update, then another a tick above its price.
fn amended_twice(ev: ExecEvent) -> Vec<ExecEvent> {
    let mut out = vec![ev.clone()];
    out.extend(wrong_px(ev).into_iter().filter(
        |e| matches!(e, ExecEvent::Order(u) if matches!(u.state, VenueOrderState::Amended { .. })),
    ));
    out
}

/// An amended order's update, then an `Open` update of the order under its new venue id, as
/// `edit` leaves it.
fn open_after(ev: ExecEvent, edit: fn(&mut fbc_core::OrderUpdate)) -> Vec<ExecEvent> {
    let ExecEvent::Order(u) = &ev else {
        return vec![ev];
    };
    let VenueOrderState::Amended { new_vid } = &u.state else {
        return vec![ev];
    };
    let mut open = u.clone();
    open.vid = new_vid.clone();
    open.state = VenueOrderState::Open;
    edit(&mut open);
    vec![ev, ExecEvent::Order(open)]
}

/// An amended order's update, then an `Open` update of it a tick above the amend's price.
fn open_after_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    open_after(ev, |u| u.px = u.px.map(|px| fbc_core::Ticks(px.0 + 1)))
}

/// An amended order's update, then an `Open` update of it stating what the amend asked for.
fn open_agrees_after_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    open_after(ev, |_| {})
}

/// An amended order's update, then a fill of a lot of the order under its new venue id.
fn fills_on_amend(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Order(u) = &ev else {
        return vec![ev];
    };
    let VenueOrderState::Amended { new_vid } = &u.state else {
        return vec![ev];
    };
    let fill = fill_of(new_vid.clone(), u.cid, u.px.unwrap_or(fbc_core::Ticks(1)));
    vec![ev, fill]
}

/// A placement's acceptance naming its venue id, then a fill of a lot of the order under it,
/// naming no client id.
fn fills_on_placement(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Outcome {
        item: Some(ItemRef { vid: Some(vid), .. }),
        outcome: SubmitOutcome::Accepted { .. },
        ..
    } = &ev
    else {
        return vec![ev];
    };
    let fill = fill_of(Some(vid.clone()), None, fbc_core::Ticks(1));
    vec![ev, fill]
}

/// A maker fill of a lot of the toy's buy on its first instrument, naming the order by `vid`
/// and `cid`, at `px`.
fn fill_of(
    vid: Option<fbc_core::VenueOrderId>,
    cid: Option<fbc_core::CidMatch>,
    px: fbc_core::Ticks,
) -> ExecEvent {
    let usdc = fbc_core::AssetSym::new("USDC").unwrap();
    let fee = toy::with_scope(|scope| scope.fee(0, usdc)).unwrap();
    let fill_id = toy::with_scope(|scope| scope.fill_id("toy-fill")).unwrap();
    let fill = fbc_core::FillEvent {
        ident: fbc_core::FillIdent::Venue {
            fill: fill_id,
            vid,
            cum_after: None,
        },
        cid,
        inst: toy::INST_A,
        side: fbc_core::Side::Buy,
        px,
        qty: fbc_core::Lots::new(1).unwrap(),
        liquidity: fbc_core::Liquidity3::Maker,
        fee,
        realized_pnl: None,
        realized_funding: None,
        replay: false,
    };
    ExecEvent::Fill(fill)
}

/// Every acceptance naming no venue id, naming a stranger's.
fn stranger_vid_on_ack(ev: ExecEvent) -> Vec<ExecEvent> {
    if !acceptance(&ev, false) {
        return vec![ev];
    }
    let ExecEvent::Outcome {
        rpc,
        item: Some(mut item),
        outcome,
    } = ev
    else {
        return vec![ev];
    };
    item.vid = toy::with_scope(|scope| scope.venue_order_id("toy-stranger")).ok();
    vec![ExecEvent::Outcome {
        rpc,
        item: Some(item),
        outcome,
    }]
}

/// An amended order's update, then another naming another new venue id.
fn renamed_twice(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Order(u) = &ev else {
        return vec![ev];
    };
    if !matches!(u.state, VenueOrderState::Amended { .. }) {
        return vec![ev];
    }
    let mut again = u.clone();
    let new_vid = toy::with_scope(|scope| scope.venue_order_id("toy-other")).ok();
    again.state = VenueOrderState::Amended { new_vid };
    vec![ev, ExecEvent::Order(again)]
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

/// A placement's acceptance naming no venue id, then an `Open` update of the order with it.
fn vid_on_update(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Outcome {
        rpc,
        item:
            Some(ItemRef {
                idx,
                cid: Some(cid),
                vid: vid @ Some(_),
            }),
        outcome: outcome @ SubmitOutcome::Accepted { .. },
    } = ev
    else {
        return vec![ev];
    };
    let item = Some(ItemRef {
        idx,
        cid: Some(cid),
        vid: None,
    });
    let update = fbc_core::OrderUpdate {
        cid: Some(fbc_core::CidMatch::Ours(cid)),
        vid,
        inst: toy::INST_A,
        side: fbc_core::Side::Buy,
        state: VenueOrderState::Open,
        cum_filled: fbc_core::Lots::ZERO,
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    let outcome = ExecEvent::Outcome { rpc, item, outcome };
    vec![outcome, ExecEvent::Order(update)]
}

/// A placement's acceptance as item 1.
fn placed_as_item_one(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item: Some(mut it),
            outcome: outcome @ SubmitOutcome::Accepted { .. },
        } if it.vid.is_some() => {
            it.idx = 1;
            ExecEvent::Outcome {
                rpc,
                item: Some(it),
                outcome,
            }
        }
        other => other,
    }]
}

/// An amended order's update with a client id not canonical.
fn stranger_amended(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Order(mut u) if matches!(u.state, VenueOrderState::Amended { .. }) => {
            u.cid = Some(fbc_core::CidMatch::Unparseable);
            ExecEvent::Order(u)
        }
        other => other,
    }]
}

/// A whole request's `Unknown` as its item 0's, naming another order's client id.
fn stranger_on_timeout(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item: None,
            outcome: outcome @ SubmitOutcome::Unknown,
        } => {
            let item = Some(ItemRef {
                idx: 0,
                cid: Some(stranger()),
                vid: None,
            });
            ExecEvent::Outcome { rpc, item, outcome }
        }
        other => other,
    }]
}

/// A timed-out request's `Unknown` naming its item, or the whole request as item 0 with no
/// client id, by a venue id the stub never sent.
fn vid_on_timeout(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item,
            outcome: outcome @ SubmitOutcome::Unknown,
        } => {
            let mut item = item.unwrap_or(ItemRef {
                idx: 0,
                cid: None,
                vid: None,
            });
            item.vid = toy::with_scope(|scope| scope.venue_order_id("toy-stranger")).ok();
            let item = Some(item);
            ExecEvent::Outcome { rpc, item, outcome }
        }
        other => other,
    }]
}

/// A client id of another engine's, minted under a lease of its own.
fn stranger() -> fbc_core::ClientOrderId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let k = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-stranger-{}-{k}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ns = fbc_core::Namespace::new(7);
    let lease = fbc_core::NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(9), ns);
    let start = fbc_core::WallNs(0);
    let cid = fbc_core::CidMint::new(lease.unwrap(), 0, 0, start)
        .mint()
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    cid
}

/// Every refusal naming item 1, with no venue code, as `Other`.
fn misread_rejects(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item,
            outcome: SubmitOutcome::Rejected(mut reject),
        } => {
            reject.kind = fbc_core::RejectKind::Other;
            reject.venue_code = None;
            let item = item.map(|it| ItemRef { idx: 1, ..it });
            let outcome = SubmitOutcome::Rejected(reject);
            ExecEvent::Outcome { rpc, item, outcome }
        }
        other => other,
    }]
}

/// A placement's acceptance, naming its venue id, provisional and then final.
fn provisional_then_final(ev: ExecEvent) -> Vec<ExecEvent> {
    if !acceptance(&ev, true) {
        return vec![ev];
    }
    let mut out = provisional(ev.clone());
    out.push(ev);
    out
}

/// A placement's acceptance, naming its venue id, provisional and then refused by the venue's
/// risk check, naming the order by that venue id.
fn provisional_then_risk_reject(ev: ExecEvent) -> Vec<ExecEvent> {
    let ExecEvent::Outcome {
        item: Some(ItemRef { vid: Some(vid), .. }),
        outcome: SubmitOutcome::Accepted { .. },
        ..
    } = &ev
    else {
        return vec![ev];
    };
    let reject = ExecEvent::AsyncReject {
        target: fbc_core::OrderRef::Venue(vid.clone()),
        op: OpKind::Place,
        reject: fbc_core::Reject {
            kind: fbc_core::RejectKind::Margin,
            venue_code: Some("1005".into()),
            raw: "".into(),
        },
    };
    let mut out = provisional(ev);
    out.push(reject);
    out
}

/// A placement's acceptance, naming its venue id, provisional, then final, then refused by the
/// venue's risk check.
fn final_and_risk_reject(ev: ExecEvent) -> Vec<ExecEvent> {
    let mut out = provisional_then_risk_reject(ev.clone());
    if out.len() > 1 {
        out.insert(1, ev);
    }
    out
}

/// As [`provisional_then_risk_reject`], the reject naming a stranger's client id beside the
/// order's venue id.
fn risk_reject_naming_two_orders(ev: ExecEvent) -> Vec<ExecEvent> {
    let mut out = provisional_then_risk_reject(ev);
    if let Some(ExecEvent::AsyncReject { target, .. }) = out.last_mut()
        && let Some(vid) = target.venue().cloned()
    {
        *target = fbc_core::OrderRef::Both(stranger(), vid);
    }
    out
}

/// A placement's acceptance, naming its venue id, preceded by the venue's risk check refusing
/// it, and reported provisional.
fn risk_reject_first(ev: ExecEvent) -> Vec<ExecEvent> {
    let mut out = provisional_then_risk_reject(ev);
    out.reverse();
    out
}

/// Every refusal's item naming a stranger's client id.
fn stranger_on_reject(ev: ExecEvent) -> Vec<ExecEvent> {
    vec![match ev {
        ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome: outcome @ SubmitOutcome::Rejected(_),
        } => {
            let cid = Some(stranger());
            let item = Some(ItemRef { cid, ..item });
            ExecEvent::Outcome { rpc, item, outcome }
        }
        other => other,
    }]
}

/// A placement's acceptance, naming its venue id, followed by a stray resync end.
fn stray_resync_end(ev: ExecEvent) -> Vec<ExecEvent> {
    if acceptance(&ev, true) {
        vec![ev, ExecEvent::ResyncEnd]
    } else {
        vec![ev]
    }
}

/// The frame a stub sends, after its answer, for [`Twist::EagerTimeout`].
const EARLY: &str = "early";

impl ExecCodec for Twisted {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens += 1;
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
            Twist::MaintainsAt70Ms => fx.push(Effect::Timer {
                tag: MAINTAIN,
                after: Duration::from_millis(70),
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
        self.rpc = Some(rpc);
        for effect in mine.take() {
            let placing = matches!(cmd, VenueCommand::Place(_) | VenueCommand::PlaceBatch(_));
            if let Effect::Send { frame, .. } = &effect
                && placing
            {
                self.last = Some(frame.bytes().to_vec());
            }
            if let Effect::Send { frame, .. } = &effect
                && matches!(cmd, VenueCommand::Amend(_))
            {
                self.amend = Some(frame.bytes().to_vec());
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
        if self.twist == Twist::EagerTimeout && f.bytes() == EARLY.as_bytes() {
            if let Some(rpc) = self.rpc {
                self.inner.on_rpc_timeout(rpc, sink);
                self.early = Some(rpc);
            }
            return Ok(());
        }
        let f_ = match self.twist {
            Twist::NoNewVid => no_new_vid,
            Twist::VidOnUpdate => vid_on_update,
            Twist::StrangerAmended => stranger_amended,
            Twist::PlacedAsItemOne => placed_as_item_one,
            Twist::SameVid => same_vid,
            Twist::EarlyAmended => early_amended,
            Twist::WholeAcceptance => whole_acceptance,
            Twist::DoubledAck => doubled_ack,
            Twist::DoubledPlacementAck => doubled_placement_ack,
            Twist::ProvisionalPlacement => provisional_placement,
            Twist::RefusesAmendToo => refuses_amend_too,
            Twist::CancelsOnAmend => cancels_on_amend,
            Twist::FilledOnAmend => filled_on_amend,
            Twist::AmendedTwice => amended_twice,
            Twist::PostOnlyOnAmend => post_only_on_amend,
            Twist::NoFlagsOnAmend => no_flags_on_amend,
            Twist::NoCidOnAmend => no_cid_on_amend,
            Twist::WrongPx => wrong_px,
            Twist::OpenAfterAmend => open_after_amend,
            Twist::OpenAgreesAfterAmend => open_agrees_after_amend,
            Twist::FillsOnAmend => fills_on_amend,
            Twist::FillsOnPlacement => fills_on_placement,
            Twist::RenamedTwice => renamed_twice,
            Twist::StrangerVidOnAck => stranger_vid_on_ack,
            Twist::MisreadsRejects => misread_rejects,
            Twist::ProvisionalThenFinal => provisional_then_final,
            Twist::ProvisionalThenRiskReject => provisional_then_risk_reject,
            Twist::FinalAndRiskReject => final_and_risk_reject,
            Twist::RiskRejectFirst => risk_reject_first,
            Twist::RiskRejectNamingTwoOrders => risk_reject_naming_two_orders,
            Twist::StrangerOnReject => stranger_on_reject,
            Twist::StrayResyncEnd => stray_resync_end,
            _ => kept,
        };
        // The toy's resync ends with this frame.
        let resync_ended = f.bytes() == b"rsend";
        let resync_frame = f.bytes().starts_with(b"rs");
        let strays = matches!(
            self.twist,
            Twist::StrayResyncEnd | Twist::StrayResyncEndOnReconnect
        );
        if strays && self.opens > 1 && resync_frame {
            return Ok(());
        }
        if self.twist == Twist::StrayResyncEndOnReconnect
            && self.opens > 1
            && f.bytes().starts_with(b"auth|")
        {
            self.inner.on_frame(stream, f, scope, specs, sink, fx)?;
            sink.push(VenueMeta::NONE, ExecEvent::ResyncEnd);
            return Ok(());
        }
        let sink = &mut Rewrite { inner: sink, f: f_ };
        let decoded = self.inner.on_frame(stream, f, scope, specs, sink, fx);
        if self.twist == Twist::ReplacesAfterResync
            && resync_ended
            && self.opens > 1
            && let Some(frame) = self.last.take()
        {
            fx.push(again(&frame));
        }
        if self.twist == Twist::ResendsAmend
            && let Some(frame) = self.amend.take()
        {
            fx.push(again(&frame));
        }
        decoded
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
        if tag == MAINTAIN {
            return fx.push(Effect::Send {
                stream: EXEC_STREAM,
                frame: WireSlice::plain(b"maintain".to_vec()),
                rpc: None,
                class: TrafficClass::Normal,
                charge: RateCharge::one(OpKind::Control, None),
            });
        }
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
        if self.early == Some(rpc) {
            return;
        }
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
            Twist::ProvisionalOnTimeout => provisional,
            Twist::StrangerOnTimeout => stranger_on_timeout,
            Twist::VidOnTimeout => vid_on_timeout,
            _ => return self.inner.on_rpc_timeout(rpc, sink),
        };
        self.inner
            .on_rpc_timeout(rpc, &mut Rewrite { inner: sink, f });
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.resyncs += 1;
        if self.twist == Twist::ResyncsOnce && self.resyncs > 1 {
            return;
        }
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

/// The toy's setup whose stub sends, after its answer to each request, a frame `early`.
fn early() -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let mut frames = inner.reply(frame, answers)?;
        frames.push(Frame::Text(EARLY.into()));
        Ok(frames)
    });
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup on an instrument whose orders are 6 to `max` lots, its stub rejecting an
/// amend to more.
fn sized(max: i64) -> Setup {
    let mut specs = toy::specs();
    let mut a = specs.get(toy::INST_A).unwrap().clone();
    a.min_size = fbc_core::Lots::new(6).unwrap();
    a.max_order_size = Some(fbc_core::Lots::new(max).unwrap());
    specs.insert(a);
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let over = |t: &str| {
            let qty = t.split('|').find_map(|kv| kv.strip_prefix("qty="));
            qty.and_then(|q| q.parse::<i64>().ok())
                .is_some_and(|q| q > max)
        };
        let amend = matches!(frame, Frame::Text(t) if t.starts_with("amend|") && over(t));
        let rejects = vec![suite::Answer::Reject; answers.len()];
        inner.reply(frame, if amend { &rejects } else { answers })
    });
    Setup {
        specs,
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
        suite::resync_after_reconnect,
        suite::two_phase_ack,
        suite::reject_coverage,
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
        suite::resync_after_reconnect,
        suite::two_phase_ack,
        suite::reject_coverage,
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
    // The account's bucket, the instrument's and the connection's (Codex r4223629855).
    for variant in [
        &ONE_ORDER_A_SECOND,
        &ONE_ORDER_A_SECOND_A_PAIR,
        &ONE_ORDER_A_SECOND_A_CONNECTION,
    ] {
        let passed = suite::amend_ack(&variant.subject(assumed));
        assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
    }
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

#[test]
fn mixed_batch_takes_a_provisional_acceptance_only_on_a_two_phase_venue() {
    let passed = suite::mixed_batch(&TWO_PHASE.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
    let failure = failed(suite::mixed_batch(&PROVISIONAL_ON_TIMEOUT.subject(assumed)));
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
fn amend_ack_fails_a_toy_that_reports_the_amends_acceptance_twice() {
    let failure = failed(suite::amend_ack(&DOUBLED_AMEND_ACK.subject(assumed)));
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
fn amend_ack_fails_a_toy_whose_amended_update_states_another_price() {
    let failure = failed(suite::amend_ack(&WRONG_PX.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "contradicts the amend"),
        "{failure}"
    );
}

#[test]
fn mixed_batch_fails_a_toy_that_reports_the_unanswered_item_unknown_before_its_deadline() {
    let failure = failed(suite::mixed_batch(&EAGER_TIMEOUT.subject(early)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "item 2, which the stub never answered, was reported before its deadline"
        ),
        "{failure}"
    );
    // Without the frame that brings it early, the same codec passes.
    let passed = suite::mixed_batch(&EAGER_TIMEOUT.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn unknown_on_timeout_fails_a_toy_that_reports_the_placement_unknown_before_its_deadline() {
    let failure = failed(suite::unknown_on_timeout(&EAGER_TIMEOUT.subject(early)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "before its deadline, before the clock moved"
        ),
        "{failure}"
    );
}

#[test]
fn unknown_on_timeout_fails_a_toy_whose_unknown_names_another_orders_client_id() {
    let failure = failed(suite::unknown_on_timeout(
        &STRANGER_ON_TIMEOUT.subject(assumed),
    ));
    assert!(
        says(&failure, "ExecCodec::on_rpc_timeout", "not Unknown once"),
        "{failure}"
    );
}

#[test]
fn mixed_batch_fails_a_toy_whose_unknown_item_names_a_venue_id() {
    let failure = failed(suite::mixed_batch(&VID_ON_TIMEOUT.subject(assumed)));
    assert!(
        says(
            &failure,
            "ItemRef.vid",
            "item 2, which the stub never answered"
        ),
        "{failure}"
    );
}

#[test]
fn unknown_on_timeout_fails_a_toy_whose_unknown_names_a_venue_id() {
    let failure = failed(suite::unknown_on_timeout(&VID_ON_TIMEOUT.subject(assumed)));
    assert!(
        says(&failure, "ExecCodec::on_rpc_timeout", "not Unknown once"),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_update_names_the_order_with_a_stranger_client_id() {
    let failure = failed(suite::amend_ack(&STRANGER_AMENDED.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "names the amended order by"),
        "{failure}"
    );
}

#[test]
fn amend_ack_learns_the_venue_id_from_the_placements_order_update() {
    let passed = suite::amend_ack(&VID_ON_UPDATE.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn amend_ack_amends_the_quantity_within_the_instruments_largest_order() {
    let passed = suite::amend_ack(&QTY_ONLY.subject(|| sized(10)));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
    // No other size fits: nothing to amend to.
    let skipped = suite::amend_ack(&QTY_ONLY.subject(|| sized(6)));
    assert!(
        matches!(skipped, Ok(Verdict::Skipped { why, .. }) if why.contains("max_order_size")),
        "{skipped:?}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_placement_acceptance_names_another_item() {
    let failure = failed(suite::amend_ack(&PLACED_AS_ITEM_ONE.subject(assumed)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "the placement the stub accepted was reported"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_declaring_its_amend_keeps_the_venue_id_that_names_a_new_one() {
    let failure = failed(suite::amend_ack(&KEEPS_VID.subject(assumed)));
    assert!(
        says(
            &failure,
            "AmendCaps.keeps_venue_id is true",
            "names a new venue id"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_placement_acceptance_comes_twice_or_provisional_on_a_single_phase_venue()
 {
    for variant in [&DOUBLED_PLACEMENT_ACK, &PROVISIONAL_PLACEMENT] {
        let failure = failed(suite::amend_ack(&variant.subject(assumed)));
        assert!(
            says(
                &failure,
                "ExecCodec::on_frame",
                "the placement the stub accepted was reported"
            ),
            "{failure}"
        );
    }
}

#[test]
fn amend_ack_fails_a_toy_that_reports_the_amend_it_reports_amended_refused_too() {
    let failure = failed(suite::amend_ack(&REFUSES_AMEND_TOO.subject(assumed)));
    assert!(
        says(
            &failure,
            "ExecEvent::AsyncReject",
            "the amend the stub accepted"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_that_reports_the_amended_order_canceled() {
    let failure = failed(suite::amend_ack(&CANCELS_ON_AMEND.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "ended the order"),
        "{failure}"
    );
}

#[test]
fn a_limit_the_opening_frames_never_charge_is_not_waited_out_before_the_first_placement() {
    for check in [suite::amend_ack, suite::unknown_on_timeout] {
        let passed = check(&ONE_PLACEMENT_A_MINUTE.subject(assumed));
        assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
    }
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_update_states_a_fill_the_stub_never_made() {
    let failure = failed(suite::amend_ack(&FILLED_ON_AMEND.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "contradicts the amend"),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_second_amended_update_states_another_price() {
    let failure = failed(suite::amend_ack(&AMENDED_TWICE.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "contradicts the amend"),
        "{failure}"
    );
}

#[test]
fn amend_ack_skips_a_venue_whose_caps_allow_no_limit_order() {
    let skipped = suite::amend_ack(&MARKET_ONLY.subject(assumed));
    assert!(
        matches!(skipped, Ok(Verdict::Skipped { why, .. }) if why.contains("no limit order")),
        "{skipped:?}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_update_misstates_or_omits_the_flags_it_echoes() {
    for variant in [&POST_ONLY_ON_AMEND, &NO_FLAGS_ON_AMEND] {
        let failure = failed(suite::amend_ack(&variant.subject(assumed)));
        assert!(
            says(&failure, "OrderUpdate", "contradicts the amend"),
            "{failure}"
        );
    }
}

#[test]
fn a_limit_is_waited_out_by_its_window_and_no_more() {
    let passed = suite::amend_ack(&FIFTY_MS_WINDOW.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn amend_ack_fails_a_toy_whose_amended_update_omits_the_client_id_it_echoes() {
    let failure = failed(suite::amend_ack(&NO_CID_ON_AMEND.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "names the amended order by"),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_that_writes_the_amend_again_once_answered() {
    let failure = failed(suite::amend_ack(&RESENDS_AMEND.subject(assumed)));
    assert!(
        says(
            &failure,
            "ExecCodec: never resent",
            "the check never asked for"
        ),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_open_update_after_the_amend_states_another_price() {
    let failure = failed(suite::amend_ack(&OPEN_AFTER_AMEND.subject(assumed)));
    assert!(
        says(&failure, "OrderUpdate", "contradicts the amend"),
        "{failure}"
    );
}

#[test]
fn amend_ack_passes_a_toy_whose_open_update_after_the_amend_states_what_it_asked_for() {
    let passed = suite::amend_ack(&OPEN_AGREES_AFTER_AMEND.subject(assumed));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn amend_ack_fails_a_toy_that_reports_a_fill_of_the_order_it_amends() {
    let failure = failed(suite::amend_ack(&FILLS_ON_AMEND.subject(assumed)));
    assert!(
        says(&failure, "ExecEvent::Fill", "the stub filled nothing"),
        "{failure}"
    );
}

#[test]
fn amend_ack_fails_a_toy_that_reports_a_fill_of_the_order_it_places() {
    let failure = failed(suite::amend_ack(&FILLS_ON_PLACEMENT.subject(assumed)));
    assert!(
        says(&failure, "ExecEvent::Fill", "the stub filled nothing"),
        "{failure}"
    );
}

#[test]
fn the_order_entry_checks_skip_a_venue_taking_no_orders_whose_setup_lists_no_instrument() {
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let skipped = check(&NO_EXEC.subject(no_instruments));
        let why = "VenueCaps.exec is None: the venue takes no orders";
        assert!(
            matches!(skipped, Ok(Verdict::Skipped { why: w, .. }) if w == why),
            "{skipped:?}"
        );
    }
}

#[test]
fn amend_ack_fails_a_toy_reporting_two_new_venue_ids_for_one_amend() {
    let failure = failed(suite::amend_ack(&RENAMED_TWICE.subject(assumed)));
    assert!(
        says(
            &failure,
            "AmendCaps.keeps_venue_id is false",
            "new venue ids"
        ),
        "{failure}"
    );
}

/// The toy's setup with its first instrument's grid allowing no price but the harness's, one
/// tick up overflowing and none below: a band whose step is 2^62 ticks starting at tick 1, then
/// one starting at 2^62 + 2 whose first multiple, 2^63 + 2 ticks, fits no `i64`.
fn one_price() -> Setup {
    let mut setup = assumed();
    let mut spec = setup.specs.get(toy::INST_A).unwrap().clone();
    let f = |ticks: i128| rust_decimal::Decimal::from_i128_with_scale(ticks, 18);
    let bands = [(f(1), f(1 << 62)), (f((1 << 62) + 2), f((1 << 62) + 1))];
    spec.price_grid = fbc_core::PriceGrid::banded(&bands).unwrap();
    setup.specs.insert(spec);
    setup
}

#[test]
fn amend_ack_amends_the_quantity_where_no_other_price_is_valid() {
    let passed = suite::amend_ack(&ToyFactory.subject_for(one_price));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn amend_ack_skips_a_venue_amending_prices_only_where_no_other_price_is_valid() {
    let skipped = suite::amend_ack(&PRICE_ONLY.subject(one_price));
    let why = "no valid price lies one step from the harness's";
    assert!(
        matches!(skipped, Ok(Verdict::Skipped { why: w, .. }) if w == why),
        "{skipped:?}"
    );
}

#[test]
fn amend_ack_fails_a_toy_whose_amend_acceptance_names_a_strangers_venue_id() {
    let failure = failed(suite::amend_ack(&STRANGER_VID_ON_ACK.subject(assumed)));
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
fn amend_ack_skips_a_venue_whose_limit_orders_cannot_rest() {
    let skipped = suite::amend_ack(&IOC_LIMITS.subject(assumed));
    let why = "OrderCaps allows no limit order good till cancelled to amend";
    assert!(
        matches!(skipped, Ok(Verdict::Skipped { why: w, .. }) if w == why),
        "{skipped:?}"
    );
}

/// The toy's setup whose stub answers nothing it is asked.
fn silent() -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let silent = vec![suite::Answer::Silent; answers.len()];
        inner.reply(frame, &silent)
    });
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

#[test]
fn resync_after_reconnect_fails_a_toy_that_re_places_an_open_order_after_a_reconnect() {
    let failure = failed(suite::resync_after_reconnect(
        &REPLACES_AFTER_RESYNC.subject(assumed),
    ));
    assert_eq!(failure.check, "resync_after_reconnect");
    assert!(
        says(
            &failure,
            "ExecCodec: nothing re-placed",
            "written 2 times over the 2 connections"
        ),
        "{failure}"
    );
    // The new connection's opening was answered, and its resync ended: only the re-placement
    // breaks the check.
    assert_eq!(failure.breaches.len(), 1, "{failure}");
}

#[test]
fn resync_after_reconnect_fails_a_toy_whose_reconnect_leads_to_no_resync() {
    let failure = failed(suite::resync_after_reconnect(
        &RESYNCS_ONCE.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "ExecCodec::resync",
            "no resync ended once the stub closed the connection"
        ),
        "{failure}"
    );
    // The stub still waits for the new connection's resync request.
    assert!(
        says(&failure, "OrderEntryStub", "did not play to its end"),
        "{failure}"
    );
}

#[test]
fn resync_after_reconnect_fails_when_the_placement_the_stub_accepted_comes_back_rejected() {
    let failure = failed(suite::resync_after_reconnect(
        &ToyFactory.subject_for(rejecting),
    ));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "the placement the stub accepted was reported"
        ),
        "{failure}"
    );
}

#[test]
fn two_phase_ack_fails_a_single_phase_toy_reporting_a_provisional_acceptance() {
    let failure = failed(suite::two_phase_ack(
        &PROVISIONAL_PLACEMENT.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "AckModel is SinglePhase",
            "provisional acceptances were reported"
        ),
        "{failure}"
    );
    assert!(
        says(
            &failure,
            "OrderCaps.ack",
            "not accepted once as SinglePhase"
        ),
        "{failure}"
    );
}

/// What `two_phase_ack` probed on `variant`, which must pass.
fn two_phase_probed(variant: &'static Variant) -> Vec<String> {
    match suite::two_phase_ack(&variant.subject(assumed)) {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("{other:?}"),
    }
}

#[test]
fn two_phase_ack_passes_a_two_phase_toy_whose_provisional_acceptance_is_resolved_in_the_window() {
    let model = "AckModel is TwoPhase { risk_reject_window: 1s }";
    assert_eq!(
        two_phase_probed(&TWO_PHASE_FINAL),
        [format!(
            "{model}: a provisional acceptance was followed by a final acceptance within the \
             window"
        )]
    );
    assert_eq!(
        two_phase_probed(&TWO_PHASE_RISK_REJECT),
        [format!(
            "{model}: a provisional acceptance was followed by an asynchronous reject within \
             the window"
        )]
    );
    assert_eq!(
        two_phase_probed(&TWO_PHASE_AT_ONCE),
        [format!("{model}: the placement was accepted final at once")]
    );
}

#[test]
fn two_phase_ack_fails_a_two_phase_toy_whose_provisional_acceptance_is_never_resolved() {
    let failure = failed(suite::two_phase_ack(&TWO_PHASE_DANGLING.subject(assumed)));
    assert!(
        says(
            &failure,
            "AckModel is TwoPhase { risk_reject_window: 1s }",
            "followed by neither a final acceptance nor an asynchronous reject within the window"
        ),
        "{failure}"
    );
    // A provisional acceptance alone is an acceptance once as the two-phase model has it.
    assert_eq!(failure.breaches.len(), 1, "{failure}");
}

#[test]
fn two_phase_ack_fails_a_two_phase_toy_that_both_accepts_and_rejects_after_a_provisional_acceptance()
 {
    let failure = failed(suite::two_phase_ack(&TWO_PHASE_BOTH.subject(assumed)));
    assert!(
        says(
            &failure,
            "AckModel is TwoPhase { risk_reject_window: 1s }",
            "followed by both a final acceptance and an asynchronous reject"
        ),
        "{failure}"
    );
}

#[test]
fn reject_coverage_fails_a_toy_that_misreads_every_refusal() {
    let failure = failed(suite::reject_coverage(&MISREADS_REJECTS.subject(assumed)));
    let code = "reject_coverage/table.txt: 1001";
    assert!(
        says(
            &failure,
            code,
            "the code maps to Other; the table says PostOnlyWouldCross"
        ),
        "{failure}"
    );
    assert!(
        says(&failure, code, "does not carry the code the stub sent"),
        "{failure}"
    );
    assert!(
        says(&failure, code, "not the placement's one item"),
        "{failure}"
    );
    // The code the table maps to Other maps to Other: only its code and item are wrong.
    let other = "reject_coverage/table.txt: 9999";
    assert!(!says(&failure, other, "maps to"), "{failure}");
    assert!(failure.names(other), "{failure}");
}

#[test]
fn reject_coverage_fails_when_no_placement_is_refused() {
    let failure = failed(suite::reject_coverage(&ToyFactory.subject_for(silent)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "refused under code 1001 was reported [], not Rejected once"
        ),
        "{failure}"
    );
}

/// A fixture directory of the test's own holding `table` as the reject table, or none.
fn fixtures_with(name: &str, table: Option<&str>) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fbc-conformance-rejects-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("reject_coverage")).unwrap();
    if let Some(table) = table {
        std::fs::write(dir.join("reject_coverage/table.txt"), table).unwrap();
    }
    dir
}

#[test]
fn reject_coverage_fails_a_table_that_is_missing_lists_no_code_or_names_no_kind() {
    let failure_with = |name, table| {
        let dir = fixtures_with(name, table);
        let subject = Subject::new(&ToyFactory, &dir, assumed).unwrap();
        let failure = failed(suite::reject_coverage(&subject));
        let _ = std::fs::remove_dir_all(&dir);
        failure
    };
    let file = "reject_coverage/table.txt";
    let missing = failure_with("missing", None);
    assert!(says(&missing, file, "cannot read"), "{missing}");
    let empty = failure_with("empty", Some("# no code\n\n"));
    assert!(says(&empty, file, "lists no code"), "{empty}");
    let kindless = failure_with("kindless", Some("1001 InvalidPrice\n1002\n"));
    assert!(
        says(&kindless, file, "the line \"1002\" names no kind"),
        "{kindless}"
    );
}

#[test]
fn two_phase_ack_fails_a_two_phase_toy_whose_risk_reject_comes_before_its_provisional_acceptance() {
    let failure = failed(suite::two_phase_ack(
        &TWO_PHASE_REJECT_FIRST.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "AckModel is TwoPhase { risk_reject_window: 1s }",
            "followed by neither a final acceptance nor an asynchronous reject"
        ),
        "{failure}"
    );
}

#[test]
fn reject_coverage_fails_a_toy_whose_refusals_name_a_strangers_client_id() {
    let failure = failed(suite::reject_coverage(&STRANGER_ON_REJECT.subject(assumed)));
    assert!(
        says(
            &failure,
            "reject_coverage/table.txt: 1001",
            "not the placement's one item"
        ),
        "{failure}"
    );
}

#[test]
fn resync_after_reconnect_takes_no_resync_end_of_the_first_connection_for_the_reconnects() {
    let failure = failed(suite::resync_after_reconnect(
        &STRAY_RESYNC_END.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "ExecCodec::resync",
            "no resync ended once the stub closed the connection"
        ),
        "{failure}"
    );
}

#[test]
fn resync_after_reconnect_takes_no_stray_resync_end_on_the_new_connection_for_a_resync() {
    let failure = failed(suite::resync_after_reconnect(
        &STRAY_RESYNC_END_ON_RECONNECT.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "ExecCodec::resync",
            "no resync ended once the stub closed the connection"
        ),
        "{failure}"
    );
}

#[test]
fn two_phase_ack_and_resync_after_reconnect_fail_a_placement_acceptance_naming_item_one() {
    let failure = failed(suite::two_phase_ack(&PLACED_AS_ITEM_ONE.subject(assumed)));
    assert!(
        says(&failure, "OrderCaps.ack", "naming its one item"),
        "{failure}"
    );
    let failure = failed(suite::resync_after_reconnect(
        &PLACED_AS_ITEM_ONE.subject(assumed),
    ));
    assert!(
        says(&failure, "ExecCodec::on_frame", "naming its one item"),
        "{failure}"
    );
}

#[test]
fn two_phase_ack_takes_no_risk_reject_naming_another_order_beside_the_placement() {
    let failure = failed(suite::two_phase_ack(
        &TWO_PHASE_REJECT_TWO_ORDERS.subject(assumed),
    ));
    assert!(
        says(
            &failure,
            "AckModel is TwoPhase { risk_reject_window: 1s }",
            "followed by neither a final acceptance nor an asynchronous reject"
        ),
        "{failure}"
    );
}
