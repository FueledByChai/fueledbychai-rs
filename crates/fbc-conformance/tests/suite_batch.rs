//! FBC-3pv6's done line: `mixed_batch` judges a batch's refused and unanswered items as the
//! setup's [`OrderEntryStub`](suite::OrderEntryStub) declares its venue answers them
//! ([`BatchFailures`]). A toy variant answering as decision 0069 has Paradex answer (a refused
//! item's error `Unknown`, an item its reply leaves out `Unknown` in that reply) passes,
//! declared so by its setup; the toy reporting every item of a timed-out batch `Accepted` still
//! fails (`tests/suite_orders.rs`); and a variant breaking what its setup declares fails, each
//! way round and one declaration at a time.

mod toy_setup;

use std::collections::{BTreeSet, HashMap};

use fbc_conformance::Frame;
use fbc_conformance::suite::{
    self, BatchFailures, Failure, RefusedItem, Replier, Setup, Subject, UnansweredItem, Verdict,
};
use fbc_conformance::toy::ToyFactory;
use fbc_core::{
    AccountSummary, AckLevel, AssetKey, ConfigError, CtxCall, DecodeError, DecodeScope, Effects,
    EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint, ExecEvent, ExecSink,
    FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentSpecDraft, MdCodec, NotSentReason, PathStamps, RawFrame, RpcId, Secrets, SpecTable,
    StreamId, SubmitOutcome, Subscription, SymbolError, TimerTag, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, VenueMeta,
};
use toy_setup::{FIXTURES, assumed, order_entry};

/// How a variant's codec reads a batch's reply, which its setup's stub sends as one frame.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Rest {
    /// The items the reply leaves out wait for the request's deadline, as the toy's do.
    AtDeadline,
    /// The items the reply leaves out are reported `Unknown` with the rest of it, in the same
    /// call, as decision 0069 has Paradex's codec report them.
    UnknownInReply,
    /// The items the reply leaves out are reported `Accepted` with the rest of it.
    AcceptedInReply,
}

/// The toy, its codec reading a batch's reply whole: a refused item `Unknown` where
/// `refused_unknown` (Paradex's per-item error, a message with no code), the items the reply
/// leaves out as `rest` says.
struct PerReply {
    refused_unknown: bool,
    rest: Rest,
}

/// Decision 0069's Paradex: a refused item `Unknown`, an item left out `Unknown` in the reply.
static PARADEX_LIKE: PerReply = PerReply {
    refused_unknown: true,
    rest: Rest::UnknownInReply,
};
/// A refused item `Unknown`, an item left out `Unknown` at the deadline.
static UNKNOWN_REFUSAL: PerReply = PerReply {
    refused_unknown: true,
    rest: Rest::AtDeadline,
};
/// A refused item `Rejected`, an item left out `Unknown` in the reply.
static REST_IN_REPLY: PerReply = PerReply {
    refused_unknown: false,
    rest: Rest::UnknownInReply,
};
/// As [`PARADEX_LIKE`], but an item the reply leaves out is reported `Accepted`.
static ACCEPTS_THE_REST: PerReply = PerReply {
    refused_unknown: true,
    rest: Rest::AcceptedInReply,
};

impl PerReply {
    fn subject(&'static self, setup: fn() -> Setup) -> Subject<'static> {
        Subject::new(self, FIXTURES, setup).unwrap()
    }
}

impl VenueFactory for PerReply {
    fn id(&self) -> &'static str {
        "TOY-PER-REPLY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        ToyFactory.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        ToyFactory.caps(cfg)
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
        let (refused_unknown, rest) = (self.refused_unknown, self.rest);
        let codec = ToyFactory.exec_codec(cfg, creds)?;
        Some(codec.map(|inner| {
            let batches = HashMap::new();
            let codec = WholeReply {
                inner,
                refused_unknown,
                rest,
                batches,
            };
            Box::new(codec) as Box<dyn ExecCodec>
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

/// The toy's codec reading a batch's reply, the toy's item records one per line of one frame,
/// whole.
struct WholeReply {
    inner: Box<dyn ExecCodec>,
    refused_unknown: bool,
    rest: Rest,
    /// Each batch sent and not yet answered: its item count.
    batches: HashMap<RpcId, usize>,
}

/// A sink handing `inner` each event as `f` rewrites it.
struct Map<'a> {
    inner: &'a mut dyn ExecSink,
    f: &'a dyn Fn(ExecEvent) -> ExecEvent,
}

impl ExecSink for Map<'_> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.inner.push(meta, (self.f)(ev));
    }
}

/// An item's outcome `from` reported as `to`.
fn item_outcome(ev: ExecEvent, from: fn(&SubmitOutcome) -> bool, to: SubmitOutcome) -> ExecEvent {
    match ev {
        ExecEvent::Outcome {
            rpc,
            item: item @ Some(_),
            outcome,
        } if from(&outcome) => ExecEvent::Outcome {
            rpc,
            item,
            outcome: to,
        },
        other => other,
    }
}

/// The value of `key` in a `kind|key=value|...` record.
fn field<'a>(record: &'a str, key: &str) -> Option<&'a str> {
    let kv = record
        .split('|')
        .skip(1)
        .filter_map(|kv| kv.split_once('='));
    kv.into_iter().find_map(|(k, v)| (k == key).then_some(v))
}

impl ExecCodec for WholeReply {
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
        let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, fx)?;
        if let VenueCommand::PlaceBatch(orders) = cmd {
            self.batches.insert(rpc, orders.len());
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
        let RawFrame::Text(text) = f else {
            return self.inner.on_frame(stream, f, scope, specs, sink, fx);
        };
        let rpc = text
            .starts_with("item|")
            .then(|| field(text, "rpc")?.parse().ok().map(RpcId))
            .flatten();
        let Some((rpc, items)) = rpc.and_then(|rpc| Some((rpc, self.batches.remove(&rpc)?))) else {
            return self.inner.on_frame(stream, f, scope, specs, sink, fx);
        };
        let refused_unknown = self.refused_unknown;
        let rest = self.rest;
        // The items left out first, which the toy reports `Unknown`, then a refused one.
        let rewrite = move |ev| {
            let unknown = |o: &SubmitOutcome| *o == SubmitOutcome::Unknown;
            let ev = match rest {
                Rest::AcceptedInReply => item_outcome(ev, unknown, accepted()),
                _ => ev,
            };
            let refused = |o: &SubmitOutcome| matches!(o, SubmitOutcome::Rejected(_));
            if refused_unknown {
                item_outcome(ev, refused, SubmitOutcome::Unknown)
            } else {
                ev
            }
        };
        let sink = &mut Map {
            inner: sink,
            f: &rewrite,
        };
        let lines: Vec<&str> = text.lines().collect();
        for line in &lines {
            self.inner
                .on_frame(stream, RawFrame::Text(line), scope, specs, sink, fx)?;
        }
        // The items the reply left out: the toy holds those it answered until the deadline,
        // which reports them with the rest `Unknown`.
        if lines.len() < items && rest != Rest::AtDeadline {
            self.inner.on_rpc_timeout(rpc, sink);
        }
        Ok(())
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
        let refused_unknown = self.refused_unknown;
        let rewrite = move |ev| {
            let refused = |o: &SubmitOutcome| matches!(o, SubmitOutcome::Rejected(_));
            if refused_unknown {
                item_outcome(ev, refused, SubmitOutcome::Unknown)
            } else {
                ev
            }
        };
        self.batches.remove(&rpc);
        let sink = &mut Map {
            inner: sink,
            f: &rewrite,
        };
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// The toy's setup declaring `batch`, its stub sending a batch's reply, the toy's item records
/// one per line, as one frame, as Paradex's `order.create_batch` answers in one `results` list.
fn whole_replies(batch: BatchFailures) -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let frames = inner.reply(frame, answers)?;
        let batch = matches!(frame, Frame::Text(t) if t.starts_with("batch|"));
        if !batch || frames.is_empty() {
            return Ok(frames);
        }
        let text = |f: &Frame| match f {
            Frame::Text(t) => Ok(t.clone()),
            Frame::Binary(_) => Err("a binary frame".to_owned()),
        };
        let lines = frames.iter().map(text).collect::<Result<Vec<_>, _>>()?;
        Ok(vec![Frame::Text(lines.join("\n"))])
    });
    stub.batch = batch;
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// Decision 0069's Paradex declaration.
const PARADEX: BatchFailures = BatchFailures {
    refused: RefusedItem::Unknown,
    unanswered: UnansweredItem::InReply,
};
/// The toy's declaration.
const TOY: BatchFailures = BatchFailures {
    refused: RefusedItem::Rejected,
    unanswered: UnansweredItem::AtDeadline,
};

fn paradex() -> Setup {
    whole_replies(PARADEX)
}

fn toy() -> Setup {
    whole_replies(TOY)
}

fn refused_unknown_at_deadline() -> Setup {
    whole_replies(BatchFailures {
        refused: RefusedItem::Unknown,
        unanswered: UnansweredItem::AtDeadline,
    })
}

fn rejected_rest_in_reply() -> Setup {
    whole_replies(BatchFailures {
        refused: RefusedItem::Rejected,
        unanswered: UnansweredItem::InReply,
    })
}

/// The plain toy, its stub as the toy's, declaring Paradex's semantics.
fn toy_declaring_paradex() -> Setup {
    let mut stub = order_entry();
    stub.batch = PARADEX;
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    outcome.expect_err("the check failed")
}

fn passed(outcome: Result<Verdict, Failure>) -> Vec<String> {
    match outcome {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("expected a pass, got {other:?}"),
    }
}

/// Whether a breach of `capability` says `what`.
fn says(failure: &Failure, capability: &str, what: &str) -> bool {
    let found = failure.breaches.iter();
    let mut found = found.filter(|b| b.capability == capability);
    found.any(|b| b.what.contains(what))
}

#[test]
fn the_toys_own_setup_declares_a_refused_item_rejected_and_an_unanswered_one_at_the_deadline() {
    let stub = assumed()
        .order_entry
        .expect("the toy states its order entry");
    assert_eq!(stub.batch, TOY);
}

#[test]
fn mixed_batch_passes_a_toy_answering_as_paradex_does_declared_so_by_its_setup() {
    let probed = passed(suite::mixed_batch(&PARADEX_LIKE.subject(paradex)));
    let probed = probed.join("\n");
    assert!(probed.contains("an item rejected is Unknown"), "{probed}");
    assert!(
        probed.contains("an item unanswered is Unknown in the reply, once"),
        "{probed}"
    );
}

#[test]
fn mixed_batch_takes_each_declaration_on_its_own() {
    let probed = passed(suite::mixed_batch(
        &UNKNOWN_REFUSAL.subject(refused_unknown_at_deadline),
    ));
    assert!(
        probed.iter().any(|p| p.contains("Unknown at the deadline")),
        "{probed:?}"
    );
    let probed = passed(suite::mixed_batch(
        &REST_IN_REPLY.subject(rejected_rest_in_reply),
    ));
    assert!(
        probed
            .iter()
            .any(|p| p.contains("an item rejected is Rejected")),
        "{probed:?}"
    );
}

#[test]
fn mixed_batch_fails_the_toy_when_its_setup_declares_paradexs_semantics() {
    let toy = Subject::new(&ToyFactory, FIXTURES, toy_declaring_paradex).unwrap();
    let failure = failed(suite::mixed_batch(&toy));
    assert!(
        says(
            &failure,
            "OrderCaps.batch_place",
            "item 1, which the stub rejected, was reported [Rejected"
        ),
        "{failure}"
    );
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "item 2, which the stub's reply left out, was reported [] in the reply"
        ),
        "{failure}"
    );
    // The item the stub accepted stays accepted.
    assert!(!failure.to_string().contains("item 0"), "{failure}");
}

#[test]
fn mixed_batch_fails_a_paradex_like_toy_whose_setup_declares_the_toys_semantics() {
    let failure = failed(suite::mixed_batch(&PARADEX_LIKE.subject(toy)));
    assert!(
        says(
            &failure,
            "OrderCaps.batch_place",
            "item 1, which the stub rejected, was reported [Unknown], not Rejected once"
        ),
        "{failure}"
    );
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "item 2, which the stub never answered, was reported before its deadline"
        ),
        "{failure}"
    );
}

#[test]
fn mixed_batch_fails_each_declaration_broken_on_its_own() {
    // A refused item `Unknown` where the setup declares it `Rejected`, the rest as declared.
    let failure = failed(suite::mixed_batch(&UNKNOWN_REFUSAL.subject(toy)));
    assert!(
        says(
            &failure,
            "OrderCaps.batch_place",
            "item 1, which the stub rejected"
        ),
        "{failure}"
    );
    assert!(!failure.to_string().contains("item 2"), "{failure}");
    // An item left out reported in the reply where the setup declares it at the deadline.
    let failure = failed(suite::mixed_batch(&REST_IN_REPLY.subject(toy)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_rpc_timeout",
            "item 2, which the stub never answered, was reported before its deadline"
        ),
        "{failure}"
    );
    assert!(!failure.to_string().contains("item 1"), "{failure}");
}

#[test]
fn mixed_batch_fails_a_paradex_like_toy_reporting_the_item_its_reply_left_out_accepted() {
    let failure = failed(suite::mixed_batch(&ACCEPTS_THE_REST.subject(paradex)));
    assert!(
        says(
            &failure,
            "ExecCodec::on_frame",
            "item 2, which the stub's reply left out, was reported [Accepted"
        ),
        "{failure}"
    );
}
