//! FBC-6c9: an amend and a batch cancel name their orders only by the reference kinds the venue
//! declares for them (`AmendCaps::refs`, `CancelBatch::refs`), and a codec refuses, before any
//! byte is written, one whose only reference is undeclared.
//!
//! The codec below encodes amends, single cancels and batch cancels for the synthetic venue in
//! `tests/common/`, reading every declaration from its caps: amends and batch cancels name the
//! order by the venue's id only, while a single cancel also takes our client id. It picks each
//! reference with the library's `AmendOrder::reference` and `CancelOrder::reference`, which a
//! planner reads the same way. It decodes nothing; its protocol describes no real venue.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::synthetic_caps;
use fbc_core::{
    AmendOrder, CancelOrder, Channel, ChosenRef, CidMint, ClientOrderId, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec, ExecEvent, ExecSink,
    HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId, Lots, MonoNs,
    Namespace, NamespaceLease, NonceBlock, NotSentReason, OpKind, OrderCaps, OrderRef, PathStamps,
    RateCharge, RawFrame, RpcCall, RpcId, Side, SpecTable, StreamId, SubmitOutcome, Ticks, TifTag,
    TimerTag, VenueCommand, VenueMeta, VenueOrderId, WallNs, WireSlice, dispatch, encode_cid,
};

const STREAM: StreamId = StreamId(0);
const INST: InstrumentId = InstrumentId::new(1);
const NS: Namespace = Namespace::new(3);
const RPC: RpcId = RpcId(21);

/// An order-entry codec for the synthetic venue that writes amends and cancels, one per line.
struct RefCodec {
    order: OrderCaps,
}

impl RefCodec {
    fn new() -> RefCodec {
        let exec = synthetic_caps()
            .exec
            .expect("the synthetic venue takes orders");
        RefCodec { order: exec.order }
    }

    /// The wire form of the reference chosen, or `Unsupported` when none was: the command
    /// carries no reference the venue declares for the operation.
    fn wire(&self, chosen: Option<ChosenRef<'_>>) -> Result<String, NotSentReason> {
        match chosen {
            Some(ChosenRef::Venue(vid)) => Ok(format!("vid={}", vid.as_str())),
            Some(ChosenRef::Client(cid)) => encode_cid(&self.order.client_id, cid)
                .map(|wire| format!("cid={wire}"))
                .map_err(|_| NotSentReason::Unencodable),
            // The synthetic venue declares no placement-nonce reference for any operation.
            Some(ChosenRef::PlacementNonce(_)) | None => Err(NotSentReason::Unsupported),
        }
    }

    fn frame(&self, cmd: &VenueCommand) -> Result<(String, RateCharge), NotSentReason> {
        Ok(match cmd {
            VenueCommand::Amend(a) => {
                let caps = self.order.amend.ok_or(NotSentReason::Unsupported)?;
                let target = self.wire(a.reference(&caps))?;
                let qty = a.wire_qty(caps.qty_semantics);
                let qty = qty.ok_or(NotSentReason::Unencodable)?.get();
                let line = format!("amend|{target}|px={}|qty={qty}", a.px.0);
                (line, RateCharge::one(OpKind::Amend, Some(a.inst)))
            }
            VenueCommand::Cancel(c) => {
                let target = self.wire(c.reference(self.order.cancel_refs))?;
                (
                    format!("cancel|{target}"),
                    RateCharge::one(OpKind::Cancel, Some(c.inst)),
                )
            }
            // Every item must name its order by a reference the batch takes; one that cannot
            // refuses the whole batch, so nothing is sent for any of them.
            VenueCommand::CancelMany(items) => {
                let batch = self.order.batch_cancel.ok_or(NotSentReason::Unsupported)?;
                if items.is_empty() || items.len() > usize::from(batch.max_items) {
                    return Err(NotSentReason::Unsupported);
                }
                let targets = items
                    .iter()
                    .map(|c| self.wire(c.reference(batch.refs)))
                    .collect::<Result<Vec<_>, _>>()?;
                let line = format!("cancel_many|{}", targets.join("|"));
                (line, RateCharge::one(OpKind::Cancel, None))
            }
            _ => return Err(NotSentReason::Unsupported),
        })
    }
}

impl ExecCodec for RefCodec {
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    fn on_open(&mut self, _stream: StreamId, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        _specs: &SpecTable,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let (line, charge) = self.frame(cmd)?;
        let frame = WireSlice::plain(format!("rpc={}\n{line}", rpc.0).into_bytes());
        let rpc = Some(RpcCall {
            id: rpc,
            timeout: Duration::from_secs(5),
        });
        let class = cmd.traffic_class();
        fx.push(Effect::Send {
            stream: STREAM,
            frame,
            rpc,
            class,
            charge,
        });
        Ok(EncodeReceipt::new())
    }

    fn on_frame(
        &mut self,
        _stream: StreamId,
        _f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("this codec decodes nothing"))
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("this codec asks for no HTTP"))
    }

    fn on_timer(&mut self, _tag: TimerTag, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let (item, outcome) = (None, SubmitOutcome::Unknown);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    fn resync(&mut self, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

struct Events(Vec<ExecEvent>);

impl ExecSink for Events {
    fn push(&mut self, _meta: VenueMeta, ev: ExecEvent) {
        self.0.push(ev);
    }
}

fn ctx() -> EncodeCtx {
    let (wall, mono) = (WallNs(1_759_363_200_000_000_000), MonoNs(5));
    let nonces = NonceBlock::new(Vec::new());
    EncodeCtx { wall, mono, nonces }
}

/// Encodes `cmd` with a fresh codec: the frame written, or why it was not sent (and then
/// nothing was asked for).
fn encode(cmd: &VenueCommand) -> Result<String, NotSentReason> {
    let mut fx = Effects::new();
    let (specs, off) = (SpecTable::new(), &mut PathStamps::off());
    let result = RefCodec::new().encode(cmd, RPC, &specs, &ctx(), off, &mut fx);
    if let Err(reason) = result {
        assert!(fx.is_empty(), "a refused command asked for {fx:?}");
        return Err(reason);
    }
    assert!(fx.carry_request(RPC, cmd.traffic_class()), "{fx:?}");
    let [Effect::Send { frame, .. }] = fx.as_slice() else {
        panic!("one frame per request: {fx:?}");
    };
    Ok(String::from_utf8(frame.bytes().to_vec()).unwrap())
}

fn vid(text: &str) -> VenueOrderId {
    dispatch(&synthetic_caps(), NS, |scope| scope.venue_order_id(text)).unwrap()
}

/// A client id minted in the codec's namespace, under a lease in a fresh directory.
fn cid() -> ClientOrderId {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-refs-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let lease = NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(1), NS).unwrap();
    let cid = CidMint::new(lease, 0, 0, WallNs(1_759_363_200_000_000_000)).mint();
    let _ = std::fs::remove_dir_all(&dir);
    cid.unwrap()
}

fn amend(target: OrderRef) -> VenueCommand {
    VenueCommand::Amend(AmendOrder {
        target,
        inst: INST,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(101),
        qty: Lots::new(4).unwrap(),
        cum_filled: Lots::new(0).unwrap(),
    })
}

fn cancel(target: OrderRef) -> CancelOrder {
    CancelOrder {
        target,
        inst: INST,
        side: Side::Sell,
        placement_nonce: None,
    }
}

#[test]
fn an_amend_whose_only_reference_is_undeclared_is_not_sent() {
    let cid = cid();
    // Amends name the order by the venue's id only: one not yet acknowledged has no reference
    // the venue takes, so it is refused with nothing written.
    let unacked = amend(OrderRef::Client(cid));
    assert_eq!(encode(&unacked), Err(NotSentReason::Unsupported));
    // Once the venue's id is known the amend names it, never our id.
    let frame = encode(&amend(OrderRef::Both(cid, vid("V-1")))).unwrap();
    assert_eq!(frame, "rpc=21\namend|vid=V-1|px=101|qty=4");
    let frame = encode(&amend(OrderRef::Venue(vid("V-2")))).unwrap();
    assert_eq!(frame, "rpc=21\namend|vid=V-2|px=101|qty=4");
}

#[test]
fn a_cancel_many_item_whose_only_reference_is_undeclared_refuses_the_batch() {
    let cid = cid();
    let unacked = cancel(OrderRef::Client(cid));
    // A single cancel takes our client id on this venue...
    let single = encode(&VenueCommand::Cancel(unacked.clone())).unwrap();
    assert!(single.starts_with("rpc=21\ncancel|cid="), "{single}");
    // ...but a batch cancel takes venue ids only: the item that has only our id refuses the
    // whole batch before any byte is written, the acknowledged item with it.
    let acked = cancel(OrderRef::Both(cid, vid("V-3")));
    let batch = VenueCommand::CancelMany(vec![acked.clone(), unacked]);
    assert_eq!(encode(&batch), Err(NotSentReason::Unsupported));
    let by_venue_id = cancel(OrderRef::Venue(vid("V-4")));
    let batch = VenueCommand::CancelMany(vec![acked, by_venue_id]);
    assert_eq!(
        encode(&batch).unwrap(),
        "rpc=21\ncancel_many|vid=V-3|vid=V-4"
    );
    // The batch's limits come from the same declaration.
    let too_many = vec![cancel(OrderRef::Venue(vid("V-5"))); 21];
    let too_many = VenueCommand::CancelMany(too_many);
    assert_eq!(encode(&too_many), Err(NotSentReason::Unsupported));
    let empty = VenueCommand::CancelMany(Vec::new());
    assert_eq!(encode(&empty), Err(NotSentReason::Unsupported));
}

#[test]
fn the_codec_writes_nothing_but_amends_and_cancels() {
    let other = VenueCommand::FeeQuery;
    assert_eq!(encode(&other), Err(NotSentReason::Unsupported));
    let mut codec = RefCodec::new();
    let (mut fx, mut sink) = (Effects::new(), Events(Vec::new()));
    assert_eq!(codec.nonces_for(CtxCall::Resync), 0);
    codec.on_open(STREAM, &ctx(), &mut fx);
    codec.on_timer(TimerTag(1), &ctx(), &mut fx);
    codec.resync(&ctx(), &mut fx);
    assert!(fx.is_empty(), "{fx:?}");
    let frame = RawFrame::Text("anything");
    assert!(codec.redact_inbound(Inbound::Frame(frame)).is_empty());
    let specs = SpecTable::new();
    dispatch(&synthetic_caps(), NS, |scope| {
        let decoded = codec.on_frame(STREAM, frame, scope, &specs, &mut sink, &mut fx);
        assert!(decoded.is_err());
        let failed = Err(HttpFailure::NotSent);
        let http = codec.on_http(HttpTag(1), failed, scope, &specs, &mut sink, &mut fx);
        assert!(http.is_err());
    });
    codec.on_rpc_timeout(RPC, &mut sink);
    let unknown = ExecEvent::Outcome {
        rpc: RPC,
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(sink.0, [unknown]);
}
