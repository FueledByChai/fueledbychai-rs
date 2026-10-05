//! SimVenue's codec: an [`ExecCodec`] like any venue's (decision 0014, unchanged), whose
//! stream is the simulated one its [`SimEngine`](crate::SimEngine) reads (decision 0043).

use core::time::Duration;
use std::sync::Arc;

use fbc_core::{
    AckLevel, Channel, ChosenRef, CidMatch, CtxCall, DecodeError, DecodeScope, Effect, Effects,
    EncodeCtx, EncodeReceipt, ExchTsKind, ExecCodec, ExecEvent, ExecSink, FillEvent, FillIdent,
    HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, ItemRef, Liquidity, Liquidity3,
    NotSentReason, OpKind, OrderCaps, OrderKind, OrderUpdate, PathStamps, RateCharge, RawFrame,
    Reject, RpcCall, RpcId, SpecTable, StreamId, SubmitOutcome, TimerTag, VenueCommand, VenueMeta,
    VenueOrderState, WireSlice, encode_cid,
};

use crate::config::SimConfig;
use crate::wire::{Cancel, Command, Place, Refusal, Reply, Sent, SimState, Target, WireError};

/// The codec half of SimVenue. It writes each [`VenueCommand::Place`] and
/// [`VenueCommand::Cancel`] as a frame on the simulated stream, stamped with its encode's time
/// from [`EncodeCtx`], and decodes the engine's answers into outcomes, order updates and fills
/// through [`DecodeScope`] only, so venue ids, fill ids and fees are built as a real codec
/// builds them (0004). It refuses what the stood-in venue's [`OrderCaps`] do not offer, and
/// RPI orders, which the engine cannot fill yet (FBC-njk, decision 0043); amends, batches and
/// queries are FBC-nv2's.
#[derive(Clone, Debug)]
pub struct SimCodec {
    caps: OrderCaps,
    stream: StreamId,
    rpc_timeout: Duration,
}

impl SimCodec {
    /// A codec for the venue `config` stands in for.
    pub fn new(config: &SimConfig) -> SimCodec {
        SimCodec {
            caps: config.exec.order.clone(),
            stream: config.stream,
            rpc_timeout: config.rpc_timeout,
        }
    }

    fn place(
        &self,
        o: &fbc_core::NewOrder,
        rpc: RpcId,
        sent: Sent,
    ) -> Result<Command, NotSentReason> {
        let offered = self.caps.kinds.contains(o.kind.tag())
            && self.caps.tifs.contains(o.tif)
            && self.caps.channels.contains(o.channel)
            && o.channel == Channel::Public
            && (self.caps.post_only || !o.post_only)
            && (self.caps.reduce_only || !o.reduce_only);
        if !offered {
            return Err(NotSentReason::Unsupported);
        }
        let cid =
            encode_cid(&self.caps.client_id, o.cid).map_err(|_| NotSentReason::Unencodable)?;
        Ok(Command::Place(Place {
            rpc: rpc.0,
            sent,
            cid: cid.to_string(),
            inst: o.inst,
            side: o.side,
            px: match o.kind {
                OrderKind::Limit { px } => Some(px),
                OrderKind::Market => None,
            },
            qty: o.qty,
            tif: o.tif,
            post_only: o.post_only,
            reduce_only: o.reduce_only,
        }))
    }

    fn cancel(
        &self,
        c: &fbc_core::CancelOrder,
        rpc: RpcId,
        sent: Sent,
    ) -> Result<Command, NotSentReason> {
        let target = match c.reference(self.caps.cancel_refs) {
            Some(ChosenRef::Venue(vid)) => Target::Venue(vid.as_str().to_owned()),
            Some(ChosenRef::Client(cid)) => {
                let wire = encode_cid(&self.caps.client_id, cid);
                Target::Client(wire.map_err(|_| NotSentReason::Unencodable)?.to_string())
            }
            Some(ChosenRef::PlacementNonce(_)) | None => return Err(NotSentReason::Unsupported),
        };
        Ok(Command::Cancel(Cancel {
            rpc: rpc.0,
            sent,
            target,
        }))
    }
}

fn malformed(err: WireError) -> DecodeError {
    DecodeError::Malformed(err.0)
}

/// Our client id in an item reference: only one the scope reads as ours.
fn ours(scope: &DecodeScope<'_>, wire: &str) -> Option<fbc_core::ClientOrderId> {
    match scope.client_order_id(wire) {
        CidMatch::Ours(cid) => Some(cid),
        CidMatch::Foreign(_) | CidMatch::Unparseable => None,
    }
}

impl ExecCodec for SimCodec {
    /// The simulated venue takes no nonces.
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// The simulated stream needs no authentication.
    fn on_open(&mut self, _stream: StreamId, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let sent = Sent {
            mono: ctx.mono,
            wall: ctx.wall,
        };
        let (command, op, inst) = match cmd {
            VenueCommand::Place(o) => (self.place(o, rpc, sent)?, OpKind::Place, o.inst),
            VenueCommand::Cancel(c) => (self.cancel(c, rpc, sent)?, OpKind::Cancel, c.inst),
            _ => return Err(NotSentReason::Unsupported),
        };
        if specs.get(inst).is_none() {
            return Err(NotSentReason::Unencodable);
        }
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(command.encode()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: self.rpc_timeout,
            }),
            class: cmd.traffic_class(),
            charge: RateCharge::one(op, Some(inst)),
        });
        Ok(EncodeReceipt::new())
    }

    /// One answer per frame, decoded whole before it is pushed.
    fn on_frame(
        &mut self,
        _stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let (seq, reply) = Reply::decode(f.bytes()).map_err(malformed)?;
        let event = match reply {
            Reply::Accepted { rpc, cid, vid } => ExecEvent::Outcome {
                rpc: RpcId(rpc),
                item: Some(ItemRef {
                    idx: 0,
                    cid: ours(scope, &cid),
                    vid: Some(scope.venue_order_id(&vid)?),
                }),
                outcome: SubmitOutcome::Accepted {
                    ack: AckLevel::Final,
                },
            },
            Reply::Rejected { rpc, refusal } => ExecEvent::Outcome {
                rpc: RpcId(rpc),
                item: Some(ItemRef {
                    idx: 0,
                    cid: None,
                    vid: None,
                }),
                outcome: SubmitOutcome::Rejected(reject(refusal)),
            },
            Reply::Order(o) => ExecEvent::Order(OrderUpdate {
                cid: Some(scope.client_order_id(&o.cid)),
                vid: Some(scope.venue_order_id(&o.vid)?),
                inst: o.inst,
                side: o.side,
                state: match o.state {
                    SimState::Open => VenueOrderState::Open,
                    SimState::Filled => VenueOrderState::Filled,
                    SimState::Canceled(reason) => VenueOrderState::Canceled(reason),
                },
                cum_filled: o.cum,
                px: o.px,
                qty: Some(o.qty),
                post_only: Some(o.post_only),
                reduce_only: Some(o.reduce_only),
            }),
            Reply::Fill(fill) => ExecEvent::Fill(FillEvent {
                ident: FillIdent::Venue {
                    fill: scope.fill_id(&fill.fid)?,
                    vid: Some(scope.venue_order_id(&fill.vid)?),
                    cum_after: Some(fill.cum),
                },
                cid: Some(scope.client_order_id(&fill.cid)),
                inst: fill.inst,
                side: fill.side,
                px: fill.px,
                qty: fill.qty,
                liquidity: match fill.liquidity {
                    Liquidity::Maker => Liquidity3::Maker,
                    Liquidity::Taker => Liquidity3::Taker,
                },
                fee: scope.fee(fill.fee, fill.asset)?,
                realized_pnl: None,
                realized_funding: None,
                replay: false,
            }),
        };
        let meta = VenueMeta {
            exch_ts: None,
            exch_ts_kind: ExchTsKind::Unknown,
            venue_seq: Some(seq),
        };
        sink.push(meta, event);
        Ok(())
    }

    /// The simulated venue answers over its stream only.
    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "the simulated venue asks for no HTTP",
        ))
    }

    /// The codec sets no timer.
    fn on_timer(&mut self, _tag: TimerTag, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let (item, outcome) = (None, SubmitOutcome::Unknown);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    /// Not yet: the simulated venue answers no resync (FBC-bq3).
    fn resync(&mut self, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    /// The simulated stream carries no credential.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

/// The reject a refusal is, its code the venue code and its text.
fn reject(refusal: Refusal) -> Reject {
    let code = refusal.code();
    Reject {
        kind: refusal.kind(),
        venue_code: Some(code.into()),
        raw: Arc::from(code),
    }
}
