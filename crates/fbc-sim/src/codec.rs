//! SimVenue's codec: an [`ExecCodec`] like any venue's (decision 0014, unchanged), whose
//! stream is the simulated one its [`SimEngine`](crate::SimEngine) reads (decision 0044).

use core::time::Duration;
use std::sync::Arc;

use fbc_core::{
    AckLevel, AckModel, Channel, ChosenRef, CidMatch, CtxCall, DecodeError, DecodeScope, Effect,
    Effects, EncodeCtx, EncodeReceipt, ExchTsKind, ExecCaps, ExecCodec, ExecEvent, ExecSink,
    Feature, FillCaps, FillEvent, FillIdent, FillSource, HttpFailure, HttpResponse, HttpTag,
    Inbound, InboundSpans, ItemRef, Liquidity, Liquidity3, MatchingCaps, NotSentReason, OpKind,
    OrderCaps, OrderKind, OrderUpdate, OrderingKey, PathStamps, RateCharge, RawFrame, Reject,
    RpcCall, RpcId, SpecTable, StreamId, SubmitOutcome, TifTag, TimerTag, VenueCommand, VenueMeta,
    VenueOrderState, WireSlice, encode_cid,
};

use crate::config::SimConfig;
use crate::wire::{Cancel, Command, Place, Refusal, Reply, Sent, SimState, Target, WireError};

/// The codec half of SimVenue. It writes each [`VenueCommand::Place`] and
/// [`VenueCommand::Cancel`] as a frame on the simulated stream, stamped with its encode's time
/// from [`EncodeCtx`], and decodes the engine's answers into outcomes, order updates and fills
/// through [`DecodeScope`] only, so venue ids, fill ids and fees are built as a real codec
/// builds them (0004), saying on its events only what the stood-in venue echoes. It refuses
/// what the stood-in venue's [`OrderCaps`] do not offer, RPI orders, which the engine cannot
/// fill yet (FBC-njk, decision 0044), and placements for a venue whose events the engine
/// cannot say yet: two-phase acknowledgement (FBC-zr1), an ordering key other than a venue
/// sequence, realized values on fills, or fills derived from order status (FBC-938), a venue
/// with a speed bump (FBC-7y8), or one whose fills replay on reconnect (FBC-3q6); amends,
/// batches and queries are FBC-nv2's.
#[derive(Clone, Debug)]
pub struct SimCodec {
    /// Whether the engine can say what the stood-in venue's events say ([`modelled`]).
    modelled: bool,
    caps: OrderCaps,
    fills: FillCaps,
    stream: StreamId,
    rpc_timeout: Duration,
}

impl SimCodec {
    /// A codec for the venue `config` stands in for.
    pub fn new(config: &SimConfig) -> SimCodec {
        SimCodec {
            modelled: modelled(&config.exec, &config.matching),
            caps: config.exec.order.clone(),
            fills: config.exec.fills,
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
        if !offered || !self.modelled {
            return Err(NotSentReason::Unsupported);
        }
        // Codex r4182154713: a pair the stood-in venue refuses together is refused here too.
        let has = |feature| match feature {
            Feature::PostOnly => o.post_only,
            Feature::ReduceOnly => o.reduce_only,
            Feature::Ioc => o.tif == TifTag::Ioc,
            Feature::Fok => o.tif == TifTag::Fok,
            Feature::Rpi => o.channel == Channel::Rpi,
        };
        if self
            .caps
            .flag_conflicts
            .iter()
            .any(|&(a, b)| has(a) && has(b))
        {
            return Err(NotSentReason::FlagConflict);
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

    /// An event's client id, read only where the stood-in venue echoes it on events.
    fn echoed_cid(&self, scope: &DecodeScope<'_>, wire: &str) -> Option<CidMatch> {
        let echoed = self.caps.cid_echoed_on_events;
        echoed.then(|| scope.client_order_id(wire))
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

/// Whether the engine answers as the venue `exec` and `matching` describe: it delays no
/// command past its latency (Codex r4184245574; FBC-7y8), it acknowledges in one phase
/// (Codex r4182678509; FBC-zr1), orders its answers by a venue sequence, keeps no position to
/// report realized P&L or funding from, and sends fills of their own (Codex r4182991971,
/// r4182991978; FBC-938), and never replays a fill on reconnect (Codex r4184546713; FBC-3q6).
/// Placements for any other venue are refused rather than answered with events unlike its own.
fn modelled(exec: &ExecCaps, matching: &MatchingCaps) -> bool {
    matching.speed_bump.is_none()
        && exec.order.ack == AckModel::SinglePhase
        && exec.order.ordering_key == OrderingKey::VenueSeq
        && !exec.fills.realized_pnl
        && !exec.fills.realized_funding
        && exec.fills.source == FillSource::Native
        && !exec.fills.replays_fills_on_reconnect
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
            // A placement needs its instrument's spec; a cancel names only the order, so an
            // instrument the table no longer lists does not stop it (Codex r4182991965).
            VenueCommand::Place(o) if specs.get(o.inst).is_none() => {
                return Err(NotSentReason::Unencodable);
            }
            VenueCommand::Place(o) => (self.place(o, rpc, sent)?, OpKind::Place, o.inst),
            VenueCommand::Cancel(c) => (self.cancel(c, rpc, sent)?, OpKind::Cancel, c.inst),
            _ => return Err(NotSentReason::Unsupported),
        };
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
            // Only what the stood-in venue echoes on its events (Codex r4182678498,
            // r4182678504): its client ids and the order's flags.
            Reply::Order(o) => ExecEvent::Order(OrderUpdate {
                cid: self.echoed_cid(scope, &o.cid),
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
                post_only: self.caps.events_echo_flags.then_some(o.post_only),
                reduce_only: self.caps.events_echo_flags.then_some(o.reduce_only),
            }),
            Reply::Fill(fill) => ExecEvent::Fill(FillEvent {
                // Only what the stood-in venue reports (Codex r4182448147): without fill ids
                // a fill is keyed by its order and cumulative quantity. With them it names both,
                // which `FillCaps` cannot yet say a venue's fills omit (FBC-2g7).
                ident: if self.fills.fill_id {
                    FillIdent::Venue {
                        fill: scope.fill_id(&fill.fid)?,
                        vid: Some(scope.venue_order_id(&fill.vid)?),
                        cum_after: Some(fill.cum),
                    }
                } else {
                    FillIdent::Derived {
                        vid: scope.venue_order_id(&fill.vid)?,
                        cum_after: fill.cum,
                    }
                },
                cid: self.echoed_cid(scope, &fill.cid),
                inst: fill.inst,
                side: fill.side,
                px: fill.px,
                qty: fill.qty,
                // Codex r4182448157: a venue without the flag does not say.
                liquidity: match (self.fills.liquidity_flag, fill.liquidity) {
                    (false, _) => Liquidity3::Unknown,
                    (true, Liquidity::Maker) => Liquidity3::Maker,
                    (true, Liquidity::Taker) => Liquidity3::Taker,
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
