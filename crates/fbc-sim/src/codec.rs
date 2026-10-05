//! SimVenue's codec: an [`ExecCodec`] like any venue's (decision 0014, unchanged), whose
//! stream is the simulated one its [`SimEngine`](crate::SimEngine) reads (decision 0046).

use core::num::NonZeroU32;
use core::time::Duration;
use std::sync::Arc;

use fbc_core::{
    AckLevel, AckModel, AmendOrder, CancelOnDisconnect, CancelOrder, CancelScope, Channel,
    ChosenRef, CidMatch, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx,
    EncodeReceipt, ExchTsKind, ExecCaps, ExecCodec, ExecEvent, ExecSink, Feature, FillCaps,
    FillEvent, FillIdent, FillSource, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentId, ItemRef, Liquidity, Liquidity3, MatchingCaps, NotSentReason, OpKind, OrderCaps,
    OrderKindTag, OrderRef, OrderUpdate, OrderingKey, PathStamps, QueryAnswer, RateCharge,
    RawFrame, Reject, RpcCall, RpcId, SignedLots, SpecTable, StreamId, SubmitOutcome, Support,
    TifTag, TimerTag, TrafficClass, VenueCommand, VenueMeta, VenueOrderSnapshot, VenueOrderState,
    WallNs, WireSlice, encode_cid,
};

use crate::config::SimConfig;
use crate::wire::{
    Amend, Cancel, Command, Head, ItemResult, OrderEvent, Place, Query, Refusal, Reply, Sent,
    SimState, Target, WireError,
};

/// The codec half of SimVenue. It writes each placement, batch of placements, amend, cancel,
/// batch of cancels, instrument cancel-all and order query as a frame on the simulated stream,
/// stamped with its encode's time from [`EncodeCtx`] and charged one per item, and decodes the
/// engine's answers into outcomes (a batch's every item in the one call that decodes its
/// frame, record 0014), order updates, fills and query results through [`DecodeScope`] only,
/// so venue ids, fill ids and fees are built as a real codec builds them (0004), saying on its
/// events only what the stood-in venue echoes. An amend is answered as an outcome then the
/// amended order's event, whether the venue reports it with an event or only in its reply
/// ([`AmendAck`](fbc_core::AmendAck)), which a real codec turns into the same event.
///
/// It refuses what the stood-in venue's [`OrderCaps`] do not offer: a kind, time in force,
/// flag or reference, amends, batches (or one longer than their `max_items`; an empty one is
/// unencodable), an instrument cancel-all, queries; and never sends an account-wide cancel-all
/// (owner decision A). It refuses RPI orders, which the engine cannot fill yet (FBC-njk,
/// decision 0046), amends for a venue that gives the amended order a new venue id (FBC-kodq),
/// and placements and amends for a venue whose events the engine cannot say yet: two-phase
/// acknowledgement (FBC-zr1), an ordering key other than a venue sequence, realized values on
/// fills, or fills derived from order status (FBC-938), a venue with a speed bump (FBC-7y8),
/// one whose fills replay on reconnect (FBC-3q6), or one that cancels orders on a disconnect,
/// which the simulated stream never has (FBC-fji). Cancels and queries still go.
///
/// A resync (decision 0049) is one frame asking the engine for its resting orders and
/// positions, one at a time, whose one-frame answer it decodes into the `Resync*` events.
#[derive(Clone, Debug)]
pub struct SimCodec {
    /// Whether the engine can say what the stood-in venue's events say ([`modelled`]).
    modelled: bool,
    caps: OrderCaps,
    fills: FillCaps,
    stream: StreamId,
    rpc_timeout: Duration,
    /// The wall time of the resync asked for and not answered yet.
    resync_at: Option<WallNs>,
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
            resync_at: None,
        }
    }

    /// Whether the stood-in venue takes an order of this kind, time in force, channel and
    /// flags, which the engine can answer as it would.
    fn offered(
        &self,
        kind: OrderKindTag,
        tif: TifTag,
        channel: Channel,
        post_only: bool,
        reduce_only: bool,
    ) -> Result<(), NotSentReason> {
        let offered = self.caps.kinds.contains(kind)
            && self.caps.tifs.contains(tif)
            && self.caps.channels.contains(channel)
            && channel == Channel::Public
            && (self.caps.post_only || !post_only)
            && (self.caps.reduce_only || !reduce_only);
        if !offered || !self.modelled {
            return Err(NotSentReason::Unsupported);
        }
        // Codex r4182154713: a pair the stood-in venue refuses together is refused here too.
        let has = |feature| match feature {
            Feature::PostOnly => post_only,
            Feature::ReduceOnly => reduce_only,
            Feature::Ioc => tif == TifTag::Ioc,
            Feature::Fok => tif == TifTag::Fok,
            Feature::Rpi => channel == Channel::Rpi,
        };
        if self
            .caps
            .flag_conflicts
            .iter()
            .any(|&(a, b)| has(a) && has(b))
        {
            return Err(NotSentReason::FlagConflict);
        }
        Ok(())
    }

    /// A placement, refused as unencodable for an instrument the spec table does not list.
    fn place(
        &self,
        o: &fbc_core::NewOrder,
        head: Head,
        specs: &SpecTable,
    ) -> Result<Place, NotSentReason> {
        if specs.get(o.inst).is_none() {
            return Err(NotSentReason::Unencodable);
        }
        self.offered(o.kind.tag(), o.tif, o.channel, o.post_only, o.reduce_only)?;
        Ok(Place {
            rpc: head.rpc,
            sent: head.sent,
            cid: self.wire_cid(o.cid)?,
            inst: o.inst,
            side: o.side,
            px: o.kind.limit_px(),
            qty: o.qty,
            tif: o.tif,
            post_only: o.post_only,
            reduce_only: o.reduce_only,
        })
    }

    /// Our client id in the stood-in venue's wire format.
    fn wire_cid(&self, cid: fbc_core::ClientOrderId) -> Result<String, NotSentReason> {
        let wire = encode_cid(&self.caps.client_id, cid);
        Ok(wire.map_err(|_| NotSentReason::Unencodable)?.to_string())
    }

    /// The order a request names, by the reference chosen for it; the simulated venue tracks
    /// no placement nonces.
    fn target(&self, chosen: Option<ChosenRef<'_>>) -> Result<Target, NotSentReason> {
        match chosen {
            Some(ChosenRef::Venue(vid)) => Ok(Target::Venue(vid.as_str().to_owned())),
            Some(ChosenRef::Client(cid)) => Ok(Target::Client(self.wire_cid(cid)?)),
            Some(ChosenRef::PlacementNonce(_)) | None => Err(NotSentReason::Unsupported),
        }
    }

    /// An amend, for a venue whose amends the engine answers as it would: one that keeps the
    /// order's venue id (FBC-kodq), with the order's full values after it and the quantity the
    /// venue's wire carries.
    fn amend(&self, a: &AmendOrder, head: Head, specs: &SpecTable) -> Result<Amend, NotSentReason> {
        let caps = self.caps.amend.filter(|caps| caps.keeps_venue_id);
        let caps = caps.ok_or(NotSentReason::Unsupported)?;
        let kind = OrderKindTag::Limit;
        self.offered(kind, a.tif, a.channel, a.post_only, a.reduce_only)?;
        let target = self.target(a.reference(&caps))?;
        // A total at or below the filled quantity leaves nothing to rest: a cancel, not an
        // amend (`AmendOrder::wire_qty`).
        let qty = a.wire_qty(caps.qty_semantics);
        let qty = qty.ok_or(NotSentReason::Unencodable)?;
        specs.get(a.inst).ok_or(NotSentReason::Unencodable)?;
        Ok(Amend {
            head,
            target,
            inst: a.inst,
            side: a.side,
            px: a.px,
            qty,
            tif: a.tif,
            post_only: a.post_only,
            reduce_only: a.reduce_only,
        })
    }

    /// The frame `cmd` is written as, with its operation, the instrument its charge names and
    /// its weight: one per item of a batch.
    fn command(
        &self,
        cmd: &VenueCommand,
        head: Head,
        specs: &SpecTable,
    ) -> Result<(Command, OpKind, Option<InstrumentId>, usize), NotSentReason> {
        use NotSentReason::Unsupported;
        Ok(match cmd {
            VenueCommand::Place(o) => {
                let place = self.place(o, head, specs)?;
                (Command::Place(place), OpKind::Place, Some(o.inst), 1)
            }
            VenueCommand::PlaceBatch(orders) => {
                let max = self.caps.batch_place.map(|batch| batch.max_items);
                batch_len(orders.len(), max)?;
                let place = |o| self.place(o, head, specs);
                let places = orders.iter().map(place).collect::<Result<_, _>>()?;
                let inst = shared(orders.iter().map(|o| o.inst));
                (
                    Command::Batch(head, places),
                    OpKind::Place,
                    inst,
                    orders.len(),
                )
            }
            VenueCommand::Amend(a) => {
                let amend = self.amend(a, head, specs)?;
                (Command::Amend(amend), OpKind::Amend, Some(a.inst), 1)
            }
            // A cancel names only the order, so an instrument the table no longer lists does
            // not stop it (Codex r4182991965).
            VenueCommand::Cancel(c) => {
                let target = self.target(c.reference(self.caps.cancel_refs))?;
                let cancel = Cancel {
                    rpc: head.rpc,
                    sent: head.sent,
                    target,
                };
                (Command::Cancel(cancel), OpKind::Cancel, Some(c.inst), 1)
            }
            VenueCommand::CancelMany(cancels) => {
                let batch = self.caps.batch_cancel.ok_or(Unsupported)?;
                batch_len(cancels.len(), Some(batch.max_items))?;
                let target = |c: &CancelOrder| self.target(c.reference(batch.refs));
                let targets = cancels.iter().map(target).collect::<Result<_, _>>()?;
                let inst = shared(cancels.iter().map(|c| c.inst));
                let command = Command::Cancels(head, targets);
                (command, OpKind::Cancel, inst, cancels.len())
            }
            // Never widened to the account, and never account-wide (owner decision A).
            VenueCommand::CancelAll(CancelScope::Instrument(inst))
                if self.caps.cancel_all_instrument == Support::Native =>
            {
                (
                    Command::CancelAll(head, *inst),
                    OpKind::CancelAll,
                    Some(*inst),
                    1,
                )
            }
            VenueCommand::Query(q) => {
                let target = self.target(q.reference(self.caps.query_refs))?;
                let vid = q.target.venue().map(|vid| vid.as_str().to_owned());
                let cid = q
                    .target
                    .client()
                    .map(|cid| self.wire_cid(cid))
                    .transpose()?;
                let query = Query {
                    head,
                    target,
                    vid,
                    cid,
                };
                (Command::Query(query), OpKind::Query, Some(q.inst), 1)
            }
            _ => return Err(Unsupported),
        })
    }

    /// An event's client id, read only where the stood-in venue echoes it on events.
    fn echoed_cid(&self, scope: &DecodeScope<'_>, wire: &str) -> Option<CidMatch> {
        let echoed = self.caps.cid_echoed_on_events;
        echoed.then(|| scope.client_order_id(wire))
    }

    /// The outcome of item `idx` of request `rpc`.
    fn outcome(
        rpc: u64,
        idx: u16,
        result: ItemResult,
        scope: &DecodeScope<'_>,
    ) -> Result<ExecEvent, DecodeError> {
        let (item, outcome) = match result {
            ItemResult::Accepted { cid, vid } => {
                let item = ItemRef {
                    idx,
                    cid: ours(scope, &cid),
                    vid: Some(scope.venue_order_id(&vid)?),
                };
                let ack = AckLevel::Final;
                (item, SubmitOutcome::Accepted { ack })
            }
            ItemResult::Rejected(refusal) => {
                let (cid, vid) = (None, None);
                (
                    ItemRef { idx, cid, vid },
                    SubmitOutcome::Rejected(reject(refusal)),
                )
            }
        };
        Ok(ExecEvent::Outcome {
            rpc: RpcId(rpc),
            item: Some(item),
            outcome,
        })
    }

    /// An order as the venue reports it, saying only what the stood-in venue echoes (Codex
    /// r4182678498, r4182678504): its client ids and the order's flags.
    fn snapshot(
        &self,
        o: OrderEvent,
        scope: &DecodeScope<'_>,
    ) -> Result<VenueOrderSnapshot, DecodeError> {
        let flags = self.caps.events_echo_flags;
        Ok(VenueOrderSnapshot {
            cid: self.echoed_cid(scope, &o.cid),
            vid: scope.venue_order_id(&o.vid)?,
            inst: o.inst,
            side: o.side,
            state: match o.state {
                SimState::Open => VenueOrderState::Open,
                SimState::Amended => VenueOrderState::Amended { new_vid: None },
                SimState::Filled => VenueOrderState::Filled,
                SimState::Canceled(reason) => VenueOrderState::Canceled(reason),
            },
            px: o.px,
            qty: o.qty,
            cum_filled: o.cum,
            post_only: flags.then_some(o.post_only),
            reduce_only: flags.then_some(o.reduce_only),
        })
    }

    /// A resync's answer, decoded whole: only the one asked for, named by its watermark, the
    /// request's wall time; a position past an `i64` of lots is refused, never wrapped.
    fn resync_answer(
        &mut self,
        wm: WallNs,
        orders: Vec<OrderEvent>,
        positions: Vec<(InstrumentId, i128)>,
        scope: &DecodeScope<'_>,
    ) -> Result<Vec<ExecEvent>, DecodeError> {
        match self.resync_at {
            None => return Err(DecodeError::Malformed("no resync asked for")),
            Some(asked) if asked != wm => {
                return Err(DecodeError::Malformed("resync for another request"));
            }
            Some(_) => {}
        }
        let mut events = vec![ExecEvent::ResyncBegin { watermark: wm }];
        for o in orders {
            events.push(ExecEvent::ResyncOrder(self.snapshot(o, scope)?));
        }
        for (inst, qty) in positions {
            let qty = i64::try_from(qty).map_err(|_| DecodeError::Malformed("qty"))?;
            events.push(ExecEvent::ResyncPosition {
                inst,
                qty: SignedLots(qty),
                // The engine keeps no entry price.
                avg_entry: None,
            });
        }
        events.push(ExecEvent::ResyncEnd);
        self.resync_at = None;
        Ok(events)
    }

    /// A query's answer: the order it asked about, named by the identifiers it echoes, and
    /// the order the venue reports, which must be that order ([`QueryAnswer::new`]).
    fn query_answer(
        &self,
        rpc: u64,
        ids: (Option<String>, Option<String>),
        found: Option<OrderEvent>,
        scope: &DecodeScope<'_>,
    ) -> Result<ExecEvent, DecodeError> {
        let vid = ids.0.map(|vid| scope.venue_order_id(&vid)).transpose()?;
        let cid = match ids.1 {
            Some(cid) => Some(ours(scope, &cid).ok_or(DecodeError::Malformed("query cid"))?),
            None => None,
        };
        let target = match (cid, vid) {
            (Some(cid), Some(vid)) => OrderRef::Both(cid, vid),
            (Some(cid), None) => OrderRef::Client(cid),
            (None, Some(vid)) => OrderRef::Venue(vid),
            (None, None) => return Err(DecodeError::Malformed("query target")),
        };
        let found = found.map(|o| self.snapshot(o, scope)).transpose()?;
        let answer = QueryAnswer::new(RpcId(rpc), target, found);
        let answer = answer.ok_or(DecodeError::Malformed("query answer names another order"))?;
        Ok(ExecEvent::QueryResult(answer))
    }
}

/// A batch's length checked against the venue's: refused when it takes no batch or the batch
/// is longer than it takes, and unencodable when empty.
fn batch_len(len: usize, max_items: Option<u16>) -> Result<(), NotSentReason> {
    let max = max_items.ok_or(NotSentReason::Unsupported)?;
    if len > usize::from(max) {
        return Err(NotSentReason::Unsupported);
    }
    if len == 0 {
        return Err(NotSentReason::Unencodable);
    }
    Ok(())
}

/// The instrument every item names, or `None` for a batch that spans several.
fn shared(mut insts: impl Iterator<Item = InstrumentId>) -> Option<InstrumentId> {
    let first = insts.next()?;
    insts.all(|inst| inst == first).then_some(first)
}

/// Whether the engine answers as the venue `exec` and `matching` describe: it delays no
/// command past its latency (Codex r4184245574; FBC-7y8), it acknowledges in one phase
/// (Codex r4182678509; FBC-zr1), orders its answers by a venue sequence, keeps no position to
/// report realized P&L or funding from, and sends fills of their own (Codex r4182991971,
/// r4182991978; FBC-938), never replays a fill on reconnect (Codex r4184546713; FBC-3q6), and
/// leaves orders resting across a disconnect, which the simulated stream has no notion of
/// (Codex r4184778435; FBC-fji).
/// Placements for any other venue are refused rather than answered with events unlike its own.
fn modelled(exec: &ExecCaps, matching: &MatchingCaps) -> bool {
    matching.speed_bump.is_none()
        && exec.order.ack == AckModel::SinglePhase
        && exec.order.ordering_key == OrderingKey::VenueSeq
        && !exec.fills.realized_pnl
        && !exec.fills.realized_funding
        && exec.fills.source == FillSource::Native
        && !exec.fills.replays_fills_on_reconnect
        && exec.order.cancel_on_disconnect == CancelOnDisconnect::None
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
        let head = Head {
            rpc: rpc.0,
            sent: Sent {
                mono: ctx.mono,
                wall: ctx.wall,
            },
        };
        let (command, op, inst, items) = self.command(cmd, head, specs)?;
        // At most a batch's `max_items`, a u16, so never zero and always a u32.
        let weight = u32::try_from(items).ok().and_then(NonZeroU32::new);
        let weight = weight.unwrap_or(NonZeroU32::MIN);
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(command.encode()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: self.rpc_timeout,
            }),
            class: cmd.traffic_class(),
            charge: RateCharge { op, inst, weight },
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
        let events = match reply {
            Reply::Accepted { rpc, cid, vid } => {
                let result = ItemResult::Accepted { cid, vid };
                vec![SimCodec::outcome(rpc, 0, result, scope)?]
            }
            Reply::Rejected { rpc, refusal } => {
                let result = ItemResult::Rejected(refusal);
                vec![SimCodec::outcome(rpc, 0, result, scope)?]
            }
            // Every item's outcome is pushed in this one call (the one-call contract, 0014).
            Reply::Items { rpc, items } => {
                let outcome = |(idx, result): (usize, ItemResult)| {
                    let idx = u16::try_from(idx).map_err(|_| DecodeError::Malformed("items"))?;
                    SimCodec::outcome(rpc, idx, result, scope)
                };
                let items = items.into_iter().enumerate().map(outcome);
                items.collect::<Result<_, _>>()?
            }
            Reply::Done { rpc } => vec![ExecEvent::Outcome {
                rpc: RpcId(rpc),
                item: None,
                outcome: SubmitOutcome::Accepted {
                    ack: AckLevel::Final,
                },
            }],
            Reply::Query {
                rpc,
                vid,
                cid,
                found,
            } => vec![self.query_answer(rpc, (vid, cid), found, scope)?],
            Reply::Order(o) => {
                let o = self.snapshot(o, scope)?;
                vec![ExecEvent::Order(OrderUpdate {
                    cid: o.cid,
                    vid: Some(o.vid),
                    inst: o.inst,
                    side: o.side,
                    state: o.state,
                    cum_filled: o.cum_filled,
                    px: o.px,
                    qty: Some(o.qty),
                    post_only: o.post_only,
                    reduce_only: o.reduce_only,
                })]
            }
            Reply::Fill(fill) => vec![ExecEvent::Fill(FillEvent {
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
            })],
            Reply::Resync {
                wm,
                orders,
                positions,
            } => self.resync_answer(wm, orders, positions, scope)?,
        };
        let meta = VenueMeta {
            exch_ts: None,
            exch_ts_kind: ExchTsKind::Unknown,
            venue_seq: Some(seq),
        };
        for event in events {
            sink.push(meta, event);
        }
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

    /// Asks the engine for its resting orders and positions (decision 0049): one frame,
    /// stamped with the call's time, whose wall time is the answer's watermark. A read, so it
    /// awaits no RPC deadline; the simulated stream never loses a frame, so the answer always
    /// comes, and while one is unanswered another call asks nothing.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        if self.resync_at.is_some() {
            return;
        }
        self.resync_at = Some(ctx.wall);
        let sent = Sent {
            mono: ctx.mono,
            wall: ctx.wall,
        };
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(Command::Resync(sent).encode()),
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Query, None),
        });
    }

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
