//! The toy's order-entry codec. Every request is one frame on [`EXEC_STREAM`] that names its
//! `rpc` (with [`RPC_TIMEOUT`]), is labelled with its command's traffic class and carries the
//! rate charge the toy's account limit counts it by (0014 item 3, 0018). It takes time and
//! nonces only from the [`EncodeCtx`] (0014 item 1): a placement keeps its nonce in the receipt
//! as the order's placement nonce, while an amend's or a cancel's nonce only signs it. Each
//! signer call is marked as the sign stage through [`PathStamps`] (0034).
//!
//! Before it signs anything it refuses, as `NotSent(Unsupported)`, an order (a placement, a
//! batch item or an amend) of a kind, time in force, channel or flag its caps do not declare
//! (and as `NotSent(FlagConflict)` one combining a pair its caps declare in conflict),
//! an amend or cancel whose only references are undeclared for it (a batch cancel's
//! references are narrower than a single cancel's), a batch longer than its `max_items`, an
//! account-wide cancel-all, and a dead-man refresh (its cancel-on-disconnect is per
//! connection). What cannot be written is `Unencodable`: an instrument missing from the spec
//! table, a missing nonce, an empty batch, and an amend to its filled quantity or below, which
//! leaves nothing to rest. A signer that fails is `SignFailed`.
//!
//! As [`ToyExec::new`] builds it, it resyncs in frames on its stream and sets no timer; its
//! factory builds it [`pinging`](ToyExec::pinging), a `ping|ts=<wall>` sent on the opened
//! stream every [`PING_EVERY`] (Codex r4172835747), and, given a REST base, resyncing
//! [over REST](ToyExec::rest_resync) (`session.rs`, Codex r4172917335).

use fbc_core::{
    AmendOrder, AmendRef, AmendWire, CancelOrder, CancelRef, CancelScope, CancelWire, Channel,
    ChosenRef, ClientOrderId, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx,
    EncodeReceipt, ExecCodec, ExecEvent, ExecSink, Feature, FillCaps, HttpFailure, HttpResponse,
    HttpTag, Inbound, InboundSpans, InstrumentId, NewOrder, NotSentReason, OpKind, OrderCaps,
    OrderKindTag, OrderSigner, PathStage, PathStamps, PlaceWire, RateCharge, RawFrame, RefKind,
    RpcCall, RpcId, Side, SpecTable, StreamId, TagSet, Tif, TimerTag, TrafficClass, VenueCommand,
    VenueOrderId, WireCid, WireSlice, WireUrl, encode_cid,
};

use super::session::{self, Answers};
use super::url::{echoed, secrets};
use super::{EXEC_STREAM, FillIds, PING_EVERY, PING_TAG, RPC_TIMEOUT, caps_for, decode, weight};

use NotSentReason::{FlagConflict, SignFailed, Unencodable, Unsupported};

/// The toy's order-entry codec, signing through the [`OrderSigner`] it is given and decoding
/// what the venue sends as the caps it was built with declare.
pub struct ToyExec {
    order: OrderCaps,
    fills: FillCaps,
    signer: Box<dyn OrderSigner>,
    answers: Answers,
    /// The base a resync is asked for under over REST; `None` resyncs in frames.
    rest: Option<WireUrl>,
    /// Whether `on_open` arms a ping.
    ping: bool,
    /// The stream the last `on_open` opened, which the ping goes out on.
    opened: Option<StreamId>,
}

/// A request's frame and the charge it carries.
type Request = (String, RateCharge);

impl ToyExec {
    /// A codec for the toy's declared [`caps`], signing with `signer`.
    pub fn new(signer: Box<dyn OrderSigner>) -> ToyExec {
        ToyExec::with_fill_ids(signer, FillIds::Venue)
    }

    /// A codec for the toy declared with `fill_ids` ([`caps_for`]), signing with `signer`.
    pub fn with_fill_ids(signer: Box<dyn OrderSigner>, fill_ids: FillIds) -> ToyExec {
        let exec = caps_for(fill_ids).exec.expect("the toy takes orders");
        let (order, fills) = (exec.order, exec.fills);
        ToyExec {
            order,
            fills,
            signer,
            answers: Answers::default(),
            rest: None,
            ping: false,
            opened: None,
        }
    }

    /// This codec, keeping its order-entry connection alive (Codex r4172835747): `on_open`
    /// arms [`PING_TAG`], and each firing sends `ping|ts=<wall>` on the opened stream (one
    /// `Control` unit, safety traffic, answered by nothing) and arms it again, [`PING_EVERY`]
    /// apart.
    pub fn pinging(self) -> ToyExec {
        ToyExec { ping: true, ..self }
    }

    /// This codec, resyncing over REST under `base`, whose credential spans every request
    /// keeps (`session.rs`; Codex r4172917335, r4172917294).
    pub fn rest_resync(self, base: WireUrl) -> ToyExec {
        let rest = Some(base);
        ToyExec { rest, ..self }
    }

    /// Refuses an order of a kind, time in force, channel or flag the caps do not declare
    /// (`Unsupported`), then one combining a pair of features the caps declare in conflict
    /// (`FlagConflict`).
    fn declared(
        &self,
        kind: OrderKindTag,
        tif: Tif,
        channel: Channel,
        flags: (bool, bool),
    ) -> Result<(), NotSentReason> {
        let c = &self.order;
        let (post_only, reduce_only) = flags;
        let offered = (!post_only || c.post_only) && (!reduce_only || c.reduce_only);
        let shape = c.kinds.contains(kind) && c.tifs.contains(tif) && c.channels.contains(channel);
        (shape && offered).then_some(()).ok_or(Unsupported)?;
        let features = [
            (Feature::PostOnly, post_only),
            (Feature::ReduceOnly, reduce_only),
            (Feature::Ioc, tif == Tif::Ioc),
            (Feature::Fok, tif == Tif::Fok),
            (Feature::Rpi, channel == Channel::Rpi),
        ];
        let has = |feature| features.contains(&(feature, true));
        let conflict = c.flag_conflicts.iter().any(|&(a, b)| has(a) && has(b));
        (!conflict).then_some(()).ok_or(FlagConflict)
    }

    fn check_place(&self, o: &NewOrder) -> Result<(), NotSentReason> {
        self.declared(o.kind.tag(), o.tif, o.channel, (o.post_only, o.reduce_only))
    }

    fn cid(&self, cid: ClientOrderId) -> Result<WireCid, NotSentReason> {
        encode_cid(&self.order.client_id, cid).map_err(|_| Unencodable)
    }

    /// `chosen` as the toy's wire spells it.
    fn name<'a>(&self, chosen: ChosenRef<'a>) -> Result<Named<'a>, NotSentReason> {
        Ok(match chosen {
            ChosenRef::Venue(vid) => Named::Venue(vid),
            ChosenRef::Client(cid) => Named::Client(self.cid(cid)?),
            ChosenRef::PlacementNonce(nonce) => Named::Nonce(nonce),
        })
    }

    /// One placement's fields, signed with item `item`'s nonce, which the receipt keeps.
    fn place(
        &mut self,
        item: u16,
        o: &NewOrder,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        receipt: &mut EncodeReceipt,
        t: &mut PathStamps<'_>,
    ) -> Result<String, NotSentReason> {
        let spec = specs.get(o.inst).ok_or(Unencodable)?;
        let px = o.kind.limit_px().ok_or(Unsupported)?;
        let cid = self.cid(o.cid)?;
        let nonce = receipt.use_nonce(ctx, item).ok_or(Unencodable)?;
        let wire = PlaceWire {
            spec,
            cid: cid.as_str(),
            side: o.side,
            kind: o.kind,
            qty: o.qty,
            tif: o.tif,
            channel: o.channel,
            post_only: o.post_only,
            reduce_only: o.reduce_only,
            wall: ctx.wall,
            nonce: Some(nonce),
        };
        let sig = t.span(PathStage::Sign, || self.signer.sign_place(&wire));
        let sig = sig.map_err(|_| SignFailed)?;
        Ok(format!(
            "cid={cid}|sym={}|side={}|px={}|qty={}|tif={}|ch={}|po={}|ro={}|ts={}|nonce={nonce}\
             |sig={}",
            spec.venue_symbol.as_wire(),
            side(o.side),
            px.0,
            o.qty.get(),
            lower(o.tif),
            lower(o.channel),
            u8::from(o.post_only),
            u8::from(o.reduce_only),
            ctx.wall.0,
            String::from_utf8_lossy(sig.as_bytes()),
        ))
    }

    fn amend(
        &mut self,
        a: &AmendOrder,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> Result<Request, NotSentReason> {
        let caps = self.order.amend.ok_or(Unsupported)?;
        let flags = (a.post_only, a.reduce_only);
        self.declared(OrderKindTag::Limit, a.tif, a.channel, flags)?;
        let chosen = a.reference(&caps).ok_or(Unsupported)?;
        let qty = a.wire_qty(caps.qty_semantics).ok_or(Unencodable)?;
        let spec = specs.get(a.inst).ok_or(Unencodable)?;
        let named = self.name(chosen)?;
        let target = named.amend_ref().ok_or(Unsupported)?;
        let nonce = ctx.nonce(0).ok_or(Unencodable)?;
        let wire = AmendWire {
            spec,
            target,
            side: a.side,
            px: a.px,
            qty,
            tif: a.tif,
            channel: a.channel,
            post_only: a.post_only,
            reduce_only: a.reduce_only,
            wall: ctx.wall,
            nonce: Some(nonce),
        };
        let sig = t.span(PathStage::Sign, || self.signer.sign_amend(&wire));
        let sig = sig.map_err(|_| SignFailed)?;
        let frame = format!(
            "amend|rpc={}|{}|sym={}|side={}|px={}|qty={}|tif={}|ch={}|po={}|ro={}|ts={}\
             |nonce={nonce}|sig={}",
            rpc.0,
            named.field(),
            spec.venue_symbol.as_wire(),
            side(a.side),
            a.px.0,
            qty.get(),
            lower(a.tif),
            lower(a.channel),
            u8::from(a.post_only),
            u8::from(a.reduce_only),
            ctx.wall.0,
            String::from_utf8_lossy(sig.as_bytes()),
        );
        Ok((frame, RateCharge::one(OpKind::Amend, Some(a.inst))))
    }

    /// One cancel's fields, naming its order by the first of `declared` it carries and signed
    /// with item `item`'s nonce.
    fn cancel(
        &mut self,
        item: u16,
        c: &CancelOrder,
        declared: TagSet<RefKind>,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> Result<String, NotSentReason> {
        let chosen = c.reference(declared).ok_or(Unsupported)?;
        let spec = specs.get(c.inst).ok_or(Unencodable)?;
        let named = self.name(chosen)?;
        let nonce = ctx.nonce(item).ok_or(Unencodable)?;
        let wire = CancelWire {
            spec,
            target: named.cancel_ref(),
            side: c.side,
            wall: ctx.wall,
            nonce: Some(nonce),
        };
        // The toy's cancels are signed: a signer that signs none fails.
        let sig = t.span(PathStage::Sign, || self.signer.sign_cancel(&wire));
        let sig = sig.ok().flatten().ok_or(SignFailed)?;
        Ok(format!(
            "{}|sym={}|side={}|ts={}|nonce={nonce}|sig={}",
            named.field(),
            spec.venue_symbol.as_wire(),
            side(c.side),
            ctx.wall.0,
            String::from_utf8_lossy(sig.as_bytes()),
        ))
    }

    fn request(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        receipt: &mut EncodeReceipt,
        t: &mut PathStamps<'_>,
    ) -> Result<Request, NotSentReason> {
        let n = rpc.0;
        Ok(match cmd {
            VenueCommand::Place(o) => {
                self.check_place(o)?;
                let fields = self.place(0, o, specs, ctx, receipt, t)?;
                let charge = RateCharge::one(OpKind::Place, Some(o.inst));
                (format!("place|rpc={n}|{fields}"), charge)
            }
            VenueCommand::PlaceBatch(orders) => {
                let max = self.order.batch_place.map(|b| b.max_items);
                let weight = batch_weight(orders.len(), max)?;
                orders.iter().try_for_each(|o| self.check_place(o))?;
                let mut frame = format!("batch|rpc={n}|n={}", orders.len());
                for (item, o) in (0..).zip(orders) {
                    let fields = self.place(item, o, specs, ctx, receipt, t)?;
                    frame.push_str(&format!("\nplace|i={item}|{fields}"));
                }
                let inst = shared(orders.iter().map(|o| o.inst));
                let op = OpKind::Place;
                (frame, RateCharge { op, inst, weight })
            }
            VenueCommand::Amend(a) => self.amend(a, rpc, specs, ctx, t)?,
            VenueCommand::Cancel(c) => {
                let fields = self.cancel(0, c, self.order.cancel_refs, specs, ctx, t)?;
                let charge = RateCharge::one(OpKind::Cancel, Some(c.inst));
                (format!("cancel|rpc={n}|{fields}"), charge)
            }
            VenueCommand::CancelMany(cancels) => {
                let batch = self.order.batch_cancel.ok_or(Unsupported)?;
                let weight = batch_weight(cancels.len(), Some(batch.max_items))?;
                // Every item names a reference a batch cancel takes before any is signed.
                let named = |c: &CancelOrder| c.reference(batch.refs).is_some();
                cancels.iter().all(named).then_some(()).ok_or(Unsupported)?;
                let mut frame = format!("cancels|rpc={n}|n={}", cancels.len());
                for (item, c) in (0..).zip(cancels) {
                    let fields = self.cancel(item, c, batch.refs, specs, ctx, t)?;
                    frame.push_str(&format!("\ncancel|i={item}|{fields}"));
                }
                let inst = shared(cancels.iter().map(|c| c.inst));
                let op = OpKind::Cancel;
                (frame, RateCharge { op, inst, weight })
            }
            // The toy offers no account-wide cancel-all; its caps say so.
            VenueCommand::CancelAll(scope) => {
                let CancelScope::Instrument(inst) = *scope else {
                    return Err(Unsupported);
                };
                let spec = specs.get(inst).ok_or(Unencodable)?;
                let sym = spec.venue_symbol.as_wire();
                let charge = RateCharge::one(OpKind::CancelAll, Some(inst));
                (format!("cancelall|rpc={n}|sym={sym}"), charge)
            }
            // Cancel-on-disconnect protects the orders of the connection it is asked on, until it
            // drops; the toy has no dead-man timer, so it refreshes none.
            VenueCommand::ArmCancelOnDisconnect(on) => {
                let charge = RateCharge::one(OpKind::Control, None);
                (format!("cod|rpc={n}|on={}", u8::from(*on)), charge)
            }
            VenueCommand::RefreshDeadMan => return Err(Unsupported),
            VenueCommand::Query(q) => {
                let chosen = q.reference(self.order.query_refs).ok_or(Unsupported)?;
                let spec = specs.get(q.inst).ok_or(Unencodable)?;
                let named = self.name(chosen)?;
                let sym = spec.venue_symbol.as_wire();
                let charge = RateCharge::one(OpKind::Query, Some(q.inst));
                (format!("query|rpc={n}|{}|sym={sym}", named.field()), charge)
            }
            VenueCommand::FeeQuery => {
                let charge = RateCharge::one(OpKind::Query, None);
                (format!("fees|rpc={n}"), charge)
            }
        })
    }
}

/// The ping's next firing.
fn ping_timer() -> Effect {
    Effect::Timer {
        tag: PING_TAG,
        after: PING_EVERY,
    }
}

/// A batch's weight, its item count: refused when the venue takes no batch or it is longer than
/// the venue takes, and unencodable when empty.
fn batch_weight(
    len: usize,
    max_items: Option<u16>,
) -> Result<core::num::NonZeroU32, NotSentReason> {
    let max = max_items.ok_or(Unsupported)?;
    if len > usize::from(max) {
        return Err(Unsupported);
    }
    weight(len).ok_or(Unencodable)
}

/// The instrument every item names, or `None` for a batch that spans several.
fn shared(mut insts: impl Iterator<Item = InstrumentId>) -> Option<InstrumentId> {
    let first = insts.next()?;
    insts.all(|inst| inst == first).then_some(first)
}

fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "B",
        Side::Sell => "S",
    }
}

/// A tag in lower case: `gtc`, `public`.
fn lower(tag: impl core::fmt::Debug) -> String {
    format!("{tag:?}").to_lowercase()
}

/// The one reference a request names its order by, its client id spelled for the wire.
enum Named<'a> {
    Venue(&'a VenueOrderId),
    Client(WireCid),
    Nonce(u64),
}

impl Named<'_> {
    fn field(&self) -> String {
        match self {
            Named::Venue(vid) => format!("vid={}", vid.as_str()),
            Named::Client(cid) => format!("cid={cid}"),
            Named::Nonce(nonce) => format!("pnonce={nonce}"),
        }
    }

    fn cancel_ref(&self) -> CancelRef<'_> {
        match self {
            Named::Venue(vid) => CancelRef::Venue(vid),
            Named::Client(cid) => CancelRef::Client(cid),
            Named::Nonce(nonce) => CancelRef::PlacementNonce(*nonce),
        }
    }

    /// An amend names no placement nonce.
    fn amend_ref(&self) -> Option<AmendRef<'_>> {
        match self.cancel_ref() {
            CancelRef::Venue(vid) => Some(AmendRef::Venue(vid)),
            CancelRef::Client(cid) => Some(AmendRef::Client(cid)),
            CancelRef::PlacementNonce(_) => None,
        }
    }
}

impl ExecCodec for ToyExec {
    /// Only `encode` signs, and it takes its nonces from the runtime's reservation for it.
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// Authenticates with the toy's token in a redaction span; Authenticated waits for the
    /// venue's acknowledgement. A resync still being read or asked for was cut short by the
    /// reconnect. A pinging codec arms its ping.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.answers.ask_auth(stream, ctx, fx);
        self.opened = Some(stream);
        if self.ping {
            fx.push(ping_timer());
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
        let mut receipt = EncodeReceipt::new();
        let (frame, charge) = self.request(cmd, rpc, specs, ctx, &mut receipt, t)?;
        fx.push(Effect::Send {
            stream: EXEC_STREAM,
            frame: WireSlice::plain(frame.into_bytes()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: RPC_TIMEOUT,
            }),
            class: cmd.traffic_class(),
            charge,
        });
        self.answers.sent(rpc, cmd);
        Ok(receipt)
    }

    /// One record per frame, read whole before anything is pushed: the answers to requests,
    /// resyncs and authentication through the session it holds (`session.rs`), the rest as
    /// events (`decode.rs`). Nothing it receives asks for an effect.
    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let r = decode::record(f)?;
        let answers = &mut self.answers;
        match r.kind {
            "item" => answers.item(&r, scope, sink),
            "qres" => answers.query(&r, scope, specs, sink),
            "rsbegin" | "rsorder" | "rspos" | "rsend" => {
                answers.resync_frame(&r, scope, specs, sink)
            }
            "auth" => answers.auth(stream, &r, sink),
            _ => {
                let (meta, event) = decode::event(&r, &self.fills, scope, specs)?;
                // A refusal of the whole request answers every item.
                if let ExecEvent::Outcome { rpc, .. } = event {
                    answers.answered(rpc);
                }
                sink.push(meta, event);
                Ok(())
            }
        }
    }

    /// A resync over REST's response; a codec resyncing in frames asks for no HTTP.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if self.rest.is_none() {
            return Err(DecodeError::Malformed("the toy asks for no HTTP"));
        }
        self.answers.rest_answer(tag, resp, scope, specs, sink, fx)
    }

    /// The ping, sent and armed again; a resync over REST asked again on the timer carrying its
    /// failed request's tag. Any other tag asks nothing.
    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        match (tag, &self.rest, self.opened) {
            (PING_TAG, _, Some(stream)) if self.ping => {
                fx.push(Effect::Send {
                    stream,
                    frame: WireSlice::plain(format!("ping|ts={}", ctx.wall.0).into_bytes()),
                    rpc: None,
                    class: TrafficClass::Safety,
                    charge: RateCharge::one(OpKind::Control, None),
                });
                fx.push(ping_timer());
            }
            (PING_TAG, ..) => {}
            (_, Some(base), _) => self.answers.retry_rest(tag, base, ctx, fx),
            _ => {}
        }
    }

    /// The outcomes held for `rpc`'s answered items, then `Unknown` for every item still
    /// unanswered; `Unknown` for the whole request when none was answered (0014 item 3).
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.answers.timed_out(rpc, sink);
    }

    /// Asks for the open orders and positions as of `ctx.wall`, the resync's watermark: over
    /// REST when built with a base, otherwise in frames.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        match &self.rest {
            Some(base) => self.answers.ask_rest_resync(base, ctx, fx),
            None => self.answers.ask_resync(EXEC_STREAM, ctx, fx),
        }
    }

    /// The toy's token wherever a text frame carries one (the authentication acknowledgement
    /// echoes it), and the REST base's credentials wherever an HTTP response echoes them.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match (input, &self.rest) {
            (Inbound::Http(_, resp), Some(base)) => echoed(&resp, &secrets(base)),
            (Inbound::Http(..), None) => InboundSpans::NONE,
            (Inbound::Frame(_), _) => session::token_spans(input),
        }
    }
}
