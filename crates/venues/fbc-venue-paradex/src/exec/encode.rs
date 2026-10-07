//! Paradex order entry encoded as the socket's JSON-RPC 2.0 text frames (BT-402; decisions
//! 0002, 0004, 0014, 0018, 0054), from the full command, the spec table and the
//! [`EncodeCtx`] only: the encoder holds no order registry and reads no clock.
//!
//! Each frame is `{"jsonrpc":"2.0","method":<method>,"params":{..},"id":<rpc>}`, the envelope
//! the Java library's `ParadexOrderWebSocketClient.call` writes, with the params of
//! docs.paradex.trade's WebSocket method pages (`ws/web-socket-channels/<method>/<method>`):
//!
//! - `order.create`: `client_id` (our id in Paradex's UUID format, 0004), `market`, `side`,
//!   `signature_timestamp` (the context's wall time in milliseconds), `size` and `price` as
//!   exact decimal strings (`price` is `"0"` for a MARKET order, as the page requires),
//!   `type`, `instruction` (GTC, IOC, POST_ONLY or RPI, RPI whether or not the order also asks
//!   for post-only, 0054), `flags` `["REDUCE_ONLY"]` when the venue's reduce-only flag is set,
//!   and `signature` from [`ParadexSigner`](crate::sign::ParadexSigner) over the same market,
//!   side, type, size, price and timestamp; the Java library's `buildSignedPlaceOrderJson`
//!   writes the same fields.
//! - `order.create_batch`: `{"orders":[..]}`, each item as `order.create`'s params, each signed.
//! - `order.modify`: `id`, `market`, `price`, `side`, `signature` over the venue order id,
//!   `signature_timestamp`, the total `size` (0054's `AmendQty::TotalIncludingFilled`) and
//!   `type` LIMIT, as `buildSignedModifyOrderJson` writes them.
//! - `order.cancel`: `{"id":..}` by venue id, or `{"client_id":..,"market":..}`;
//!   `order.cancel_batch`: `{"order_ids":[..]}`, venue ids only; `order.cancel_all`: `{}` for
//!   the account or `{"market":..}`; all unsigned. An instrument cancel-all is never widened.
//! - `order.cancel_on_disconnect`: `{"enabled":true}` or `{"enabled":false}`.
//!
//! Every frame names its rpc with the consumer's request timeout, is labelled with the
//! command's traffic class ([`VenueCommand::traffic_class`]) and carries its [`RateCharge`]: a
//! batch is charged its item count (0054), cancel-on-disconnect is a control frame. Each signer
//! call is marked as [`PathStage::Sign`] (0034). Paradex takes no nonce, so the receipt is
//! always empty.
//!
//! Before anything is signed, an order (a placement, a batch item or an amend) of a time in
//! force, kind or channel the caps do not declare is `NotSent(Unsupported)`, as is an amend or a
//! cancel whose references are undeclared for it (a batch cancel takes venue ids only), a batch
//! longer than its declared maximum, a market order that is post-only or RPI, a dead-man refresh
//! and a fee query; an order combining a pair the caps declare in conflict is
//! `NotSent(FlagConflict)`. What cannot be written is `Unencodable`: an instrument missing from
//! the spec table, an empty batch, a size of zero or out of range, a price off the grid, a wall
//! time before 1970, and an amend to its filled quantity or below. A signer that fails is
//! `SignFailed`. A refusal pushes no effect.
//!
//! An order query is not a frame: the order-entry codec (FBC-xvf) builds it as the REST
//! request FBC-0sc defines, so this encoder refuses it as `NotSent(Unsupported)`.

use core::time::Duration;

use fbc_core::{
    AmendOrder, AmendRef, AmendWire, CancelOrder, CancelScope, Channel, ChosenRef, ClientOrderId,
    Effect, Effects, EncodeCtx, EncodeReceipt, Feature, InstrumentId, InstrumentSpec, Lots,
    NewOrder, NotSentReason, OpKind, OrderCaps, OrderKind, OrderKindTag, OrderSigner, PathStage,
    PathStamps, PlaceWire, RateCharge, RpcCall, RpcId, Side, SpecTable, StreamId, Tif,
    VenueCommand, WallNs, WireCid, WireSlice, encode_cid,
};
use rust_decimal::Decimal;

use super::exec_caps;

use NotSentReason::{FlagConflict, SignFailed, Unencodable, Unsupported};

/// Encodes Paradex order-entry commands as JSON-RPC frames on one stream, signing through the
/// [`OrderSigner`] it is given under the order capabilities [`exec_caps`] declares.
pub struct ParadexEncoder {
    order: OrderCaps,
    signer: Box<dyn OrderSigner>,
    stream: StreamId,
    rpc_timeout: Duration,
}

impl core::fmt::Debug for ParadexEncoder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ParadexEncoder")
            .field("stream", &self.stream)
            .field("rpc_timeout", &self.rpc_timeout)
            .finish_non_exhaustive()
    }
}

/// A request's method, its params object, and the charge it carries.
struct Request {
    method: &'static str,
    params: String,
    charge: RateCharge,
}

impl ParadexEncoder {
    /// An encoder writing its frames to `stream`, each request awaiting its answer for
    /// `rpc_timeout` (the consumer's configuration), signed by `signer`.
    pub fn new(
        signer: Box<dyn OrderSigner>,
        stream: StreamId,
        rpc_timeout: Duration,
    ) -> ParadexEncoder {
        ParadexEncoder {
            order: exec_caps().order,
            signer,
            stream,
            rpc_timeout,
        }
    }

    /// Encodes and signs `cmd` as request `rpc`, as [`fbc_core::ExecCodec::encode`] does: on
    /// `Ok`, one [`Effect::Send`] carrying the request; on `Err`, nothing pushed.
    pub fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let req = self.request(cmd, specs, ctx, t)?;
        let frame = format!(
            r#"{{"jsonrpc":"2.0","method":"{}","params":{},"id":{}}}"#,
            req.method, req.params, rpc.0
        );
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(frame.into_bytes()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: self.rpc_timeout,
            }),
            class: cmd.traffic_class(),
            charge: req.charge,
        });
        Ok(EncodeReceipt::new())
    }

    fn request(
        &mut self,
        cmd: &VenueCommand,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> Result<Request, NotSentReason> {
        Ok(match cmd {
            VenueCommand::Place(o) => {
                self.check_place(o)?;
                Request {
                    method: "order.create",
                    params: self.place(o, specs, ctx, t)?,
                    charge: RateCharge::one(OpKind::Place, Some(o.inst)),
                }
            }
            VenueCommand::PlaceBatch(orders) => {
                let max = self.order.batch_place.map(|b| b.max_items);
                let weight = batch_weight(orders.len(), max)?;
                orders.iter().try_for_each(|o| self.check_place(o))?;
                let items = orders
                    .iter()
                    .map(|o| self.place(o, specs, ctx, t))
                    .collect::<Result<Vec<_>, _>>()?;
                let inst = shared(orders.iter().map(|o| o.inst));
                Request {
                    method: "order.create_batch",
                    params: format!(r#"{{"orders":[{}]}}"#, items.join(",")),
                    charge: RateCharge {
                        op: OpKind::Place,
                        inst,
                        weight,
                    },
                }
            }
            VenueCommand::Amend(a) => Request {
                method: "order.modify",
                params: self.amend(a, specs, ctx, t)?,
                charge: RateCharge::one(OpKind::Amend, Some(a.inst)),
            },
            VenueCommand::Cancel(c) => Request {
                method: "order.cancel",
                params: self.cancel(c, specs)?,
                charge: RateCharge::one(OpKind::Cancel, Some(c.inst)),
            },
            VenueCommand::CancelMany(cancels) => {
                let batch = self.order.batch_cancel.ok_or(Unsupported)?;
                let weight = batch_weight(cancels.len(), Some(batch.max_items))?;
                // Every item names a venue id, the one reference order.cancel_batch takes.
                let ids = cancels
                    .iter()
                    .map(|c| match c.reference(batch.refs) {
                        Some(ChosenRef::Venue(vid)) => Ok(json_str(vid.as_str())),
                        _ => Err(Unsupported),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let inst = shared(cancels.iter().map(|c| c.inst));
                Request {
                    method: "order.cancel_batch",
                    params: format!(r#"{{"order_ids":[{}]}}"#, ids.join(",")),
                    charge: RateCharge {
                        op: OpKind::Cancel,
                        inst,
                        weight,
                    },
                }
            }
            VenueCommand::CancelAll(scope) => match *scope {
                CancelScope::Instrument(inst) => {
                    let spec = spec(specs, inst)?;
                    Request {
                        method: "order.cancel_all",
                        params: format!(r#"{{"market":{}}}"#, market(spec)),
                        charge: RateCharge::one(OpKind::CancelAll, Some(inst)),
                    }
                }
                // The account one is encoded because the caps declare it Native (decision
                // 0054) and the conformance suite holds a Native capability to being sent. It
                // never reaches this codec: `ExecOrders` takes an order-affecting command only
                // as the grant fbc-oms issues for it (0045, 0057), which it refuses here
                // (`IssueRefusal::AccountCancelAll`), and a `ControlCommand` has no cancel-all.
                CancelScope::Account => Request {
                    method: "order.cancel_all",
                    params: "{}".to_owned(),
                    charge: RateCharge::one(OpKind::CancelAll, None),
                },
            },
            VenueCommand::ArmCancelOnDisconnect(on) => Request {
                method: "order.cancel_on_disconnect",
                params: format!(r#"{{"enabled":{on}}}"#),
                charge: RateCharge::one(OpKind::Control, None),
            },
            VenueCommand::RefreshDeadMan | VenueCommand::FeeQuery | VenueCommand::Query(_) => {
                return Err(Unsupported);
            }
        })
    }

    /// Refuses an order of a time in force, kind or channel the caps do not declare, or with a
    /// flag they do not offer (`Unsupported`), then one combining a pair of features the caps
    /// declare in conflict (`FlagConflict`).
    fn declared(
        &self,
        kind: OrderKindTag,
        tif: Tif,
        channel: Channel,
        (post_only, reduce_only): (bool, bool),
    ) -> Result<(), NotSentReason> {
        let c = &self.order;
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

    /// Refuses a placement [`Self::declared`] refuses, and a market order that is post-only or
    /// on the RPI channel: both instructions rest on the book, which a market order never does
    /// (docs.paradex.trade `trading/orders/order-instructions`), and the caps cannot pair a
    /// feature with a kind.
    fn check_place(&self, o: &NewOrder) -> Result<(), NotSentReason> {
        self.declared(o.kind.tag(), o.tif, o.channel, (o.post_only, o.reduce_only))?;
        let resting = o.post_only || o.channel == Channel::Rpi;
        let market = matches!(o.kind, OrderKind::Market);
        (!(market && resting)).then_some(()).ok_or(Unsupported)
    }

    fn cid(&self, cid: ClientOrderId) -> Result<WireCid, NotSentReason> {
        encode_cid(&self.order.client_id, cid).map_err(|_| Unencodable)
    }

    /// One `order.create` params object, signed.
    fn place(
        &mut self,
        o: &NewOrder,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> Result<String, NotSentReason> {
        let spec = spec(specs, o.inst)?;
        let cid = self.cid(o.cid)?;
        let (order_type, price) = match o.kind {
            OrderKind::Limit { px } => ("LIMIT", price(spec, px)?),
            OrderKind::Market => ("MARKET", Decimal::ZERO),
        };
        let size = size(spec, o.qty)?;
        let ts = timestamp_ms(ctx.wall)?;
        let instruction = instruction(o.tif, o.channel, o.post_only)?;
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
            nonce: None,
        };
        let sig = t.span(PathStage::Sign, || self.signer.sign_place(&wire));
        let sig = sig.map_err(|_| SignFailed)?;
        let flags = if o.reduce_only {
            r#","flags":["REDUCE_ONLY"]"#
        } else {
            ""
        };
        Ok(format!(
            r#"{{"client_id":{},"market":{},"side":"{}","signature_timestamp":{ts},"size":"{}","type":"{order_type}","price":"{}","instruction":"{instruction}"{flags},"signature":{}}}"#,
            json_str(cid.as_str()),
            market(spec),
            side(o.side),
            wire_decimal(size),
            wire_decimal(price),
            sig_str(sig.as_bytes())?,
        ))
    }

    /// The `order.modify` params object, signed over the venue order id.
    fn amend(
        &mut self,
        a: &AmendOrder,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> Result<String, NotSentReason> {
        // `order.modify` writes no instruction or flags (caps `amend.flags` false): the order
        // keeps the ones it was placed with. The amend's tif, channel and flags are that order's
        // (fbc-oms builds them from the placed order and never amends flags), so they are
        // checked against the caps here and not written.
        let caps = self.order.amend.ok_or(Unsupported)?;
        self.declared(
            OrderKindTag::Limit,
            a.tif,
            a.channel,
            (a.post_only, a.reduce_only),
        )?;
        let Some(ChosenRef::Venue(vid)) = a.reference(&caps) else {
            return Err(Unsupported);
        };
        let qty = a.wire_qty(caps.qty_semantics).ok_or(Unencodable)?;
        let spec = spec(specs, a.inst)?;
        let price = price(spec, a.px)?;
        let size = size(spec, qty)?;
        let ts = timestamp_ms(ctx.wall)?;
        let wire = AmendWire {
            spec,
            target: AmendRef::Venue(vid),
            side: a.side,
            px: a.px,
            qty,
            tif: a.tif,
            channel: a.channel,
            post_only: a.post_only,
            reduce_only: a.reduce_only,
            wall: ctx.wall,
            nonce: None,
        };
        let sig = t.span(PathStage::Sign, || self.signer.sign_amend(&wire));
        let sig = sig.map_err(|_| SignFailed)?;
        Ok(format!(
            r#"{{"id":{},"market":{},"price":"{}","side":"{}","signature":{},"signature_timestamp":{ts},"size":"{}","type":"LIMIT"}}"#,
            json_str(vid.as_str()),
            market(spec),
            wire_decimal(price),
            side(a.side),
            sig_str(sig.as_bytes())?,
            wire_decimal(size),
        ))
    }

    /// The `order.cancel` params object: by venue id, or by client id with its market.
    fn cancel(&self, c: &CancelOrder, specs: &SpecTable) -> Result<String, NotSentReason> {
        match c.reference(self.order.cancel_refs).ok_or(Unsupported)? {
            ChosenRef::Venue(vid) => Ok(format!(r#"{{"id":{}}}"#, json_str(vid.as_str()))),
            ChosenRef::Client(cid) => {
                let spec = spec(specs, c.inst)?;
                let cid = self.cid(cid)?;
                Ok(format!(
                    r#"{{"client_id":{},"market":{}}}"#,
                    json_str(cid.as_str()),
                    market(spec)
                ))
            }
            ChosenRef::PlacementNonce(_) => Err(Unsupported),
        }
    }
}

/// The instruction an order's time in force, channel and post-only flag make: RPI for an order
/// on the RPI channel (post-only by the venue's rule), then POST_ONLY, then the time in force.
/// Fill-or-kill has no instruction and is refused first, whatever the channel or flag, so it is
/// never written as a resting order even if the caps came to declare it.
fn instruction(tif: Tif, channel: Channel, post_only: bool) -> Result<&'static str, NotSentReason> {
    Ok(match (tif, channel, post_only) {
        (Tif::Fok, _, _) => return Err(Unsupported),
        (_, Channel::Rpi, _) => "RPI",
        (_, Channel::Public, true) => "POST_ONLY",
        (Tif::Gtc, Channel::Public, false) => "GTC",
        (Tif::Ioc, Channel::Public, false) => "IOC",
    })
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
    u32::try_from(len)
        .ok()
        .and_then(core::num::NonZeroU32::new)
        .ok_or(Unencodable)
}

/// The instrument every item names, or `None` for a batch that spans several.
fn shared(mut insts: impl Iterator<Item = InstrumentId>) -> Option<InstrumentId> {
    let first = insts.next()?;
    insts.all(|inst| inst == first).then_some(first)
}

fn spec(specs: &SpecTable, inst: InstrumentId) -> Result<&InstrumentSpec, NotSentReason> {
    specs.get(inst).ok_or(Unencodable)
}

/// The market symbol as a JSON string.
fn market(spec: &InstrumentSpec) -> String {
    json_str(spec.venue_symbol.as_wire())
}

/// `qty` size steps in the instrument's units; zero is no order.
fn size(spec: &InstrumentSpec, qty: Lots) -> Result<Decimal, NotSentReason> {
    if qty.get() == 0 {
        return Err(Unencodable);
    }
    Decimal::from(qty.get())
        .checked_mul(spec.size_step.get())
        .ok_or(Unencodable)
}

/// The exact, positive price of tick index `px`.
fn price(spec: &InstrumentSpec, px: fbc_core::Ticks) -> Result<Decimal, NotSentReason> {
    let price = spec
        .price_grid
        .px_of(px)
        .and_then(|p| p.to_decimal())
        .ok_or(Unencodable)?;
    (price > Decimal::ZERO).then_some(price).ok_or(Unencodable)
}

/// A decimal as the wire writes it: exact, without trailing zeros or an exponent.
fn wire_decimal(d: Decimal) -> String {
    d.normalize().to_string()
}

/// The signature time: `wall` in whole milliseconds since the epoch.
fn timestamp_ms(wall: WallNs) -> Result<u64, NotSentReason> {
    u64::try_from(wall.0)
        .map(|ns| ns / 1_000_000)
        .map_err(|_| Unencodable)
}

fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

/// `text` as a JSON string, quoted and escaped.
fn json_str(text: &str) -> String {
    serde_json::Value::from(text).to_string()
}

/// The signer's wire form (`["r","s"]`) as the JSON string the `signature` field carries.
fn sig_str(sig: &[u8]) -> Result<String, NotSentReason> {
    core::str::from_utf8(sig)
        .map(json_str)
        .map_err(|_| SignFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instruction_puts_rpi_first_then_post_only_then_the_time_in_force() {
        assert_eq!(instruction(Tif::Gtc, Channel::Rpi, false), Ok("RPI"));
        assert_eq!(instruction(Tif::Gtc, Channel::Rpi, true), Ok("RPI"));
        assert_eq!(
            instruction(Tif::Gtc, Channel::Public, true),
            Ok("POST_ONLY")
        );
        assert_eq!(instruction(Tif::Gtc, Channel::Public, false), Ok("GTC"));
        assert_eq!(instruction(Tif::Ioc, Channel::Public, false), Ok("IOC"));
        assert_eq!(
            instruction(Tif::Fok, Channel::Public, false),
            Err(Unsupported)
        );
    }

    #[test]
    fn a_fill_or_kill_order_is_unsupported_on_any_channel_post_only_or_not() {
        // No Paradex instruction kills an unfilled order whole: a post-only or RPI FOK must not
        // be written as a resting POST_ONLY or RPI order (DeepSeek DS-2 on 66235b2).
        for (channel, post_only) in [
            (Channel::Public, false),
            (Channel::Public, true),
            (Channel::Rpi, false),
            (Channel::Rpi, true),
        ] {
            assert_eq!(
                instruction(Tif::Fok, channel, post_only),
                Err(Unsupported),
                "{channel:?} post_only={post_only}"
            );
        }
    }

    #[test]
    fn a_wire_decimal_is_exact_without_trailing_zeros() {
        let d = |s: &str| s.parse::<Decimal>().unwrap();
        assert_eq!(wire_decimal(d("62000.0")), "62000");
        assert_eq!(wire_decimal(d("0.00500")), "0.005");
        assert_eq!(wire_decimal(Decimal::ZERO), "0");
        assert_eq!(
            wire_decimal(d("123456789.123456789")),
            "123456789.123456789"
        );
    }

    #[test]
    fn a_signature_that_is_not_text_is_a_sign_failure() {
        assert_eq!(sig_str(&[0xff, 0xfe]), Err(SignFailed));
        assert_eq!(sig_str(br#"["1","2"]"#).unwrap(), r#""[\"1\",\"2\"]""#);
    }

    #[test]
    fn a_wall_time_before_1970_has_no_signature_timestamp() {
        assert_eq!(timestamp_ms(WallNs(-1)), Err(Unencodable));
        assert_eq!(timestamp_ms(WallNs(1_999_999)), Ok(1));
    }

    #[test]
    fn a_batch_weighs_its_items_within_the_declared_bound() {
        assert_eq!(batch_weight(3, Some(10)).map(|w| w.get()), Ok(3));
        assert_eq!(batch_weight(11, Some(10)), Err(Unsupported));
        assert_eq!(batch_weight(1, None), Err(Unsupported));
        assert_eq!(batch_weight(0, Some(10)), Err(Unencodable));
    }
}
