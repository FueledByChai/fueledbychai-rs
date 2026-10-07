//! Paradex's REST reads for order entry (BT-402, decisions 0005, 0013, 0014): the resync, which
//! re-reads the account's open orders and positions (0013 rule 1), and the Unknown ladder's
//! query of one order by our client id (0005). Each comes in pieces an order-entry codec puts
//! together itself, the requests as effects ([`resync_requests`], [`query_request`]) and the
//! decoders of their answers ([`decode_resync`], [`decode_order_query`]), and as one
//! [`HttpPlan`] built from them ([`resync_plan`], [`query_plan`]). Either way the answers decode
//! into a [`RestAnswer`], the events to push.
//!
//! The requests, from docs.paradex.trade's REST pages, under the REST base (`…/v1`):
//! - "Get open orders": `GET /orders`, every market (its optional `market` query is not sent).
//!   The answer is `results`, a list of orders (`OrderResp`).
//! - "List open positions": `GET /positions`, no query. The answer is `results`, a list of
//!   positions (`PositionResp`).
//! - "Get orders history": `GET /orders-history?client_id=`, our client id in its wire format
//!   (the Java library's `getOrderByClientOrderId`). Unlike `GET /orders/by_client_id/{id}`,
//!   which answers open orders only, it finds a closed order too, so a filled or cancelled
//!   order is answered with its terminal state, never reported absent. The answer is `results`
//!   (with the `next` and `prev` cursors, not read). An order it does not list is absent; the
//!   Unknown ladder leaves such an order to resyncs rather than end it (0005), so a history that
//!   has not caught up with a fresh order ends nothing.
//!
//! Every request is built without the session token, carrying exactly the headers its caller
//! gives: FBC-xvf's codec passes the current token's header from `src/auth` (0009), so this
//! module never sees one. The reads are safety traffic, as the query's command is
//! ([`VenueCommand::traffic_class`]); the query names its request (`rpc`), the resync does not.
//!
//! What an order states (`OrderResp`): `id` the venue id, through [`DecodeScope`]; `client_id`
//! our client id, through [`DecodeScope::client_order_id`], an empty or missing one none;
//! `market` the instrument, refused when the spec table does not hold it; `side` BUY or SELL;
//! `type` LIMIT or MARKET (the stop and take-profit types are not modelled, 0054, and refused);
//! `instruction` GTC or IOC, or POST_ONLY or RPI, both post-only (0054); `price` on the
//! instrument's grid and above zero, or none for a MARKET order, whose price must be "0";
//! `size` the total and `remaining_size` what is open, on the size step, the cumulative fill
//! their difference; `flags` holding `REDUCE_ONLY` or not (no flags stated: not echoed); and
//! `status` NEW or OPEN resting, CLOSED ended as the order event reads it (by its
//! `cancel_reason`, filled when none is and nothing is open), UNTRIGGERED not modelled and
//! refused. The other fields (prices filled at, times, `seq_no`, `stp`, `request_info`) are not
//! read. A resync lists open orders only: a CLOSED one in `GET /orders` is refused.
//!
//! What a position states (`PositionResp`): `size` "with sign (positive if long or negative if
//! short)", on the size step, its sign the `side`'s (LONG or SHORT); `average_entry_price`
//! exactly as sent, above zero; `status` OPEN or CLOSED, CLOSED only when flat. A flat position
//! (size zero, CLOSED or not: the page lists closed positions too) is not listed at all, its
//! market not looked up: an instrument a resync does not list is flat (0049). A
//! market listed twice is refused. Every event carries [`VenueMeta::NONE`]: a REST snapshot is
//! on no stream's sequence.
//!
//! Each answer is decoded whole before anything is pushed (0014 item 2): the resync's two
//! answers decode into `ResyncBegin`, a `ResyncOrder` per open order, a `ResyncPosition` per
//! position held and `ResyncEnd`, or into a refusal and no event at all. Two caveats the
//! answer cannot remove: an open order or position held in a market missing from the spec
//! table refuses the whole resync (the session cannot place what it holds there); and the two
//! reads are not one instant, so a fill can land between them (FBC-k7t7 makes that declared;
//! Paradex's `snapshot_source` stays `Untrustworthy` meanwhile, 0054).

use core::time::Duration;
use std::collections::BTreeSet;

use fbc_core::{
    ChosenRef, CidMatch, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, ExecEvent, ExecSink,
    Header, HttpMethod, HttpPlan, HttpRequest, HttpTag, InstrumentId, InstrumentSpec, Lots,
    NotSentReason, OpKind, PlanError, PxExact, QueryAnswer, QueryOrder, RateCharge, RpcId, Side,
    SignedLots, SpecTable, Ticks, TrafficClass, VenueCommand, VenueMeta, VenueOrderSnapshot,
    VenueOrderState, WallNs, WireSlice, WireUrl, encode_cid,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use super::caps::exec_caps;
use super::order::closed;
use crate::md::{lots_of, market};

/// The request `GET {base}/orders`: the account's open orders in every market. No header: the
/// caller adds the session's.
pub fn open_orders_request(base: &str) -> HttpRequest {
    get(format!("{base}/orders"))
}

/// The request `GET {base}/positions`: the account's positions. No header, as
/// [`open_orders_request`].
pub fn positions_request(base: &str) -> HttpRequest {
    get(format!("{base}/positions"))
}

/// The request `GET {base}/orders-history?client_id={client_id}`: the account's order with
/// that client id, open or closed. `client_id` is the wire format's (a UUID, whose characters
/// need no escaping in a query). No header, as [`open_orders_request`].
pub fn order_history_request(base: &str, client_id: &str) -> HttpRequest {
    get(format!("{base}/orders-history?client_id={client_id}"))
}

fn get(url: String) -> HttpRequest {
    HttpRequest {
        method: HttpMethod::Get,
        url: WireUrl::plain(url),
        headers: Vec::new(),
        body: WireSlice::plain(Vec::new()),
    }
}

/// The tags a resync's two reads go out under: the codec routes their answers by them.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ResyncTags {
    pub orders: HttpTag,
    pub positions: HttpTag,
}

/// What a REST read's answers decoded into: the events to push, in order, every one decoded
/// before any is pushed.
#[derive(Clone, PartialEq, Debug)]
pub struct RestAnswer {
    events: Vec<(VenueMeta, ExecEvent)>,
}

impl RestAnswer {
    /// The events, in the order they are pushed.
    pub fn events(&self) -> &[(VenueMeta, ExecEvent)] {
        &self.events
    }

    /// Pushes every event into `sink`, in order.
    pub fn push_into(self, sink: &mut dyn ExecSink) {
        for (meta, ev) in self.events {
            sink.push(meta, ev);
        }
    }

    fn of(events: Vec<ExecEvent>) -> RestAnswer {
        let events = events.into_iter().map(|ev| (VenueMeta::NONE, ev)).collect();
        RestAnswer { events }
    }
}

/// A resync's two reads, as effects: [`open_orders_request`] tagged `tags.orders`, then
/// [`positions_request`] tagged `tags.positions`, each carrying `headers`, waiting `timeout`,
/// on the safety floor and charged as a query of no one instrument.
pub fn resync_requests(
    base: &str,
    headers: &[Header],
    timeout: Duration,
    tags: ResyncTags,
) -> Effects {
    let mut fx = Effects::new();
    for (tag, mut req) in [
        (tags.orders, open_orders_request(base)),
        (tags.positions, positions_request(base)),
    ] {
        req.headers = headers.to_vec();
        fx.push(Effect::Http {
            tag,
            req,
            rpc: None,
            timeout,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Query, None),
        });
    }
    fx
}

/// Query `query`'s request `rpc`, as one effect: [`order_history_request`] by its client id,
/// tagged `tag`, carrying `headers`, waiting `timeout`, in the query's traffic class and
/// charged as a query of its instrument. [`NotSentReason::Unsupported`] when the query names
/// no client id (Paradex's caps declare queries by client id only, 0054), and
/// [`NotSentReason::Unencodable`] when that id does not fit the caps' client-id format.
pub fn query_request(
    query: &QueryOrder,
    rpc: RpcId,
    base: &str,
    headers: &[Header],
    timeout: Duration,
    tag: HttpTag,
) -> Result<Effects, NotSentReason> {
    let caps = exec_caps();
    let Some(ChosenRef::Client(cid)) = query.reference(caps.order.query_refs) else {
        return Err(NotSentReason::Unsupported);
    };
    let wire = encode_cid(&caps.order.client_id, cid).map_err(|_| NotSentReason::Unencodable)?;
    let mut req = order_history_request(base, &wire);
    req.headers = headers.to_vec();
    let mut fx = Effects::new();
    fx.push(Effect::Http {
        tag,
        req,
        rpc: Some(rpc),
        timeout,
        class: VenueCommand::Query(query.clone()).traffic_class(),
        charge: RateCharge::one(OpKind::Query, Some(query.inst)),
    });
    Ok(fx)
}

/// The resync as one plan: [`resync_requests`] in one round, their answers read by
/// [`decode_resync`] against `specs` with the watermark `ctx.wall`, the time the requests are
/// built at (0014's Alternatives). Refused when the two tags are one
/// ([`PlanError::DuplicateTag`]).
pub fn resync_plan(
    base: &str,
    headers: &[Header],
    timeout: Duration,
    tags: ResyncTags,
    ctx: &EncodeCtx,
    specs: &SpecTable,
) -> Result<HttpPlan<RestAnswer>, PlanError> {
    let fx = resync_requests(base, headers, timeout, tags);
    let (watermark, specs) = (ctx.wall, specs.clone());
    HttpPlan::new(fx, move |responses, scope| {
        let [orders, positions] = responses else {
            return Err(PlanError::Answers);
        };
        Ok(decode_resync(
            watermark,
            orders.body,
            positions.body,
            scope,
            &specs,
        )?)
    })
}

/// The query as one plan: [`query_request`] in one round, its answer read by
/// [`decode_order_query`] against `specs`. Refused as [`query_request`] is, and
/// [`NotSentReason::Unencodable`] when `specs` does not hold the query's instrument.
pub fn query_plan(
    query: &QueryOrder,
    rpc: RpcId,
    base: &str,
    headers: &[Header],
    timeout: Duration,
    tag: HttpTag,
    specs: &SpecTable,
) -> Result<HttpPlan<RestAnswer>, NotSentReason> {
    if specs.get(query.inst).is_none() {
        return Err(NotSentReason::Unencodable);
    }
    let fx = query_request(query, rpc, base, headers, timeout, tag)?;
    let (query, specs) = (query.clone(), specs.clone());
    // One request under one tag: the plan refuses neither.
    HttpPlan::new(fx, move |responses, scope| {
        let [history] = responses else {
            return Err(PlanError::Answers);
        };
        Ok(decode_order_query(
            rpc,
            &query,
            history.body,
            scope,
            &specs,
        )?)
    })
    .map_err(|_| NotSentReason::Unencodable)
}

/// Decodes a resync's answers, `GET /orders` and `GET /positions`, into `ResyncBegin` at
/// `watermark`, a `ResyncOrder` per open order, a `ResyncPosition` per position held and
/// `ResyncEnd`. Refused, naming the part, when either answer is not what the module
/// documentation says it states: then nothing is pushed.
pub fn decode_resync(
    watermark: WallNs,
    orders: &[u8],
    positions: &[u8],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<RestAnswer, DecodeError> {
    let mut events = vec![ExecEvent::ResyncBegin { watermark }];
    for order in &results(orders, ORDERS)? {
        let snap = snapshot(order, scope, specs)?;
        if snap.state != VenueOrderState::Open {
            return Err(DecodeError::Malformed("orders: an order that is not open"));
        }
        events.push(ExecEvent::ResyncOrder(snap));
    }
    let mut listed = BTreeSet::new();
    for position in &results(positions, POSITIONS)? {
        let Some((inst, qty, avg_entry)) = position_of(position, specs)? else {
            continue;
        };
        if !listed.insert(inst) {
            return Err(DecodeError::Malformed("positions: a market listed twice"));
        }
        events.push(ExecEvent::ResyncPosition {
            inst,
            qty,
            avg_entry: Some(avg_entry),
        });
    }
    events.push(ExecEvent::ResyncEnd);
    Ok(RestAnswer::of(events))
}

/// Decodes the answer to query `query`, sent as request `rpc`, from `GET /orders-history`:
/// a `QueryResult` naming `rpc` and the query's target, with the order found, open or closed,
/// or none when the answer lists none. Refused, naming the part, when the answer is not what
/// the module documentation says it states, lists more than one order, or lists an order that
/// is not the one queried (another client id or venue id, or another market).
pub fn decode_order_query(
    rpc: RpcId,
    query: &QueryOrder,
    body: &[u8],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<RestAnswer, DecodeError> {
    let found = match results(body, HISTORY)?.as_slice() {
        [] => None,
        [order] => Some(snapshot(order, scope, specs)?),
        _ => {
            return Err(DecodeError::Malformed(
                "orders-history: more than one order",
            ));
        }
    };
    let another = DecodeError::Malformed("orders-history: the order is not the one queried");
    if let Some(snap) = &found {
        if snap.inst != query.inst {
            return Err(DecodeError::Malformed(
                "orders-history: the order is in another market",
            ));
        }
        // Paradex filters by the client id: the order listed must carry ours.
        let ours = query.target.client().map(CidMatch::Ours);
        if snap.cid.is_none() || snap.cid != ours {
            return Err(another);
        }
    }
    let answer = QueryAnswer::new(rpc, query.target.clone(), found).ok_or(another)?;
    Ok(RestAnswer::of(vec![ExecEvent::QueryResult(answer)]))
}

/// The refusals of an answer that is not an object, or holds no `results` list.
type Shape = (&'static str, &'static str);

const ORDERS: Shape = ("orders: not a JSON object", "orders: results");
const POSITIONS: Shape = ("positions: not a JSON object", "positions: results");
const HISTORY: Shape = (
    "orders-history: not a JSON object",
    "orders-history: results",
);

/// The `results` list of an answer, refused as `shape` names.
fn results(body: &[u8], (not_object, no_results): Shape) -> Result<Vec<Value>, DecodeError> {
    let doc: Value =
        serde_json::from_slice(body).map_err(|_| DecodeError::Malformed(not_object))?;
    let Value::Object(mut doc) = doc else {
        return Err(DecodeError::Malformed(not_object));
    };
    match doc.remove("results") {
        Some(Value::Array(list)) => Ok(list),
        _ => Err(DecodeError::Malformed(no_results)),
    }
}

/// The text of field `key`, refused as `what` when it is missing or not a string.
fn text<'v>(
    obj: &'v Map<String, Value>,
    key: &str,
    what: &'static str,
) -> Result<&'v str, DecodeError> {
    optional(obj, key, what)?.ok_or(DecodeError::Malformed(what))
}

/// The text of field `key`, `None` when it is missing or null; refused as `what` when it is
/// anything but a string.
fn optional<'v>(
    obj: &'v Map<String, Value>,
    key: &str,
    what: &'static str,
) -> Result<Option<&'v str>, DecodeError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(DecodeError::Malformed(what)),
    }
}

/// A decimal string, refused as `what` when it is not one.
fn decimal(text: &str, what: &'static str) -> Result<Decimal, DecodeError> {
    Decimal::from_str_exact(text).map_err(|_| DecodeError::Malformed(what))
}

/// One order of `GET /orders` or `GET /orders-history`, as the module documentation reads it.
fn snapshot(
    order: &Value,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<VenueOrderSnapshot, DecodeError> {
    let order = order
        .as_object()
        .ok_or(DecodeError::Malformed("order: not a JSON object"))?;
    let spec = market(text(order, "market", "order market")?, specs)?;
    let vid = scope.venue_order_id(text(order, "id", "order id")?)?;
    let cid = match optional(order, "client_id", "order client id")? {
        None | Some("") => None,
        Some(wire) => Some(scope.client_order_id(wire)),
    };
    let side = match text(order, "side", "order side")? {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return Err(DecodeError::Malformed("order side")),
    };
    let market_order = match text(order, "type", "order type not modelled")? {
        "LIMIT" => false,
        "MARKET" => true,
        // The stop and take-profit types are not modelled (0054).
        _ => return Err(DecodeError::Malformed("order type not modelled")),
    };
    let post_only = match text(order, "instruction", "order instruction")? {
        "GTC" | "IOC" => false,
        // RPI is post-only by the venue's rule (0054).
        "POST_ONLY" | "RPI" => true,
        _ => return Err(DecodeError::Malformed("order instruction")),
    };
    let px = order_price(spec, text(order, "price", "order price")?, market_order)?;
    let qty = size(spec, text(order, "size", "order size")?, "order size")?;
    let remaining = text(order, "remaining_size", "order remaining size")?;
    let open = size(spec, remaining, "order remaining size")?;
    let cum_filled = qty.checked_sub(open).ok_or(DecodeError::Malformed(
        "order remaining size above its size",
    ))?;
    let reduce_only = match order.get("flags") {
        None | Some(Value::Null) => None,
        Some(Value::Array(flags)) => {
            let mut reduce_only = false;
            for flag in flags {
                let flag = flag.as_str().ok_or(DecodeError::Malformed("order flags"))?;
                reduce_only |= flag == "REDUCE_ONLY";
            }
            Some(reduce_only)
        }
        Some(_) => return Err(DecodeError::Malformed("order flags")),
    };
    let reason = optional(order, "cancel_reason", "order cancel reason")?.unwrap_or("");
    let state = match text(order, "status", "order status not modelled")? {
        "NEW" | "OPEN" => VenueOrderState::Open,
        "CLOSED" => closed(reason, open.get() == 0),
        // UNTRIGGERED (a stop order's) is not modelled (0054).
        _ => return Err(DecodeError::Malformed("order status not modelled")),
    };
    Ok(VenueOrderSnapshot {
        cid,
        vid,
        inst: spec.id,
        side,
        state,
        px,
        qty,
        cum_filled,
        post_only: Some(post_only),
        reduce_only,
    })
}

/// An order's price: on the grid and above zero for a limit order; "0" (or zero at any
/// precision) for a market order, which has none, any other price on one refused.
fn order_price(
    spec: &InstrumentSpec,
    text: &str,
    market_order: bool,
) -> Result<Option<Ticks>, DecodeError> {
    let bad = DecodeError::Malformed("order price");
    let px: PxExact = text.parse().map_err(|_| bad)?;
    match (market_order, px.mantissa) {
        (true, 0) => Ok(None),
        (true, _) => Err(DecodeError::Malformed("market order with a price")),
        (false, mantissa) if mantissa <= 0 => Err(bad),
        (false, _) => spec.price_grid.ticks_exact(px).map(Some).ok_or(bad),
    }
}

/// A size string as lots of the instrument's size step: refused as `what` when it is not a
/// decimal, and off the step or negative as [`lots_of`] refuses it.
fn size(spec: &InstrumentSpec, text: &str, what: &'static str) -> Result<Lots, DecodeError> {
    lots_of(spec, decimal(text, what)?)
}

/// One position of `GET /positions`: `None` when flat (not listed), else its instrument, its
/// signed lots and its average entry.
fn position_of(
    position: &Value,
    specs: &SpecTable,
) -> Result<Option<(InstrumentId, SignedLots, PxExact)>, DecodeError> {
    let position = position
        .as_object()
        .ok_or(DecodeError::Malformed("position: not a JSON object"))?;
    let signed = decimal(text(position, "size", "position size")?, "position size")?;
    let flat = signed.is_zero();
    match text(position, "status", "position status")? {
        "OPEN" => {}
        "CLOSED" if flat => {}
        "CLOSED" => return Err(DecodeError::Malformed("closed position with a size")),
        _ => return Err(DecodeError::Malformed("position status")),
    }
    if flat {
        return Ok(None);
    }
    let spec = market(text(position, "market", "position market")?, specs)?;
    let side = match (
        text(position, "side", "position side")?,
        signed.is_sign_positive(),
    ) {
        ("LONG", true) => Side::Buy,
        ("SHORT", false) => Side::Sell,
        _ => return Err(DecodeError::Malformed("position side")),
    };
    let lots = lots_of(spec, signed.abs())?;
    let entry = DecodeError::Malformed("position average entry");
    let avg: PxExact = text(position, "average_entry_price", "position average entry")?
        .parse()
        .map_err(|_| entry)?;
    if avg.mantissa <= 0 {
        return Err(entry);
    }
    Ok(Some((spec.id, SignedLots::of(side, lots), avg)))
}
