//! Paradex's private `OrderEvent` (SBE template 20, the `orders.{market}` channel) decoded into
//! order updates (BT-402, decisions 0004, 0022, 0054).
//!
//! The layout is the schema's (`paradex_1_0.xml` at the paradex-py commit 0022 cites), read
//! through [`sbe`](crate::md::sbe)'s block-length-gated reader; FueledByChaiTrading's
//! `ParadexSbeTranscoder` reads the same offsets. The root block: `ts`@0, `seq`@8, `status`@16,
//! `side`@17, `orderType`@18, `timeInForce`@19, `price`@20 (`Price8NULL`), `triggerPrice`@28,
//! `size`@36, `sizeOpen`@44, `avgFillPrice`@52, `createdAt`@60, `updatedAt`@68, `account`@76
//! (32 bytes), `receivedAt`@108, `publishedAt`@116, `stp`@124 and `flags`@125, 126 bytes up to
//! version 1; at version 2 `requestStatus`@126 and `requestType`@127 are appended in place.
//! Then the var data: `orderId`, `clientOrderId`, `market`, `cancelReason`, and at version 2
//! `requestId` and `requestMessage`. The schema says version 2 describes both layouts, so
//! request_info is read only from a version-2 frame whose block holds it, and the two strings
//! only when the frame carries them (absent appended var data is missing, not malformed).
//!
//! What an update states:
//! - `status` NEW (accepted, before the risk check of a two-phase venue) and OPEN: resting.
//!   CLOSED: cancelled by its `cancelReason` when one is stated; filled when none is and
//!   nothing is open; cancelled by the venue when none is stated and part is open.
//!   UNTRIGGERED, and the STOP and TPSL order types, are not modelled (0054) and refused.
//! - `cum_filled` is `size` less `sizeOpen`; `qty` is `size` and `px` is `price` (none for a
//!   market order, whose price must be null or 0: any other is refused), the venue's truth.
//! - `post_only` is the instruction POST_ONLY or RPI (an RPI order is post-only by the venue's
//!   rule, 0054); `reduce_only` is `flags` bit 0 (REDUCE_ONLY).
//! - `seq` is the venue sequence (`OrderingKey::VenueSeq`), `ts` the venue's publish time.
//! - The client id is read through [`DecodeScope::client_order_id`]; an empty one is none.
//!
//! A modify's request_info, as 0054 decides (`AmendAck::ReplacedEvent`): SUCCESS for
//! MODIFY_ORDER makes the update the amend's confirmation
//! ([`VenueOrderState::Amended`], the order keeping its venue id, at the event's price and
//! size), unless the event shows the order closed; REJECTED for MODIFY_ORDER is an
//! [`ExecEvent::AsyncReject`] of the amend, pushed before the order update that shows the order
//! as the rejection left it (`reject_keeps_original`); PENDING, PROCESSED and any other value
//! confirm nothing.
//!
//! A modify's request_info is news once (decision 0086, FBC-g3bw): Paradex's order carries
//! `request_info` as one of its fields ("Get order"), so a later update of the same order (a
//! fill, say) may still carry an earlier modify's. [`ModifyRequests`], kept by the codec across
//! its frames and connections, holds every `(requestId, requestStatus)` of MODIFY_ORDER
//! request_info each order's events carried; an event repeating one of them, the latest or an
//! earlier modify's, reports nothing of the modify again and is the order update in the order's
//! own state (Open, not Amended, and no second asynchronous reject). A new requestId, or a new
//! status of one, is news. An event without MODIFY_ORDER request_info leaves what is held as it
//! is, and a CLOSED order's entries are dropped once its event is decoded.
//!
//! The whole frame is read before anything is pushed, so a refused frame pushes nothing.

use std::collections::{HashMap, HashSet};

use fbc_core::{
    CancelReason, CidMatch, DecodeError, DecodeScope, ExchTsKind, ExecEvent, ExecSink, OpKind,
    OrderRef, OrderUpdate, Reject, RejectKind, Side, SpecTable, VenueMeta, VenueOrderId,
    VenueOrderState,
};

use crate::md::sbe::{Block, Message, NULL_I64};
use crate::md::{lots, market, micros, price, required, seq};

/// `OrderEvent`'s template id.
pub const TEMPLATE_ORDER: u16 = 20;

/// The offset of `requestStatus` and `requestType`, appended at version 2.
const REQUEST_STATUS: usize = 126;
const REQUEST_TYPE: usize = 127;

/// `RequestStatus` REJECTED and SUCCESS, and `RequestType` MODIFY_ORDER.
const REQUEST_REJECTED: u8 = 3;
const REQUEST_SUCCESS: u8 = 4;
const MODIFY_ORDER: u8 = 1;

/// Every MODIFY_ORDER request_info each order's events carried, by venue order id (decision
/// 0086): what makes a modify's SUCCESS or REJECTED news once, not again on a later update of
/// the order that still carries it, even after another modify's (Codex r4226495973 on PR #139).
/// One per order-entry codec, kept across its frames and connections; an order's entries go
/// once an event shows it CLOSED.
#[derive(Debug, Default)]
pub struct ModifyRequests {
    seen: HashMap<VenueOrderId, HashSet<Request>>,
}

/// One MODIFY_ORDER request_info: its `requestId` (`None` when the frame carries none) and its
/// `requestStatus`.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Request {
    id: Option<Box<str>>,
    status: u8,
}

impl ModifyRequests {
    /// Nothing seen yet.
    pub fn new() -> ModifyRequests {
        ModifyRequests::default()
    }

    /// The number of orders whose MODIFY_ORDER request_info is held.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether none is held.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Whether no earlier event of `vid` carried `request`, holding it from now on.
    fn is_news(&mut self, vid: &VenueOrderId, request: Request) -> bool {
        if let Some(held) = self.seen.get_mut(vid) {
            return held.insert(request);
        }
        self.seen.insert(vid.clone(), HashSet::from([request]));
        true
    }

    /// Drops what is held of `vid`, an order now closed.
    fn forget(&mut self, vid: &VenueOrderId) {
        self.seen.remove(vid);
    }
}

/// What a modify's request_info says about the amend, as decision 0054 reads it.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Modify {
    /// No request_info, one that confirms nothing (PENDING, PROCESSED, another request), or
    /// one an earlier event of the order already carried (decision 0086).
    Nothing,
    /// SUCCESS for MODIFY_ORDER: the amend is final.
    Succeeded,
    /// REJECTED for MODIFY_ORDER: the amend is refused, the order as the event shows it.
    Rejected,
}

/// Decodes one `OrderEvent` frame into the order update it states, preceded by an
/// [`ExecEvent::AsyncReject`] of the amend when its request_info reports a modify REJECTED that
/// `seen` does not hold for the order (decision 0086); a decoded frame's MODIFY_ORDER
/// request_info is held in `seen` from then on. Refused, with nothing pushed and `seen`
/// unchanged, when the frame is not an `OrderEvent`, is shorter than its header or its declared
/// block, states a value outside the schema's or one not modelled, or names a market missing
/// from `specs`.
pub fn decode_order_event(
    frame: &[u8],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
    seen: &mut ModifyRequests,
    sink: &mut dyn ExecSink,
) -> Result<(), DecodeError> {
    let msg = Message::parse(frame)?;
    if msg.header().template_id != TEMPLATE_ORDER {
        return Err(DecodeError::Malformed("not an OrderEvent"));
    }
    let block = msg.block();
    let request_status = modify_status(&msg, &block);
    let mut tail = msg.tail();
    let mut var = |what: &'static str| tail.var_str()?.ok_or(DecodeError::Malformed(what));
    let order_id = var("order id")?;
    let client_id = var("client order id")?;
    let symbol = var("order market")?;
    let cancel_reason = var("cancel reason")?;
    // At version 2, `requestId` then `requestMessage`, each missing when absent. Read whenever
    // the frame carries them, so a frame whose strings run past it is refused alike.
    let (request_id, request_message) = if msg.header().version >= 2 {
        (tail.var_str()?, tail.var_str()?)
    } else {
        (None, None)
    };
    let spec = market(symbol, specs)?;

    let side = match block.u8_at(17) {
        Some(1) => Side::Buy,
        Some(2) => Side::Sell,
        _ => return Err(DecodeError::Malformed("order side")),
    };
    let market_order = match block.u8_at(18) {
        Some(1) => false,
        Some(2) => true,
        // The STOP and TPSL types are not modelled (0054).
        _ => return Err(DecodeError::Malformed("order type not modelled")),
    };
    let post_only = match block.u8_at(19) {
        // GTC, IOC.
        Some(1 | 2) => false,
        // POST_ONLY, and RPI, post-only by the venue's rule (0054).
        Some(3 | 4) => true,
        _ => return Err(DecodeError::Malformed("order time in force")),
    };
    // OrderFlags bit 0, REDUCE_ONLY.
    let reduce_only = block
        .u8_at(125)
        .ok_or(DecodeError::Malformed("order flags"))?
        & 1
        == 1;
    let px = match required(&block, 20, "order price")? {
        // A market order's price is null (the JSON feed's "0 for MARKET orders").
        NULL_I64 | 0 if market_order => None,
        // Any other price on a market order contradicts that rule: refused, since an update
        // with a price is what a limit order's looks like.
        _ if market_order => return Err(DecodeError::Malformed("market order with a price")),
        mantissa => Some(price(spec, mantissa)?),
    };
    let qty = lots(spec, required(&block, 36, "order size")?)?;
    let open = lots(spec, required(&block, 44, "order size open")?)?;
    let cum_filled = qty
        .checked_sub(open)
        .ok_or(DecodeError::Malformed("order size open above its size"))?;
    let is_closed = match block.u8_at(16) {
        Some(4) => true,
        // NEW and OPEN.
        Some(1 | 3) => false,
        // UNTRIGGERED (a stop order's) is not modelled (0054).
        _ => return Err(DecodeError::Malformed("order status not modelled")),
    };
    let meta = VenueMeta {
        exch_ts: Some(micros(required(&block, 0, "order ts")?)?),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq(required(&block, 8, "order seq")?)?),
    };
    let cid = (!client_id.is_empty()).then(|| scope.client_order_id(client_id));
    let vid = scope.venue_order_id(order_id)?;
    // The frame is read whole: only now is what `seen` holds of the order changed.
    let modify = match request_status {
        Some(status) => {
            let request = Request {
                id: request_id.map(Box::from),
                status,
            };
            if seen.is_news(&vid, request) {
                modify_of(status)
            } else {
                Modify::Nothing
            }
        }
        None => Modify::Nothing,
    };
    let state = if is_closed {
        // A closed order is ended whatever its request says, and nothing of it is kept.
        seen.forget(&vid);
        closed(cancel_reason, open.get() == 0)
    } else if modify == Modify::Succeeded {
        VenueOrderState::Amended { new_vid: None }
    } else {
        VenueOrderState::Open
    };
    let update = OrderUpdate {
        cid,
        vid: Some(vid.clone()),
        inst: spec.id,
        side,
        state,
        cum_filled,
        px,
        qty: Some(qty),
        post_only: Some(post_only),
        reduce_only: Some(reduce_only),
    };
    if modify == Modify::Rejected {
        let target = match cid {
            Some(CidMatch::Ours(cid)) => OrderRef::Both(cid, vid),
            _ => OrderRef::Venue(vid),
        };
        let reject = Reject {
            // Paradex states no code for a refused modify, only its message.
            kind: RejectKind::Other,
            venue_code: None,
            raw: request_message.unwrap_or("").into(),
        };
        let op = OpKind::Amend;
        sink.push(meta, ExecEvent::AsyncReject { target, op, reject });
    }
    sink.push(meta, ExecEvent::Order(update));
    Ok(())
}

/// The state of a CLOSED order: cancelled by `reason` when one is stated; filled when none is
/// and nothing is open (`filled`); cancelled by the venue when none is stated and part is open.
/// The reasons mapped are the ones a source names (`fixtures/paradex/exec/README.md`): no
/// Paradex page lists them all, so any other is the venue's.
pub(super) fn closed(reason: &str, filled: bool) -> VenueOrderState {
    match reason {
        "" if filled => VenueOrderState::Filled,
        "USER_CANCELED" => VenueOrderState::Canceled(CancelReason::Requested),
        "POST_ONLY_WOULD_CROSS" => VenueOrderState::Canceled(CancelReason::PostOnly),
        _ => VenueOrderState::Canceled(CancelReason::Venue),
    }
}

/// The `requestStatus` of `msg`'s request_info when its `requestType` is MODIFY_ORDER: read
/// only from a version-2 frame whose root block holds it.
fn modify_status(msg: &Message<'_>, block: &Block<'_>) -> Option<u8> {
    if msg.header().version < 2 {
        return None;
    }
    match (block.u8_at(REQUEST_STATUS), block.u8_at(REQUEST_TYPE)) {
        (Some(status), Some(MODIFY_ORDER)) => Some(status),
        _ => None,
    }
}

/// What a MODIFY_ORDER request_info's `status`, when it is news, says about the amend.
fn modify_of(status: u8) -> Modify {
    match status {
        REQUEST_SUCCESS => Modify::Succeeded,
        REQUEST_REJECTED => Modify::Rejected,
        _ => Modify::Nothing,
    }
}
