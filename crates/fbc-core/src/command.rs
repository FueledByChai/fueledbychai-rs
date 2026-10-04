//! Venue commands and what becomes of them (decisions 0002, 0004, 0005 and 0014, design §4.6).
//!
//! A [`VenueCommand`] carries every field the venue needs to encode and sign it: the OMS
//! resolves each one from its order record, and codecs hold no order registry, so an amend,
//! cancel or query encodes correctly from the command and the spec table alone, with an empty
//! codec state. An amend carries the FULL post-amend order (venues that sign amends sign every
//! field).
//!
//! What becomes of a command is a [`SubmitOutcome`] per item: never sent (no byte reached a
//! socket buffer), accepted, rejected, or `Unknown` (sent with no answer), which the OMS
//! resolves through its Unknown ladder and never resends (decision 0005).

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use compact_str::CompactString;

use crate::caps::{AmendQty, Feature, OrderKindTag, TifTag};
use crate::codec::TrafficClass;
use crate::event::VenueMode;
use crate::ids::{ClientOrderId, InstrumentId, OrderRef};
use crate::units::{Channel, Lots, Side, Ticks};

/// A time in force.
pub type Tif = TifTag;

/// An order's kind, with its limit price.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderKind {
    /// A limit order at `px`.
    Limit { px: Ticks },
    /// A market order.
    Market,
}

impl OrderKind {
    /// The kind without its price, as capabilities list it.
    pub fn tag(self) -> OrderKindTag {
        match self {
            OrderKind::Limit { .. } => OrderKindTag::Limit,
            OrderKind::Market => OrderKindTag::Market,
        }
    }

    /// The limit price, for a limit order.
    pub fn limit_px(self) -> Option<Ticks> {
        match self {
            OrderKind::Limit { px } => Some(px),
            OrderKind::Market => None,
        }
    }
}

/// A new order, every field stated.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct NewOrder {
    pub cid: ClientOrderId,
    pub inst: InstrumentId,
    pub side: Side,
    pub qty: Lots,
    pub kind: OrderKind,
    pub tif: Tif,
    pub channel: Channel,
    pub post_only: bool,
    /// The venue's reduce-only flag, sent on the wire where the venue has one
    /// ([`OrderCaps::reduce_only`](crate::OrderCaps)).
    pub reduce_only: bool,
    /// The OMS's own classification: this order can only reduce the position (sized not to
    /// cross zero), whether or not it carries the venue flag. It makes the order safety traffic
    /// ([`VenueCommand::traffic_class`]) on a venue without a reduce-only flag.
    pub reducing: bool,
}

impl NewOrder {
    /// The order can only reduce the position: the OMS says so, or the venue enforces it.
    pub fn reduces(&self) -> bool {
        self.reducing || self.reduce_only
    }
}

/// An amend of a resting limit order, with the FULL post-amend values: venues that sign
/// amends sign every field, and the codec has no record to fill one in from.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct AmendOrder {
    pub target: OrderRef,
    pub inst: InstrumentId,
    pub side: Side,
    pub tif: Tif,
    pub channel: Channel,
    pub post_only: bool,
    /// The venue's reduce-only flag.
    pub reduce_only: bool,
    /// The OMS's classification of the amended order as one that can only reduce the position,
    /// as on [`NewOrder::reducing`].
    pub reducing: bool,
    /// The new limit price.
    pub px: Ticks,
    /// The new total quantity, filled part included.
    pub qty: Lots,
    /// The order's cumulative filled quantity in the OMS's record when it built the amend. A
    /// venue whose amend quantity is the remaining quantity ([`AmendQty::Remaining`]) is sent
    /// `qty - cum_filled` ([`AmendOrder::wire_qty`]): the resting quantity the OMS checked
    /// against its caps. Fills the OMS has not seen yet may still reach the venue first; they
    /// come out of the old resting quantity, and the venue then rests the whole wire quantity.
    /// So the OMS counts the order's resting as the wire quantity it sent (less fills reported
    /// after the amend's acknowledgement) until the venue reports the order's total, never as
    /// `qty - cum_filled` recomputed after later fills (FBC-w5n).
    pub cum_filled: Lots,
}

impl AmendOrder {
    /// The quantity the venue's wire carries under `semantics`: the total, or the total less
    /// the filled quantity. `None` when the total is at or below the filled quantity, which
    /// leaves nothing to rest: that is a cancel, not an amend, and a codec does not send it.
    pub fn wire_qty(&self, semantics: AmendQty) -> Option<Lots> {
        let remaining = self.qty.checked_sub(self.cum_filled)?;
        if remaining.get() == 0 {
            return None;
        }
        Some(match semantics {
            AmendQty::TotalIncludingFilled => self.qty,
            AmendQty::Remaining => remaining,
        })
    }
}

/// A cancel of one order.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct CancelOrder {
    pub target: OrderRef,
    pub inst: InstrumentId,
    pub side: Side,
    /// The nonce the order was placed with, for venues that cancel by it.
    pub placement_nonce: Option<u64>,
}

/// A query for one order.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct QueryOrder {
    pub target: OrderRef,
    pub inst: InstrumentId,
    /// The nonce the order was placed with, for venues that query by it
    /// ([`RefKind::PlacementNonce`](crate::RefKind::PlacementNonce)): an order in Unknown may
    /// have no venue id yet, and the query is how the Unknown ladder resolves it (0005).
    pub placement_nonce: Option<u64>,
}

/// What a cancel-all covers. An instrument cancel-all is never widened to the account.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelScope {
    /// Every order on the account.
    Account,
    /// Every order on one instrument.
    Instrument(InstrumentId),
}

/// A command for a venue. The OMS resolves every field from its record.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueCommand {
    Place(NewOrder),
    PlaceBatch(Vec<NewOrder>),
    Amend(AmendOrder),
    Cancel(CancelOrder),
    CancelMany(Vec<CancelOrder>),
    CancelAll(CancelScope),
    /// Turn the venue's cancel-on-disconnect protection on or off.
    ArmCancelOnDisconnect(bool),
    /// Refresh the venue's dead-man timer.
    RefreshDeadMan,
    Query(QueryOrder),
    /// Ask for the account's fee rates.
    FeeQuery,
}

impl VenueCommand {
    /// The traffic class of the command's request: `Safety` for cancels, turning
    /// cancel-on-disconnect on (turning it off removes protection, so it is `Normal`), dead-man
    /// refreshes, order queries (the Unknown ladder) and orders or amends that
    /// can only reduce the position ([`NewOrder::reduces`]); a batch is `Safety` only when it
    /// has items and every one reduces. Everything else is `Normal`. A codec labels its effects
    /// with this, so the rule has one source.
    pub fn traffic_class(&self) -> TrafficClass {
        let safety = match self {
            VenueCommand::Place(o) => o.reduces(),
            VenueCommand::PlaceBatch(orders) => {
                !orders.is_empty() && orders.iter().all(NewOrder::reduces)
            }
            VenueCommand::Amend(a) => a.reducing || a.reduce_only,
            VenueCommand::Cancel(_)
            | VenueCommand::CancelMany(_)
            | VenueCommand::CancelAll(_)
            | VenueCommand::ArmCancelOnDisconnect(true)
            | VenueCommand::RefreshDeadMan
            | VenueCommand::Query(_) => true,
            VenueCommand::ArmCancelOnDisconnect(false) | VenueCommand::FeeQuery => false,
        };
        if safety {
            TrafficClass::Safety
        } else {
            TrafficClass::Normal
        }
    }

    /// The number of items the command's request carries: the batch length for a batch, one
    /// otherwise. The runtime reserves this many nonces for its encode
    /// ([`NonceSource`](crate::NonceSource)); `None` for a batch longer than `u16::MAX` items,
    /// which no venue accepts.
    pub fn items(&self) -> Option<u16> {
        match self {
            VenueCommand::PlaceBatch(orders) => u16::try_from(orders.len()).ok(),
            VenueCommand::CancelMany(cancels) => u16::try_from(cancels.len()).ok(),
            _ => Some(1),
        }
    }
}

/// How far the venue's acceptance of a command goes.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AckLevel {
    /// Received and provisionally accepted: a two-phase venue may still reject it within its
    /// risk window ([`AckModel::TwoPhase`](crate::AckModel::TwoPhase)).
    Provisional,
    /// Accepted for good.
    Final,
}

/// Why a command never reached the venue: no byte of it was written to a socket buffer.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum NotSentReason {
    /// The stream it needs is not connected.
    Disconnected,
    /// The socket's buffer is full.
    Backpressure,
    /// The rate budget for its scope is spent.
    RateBudget,
    /// The venue does not offer the operation (or the kind, flag or reference it needs).
    Unsupported,
    /// The order combines features the venue refuses together.
    FlagConflict,
    /// The command cannot be written in the venue's wire format (a client id that does not fit
    /// it, an instrument missing from the spec table).
    Unencodable,
    /// The signer refused or failed.
    SignFailed,
}

/// What became of one command, or one item of a batch.
#[derive(Clone, PartialEq, Debug)]
pub enum SubmitOutcome {
    /// Never sent: nothing reached a socket buffer.
    NotSent(NotSentReason),
    /// Accepted. The venue's order id, when the reply carries it, is the item's
    /// ([`ItemRef::vid`](crate::ItemRef::vid)), stated once there.
    Accepted { ack: AckLevel },
    /// Refused by the venue.
    Rejected(Reject),
    /// Sent with no answer: resolved by the Unknown ladder and never resent (decision 0005).
    Unknown,
}

/// A venue's refusal. Its `Debug` shows the kind, and the venue's code and message by length
/// only: the venue writes both, it can echo a key or an authorization value in either, and
/// nothing marks it (0009). The kind is what the code maps to, so it says what a log needs.
#[derive(Clone, PartialEq)]
pub struct Reject {
    /// What kind of refusal it is.
    pub kind: RejectKind,
    /// The venue's error code, when it sends one; reject maps key on this, not on text.
    pub venue_code: Option<CompactString>,
    /// The venue's message as received, for the code that maps it; never logged as is.
    pub raw: Arc<str>,
}

impl fmt::Debug for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reject")
            .field("kind", &self.kind)
            .field("venue_code", &self.venue_code.as_deref().map(ByLength))
            .field("raw", &ByLength(&self.raw))
            .finish()
    }
}

/// Venue text a `Debug` shows by its length only.
struct ByLength<'a>(&'a str);

impl fmt::Debug for ByLength<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} bytes>", self.0.len())
    }
}

/// What kind of refusal a venue reject is.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RejectKind {
    /// A post-only order would have crossed.
    PostOnlyWouldCross,
    InvalidPrice,
    InvalidQty,
    /// Below the venue's minimum notional.
    MinNotional,
    /// Not enough margin.
    Margin,
    /// Rate limited, retryable after `retry_after` when the venue says.
    RateLimited {
        retry_after: Option<Duration>,
    },
    /// The venue does not know the order: the order becomes Unknown and is queried; never
    /// terminal (decision 0005).
    NotFound,
    /// The order already reached a terminal state.
    AlreadyTerminal(TerminalHint),
    /// The order cannot be amended.
    NotAmendable(NotAmendable),
    /// The amend changes nothing.
    NoChange,
    /// The order combines two features the venue refuses together.
    FlagConflict(Feature, Feature),
    /// The venue's mode refuses it.
    VenueMode(VenueMode),
    /// The venue does not offer what was asked.
    Unsupported,
    /// Anything the venue's codes do not map to the kinds above.
    Other,
}

/// A refusal that ends an order: a refusal of the order itself, which therefore never rests.
/// It is what [`VenueOrderState::Rejected`](crate::VenueOrderState::Rejected) carries, so an
/// order state cannot report an order as rejected for a refusal that leaves it as it was:
/// [`RejectKind::NotFound`] moves an order to Unknown and is never terminal (decision 0005), and
/// [`RejectKind::AlreadyTerminal`], [`RejectKind::NotAmendable`] and [`RejectKind::NoChange`]
/// refuse an operation on an existing order (a cancel, an amend) that stays as it was; those
/// are command outcomes ([`SubmitOutcome::Rejected`]), never an order's state.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TerminalReject(RejectKind);

impl TerminalReject {
    /// `kind` as an order-ending refusal, or `None` for a refusal that does not end an order
    /// (`NotFound`, `AlreadyTerminal`, `NotAmendable`, `NoChange`).
    pub fn new(kind: RejectKind) -> Option<TerminalReject> {
        let leaves_order = matches!(
            kind,
            RejectKind::NotFound
                | RejectKind::AlreadyTerminal(_)
                | RejectKind::NotAmendable(_)
                | RejectKind::NoChange
        );
        (!leaves_order).then_some(TerminalReject(kind))
    }

    /// What kind of refusal it is.
    pub fn kind(self) -> RejectKind {
        self.0
    }
}

/// The terminal state a venue says an order already reached, when it says which.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TerminalHint {
    Filled,
    Canceled,
    Rejected,
    Expired,
    /// The venue says only that the order is closed.
    Unspecified,
}

/// Why an order cannot be amended.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum NotAmendable {
    /// The venue does not amend orders (or not this field).
    Unsupported,
    /// The venue does not amend a partially filled order.
    PartiallyFilled,
    /// The amend needs the venue's order id and none is known yet.
    NoVenueId,
    /// The venue refused for another reason.
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caps::AmendQty;
    use crate::caps::testing::caps;
    use crate::ids::Namespace;
    use crate::scope::dispatch;

    #[test]
    fn an_order_kind_names_its_tag_and_limit_price() {
        let limit = OrderKind::Limit { px: Ticks(5) };
        assert_eq!(
            (limit.tag(), limit.limit_px()),
            (OrderKindTag::Limit, Some(Ticks(5)))
        );
        let market = OrderKind::Market;
        assert_eq!(
            (market.tag(), market.limit_px()),
            (OrderKindTag::Market, None)
        );
    }

    #[test]
    fn an_amend_carries_the_fill_a_remaining_quantity_wire_needs() {
        let lots = |n| Lots::new(n).unwrap();
        let amend = |qty, cum_filled| AmendOrder {
            target: OrderRef::Client(ClientOrderId::new(Namespace::new(1), 1)),
            inst: InstrumentId::new(1),
            side: Side::Buy,
            tif: TifTag::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(100),
            qty: lots(qty),
            cum_filled: lots(cum_filled),
        };
        // 4 lots filled, total amended to 10: a total-quantity wire sends 10, a
        // remaining-quantity wire sends 6, so the venue rests what the OMS checked.
        let partly = amend(10, 4);
        assert_eq!(
            partly.wire_qty(AmendQty::TotalIncludingFilled),
            Some(lots(10))
        );
        assert_eq!(partly.wire_qty(AmendQty::Remaining), Some(lots(6)));
        assert_eq!(amend(10, 0).wire_qty(AmendQty::Remaining), Some(lots(10)));
        // Amending to the filled quantity or below leaves nothing to rest: not an amend.
        assert_eq!(amend(4, 4).wire_qty(AmendQty::Remaining), None);
        assert_eq!(amend(3, 4).wire_qty(AmendQty::Remaining), None);
        assert_eq!(amend(3, 4).wire_qty(AmendQty::TotalIncludingFilled), None);
    }

    #[test]
    fn debug_never_shows_the_text_a_venue_sent_with_a_reject() {
        // The venue writes both the message and the code (Codex r4173243254): either can echo
        // a key, so Debug shows each by length only.
        let secret = "SYNTHETIC-ECHOED-KEY";
        let code = "SYNTHETIC-CODE-KEY";
        let reject = Reject {
            kind: RejectKind::Other,
            venue_code: Some(CompactString::from(code)),
            raw: Arc::from(format!("bad signature for key {secret}")),
        };
        let shown = [
            format!("{reject:?}"),
            format!("{:?}", SubmitOutcome::Rejected(reject.clone())),
            format!(
                "{:#?}",
                crate::event::ExecEvent::UncorrelatedError(reject.clone())
            ),
        ];
        for text in &shown {
            assert!(!text.contains(secret), "venue text reached Debug: {text}");
            assert!(!text.contains(code), "venue code reached Debug: {text}");
            assert!(text.contains("Other"), "{text}");
        }
        assert!(shown[0].contains(&format!("<{} bytes>", reject.raw.len())));
        assert!(shown[0].contains(&format!("Some(<{} bytes>)", code.len())));
        let bare = Reject {
            venue_code: None,
            ..reject.clone()
        };
        assert!(format!("{bare:?}").contains("venue_code: None"));
        // The text itself stays available to the code that maps it.
        assert!(reject.raw.contains(secret));
    }

    #[test]
    fn a_terminal_reject_is_only_a_refusal_that_ends_the_order() {
        assert_eq!(TerminalReject::new(RejectKind::NotFound), None);
        let margin = TerminalReject::new(RejectKind::Margin).unwrap();
        assert_eq!(margin.kind(), RejectKind::Margin);
        let mode = RejectKind::VenueMode(crate::event::VenueMode::Halted);
        assert_eq!(
            TerminalReject::new(mode).map(TerminalReject::kind),
            Some(mode)
        );
        // Codex r4173187115: a refusal of an operation on an existing order (an amend that
        // changes nothing or cannot be made, a cancel of an order already closed) leaves that
        // order as it was, so it is never the reason an order is Rejected.
        let not_amendable = RejectKind::NotAmendable(NotAmendable::PartiallyFilled);
        let closed = RejectKind::AlreadyTerminal(TerminalHint::Filled);
        for kind in [RejectKind::NoChange, not_amendable, closed] {
            assert_eq!(TerminalReject::new(kind), None, "{kind:?}");
        }
        let limited = RejectKind::RateLimited { retry_after: None };
        assert!(
            TerminalReject::new(limited).is_some(),
            "a placement refused never rested"
        );
    }

    #[test]
    fn a_command_counts_one_item_or_its_batch_length() {
        let vid = dispatch(&caps(), Namespace::new(1), |scope| {
            scope.venue_order_id("V-1")
        })
        .unwrap();
        let cancel = CancelOrder {
            target: OrderRef::Venue(vid),
            inst: InstrumentId::new(1),
            side: Side::Sell,
            placement_nonce: None,
        };
        let order = NewOrder {
            cid: ClientOrderId::new(Namespace::new(1), 1),
            inst: InstrumentId::new(1),
            side: Side::Buy,
            qty: Lots::new(1).unwrap(),
            kind: OrderKind::Market,
            tif: TifTag::Ioc,
            channel: Channel::Public,
            post_only: false,
            reduce_only: true,
            reducing: true,
        };
        assert_eq!(VenueCommand::Cancel(cancel.clone()).items(), Some(1));
        assert_eq!(VenueCommand::FeeQuery.items(), Some(1));
        assert_eq!(VenueCommand::PlaceBatch(vec![order; 3]).items(), Some(3));
        assert_eq!(VenueCommand::CancelMany(Vec::new()).items(), Some(0));
        let huge = vec![cancel; usize::from(u16::MAX) + 1];
        assert_eq!(VenueCommand::CancelMany(huge).items(), None);
    }

    #[test]
    fn a_command_is_safety_traffic_only_when_everything_in_it_reduces_or_protects() {
        // On a venue without a reduce-only flag an exit goes out with reduce_only false; the
        // OMS's own classification, `reducing`, keeps it on the safety floor. A reduce-only
        // order is reducing too, and a batch is safety traffic only when every order is.
        let vid = dispatch(&caps(), Namespace::new(1), |scope| {
            scope.venue_order_id("V-1")
        })
        .unwrap();
        let order = |reducing, reduce_only| NewOrder {
            cid: ClientOrderId::new(Namespace::new(1), 1),
            inst: InstrumentId::new(1),
            side: Side::Sell,
            qty: Lots::new(1).unwrap(),
            kind: OrderKind::Limit { px: Ticks(5) },
            tif: TifTag::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only,
            reducing,
        };
        let (exit, flagged, plain) = (order(true, false), order(false, true), order(false, false));
        let class = |cmd: VenueCommand| cmd.traffic_class();
        let (safety, normal) = (TrafficClass::Safety, TrafficClass::Normal);
        assert_eq!(class(VenueCommand::Place(exit.clone())), safety);
        assert_eq!(class(VenueCommand::Place(flagged.clone())), safety);
        assert_eq!(class(VenueCommand::Place(plain.clone())), normal);
        let batch = VenueCommand::PlaceBatch(vec![exit.clone(), flagged]);
        assert_eq!(class(batch), safety);
        assert_eq!(class(VenueCommand::PlaceBatch(vec![exit, plain])), normal);
        assert_eq!(class(VenueCommand::PlaceBatch(vec![])), normal);
        let amend = |reducing, reduce_only| {
            VenueCommand::Amend(AmendOrder {
                target: OrderRef::Venue(vid.clone()),
                inst: InstrumentId::new(1),
                side: Side::Sell,
                tif: TifTag::Gtc,
                channel: Channel::Public,
                post_only: true,
                reduce_only,
                reducing,
                px: Ticks(5),
                qty: Lots::new(10).unwrap(),
                cum_filled: Lots::new(0).unwrap(),
            })
        };
        assert_eq!(class(amend(true, false)), safety);
        assert_eq!(class(amend(false, true)), safety);
        assert_eq!(class(amend(false, false)), normal);
        // Cancels, protection and the Unknown ladder's queries are safety traffic; a fee query
        // is not.
        let cancel = CancelOrder {
            target: OrderRef::Venue(vid.clone()),
            inst: InstrumentId::new(1),
            side: Side::Sell,
            placement_nonce: None,
        };
        let query = QueryOrder {
            target: OrderRef::Venue(vid),
            inst: InstrumentId::new(1),
            placement_nonce: Some(3),
        };
        for cmd in [
            VenueCommand::Cancel(cancel.clone()),
            VenueCommand::CancelMany(vec![cancel]),
            VenueCommand::CancelAll(CancelScope::Instrument(InstrumentId::new(1))),
            VenueCommand::ArmCancelOnDisconnect(true),
            VenueCommand::RefreshDeadMan,
            VenueCommand::Query(query),
        ] {
            assert_eq!(cmd.traffic_class(), safety, "{cmd:?}");
        }
        assert_eq!(class(VenueCommand::FeeQuery), normal);
        // Turning cancel-on-disconnect off removes protection, so it does not ride the safety
        // floor (Codex r4173389313).
        assert_eq!(class(VenueCommand::ArmCancelOnDisconnect(false)), normal);
    }
}
