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
use crate::event::VenueMode;
use crate::ids::{ClientOrderId, InstrumentId, OrderRef, VenueOrderId};
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
    pub reduce_only: bool,
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
    pub reduce_only: bool,
    /// The new limit price.
    pub px: Ticks,
    /// The new total quantity, filled part included.
    pub qty: Lots,
    /// The order's cumulative filled quantity in the OMS's record when it built the amend. A
    /// venue whose amend quantity is the remaining quantity ([`AmendQty::Remaining`]) is sent
    /// `qty - cum_filled` ([`AmendOrder::wire_qty`]): exactly the resting quantity the OMS
    /// checked against its caps, even if fills the OMS has not seen yet have arrived at the
    /// venue.
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
    /// Accepted, with the venue's order id when the reply carries it.
    Accepted {
        vid: Option<VenueOrderId>,
        ack: AckLevel,
    },
    /// Refused by the venue.
    Rejected(Reject),
    /// Sent with no answer: resolved by the Unknown ladder and never resent (decision 0005).
    Unknown,
}

/// A venue's refusal. Its `Debug` shows the kind and the venue's code, and the venue's message
/// by length only: a venue can echo a key or an authorization value in an error message, and
/// nothing marks it (0009).
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
            .field("venue_code", &self.venue_code)
            .field("raw", &format_args!("<{} bytes>", self.raw.len()))
            .finish()
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

/// A refusal that ends an order: any [`RejectKind`] but [`RejectKind::NotFound`], which moves an
/// order to Unknown and is never terminal (decision 0005). It is what
/// [`VenueOrderState::Rejected`](crate::VenueOrderState::Rejected) carries, so an order state
/// cannot report an order as rejected for being unknown.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TerminalReject(RejectKind);

impl TerminalReject {
    /// `kind` as an order-ending refusal, or `None` for [`RejectKind::NotFound`].
    pub fn new(kind: RejectKind) -> Option<TerminalReject> {
        (kind != RejectKind::NotFound).then_some(TerminalReject(kind))
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
    use crate::cid::ClientIdFormat;
    use crate::fee::VenueFeeSign;
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
        let secret = "SYNTHETIC-ECHOED-KEY";
        let reject = Reject {
            kind: RejectKind::Other,
            venue_code: Some(CompactString::from("E401")),
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
            assert!(text.contains("Other") && text.contains("E401"), "{text}");
        }
        assert!(shown[0].contains(&format!("<{} bytes>", reject.raw.len())));
        // The text itself stays available to the code that maps it.
        assert!(reject.raw.contains(secret));
    }

    #[test]
    fn a_terminal_reject_is_any_kind_but_not_found() {
        assert_eq!(TerminalReject::new(RejectKind::NotFound), None);
        let margin = TerminalReject::new(RejectKind::Margin).unwrap();
        assert_eq!(margin.kind(), RejectKind::Margin);
        let mode = RejectKind::VenueMode(crate::event::VenueMode::Halted);
        assert_eq!(
            TerminalReject::new(mode).map(TerminalReject::kind),
            Some(mode)
        );
    }

    #[test]
    fn a_command_counts_one_item_or_its_batch_length() {
        let vid = dispatch(
            &ClientIdFormat::Uuid,
            Namespace::new(1),
            VenueFeeSign::PositiveIsCost,
            |scope| scope.venue_order_id("V-1"),
        )
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
        };
        assert_eq!(VenueCommand::Cancel(cancel.clone()).items(), Some(1));
        assert_eq!(VenueCommand::FeeQuery.items(), Some(1));
        assert_eq!(VenueCommand::PlaceBatch(vec![order; 3]).items(), Some(3));
        assert_eq!(VenueCommand::CancelMany(Vec::new()).items(), Some(0));
        let huge = vec![cancel; usize::from(u16::MAX) + 1];
        assert_eq!(VenueCommand::CancelMany(huge).items(), None);
    }
}
