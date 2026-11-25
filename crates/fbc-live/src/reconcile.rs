//! The stream check (the owner's condition in decision 0094: positions follow fills; decision
//! 0155 items 2 and 3): whether what the venue's streams reported is what the registry holds
//! yet, so a consumer plans nothing on an inventory its fills have not caught up with.
//!
//! A venue's order, fill and position streams are separate, so for a moment one can lead
//! another. Two disagreements are reported ([`Unreconciled`]), in this order:
//!
//! - **The streamed position.** A position the venue's position stream reported for the market
//!   (the link's [`Note::Position`]) other than the inventory's (Codex r4244080127 on PR #149):
//!   its fill not read yet, or a change not ours. None streamed yet is no disagreement.
//! - **An order's fills.** An order of ours whose cumulative fill the venue reported on an order
//!   event ([`OrderRecord::cum_venue`]) is above the fills read for it
//!   ([`OrderRecord::cum_fills`]): the fill stream has not brought them yet (Codex r4244145969 on
//!   PR #149). The orders checked are the consumer's: those it watches, then those that ended
//!   while behind ([`fills_behind`]), which it keeps in a list this check trims, each leaving it
//!   once it is no longer behind or the registry no longer holds it.
//!
//! It reports and does nothing else. It keeps no clock and no timeout and names no reaction:
//! whether the consumer holds its planning while a disagreement lasts, what it prints, and when
//! it gives up and Stops are the consumer's (decision 0155 item 2). Its answer depends only on
//! what the consumer passes, so it reads the same in replay as live.
//!
//! [`Note::Position`]: crate::link::Note::Position

use fbc_core::{ClientOrderId, Lots, SignedLots};
use fbc_oms::{OrderRecord, Registry};

/// What the venue's streams reported that the registry does not hold yet. It names no reaction:
/// what follows is the consumer's.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Unreconciled {
    /// The venue's position stream reported `streamed` for the market, and the inventory is
    /// `inventory`.
    Position {
        /// The position the venue's stream last reported, as the consumer passed it.
        streamed: SignedLots,
        /// The inventory's position, as the consumer passed it.
        inventory: SignedLots,
    },
    /// The venue reported `venue` filled of our order `cid` on an order event, and the fills
    /// read for it are `read`, fewer.
    Fills {
        /// The order of ours.
        cid: ClientOrderId,
        /// Its cumulative fill as the venue reported it on an order event.
        venue: Lots,
        /// The fills of it the registry read from the fill stream.
        read: Lots,
    },
}

/// Whether the order's fills read are behind what the venue reported filled on an order event:
/// its cumulative venue fill is above the fills read. A consumer keeps an order that ended so
/// in the list [`unreconciled`] trims.
pub fn fills_behind(order: &OrderRecord) -> bool {
    order.cum_venue() > order.cum_fills()
}

/// What does not agree, if anything (module documentation): `streamed`, the position the venue's
/// stream last reported for the market (`None` before any), against `inventory`, the inventory's
/// position as the consumer read it; then, once they agree, the first order of ours in `watched`
/// and then in `ended` whose fills read are behind ([`fills_behind`]), as `registry` holds it.
///
/// Once the positions agree, `ended` is trimmed first to the orders the registry still holds
/// behind, its order kept; while they differ it is left as it is.
pub fn unreconciled(
    registry: &Registry,
    streamed: Option<SignedLots>,
    inventory: SignedLots,
    watched: &[ClientOrderId],
    ended: &mut Vec<ClientOrderId>,
) -> Option<Unreconciled> {
    if let Some(streamed) = streamed.filter(|s| *s != inventory) {
        return Some(Unreconciled::Position {
            streamed,
            inventory,
        });
    }
    let behind = |cid: &ClientOrderId| registry.get(*cid).is_some_and(fills_behind);
    ended.retain(behind);
    watched.iter().chain(ended.iter()).find_map(|cid| {
        let order = registry.get(*cid).filter(|o| fills_behind(o))?;
        Some(Unreconciled::Fills {
            cid: *cid,
            venue: order.cum_venue(),
            read: order.cum_fills(),
        })
    })
}
