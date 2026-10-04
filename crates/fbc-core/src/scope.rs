//! The decode scope: the only constructor of [`VenueOrderId`], [`FillId`], [`VenueSymbol`] and
//! [`Fee`] (decision 0004, design §4.2, §4.3, §4.4).
//!
//! A [`DecodeScope`] cannot be built outside this crate. The core's [`dispatch`] (and, for
//! market data, [`dispatch_market_data`]) makes one and lends it to a codec callback for the
//! length of that callback; the callback cannot keep it. Adapters, and tests too (decision 0004:
//! no back door), obtain venue ids, venue symbols and fees only through it.
//!
//! The scope reads the client-id format and the fee sign from the venue's own [`VenueCaps`], so
//! a codec cannot decode under a format or sign other than the ones its venue declares. Both
//! live in the exec block (decision 0015), and so does the engine namespace: a market-data-only
//! venue (`exec: None`) declares neither, holds no namespace, and is dispatched without one. Its
//! scope reads no client id as ours ([`CidMatch::Unparseable`]) and refuses every fee
//! ([`FeeError::NoExecBlock`]) rather than guess a sign.

use crate::caps::VenueCaps;
use crate::cid::decode_cid;
use crate::fee::{Fee, FeeError};
use crate::ids::{CidMatch, FillId, IdError, Namespace, VenueOrderId, VenueSymbol};
use crate::units::AssetSym;

/// What a codec callback decodes ids with. Lent by [`dispatch`] and [`dispatch_market_data`];
/// never built elsewhere.
#[derive(Debug)]
pub struct DecodeScope<'a> {
    caps: &'a VenueCaps,
    own: Option<Namespace>,
    _seal: Seal,
}

/// Keeps [`DecodeScope`] unconstructible outside this crate even if its other fields go public.
#[derive(Debug)]
struct Seal;

impl DecodeScope<'_> {
    /// The venue's order id as sent: refused when empty or longer than
    /// [`MAX_VENUE_ID_LEN`](crate::MAX_VENUE_ID_LEN) bytes.
    pub fn venue_order_id(&self, wire: &str) -> Result<VenueOrderId, IdError> {
        VenueOrderId::from_wire(wire)
    }

    /// The venue's fill id as sent: refused when empty or longer than
    /// [`MAX_VENUE_ID_LEN`](crate::MAX_VENUE_ID_LEN) bytes.
    pub fn fill_id(&self, wire: &str) -> Result<FillId, IdError> {
        FillId::from_wire(wire)
    }

    /// An instrument's symbol as the venue spells it: refused when empty or longer than
    /// [`MAX_VENUE_ID_LEN`](crate::MAX_VENUE_ID_LEN) bytes.
    pub fn venue_symbol(&self, wire: &str) -> Result<VenueSymbol, IdError> {
        VenueSymbol::from_wire(wire)
    }

    /// A client id read off the wire, decoded with the venue's declared format
    /// (`caps.exec.order.client_id`) against our namespace. [`CidMatch::Unparseable`] when the
    /// venue declares `exec: None` (it has no client-id format) or the scope was lent by
    /// [`dispatch_market_data`] (it holds no namespace to call an id ours).
    pub fn client_order_id(&self, wire: &str) -> CidMatch {
        match (&self.caps.exec, self.own) {
            (Some(exec), Some(own)) => decode_cid(&exec.order.client_id, own, wire),
            _ => CidMatch::Unparseable,
        }
    }

    /// The fee `raw_nanos` of `asset` means under the venue's declared fee sign
    /// (`caps.exec.fills.fee_sign`), as a cost to us: positive when we paid, negative for a
    /// rebate. Refused with [`FeeError::NoExecBlock`] when the venue declares `exec: None`, so
    /// no fee is ever decoded under a guessed sign, and with [`FeeError::OutOfRange`] for
    /// `i128::MIN` nanos, which has no negation.
    pub fn fee(&self, raw_nanos: i128, asset: AssetSym) -> Result<Fee, FeeError> {
        let exec = self.caps.exec.as_ref().ok_or(FeeError::NoExecBlock)?;
        Fee::from_declared(raw_nanos, asset, exec.fills.fee_sign)
    }
}

/// The core's dispatch for an order-entry session: runs `callback` with a [`DecodeScope`] that
/// decodes client ids and fees as `caps` declares (`caps.exec.order.client_id`,
/// `caps.exec.fills.fee_sign`), in the engine namespace `own`, and returns what the callback
/// returns.
///
/// The scope is lent for any lifetime the callback must accept, so the callback cannot return
/// or store it.
pub fn dispatch<R>(
    caps: &VenueCaps,
    own: Namespace,
    callback: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R,
) -> R {
    lend(caps, Some(own), callback)
}

/// The core's dispatch for a market-data session, which holds no engine namespace: runs
/// `callback` with a [`DecodeScope`] for the venue `caps` describes, and returns what the
/// callback returns. Every venue's market data is dispatched this way, a market-data-only venue
/// (`exec: None`) included. Market data carries no client ids of ours, so the scope reads none
/// as ours; fees decode under the declared sign, and are refused for a venue with no exec block.
pub fn dispatch_market_data<R>(
    caps: &VenueCaps,
    callback: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R,
) -> R {
    lend(caps, None, callback)
}

fn lend<R>(
    caps: &VenueCaps,
    own: Option<Namespace>,
    callback: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R,
) -> R {
    let scope = DecodeScope {
        caps,
        own,
        _seal: Seal,
    };
    callback(&scope)
}
