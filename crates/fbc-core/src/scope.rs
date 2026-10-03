//! The decode scope: the only constructor of [`VenueOrderId`], [`FillId`], [`VenueSymbol`] and
//! [`Fee`] (decision 0004, design §4.2, §4.3, §4.4).
//!
//! A [`DecodeScope`] cannot be built outside this crate. The core's [`dispatch`] makes one and
//! lends it to a codec callback for the length of that callback; the callback cannot keep it.
//! Adapters, and tests too (decision 0004: no back door), obtain venue ids, venue symbols and
//! fees only through it.

use crate::cid::{ClientIdFormat, decode_cid};
use crate::fee::{Fee, FeeError, VenueFeeSign};
use crate::ids::{CidMatch, FillId, IdError, Namespace, VenueOrderId, VenueSymbol};
use crate::units::AssetSym;

/// What a codec callback decodes ids with. Lent by [`dispatch`]; never built elsewhere.
#[derive(Debug)]
pub struct DecodeScope<'a> {
    fmt: &'a ClientIdFormat,
    own: Namespace,
    fee_sign: VenueFeeSign,
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

    /// A client id read off the wire, decoded with the venue's format against our namespace.
    pub fn client_order_id(&self, wire: &str) -> CidMatch {
        decode_cid(self.fmt, self.own, wire)
    }

    /// The fee `raw_nanos` of `asset` means under the venue's declared fee sign, as a cost to
    /// us: positive when we paid, negative for a rebate. Refused only for `i128::MIN` nanos,
    /// which has no negation.
    pub fn fee(&self, raw_nanos: i128, asset: AssetSym) -> Result<Fee, FeeError> {
        Fee::from_declared(raw_nanos, asset, self.fee_sign)
    }
}

/// The core's dispatch: runs `callback` with a [`DecodeScope`] for a venue whose client ids use
/// `fmt` and whose fees are reported under `fee_sign`, in the engine namespace `own`, and
/// returns what the callback returns.
///
/// The scope is lent for any lifetime the callback must accept, so the callback cannot return
/// or store it.
pub fn dispatch<R>(
    fmt: &ClientIdFormat,
    own: Namespace,
    fee_sign: VenueFeeSign,
    callback: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R,
) -> R {
    let scope = DecodeScope {
        fmt,
        own,
        fee_sign,
        _seal: Seal,
    };
    callback(&scope)
}
