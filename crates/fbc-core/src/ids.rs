//! Order and fill ids as distinct, sealed types (decision 0004, design §4.3).
//!
//! A venue id handed to a call that wants a client id is a compile error, because
//! [`ClientOrderId`], [`VenueOrderId`] and [`FillId`] are different types, and none of them can
//! be built outside this crate:
//!
//! - a [`ClientOrderId`] comes only from [`CidMint::mint`](crate::CidMint::mint), which needs a
//!   held [`NamespaceLease`](crate::NamespaceLease), or from decoding one of our own wire ids
//!   ([`decode_cid`](crate::decode_cid));
//! - a [`VenueOrderId`] or a [`FillId`] comes only from a
//!   [`DecodeScope`](crate::DecodeScope), which the core's dispatch lends to codec callbacks.
//!
//! `trybuild` compile-fail tests (`tests/ui_ids/`) prove each seal.

use core::fmt;

use compact_str::CompactString;

/// An engine instance's client-id namespace, unique per venue account across every box.
///
/// The consumer allocates namespaces in its configuration and checks them against the venue
/// snapshot at start; this crate only carries the number.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct Namespace(u16);

impl Namespace {
    /// The namespace with this number.
    pub const fn new(n: u16) -> Namespace {
        Namespace(n)
    }

    /// Its number.
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A venue account as the consumer numbers it.
///
/// Only a small number: the account's address, sub-account id or keys never enter this crate
/// (decision 0009), so a lock file named after an `AccountKey` names no real account.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct AccountKey(u16);

impl AccountKey {
    /// The account with this number.
    pub const fn new(n: u16) -> AccountKey {
        AccountKey(n)
    }

    /// Its number.
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// An instrument as the core numbers it, the same on every venue that lists it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct InstrumentId(u32);

impl InstrumentId {
    /// The instrument with this number.
    pub const fn new(n: u32) -> InstrumentId {
        InstrumentId(n)
    }

    /// Its number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A venue as the core numbers it.
///
/// Only a number: nothing outside the venue crates and the registry branches on which venue
/// it names (decision 0003); what a venue can do is its [`VenueCaps`](crate::VenueCaps).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct VenueId(u16);

impl VenueId {
    /// The venue with this number.
    pub const fn new(n: u16) -> VenueId {
        VenueId(n)
    }

    /// Its number.
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// What an instrument is a contract on (BTC, XAU, EUR), the same on every venue that lists it,
/// so instruments on different venues can be netted against each other.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct UnderlyingId(u32);

impl UnderlyingId {
    /// The underlying with this number.
    pub const fn new(n: u32) -> UnderlyingId {
        UnderlyingId(n)
    }

    /// Its number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// An instrument's name on its venue's wire (`ETH-USD-PERP`, `ETHUSDT`), exactly as the venue
/// spells it.
///
/// Opaque for construction: only an adapter builds one, through
/// [`DecodeScope::venue_symbol`](crate::DecodeScope::venue_symbol); everyone may read it for
/// display and compatibility output.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct VenueSymbol(CompactString);

impl VenueSymbol {
    pub(crate) fn from_wire(wire: &str) -> Result<VenueSymbol, IdError> {
        check_venue_id(wire).map(|()| VenueSymbol(CompactString::new(wire)))
    }

    /// The symbol as the venue spells it.
    pub fn as_wire(&self) -> &str {
        &self.0
    }
}

/// Our id for an order: a namespace and a sequence number.
///
/// The fields are private and there is no public constructor: one is minted by
/// [`CidMint`](crate::CidMint) under a held namespace lease, or decoded from one of our own
/// wire ids by [`decode_cid`](crate::decode_cid).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct ClientOrderId {
    ns: Namespace,
    seq: u64,
}

impl ClientOrderId {
    pub(crate) const fn new(ns: Namespace, seq: u64) -> ClientOrderId {
        ClientOrderId { ns, seq }
    }

    /// The namespace that minted it.
    pub const fn namespace(self) -> Namespace {
        self.ns
    }

    /// Its sequence number within the namespace.
    pub const fn seq(self) -> u64 {
        self.seq
    }
}

/// The longest venue order id, fill id or venue symbol accepted, in bytes.
pub const MAX_VENUE_ID_LEN: usize = 96;

/// A venue's id for an order, exactly as the venue sent it.
///
/// Built only by [`DecodeScope::venue_order_id`](crate::DecodeScope::venue_order_id).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct VenueOrderId(CompactString);

impl VenueOrderId {
    pub(crate) fn from_wire(wire: &str) -> Result<VenueOrderId, IdError> {
        check_venue_id(wire).map(|()| VenueOrderId(CompactString::new(wire)))
    }

    /// The id as the venue sent it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A venue's id for a fill, exactly as the venue sent it.
///
/// Built only by [`DecodeScope::fill_id`](crate::DecodeScope::fill_id).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct FillId(CompactString);

impl FillId {
    pub(crate) fn from_wire(wire: &str) -> Result<FillId, IdError> {
        check_venue_id(wire).map(|()| FillId(CompactString::new(wire)))
    }

    /// The id as the venue sent it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn check_venue_id(wire: &str) -> Result<(), IdError> {
    if wire.is_empty() {
        Err(IdError::Empty)
    } else if wire.len() > MAX_VENUE_ID_LEN {
        Err(IdError::TooLong {
            len: wire.len(),
            max: MAX_VENUE_ID_LEN,
        })
    } else {
        Ok(())
    }
}

/// How a command names an order: by our id, by the venue's, or by both.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderRef {
    /// Our id only (the venue has not acknowledged the order yet).
    Client(ClientOrderId),
    /// The venue's id only (an order we did not mint, or one whose client id is unknown).
    Venue(VenueOrderId),
    /// Both ids.
    Both(ClientOrderId, VenueOrderId),
}

impl OrderRef {
    /// Our id, when the reference carries one.
    pub fn client(&self) -> Option<ClientOrderId> {
        match self {
            OrderRef::Client(cid) | OrderRef::Both(cid, _) => Some(*cid),
            OrderRef::Venue(_) => None,
        }
    }

    /// The venue's id, when the reference carries one.
    pub fn venue(&self) -> Option<&VenueOrderId> {
        match self {
            OrderRef::Venue(vid) | OrderRef::Both(_, vid) => Some(vid),
            OrderRef::Client(_) => None,
        }
    }
}

/// What a client id read off the wire turned out to be.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CidMatch {
    /// One of ours: canonical, and minted in our namespace.
    Ours(ClientOrderId),
    /// Canonical, but minted in another namespace (another engine instance on the account).
    Foreign(Namespace),
    /// Not a canonical client id: minted by another system (the Java brokers, the venue's UI,
    /// a random UUID) or corrupted.
    Unparseable,
}

/// Why an id could not be built, encoded or minted.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum IdError {
    /// The canonical payload needs `needed` characters and the wire format allows `max`.
    DoesNotFit {
        /// Characters the canonical payload needs in this format.
        needed: usize,
        /// Characters the format allows.
        max: usize,
    },
    /// A venue order id, fill id or symbol is empty.
    Empty,
    /// A venue order id, fill id or symbol is longer than [`MAX_VENUE_ID_LEN`].
    TooLong {
        /// Its length in bytes.
        len: usize,
        /// The longest accepted.
        max: usize,
    },
    /// The namespace has issued its last sequence number.
    Exhausted,
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdError::DoesNotFit { needed, max } => write!(
                f,
                "a canonical client id needs {needed} characters and the format allows {max}"
            ),
            IdError::Empty => f.write_str("the venue id is empty"),
            IdError::TooLong { len, max } => {
                write!(
                    f,
                    "the venue id is {len} bytes long; at most {max} are accepted"
                )
            }
            IdError::Exhausted => f.write_str("the namespace has no sequence numbers left"),
        }
    }
}

impl std::error::Error for IdError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caps::testing::caps;
    use crate::scope::{DecodeScope, dispatch};

    #[test]
    fn numbers_round_trip() {
        assert_eq!(Namespace::new(65_535).get(), 65_535);
        assert_eq!(AccountKey::new(3).get(), 3);
        assert_eq!(InstrumentId::new(4_000_000_000).get(), 4_000_000_000);
        assert_eq!(VenueId::new(7).get(), 7);
        assert_eq!(UnderlyingId::new(4_000_000_001).get(), 4_000_000_001);
        let cid = ClientOrderId::new(Namespace::new(9), 42);
        assert_eq!((cid.namespace(), cid.seq()), (Namespace::new(9), 42));
    }

    // Venue ids come only from the decode scope, in tests as well (decision 0004);
    // tests/no_back_door.rs fails if anything else calls `from_wire`.
    fn in_scope<R>(callback: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
        dispatch(&caps(), Namespace::new(1), callback)
    }

    #[test]
    fn venue_ids_keep_the_wire_text_and_refuse_empty_or_overlong() {
        in_scope(|scope| {
            let vid = scope.venue_order_id("1759363200000201030").unwrap();
            assert_eq!(vid.as_str(), "1759363200000201030");
            let fid = scope.fill_id("f-1").unwrap();
            assert_eq!(fid.as_str(), "f-1");
            let longest = "x".repeat(MAX_VENUE_ID_LEN);
            assert!(scope.venue_order_id(&longest).is_ok());
            assert!(scope.fill_id(&longest).is_ok());
            let over = "x".repeat(MAX_VENUE_ID_LEN + 1);
            assert_eq!(
                scope.venue_order_id(&over),
                Err(IdError::TooLong {
                    len: 97,
                    max: MAX_VENUE_ID_LEN
                })
            );
            assert_eq!(scope.fill_id(""), Err(IdError::Empty));
            let symbol = scope.venue_symbol("ETH-USD-PERP").unwrap();
            assert_eq!(symbol.as_wire(), "ETH-USD-PERP");
            assert!(scope.venue_symbol(&longest).is_ok());
            assert_eq!(scope.venue_symbol(""), Err(IdError::Empty));
            assert!(matches!(
                scope.venue_symbol(&over),
                Err(IdError::TooLong { len: 97, .. })
            ));
        });
    }

    #[test]
    fn an_order_ref_yields_the_ids_it_carries() {
        let cid = ClientOrderId::new(Namespace::new(1), 2);
        let vid = in_scope(|scope| scope.venue_order_id("v")).unwrap();
        assert_eq!(OrderRef::Client(cid).client(), Some(cid));
        assert_eq!(OrderRef::Client(cid).venue(), None);
        assert_eq!(OrderRef::Venue(vid.clone()).client(), None);
        assert_eq!(OrderRef::Venue(vid.clone()).venue(), Some(&vid));
        let both = OrderRef::Both(cid, vid.clone());
        assert_eq!((both.client(), both.venue()), (Some(cid), Some(&vid)));
    }

    #[test]
    fn errors_say_what_went_wrong() {
        let cases = [
            (
                IdError::DoesNotFit {
                    needed: 21,
                    max: 20,
                },
                "needs 21 characters",
            ),
            (IdError::Empty, "empty"),
            (IdError::TooLong { len: 97, max: 96 }, "97 bytes"),
            (IdError::Exhausted, "no sequence numbers left"),
        ];
        for (err, text) in cases {
            assert!(err.to_string().contains(text), "{err}");
        }
    }
}
