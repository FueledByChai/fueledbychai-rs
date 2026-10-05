//! The toy's signer: an FNV-1a hash, in hexadecimal, of the fields of a request as it goes on
//! the wire. It holds no key and stands for no real scheme; what matters is what it is shown:
//! the wire view alone, whose only order reference is the one the request carries.

use fbc_core::{AmendWire, CancelWire, OrderSigner, PlaceWire, Sig, SignError};

/// Signs places, amends and cancels for the toy venue; cancels are signed (its caps say so).
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct ToySigner;

impl OrderSigner for ToySigner {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        Ok(hash(&format!(
            "place|{}|{}|{:?}|{:?}|{}|{:?}|{:?}|{}|{}|{}|{:?}",
            w.spec.venue_symbol.as_wire(),
            w.cid,
            w.side,
            w.kind,
            w.qty.get(),
            w.tif,
            w.channel,
            w.post_only,
            w.reduce_only,
            w.wall.0,
            w.nonce,
        )))
    }

    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        Ok(hash(&format!(
            "amend|{}|{:?}|{:?}|{}|{}|{:?}|{:?}|{}|{}|{}|{:?}",
            w.spec.venue_symbol.as_wire(),
            w.target,
            w.side,
            w.px.0,
            w.qty.get(),
            w.tif,
            w.channel,
            w.post_only,
            w.reduce_only,
            w.wall.0,
            w.nonce,
        )))
    }

    fn sign_cancel(&mut self, w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        Ok(Some(hash(&format!(
            "cancel|{}|{:?}|{:?}|{}|{:?}",
            w.spec.venue_symbol.as_wire(),
            w.target,
            w.side,
            w.wall.0,
            w.nonce,
        ))))
    }
}

fn hash(signed: &str) -> Sig {
    let hash = signed.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    Sig::new(format!("{hash:016x}").as_bytes()).expect("16 bytes fit a signature")
}
