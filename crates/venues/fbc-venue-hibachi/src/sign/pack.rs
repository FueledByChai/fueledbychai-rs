//! The bytes Hibachi signs: every field big-endian, with no tag or length (Java
//! `HibachiPayloadPacker.java` L59-L110; the SDK's `__create_or_update_order_payload` and
//! `__cancel_order_payload`, `api.py` L2047-L2097 and L2239-L2259 at 14a76b9; the Signing page of
//! Hibachi's docs).
//!
//! - A place or a modify: nonce u64, contract id u32, quantity u64, side u32 (ASK 0, BID 1),
//!   price u64, max fees u64: 40 bytes. A market order, or a modify that names no price, leaves
//!   the price out (not zero-filled): 32 bytes. A modify packs as a place does, under the
//!   modify's own fresh nonce; the order it changes is named only in the request's JSON.
//! - A cancel: the venue's order id, or the nonce the order was placed with, as u64: 8 bytes.
//! - A cancel-all: the request's nonce as u64: 8 bytes. The cancel-on-disconnect arm's nonce
//!   packs the same way, though neither Java nor the SDK signs the arm ([`pack_arm`]).
//!
//! Scaling, from the decimal the request's JSON carries:
//!
//! - quantity × 10^underlyingDecimals, which must come out whole;
//! - price × 2^32 × 10^(settlementDecimals − underlyingDecimals), truncated toward zero, as the
//!   SDK's `price_to_bytes` (`api.py` L213-L231) and Java's `scalePrice` do: the truncation is
//!   part of the wire format, and an on-grid price rarely scales to a whole number;
//! - max fees × 10^8, which must come out whole. Hibachi's signing docs say rate fees scale by
//!   10^8 but pack their example's 0.0005 as 5000 (0x1388), which is 0.0005 × 10^7; the SDK
//!   (`int(max_fees_percent * pow(10, 8))`) and Java (`MAX_FEES_SCALE`) scale by 10^8, and so does
//!   this, with no 10^7 path. The unit of max fees (a fraction, a percent or basis points: the
//!   sources disagree) is the caller's; the packer scales whatever decimal the request carries.
//!
//! Every check comes before a byte is packed. The decimal price must lie on the instrument's
//! price grid (its `tickSize`, as the SDK's `check_tick_size` requires) and the quantity on its
//! size step (`stepSize`; stricter than the SDK, which truncates a quantity silently), so the
//! JSON never says one value while the signature packs another. Nothing is negative, and every
//! u64 field is at most 2^63 − 1: Java holds each in a signed `long` and refuses a scaled value
//! past 63 bits, while the SDK takes any u64, so this packs only what both pack the same.
//!
//! The order's other fields (its client id, `creationDeadline`, `orderFlags`, trigger price and
//! direction, TWAP configuration and parent order) are JSON only and never packed (Java's packer,
//! L36-L38; the SDK's `_create_order_request_data`, `api.py` L2161-L2172). Hibachi's Postman
//! collection lists `creationDeadline` and `triggerPrice` among the signed fields and documents a
//! market order's 8-byte price limit; neither Java, the SDK nor the docs' own JavaScript example
//! packs any of them, and neither does this.

use core::fmt;

use fbc_core::{InstrumentSpec, PxExact, Side, SignError, SigningParams, VenueNativeId};
use rust_decimal::Decimal;

/// 2^32: a packed price is a fixed-point number with 32 fraction bits.
const PRICE_ONE: u128 = 1 << 32;
/// Max fees are packed × 10^8 (Java `MAX_FEES_SCALE`; the SDK's `pow(10, 8)`).
const FEES_DECIMALS: u32 = 8;
/// The largest value a u64 field holds here: 2^63 − 1, a Java `long`'s maximum.
pub const MAX_FIELD: u64 = i64::MAX as u64;

/// The longest payload: a limit order's 40 bytes.
const MAX_LEN: usize = 40;

/// The bytes one command signs: 8, 32 or 40 of them. Its `Debug` shows only the length, so a
/// signed payload never reaches a log line through it.
#[derive(Clone, Eq, PartialEq)]
pub struct Payload {
    bytes: [u8; MAX_LEN],
    len: usize,
}

impl Payload {
    fn empty() -> Payload {
        Payload {
            bytes: [0; MAX_LEN],
            len: 0,
        }
    }

    fn put(&mut self, bytes: &[u8]) {
        self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }

    /// The packed bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Payload({} bytes)", self.len)
    }
}

/// A place or a modify, as its request states it: what the packer reads, and the fields the
/// request's JSON carries beside them ([`JsonOnly`]), which it never reads.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct OrderMessage<'a> {
    /// The instrument: its `native_id` is the contract id, its `signing` the asset decimals
    /// (decision 0125), its price grid and size step what the price and quantity must lie on.
    pub spec: &'a InstrumentSpec,
    /// The request's nonce; a modify's is its own, never the order's placement nonce.
    pub nonce: u64,
    pub side: Side,
    /// The quantity in the instrument's units, as the JSON's `quantity` carries it.
    pub quantity: Decimal,
    /// The limit price, as the JSON's `price` carries it; `None` for a market order and for a
    /// modify that names no price.
    pub price: Option<Decimal>,
    /// The JSON's `maxFeesPercent`, a required input of every place and modify: the signer holds
    /// no value and no default for it.
    pub max_fees: Decimal,
    /// What the JSON carries beside these.
    pub json_only: JsonOnly<'a>,
}

/// The order fields Hibachi's JSON carries that are never packed (Java
/// `HibachiTranslator.java` L80-L122 puts them only in the request's parameters; the SDK's
/// `_create_order_request_data` and `__update_order_request_data` add them after signing), each
/// as the request writes it. They ride in [`OrderMessage`] so one value can describe the whole
/// order; the packer reads none of them.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct JsonOnly<'a> {
    pub client_id: Option<&'a str>,
    pub creation_deadline: Option<&'a str>,
    pub order_flags: Option<&'a str>,
    pub trigger_price: Option<&'a str>,
    pub trigger_direction: Option<&'a str>,
    pub twap_config: Option<&'a str>,
    pub parent_order: Option<&'a str>,
}

impl JsonOnly<'_> {
    /// None of them.
    pub const NONE: JsonOnly<'static> = JsonOnly {
        client_id: None,
        creation_deadline: None,
        order_flags: None,
        trigger_price: None,
        trigger_direction: None,
        twap_config: None,
        parent_order: None,
    };
}

/// What a cancel names its order by, as its signature packs it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelTarget {
    /// The venue's order id.
    OrderId(u64),
    /// The nonce the order was placed with.
    PlacementNonce(u64),
}

/// The side as a packed word: ASK 0, BID 1 (Java `HibachiSide.java` L13-L14).
fn side_word(side: Side) -> u32 {
    match side {
        Side::Buy => 1,
        Side::Sell => 0,
    }
}

/// A u64 field's value, refused past 2^63 − 1.
fn field(value: u64, what: &'static str) -> Result<u64, SignError> {
    if value > MAX_FIELD {
        return Err(SignError::Unsignable(what));
    }
    Ok(value)
}

/// The contract id: the instrument's `native_id`, a number that fits a u32.
fn contract_id(spec: &InstrumentSpec) -> Result<u32, SignError> {
    match &spec.native_id {
        Some(VenueNativeId::Number(id)) => {
            u32::try_from(*id).map_err(|_| SignError::Unsignable("contract id past u32"))
        }
        Some(VenueNativeId::Text(_)) | None => Err(SignError::Unsignable("no numeric contract id")),
    }
}

/// The asset decimals (decision 0125).
fn decimals(spec: &InstrumentSpec) -> Result<(u8, u8), SignError> {
    match spec.signing {
        Some(SigningParams::AssetDecimals {
            underlying,
            settlement,
        }) => Ok((underlying, settlement)),
        None => Err(SignError::Unsignable("no asset decimals")),
    }
}

/// Whether `price` lies on the instrument's price grid.
fn on_tick(spec: &InstrumentSpec, price: Decimal) -> bool {
    PxExact::from_decimal(price)
        .and_then(|px| spec.price_grid.ticks_exact(px))
        .is_some_and(|ticks| spec.price_grid.valid_at(ticks))
}

/// Whether `quantity` is a whole number of the instrument's size steps.
fn on_step(spec: &InstrumentSpec, quantity: Decimal) -> bool {
    let step = spec.size_step.get();
    quantity
        .checked_div(step)
        .is_some_and(|count| count.fract().is_zero() && count.checked_mul(step) == Some(quantity))
}

/// Why a decimal does not scale into a field.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Scale {
    /// It does not come out whole.
    NotWhole,
    /// It comes out past 2^63 − 1.
    TooLarge,
}

/// `value × 10^exp` exactly: a whole number of at most 2^63 − 1. `value` is not negative.
fn scale_exact(value: Decimal, exp: u32) -> Result<u64, Scale> {
    let (mantissa, scale) = (value.mantissa().unsigned_abs(), value.scale());
    if mantissa == 0 {
        return Ok(0);
    }
    let scaled = if exp >= scale {
        let factor = 10u128.checked_pow(exp - scale).ok_or(Scale::TooLarge)?;
        mantissa.checked_mul(factor).ok_or(Scale::TooLarge)?
    } else {
        // A Decimal's scale is at most 28, so 10^(scale − exp) fits.
        let divisor = 10u128.pow(scale - exp);
        if mantissa % divisor != 0 {
            return Err(Scale::NotWhole);
        }
        mantissa / divisor
    };
    u64::try_from(scaled)
        .ok()
        .filter(|v| *v <= MAX_FIELD)
        .ok_or(Scale::TooLarge)
}

/// `price × 2^32 × 10^shift`, truncated toward zero, refused past 2^63 − 1. `price` is not
/// negative; `shift` is settlementDecimals − underlyingDecimals.
fn scale_price(price: Decimal, shift: i32) -> Option<u64> {
    let (mantissa, scale) = (price.mantissa().unsigned_abs(), price.scale());
    // The mantissa is below 2^96, so this is below 2^128.
    let fixed = mantissa.checked_mul(PRICE_ONE)?;
    // price × 2^32 × 10^shift = fixed × 10^(shift − scale); a Decimal's scale is at most 28.
    let exp = shift - i32::try_from(scale).ok()?;
    let scaled = if fixed == 0 {
        0
    } else if exp >= 0 {
        fixed.checked_mul(10u128.checked_pow(exp.unsigned_abs())?)?
    } else {
        // 10^39 exceeds every u128: past it the quotient is zero.
        10u128
            .checked_pow(exp.unsigned_abs())
            .map_or(0, |divisor| fixed / divisor)
    };
    u64::try_from(scaled).ok().filter(|v| *v <= MAX_FIELD)
}

/// The packed place or modify, after every check: the instrument's contract id and asset
/// decimals, no negative value, the price on the price grid and the quantity on the size step,
/// then the quantity and max fees scaling exactly and every field within 2^63 − 1. Refused naming
/// the part, never a value.
pub fn pack_order(m: &OrderMessage<'_>) -> Result<Payload, SignError> {
    let contract = contract_id(m.spec)?;
    let (underlying, settlement) = decimals(m.spec)?;
    if m.quantity.is_sign_negative() && !m.quantity.is_zero() {
        return Err(SignError::Unsignable("negative quantity"));
    }
    if m.price
        .is_some_and(|p| p.is_sign_negative() && !p.is_zero())
    {
        return Err(SignError::Unsignable("negative price"));
    }
    if m.max_fees.is_sign_negative() && !m.max_fees.is_zero() {
        return Err(SignError::Unsignable("negative max fees"));
    }
    if m.price.is_some_and(|p| !on_tick(m.spec, p)) {
        return Err(SignError::Unsignable("price off the tick size"));
    }
    if !on_step(m.spec, m.quantity) {
        return Err(SignError::Unsignable("quantity off the step size"));
    }

    let nonce = field(m.nonce, "nonce past 2^63 - 1")?;
    let quantity = scale_exact(m.quantity, u32::from(underlying)).map_err(|e| match e {
        Scale::NotWhole => SignError::Unsignable("quantity finer than the underlying decimals"),
        Scale::TooLarge => SignError::Unsignable("quantity past 2^63 - 1 when scaled"),
    })?;
    let shift = i32::from(settlement) - i32::from(underlying);
    let price = m
        .price
        .map(|p| {
            scale_price(p, shift).ok_or(SignError::Unsignable("price past 2^63 - 1 when scaled"))
        })
        .transpose()?;
    let max_fees = scale_exact(m.max_fees, FEES_DECIMALS).map_err(|e| match e {
        Scale::NotWhole => SignError::Unsignable("max fees finer than 1e-8"),
        Scale::TooLarge => SignError::Unsignable("max fees past 2^63 - 1 when scaled"),
    })?;

    let mut payload = Payload::empty();
    payload.put(&nonce.to_be_bytes());
    payload.put(&contract.to_be_bytes());
    payload.put(&quantity.to_be_bytes());
    payload.put(&side_word(m.side).to_be_bytes());
    if let Some(price) = price {
        payload.put(&price.to_be_bytes());
    }
    payload.put(&max_fees.to_be_bytes());
    Ok(payload)
}

/// One u64 as 8 bytes.
fn pack_u64(value: u64) -> Payload {
    let mut payload = Payload::empty();
    payload.put(&value.to_be_bytes());
    payload
}

/// The packed cancel: the order id, or the placement nonce, as 8 bytes.
pub fn pack_cancel(target: CancelTarget) -> Result<Payload, SignError> {
    let value = match target {
        CancelTarget::OrderId(id) => field(id, "order id past 2^63 - 1")?,
        CancelTarget::PlacementNonce(nonce) => field(nonce, "placement nonce past 2^63 - 1")?,
    };
    Ok(pack_u64(value))
}

/// The packed cancel-all (`orders.cancel`, `DELETE /trade/orders`): its nonce as 8 bytes.
pub fn pack_cancel_all(nonce: u64) -> Result<Payload, SignError> {
    Ok(pack_u64(field(nonce, "nonce past 2^63 - 1")?))
}

/// The packed cancel-on-disconnect arm (`orders.enableCancelOnDisconnect`, params `{nonce}`):
/// its nonce as 8 bytes, as cancel-all packs it. Java and the SDK at 14a76b9 send the arm
/// unsigned; Hibachi's API collection is cited as wanting it signed. The probe of FBC-xvtr finds
/// which form Hibachi takes, and the order-entry codec sends that one.
pub fn pack_arm(nonce: u64) -> Result<Payload, SignError> {
    pack_cancel_all(nonce)
}

/// The venue's order id as a cancel packs it: decimal digits, no sign and no leading zero, at
/// most 2^63 − 1.
pub fn order_id(text: &str) -> Result<u64, SignError> {
    const NOT_AN_ID: SignError = SignError::Unsignable("order id not a decimal u64");
    let digits = text.as_bytes();
    let canonical = digits.first() != Some(&b'0') || digits.len() == 1;
    if digits.is_empty() || !canonical || !digits.iter().all(u8::is_ascii_digit) {
        return Err(NOT_AN_ID);
    }
    let id: u64 = text.parse().map_err(|_| NOT_AN_ID)?;
    field(id, "order id past 2^63 - 1")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }

    #[test]
    fn scaling_exactly_refuses_a_fraction_and_a_value_past_63_bits() {
        assert_eq!(scale_exact(dec("0.01"), 9), Ok(10_000_000));
        assert_eq!(scale_exact(dec("1.000"), 0), Ok(1));
        assert_eq!(scale_exact(dec("0"), 255), Ok(0));
        assert_eq!(scale_exact(dec("0.0123456789"), 9), Err(Scale::NotWhole));
        assert_eq!(scale_exact(dec("0.000000001"), 8), Err(Scale::NotWhole));
        assert_eq!(scale_exact(dec("9.223372036854775807"), 18), Ok(MAX_FIELD));
        assert_eq!(
            scale_exact(dec("9.223372036854775808"), 18),
            Err(Scale::TooLarge)
        );
        // 10^255 does not fit a u128; neither does a mantissa times 10^39.
        assert_eq!(scale_exact(dec("1"), 255), Err(Scale::TooLarge));
        assert_eq!(scale_exact(dec("1"), 39), Err(Scale::TooLarge));
        // Past u64 but within u128.
        assert_eq!(scale_exact(dec("1"), 20), Err(Scale::TooLarge));
    }

    #[test]
    fn a_price_truncates_toward_zero_and_is_refused_past_63_bits() {
        // 2500.01 × 2^32 × 10^-3 = 10737461189.67296.
        assert_eq!(scale_price(dec("2500.01"), -3), Some(10_737_461_189));
        // 100000 × 2^32 × 10^-4: the docs' example, exact.
        assert_eq!(scale_price(dec("100000"), -4), Some(42_949_672_960));
        // Settlement above underlying multiplies.
        assert_eq!(scale_price(dec("1"), 4), Some(42_949_672_960_000));
        assert_eq!(
            scale_price(dec("2147483647"), 0),
            Some(9_223_372_032_559_808_512)
        );
        assert_eq!(scale_price(dec("2147483648"), 0), None);
        assert_eq!(scale_price(dec("1"), 18), None);
        assert_eq!(scale_price(dec("1"), 255), None);
        // Far below one unit: zero, however large the divisor.
        assert_eq!(scale_price(dec("0.000001"), -12), Some(0));
        assert_eq!(scale_price(dec("12345.678"), -255), Some(0));
        assert_eq!(scale_price(dec("0"), 255), Some(0));
        // The largest Decimal mantissa times 2^32 still fits a u128.
        assert_eq!(scale_price(Decimal::MAX, -60), Some(0));
    }

    #[test]
    fn an_order_id_is_canonical_decimal_digits_within_63_bits() {
        assert_eq!(order_id("579183763093760000"), Ok(579_183_763_093_760_000));
        assert_eq!(order_id("0"), Ok(0));
        assert_eq!(order_id("9223372036854775807"), Ok(MAX_FIELD));
        let not_an_id = Err(SignError::Unsignable("order id not a decimal u64"));
        for text in [
            "",
            "+1",
            "-1",
            "01",
            "1.0",
            "1e3",
            " 1",
            "x",
            "18446744073709551616",
        ] {
            assert_eq!(order_id(text), not_an_id, "{text:?}");
        }
        assert_eq!(
            order_id("9223372036854775808"),
            Err(SignError::Unsignable("order id past 2^63 - 1"))
        );
    }

    #[test]
    fn eight_byte_payloads_refuse_a_value_past_63_bits() {
        assert_eq!(
            pack_cancel(CancelTarget::OrderId(12345))
                .unwrap()
                .as_bytes(),
            &12345u64.to_be_bytes()
        );
        assert_eq!(
            pack_cancel(CancelTarget::PlacementNonce(MAX_FIELD + 1)),
            Err(SignError::Unsignable("placement nonce past 2^63 - 1"))
        );
        assert_eq!(
            pack_cancel(CancelTarget::OrderId(u64::MAX)),
            Err(SignError::Unsignable("order id past 2^63 - 1"))
        );
        assert_eq!(
            pack_cancel_all(MAX_FIELD + 1),
            Err(SignError::Unsignable("nonce past 2^63 - 1"))
        );
        assert_eq!(pack_arm(7).unwrap(), pack_cancel_all(7).unwrap());
    }

    #[test]
    fn a_payload_shows_only_its_length() {
        let payload = pack_cancel_all(0x0102_0304_0506_0708).unwrap();
        assert_eq!(format!("{payload:?}"), "Payload(8 bytes)");
    }

    #[test]
    fn the_side_words_are_ask_zero_and_bid_one() {
        assert_eq!(side_word(Side::Sell), 0);
        assert_eq!(side_word(Side::Buy), 1);
    }
}
