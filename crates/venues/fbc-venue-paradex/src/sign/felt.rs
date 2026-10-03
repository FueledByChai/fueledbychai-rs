//! How a value in a Paradex typed-data message becomes a field element, exactly as the Java
//! signer's TypedData (starknet-jvm 0.17.0, `TypedData.feltFromPrimitive`) does it, and how a
//! decimal size or price is scaled to an integer as the Java `ParadexOrder` does it.

use rust_decimal::Decimal;
use starknet_crypto::Felt;

/// Why a value has no field element.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FeltError {
    /// The text is not ASCII. Java would read some non-ASCII digits as numbers; this signer
    /// signs ASCII only, so it never guesses.
    NotAscii,
    /// A number below zero.
    Negative,
    /// A number at or past the field prime.
    TooLarge,
    /// Text that is neither a number nor a short string of at most 31 characters.
    TooLong,
}

/// The field element Java's TypedData makes of the string `s` in a message:
///
/// 1. decimal digits, with an optional leading `-`, are that integer (a negative one, or one at
///    or past the prime, is refused as Java refuses it);
/// 2. the empty string is zero;
/// 3. `0x` then an optional sign and hex digits is that integer, when it lies in the field;
/// 4. anything else is a Cairo short string: at most 31 ASCII characters, big-endian bytes.
pub fn message_felt(s: &str) -> Result<Felt, FeltError> {
    if !s.is_ascii() {
        return Err(FeltError::NotAscii);
    }
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        let value = parse_radix(digits, 10).ok_or(FeltError::TooLarge)?;
        return match (negative, value == Felt::ZERO) {
            (true, false) => Err(FeltError::Negative),
            _ => Ok(value),
        };
    }
    if s.is_empty() {
        return Ok(Felt::ZERO);
    }
    if let Some(hex) = s.strip_prefix("0x").and_then(hex_value) {
        return Ok(hex);
    }
    short_string(s)
}

/// What `new BigInteger(rest, 16)` then `new Felt(..)` accept after the `0x`: an optional sign,
/// at least one hex digit, and a value in the field.
fn hex_value(rest: &str) -> Option<Felt> {
    let (negative, digits) = match rest.as_bytes().first() {
        Some(b'-') => (true, &rest[1..]),
        Some(b'+') => (false, &rest[1..]),
        _ => (false, rest),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let value = parse_radix(digits, 16)?;
    (!negative || value == Felt::ZERO).then_some(value)
}

/// A Cairo short string: at most 31 ASCII bytes, read as a big-endian integer.
pub fn short_string(s: &str) -> Result<Felt, FeltError> {
    if !s.is_ascii() {
        return Err(FeltError::NotAscii);
    }
    if s.len() > 31 {
        return Err(FeltError::TooLong);
    }
    let mut bytes = [0u8; 32];
    bytes[32 - s.len()..].copy_from_slice(s.as_bytes());
    Ok(Felt::from_bytes_be(&bytes))
}

/// The ASCII digits in `radix` (10 or 16) as a field element, or `None` at or past the prime.
/// Parsed by hand because `Felt::from_dec_str` and `Felt::from_hex` reduce modulo the prime,
/// where Java refuses.
pub(super) fn parse_radix(digits: &str, radix: u32) -> Option<Felt> {
    // Little-endian 64-bit limbs of a 256-bit accumulator.
    let mut limbs = [0u64; 4];
    for c in digits.chars() {
        let mut carry = u128::from(c.to_digit(radix)?);
        for limb in &mut limbs {
            let wide = u128::from(*limb) * u128::from(radix) + carry;
            *limb = wide as u64; // the low 64 bits; the rest carries
            carry = wide >> 64;
        }
        if carry != 0 {
            return None;
        }
    }
    let be = [limbs[3], limbs[2], limbs[1], limbs[0]];
    if be > Felt::MAX.to_be_digits() {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (chunk, limb) in bytes.chunks_exact_mut(8).zip(be) {
        chunk.copy_from_slice(&limb.to_be_bytes());
    }
    Some(Felt::from_bytes_be(&bytes))
}

/// Why a decimal has no scaled integer.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ScaleError {
    Negative,
}

/// `d` x 10^8, truncated toward zero, as the Java `ParadexOrder.getChainSize` and
/// `getChainLimitPrice` compute it (`BigDecimal.scaleByPowerOfTen(8).toBigInteger()`): digits
/// past the eighth decimal place are dropped, never rounded. A negative value is refused; Java
/// would build a negative felt and fail.
///
/// Every `Decimal` fits: its mantissa is below 2^96, so the result is below 2^96 x 10^8 < 2^123.
pub fn scale_e8(d: Decimal) -> Result<u128, ScaleError> {
    if d.is_sign_negative() && !d.is_zero() {
        return Err(ScaleError::Negative);
    }
    let mantissa = d.mantissa().unsigned_abs();
    let scale = d.scale();
    Ok(if scale >= 8 {
        mantissa / 10u128.pow(scale - 8)
    } else {
        mantissa * 10u128.pow(8 - scale)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }

    #[test]
    fn decimal_strings_are_integers() {
        assert_eq!(message_felt("1"), Ok(Felt::ONE));
        assert_eq!(message_felt("0007"), Ok(Felt::from(7u64)));
        assert_eq!(message_felt("-0"), Ok(Felt::ZERO));
        assert_eq!(message_felt("-5"), Err(FeltError::Negative));
        assert_eq!(
            message_felt("1759400000123201703010000"),
            Ok(Felt::from(1_759_400_000_123_201_703_010_000u128))
        );
        // The prime minus one is the largest; the prime itself and past 2^256 are refused.
        let max = Felt::MAX.to_string();
        assert_eq!(message_felt(&max), Ok(Felt::MAX));
        let prime = "3618502788666131213697322783095070105623107215331596699973092056135872020481";
        assert_eq!(message_felt(prime), Err(FeltError::TooLarge));
        assert_eq!(message_felt(&"9".repeat(80)), Err(FeltError::TooLarge));
    }

    #[test]
    fn empty_is_zero_and_hex_is_an_integer() {
        assert_eq!(message_felt(""), Ok(Felt::ZERO));
        assert_eq!(message_felt("0x1F"), Ok(Felt::from(31u64)));
        assert_eq!(message_felt("0x+1f"), Ok(Felt::from(31u64)));
        assert_eq!(message_felt("0x-0"), Ok(Felt::ZERO));
        assert_eq!(message_felt("0x0"), Ok(Felt::ZERO));
    }

    #[test]
    fn hex_java_refuses_falls_back_to_a_short_string() {
        for s in ["0x", "0x-1", "0xZZ", "0x+", "-"] {
            assert_eq!(message_felt(s), short_string(s), "{s}");
            assert!(message_felt(s).is_ok(), "{s}");
        }
        // The prime in hex is not in the field, and past 31 characters it is no short string
        // either; one below it is the largest felt.
        let prime = format!("0x800000000000011{}1", "0".repeat(47));
        assert_eq!(message_felt(&prime), Err(FeltError::TooLong));
        let max = format!("0x800000000000011{}0", "0".repeat(47));
        assert_eq!(message_felt(&max), Ok(Felt::MAX));
        let past_256_bits = format!("0x1{}", "0".repeat(64));
        assert_eq!(message_felt(&past_256_bits), Err(FeltError::TooLong));
    }

    #[test]
    fn other_text_is_a_short_string() {
        assert_eq!(message_felt("LIMIT"), Ok(Felt::from(0x4c_49_4d_49_54u64)));
        assert_eq!(short_string("A"), Ok(Felt::from(65u64)));
        assert_eq!(message_felt(&"a".repeat(31)), short_string(&"a".repeat(31)));
        assert_eq!(message_felt(&"a".repeat(32)), Err(FeltError::TooLong));
        assert_eq!(message_felt("BTC-USD-PERP\u{e9}"), Err(FeltError::NotAscii));
        assert_eq!(short_string("\u{e9}"), Err(FeltError::NotAscii));
    }

    #[test]
    fn scaling_truncates_past_eight_places() {
        assert_eq!(scale_e8(dec("65123.45")), Ok(6_512_345_000_000));
        assert_eq!(scale_e8(dec("0.000000009")), Ok(0));
        assert_eq!(scale_e8(dec("1.999999999")), Ok(199_999_999));
        assert_eq!(scale_e8(dec("7")), Ok(700_000_000));
        assert_eq!(scale_e8(dec("0.12345678")), Ok(12_345_678));
        assert_eq!(
            scale_e8(Decimal::MAX),
            Ok(7_922_816_251_426_433_759_354_395_033_500_000_000)
        );
        assert_eq!(scale_e8(Decimal::new(1, 28)), Ok(0));
        assert_eq!(scale_e8(dec("-0")), Ok(0));
        assert_eq!(scale_e8(dec("-0.1")), Err(ScaleError::Negative));
    }
}
