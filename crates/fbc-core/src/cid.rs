//! The canonical client-id codec (decision 0004, design §4.3).
//!
//! This crate owns the canonical payload: a format tag (version 1), the namespace and the
//! sequence, plus a check value. Adapters supply only the wire format ([`ClientIdFormat`]);
//! they never see or build a [`ClientOrderId`]'s fields. Decoding accepts exactly what encoding
//! produces, so an id minted by any other system (a Java broker's millisecond counter, a random
//! UUID, a typed-in label) or a corrupted one decodes as [`CidMatch::Unparseable`].
//!
//! # Layouts
//!
//! Every layout carries the tag, the namespace and the sequence in fixed positions, so they
//! all decode to the same [`ClientOrderId`]:
//!
//! - [`ClientIdFormat::Uuid`]: an RFC 9562 version 8 UUID, lowercase and hyphenated. Bits from
//!   the top: tag (8), namespace (16), seq bits 63..40 (24), version `8` (4), seq bits 39..28
//!   (12), variant `10` (2), seq bits 27..0 (28), check (34). A random (version 4) or time-based
//!   (version 7) UUID fails the version test.
//! - [`ClientIdFormat::Hex`]: 30 lowercase hex digits, tag (2), namespace (4), seq (16) and
//!   check (8), left-padded with `0` to the format's exact length.
//! - [`ClientIdFormat::Numeric`]: decimal digits `9`, the tag as two digits, the namespace as
//!   five, the seq with no leading zeros, two check digits, and two ISO 7064 MOD 97-10 digits
//!   over the whole string, which catch every single-digit substitution. The leading `9` and
//!   the tag sit in the top digits, so the Java brokers' counters (seeded from
//!   `System.currentTimeMillis()`, so starting with `1`) never decode.
//! - [`ClientIdFormat::Alnum`]: the 120-bit number tag (8) | namespace (16) | seq (64) |
//!   check (32), in base 62 (`0-9A-Za-z`, 21 characters) or base 36 (`0-9a-z`, 24
//!   characters), left-padded with `0`.
//!
//! The check value is a fixed mixing hash of the format, tag, namespace and seq. It guards
//! against misreading a foreign or corrupted id as ours; it is not a signature.

use arrayvec::ArrayString;

use crate::ids::{CidMatch, ClientOrderId, IdError, Namespace};

/// The canonical payload's version, carried in every wire id.
const TAG_V1: u8 = 1;

/// The longest wire id [`encode_cid`] produces.
pub const MAX_WIRE_LEN: usize = 48;

/// A wire client id produced by [`encode_cid`].
pub type WireCid = ArrayString<MAX_WIRE_LEN>;

/// The characters an alphanumeric client id may use.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Charset {
    /// `0-9A-Za-z`, case-sensitive (base 62).
    Alphanumeric,
    /// `0-9a-z`, for venues that fold or restrict case (base 36).
    LowerAlphanumeric,
}

/// How a venue spells a client id on the wire; declared by the venue adapter.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ClientIdFormat {
    /// Up to `max_len` characters from `charset` (Hibachi: up to 32 of `[A-Za-z0-9-]`).
    Alnum {
        /// The longest id the venue accepts.
        max_len: u8,
        /// The characters it accepts.
        charset: Charset,
    },
    /// A UUID (Paradex); the payload sits in a version 8 UUID.
    Uuid,
    /// Up to `max_digits` decimal digits.
    Numeric {
        /// The most digits the venue accepts.
        max_digits: u8,
    },
    /// Exactly `len` hex digits.
    Hex {
        /// The exact number of digits the venue requires.
        len: u8,
    },
}

/// The canonical wire spelling of `cid` in `fmt`, or [`IdError::DoesNotFit`] when the format is
/// too small to carry the payload.
pub fn encode_cid(fmt: &ClientIdFormat, cid: ClientOrderId) -> Result<WireCid, IdError> {
    let (ns, seq) = (cid.namespace().get(), cid.seq());
    let mut out = WireCid::new();
    match *fmt {
        ClientIdFormat::Uuid => push_uuid(&mut out, uuid_bits(ns, seq)),
        ClientIdFormat::Hex { len } => {
            fits(HEX_DIGITS, usize::from(len))?;
            fits(usize::from(len), MAX_WIRE_LEN)?;
            push_zeros(&mut out, usize::from(len) - HEX_DIGITS);
            push_radix(&mut out, hex_value(ns, seq), 16, HEX_DIGITS, HEX_LOWER);
        }
        ClientIdFormat::Numeric { max_digits } => {
            let body = numeric_body(ns, seq);
            fits(body.len() + 2, usize::from(max_digits))?;
            out.push_str(&body);
            push_mod97(&mut out);
        }
        ClientIdFormat::Alnum { max_len, charset } => {
            let (radix, width, digits) = alnum_params(charset);
            fits(width, usize::from(max_len))?;
            push_radix(&mut out, alnum_value(ns, seq), radix, width, digits);
        }
    }
    Ok(out)
}

/// What `wire` is to the engine that owns namespace `own`: one of its ids, another namespace's,
/// or not a canonical client id at all.
pub fn decode_cid(fmt: &ClientIdFormat, own: Namespace, wire: &str) -> CidMatch {
    let Some((ns, seq)) = parse(fmt, wire) else {
        return CidMatch::Unparseable;
    };
    let cid = ClientOrderId::new(Namespace::new(ns), seq);
    match encode_cid(fmt, cid) {
        Ok(canonical) if is_canonical(fmt, &canonical, wire) => {
            if cid.namespace() == own {
                CidMatch::Ours(cid)
            } else {
                CidMatch::Foreign(cid.namespace())
            }
        }
        _ => CidMatch::Unparseable,
    }
}

/// Only the canonical spelling decodes: exactly, for the case-sensitive alphanumeric layouts,
/// and in either case for hex and UUIDs, which venues may echo in upper case.
fn is_canonical(fmt: &ClientIdFormat, canonical: &str, wire: &str) -> bool {
    match fmt {
        ClientIdFormat::Alnum { .. } | ClientIdFormat::Numeric { .. } => canonical == wire,
        ClientIdFormat::Uuid | ClientIdFormat::Hex { .. } => canonical.eq_ignore_ascii_case(wire),
    }
}

/// The namespace and seq a wire id carries, when it has the shape of `fmt` and its checks hold.
fn parse(fmt: &ClientIdFormat, wire: &str) -> Option<(u16, u64)> {
    match *fmt {
        ClientIdFormat::Uuid => {
            let bits = parse_uuid(wire)?;
            let field = |shift: u32, width: u32| (bits >> shift) & ((1u128 << width) - 1);
            if field(76, 4) != 8 || field(62, 2) != 0b10 || field(120, 8) != u128::from(TAG_V1) {
                return None;
            }
            let ns = field(104, 16) as u16;
            let seq = (field(80, 24) << 40 | field(64, 12) << 28 | field(34, 28)) as u64;
            (field(0, 34) as u64 == check(KIND_UUID, ns, seq) & UUID_CHECK_MASK)
                .then_some((ns, seq))
        }
        ClientIdFormat::Hex { len } => {
            // Byte offsets below are character offsets only in ASCII text.
            if !wire.is_ascii() || wire.len() != usize::from(len) || wire.len() < HEX_DIGITS {
                return None;
            }
            let value = parse_radix(&wire[wire.len() - HEX_DIGITS..], 16, HEX_LOWER)?;
            split_wide(value, KIND_HEX)
        }
        ClientIdFormat::Numeric { max_digits } => {
            if wire.len() > usize::from(max_digits) || wire.len() < NUMERIC_FIXED + 1 {
                return None;
            }
            if !wire.bytes().all(|b| b.is_ascii_digit()) || mod97(wire) != 1 {
                return None;
            }
            let body = &wire[..wire.len() - 2];
            if &body[..3] != NUMERIC_PREFIX {
                return None;
            }
            let ns = u16::try_from(body[3..8].parse::<u32>().ok()?).ok()?;
            let seq = body[8..body.len() - 2].parse::<u64>().ok()?;
            let chk = body[body.len() - 2..].parse::<u64>().ok()?;
            (chk == check(KIND_NUMERIC, ns, seq) % 100).then_some((ns, seq))
        }
        ClientIdFormat::Alnum { max_len, charset } => {
            let (radix, width, digits) = alnum_params(charset);
            if wire.len() != width || wire.len() > usize::from(max_len) {
                return None;
            }
            split_wide(parse_radix(wire, radix, digits)?, KIND_ALNUM)
        }
    }
}

const KIND_UUID: u8 = 1;
const KIND_HEX: u8 = 2;
const KIND_NUMERIC: u8 = 3;
const KIND_ALNUM: u8 = 4;

const UUID_CHECK_MASK: u64 = (1 << 34) - 1;
const HEX_DIGITS: usize = 30;
const HEX_LOWER: &[u8] = b"0123456789abcdef";
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const BASE36: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
/// `9` then the tag as two digits.
const NUMERIC_PREFIX: &str = "901";
/// Prefix, namespace, two check digits and two MOD 97-10 digits; the seq adds the rest.
const NUMERIC_FIXED: usize = 3 + 5 + 2 + 2;

/// Radix, fixed width and digits of an alphanumeric layout: the smallest width that holds 2^120.
fn alnum_params(charset: Charset) -> (u32, usize, &'static [u8]) {
    match charset {
        Charset::Alphanumeric => (62, 21, BASE62),
        Charset::LowerAlphanumeric => (36, 24, BASE36),
    }
}

/// The check value: a splitmix64-style mix of the format, tag, namespace and seq.
fn check(kind: u8, ns: u16, seq: u64) -> u64 {
    let head = u64::from(kind) << 24 | u64::from(TAG_V1) << 16 | u64::from(ns);
    let mut x = seq ^ head.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(29);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// The 120-bit payload tag | ns | seq | check(32) used by the hex and alphanumeric layouts.
fn wide(kind: u8, ns: u16, seq: u64) -> u128 {
    u128::from(TAG_V1) << 112
        | u128::from(ns) << 96
        | u128::from(seq) << 32
        | u128::from(check(kind, ns, seq) as u32)
}

fn split_wide(value: u128, kind: u8) -> Option<(u16, u64)> {
    if value >> 112 != u128::from(TAG_V1) {
        return None;
    }
    let ns = (value >> 96) as u16;
    let seq = (value >> 32) as u64;
    (value as u32 == check(kind, ns, seq) as u32).then_some((ns, seq))
}

fn hex_value(ns: u16, seq: u64) -> u128 {
    wide(KIND_HEX, ns, seq)
}

fn alnum_value(ns: u16, seq: u64) -> u128 {
    wide(KIND_ALNUM, ns, seq)
}

fn uuid_bits(ns: u16, seq: u64) -> u128 {
    let seq = u128::from(seq);
    u128::from(TAG_V1) << 120
        | u128::from(ns) << 104
        | (seq >> 40) << 80
        | 8 << 76
        | ((seq >> 28) & 0xfff) << 64
        | 0b10 << 62
        | (seq & 0x0fff_ffff) << 34
        | u128::from(check(KIND_UUID, ns, seq as u64) & UUID_CHECK_MASK)
}

fn push_uuid(out: &mut WireCid, bits: u128) {
    let mut hex = WireCid::new();
    push_radix(&mut hex, bits, 16, 32, HEX_LOWER);
    for (i, group) in [(0, 8), (8, 12), (12, 16), (16, 20), (20, 32)]
        .into_iter()
        .enumerate()
    {
        if i > 0 {
            out.push('-');
        }
        out.push_str(&hex[group.0..group.1]);
    }
}

fn parse_uuid(wire: &str) -> Option<u128> {
    let b = wire.as_bytes();
    if b.len() != 36 || [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
        return None;
    }
    let mut hex = WireCid::new();
    for part in wire.split('-') {
        hex.try_push_str(part).ok()?;
    }
    if hex.len() != 32 {
        return None;
    }
    parse_radix(&hex, 16, HEX_LOWER)
}

/// `9`, the tag, the namespace, the seq and two check digits; the MOD 97-10 digits follow.
fn numeric_body(ns: u16, seq: u64) -> ArrayString<40> {
    let mut body = ArrayString::<40>::new();
    let chk = check(KIND_NUMERIC, ns, seq) % 100;
    // At most 3 + 5 + 20 + 2 = 30 digits.
    let _ = core::fmt::write(
        &mut body,
        format_args!("{NUMERIC_PREFIX}{ns:05}{seq}{chk:02}"),
    );
    body
}

/// Appends the two ISO 7064 MOD 97-10 check digits that make the whole string ≡ 1 (mod 97).
fn push_mod97(out: &mut WireCid) {
    let r = mod97(out) * 100 % 97;
    let digits = (98 - r) % 97;
    let _ = core::fmt::write(out, format_args!("{digits:02}"));
}

fn mod97(digits: &str) -> u32 {
    digits
        .bytes()
        .fold(0, |r, b| (r * 10 + u32::from(b - b'0')) % 97)
}

fn push_zeros(out: &mut WireCid, n: usize) {
    for _ in 0..n {
        out.push('0');
    }
}

/// `value` in `radix`, exactly `width` digits, left-padded with the zero digit.
fn push_radix(out: &mut WireCid, mut value: u128, radix: u32, width: usize, digits: &[u8]) {
    let mut buf = [b'0'; 32];
    for slot in buf[..width].iter_mut().rev() {
        *slot = digits[(value % u128::from(radix)) as usize];
        value /= u128::from(radix);
    }
    out.push_str(core::str::from_utf8(&buf[..width]).unwrap_or_default());
}

/// Parses digits of `radix` (hex in either case), refusing overflow and foreign characters.
fn parse_radix(wire: &str, radix: u32, digits: &[u8]) -> Option<u128> {
    wire.bytes().try_fold(0u128, |acc, b| {
        let b = if radix == 16 {
            b.to_ascii_lowercase()
        } else {
            b
        };
        let d = digits.iter().position(|&c| c == b)?;
        acc.checked_mul(u128::from(radix))?.checked_add(d as u128)
    })
}

fn fits(needed: usize, max: usize) -> Result<(), IdError> {
    if needed <= max {
        Ok(())
    } else {
        Err(IdError::DoesNotFit { needed, max })
    }
}
