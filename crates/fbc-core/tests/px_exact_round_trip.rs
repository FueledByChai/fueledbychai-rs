//! `PxExact`'s `Display` and `FromStr` round-trip over the whole representable range: every
//! exponent from `i8::MIN` to `i8::MAX`, the extreme mantissas (`i64::MIN` included), and a
//! seeded pseudo-random sweep. Parsing what was printed gives back the same value (by
//! `PxExact`'s value equality) and prints the same text again.

use fbc_core::PxExact;

const EXTREME_MANTISSAS: [i64; 16] = [
    i64::MIN,
    i64::MIN + 1,
    -1_000_000_000_000_000_000,
    -999_999_999_999_999_999,
    -10,
    -1,
    0,
    1,
    7,
    10,
    100,
    123_456_789,
    999_999_999_999_999_999,
    1_000_000_000_000_000_000,
    i64::MAX - 1,
    i64::MAX,
];

fn assert_round_trip(px: PxExact) {
    let text = px.to_string();
    let back: PxExact = text
        .parse()
        .unwrap_or_else(|err| panic!("{px:?} printed as {text:?} does not parse: {err}"));
    assert_eq!(back, px, "{text}");
    assert_eq!(back.to_string(), text, "{px:?}");
}

#[test]
fn every_exponent_round_trips_with_extreme_mantissas() {
    for exp in i8::MIN..=i8::MAX {
        for mantissa in EXTREME_MANTISSAS {
            assert_round_trip(PxExact::new(mantissa, exp));
        }
    }
}

/// splitmix64: a fixed seed, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[test]
fn pseudo_random_prices_round_trip() {
    let mut rng = Rng(0x00f8_c0de);
    for _ in 0..100_000 {
        let raw = rng.next();
        // Shift by 0..64 bits so short and long mantissas are both common.
        let mantissa = (raw as i64) >> (rng.next() % 64);
        let exp = rng.next() as u8 as i8;
        assert_round_trip(PxExact::new(mantissa, exp));
    }
}

fn parse(text: &str) -> Option<PxExact> {
    text.parse().ok()
}

#[test]
fn the_extreme_mantissas_parse_and_one_past_them_does_not() {
    assert_eq!(
        parse("-9223372036854775808"),
        Some(PxExact::new(i64::MIN, 0))
    );
    assert_eq!(
        parse("-0.9223372036854775808"),
        Some(PxExact::new(i64::MIN, -19))
    );
    assert_eq!(
        parse("9223372036854775807"),
        Some(PxExact::new(i64::MAX, 0))
    );
    assert_eq!(parse("9223372036854775808"), None);
    assert_eq!(parse("-9223372036854775809"), None);
    // Trailing zeros beyond what an i64 holds move into the exponent, exactly.
    assert_eq!(
        parse("92233720368547758070"),
        Some(PxExact::new(i64::MAX, 1))
    );
    assert_eq!(parse("92233720368547758071"), None);
    assert_eq!(
        parse("000000000000000000000001.5"),
        Some(PxExact::new(15, -1))
    );
}

#[test]
fn the_extreme_exponents_parse_and_one_past_them_does_not() {
    let zeros = |n: usize| "0".repeat(n);
    // 10^-128, the smallest exponent, and 10^127, the largest.
    assert_eq!(
        parse(&format!("0.{}1", zeros(127))),
        Some(PxExact::new(1, i8::MIN))
    );
    assert_eq!(
        parse(&format!("-0.{}1", zeros(127))),
        Some(PxExact::new(-1, i8::MIN))
    );
    assert_eq!(
        parse(&format!("1{}", zeros(127))),
        Some(PxExact::new(1, i8::MAX))
    );
    // The largest magnitudes are a 19-digit mantissa times 10^127: 10^145 fits, 10^146 does
    // not, and neither does 10^-129.
    assert_eq!(
        parse(&format!("1{}", zeros(145))),
        Some(PxExact::new(1_000_000_000_000_000_000, i8::MAX))
    );
    assert_eq!(
        parse(&format!("-9223372036854775808{}", zeros(127))),
        Some(PxExact::new(i64::MIN, i8::MAX))
    );
    assert_eq!(parse(&format!("1{}", zeros(146))), None);
    assert_eq!(parse(&format!("0.{}1", zeros(128))), None);
    // More than 128 fraction digits is fine when the extra ones are zeros.
    assert_eq!(
        parse(&format!("0.{}1{}", zeros(127), zeros(50))),
        Some(PxExact::new(1, i8::MIN))
    );
    // Zero is exact at any written precision.
    assert_eq!(
        parse(&format!("0.{}", zeros(300))),
        Some(PxExact::new(0, 0))
    );
    assert_eq!(parse(&format!("-{}", zeros(300))), Some(PxExact::new(0, 0)));
}
