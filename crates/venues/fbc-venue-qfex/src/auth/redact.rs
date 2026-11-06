//! Which bytes of a QFEX private frame or response identify the account (decisions 0009 and
//! 0028; a review path, under `src/auth`).
//!
//! [`spans`] walks the input as JSON, byte by byte, so it sees every member as it is written: a
//! duplicate member a parser would overwrite, and a name or a value spelled with `\u` escapes,
//! which it decodes before it compares. It only names spans. The journal writes each span as a
//! keyed hash and reads it back as the ASCII digit `2` (fbc-journal's `BLANK`), so a redacted
//! string's characters read back as a string of `2`s of the same length between the same
//! quotes, a redacted number's digits as a number, and every other byte as it came. A codec
//! that never reads the redacted members decodes the record in replay as it decoded it live.

use core::ops::Range;
use std::borrow::Cow;

use serde_json::Value;

/// What identifies this account in an input, besides the members [`SENSITIVE`] names.
pub(super) struct Marks<'a> {
    /// The credential's public key.
    pub(super) public_key: &'a str,
    /// The selected subaccount's UUID, lowercase; `None` for the primary account.
    pub(super) subaccount: Option<&'a str>,
}

/// The members whose value names an account, a user or a credential, wherever they sit:
///
/// - `user_id`: QFEX's trade AsyncAPI gives it to `OrderResponse` (so to `order_response` and
///   every row of `all_orders_response.orders`), `FillResponse`, `TwapResponse`,
///   `BalancePayload`, `StopOrder` and `PnlEntryPayload`;
/// - `id`: `PositionPayload`, `BalancePayload` and `LeveragePublic` carry one that no page
///   describes; it could name the account, so it is treated as if it did;
/// - `account_id`, `public_key`, `signature`, `jwt`: what an authentication sends, which an
///   echo can return;
/// - `api_key`, `secret_key`: credential names no QFEX reply is documented to carry, kept out
///   all the same;
/// - `account_ids`: the provider user's subaccounts, which
///   [`GET /user/subaccounts`](https://docs.qfex.com/api-reference/rest/user/list-subaccounts)
///   lists (FBC-h3z1, decision 0122 item 3);
/// - `balance`: every value of the account's balance, which
///   [`GET /user/positions`](https://docs.qfex.com/api-reference/rest/user/user-positions)
///   answers beside the positions (FBC-h3z1, decision 0009).
pub(super) const SENSITIVE: [&str; 10] = [
    "user_id",
    "id",
    "account_id",
    "public_key",
    "signature",
    "jwt",
    "api_key",
    "secret_key",
    "account_ids",
    "balance",
];

/// The deepest nesting walked, past serde_json's own limit of 128, so whatever serde_json
/// parses is walked whole.
const MAX_DEPTH: usize = 256;

/// The spans of `bytes` to redact, in order and apart. Empty input has none; input that is not
/// UTF-8 JSON that `serde_json` parses is redacted whole (see [`whole`]).
pub(super) fn spans(bytes: &[u8], marks: &Marks<'_>) -> Vec<Range<u32>> {
    if bytes.is_empty() {
        return Vec::new();
    }
    // A codec reads a QFEX frame with serde_json: what it cannot parse, nothing decodes.
    let Some(text) = core::str::from_utf8(bytes)
        .ok()
        .filter(|text| u32::try_from(text.len()).is_ok())
        .filter(|text| serde_json::from_str::<Value>(text).is_ok())
    else {
        return whole(bytes);
    };
    let mut walk = Walk {
        text,
        bytes,
        at: 0,
        marks,
        spans: Vec::new(),
    };
    // Each end is at most the input's length, which fits a u32. The walk does not fail on
    // input serde_json parses; were it to, nothing would be left in clear.
    let walked = walk
        .document()
        .map(|()| walk.spans.into_iter().map(to_u32).collect());
    walked.unwrap_or_else(|| whole(bytes))
}

fn to_u32(span: Range<usize>) -> Range<u32> {
    span.start as u32..span.end as u32
}

/// All of an input that does not parse, but its leading whitespace and first character when
/// that character is ASCII and cannot start a number (a `{`, a `[`, a quote, a letter): they
/// name nothing, and kept, they leave the blanked input as unparseable as it was (`{222…`, not
/// the bare number `222…`), so a replayed codec meets what it met live. An input of nothing
/// else has no span. One longer than a `u32` can address is cut at `u32::MAX`, far past any
/// frame the runtime reads; the journal refuses a record that long (`JournalError::TooLarge`),
/// so none is written.
fn whole(bytes: &[u8]) -> Vec<Range<u32>> {
    let end = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    let Some(lead) = bytes
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
    else {
        return Vec::new();
    };
    let first = bytes[lead];
    let kept = if first.is_ascii() && !first.is_ascii_digit() && first != b'-' {
        lead + 1
    } else {
        0
    };
    let start = u32::try_from(kept).unwrap_or(end);
    // One span, or none when nothing follows what is kept.
    (start < end).then_some(start..end).into_iter().collect()
}

/// A string as written: the range of its characters between the quotes, and its text with
/// escapes decoded.
struct Str<'a> {
    chars: Range<usize>,
    text: Cow<'a, str>,
}

/// One pass over a JSON document, gathering the spans to redact.
struct Walk<'a> {
    text: &'a str,
    bytes: &'a [u8],
    at: usize,
    marks: &'a Marks<'a>,
    spans: Vec<Range<usize>>,
}

impl<'a> Walk<'a> {
    /// The document: one value, with nothing but whitespace after it.
    fn document(&mut self) -> Option<()> {
        self.value(0, false)?;
        self.whitespace();
        (self.at == self.bytes.len()).then_some(())
    }

    /// One value at `depth`; `sensitive` when it is, or is inside, a member [`SENSITIVE`]
    /// names, so each string and number in it is redacted.
    fn value(&mut self, depth: usize, sensitive: bool) -> Option<()> {
        self.whitespace();
        match *self.bytes.get(self.at)? {
            b'{' => self.object(depth + 1, sensitive),
            b'[' => self.array(depth + 1, sensitive),
            b'"' => {
                let string = self.string()?;
                if sensitive || self.marked(&string.text) {
                    self.redact(string.chars);
                }
                Some(())
            }
            // Literals name nothing, so they are kept even under a sensitive member.
            b't' => self.literal("true"),
            b'f' => self.literal("false"),
            b'n' => self.literal("null"),
            _ => {
                let start = self.at;
                self.number()?;
                if sensitive {
                    self.redact(start..self.at);
                }
                Some(())
            }
        }
    }

    fn object(&mut self, depth: usize, sensitive: bool) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.at += 1;
        self.whitespace();
        if self.eat(b'}') {
            return Some(());
        }
        loop {
            self.whitespace();
            if self.bytes.get(self.at) != Some(&b'"') {
                return None;
            }
            let name = self.string()?;
            // A member name is kept unless it spells this account itself.
            if self.marked(&name.text) {
                self.redact(name.chars.clone());
            }
            let inner = sensitive || SENSITIVE.contains(&name.text.as_ref());
            self.whitespace();
            if !self.eat(b':') {
                return None;
            }
            self.value(depth, inner)?;
            self.whitespace();
            if !self.eat(b',') {
                return self.eat(b'}').then_some(());
            }
        }
    }

    fn array(&mut self, depth: usize, sensitive: bool) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.at += 1;
        self.whitespace();
        if self.eat(b']') {
            return Some(());
        }
        loop {
            self.value(depth, sensitive)?;
            self.whitespace();
            if !self.eat(b',') {
                return self.eat(b']').then_some(());
            }
        }
    }

    /// The string starting at the `"` under the cursor, the cursor left past its closing quote.
    fn string(&mut self) -> Option<Str<'a>> {
        let text = self.text;
        let start = self.at + 1;
        let mut at = start;
        // The characters since the last escape, not yet copied into `decoded`.
        let mut run = start;
        let mut decoded: Option<String> = None;
        loop {
            match *self.bytes.get(at)? {
                b'"' => break,
                b'\\' => {
                    let out = decoded.get_or_insert_with(String::new);
                    out.push_str(&text[run..at]);
                    at = escape(self.bytes, at + 1, out)?;
                    run = at;
                }
                // JSON allows no raw control character inside a string.
                0x00..=0x1f => return None,
                _ => at += 1,
            }
        }
        self.at = at + 1;
        let text = match decoded {
            Some(mut out) => {
                out.push_str(&text[run..at]);
                Cow::Owned(out)
            }
            None => Cow::Borrowed(&text[start..at]),
        };
        Some(Str {
            chars: start..at,
            text,
        })
    }

    /// A number as JSON writes one: `-`, digits with no leading zero, a fraction, an exponent.
    fn number(&mut self) -> Option<()> {
        let digits = |walk: &mut Walk<'_>| {
            let start = walk.at;
            while walk.bytes.get(walk.at).is_some_and(u8::is_ascii_digit) {
                walk.at += 1;
            }
            (walk.at > start).then_some(())
        };
        self.eat(b'-');
        if !self.eat(b'0') {
            digits(self)?;
        }
        if self.eat(b'.') {
            digits(self)?;
        }
        if self.eat(b'e') || self.eat(b'E') {
            if !self.eat(b'+') {
                self.eat(b'-');
            }
            digits(self)?;
        }
        Some(())
    }

    fn literal(&mut self, word: &str) -> Option<()> {
        let end = self.at + word.len();
        if self.bytes.get(self.at..end)? != word.as_bytes() {
            return None;
        }
        self.at = end;
        Some(())
    }

    fn whitespace(&mut self) {
        while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    /// Steps past `byte` when it is under the cursor.
    fn eat(&mut self, byte: u8) -> bool {
        let here = self.bytes.get(self.at) == Some(&byte);
        if here {
            self.at += 1;
        }
        here
    }

    /// Whether a string's decoded text spells this account: its public key, its subaccount's
    /// UUID in either case, or a quoted [`SENSITIVE`] name, as a request echoed as a string
    /// would hold one.
    fn marked(&self, text: &str) -> bool {
        (!self.marks.public_key.is_empty() && text.contains(self.marks.public_key))
            || self
                .marks
                .subaccount
                .is_some_and(|id| text.to_ascii_lowercase().contains(id))
            || (text.contains('"')
                && SENSITIVE
                    .iter()
                    .any(|name| text.contains(&format!("\"{name}\""))))
    }

    /// Names `span` for the journal; an empty one holds nothing.
    fn redact(&mut self, span: Range<usize>) {
        if !span.is_empty() {
            self.spans.push(span);
        }
    }
}

/// Decodes the escape whose letter is at `at` (just past its `\`) onto `out`; where the string
/// goes on, or `None` for an escape JSON does not allow, a lone surrogate included.
fn escape(bytes: &[u8], at: usize, out: &mut String) -> Option<usize> {
    let simple = match *bytes.get(at)? {
        b'"' => '"',
        b'\\' => '\\',
        b'/' => '/',
        b'b' => '\u{8}',
        b'f' => '\u{c}',
        b'n' => '\n',
        b'r' => '\r',
        b't' => '\t',
        b'u' => {
            let high = hex4(bytes, at + 1)?;
            let (code, next) = match high {
                0xd800..=0xdbff => {
                    // A leading surrogate must be followed by `\u` and a trailing one.
                    if bytes.get(at + 5..at + 7)? != b"\\u" {
                        return None;
                    }
                    let low = hex4(bytes, at + 7)?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        return None;
                    }
                    let code =
                        0x1_0000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
                    (code, at + 11)
                }
                0xdc00..=0xdfff => return None,
                code => (u32::from(code), at + 5),
            };
            out.push(char::from_u32(code)?);
            return Some(next);
        }
        _ => return None,
    };
    out.push(simple);
    Some(at + 1)
}

/// The four hex digits at `at`.
fn hex4(bytes: &[u8], at: usize) -> Option<u16> {
    let digits = core::str::from_utf8(bytes.get(at..at + 4)?).ok()?;
    if !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u16::from_str_radix(digits, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
    const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";

    fn marks(subaccount: Option<&'static str>) -> Marks<'static> {
        Marks {
            public_key: PUBLIC,
            subaccount,
        }
    }

    /// What the journal reads back: every span as `2`s.
    fn blanked(input: &str, spans: &[Range<u32>]) -> String {
        let mut bytes = input.as_bytes().to_vec();
        for span in spans {
            bytes[span.start as usize..span.end as usize].fill(b'2');
        }
        String::from_utf8(bytes).unwrap()
    }

    fn redacted(input: &str) -> String {
        blanked(input, &spans(input.as_bytes(), &marks(Some(SUB))))
    }

    #[test]
    fn sensitive_members_are_redacted_at_any_depth_and_nothing_else() {
        let input = r#"{"a":{"user_id":"u-1","b":[{"id":12},{"id":-3.5e2}]},"user_id":"","x":"y"}"#;
        assert_eq!(
            redacted(input),
            r#"{"a":{"user_id":"222","b":[{"id":22},{"id":222222}]},"user_id":"","x":"y"}"#
        );
        // Every string and number inside a sensitive member; literals and names are kept.
        let input = r#"{"signature":{"k":["v",1,true,null]},"public_key":false}"#;
        assert_eq!(
            redacted(input),
            r#"{"signature":{"k":["2",2,true,null]},"public_key":false}"#
        );
        // Duplicates are each redacted, as written.
        let input = r#"{"user_id":"a","user_id":"b"}"#;
        assert_eq!(redacted(input), r#"{"user_id":"2","user_id":"2"}"#);
        // The private REST reads' listed account ids and balance values (FBC-h3z1).
        let input = r#"{"account_ids":["ab",null],"balance":{"deposit":1.5,"x":true}}"#;
        assert_eq!(
            redacted(input),
            r#"{"account_ids":["22",null],"balance":{"deposit":222,"x":true}}"#
        );
    }

    #[test]
    fn escaped_names_and_values_are_decoded_before_they_are_compared() {
        // `user\u005fid` is user_id; `qfex_\u0070ub...` is the public key.
        let input = r#"{"user\u005fid":"a\"b","m":"qfex_\u0070ub_SYNTHETIC_0001"}"#;
        assert_eq!(
            redacted(input),
            r#"{"user\u005fid":"2222","m":"2222222222222222222222222222"}"#
        );
        // The subaccount in upper case, and a surrogate pair in a kept string.
        let input = r#"{"m":"5A5A5A5A-0000-4000-8000-00000000C0DE","e":"\ud83d\ude00\n"}"#;
        assert_eq!(
            redacted(input),
            r#"{"m":"222222222222222222222222222222222222","e":"\ud83d\ude00\n"}"#
        );
        // A member name holding the public key is redacted, its value kept.
        let input = r#"{"qfex_pub_SYNTHETIC_0001":1}"#;
        assert_eq!(redacted(input), r#"{"22222222222222222222222":1}"#);
        // A request echoed as a string: its quoted sensitive name redacts it whole.
        let input = r#"{"echo":"{\"signature\":\"abc\"}","note":"bad signature"}"#;
        assert_eq!(
            redacted(input),
            r#"{"echo":"22222222222222222222222","note":"bad signature"}"#
        );
    }

    #[test]
    fn only_the_selected_subaccount_is_a_mark() {
        let input = format!(r#"{{"m":"{SUB}"}}"#);
        assert!(spans(input.as_bytes(), &marks(None)).is_empty());
        assert_eq!(spans(input.as_bytes(), &marks(Some(SUB))), vec![6..42]);
        // An empty public key marks nothing.
        let empty = Marks {
            public_key: "",
            subaccount: None,
        };
        assert!(spans(br#"{"m":"x"}"#, &empty).is_empty());
    }

    #[test]
    fn input_serde_json_cannot_parse_is_redacted_whole_and_stays_unparseable() {
        for input in [
            r#"{"user_id":"u-1""#,
            r#"{"user_id":"u-1"} x"#,
            r#"["u-1""#,
            r#"{"n":1e400}"#,
            r#"{"s":"\ud800"}"#,
        ] {
            let spans = spans(input.as_bytes(), &marks(None));
            assert_eq!(spans, vec![1..input.len() as u32], "{input}");
            let back = blanked(input, &spans);
            assert!(serde_json::from_str::<Value>(&back).is_err(), "{input}");
        }
        // The first character is kept only when it cannot start a number.
        assert_eq!(spans(b"nope", &marks(None)), vec![1..4]);
        assert_eq!(spans(b"  \"open", &marks(None)), vec![3..7]);
        assert_eq!(spans(b" 12x", &marks(None)), vec![0..4]);
        assert_eq!(spans(b"-x", &marks(None)), vec![0..2]);
        assert_eq!(spans(&[0xff, 0xfe], &marks(None)), vec![0..2]);
        // Nothing after it, or nothing but whitespace: nothing to hide.
        assert!(spans(b"{", &marks(None)).is_empty());
        assert!(spans(b" \n", &marks(None)).is_empty());
        assert!(spans(b"", &marks(None)).is_empty());
        assert_eq!(whole(b"[1"), vec![1..2]);
    }

    #[test]
    fn scalars_at_the_top_and_whitespace_walk() {
        assert!(spans(b" \t\r\n\"x\" ", &marks(None)).is_empty());
        assert!(spans(b"[ ]", &marks(None)).is_empty());
        assert!(spans(b"{ }", &marks(None)).is_empty());
        assert!(spans(b"-0.5E+3", &marks(None)).is_empty());
        assert!(spans(br#"["\/\b\f\r\t\\"]"#, &marks(None)).is_empty());
    }

    /// The walker's own refusals, for input serde_json would never let through to it.
    #[test]
    fn the_walker_refuses_what_json_does_not_allow() {
        let refuses = |input: &str| {
            let mut walk = Walk {
                text: input,
                bytes: input.as_bytes(),
                at: 0,
                marks: &marks(None),
                spans: Vec::new(),
            };
            walk.document().is_none()
        };
        for input in [
            "",
            "{",
            "[",
            "[1",
            "[1 2]",
            "{\"a\"}",
            "{\"a\":1",
            "{\"a\":1 \"b\":2}",
            "{1:2}",
            "\"a",
            "\"\u{1}\"",
            "\"\\x\"",
            "\"\\u12\"",
            "\"\\u12g4\"",
            "\"\\ud800\"",
            "\"\\ud800\\u0041\"",
            "\"\\ud800\\n\"",
            "\"\\udc00\"",
            "-",
            "01",
            "1.",
            "1e",
            "1e+",
            "tru",
            "nul",
            "fals",
            "trux",
            "nulL",
            "falsy",
            "1 2",
            "+1",
        ] {
            assert!(refuses(input), "{input:?}");
        }
        let deep = format!("{}{}", "[".repeat(MAX_DEPTH + 1), "]".repeat(MAX_DEPTH + 1));
        assert!(refuses(&deep));
        let deep = "{\"a\":".repeat(MAX_DEPTH + 1) + "1" + &"}".repeat(MAX_DEPTH + 1);
        assert!(refuses(&deep));
        let ok = format!("{}{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(!refuses(&ok));
        assert_eq!(hex4(b"00\xff0", 0), None);
    }

    /// Varies the frames this module is for, byte by byte, and holds the walker to serde_json:
    /// whatever serde_json parses is walked, never redacted whole, and blanked still parses,
    /// equal to the input but for the redacted values; nothing panics.
    #[test]
    fn mutated_frames_walk_exactly_as_serde_json_parses_them() {
        let seeds = [
            format!(
                r#"{{"order_response":{{"order_id":"o-1","user_id":"u-1","price":200.5,"trade_id":null,"m":"{PUBLIC}"}}}}"#
            ),
            r#"{"all_orders_response":{"orders":[{"user_id":"u-2","quantity":1e-3}],"twaps":[]}}"#
                .to_owned(),
            r#"{"err":{"error_code":"PermissionDenied","incoming_message":{"type":"auth","params":{"hmac":{"public_key":"k","signature":"s"}}}}}"#
                .to_owned(),
            r#"[{"id":"\u0041\ud83d\ude00"}, -0, true, false, "\"user_id\""]"#.to_owned(),
        ];
        let alphabet = b"{}[]\":,\\u0123456789abcdefE.-+ \ttrnl_x";
        // A fixed linear congruential sequence: the same cases on every run.
        let mut state: u64 = 0x5eed_77aa;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        let mut parsed = 0;
        for round in 0..20_000 {
            let mut bytes = seeds[round % seeds.len()].clone().into_bytes();
            for _ in 0..1 + next() % 3 {
                let at = next() % bytes.len();
                // Never removes the last byte, so the input is never empty.
                let edit = next() % if bytes.len() > 1 { 3 } else { 2 };
                match edit {
                    0 => bytes[at] = alphabet[next() % alphabet.len()],
                    1 => bytes.insert(at, alphabet[next() % alphabet.len()]),
                    _ => {
                        bytes.remove(at);
                    }
                }
            }
            let got = spans(&bytes, &marks(Some(SUB)));
            fbc_core::check_redactions(&bytes, &got).unwrap();
            // ASCII seeds edited with ASCII stay UTF-8.
            let text = core::str::from_utf8(&bytes).unwrap();
            let Ok(live) = serde_json::from_str::<Value>(text) else {
                continue;
            };
            parsed += 1;
            let mut walk = Walk {
                text,
                bytes: &bytes,
                at: 0,
                marks: &marks(Some(SUB)),
                spans: Vec::new(),
            };
            assert!(walk.document().is_some(), "{text}");
            let back = blanked(text, &got);
            let replayed: Value = serde_json::from_str(&back).expect(&back);
            same_but_redacted(&live, &replayed, text);
        }
        assert!(parsed > 1_000, "only {parsed} mutated frames parsed");
    }

    /// `replayed` has the shape of `live`, every value equal but a redacted string (of `2`s)
    /// or number (of `2`s, which a long run reads as a float), and every member there but one
    /// whose name was redacted (to `2`s).
    fn same_but_redacted(live: &Value, replayed: &Value, input: &str) {
        let blank = |s: &str| s.bytes().all(|b| b == b'2');
        match (live, replayed) {
            (Value::Object(a), Value::Object(b)) => {
                let lost = a.keys().filter(|name| !b.contains_key(*name)).count();
                assert!(
                    lost == 0 || b.keys().any(|k| blank(k)),
                    "{input}: {lost} lost"
                );
                for (name, va) in a {
                    if let Some(vb) = b.get(name) {
                        same_but_redacted(va, vb, input);
                    }
                }
            }
            (Value::Array(a), Value::Array(b)) => {
                assert_eq!(a.len(), b.len(), "{input}");
                for (va, vb) in a.iter().zip(b) {
                    same_but_redacted(va, vb, input);
                }
            }
            (Value::String(a), Value::String(b)) => {
                assert!(a == b || blank(b), "{input}: {a} -> {b}");
            }
            (Value::Number(a), Value::Number(b)) => {
                let run = b.as_f64().is_some_and(|n| n >= 2.0);
                assert!(
                    a == b || blank(&b.to_string()) || run,
                    "{input}: {a} -> {b}"
                );
            }
            (a, b) => assert_eq!(a, b, "{input}"),
        }
    }
}
