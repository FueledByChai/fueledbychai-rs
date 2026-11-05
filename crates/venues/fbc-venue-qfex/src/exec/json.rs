//! A JSON reader that keeps where each value sits and each number's text as written.
//!
//! The order-entry codec needs three things serde_json's `Value` does not give: the byte range
//! of every value, so its redaction can name a span inside a string's quotes or over a number
//! (decision 0134 item 4); every member as written, so a repeated member name is seen rather
//! than overwritten (0134 item 2); and a number's own text, so a private quantity is read
//! exactly rather than through `f64` (FBC-zeb0 (c)). [`parse`] gives all three. It reads only
//! what serde_json parses as a `Value`: it asks serde_json first, so the two never disagree on
//! what is JSON, and serde_json's nesting limit bounds the recursion here.

use core::ops::Range;
use std::borrow::Cow;

use serde_json::Value;

/// One value, and the bytes it was written in: a string's range includes its quotes.
#[derive(Debug)]
pub(crate) struct Node<'a> {
    pub(crate) at: Range<usize>,
    pub(crate) kind: Kind<'a>,
}

/// A member of an object: its name with escapes decoded, and its value.
pub(crate) type Member<'a> = (Cow<'a, str>, Node<'a>);

#[derive(Debug)]
pub(crate) enum Kind<'a> {
    /// Every member as written, in order, repeats included.
    Object(Vec<Member<'a>>),
    Array(Vec<Node<'a>>),
    /// The text with escapes decoded.
    Str(Cow<'a, str>),
    /// The number as written.
    Num(&'a str),
    /// `true`, `false` or `null`, as written.
    Literal(&'a str),
}

impl<'a> Node<'a> {
    /// Its members, when it is an object.
    pub(crate) fn members(&self) -> Option<&[Member<'a>]> {
        match &self.kind {
            Kind::Object(members) => Some(members),
            _ => None,
        }
    }

    /// Its items, when it is an array.
    pub(crate) fn items(&self) -> Option<&[Node<'a>]> {
        match &self.kind {
            Kind::Array(items) => Some(items),
            _ => None,
        }
    }

    /// Its text, when it is a string.
    pub(crate) fn str(&self) -> Option<&str> {
        match &self.kind {
            Kind::Str(text) => Some(text),
            _ => None,
        }
    }

    /// Its text, when it is a string written with no escape: exactly the bytes between its
    /// quotes. What the journal keeps in clear is judged on these bytes (decision 0134 item 3).
    pub(crate) fn plain_str(&self) -> Option<&str> {
        match &self.kind {
            Kind::Str(Cow::Borrowed(text)) => Some(text),
            _ => None,
        }
    }

    /// Whether an object anywhere in it names a member twice.
    pub(crate) fn repeats(&self) -> bool {
        match &self.kind {
            Kind::Object(members) => members.iter().enumerate().any(|(at, (name, value))| {
                members[..at].iter().any(|(seen, _)| seen == name) || value.repeats()
            }),
            Kind::Array(items) => items.iter().any(Node::repeats),
            _ => false,
        }
    }

    /// Where it would be blanked as a scalar: a string's characters between its quotes (none
    /// for an empty string), any other scalar whole; `None` for an object or an array.
    pub(crate) fn scalar_span(&self) -> Option<Range<usize>> {
        match self.kind {
            Kind::Object(_) | Kind::Array(_) => None,
            Kind::Str(_) => Some(self.at.start + 1..self.at.end - 1),
            Kind::Num(_) | Kind::Literal(_) => Some(self.at.clone()),
        }
    }
}

/// The first member named `name` in `members`.
pub(crate) fn get<'n, 'a>(members: &'n [Member<'a>], name: &str) -> Option<&'n Node<'a>> {
    members
        .iter()
        .find(|(member, _)| member == name)
        .map(|(_, value)| value)
}

/// `text` as one JSON value with nothing but whitespace around it, or `None` when serde_json
/// does not parse it as a `Value`.
pub(crate) fn parse(text: &str) -> Option<Node<'_>> {
    serde_json::from_str::<Value>(text).ok()?;
    let mut reader = Reader {
        text,
        bytes: text.as_bytes(),
        at: 0,
    };
    let node = reader.value()?;
    reader.whitespace();
    (reader.at == text.len()).then_some(node)
}

/// `bytes` with every span's bytes replaced by the ASCII digit `2`, as the journal reads a
/// redacted record back (decision 0028 item 5).
pub(crate) fn blank(bytes: &[u8], spans: &[Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        out[span.start as usize..span.end as usize].fill(b'2');
    }
    out
}

/// One pass over text serde_json has already parsed, so every step here succeeds on it; each
/// `None` is only the reader refusing to go on.
struct Reader<'a> {
    text: &'a str,
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn value(&mut self) -> Option<Node<'a>> {
        self.whitespace();
        let start = self.at;
        let kind = match *self.bytes.get(self.at)? {
            b'{' => Kind::Object(self.object()?),
            b'[' => Kind::Array(self.array()?),
            b'"' => Kind::Str(self.string()?),
            b't' | b'f' | b'n' => {
                while self.bytes.get(self.at).is_some_and(u8::is_ascii_lowercase) {
                    self.at += 1;
                }
                Kind::Literal(&self.text[start..self.at])
            }
            _ => {
                let number = |b: &u8| b.is_ascii_digit() || b"+-.eE".contains(b);
                while self.bytes.get(self.at).is_some_and(number) {
                    self.at += 1;
                }
                Kind::Num(&self.text[start..self.at])
            }
        };
        Some(Node {
            at: start..self.at,
            kind,
        })
    }

    fn object(&mut self) -> Option<Vec<Member<'a>>> {
        let mut members = Vec::new();
        self.at += 1;
        self.whitespace();
        if self.eat(b'}') {
            return Some(members);
        }
        loop {
            self.whitespace();
            let name = self.string()?;
            self.whitespace();
            self.eat(b':').then_some(())?;
            members.push((name, self.value()?));
            self.whitespace();
            if !self.eat(b',') {
                return self.eat(b'}').then_some(members);
            }
        }
    }

    fn array(&mut self) -> Option<Vec<Node<'a>>> {
        let mut items = Vec::new();
        self.at += 1;
        self.whitespace();
        if self.eat(b']') {
            return Some(items);
        }
        loop {
            items.push(self.value()?);
            self.whitespace();
            if !self.eat(b',') {
                return self.eat(b']').then_some(items);
            }
        }
    }

    /// The string at the `"` under the cursor, the cursor left past its closing quote. A
    /// string with an escape is decoded by serde_json itself.
    fn string(&mut self) -> Option<Cow<'a, str>> {
        let start = self.at;
        (self.bytes.get(start) == Some(&b'"')).then_some(())?;
        let mut at = start + 1;
        let mut escaped = false;
        loop {
            match *self.bytes.get(at)? {
                b'"' => break,
                b'\\' => {
                    escaped = true;
                    at += 2;
                }
                _ => at += 1,
            }
        }
        self.at = at + 1;
        let raw = &self.text[start..self.at];
        if escaped {
            serde_json::from_str::<String>(raw).ok().map(Cow::Owned)
        } else {
            Some(Cow::Borrowed(&raw[1..raw.len() - 1]))
        }
    }

    fn whitespace(&mut self) {
        while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> bool {
        let here = self.bytes.get(self.at) == Some(&byte);
        if here {
            self.at += 1;
        }
        here
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_keep_their_bytes_their_text_and_every_member() {
        let text =
            r#" {"a" : [1.50, -2e3, "x\"y", true, null, {}], "a": "", "b\u0063": {"d":false}} "#;
        let node = parse(text).unwrap();
        let members = node.members().unwrap();
        assert_eq!(members.len(), 3);
        assert!(node.repeats());
        let items = members[0].1.items().unwrap();
        assert!(matches!(items[0].kind, Kind::Num("1.50")));
        assert!(members[1].1.items().is_none());
        assert_eq!(&text[items[1].at.clone()], "-2e3");
        assert_eq!(items[2].str(), Some("x\"y"));
        assert_eq!(&text[items[2].scalar_span().unwrap()], r#"x\"y"#);
        // A string written with an escape has no plain text; one without, its bytes.
        assert_eq!(items[2].plain_str(), None);
        assert_eq!(members[1].1.plain_str(), Some(""));
        assert_eq!(items[0].plain_str(), None);
        assert_eq!(&text[items[3].scalar_span().unwrap()], "true");
        assert_eq!(items[5].scalar_span(), None);
        assert_eq!(members[1].1.scalar_span().unwrap().len(), 0);
        assert_eq!(members[2].0, "bc");
        assert!(get(members, "bc").unwrap().members().is_some());
        assert!(get(members, "z").is_none());
        assert_eq!(get(members, "a").unwrap().str(), None);
        assert!(!parse("[{\"a\":1},{\"a\":1}]").unwrap().repeats());
        assert!(parse("[{\"a\":1,\"a\":1}]").unwrap().repeats());
    }

    #[test]
    fn only_what_serde_json_parses_is_read() {
        for text in [
            "",
            "{",
            "[1,]",
            "{\"a\":1e400}",
            "\"\\ud800\"",
            "nul",
            "1 2",
            "{\"a\" 1}",
        ] {
            assert!(parse(text).is_none(), "{text:?}");
        }
        assert!(parse("[]").unwrap().members().is_none());
        assert_eq!(blank(b"abcd", std::slice::from_ref(&(1..3))), b"a22d");
    }

    /// The reader's own refusals, which serde_json never lets reach it.
    #[test]
    fn the_reader_refuses_what_is_not_json() {
        let read = |text: &str| {
            let mut reader = Reader {
                text,
                bytes: text.as_bytes(),
                at: 0,
            };
            reader.value().map(|_| reader.at)
        };
        for text in [
            "", "{\"a\":1", "{1:2}", "{\"a\"1}", "[1", "[1 2]", "\"abc", "{\"a", "{\"a\":",
        ] {
            assert_eq!(read(text), None, "{text:?}");
        }
        assert_eq!(read("1 2"), Some(1));
    }
}
