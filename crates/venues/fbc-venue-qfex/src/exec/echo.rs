//! QFEX's err and ack envelopes: which bytes of one that echoes a request the journal keeps in
//! clear (decision 0134 items 1 to 4), and what the codec reads of an envelope from the bytes
//! kept (item 5).
//!
//! The three envelopes (item 1), from QFEX's [rate page](https://docs.qfex.com/websocket/rate),
//! [errors page](https://docs.qfex.com/websocket/errors) and
//! [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml):
//!
//! - the flat err, `{"type":"Err","error_code":…,"message":…,"incoming_message":…}`;
//! - the nested err, `{"err":{"error_code":…,"message":…,"incoming_message":…}}`;
//! - the ack, `{"ack":{"incoming_message":…}}`.
//!
//! A frame that can echo a request ([`crate::rate::can_echo`]) is redacted by [`spans`]: kept
//! as its shape, a documented `error_code`, an echo `type` the codec maps to a bucket and a
//! RateLimited err's `message` in one of the two hint forms, every other value a span of its
//! own; or one span, whole, when it is malformed, holds a backslash, repeats a member name,
//! echoes authentication, or would not blank to the same shape.

use core::ops::Range;

use super::json::{self, Member, Node};
use crate::rate;

/// QFEX's documented error codes: the errors page's table and the AsyncAPI's `ErrResponse`
/// enum list the same ten, read 2026-11-04 (0134 item 3). A code QFEX adds is undocumented
/// until it is listed here.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ErrCode {
    RateLimited,
    InvalidJsonFormat,
    AlreadyAuthenticated,
    InvalidParameter,
    PermissionDenied,
    ServerError,
    InvalidOrder,
    InvalidOrderId,
    KycRequired,
    TncRequired,
}

impl ErrCode {
    /// Every documented code, in the errors page's order.
    pub const ALL: [ErrCode; 10] = [
        ErrCode::RateLimited,
        ErrCode::InvalidJsonFormat,
        ErrCode::AlreadyAuthenticated,
        ErrCode::InvalidParameter,
        ErrCode::PermissionDenied,
        ErrCode::ServerError,
        ErrCode::InvalidOrder,
        ErrCode::InvalidOrderId,
        ErrCode::KycRequired,
        ErrCode::TncRequired,
    ];

    /// The code as QFEX writes it in `error_code`.
    pub fn wire(self) -> &'static str {
        match self {
            ErrCode::RateLimited => "RateLimited",
            ErrCode::InvalidJsonFormat => "InvalidJSONFormat",
            ErrCode::AlreadyAuthenticated => "AlreadyAuthenticated",
            ErrCode::InvalidParameter => "InvalidParameter",
            ErrCode::PermissionDenied => "PermissionDenied",
            ErrCode::ServerError => "ServerError",
            ErrCode::InvalidOrder => "InvalidOrder",
            ErrCode::InvalidOrderId => "InvalidOrderId",
            ErrCode::KycRequired => "KycRequired",
            ErrCode::TncRequired => "TncRequired",
        }
    }

    /// The documented code written exactly `wire`.
    pub fn from_wire(wire: &str) -> Option<ErrCode> {
        ErrCode::ALL.into_iter().find(|code| code.wire() == wire)
    }

    /// Whether, once the stream is authenticated, an err with this code says the session's own
    /// standing is wrong rather than one request, and so ends the epoch (0134 item 5):
    /// `AlreadyAuthenticated` (the venue's view of the connection is not the codec's),
    /// `PermissionDenied` (the key or user may not act, a cancel perhaps included), and
    /// `KycRequired` and `TncRequired` (the account cannot act until its user does something
    /// outside the session). `RateLimited` holds a budget instead, and the other five are a
    /// request's err: counted, settling nothing.
    pub fn ends_session(self) -> bool {
        matches!(
            self,
            ErrCode::AlreadyAuthenticated
                | ErrCode::PermissionDenied
                | ErrCode::KycRequired
                | ErrCode::TncRequired
        )
    }

    pub(crate) fn index(self) -> usize {
        self as usize
    }
}

/// Which envelope a top-level object is (0134 item 1), and the members of its body: the
/// top-level object's own for the flat err, the `err` or `ack` object's for the other two.
pub(crate) enum Envelope<'n, 'a> {
    FlatErr(&'n [Member<'a>]),
    NestedErr(&'n [Member<'a>]),
    Ack(&'n [Member<'a>]),
    /// None of the three.
    Other,
    /// More than one.
    Ambiguous,
}

pub(crate) fn envelope<'n, 'a>(top: &'n [Member<'a>]) -> Envelope<'n, 'a> {
    let flat = json::get(top, "type").and_then(Node::str) == Some("Err");
    let nested = json::get(top, "err").and_then(Node::members);
    let ack = json::get(top, "ack").and_then(Node::members);
    match (flat, nested, ack) {
        (true, None, None) => Envelope::FlatErr(top),
        (false, Some(body), None) => Envelope::NestedErr(body),
        (false, None, Some(body)) => Envelope::Ack(body),
        (false, None, None) => Envelope::Other,
        _ => Envelope::Ambiguous,
    }
}

/// What the codec reads of an err, from the bytes kept in clear: a spanned value reads as
/// absent, or as an undocumented code (0134 item 5).
#[derive(Copy, Clone, Debug)]
pub(crate) struct Err<'n> {
    /// `None` for a code that is absent, not a string, undocumented or spanned.
    pub(crate) code: Option<ErrCode>,
    pub(crate) message: Option<&'n str>,
    /// The echo's `type`, when the echo is an object with a string `type`.
    pub(crate) echo_type: Option<&'n str>,
}

pub(crate) fn err<'n>(body: &'n [Member<'_>]) -> Err<'n> {
    let text = |name: &str| json::get(body, name).and_then(Node::str);
    Err {
        code: text("error_code").and_then(ErrCode::from_wire),
        message: text("message"),
        echo_type: json::get(body, "incoming_message")
            .and_then(Node::members)
            .and_then(|echo| json::get(echo, "type"))
            .and_then(Node::str),
    }
}

/// The journal's spans of `text`, a frame that can echo a request (0134 items 2 to 4), given the
/// spans `account` the trade socket's authentication names in it (in `src/auth`):
/// the kept spans, or one span over the whole frame when item 2 keeps it whole. It is kept
/// whole too when an account span falls outside the kept spans, so no account material is ever
/// left in clear, and when the blanked frame would not be kept the same way: replay then meets
/// the frame the codec read live, and redacts it to the same bytes again.
pub(crate) fn spans(text: &str, account: &[Range<u32>]) -> Vec<Range<u32>> {
    spans_by(text, account, kept)
}

/// The journal's spans of `text`, a frame that can echo a request, on an epoch whose arm the
/// codec encoded (decision 0132 items 1 and 3, as 0134 item 7 narrows them), with the same
/// account and replay checks as [`spans`]:
///
/// - a frame whose only content is an err or ack envelope echoing the arm ([`arm_kept`]) is
///   kept in clear but for its `message`, which is kept too only for a RateLimited err in one
///   of the two hint forms; a member added to the body or beside the envelope makes it any
///   other frame;
/// - while the arm is the connection's only request in flight (`only`), any other frame keeps
///   its shape whatever its envelope, with what 0134 item 3 keeps in clear in an err or an ack,
///   every other value a span of its own ([`window_kept`]); an echo of authentication, and a
///   frame that is not JSON, stay whole;
/// - a value either rule keeps in clear for what it says, a code, an echo type or a hint, is
///   judged on the bytes between its quotes, so an escaped one is a span ([`plain_code`]);
/// - otherwise it is redacted as [`spans`] redacts it (0126, 0134).
pub(crate) fn armed_spans(text: &str, account: &[Range<u32>], only: bool) -> Vec<Range<u32>> {
    spans_by(text, account, |text| {
        arm_kept(text)
            .or_else(|| only.then(|| window_kept(text)).flatten())
            .or_else(|| (!only).then(|| kept(text)).flatten())
    })
}

/// The spans `rule` gives `text`, or one span over the whole frame when it gives none, when an
/// account span falls outside them, or when the frame blanked by them is not given the same.
fn spans_by(
    text: &str,
    account: &[Range<u32>],
    rule: impl Fn(&str) -> Option<Vec<Range<usize>>>,
) -> Vec<Range<u32>> {
    let end = u32::try_from(text.len()).unwrap_or(u32::MAX);
    let whole = (end > 0).then_some(0..end).into_iter().collect();
    if text.len() > u32::MAX as usize {
        return whole;
    }
    let Some(live) = rule(text) else {
        return whole;
    };
    let live: Vec<Range<u32>> = live
        .into_iter()
        .map(|span| span.start as u32..span.end as u32)
        .collect();
    let covered = |span: &Range<u32>| {
        live.iter()
            .any(|kept| kept.start <= span.start && span.end <= kept.end)
    };
    let blanked = String::from_utf8(json::blank(text.as_bytes(), &live)).ok();
    let again = blanked.as_deref().and_then(rule);
    let same = again.is_some_and(|again| {
        again.len() == live.len()
            && again
                .iter()
                .zip(&live)
                .all(|(a, b)| a.start as u32 == b.start && a.end as u32 == b.end)
    });
    if !same || !account.iter().all(covered) {
        return whole;
    }
    live
}

/// The spans of a frame item 3 keeps (byte offsets, in order), or `None` when item 2 keeps it
/// whole.
fn kept(text: &str) -> Option<Vec<Range<usize>>> {
    if text.len() > u32::MAX as usize || text.contains('\\') {
        return None;
    }
    let root = json::parse(text)?;
    let top = root.members()?;
    if root.repeats() {
        return None;
    }
    let envelope = envelope(top);
    let body = match envelope {
        Envelope::FlatErr(body) | Envelope::NestedErr(body) | Envelope::Ack(body) => body,
        Envelope::Other | Envelope::Ambiguous => return None,
    };
    let echo_type = json::get(body, "incoming_message")
        .and_then(Node::members)
        .and_then(|echo| json::get(echo, "type"))
        .and_then(Node::str);
    if echo_type == Some("auth") {
        return None;
    }
    let mut spans = Vec::new();
    envelope_spans(top, &envelope, &mut spans);
    Some(spans)
}

/// What 0134 item 3 keeps of a frame in one of the three envelopes, every other value a span
/// of its own (item 4): the flat err's `"Err"`, an err's documented code, the echo's mapped
/// `type` and a RateLimited err's hint, inside the envelope's body; nothing outside it.
fn envelope_spans(top: &[Member<'_>], envelope: &Envelope<'_, '_>, spans: &mut Vec<Range<usize>>) {
    // Where the body sits: the top-level object itself, or the member named here.
    let (body, at, is_err) = match *envelope {
        Envelope::FlatErr(body) => (body, None, true),
        Envelope::NestedErr(body) => (body, Some("err"), true),
        Envelope::Ack(body) => (body, Some("ack"), false),
        Envelope::Other | Envelope::Ambiguous => return,
    };
    let body_rule = Body {
        is_err,
        rate_limited: is_err && plain_code(body) == Some(ErrCode::RateLimited),
    };
    for (name, value) in top {
        if at.is_none() && name == "type" && value.str() == Some("Err") {
            // The flat err's `type` is "Err", which made it the flat err.
        } else if at.is_none() {
            body_rule.member(name, value, spans);
        } else if at == Some(name.as_ref()) && value.members().is_some() {
            for (name, value) in value.members().unwrap_or_default() {
                body_rule.member(name, value, spans);
            }
        } else {
            scalars(value, spans);
        }
    }
}

/// The arm the codec encodes (decision 0132 item 1): the cancel-on-disconnect page's request
/// and the AsyncAPI's `CancelOnDisconnectCommand`, turning the protection on.
pub const ARM: &str = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;

/// The members each envelope's body documents (0134 item 1), the flat err's `type` included.
const ERR_MEMBERS: [&str; 4] = ["type", "error_code", "message", "incoming_message"];

/// The spans of a frame whose only content is an err or ack envelope whose echo equals the arm
/// ([`same`]), or `None` for any other frame: nothing in clear but its `message`, unless the
/// frame is a RateLimited err whose message is exactly one of the two hint forms (0132 item 3,
/// 0134 item 7). The arm carries no credential, account identifier or order.
fn arm_kept(text: &str) -> Option<Vec<Range<usize>>> {
    let root = json::parse(text)?;
    let top = root.members()?;
    if root.repeats() {
        return None;
    }
    // The body's documented members: the flat err's four, the nested err's three, the ack's one.
    let (body, documented, is_err): (_, &[&str], _) = match envelope(top) {
        Envelope::FlatErr(body) => (body, &ERR_MEMBERS, true),
        Envelope::NestedErr(body) if top.len() == 1 => (body, &ERR_MEMBERS[1..], true),
        Envelope::Ack(body) if top.len() == 1 => (body, &ERR_MEMBERS[3..], false),
        _ => return None,
    };
    let documented = |name: &str| documented.contains(&name);
    if !body.iter().all(|(name, _)| documented(name)) {
        return None;
    }
    let arm = json::parse(ARM).expect("the arm is JSON");
    if !json::get(body, "incoming_message").is_some_and(|echo| same(echo, &arm)) {
        return None;
    }
    let rate_limited = is_err && plain_code(body) == Some(ErrCode::RateLimited);
    let mut spans = Vec::new();
    for (name, value) in body {
        let hint = rate_limited && value.plain_str().is_some_and(rate::is_hint_form);
        if name == "message" && !hint {
            scalars(value, &mut spans);
        }
    }
    Some(spans)
}

/// Whether two values are the same JSON: objects with the same members and values, in any
/// order and none repeated, arrays item by item, strings by their text, and numbers and
/// literals as written. A member added is not the same.
fn same(a: &Node<'_>, b: &Node<'_>) -> bool {
    use json::Kind;
    match (&a.kind, &b.kind) {
        (Kind::Object(x), Kind::Object(y)) => {
            x.len() == y.len()
                && !a.repeats()
                && !b.repeats()
                && x.iter()
                    .all(|(name, v)| json::get(y, name).is_some_and(|w| same(v, w)))
        }
        (Kind::Array(x), Kind::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| same(v, w))
        }
        (Kind::Str(x), Kind::Str(y)) => x == y,
        (Kind::Num(x), Kind::Num(y)) | (Kind::Literal(x), Kind::Literal(y)) => x == y,
        _ => false,
    }
}

/// The spans of a frame that arrives while the arm is the connection's only request in flight
/// and is not the arm's own echo (decision 0132 item 3, 0134 item 7), or `None` when it stays
/// whole: an echo of authentication anywhere in it, or bytes that are not JSON. Its envelope,
/// member names and nesting are kept whatever its envelope, nested, flat or none, an escape or
/// a repeated member included; in an err or an ack what 0134 item 3 keeps stays in clear, and
/// every other scalar is a span of its own.
fn window_kept(text: &str) -> Option<Vec<Range<usize>>> {
    let root = json::parse(text)?;
    if echoes_auth(&root) {
        return None;
    }
    let mut spans = Vec::new();
    match root.members().map(envelope) {
        Some(envelope @ (Envelope::FlatErr(_) | Envelope::NestedErr(_) | Envelope::Ack(_))) => {
            envelope_spans(root.members().unwrap_or_default(), &envelope, &mut spans);
        }
        _ => scalars(&root, &mut spans),
    }
    Some(spans)
}

/// Whether any `incoming_message` in `node`, at any depth, is an object naming `auth` as one of
/// its `type` members.
fn echoes_auth(node: &Node<'_>) -> bool {
    match &node.kind {
        json::Kind::Object(members) => members.iter().any(|(name, value)| {
            let auth = name == "incoming_message"
                && value.members().is_some_and(|echo| {
                    echo.iter()
                        .any(|(name, v)| name == "type" && v.str() == Some("auth"))
                });
            auth || echoes_auth(value)
        }),
        json::Kind::Array(items) => items.iter().any(echoes_auth),
        _ => false,
    }
}

/// An err's code when item 3 keeps it in clear: documented, and written with no escape.
///
/// Every value kept in clear is judged on exactly the bytes between its quotes (0134 item 3),
/// [`Node::plain_str`]. Within the arm's window a frame with an escape keeps its shape (0132
/// item 3, 0134 item 7), but an escaped code, echo type or hint is never one item 3 keeps: it
/// is a span, and an escaped `RateLimited` makes no RateLimited err.
fn plain_code(body: &[Member<'_>]) -> Option<ErrCode> {
    json::get(body, "error_code")
        .and_then(Node::plain_str)
        .and_then(ErrCode::from_wire)
}

/// What an envelope's body keeps of each of its members.
struct Body {
    is_err: bool,
    rate_limited: bool,
}

impl Body {
    fn member(&self, name: &str, value: &Node<'_>, spans: &mut Vec<Range<usize>>) {
        // Judged on the bytes between the quotes ([`plain_code`]).
        let documented = || value.plain_str().and_then(ErrCode::from_wire).is_some();
        let hint = || value.plain_str().is_some_and(rate::is_hint_form);
        match name {
            "error_code" if self.is_err && documented() => {}
            "message" if self.rate_limited && hint() => {}
            "incoming_message" => match value.members() {
                Some(members) => {
                    for (name, value) in members {
                        let mapped = value.plain_str().and_then(rate::operation).is_some();
                        if name != "type" || !mapped {
                            scalars(value, spans);
                        }
                    }
                }
                None => scalars(value, spans),
            },
            _ => scalars(value, spans),
        }
    }
}

/// Each scalar in `value` as a span of its own (0134 item 4): a string's characters between its
/// quotes, any other scalar whole; an object's member names and an array's nesting kept.
fn scalars(value: &Node<'_>, spans: &mut Vec<Range<usize>>) {
    match &value.kind {
        json::Kind::Object(members) => {
            for (_, value) in members {
                scalars(value, spans);
            }
        }
        json::Kind::Array(items) => {
            for item in items {
                scalars(item, spans);
            }
        }
        _ => spans.extend(value.scalar_span().filter(|span| !span.is_empty())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blanked(text: &str) -> String {
        String::from_utf8(json::blank(text.as_bytes(), &spans(text, &[]))).unwrap()
    }

    #[test]
    fn codes_read_and_write_as_qfex_lists_them() {
        for code in ErrCode::ALL {
            assert_eq!(ErrCode::from_wire(code.wire()), Some(code));
            assert_eq!(ErrCode::ALL[code.index()], code);
        }
        assert_eq!(ErrCode::from_wire("InvalidJsonFormat"), None);
        let ends: Vec<_> = ErrCode::ALL
            .into_iter()
            .filter(|c| c.ends_session())
            .collect();
        assert_eq!(
            ends,
            [
                ErrCode::AlreadyAuthenticated,
                ErrCode::PermissionDenied,
                ErrCode::KycRequired,
                ErrCode::TncRequired
            ]
        );
    }

    #[test]
    fn each_envelope_keeps_what_item_3_lists_and_spans_every_other_value() {
        let cases = [
            (
                r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 7 seconds","incoming_message":{"type":"add_order","params":{"symbol":"S","price":1.5,"ok":true,"none":null,"list":["a",2]}}}"#,
                r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 7 seconds","incoming_message":{"type":"add_order","params":{"symbol":"2","price":222,"ok":2222,"none":2222,"list":["2",2]}}}"#.to_owned(),
            ),
            // A message in a hint form is kept only for a RateLimited err.
            (
                r#"{"err":{"error_code":"ServerError","message":"Rate limit exceeded, retry after 7 seconds","incoming_message":{"type":"get_order","params":{}}}}"#,
                format!(
                    r#"{{"err":{{"error_code":"ServerError","message":"{}","incoming_message":{{"type":"get_order","params":{{}}}}}}}}"#,
                    "2".repeat(42)
                ),
            ),
            // An undocumented code, a null message, an unmapped type, an undocumented member.
            (
                r#"{"err":{"error_code":"New","message":null,"incoming_message":{"type":"transfer","params":{"x":"y"}},"extra":{"k":[1]}},"seq":12}"#,
                r#"{"err":{"error_code":"222","message":2222,"incoming_message":{"type":"22222222","params":{"x":"2"}},"extra":{"k":[2]}},"seq":22}"#.to_owned(),
            ),
            // A string echo, a scalar echo, an empty string and an empty echo.
            (
                r#"{"err":{"error_code":"InvalidOrder","message":"","incoming_message":"add_order"}}"#,
                r#"{"err":{"error_code":"InvalidOrder","message":"","incoming_message":"222222222"}}"#.to_owned(),
            ),
            (
                r#"{"err":{"error_code":7,"incoming_message":17}}"#,
                r#"{"err":{"error_code":2,"incoming_message":22}}"#.to_owned(),
            ),
            (
                r#"{"ack":{"incoming_message":{}}}"#,
                r#"{"ack":{"incoming_message":{}}}"#.to_owned(),
            ),
            // An ack keeps its mapped echo type, and no code or message of its own.
            (
                r#"{"ack":{"incoming_message":{"type":"cancel_order","params":{"order_id":"o"}},"error_code":"InvalidOrder","message":"Too many requests. Please retry after 5ms."},"type":"Err2"}"#,
                format!(
                    r#"{{"ack":{{"incoming_message":{{"type":"cancel_order","params":{{"order_id":"2"}}}},"error_code":"{}","message":"{}"}},"type":"2222"}}"#,
                    "2".repeat(12),
                    "2".repeat(42)
                ),
            ),
            // The flat err's undocumented members, an `err` that is not an object among them.
            (
                r#"{"type":"Err","err":"x","error_code":"TncRequired","incoming_message":{"type":"Err"}}"#,
                r#"{"type":"Err","err":"2","error_code":"TncRequired","incoming_message":{"type":"222"}}"#.to_owned(),
            ),
        ];
        for (live, want) in cases {
            assert_eq!(blanked(live), want, "{live}");
            // Kept again the same way: replay meets what live read.
            assert_eq!(blanked(&want), want, "{want}");
        }
    }

    #[test]
    fn item_2_keeps_a_frame_whole() {
        for text in [
            r#"{"err":{"error_code":"AlreadyAuthenticated","incoming_message":{"type":"auth","params":{}}}}"#,
            r#"{"type":"Err","error_code":"RateLimited","incoming_message":{"type":"auth"}}"#,
            r#"{"err":{"error_code":"InvalidOrder","message":"a \"b\"","incoming_message":{}}}"#,
            r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"type":"add_order","type":"add_order"}}}"#,
            r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"#,
            r#"[{"err":{"incoming_message":{}}}]"#,
            r#"{"incoming_message":{}}"#,
            r#"{"err":{"incoming_message":{}},"ack":{"incoming_message":{}}}"#,
            r#"{"type":"Err","err":{"incoming_message":{}}}"#,
            "\u{fffd}incoming_message",
        ] {
            assert_eq!(spans(text, &[]), vec![0..text.len() as u32], "{text}");
        }
        assert!(spans("", &[]).is_empty());
        // A number whose run of 2s overflows would not parse blanked.
        let long = format!(
            r#"{{"err":{{"error_code":"InvalidOrder","incoming_message":{{"params":{{"n":0.{}1}}}}}}}}"#,
            "0".repeat(320)
        );
        assert!(json::parse(&long).is_some());
        assert_eq!(spans(&long, &[]), vec![0..long.len() as u32]);
        // An account span the kept spans do not cover: a member name.
        let named =
            r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"params":{"k":1}}}}"#;
        let name = named.find("\"k\"").unwrap() as u32 + 1;
        assert_eq!(
            spans(named, std::slice::from_ref(&(name..name + 1))),
            vec![0..named.len() as u32]
        );
        let value = named.find(":1").unwrap() as u32 + 1;
        let one = value..value + 1;
        assert_eq!(spans(named, std::slice::from_ref(&one)), vec![one]);
    }

    #[test]
    fn the_codec_reads_an_err_from_its_kept_bytes() {
        let read = |text: &str| {
            let root = json::parse(text).unwrap();
            let top = root.members().unwrap();
            match envelope(top) {
                Envelope::FlatErr(body) | Envelope::NestedErr(body) => {
                    let e = err(body);
                    Some((
                        e.code,
                        e.message.map(str::to_owned),
                        e.echo_type.map(str::to_owned),
                    ))
                }
                _ => None,
            }
        };
        assert_eq!(
            read(
                r#"{"err":{"error_code":"KycRequired","message":"m","incoming_message":{"type":"t"}}}"#
            ),
            Some((
                Some(ErrCode::KycRequired),
                Some("m".to_owned()),
                Some("t".to_owned())
            ))
        );
        assert_eq!(
            read(r#"{"type":"Err","error_code":"222","incoming_message":"x"}"#),
            Some((None, None, None))
        );
        assert_eq!(read(r#"{"ack":{}}"#), None);
        assert_eq!(read(r#"{"err":{},"ack":{}}"#), None);
        assert_eq!(read(r#"{"order_response":{}}"#), None);
    }
}
