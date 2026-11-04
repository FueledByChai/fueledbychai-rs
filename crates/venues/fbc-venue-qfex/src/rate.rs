//! What QFEX's trade WebSocket says about its rate budgets, read from the bytes its frames keep
//! in clear (decisions 0126 and 0134).
//!
//! - [`operation`]: the bucket a request type is charged to, from QFEX's
//!   [rate page](https://docs.qfex.com/websocket/rate), whose two buckets per provider user,
//!   general and cancel, [`crate::trade_rate`] declares. An echo's `type` is the only place a
//!   refusal names its bucket.
//! - [`hint`]: the retry hint, only in one of the two documented forms of a `message`, `Rate
//!   limit exceeded, retry after N seconds` (the rate page) and `Too many requests. Please retry
//!   after Nms.` (the [errors page](https://docs.qfex.com/websocket/errors)).
//! - [`can_echo`]: whether a frame can echo a request (0126): it names `incoming_message`, after
//!   JSON escapes are decoded.
//!
//! This is budget feedback only: no refusal is evidence that a sent order did or did not take
//! effect, and nothing here settles or resends a request.

use std::time::Duration;

use fbc_core::OpKind;

/// Each request type the codec maps to an operation, and so to a bucket: the types a journal
/// keeps in clear when an err or an ack echoes one (0134 item 3). `auth` is never one, nor is
/// `cancel_on_disconnect`, whose bucket the rate page does not name, so its refusal holds both.
pub(crate) const MAPPED: [(&str, OpKind); 14] = [
    ("add_order", OpKind::Place),
    ("modify_order", OpKind::Amend),
    ("modify_stop_order", OpKind::Amend),
    ("get_order", OpKind::Query),
    ("get_user_orders", OpKind::Query),
    ("get_user_trades", OpKind::Query),
    ("get_user_leverage", OpKind::Query),
    ("get_available_leverage_levels", OpKind::Query),
    ("subscribe", OpKind::Subscribe),
    ("unsubscribe", OpKind::Subscribe),
    ("set_user_leverage", OpKind::Control),
    ("cancel_order", OpKind::Cancel),
    ("cancel_stop_order", OpKind::Cancel),
    ("cancel_all_orders", OpKind::CancelAll),
];

/// The operation an echoed request type is charged as, exactly as written.
pub(crate) fn operation(kind: &str) -> Option<OpKind> {
    MAPPED
        .iter()
        .find(|(name, _)| *name == kind)
        .map(|(_, op)| *op)
}

/// The two documented forms: the text before N, the text after it, and N's unit.
const FORMS: [(&str, &str, u64); 2] = [
    ("Rate limit exceeded, retry after ", " seconds", 1_000),
    ("Too many requests. Please retry after ", "ms.", 1),
];

/// N and its unit in milliseconds, when `message` is exactly one of the two documented forms
/// with N one or more ASCII digits.
fn form(message: &str) -> Option<(&str, u64)> {
    FORMS.iter().find_map(|(before, after, unit)| {
        let n = message.strip_prefix(before)?.strip_suffix(after)?;
        (!n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())).then_some((n, *unit))
    })
}

/// Whether `message` is exactly one of the two documented hint forms, whatever its N: what the
/// journal keeps in clear of a RateLimited err's `message` (0134 item 3).
pub(crate) fn is_hint_form(message: &str) -> bool {
    form(message).is_some()
}

/// The hold `message` asks for: N seconds or N milliseconds in one of the two forms. An N of
/// zero, or one too large for a `Duration` of milliseconds, is an invalid hint and gives none,
/// so the hold lasts at least the declared window (0126, 0134 item 5).
pub(crate) fn hint(message: &str) -> Option<Duration> {
    let (n, unit) = form(message)?;
    n.parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(unit))
        .map(Duration::from_millis)
}

/// Whether `bytes` can echo a request: they name `incoming_message`, literally or once JSON
/// `\u` escapes of ASCII are decoded, valid JSON or not. A false positive only keeps more of a
/// frame from the journal.
pub(crate) fn can_echo(bytes: &[u8]) -> bool {
    const NEEDLE: &[u8] = b"incoming_message";
    let contains = |haystack: &[u8]| haystack.windows(NEEDLE.len()).any(|part| part == NEEDLE);
    if contains(bytes) {
        return true;
    }
    if !bytes.contains(&b'\\') {
        return false;
    }
    // A conservative scan, not a second JSON parser: a malformed escape is copied unchanged.
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'\\'
            && bytes.get(at + 1) == Some(&b'u')
            && let Some(hex) = bytes.get(at + 2..at + 6)
            && let Ok(hex) = std::str::from_utf8(hex)
            && let Ok(code) = u16::from_str_radix(hex, 16)
            && let Ok(ascii) = u8::try_from(code)
            && ascii.is_ascii()
        {
            decoded.push(ascii);
            at += 6;
        } else {
            decoded.push(bytes[at]);
            at += 1;
        }
    }
    contains(&decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_mapped_type_has_its_bucket_and_nothing_else_does() {
        for (kind, op) in MAPPED {
            assert_eq!(operation(kind), Some(op));
        }
        for kind in [
            "auth",
            "cancel_on_disconnect",
            "transfer_balance",
            "Add_order",
            " add_order",
            "",
        ] {
            assert_eq!(operation(kind), None, "{kind}");
        }
    }

    #[test]
    fn only_the_two_documented_forms_give_a_hint() {
        let secs = Duration::from_secs;
        assert_eq!(
            hint("Rate limit exceeded, retry after 7 seconds"),
            Some(secs(7))
        );
        assert_eq!(
            hint("Rate limit exceeded, retry after 007 seconds"),
            Some(secs(7))
        );
        assert_eq!(
            hint("Too many requests. Please retry after 200ms."),
            Some(Duration::from_millis(200))
        );
        for message in [
            "Rate limit exceeded, retry after x seconds",
            "Rate limit exceeded, retry after  seconds",
            "Rate limit exceeded, retry after 2 second",
            "Rate limit exceeded, retry after -2 seconds",
            "Rate limit exceeded, retry after 2 seconds.",
            "Too many requests. Please retry after 200ms",
            "Too many requests. Please retry after 0.2s.",
            "Too many requests.",
            "",
        ] {
            assert!(!is_hint_form(message), "{message}");
            assert_eq!(hint(message), None, "{message}");
        }
        // Kept in clear, but no hold of its own: zero, and too large for a Duration.
        for message in [
            "Rate limit exceeded, retry after 0 seconds",
            "Too many requests. Please retry after 0ms.",
            "Rate limit exceeded, retry after 18446744073709552 seconds",
            "Too many requests. Please retry after 18446744073709551616ms.",
        ] {
            assert!(is_hint_form(message), "{message}");
            assert_eq!(hint(message), None, "{message}");
        }
    }

    #[test]
    fn an_echo_is_found_literally_or_behind_ascii_escapes() {
        assert!(can_echo(br#"{"incoming_message":{}}"#));
        assert!(can_echo(br#"{"incoming_mess\u0061ge":{}"#));
        assert!(can_echo(
            br#"{"\u0069\u006e\u0063\u006f\u006d\u0069\u006e\u0067\u005f\u006d\u0065\u0073\u0073\u0061\u0067\u0065":1}"#
        ));
        assert!(!can_echo(br#"{"type":"Ok"}"#));
        assert!(!can_echo(br#"{"incoming\u005"#));
        assert!(!can_echo(br#"{"incoming\u00ff_message":1}"#));
        assert!(!can_echo(br#"{"incoming\u0100_message\uzzzz\u":1}"#));
    }
}
