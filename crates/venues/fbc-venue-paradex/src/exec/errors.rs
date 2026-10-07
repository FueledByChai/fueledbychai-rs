//! Paradex's refusals and the [`RejectKind`] each maps to, keyed by the venue's code (design §6
//! step 9; decision 0069), never by its message text.
//!
//! The codes are the ones docs.paradex.trade documents for the order socket:
//!
//! - The WebSocket "Error Handling" page (`ws/general-information/error-handling`): the
//!   JSON-RPC codes -32700 (parse error), -32600 (invalid request), -32601 (method not found),
//!   -32602 (invalid parameters) and -32603 (internal error), and Paradex's own 100 (method
//!   error), 40110 and 40111 (the session token malformed or invalid) and 40112 (geo IP
//!   blocked).
//! - Paradex's WebSocket OpenRPC specification (`fern/apis/prod_ws/openrpc.json` in
//!   github.com/tradeparadex/paradex-docs, as of commit 5dac8d3, release 1.166.0), which lists
//!   for every order method, beside the codes above, 40300 (permission denied, authentication
//!   required) and 42901 (rate limit exceeded).
//! - `order.cancel_batch`'s page (`ws/web-socket-channels/order-cancel-batch`): the per-order
//!   statuses ALREADY_CLOSED and NOT_FOUND, beside QUEUED_FOR_CANCELLATION, which accepts.
//!
//! None of these codes names a reason finer than the request's own failure, so each is
//! [`RejectKind::Other`] but -32601, a method the venue does not offer
//! ([`RejectKind::Unsupported`]), and 42901, a request refused for its rate
//! ([`RejectKind::RateLimited`], with no retry time, since the venue states none). 40300 is
//! `Other`: the connection is not authenticated, which no retry of the request mends. -32603 is not in the table: an internal error does not say the
//! venue left the request undone, so it is `Unknown`, as is every code no page documents
//! (0069). ALREADY_CLOSED and NOT_FOUND refuse a cancel of an order and leave it as it was:
//! they are [`RejectKind::AlreadyTerminal`] (Paradex does not say which terminal state) and
//! [`RejectKind::NotFound`], neither of which can end an order (0014 item 6).

use fbc_core::{RejectKind, TerminalHint};

/// Each code Paradex documents as refusing a request, with the kind it maps to (module
/// documentation). A code missing here does not say the request was refused.
pub const REJECT_CODES: [(&str, RejectKind); 12] = [
    ("-32700", RejectKind::Other),
    ("-32600", RejectKind::Other),
    ("-32601", RejectKind::Unsupported),
    ("-32602", RejectKind::Other),
    ("100", RejectKind::Other),
    ("40110", RejectKind::Other),
    ("40111", RejectKind::Other),
    ("40112", RejectKind::Other),
    ("40300", RejectKind::Other),
    ("42901", RejectKind::RateLimited { retry_after: None }),
    (
        "ALREADY_CLOSED",
        RejectKind::AlreadyTerminal(TerminalHint::Unspecified),
    ),
    ("NOT_FOUND", RejectKind::NotFound),
];

/// The kind `code` maps to, or `None` for a code that does not say the request was refused.
pub(super) fn reject_kind(code: &str) -> Option<RejectKind> {
    REJECT_CODES
        .iter()
        .find_map(|&(known, kind)| (known == code).then_some(kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_internal_error_and_an_undocumented_code_map_to_no_refusal() {
        assert_eq!(reject_kind("-32603"), None);
        assert_eq!(reject_kind("4290"), None);
        assert_eq!(reject_kind("-32601"), Some(RejectKind::Unsupported));
        assert_eq!(reject_kind("NOT_FOUND"), Some(RejectKind::NotFound));
    }
}
