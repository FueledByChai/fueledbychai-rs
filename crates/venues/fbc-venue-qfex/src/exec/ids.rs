//! QFEX's ids on the trade socket (FBC-s8wq): the one format every `client_order_id` we send is
//! written in, and the one rule every venue order id is held to, whether the codec reads it in
//! a private frame (`private.rs`) or names it in a `cancel_order` or `get_order` (`order.rs`).
//! Each is declared once, here, with the pages it cites (decision 0054's rule, which decision
//! 0132 item 6 applies to the probe's `ExecCaps`).

use fbc_core::ClientIdFormat;

/// How our client ids are written in `client_order_id`: fbc-core's UUID layout, an RFC 9562
/// version 8 UUID, lowercase and hyphenated, 36 characters (decision 0004). The encoder spells
/// every client id with it, and FBC-mfey's probe `ExecCaps` declare it as `OrderCaps.client_id`,
/// so every client id QFEX echoes is decoded with the format it was sent in. No other
/// client-id format is named in this crate.
///
/// What QFEX documents, read 2026-11-04:
///
/// - The [add_order page](https://docs.qfex.com/websocket/channels/trade/add_order) makes
///   `client_order_id` an optional client-assigned string, and the
///   [AsyncAPI](https://docs.qfex.com/websocket/trade.yaml) gives it only `type: string`, with no
///   format, length or pattern. No page states a length or a character set, yet the
///   [enums page](https://docs.qfex.com/api-reference/enums) lists `INVALID_CLIENT_ORDER_ID`.
/// - The [get_order](https://docs.qfex.com/websocket/channels/trade/get_order) and
///   [get_user_orders](https://docs.qfex.com/websocket/channels/trade/get_user_orders) example
///   responses carry a lowercase, hyphenated, 36-character UUID as `client_order_id`: the shape
///   this format writes. It may be one QFEX generated, since the add_order, cancel_order and
///   modify_order examples answer with `""`.
/// - The other examples are short hyphenated labels (the
///   [cancel_order](https://docs.qfex.com/websocket/channels/trade/cancel_order),
///   [errors](https://docs.qfex.com/websocket/errors) and
///   [order](https://docs.qfex.com/websocket/channels/trade/order) pages). Under this format
///   they, a random UUID and the decimal millisecond counter the public Java library's QFEX
///   broker writes all decode as not ours
///   (`fixtures/qfex/conformance/ids_roundtrip/java_era.txt`).
///
/// Whether QFEX accepts these ids at their full length, and echoes them unaltered, is what
/// decision 0132's report asks FBC-9sk2's run; a refusal or an altered echo is a follow-up ticket
/// citing the capture.
pub const CLIENT_ID_FORMAT: ClientIdFormat = ClientIdFormat::Uuid;

/// Whether `id` is a venue order id as QFEX writes one: 36 characters in a UUID's 8-4-4-4-12
/// hex form, of any version or variant, in either case. It is the one rule: the private
/// decoder reads `order_id` by it and the encoder sends `cancel_order` and `get_order` only for
/// an id it accepts, so an order the codec decodes is an order it can cancel. FBC-h3z1 reads
/// get_user_orders rows by it too, and FBC-jdj7 may narrow it, on every side at once, once
/// captures show which ids QFEX issues.
///
/// What QFEX documents, read 2026-11-04: the
/// [cancel_order](https://docs.qfex.com/websocket/channels/trade/cancel_order) and
/// [modify_order](https://docs.qfex.com/websocket/channels/trade/modify_order) pages call
/// `order_id` a UUID v4, yet modify_order's own example response names
/// `88888888-4444-4444-4444-1234567890ab`, whose variant digit is `4`; the
/// [errors page](https://docs.qfex.com/websocket/errors) asks only for a valid order UUID
/// (`InvalidOrderId`). No capture exists yet, so neither the version nor the variant is checked:
/// with no cancel-all in the probe caps (0132 item 6), an order whose id a narrower rule refused
/// could not be cancelled.
pub fn is_venue_order_id(id: &str) -> bool {
    uuid_form(id)
}

/// A UUID's wire form: 8-4-4-4-12 ASCII hex digits, either case, whatever its version. QFEX's
/// trade ids take this form too, and no page restricts their version or case either.
pub(crate) fn uuid_form(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(at, byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uuid_in_its_wire_form_is_hyphenated_hex_of_any_version_or_variant() {
        for id in [
            "2f05173c-992a-426b-bcaf-6b341644bb62",
            "5B309929-206F-40EC-804D-CBE46E81AFC1",
            "88888888-4444-4444-4444-1234567890ab",
            "01934f2a-6b1c-7d3e-8f40-5a6b7c8d9e0f",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            assert!(is_venue_order_id(id), "{id}");
            assert!(uuid_form(id), "{id}");
        }
        for id in [
            "",
            "not-a-uuid",
            "2f05173c-992a-426b-bcaf-6b341644bb6",
            "2f05173c-992a-426b-bcaf-6b341644bb622",
            "2f05173c0992a-426b-bcaf-6b341644bb62",
            "2f05173c-992a-426b-bcaf-6b341644bbg2",
            "2f05173c-992a-426b-bcaf-6b341644bb6\u{e9}",
            "{2f05173c-992a-426b-bcaf-6b341644bb6}",
            " 2f05173c-992a-426b-bcaf-6b341644bb6",
        ] {
            assert!(!is_venue_order_id(id), "{id:?}");
        }
    }
}
