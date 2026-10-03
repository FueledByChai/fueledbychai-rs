//! Decision 0004: venue order ids and fill ids are built only through the `DecodeScope` the
//! core's dispatch lends to a codec callback; tests use the same scope.

use fbc_core::{Charset, CidMatch, ClientIdFormat, IdError, MAX_VENUE_ID_LEN, Namespace, dispatch};

const HIBACHI_LIKE: ClientIdFormat = ClientIdFormat::Alnum {
    max_len: 32,
    charset: Charset::Alphanumeric,
};

#[test]
fn the_scope_builds_venue_ids_from_the_wire_text() {
    let (vid, fid) = dispatch(&HIBACHI_LIKE, Namespace::new(3), |scope| {
        (
            scope.venue_order_id("1759363200000201030"),
            scope.fill_id("8812"),
        )
    });
    assert_eq!(vid.unwrap().as_str(), "1759363200000201030");
    assert_eq!(fid.unwrap().as_str(), "8812");
}

#[test]
fn the_scope_refuses_empty_and_overlong_venue_ids() {
    let over = "7".repeat(MAX_VENUE_ID_LEN + 1);
    dispatch(&HIBACHI_LIKE, Namespace::new(3), |scope| {
        assert_eq!(scope.venue_order_id(""), Err(IdError::Empty));
        assert_eq!(scope.fill_id(""), Err(IdError::Empty));
        assert!(matches!(
            scope.venue_order_id(&over),
            Err(IdError::TooLong { len: 97, .. })
        ));
        assert!(matches!(scope.fill_id(&over), Err(IdError::TooLong { .. })));
    });
}

#[test]
fn the_scope_decodes_client_ids_against_its_namespace() {
    dispatch(&ClientIdFormat::Uuid, Namespace::new(3), |scope| {
        assert_eq!(
            scope.client_order_id("f47ac10b-58cc-4372-a567-0e02b2c3d479"),
            CidMatch::Unparseable
        );
        assert_eq!(
            scope.client_order_id("1759363200001"),
            CidMatch::Unparseable
        );
        assert!(format!("{scope:?}").contains("DecodeScope"));
    });
}
