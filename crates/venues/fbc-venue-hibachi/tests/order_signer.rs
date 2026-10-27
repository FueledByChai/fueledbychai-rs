//! The signer from fbc-core's wire forms, and what it refuses: a `PlaceWire`, `AmendWire` or
//! `CancelWire` built from an instrument spec (ticks and lots) signs to the signature Java wrote
//! for the same command; max fees are an input of each place and modify, never held by the
//! signer; the order's JSON-only fields (its client id, creationDeadline, orderFlags, trigger,
//! TWAP and parent order, and the wire's time in force, post-only, reduce-only, channel and
//! time) change neither the packed bytes nor the signature; and a price off the tick or a
//! quantity off the step is refused before anything is packed, as is every value the payload
//! cannot carry. The instruments are synthetic; the vectors are the Java ones.

mod common;

use std::collections::HashSet;

use common::{Row, Vectors, config, dec, hex, spec, unhex, venue_id};
use fbc_core::{
    AliasTable, AmendRef, AmendWire, CancelRef, CancelWire, Channel, HttpResponse, InstrumentId,
    InstrumentResolver, InstrumentSpec, Listing, Lots, OrderKind, PlaceWire, PxExact, Side,
    SignError, SigningParams, Ticks, Tif, UnderlyingId, VenueFactory, VenueId, VenueNativeId,
    VenueOrderId, WallNs, dispatch_market_data,
};
use fbc_venue_hibachi::Hibachi;
use fbc_venue_hibachi::discover::EXCHANGE_INFO_TAG;
use fbc_venue_hibachi::sign::{JsonOnly, OrderMessage, pack_order};
use rust_decimal::Decimal;

/// The row's price as a tick index on its spec's grid.
fn ticks(spec: &InstrumentSpec, price: Decimal) -> Ticks {
    spec.price_grid
        .ticks_exact(PxExact::from_decimal(price).unwrap())
        .unwrap()
}

/// The row's quantity as a count of its spec's size steps.
fn lots(spec: &InstrumentSpec, quantity: Decimal) -> Lots {
    let count = quantity / spec.size_step.get();
    assert!(count.fract().is_zero());
    Lots::new(i64::try_from(count.mantissa() / 10i128.pow(count.scale())).unwrap()).unwrap()
}

fn place<'a>(row: &Row, spec: &'a InstrumentSpec) -> PlaceWire<'a> {
    let kind = match row.price() {
        Some(price) => OrderKind::Limit {
            px: ticks(spec, price),
        },
        None => OrderKind::Market,
    };
    PlaceWire {
        spec,
        cid: "fbc-cid-1",
        side: row.side(),
        kind,
        qty: lots(spec, row.dec("quantity")),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        wall: WallNs(1_761_500_000_000_000_000),
        nonce: Some(row.u64("nonce")),
    }
}

/// The `modify` row's order as a modify of the venue's order `id`.
fn amend<'a>(row: &Row, spec: &'a InstrumentSpec, id: &'a VenueOrderId) -> AmendWire<'a> {
    AmendWire {
        spec,
        target: AmendRef::Venue(id),
        side: row.side(),
        px: ticks(spec, row.price().unwrap()),
        qty: lots(spec, row.dec("quantity")),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        wall: WallNs(1_761_500_000_000_000_000),
        nonce: Some(row.u64("nonce")),
    }
}

/// A cancel of the order `target` names.
fn cancel<'a>(spec: &'a InstrumentSpec, target: CancelRef<'a>) -> CancelWire<'a> {
    CancelWire {
        spec,
        target,
        side: Side::Buy,
        wall: WallNs(1_761_500_000_000_000_000),
        nonce: Some(1),
    }
}

#[test]
fn wire_forms_sign_to_the_java_signature() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    for name in [
        "limit_buy",
        "limit_sell",
        "market_buy",
        "market_sell",
        "truncation_2500_01",
    ] {
        let row = vectors.row(name);
        let spec = row.spec();
        let sig = signer
            .sign_place(&place(row, &spec), row.dec("max_fees"))
            .unwrap();
        assert_eq!(sig.to_hex(), row.get("signature"), "{name}");
        assert_eq!(
            sig.to_sig().as_bytes(),
            row.get("signature").as_bytes(),
            "{name}"
        );
    }

    let row = vectors.row("modify");
    let spec = row.spec();
    let id = venue_id("579183763093760001");
    let sig = signer
        .sign_modify(&amend(row, &spec, &id), row.dec("max_fees"))
        .unwrap();
    assert_eq!(sig.to_hex(), row.get("signature"));

    // Cancels: by the venue's order id, and by limit_buy's placement nonce.
    for name in [
        "cancel_order_id",
        "docs_cancel",
        "java_test_cancel_order_id",
        "edge_order_id_max",
    ] {
        let row = vectors.row(name);
        let id = venue_id(row.get("order_id"));
        let sig = signer
            .sign_cancel(&cancel(&spec, CancelRef::Venue(&id)))
            .unwrap();
        assert_eq!(sig.to_hex(), row.get("signature"), "{name}");
    }
    for name in [
        "cancel_nonce",
        "java_test_cancel_nonce",
        "edge_cancel_nonce_zero",
    ] {
        let row = vectors.row(name);
        let target = CancelRef::PlacementNonce(row.u64("nonce"));
        let sig = signer.sign_cancel(&cancel(&spec, target)).unwrap();
        assert_eq!(sig.to_hex(), row.get("signature"), "{name}");
    }
    for name in ["cancel_all", "edge_nonce_max"] {
        let row = vectors.row(name);
        let sig = signer.sign_cancel_all(row.u64("nonce")).unwrap();
        assert_eq!(sig.to_hex(), row.get("signature"), "{name}");
    }
    let row = vectors.row("arm");
    assert_eq!(
        signer.sign_arm(row.u64("nonce")).unwrap().to_hex(),
        row.get("signature")
    );
}

#[test]
fn the_spec_discovery_reads_carries_every_signer_input() {
    // ETH/USDT-P from the hand-built exchange-info answer, through the resolver: contract id 1,
    // decimals 9 and 6, tick 0.000001 and step 0.000000001, the limit_buy vector's instrument.
    let body = include_str!("../../../../fixtures/hibachi/rest/exchange_info.json");
    let plan = Hibachi.discover(&config()).unwrap();
    let caps = Hibachi.caps(&config()).unwrap();
    let response = HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    };
    let drafts = dispatch_market_data(&caps, |scope| {
        plan.parse(&[(EXCHANGE_INFO_TAG, Ok(response))], scope)
    })
    .map(|step| step.done().expect("one round"))
    .unwrap();
    let draft = drafts
        .into_iter()
        .find(|d| d.venue_symbol.as_wire() == "ETH/USDT-P")
        .unwrap();
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let listing = Listing {
        id: InstrumentId::new(7),
        underlying: UnderlyingId::new(7),
    };
    resolver
        .list(VenueId::new(6), draft.asset, listing)
        .unwrap();
    let spec = resolver.spec(VenueId::new(6), draft, 1, WallNs(0)).unwrap();

    let vectors = Vectors::read();
    let row = vectors.row("limit_buy");
    assert_eq!(row.spec().native_id, spec.native_id);
    assert_eq!(row.spec().signing, spec.signing);
    let sig = vectors
        .signer()
        .sign_place(&place(row, &spec), row.dec("max_fees"))
        .unwrap();
    assert_eq!(sig.to_hex(), row.get("signature"));
}

#[test]
fn max_fees_are_an_input_of_each_place_and_modify() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    let place_row = vectors.row("limit_buy");
    let place_spec = place_row.spec();
    let place_wire = place(place_row, &place_spec);
    let modify_row = vectors.row("modify");
    let modify_spec = modify_row.spec();
    let id = venue_id("579183763093760001");
    let modify_wire = amend(modify_row, &modify_spec, &id);
    // The same place, and the same modify, under four max fees: each packs and signs the fee it
    // was given, so a signer that put any one fee of its own in place of the one passed fails
    // at the other three (Reviewer DL-1 on FBC-8mzv).
    let fees = ["0.00045", "0.0005", "0", "0.12345678"];
    let mut seen = HashSet::new();
    for fee in fees {
        let place_message = OrderMessage {
            max_fees: dec(fee),
            ..place_row.order(&place_spec)
        };
        let modify_message = OrderMessage {
            max_fees: dec(fee),
            ..modify_row.order(&modify_spec)
        };
        let scaled = (dec(fee) * Decimal::from(100_000_000)).trunc();
        let fee_bytes = u64::try_from(scaled.mantissa()).unwrap().to_be_bytes();
        for (what, message, sig) in [
            (
                "place",
                place_message,
                signer.sign_place(&place_wire, dec(fee)),
            ),
            (
                "modify",
                modify_message,
                signer.sign_modify(&modify_wire, dec(fee)),
            ),
        ] {
            let payload = pack_order(&message).unwrap();
            assert_eq!(payload.as_bytes()[32..], fee_bytes, "{what} {fee}");
            let sig = sig.unwrap();
            assert_eq!(signer.sign_payload(&payload), Ok(sig), "{what} {fee}");
            assert!(seen.insert(sig), "{what} {fee} signs as another command");
        }
    }
    assert_eq!(seen.len(), 2 * fees.len(), "each fee signs differently");
    // The signer holds its key and nothing else: no fee value or default.
    assert_eq!(
        std::mem::size_of::<fbc_venue_hibachi::sign::HibachiSigner>(),
        std::mem::size_of::<Box<()>>()
    );
}

#[test]
fn json_only_fields_change_neither_the_packed_bytes_nor_the_signature() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    for name in ["limit_buy", "market_sell", "modify", "modify_no_price"] {
        let row = vectors.row(name);
        let spec = row.spec();
        let bare = row.order(&spec);
        let full = OrderMessage {
            json_only: JsonOnly {
                client_id: Some("fbc-cid-1"),
                creation_deadline: Some("1761500005"),
                order_flags: Some("POST_ONLY"),
                trigger_price: Some("2400"),
                trigger_direction: Some("LOW"),
                twap_config: Some(r#"{"twapDurationMinutes":10,"twapQuantityMode":"FIXED"}"#),
                parent_order: Some(r#"{"orderId":"579183763093760001"}"#),
            },
            ..bare
        };
        let other = OrderMessage {
            json_only: JsonOnly {
                order_flags: Some("REDUCE_ONLY"),
                trigger_direction: Some("HIGH"),
                ..full.json_only
            },
            ..full
        };
        let packed = pack_order(&bare).unwrap();
        assert_eq!(hex(packed.as_bytes()), row.get("payload"), "{name}");
        for message in [&full, &other] {
            assert_eq!(pack_order(message).unwrap(), packed, "{name}");
            assert_eq!(
                signer.sign_order(message),
                signer.sign_order(&bare),
                "{name}"
            );
        }
        assert_eq!(
            signer.sign_order(&bare).unwrap().to_hex(),
            row.get("signature")
        );
    }

    // The wire's own JSON-only fields: client id, time in force, post-only, reduce-only,
    // channel and time; and the order a modify names.
    let row = vectors.row("limit_buy");
    let spec = row.spec();
    let base = place(row, &spec);
    let fees = row.dec("max_fees");
    let expected = signer.sign_place(&base, fees).unwrap();
    for variant in [
        PlaceWire {
            cid: "other-cid",
            ..base
        },
        PlaceWire {
            tif: Tif::Ioc,
            ..base
        },
        PlaceWire {
            post_only: true,
            ..base
        },
        PlaceWire {
            reduce_only: true,
            ..base
        },
        PlaceWire {
            channel: Channel::Rpi,
            ..base
        },
        PlaceWire {
            wall: WallNs(0),
            ..base
        },
    ] {
        assert_eq!(
            signer.sign_place(&variant, fees),
            Ok(expected),
            "{variant:?}"
        );
    }
    let row = vectors.row("modify");
    let spec = row.spec();
    let id = venue_id("579183763093760001");
    let by_venue = AmendWire {
        spec: &spec,
        target: AmendRef::Venue(&id),
        side: row.side(),
        px: ticks(&spec, row.price().unwrap()),
        qty: lots(&spec, row.dec("quantity")),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        wall: WallNs(1),
        nonce: Some(row.u64("nonce")),
    };
    let by_client = AmendWire {
        target: AmendRef::Client("fbc-cid-1"),
        tif: Tif::Ioc,
        reduce_only: true,
        wall: WallNs(2),
        ..by_venue
    };
    let fees = row.dec("max_fees");
    assert_eq!(
        signer.sign_modify(&by_venue, fees),
        signer.sign_modify(&by_client, fees)
    );
    assert_eq!(
        signer.sign_modify(&by_client, fees).unwrap().to_hex(),
        row.get("signature")
    );
}

#[test]
fn a_price_off_the_tick_or_a_quantity_off_the_step_is_refused_before_packing() {
    // ETH's decimals on a 0.01 tick and a 0.001 step.
    let spec = spec(1, 9, 6, "0.01", "0.001");
    let order = |price: &str, quantity: &str| OrderMessage {
        spec: &spec,
        nonce: 1_761_500_000_000_000,
        side: Side::Buy,
        quantity: dec(quantity),
        price: Some(dec(price)),
        max_fees: dec("0.00045"),
        json_only: JsonOnly::NONE,
    };
    assert!(pack_order(&order("2500.01", "0.01")).is_ok());
    let off_tick = Err(SignError::Unsignable("price off the tick size"));
    for price in ["2500.015", "2500.001", "0.005", "2500.0100001"] {
        assert_eq!(pack_order(&order(price, "0.01")), off_tick, "{price}");
    }
    // A price whose ticks do not fit the grid's index is not on it either.
    assert_eq!(
        pack_order(&order("79228162514264337593543950335", "0.01")),
        off_tick
    );
    let off_step = Err(SignError::Unsignable("quantity off the step size"));
    for quantity in ["0.0105", "0.0001", "1.0000001"] {
        assert_eq!(
            pack_order(&order("2500.01", quantity)),
            off_step,
            "{quantity}"
        );
    }
    // Java's packer test truncates 0.0123456789 to 12345678 on these decimals; off this 0.001
    // step, the Rust packer refuses it rather than truncate.
    assert_eq!(pack_order(&order("2500.01", "0.0123456789")), off_step);
    // Off both: the price is named first, and nothing is packed for either.
    assert_eq!(pack_order(&order("2500.015", "0.0105")), off_tick);
    // A market order has no price to check; its quantity still is.
    let market = OrderMessage {
        price: None,
        ..order("1", "0.0105")
    };
    assert_eq!(pack_order(&market), off_step);
}

#[test]
fn what_the_payload_cannot_carry_is_refused_naming_the_part() {
    let unsignable = |what| Err(SignError::Unsignable(what));
    let eth = spec(1, 9, 6, "0.000001", "0.000000001");
    let order = |spec: &InstrumentSpec, quantity: &str, price: Option<&str>, fees: &str| {
        pack_order(&OrderMessage {
            spec,
            nonce: 1,
            side: Side::Sell,
            quantity: dec(quantity),
            price: price.map(dec),
            max_fees: dec(fees),
            json_only: JsonOnly::NONE,
        })
        .map(|p| hex(p.as_bytes()))
    };
    assert!(order(&eth, "1", Some("2500"), "0.0005").is_ok());
    assert_eq!(
        order(&eth, "-1", Some("2500"), "0.0005"),
        unsignable("negative quantity")
    );
    assert_eq!(
        order(&eth, "1", Some("-2500"), "0.0005"),
        unsignable("negative price")
    );
    assert_eq!(
        order(&eth, "1", Some("2500"), "-0.0005"),
        unsignable("negative max fees")
    );
    // Max fees must scale exactly by 10^8, with no other scale.
    assert_eq!(
        order(&eth, "1", Some("2500"), "0.000000001"),
        unsignable("max fees finer than 1e-8")
    );
    assert_eq!(
        order(&eth, "1", Some("2500"), "92233720368.54775808"),
        unsignable("max fees past 2^63 - 1 when scaled")
    );
    // A step finer than the underlying decimals cannot scale the quantity exactly.
    let finer = spec(1, 2, 6, "0.01", "0.001");
    assert_eq!(
        order(&finer, "0.005", Some("1"), "0"),
        unsignable("quantity finer than the underlying decimals")
    );
    // The contract id is the spec's numeric native id, at most u32::MAX.
    let mut text_id = eth.clone();
    text_id.native_id = Some(VenueNativeId::Text("ETH".into()));
    let mut no_id = eth.clone();
    no_id.native_id = None;
    let mut wide = eth.clone();
    wide.native_id = Some(VenueNativeId::Number(u64::from(u32::MAX) + 1));
    let mut no_decimals = eth.clone();
    no_decimals.signing = None;
    let no_contract = unsignable("no numeric contract id");
    assert_eq!(order(&text_id, "1", Some("2500"), "0"), no_contract);
    assert_eq!(order(&no_id, "1", Some("2500"), "0"), no_contract);
    assert_eq!(
        order(&wide, "1", Some("2500"), "0"),
        unsignable("contract id past u32")
    );
    assert_eq!(
        order(&no_decimals, "1", Some("2500"), "0"),
        unsignable("no asset decimals")
    );
    // Decimals so large no value scales into 63 bits.
    let huge = spec(1, 255, 0, "1", "1");
    assert_eq!(
        order(&huge, "1", None, "0"),
        unsignable("quantity past 2^63 - 1 when scaled")
    );
    let mut wide_settlement = huge.clone();
    wide_settlement.signing = Some(SigningParams::AssetDecimals {
        underlying: 0,
        settlement: 255,
    });
    assert_eq!(
        order(&wide_settlement, "1", Some("1"), "0"),
        unsignable("price past 2^63 - 1 when scaled")
    );
    // The nonce fits 63 bits.
    assert_eq!(
        pack_order(&OrderMessage {
            spec: &eth,
            nonce: u64::MAX,
            side: Side::Buy,
            quantity: dec("1"),
            price: None,
            max_fees: dec("0"),
            json_only: JsonOnly::NONE,
        }),
        Err(SignError::Unsignable("nonce past 2^63 - 1"))
    );
}

#[test]
fn the_wire_forms_refuse_what_hibachi_cannot_sign() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    let row = vectors.row("limit_buy");
    let spec = row.spec();
    let wire = place(row, &spec);
    let no_nonce = Err(SignError::Unsignable("no nonce"));
    assert_eq!(
        signer.sign_place(
            &PlaceWire {
                nonce: None,
                ..wire
            },
            dec("0")
        ),
        no_nonce
    );
    let negative = PlaceWire {
        kind: OrderKind::Limit { px: Ticks(-1) },
        ..wire
    };
    assert_eq!(
        signer.sign_place(&negative, dec("0")),
        Err(SignError::Unsignable("negative price"))
    );
    let amend = AmendWire {
        spec: &spec,
        target: AmendRef::Client("fbc-cid-1"),
        side: Side::Buy,
        px: Ticks(1),
        qty: Lots::new(1).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        wall: WallNs(0),
        nonce: None,
    };
    assert_eq!(signer.sign_modify(&amend, dec("0")), no_nonce);

    // A cancel by client id: no Java or SDK vector holds it, so it is not signed.
    assert_eq!(
        signer.sign_cancel(&cancel(&spec, CancelRef::Client("hello"))),
        Err(SignError::Unsignable("cancel by client id"))
    );
    for text in ["not-a-number", "01", "18446744073709551616"] {
        let id = venue_id(text);
        assert_eq!(
            signer.sign_cancel(&cancel(&spec, CancelRef::Venue(&id))),
            Err(SignError::Unsignable("order id not a decimal u64")),
            "{text}"
        );
    }
    let past = venue_id("9223372036854775808");
    assert_eq!(
        signer.sign_cancel(&cancel(&spec, CancelRef::Venue(&past))),
        Err(SignError::Unsignable("order id past 2^63 - 1"))
    );
    assert_eq!(
        signer.sign_cancel(&cancel(&spec, CancelRef::PlacementNonce(u64::MAX))),
        Err(SignError::Unsignable("placement nonce past 2^63 - 1"))
    );
    let past_63_bits = Err(SignError::Unsignable("nonce past 2^63 - 1"));
    assert_eq!(signer.sign_cancel_all(1 << 63), past_63_bits);
    assert_eq!(signer.sign_arm(1 << 63), past_63_bits);
    // The arm signs the same 8 bytes cancel-all does.
    assert_eq!(signer.sign_arm(42), signer.sign_cancel_all(42));
    assert_eq!(unhex("000000000000002a"), 42u64.to_be_bytes());
}
