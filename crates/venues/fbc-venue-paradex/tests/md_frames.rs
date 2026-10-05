//! FBC-jlr: Paradex SBE bbo and trade frames, gated on block length, decode into Touch and
//! Trade events; other schemas and short frames are refused; heartbeats and unknown templates
//! are skipped; JSON-RPC control frames are consumed or reported.

mod md;

use fbc_core::{
    Aggressor, DecodeError, ExchNs, ExchTsKind, Lots, Lvl, MdCodec, MdEvent, RawFrame, Ticks,
    VenueMeta,
};
use fbc_venue_paradex::md::{BBO, ParadexMd};
use md::{BTC, decode, decode_with, frame, refused, refused_sub, rpc_error, specs};

/// BboEvent.ts in the fixtures, in microseconds.
const TS_US: i64 = 1_759_500_000_123_456;
/// TradeEvent.createdAt in the fixtures, in microseconds.
const CREATED_US: i64 = 1_759_500_000_120_001;

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn bbo_meta() -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(TS_US * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(1001),
    }
}

#[test]
fn bbo_frames_decode_into_touch_with_seq_no_as_venue_seq() {
    // 62000.5 x 0.25 / 62001.0 x 1.5 on a 0.1 tick and 0.001 size step.
    let touch = MdEvent::Touch {
        inst: BTC,
        bid: Some(Lvl {
            px: Ticks(620_005),
            qty: lots(250),
        }),
        ask: Some(Lvl {
            px: Ticks(620_010),
            qty: lots(1_500),
        }),
        source: BBO,
    };
    // The same frame with a longer root block decodes the same: the appended bytes are skipped.
    for name in ["bbo.sbe.txt", "bbo-longer-block.sbe.txt"] {
        let out = decode(&frame(name));
        assert_eq!(out.result, Ok(()), "{name}");
        assert_eq!(out.events, [(bbo_meta(), touch)], "{name}");
        assert!(out.fx.is_empty(), "{name}");
    }
    // A null side is an empty side.
    let out = decode(&frame("bbo-ask-empty.sbe.txt"));
    let MdEvent::Touch { bid, ask, .. } = out.events[0].1 else {
        panic!("not a touch: {:?}", out.events);
    };
    assert_eq!((bid.map(|l| l.px), ask), (Some(Ticks(620_005)), None));
}

#[test]
fn trade_frames_decode_into_trades_with_seq_no_as_venue_seq() {
    let trade = MdEvent::Trade {
        inst: BTC,
        // Schema 1:1 carries only the deprecated, truncated int64 trade id.
        id: None,
        aggressor: Aggressor::Seller,
        px: Ticks(620_000),
        qty: lots(125),
    };
    let meta = VenueMeta {
        exch_ts: Some(ExchNs(CREATED_US * 1_000)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(1002),
    };
    // The longer frame has 8 appended root bytes and appended var data (tradeIdStr).
    for name in ["trade.sbe.txt", "trade-longer-block.sbe.txt"] {
        let out = decode(&frame(name));
        assert_eq!(out.result, Ok(()), "{name}");
        assert_eq!(out.events, [(meta, trade)], "{name}");
    }
}

#[test]
fn a_trade_without_an_orderbook_sequence_has_no_venue_seq() {
    // The schema: seq is 0 for block-trade legs and position transfers.
    let mut bytes = frame("trade.sbe.txt");
    bytes[8 + 8..8 + 16].copy_from_slice(&0i64.to_le_bytes());
    assert_eq!(decode(&bytes).events[0].0.venue_seq, None);
    // A buyer or an unrepresentable side are the aggressor too.
    for (side, aggressor) in [(1, Aggressor::Buyer), (254, Aggressor::Unknown)] {
        bytes[8 + 24] = side;
        let MdEvent::Trade { aggressor: got, .. } = decode(&bytes).events[0].1 else {
            panic!("not a trade");
        };
        assert_eq!(got, aggressor);
    }
}

#[test]
fn another_schema_id_is_refused_with_nothing_pushed() {
    for name in ["bbo.sbe.txt", "heartbeat.sbe.txt"] {
        let mut bytes = frame(name);
        bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
        refused(&bytes, DecodeError::Malformed("SBE schema id"));
    }
}

#[test]
fn a_frame_shorter_than_its_declared_block_is_refused_with_nothing_pushed() {
    let bytes = frame("bbo.sbe.txt");
    let short = DecodeError::Malformed("SBE frame shorter than its declared block");
    refused(&bytes[..8 + 47], short);
    refused(
        &bytes[..5],
        DecodeError::Malformed("SBE frame shorter than its header"),
    );
}

#[test]
fn a_heartbeat_and_an_unknown_template_are_skipped_without_error() {
    let mut unknown = frame("heartbeat.sbe.txt");
    unknown[2..4].copy_from_slice(&99u16.to_le_bytes());
    for bytes in [frame("heartbeat.sbe.txt"), unknown] {
        let out = decode(&bytes);
        assert_eq!(out.result, Ok(()));
        assert!(out.events.is_empty() && out.fx.is_empty());
    }
}

/// `bbo.sbe.txt` with its root block cut to `len` bytes (the header and var data kept).
fn bbo_with_block(len: u16) -> Vec<u8> {
    let bytes = frame("bbo.sbe.txt");
    let mut out = len.to_le_bytes().to_vec();
    out.extend_from_slice(&bytes[2..8]);
    out.extend_from_slice(&bytes[8..8 + usize::from(len)]);
    out.extend_from_slice(&bytes[8 + 48..]);
    out
}

#[test]
fn a_required_field_past_a_shorter_block_refuses_the_frame() {
    // A 40-byte block ends before askSize: the field is absent, and bbo needs it.
    refused(&bbo_with_block(40), DecodeError::Malformed("bbo ask"));
    refused(&bbo_with_block(4), DecodeError::Malformed("bbo bid"));
    let trade = frame("trade.sbe.txt");
    let mut short = 24u16.to_le_bytes().to_vec();
    short.extend_from_slice(&trade[2..8 + 24]);
    short.extend_from_slice(&trade[8 + 50..]);
    refused(&short, DecodeError::Malformed("trade side"));
}

/// Sets the int64 at root `offset` of `bytes` to `value`.
fn set(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[8 + offset..8 + offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn values_the_instrument_or_the_schema_cannot_hold_are_refused() {
    let bbo = frame("bbo.sbe.txt");
    let off_grid = DecodeError::Malformed("price off the instrument's grid");
    let off_step = DecodeError::Malformed("size off the instrument's size step");
    let cases: [(usize, i64, DecodeError); 7] = [
        // 62000.55 is off the 0.1 tick; 0.2505 off the 0.001 step; a size cannot be negative.
        (16, 6_200_055_000_000, off_grid),
        (24, 25_050_000, off_step),
        (24, -100_000_000, off_step),
        // A price without its size, or a size without its price.
        (16, i64::MIN, DecodeError::Malformed("bbo bid")),
        (40, i64::MIN, DecodeError::Malformed("bbo ask")),
        (8, -1, DecodeError::Malformed("negative seq")),
        (
            0,
            i64::MAX,
            DecodeError::Malformed("timestamp out of range"),
        ),
    ];
    for (offset, value, err) in cases {
        let mut bytes = bbo.clone();
        set(&mut bytes, offset, value);
        refused(&bytes, err);
    }
    let mut trade = frame("trade.sbe.txt");
    trade[8 + 24] = 3;
    refused(&trade, DecodeError::Malformed("trade side"));
    let mut trade = frame("trade.sbe.txt");
    set(&mut trade, 25, i64::MIN);
    refused(&trade, off_grid);
}

#[test]
fn a_frame_for_an_unknown_market_or_without_one_is_refused() {
    let mut bytes = frame("bbo.sbe.txt");
    let market = bytes.len() - 12;
    bytes[market..].copy_from_slice(b"SOL-USD-PERP");
    refused(&bytes, DecodeError::UnknownInstrument);
    // No var data at all: the market is required.
    refused(
        &bbo_with_block(48)[..8 + 48],
        DecodeError::Malformed("SBE market"),
    );
    // A length past the frame, and bytes that are not UTF-8.
    refused(
        &bytes[..bytes.len() - 1],
        DecodeError::Malformed("SBE string runs past the frame"),
    );
    bytes[market] = 0xff;
    refused(&bytes, DecodeError::Malformed("SBE string UTF-8"));
}

#[test]
fn a_subscribe_acknowledgement_is_consumed_and_a_refused_subscribe_is_reported_by_instrument_and_feed()
 {
    let mut codec = ParadexMd::new(fbc_core::StreamId(0));
    let mut fx = fbc_core::Effects::new();
    let subs = [
        fbc_core::Subscription {
            inst: BTC,
            feed: fbc_core::Feed::Touch(BBO),
        },
        fbc_core::Subscription {
            inst: BTC,
            feed: fbc_core::Feed::Trades,
        },
    ];
    codec.subscribe(&subs, &[], &specs(), &mut fx).unwrap();
    codec.subscribe(&[], &subs[..1], &specs(), &mut fx).unwrap();
    assert_eq!(fx.len(), 3);
    // Requests 1 and 2 subscribed, 3 unsubscribed.
    let ack = r#"{"jsonrpc":"2.0","result":{"channel":"bbo.BTC-USD-PERP"},"usIn":1,"usOut":2,"usDiff":1,"id":1}"#;
    let out = decode_with(&mut codec, RawFrame::Text(ack));
    assert_eq!(out.result, Ok(()));
    assert!(out.events.is_empty() && out.fx.is_empty());
    // A refused subscribe is reported as the subscription it refused (FBC-50m), and nothing is
    // asked for: no subscribe resent, no reconnect.
    let out = decode_with(&mut codec, RawFrame::Text(&rpc_error(2)));
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, [refused_sub(BTC, subs[1].feed)]);
    assert!(out.fx.is_empty(), "{:?}", out.fx);
    let cases = [
        (3, "the venue refused an unsubscribe"),
        // An answered request is not answered twice; an unknown id is still an error.
        (2, "the venue reported an error"),
    ];
    for (id, what) in cases {
        let out = decode_with(&mut codec, RawFrame::Text(&rpc_error(id)));
        assert_eq!(out.result, Err(DecodeError::Malformed(what)));
        assert!(out.events.is_empty() && out.fx.is_empty());
    }
    for (text, what) in [
        ("not json", "text frame is not JSON"),
        (
            r#"{"jsonrpc":"2.0","method":"subscription"}"#,
            "text frame is neither a reply nor an error",
        ),
    ] {
        let out = decode_with(&mut codec, RawFrame::Text(text));
        assert_eq!(out.result, Err(DecodeError::Malformed(what)));
    }
}
