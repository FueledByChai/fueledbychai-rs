//! FBC-9a4: Paradex `MarketSummaryEvent` (template 4) decodes at both of its documented root
//! blocks, 216 bytes at schema version 0 and 240 at version 1 (`fundingRatePrecise` appended),
//! into mark and funding events, each from the fields its version carries; a version-1 frame
//! cut below its declared block is refused. One `markets_summary` channel carries both feeds.

mod md;

use std::collections::BTreeSet;

use fbc_core::{
    DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed, FeedSource, FundingCaps, MdCodec,
    MdEvent, PxExact, RawFrame, StreamId, Subscription, VenueConfig, VenueFactory, VenueMeta,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_STREAM, MD_URL, caps};
use fbc_venue_paradex::md::ParadexMd;
use md::{BTC, Decoded, ETH, decode_with, frame, raw, refused_sub, rpc_error, specs};

const V0: &str = "markets-summary-v0.sbe.txt";
const V1: &str = "markets-summary-v1.sbe.txt";

/// MarketSummaryEvent.ts in the hand-built frames, in microseconds.
const TS_US: i64 = 1_759_500_000_200_000;

fn sub(inst: fbc_core::InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

/// A codec subscribed to `feeds` of BTC and ETH.
fn codec(feeds: &[Feed]) -> ParadexMd {
    let mut codec = ParadexMd::new(StreamId(0));
    let subs: Vec<_> = [BTC, ETH]
        .iter()
        .flat_map(|&inst| feeds.iter().map(move |&feed| sub(inst, feed)))
        .collect();
    codec
        .subscribe(&subs, &[], &specs(), &mut Effects::new())
        .unwrap();
    codec
}

/// `bytes` decoded by a codec subscribed to mark and funding.
fn decode(bytes: &[u8]) -> Decoded {
    decode_with(
        &mut codec(&[Feed::Mark, Feed::Funding]),
        RawFrame::Binary(bytes),
    )
}

fn meta() -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(TS_US * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(1005),
    }
}

/// The mark and funding events of a BTC frame with funding rate `rate_e12`.
fn events(rate_e12: i64) -> Vec<(VenueMeta, MdEvent)> {
    vec![
        (
            meta(),
            MdEvent::Mark {
                inst: BTC,
                // 62000.12345678: a mark price is exact, never put on the tick grid.
                px: PxExact::new(6_200_012_345_678, -8),
            },
        ),
        (
            meta(),
            MdEvent::Funding {
                inst: BTC,
                rate_e12,
                interval: None,
                next: None,
            },
        ),
    ]
}

/// Asserts `bytes` are refused with `err`, with nothing pushed and nothing asked for.
fn refused(bytes: &[u8], err: DecodeError) {
    let out = decode(bytes);
    assert_eq!(out.result, Err(err));
    assert!(out.events.is_empty() && out.fx.is_empty());
}

/// Sets the int64 at root `offset` of `bytes` to `value`.
fn set(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[8 + offset..8 + offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn market_summary_decodes_at_both_block_lengths_from_the_fields_each_version_carries() {
    let (v0, v1) = (frame(V0), frame(V1));
    // The header states each version's root block: 216 bytes at 1:0, 240 at 1:1.
    assert_eq!((&v0[0..2], &v0[6..8]), (&[216, 0][..], &[0, 0][..]));
    assert_eq!((&v1[0..2], &v1[6..8]), (&[240, 0][..], &[1, 0][..]));
    assert_eq!(
        v1.len() - v0.len(),
        24,
        "three appended fields, nothing else differs"
    );
    // Version 0 carries only the 8dp fundingRate, 0.00001234.
    let out = decode(&v0);
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, events(12_340_000));
    assert!(out.fx.is_empty());
    // Version 1 carries fundingRatePrecise too, 0.000012345678, which the schema says to prefer.
    let out = decode(&v1);
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, events(12_345_678));
    assert!(out.fx.is_empty());
}

#[test]
fn a_version_1_frame_cut_below_its_declared_240_byte_block_is_refused() {
    let v1 = frame(V1);
    let short = DecodeError::Malformed("SBE frame shorter than its declared block");
    // One byte short, and cut to the length of a version-0 block while declaring 240.
    refused(&v1[..8 + 239], short);
    refused(&v1[..8 + 216], short);
    // The whole block but no market.
    refused(&v1[..8 + 240], DecodeError::Malformed("SBE market"));
}

#[test]
fn a_null_precise_rate_falls_back_to_the_8dp_rate() {
    // The schema: "fall back to field 13 only when this one is null".
    let mut v1 = frame(V1);
    set(&mut v1, 232, i64::MIN);
    assert_eq!(decode(&v1).events, events(12_340_000));
}

#[test]
fn the_captured_eth_frame_decodes_its_mark_and_precise_funding_rate() {
    let out = decode(&raw("eth-markets-summary-2026-09-23.sbe"));
    assert_eq!(out.result, Ok(()));
    // Its seq is 0: no sequence, as a trade's 0 is.
    let meta = VenueMeta {
        exch_ts: Some(ExchNs(1_790_187_956_009_000 * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: None,
    };
    let mark = MdEvent::Mark {
        inst: ETH,
        px: PxExact::new(266_289_254_960, -8),
    };
    // fundingRatePrecise 0.000443605046, not the 8dp fundingRate's 0.0004436.
    let funding = MdEvent::Funding {
        inst: ETH,
        rate_e12: 443_605_046,
        interval: None,
        next: None,
    };
    assert_eq!(out.events, [(meta, mark), (meta, funding)]);
}

/// `markets-summary-v0.sbe.txt` with its root block cut to `len` bytes (the market kept).
fn v0_with_block(len: u16) -> Vec<u8> {
    let bytes = frame(V0);
    let mut out = len.to_le_bytes().to_vec();
    out.extend_from_slice(&bytes[2..8 + usize::from(len)]);
    out.extend_from_slice(&bytes[8 + 216..]);
    out
}

#[test]
fn a_summary_without_the_fields_it_needs_is_refused_with_nothing_pushed() {
    // A block that ends before markPrice, or before fundingRate.
    refused(
        &v0_with_block(16),
        DecodeError::Malformed("summary mark price"),
    );
    refused(
        &v0_with_block(56),
        DecodeError::Malformed("summary funding rate"),
    );
    let cases: [(&str, usize, i64, DecodeError); 6] = [
        (
            V0,
            16,
            i64::MIN,
            DecodeError::Malformed("summary mark price"),
        ),
        // Version 0 has no precise rate to fall back from.
        (
            V0,
            56,
            i64::MIN,
            DecodeError::Malformed("summary funding rate"),
        ),
        (
            V0,
            56,
            i64::MAX,
            DecodeError::Malformed("summary funding rate out of range"),
        ),
        (V1, 8, -1, DecodeError::Malformed("negative seq")),
        (
            V1,
            0,
            i64::MAX,
            DecodeError::Malformed("timestamp out of range"),
        ),
        // A precise rate is used as sent; only a null one falls back.
        (
            V1,
            56,
            i64::MIN,
            DecodeError::Malformed("summary funding rate"),
        ),
    ];
    for (name, offset, value, err) in cases {
        let mut bytes = frame(name);
        set(&mut bytes, offset, value);
        if name == V1 && offset == 56 {
            // Version 1 with a precise rate decodes without the 8dp one.
            assert_eq!(decode(&bytes).events, events(12_345_678));
            set(&mut bytes, 232, i64::MIN);
        }
        refused(&bytes, err);
    }
    let mut bytes = frame(V1);
    let market = bytes.len() - 12;
    bytes[market..].copy_from_slice(b"SOL-USD-PERP");
    refused(&bytes, DecodeError::UnknownInstrument);
}

/// The JSON-RPC requests in `fx`, as (method, channel).
fn requests(fx: &Effects) -> Vec<(String, String)> {
    fx.as_slice()
        .iter()
        .map(|e| {
            let Effect::Send { frame, .. } = e else {
                panic!("not a send: {e:?}");
            };
            let v: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
            (
                v["method"].as_str().unwrap().to_owned(),
                v["params"]["channel"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[test]
fn one_markets_summary_channel_carries_mark_and_funding_and_each_is_pushed_only_while_subscribed() {
    let (specs, channel) = (specs(), "markets_summary.BTC-USD-PERP".to_owned());
    let (mark, funding) = (sub(BTC, Feed::Mark), sub(BTC, Feed::Funding));
    let mut codec = ParadexMd::new(StreamId(0));
    let mut fx = Effects::new();
    // Mark and funding together: one subscribe frame; a repeated subscription sends nothing.
    codec
        .subscribe(&[mark, funding], &[], &specs, &mut fx)
        .unwrap();
    codec.subscribe(&[mark], &[], &specs, &mut fx).unwrap();
    assert_eq!(requests(&fx), [("subscribe".to_owned(), channel.clone())]);
    let v1 = frame(V1);
    let pushed = |codec: &mut ParadexMd| decode_with(codec, RawFrame::Binary(&v1)).events;
    assert_eq!(pushed(&mut codec), events(12_345_678));
    // Mark removed: the channel stays for funding, and only funding is pushed.
    let mut fx = Effects::new();
    codec.subscribe(&[], &[mark], &specs, &mut fx).unwrap();
    assert!(fx.is_empty());
    assert_eq!(pushed(&mut codec), events(12_345_678)[1..]);
    // Funding removed too: one unsubscribe frame, and a frame still in flight pushes nothing.
    codec
        .subscribe(&[], &[funding, mark], &specs, &mut fx)
        .unwrap();
    assert_eq!(requests(&fx), [("unsubscribe".to_owned(), channel)]);
    let out = decode_with(&mut codec, RawFrame::Binary(&v1));
    assert_eq!((out.result, out.events), (Ok(()), vec![]));
}

#[test]
fn a_refused_markets_summary_subscribe_reports_every_feed_the_channel_carried_and_forgets_them() {
    let (specs, channel) = (specs(), "markets_summary.BTC-USD-PERP".to_owned());
    let (mark, funding) = (sub(BTC, Feed::Mark), sub(BTC, Feed::Funding));
    let mut codec = ParadexMd::new(StreamId(0));
    // Mark goes out as request 1; funding then rides the same channel, with no frame of its own.
    let mut fx = Effects::new();
    codec.subscribe(&[mark], &[], &specs, &mut fx).unwrap();
    codec.subscribe(&[funding], &[], &specs, &mut fx).unwrap();
    assert_eq!(fx.len(), 1);
    // The refusal names both feeds the channel was to carry, and asks for nothing (FBC-50m).
    let out = decode_with(&mut codec, RawFrame::Text(&rpc_error(1)));
    assert_eq!(out.result, Ok(()));
    let both = vec![
        refused_sub(BTC, Feed::Mark),
        refused_sub(BTC, Feed::Funding),
    ];
    assert_eq!(out.events, both);
    assert!(out.fx.is_empty(), "{:?}", out.fx);
    // Neither feed is subscribed now: a summary frame pushes nothing, removing them sends no
    // unsubscribe, and only a new subscription the consumer asks for goes out again.
    let v1 = frame(V1);
    let out = decode_with(&mut codec, RawFrame::Binary(&v1));
    assert_eq!((out.result, out.events), (Ok(()), vec![]));
    let mut fx = Effects::new();
    codec.subscribe(&[], &[funding], &specs, &mut fx).unwrap();
    assert!(fx.is_empty(), "{fx:?}");
    codec.subscribe(&[mark], &[], &specs, &mut fx).unwrap();
    assert_eq!(requests(&fx), [("subscribe".to_owned(), channel)]);
    // A subscribe refused after its feeds were removed still names the feed it was sent for.
    let mut fx = Effects::new();
    codec.subscribe(&[], &[mark], &specs, &mut fx).unwrap();
    let out = decode_with(&mut codec, RawFrame::Text(&rpc_error(2)));
    assert_eq!(out.events, [refused_sub(BTC, Feed::Mark)]);
}

#[test]
fn the_caps_declare_mark_and_funding_streamed_and_plan_them_on_the_first_connection() {
    let md = caps().md;
    assert_eq!(
        (md.mark, md.index, md.stats),
        (FeedSource::Stream, FeedSource::None, FeedSource::None)
    );
    // MarketSummaryEvent states neither the funding interval nor the next funding time.
    let funding = FundingCaps {
        source: FeedSource::Stream,
        interval_reported: false,
        next_time_reported: false,
    };
    assert_eq!(md.funding, funding);
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    let subs: BTreeSet<_> = [sub(BTC, Feed::Mark), sub(BTC, Feed::Funding)].into();
    let plans = ParadexFactory.plan_md(&cfg, &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].stream, MD_STREAM);
    assert_eq!(plans[0].subs, subs.into_iter().collect::<Vec<_>>());
}
