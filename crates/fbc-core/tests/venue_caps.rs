//! Decision 0003: what a venue can do is data, and every field is mandatory. The synthetic test
//! venue below declares every field of `VenueCaps` and of each type it is made of, by name; it
//! compiles only because nothing is left out, since no capability type has a `Default` to fill
//! a gap (`tests/ui_caps/` proves a literal missing a field, and `VenueCaps::default()`, do not
//! compile). Its values describe no real venue. Order and fill capabilities come together in
//! one `exec` block (decision 0015); the market-data-only venue below declares `exec: None` and
//! nothing about fills, and `tests/ui_caps/` proves neither half can be written without the
//! other.

use std::fs;
use std::path::Path;
use std::time::Duration;

use fbc_core::{
    AckModel, AmendAck, AmendCaps, AmendQty, AssetSym, Batch, BookCaps, Cadence,
    CancelOnDisconnect, Channel, Charset, ClientIdFormat, ConnTopology, Continuity, Encoding,
    ExchTsKind, ExecCaps, Feature, FeedSource, FillCaps, FillSource, FundingCaps, LimitScope,
    MatchingCaps, MdCaps, Money, Namespace, NonceScope, OpKind, OrderCaps, OrderKindTag,
    OrderingKey, QueueModelQuality, RateLimit, Readiness, RefKind, SeqDomain, SnapshotSource,
    SpeedBump, SpeedBumpScope, StpScope, Support, TagSet, TifTag, TouchSourceCaps, TradeCaps,
    VenueCaps, VenueFeeSign, dispatch,
};

/// A synthetic venue that declares every capability field.
fn synthetic_caps() -> VenueCaps {
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit, OrderKindTag::Market]),
                tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
                channels: TagSet::of(&[Channel::Public, Channel::Rpi]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: vec![
                    (Feature::PostOnly, Feature::ReduceOnly),
                    (Feature::Rpi, Feature::ReduceOnly),
                ],
                amend: Some(AmendCaps {
                    price: true,
                    qty: true,
                    flags: false,
                    when_partially_filled: false,
                    reject_keeps_original: true,
                    keeps_venue_id: true,
                    ack: AmendAck::RpcReplyOnly,
                    qty_semantics: AmendQty::TotalIncludingFilled,
                    keeps_priority: None,
                }),
                cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
                query_refs: TagSet::of(&[RefKind::Client]),
                cancel_before_ack: false,
                cancel_is_signed: false,
                batch_place: Some(Batch { max_items: 10 }),
                batch_cancel: None,
                cancel_all_account: Support::Native,
                cancel_all_instrument: Support::Unsupported,
                cancel_on_disconnect: CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: true,
                },
                ack: AckModel::TwoPhase {
                    risk_reject_window: Duration::from_millis(250),
                },
                client_id: ClientIdFormat::Alnum {
                    max_len: 32,
                    charset: Charset::Alphanumeric,
                },
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::PerAccountMonotonic,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                events_echo_flags: false,
                sign_cost_hint_us: 150,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: true,
                realized_funding: false,
                fee_sign: VenueFeeSign::PositiveIsRebate,
                fee_asset_reported: true,
                fill_id: true,
                replays_fills_on_reconnect: false,
            },
        }),
        matching: MatchingCaps {
            speed_bump: Some(SpeedBump {
                delay: Duration::from_millis(25),
                applies_to: SpeedBumpScope::TakersOnly,
            }),
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            encoding: Encoding::Sbe,
            touch_sources: vec![TouchSourceCaps {
                channel: "bbo",
                cadence: Cadence::Realtime,
                seq_domain: SeqDomain::SharedWithBook,
                ts_kind: ExchTsKind::MatchingEngine,
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            books: vec![
                BookCaps {
                    channel: "deltas",
                    max_depth: 50,
                    cadence: Cadence::Capped(Duration::from_millis(50)),
                    continuity: Continuity::PlusOne,
                    windowed: false,
                    rest_anchor: false,
                    includes_channels: TagSet::of(&[Channel::Public]),
                    queue_model: QueueModelQuality::Calibratable,
                },
                BookCaps {
                    channel: "pulsed_book",
                    max_depth: 10,
                    cadence: Cadence::Pulsed(Duration::from_millis(250)),
                    continuity: Continuity::Windowed,
                    windowed: true,
                    rest_anchor: true,
                    includes_channels: TagSet::of(&[Channel::Public, Channel::Rpi]),
                    queue_model: QueueModelQuality::BracketOnly,
                },
            ],
            trades: TradeCaps {
                source: FeedSource::Stream,
                aggressor: true,
                trade_id: true,
            },
            funding: FundingCaps {
                source: FeedSource::Poll,
                interval_reported: true,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::Stream,
            index: FeedSource::Poll,
            ts_precision: Duration::from_micros(1),
            topology: ConnTopology::Shared {
                max_subscriptions: Some(200),
            },
            max_conn_lifetime: Some(Duration::from_secs(24 * 3600)),
        },
        limits: vec![
            RateLimit {
                scope: LimitScope::Account,
                ops: TagSet::of(&[OpKind::Place, OpKind::Amend, OpKind::Cancel]),
                per: Duration::from_secs(1),
                units: 40,
            },
            RateLimit {
                scope: LimitScope::Ip,
                ops: TagSet::of(&[OpKind::Subscribe, OpKind::Rest, OpKind::Query]),
                per: Duration::from_secs(60),
                units: 1_200,
            },
        ],
        readiness_ceiling: Readiness::Paper,
    }
}

#[test]
fn the_synthetic_venue_declares_every_capability() {
    let caps = synthetic_caps();
    let exec = caps
        .exec
        .as_ref()
        .expect("the synthetic venue takes orders");
    let order = &exec.order;
    assert!(order.kinds.contains(OrderKindTag::Market));
    assert!(!order.tifs.contains(TifTag::Fok));
    assert!(order.channels.contains(Channel::Rpi));
    assert!(
        order
            .flag_conflicts
            .contains(&(Feature::PostOnly, Feature::ReduceOnly))
    );
    let amend = order.amend.expect("the synthetic venue amends");
    assert_eq!(
        (amend.when_partially_filled, amend.keeps_priority),
        (false, None)
    );
    assert!(order.query_refs.contains(RefKind::Client));
    assert!(!order.query_refs.contains(RefKind::Venue));
    assert_eq!(order.batch_place, Some(Batch { max_items: 10 }));
    assert_eq!(order.cancel_all_instrument, Support::Unsupported);
    assert_eq!(order.nonce_scope, NonceScope::PerAccountMonotonic);
    assert_eq!(
        caps.matching.speed_bump.map(|bump| bump.applies_to),
        Some(SpeedBumpScope::TakersOnly)
    );
    assert_eq!(caps.md.books.len(), 2);
    // Mark and index are subscribable feeds, so the caps say where each comes from.
    assert_eq!(
        (caps.md.mark, caps.md.index),
        (FeedSource::Stream, FeedSource::Poll)
    );
    assert!(caps.md.books[1].includes_channels.contains(Channel::Rpi));
    assert!(!caps.md.books[0].includes_channels.contains(Channel::Rpi));
    assert_eq!(caps.limits[0].ops.iter().count(), 3);
    assert_eq!(caps.readiness_ceiling, Readiness::Paper);
    // Capabilities are plain data: a clone is equal, and a debug print names the values.
    assert_eq!(caps.clone(), caps);
    assert!(format!("{caps:?}").contains("PositiveIsRebate"));
}

#[test]
fn the_declared_fee_sign_and_client_id_format_drive_the_decode_scope() {
    let caps = synthetic_caps();
    let exec = caps.exec.as_ref().unwrap();
    let usdc = AssetSym::new("USDC").unwrap();
    // The synthetic venue reports a rebate as a positive amount; as a Fee it is a negative cost.
    let fee = dispatch(
        &exec.order.client_id,
        Namespace::new(2),
        exec.fills.fee_sign,
        |scope| scope.fee(150_000, usdc),
    );
    assert_eq!(fee.unwrap().cost(), Money::new(-150_000, usdc));
}

/// A market-data-only test venue (decision 0015): `exec: None` is all it says about order entry,
/// so it declares no order capability, no fill source and no fee sign for an account feed it does
/// not have. Its market data, matching engine, limits and ceiling are the synthetic venue's.
fn market_data_only_caps() -> VenueCaps {
    VenueCaps {
        exec: None,
        readiness_ceiling: Readiness::Record,
        ..synthetic_caps()
    }
}

#[test]
fn a_market_data_only_venue_declares_no_order_or_fill_capability() {
    let md_only = market_data_only_caps();
    assert!(md_only.exec.is_none());
    assert_ne!(md_only, synthetic_caps());
    // Nothing about fills or fees survives anywhere in the declaration.
    let printed = format!("{md_only:?}");
    for claim in ["fills", "fee_sign", "FillSource", "PositiveIs", "client_id"] {
        assert!(!printed.contains(claim), "{claim} in {printed}");
    }
    // What it does declare is still stated in full.
    assert_eq!(md_only.md, synthetic_caps().md);
    assert_eq!(md_only.matching, synthetic_caps().matching);
    assert!(Readiness::Record < Readiness::Shadow);
    assert!(Readiness::Shadow < Readiness::Paper);
    assert!(Readiness::Paper < Readiness::Live);
}

#[test]
fn no_capability_or_instrument_type_has_a_default_or_is_non_exhaustive() {
    // Decision 0003: a Default would let an adapter skip a field, and #[non_exhaustive] would
    // let one compile without declaring a new one. The compile-fail tests prove it for
    // VenueCaps; this holds the line for every type in the two modules.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for module in ["caps.rs", "instrument.rs"] {
        let text = fs::read_to_string(src.join(module)).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap();
            assert!(
                !code.contains("Default") && !code.contains("non_exhaustive"),
                "{module}:{}: {}",
                n + 1,
                line.trim()
            );
        }
    }
}
