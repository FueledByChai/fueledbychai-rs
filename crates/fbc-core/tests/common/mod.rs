//! Synthetic venue capabilities shared by the integration tests. A `DecodeScope` is lent only
//! for a venue's `VenueCaps` (FBC-75u), so every test that needs a venue id, a venue symbol or a
//! fee declares the venue it decodes for. The values describe no real venue.

#![allow(dead_code)]

use std::time::Duration;

use fbc_core::{
    AckModel, AmendAck, AmendCaps, AmendQty, Batch, BookCaps, Cadence, CancelOnDisconnect, Channel,
    Charset, ClientIdFormat, ConnTopology, Continuity, Encoding, ExchTsKind, ExecCaps, Feature,
    FeedSource, FillCaps, FillSource, FundingCaps, LimitScope, MatchingCaps, MdCaps, NonceScope,
    OpKind, OrderCaps, OrderKindTag, OrderingKey, QueueModelQuality, RateLimit, Readiness, RefKind,
    SeqDomain, SnapshotSource, SpeedBump, SpeedBumpScope, StpScope, Support, TagSet, TifTag,
    TouchSourceCaps, TradeCaps, VenueCaps, VenueFeeSign,
};

/// A synthetic venue that declares every capability field.
pub fn synthetic_caps() -> VenueCaps {
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
            RateLimit {
                scope: LimitScope::Pair,
                ops: TagSet::of(&[OpKind::Place, OpKind::Amend]),
                per: Duration::from_secs(10),
                units: 100,
            },
            RateLimit {
                scope: LimitScope::Connection,
                ops: TagSet::of(&[OpKind::Subscribe, OpKind::Control]),
                per: Duration::from_secs(1),
                units: 10,
            },
            RateLimit {
                scope: LimitScope::Ip,
                ops: TagSet::of(&[OpKind::Connect]),
                per: Duration::from_secs(300),
                units: 300,
            },
        ],
        readiness_ceiling: Readiness::Paper,
    }
}

/// A market-data-only test venue (decision 0015): `exec: None` is all it says about order entry,
/// so it declares no order capability, no fill source and no fee sign for an account feed it does
/// not have. Its market data, matching engine, limits and ceiling are the synthetic venue's.
pub fn market_data_only_caps() -> VenueCaps {
    VenueCaps {
        exec: None,
        readiness_ceiling: Readiness::Record,
        ..synthetic_caps()
    }
}

/// The synthetic venue, declaring `client_id` and `fee_sign` instead of its own.
pub fn exec_caps(client_id: ClientIdFormat, fee_sign: VenueFeeSign) -> VenueCaps {
    let mut caps = synthetic_caps();
    let exec = caps
        .exec
        .as_mut()
        .expect("the synthetic venue takes orders");
    exec.order.client_id = client_id;
    exec.fills.fee_sign = fee_sign;
    caps
}

/// A venue with UUID client ids that reports fees as costs: what a test that needs only venue
/// ids or symbols decodes for.
pub fn uuid_caps() -> VenueCaps {
    exec_caps(ClientIdFormat::Uuid, VenueFeeSign::PositiveIsCost)
}
