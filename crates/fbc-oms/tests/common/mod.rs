//! What the OMS tests share: client ids minted under a real namespace lease, venue order ids,
//! fill ids and fees built through the core's decode scope (decision 0004), placements, order
//! updates and fills.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckModel, AssetSym, CancelOnDisconnect, CancelReason, Channel, CidMatch, CidMint,
    ClientIdFormat, ClientOrderId, ConnTopology, Encoding, ExecCaps, Fee, FeedSource, FillCaps,
    FillEvent, FillId, FillIdent, FillSource, FundingCaps, InstrumentId, Liquidity3, Lots,
    MatchingCaps, MdCaps, Namespace, NamespaceLease, NewOrder, NonceScope, OrderCaps, OrderKind,
    OrderKindTag, OrderUpdate, OrderingKey, Readiness, RefKind, RejectKind, Side, SnapshotSource,
    StpScope, Support, TagSet, TerminalReject, Ticks, Tif, TifTag, TradeCaps, VenueCaps,
    VenueFeeSign, VenueOrderId, VenueOrderState, WallNs, dispatch_market_data,
};

/// The namespace every test client id is minted in.
pub const NS: Namespace = Namespace::new(7);

/// A fresh client id, minted under a namespace lease held for the whole test binary.
pub fn cid() -> ClientOrderId {
    static MINT: OnceLock<Mutex<CidMint>> = OnceLock::new();
    let mint = MINT.get_or_init(|| {
        let dir = lease_dir();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), NS).unwrap();
        Mutex::new(CidMint::new(lease, 0, 0, WallNs(0)))
    });
    mint.lock().unwrap().mint().unwrap()
}

fn lease_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("fbc-oms-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A venue order id, built as a codec builds one: through the decode scope.
pub fn vid(text: &str) -> VenueOrderId {
    dispatch_market_data(&decode_caps(), |scope| scope.venue_order_id(text)).unwrap()
}

/// A fill id, built as a codec builds one: through the decode scope.
pub fn fill_id(text: &str) -> FillId {
    dispatch_market_data(&decode_caps(), |scope| scope.fill_id(text)).unwrap()
}

/// No fee, decoded under the synthetic venue's declared sign.
pub fn no_fee() -> Fee {
    dispatch_market_data(&decode_caps(), |scope| {
        scope.fee(0, AssetSym::new("USDC").unwrap())
    })
    .unwrap()
}

/// A fill of `qty` on `side` for the client id `cid` (none when `None`), named by `ident`; a
/// replay when `replay`.
pub fn fill(
    cid: Option<ClientOrderId>,
    ident: FillIdent,
    side: Side,
    qty: i64,
    replay: bool,
) -> FillEvent {
    FillEvent {
        ident,
        cid: cid.map(CidMatch::Ours),
        inst: InstrumentId::new(1),
        side,
        px: Ticks(100),
        qty: lots(qty),
        liquidity: Liquidity3::Maker,
        fee: no_fee(),
        realized_pnl: None,
        realized_funding: None,
        replay,
    }
}

/// A fill named by the venue's fill id `fid` alone.
pub fn ident(fid: &str) -> FillIdent {
    FillIdent::Venue {
        fill: fill_id(fid),
        vid: None,
        cum_after: None,
    }
}

/// The capabilities the test scope decodes for: a synthetic venue (no real one) that takes
/// orders, so its fees decode under a declared sign.
fn decode_caps() -> VenueCaps {
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit]),
                tifs: TagSet::of(&[TifTag::Gtc]),
                channels: TagSet::of(&[Channel::Public]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: vec![],
                amend: None,
                cancel_refs: TagSet::of(&[RefKind::Venue]),
                query_refs: TagSet::of(&[RefKind::Venue]),
                cancel_before_ack: false,
                cancel_is_signed: false,
                batch_place: None,
                batch_cancel: None,
                cancel_all_account: Support::Unsupported,
                cancel_all_instrument: Support::Unsupported,
                cancel_on_disconnect: CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: true,
                },
                ack: AckModel::SinglePhase,
                client_id: ClientIdFormat::Uuid,
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::PerAccountMonotonic,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                events_echo_flags: false,
                sign_cost_hint_us: 0,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: false,
                realized_funding: false,
                fee_sign: VenueFeeSign::PositiveIsCost,
                fee_asset_reported: true,
                fill_id: true,
                replays_fills_on_reconnect: true,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: false,
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_millis(1),
            topology: ConnTopology::PerInstrument,
            max_conn_lifetime: None,
        },
        limits: vec![],
        readiness_ceiling: Readiness::Record,
    }
}

pub fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// A post-only limit buy of `qty` at `px`.
pub fn placement(cid: ClientOrderId, px: i64, qty: i64) -> NewOrder {
    NewOrder {
        cid,
        inst: InstrumentId::new(1),
        side: Side::Buy,
        qty: lots(qty),
        kind: OrderKind::Limit { px: Ticks(px) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// An order event for our order `cid` in `state`, with the venue's cumulative fill `cum`.
pub fn update(cid: Option<ClientOrderId>, state: VenueOrderState, cum: i64) -> OrderUpdate {
    OrderUpdate {
        cid: cid.map(CidMatch::Ours),
        vid: None,
        inst: InstrumentId::new(1),
        side: Side::Buy,
        state,
        cum_filled: lots(cum),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    }
}

/// A venue's order-ending refusal.
pub fn rejected(kind: RejectKind) -> VenueOrderState {
    VenueOrderState::Rejected(TerminalReject::new(kind).unwrap())
}

/// A cancel by request.
pub fn canceled() -> VenueOrderState {
    VenueOrderState::Canceled(CancelReason::Requested)
}
