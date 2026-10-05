//! What the OMS tests share: client ids minted under a real namespace lease, venue order ids
//! built through the core's decode scope (decision 0004), placements and order updates.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, CancelReason, Channel, CidMatch, CidMint, ClientOrderId, ConnTopology, Encoding,
    FeedSource, FundingCaps, InstrumentId, Lots, MatchingCaps, MdCaps, Namespace, NamespaceLease,
    NewOrder, OrderKind, OrderUpdate, Readiness, RejectKind, Side, StpScope, TerminalReject, Ticks,
    Tif, TradeCaps, VenueCaps, VenueOrderId, VenueOrderState, WallNs, dispatch_market_data,
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

fn decode_caps() -> VenueCaps {
    VenueCaps {
        exec: None,
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
