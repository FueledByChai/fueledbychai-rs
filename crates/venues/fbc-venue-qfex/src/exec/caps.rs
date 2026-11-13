//! QFEX's order and fill capabilities under decision 0132 item 6's pre-production probe mode
//! and decision 0143 item 2's production one, and only there (decisions 0003, 0015, 0054): each
//! value cites the page it is read from, and where the pages are silent the value is the one
//! under which the OMS and the planner do less. Every other configuration declares
//! `exec: None` (decisions 0117, 0122).
//!
//! Every value matches what this crate's encoder does (`order.rs` and [`super::QfexExec`]), so
//! a command these caps leave out is refused unsent for the reason they give, and one they
//! declare is sent: decision 0003's `caps_truthful` rule. The one departure is
//! `production_probe`'s: the caps are the same there, but the codec refuses every place, batch
//! place and amend unsent until a record admits places on production (decision 0145).
//!
//! Sources, named below by their short names, read 2026-11-04:
//!
//! - the trade socket's pages, `https://docs.qfex.com/websocket/channels/trade/<page>` for
//!   `add_order`, `cancel_order`, `get_order`, `get_user_orders`, `cancel_on_disconnect`,
//!   `authenticate` and `order` (order responses);
//! - the [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml) ("the AsyncAPI"): its
//!   `AddOrderCommand`, `CancelOnDisconnectCommand`, `OrderResponse` and `FillResponse`;
//! - the [enums page](https://docs.qfex.com/api-reference/enums), the
//!   [order-types page](https://docs.qfex.com/qfex/order-types), the
//!   [architecture page](https://docs.qfex.com/qfex/architecture) and the
//!   [fees page](https://docs.qfex.com/qfex/fees).

use core::time::Duration;

use fbc_core::{
    AckModel, CancelOnDisconnect, Channel, ExecCaps, Feature, FillCaps, FillSource, NonceScope,
    OrderCaps, OrderKindTag, OrderingKey, RefKind, SnapshotReads, SnapshotSource, Support, TagSet,
    TifTag, VenueFeeSign,
};

use super::ids::CLIENT_ID_FORMAT;

/// How long after an `ACK` a refusal may still come: QFEX's 500 ms speed bump
/// ([`crate::probe::SPEED_BUMP`]), which no page says the `ACK` comes before or after, plus a
/// margin of 4.5 s, the five seconds decision 0054 declared for Paradex's undocumented risk
/// check: the margin decision 0132 item 6 asks its ticket to cite, recorded in FBC-mfey's
/// notes. FBC-jdj7 sizes it from the measured `ACK` time.
pub const RISK_REJECT_WINDOW: Duration = Duration::from_secs(5);

/// What the probe's order entry offers and what its fills report, every field declared.
pub fn probe_exec_caps() -> ExecCaps {
    ExecCaps {
        order: OrderCaps {
            // Decision 0132's v1 scope: limit GTC or IOC, and post-only GTC. The enums page
            // and the AsyncAPI's `AddOrderCommand` list MARKET too, which v1 does not send.
            kinds: TagSet::of(&[OrderKindTag::Limit]),
            // The enums page's OrderTimeInForce lists FOK too, which v1 does not send.
            tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
            // No page names a channel but the public book.
            channels: TagSet::of(&[Channel::Public]),
            // The `ALO` order type, "add liquidity only" (enums page, order-types page).
            post_only: true,
            // `add_order` shows `reduce_only` as `0` in its example and as a boolean in its
            // samples, and the AsyncAPI's `AddOrderCommand` does not define it: not sent until
            // a capture settles it.
            reduce_only: false,
            // The order-types page admits ALO with GTC only, and the encoder refuses an ALO
            // IOC as this conflict.
            flag_conflicts: vec![(Feature::PostOnly, Feature::Ioc)],
            // `modify_order` is refused until a capture settles whether its quantity is the
            // total or the remaining (the enums page names `CANNOT_MODIFY_PARTIAL_FILL`), as
            // the owner chose (decision 0132).
            amend: None,
            // `cancel_order` with `cancel_order_id_type` `order_id` "cancels exactly one
            // order"; one by client id "may cancel multiple orders" of the symbol (decision
            // 0132 item 8).
            cancel_refs: TagSet::of(&[RefKind::Venue]),
            // The `get_order` page: it takes a `symbol` and an `order_id`, never a client id.
            query_refs: TagSet::of(&[RefKind::Venue]),
            // A cancel names the venue's order id (the `cancel_order` page), which only the
            // order's `ACK` carries (the `order` page).
            cancel_before_ack: false,
            // QFEX signs no order frame: the `cancel_order` and `add_order` pages and the
            // AsyncAPI's `AddOrderCommand` carry no signature, and the HMAC signs only the
            // authentication (the `authenticate` page, `src/sign`).
            cancel_is_signed: false,
            // No page of the trade socket documents a batch place.
            batch_place: None,
            // Nor a cancel-many: the `cancel_order` page cancels one order a request.
            batch_cancel: None,
            // `cancel_all_orders` exists, but decision 0132 item 6 declares no cancel-all.
            cancel_all_account: Support::Unsupported,
            // Nor `cancel_all_orders` with its symbol filter: decision 0132 item 6 declares
            // no cancel-all.
            cancel_all_instrument: Support::Unsupported,
            // `cancel_on_disconnect`: an armed connection's drop cancels every open order of
            // the authenticated account, "you need to opt in again whenever you reconnect";
            // decision 0132 items 1 and 2 widen 0121's authority for QFEX's arm. The page
            // documents the opt-in and no request that turns it off, and the codec refuses
            // every disarm unsent (decision 0132 item 1): no disarm offered (decision 0141).
            cancel_on_disconnect: CancelOnDisconnect::AccountWide {
                offers_disarm: false,
            },
            // `add_order` answers with an `order_response` whose status is `ACK`, "Order
            // accepted" (enums page), and non-ALO orders wait in a 500 ms speed bump
            // (architecture page) no page places the `ACK` before or after: two-phase, with a
            // window longer than the hold (decision 0132 item 6).
            ack: AckModel::TwoPhase {
                risk_reject_window: RISK_REJECT_WINDOW,
            },
            // The one client-id format the encoder writes every `client_order_id` in
            // (FBC-s8wq).
            client_id: CLIENT_ID_FORMAT,
            // Every `ACK` example shows an empty `client_order_id`, none of those requests
            // having sent one: not echoed until captures show it (decision 0132 item 8).
            cid_echoed_on_events: false,
            // The `add_order`, `cancel_order` and `get_order` pages carry no nonce; the nonces
            // the `authenticate` page signs are random per connection, not per order.
            nonce_scope: NonceScope::None,
            // The AsyncAPI's `OrderResponse` carries no sequence, and no page says its
            // `update_time` orders an order's updates: only a terminal state wins.
            ordering_key: OrderingKey::None,
            // Decision 0132 item 5: offset pages with no as-of time, count or has-more field.
            snapshot_source: SnapshotSource::Untrustworthy,
            // Decision 0132 item 5's three reads: `get_user_orders` pages, REST positions and
            // trade pages.
            snapshot_reads: SnapshotReads::Separate,
            // Read only from a trustworthy source (decision 0109); item 5's later record
            // measures it before any flip.
            snapshot_lag_bound: Duration::ZERO,
            // The AsyncAPI's `OrderResponse` echoes the order `type` (so ALO) and
            // `time_in_force`, but no capture shows it yet, and reduce-only is never sent:
            // declared not echoed.
            events_echo_flags: false,
            // No order is signed (`cancel_is_signed` above: the `add_order` and
            // `cancel_order` pages), so none waits for a signature.
            sign_cost_hint_us: 0,
        },
        fills: FillCaps {
            // The `fills` channel's `fill_response` (the AsyncAPI's `FillResponse`).
            source: FillSource::Native,
            // The AsyncAPI's `FillResponse` names no liquidity: its `execution_type` is NEW or
            // LIQUIDATION and its `aggressor_side` is not said to be ours.
            liquidity_flag: false,
            // The AsyncAPI's `FillResponse` has a `realised_pnl`, a number no page describes:
            // not claimed.
            realized_pnl: false,
            // The AsyncAPI's `FillResponse` has no funding member.
            realized_funding: false,
            // The fees page describes every fee, maker and taker, as one we pay, from 0.00%,
            // with no rebate; `fee`'s sign is otherwise unstated.
            fee_sign: VenueFeeSign::PositiveIsCost,
            // The AsyncAPI's `FillResponse` names no fee currency.
            fee_asset_reported: false,
            // `trade_id`, which decision 0132 item 5 keys fills by.
            fill_id: true,
            // The order page: "Live updates are not a replay of missed events".
            replays_fills_on_reconnect: false,
        },
    }
}
