//! Paradex's order and fill capabilities, each value cited (decisions 0003, 0015, 0054).
//!
//! Sources, named below by their short names:
//! - the method pages, `https://docs.paradex.trade/ws/web-socket-channels/<method>/<method>`
//!   for `order-create`, `order-create-batch`, `order-modify`, `order-cancel`,
//!   `order-cancel-batch`, `order-cancel-all` and `order-cancel-on-disconnect`, and the private
//!   channels `orders-market-symbol` and `fills-market-symbol`;
//! - "Order Instructions" (`/trading/orders/order-instructions`), "Retail Price Improvement"
//!   (`/trading/rpi`), the REST pages "Modify order" (`/api/prod/orders/modify`), "Create batch
//!   of orders" (`/api/prod/orders/batch`) and "Get order by client id"
//!   (`/api/prod/orders/get-by-client-id`), "API" rate limits
//!   (`/api/general-information/rate-limits/api`) and "Advanced API Trader Best Practices"
//!   (`/api/general-information/api-best-practices`), all on docs.paradex.trade;
//! - the SBE schema `paradex_1_0.xml` at the paradex-py commit decision 0022 cites ("the
//!   schema"), version 1:2;
//! - FueledByChaiTrading's Paradex broker ("the Java library").
//!
//! A value no Paradex document states is declared at the value under which the OMS and the
//! planner do less, and decision 0054 lists it for the owner's testnet run (FBC-8xr).

use core::time::Duration;

use fbc_core::{
    AckModel, AmendAck, AmendCaps, AmendQty, Batch, CancelBatch, CancelOnDisconnect, Channel,
    ClientIdFormat, ExecCaps, Feature, FillCaps, FillSource, LimitScope, NonceScope, OpKind,
    OrderCaps, OrderKindTag, OrderingKey, RateLimit, RefKind, SnapshotSource, Support, TagSet,
    TifTag, VenueFeeSign,
};

/// The order methods Paradex counts against its order rate limits: `order.create` and
/// `order.create_batch` (place), `order.modify` (amend), `order.cancel` and
/// `order.cancel_batch` (cancel) and `order.cancel_all`. "API" rate limits counts `POST, DELETE,
/// PUT /orders` in one pool, and `order.create`'s page says the WebSocket methods' "Rate limits
/// are shared with the REST API (same Redis counters)".
pub const ORDER_OPS: [OpKind; 4] = [
    OpKind::Place,
    OpKind::Amend,
    OpKind::Cancel,
    OpKind::CancelAll,
];

/// Paradex's per-account order rate limits, both windows: "API" rate limits, "`POST, DELETE,
/// PUT /orders` | 800 req/s OR 17250 req/m | Account". A batch counts once ("Rate limit is
/// consumed once for the entire batch", `order-create-batch`), so a codec charges a batch one
/// unit. The per-IP limit the market-data caps already declare counts these methods too
/// ([`caps_with_order_entry`](crate::factory::caps_with_order_entry)).
pub fn order_limits() -> [RateLimit; 2] {
    let account = |per, units| RateLimit {
        scope: LimitScope::Account,
        ops: TagSet::of(&ORDER_OPS),
        per,
        units,
    };
    [
        account(Duration::from_secs(1), 800),
        account(Duration::from_secs(60), 17_250),
    ]
}

/// What Paradex order entry offers and what its fills report, every field declared.
pub fn exec_caps() -> ExecCaps {
    ExecCaps {
        order: OrderCaps {
            // `order-create`: type LIMIT or MARKET (the STOP_* types are not modelled).
            kinds: TagSet::of(&[OrderKindTag::Limit, OrderKindTag::Market]),
            // `order-create`: instruction GTC or IOC; Paradex has no fill-or-kill.
            tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
            // `order-create`: instruction RPI places an order on the retail price improvement
            // channel; every other instruction on the public book.
            channels: TagSet::of(&[Channel::Public, Channel::Rpi]),
            // `order-create`: instruction POST_ONLY.
            post_only: true,
            // `order-create`: flags REDUCE_ONLY ("Order Instructions": "A 'Reduce-only' order
            // can be placed with any Order Types and Instructions").
            reduce_only: true,
            // One `instruction` carries the time in force, post-only and RPI, so:
            // - IOC "Can be used with Reduce-only, but not with GTC and Post-only" ("Order
            //   Instructions");
            // - RPI orders "must be Post-Only" ("Retail Price Improvement"), so never IOC;
            // - "RPI orders cannot be placed in combination with other special order types"
            //   ("Retail Price Improvement"), reduce-only among them.
            // An RPI order is post-only by the venue's rule: the codec writes RPI whether or not
            // the order also asks for post-only, as the Java library does (decision 0054).
            flag_conflicts: vec![
                (Feature::PostOnly, Feature::Ioc),
                (Feature::Ioc, Feature::Rpi),
                (Feature::Rpi, Feature::ReduceOnly),
            ],
            amend: Some(AmendCaps {
                // `order-modify`: `id` is required, and the modify signs it (FBC-6's
                // `ModifyOrder` message), so an order is amended by its venue id only.
                refs: TagSet::of(&[RefKind::Venue]),
                // `order-modify`: "Updates the price and/or size of an open order".
                price: true,
                qty: true,
                // `order-modify` takes no instruction or flags.
                flags: false,
                // Undocumented: declared false (decision 0054), so a partly filled order is
                // cancelled and replaced rather than amended.
                when_partially_filled: false,
                // The schema's OrderEvent request_info: a REJECTED modify is reported on an
                // update of the order itself, whose own status says it still rests (decision
                // 0054; to be confirmed on FBC-8xr).
                reject_keeps_original: true,
                // "Modify order": "The modified order maintains its original order ID."
                keeps_venue_id: true,
                // The schema's OrderEvent `requestStatus`/`requestType` (1:2): the amend is
                // final on the order event reporting SUCCESS for MODIFY_ORDER, not on the
                // method's reply (decision 0054).
                ack: AmendAck::ReplacedEvent,
                // `order-modify`: size is the "New (or unchanged) size", the order's size, of
                // which OrderEvent `sizeOpen` is the remaining part.
                qty_semantics: AmendQty::TotalIncludingFilled,
                // Measured in calibration, never assumed (design §4.5).
                keeps_priority: None,
            }),
            // `order-cancel`: "Either id or (client_id + market) must be provided"; the codec
            // writes the market with a client id.
            cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
            // "Get order by client id" (`GET /orders/by_client_id/{client_id}`).
            query_refs: TagSet::of(&[RefKind::Client]),
            // Undocumented: declared false (decision 0054), so an order's cancel waits for its
            // acknowledgement.
            cancel_before_ack: false,
            // `order-cancel`, `order-cancel-batch` and `order-cancel-all` carry no signature.
            cancel_is_signed: false,
            // `order-create-batch`: "up to the configured maximum number of orders"; "Create
            // batch of orders" (REST): "Valid batch size is between 1-10 order(s)". Ten, the
            // documented REST bound (decision 0054).
            batch_place: Some(Batch { max_items: 10 }),
            // `order-cancel-batch` takes `order_ids` only (the Java library met "Client order
            // IDs are not currently supported"), and states no maximum: ten, as for
            // placement (decision 0054).
            batch_cancel: Some(CancelBatch {
                max_items: 10,
                refs: TagSet::of(&[RefKind::Venue]),
            }),
            // `order-cancel-all`: "Cancels all open orders for the authenticated account. If
            // market is provided only orders in that market are cancelled." fbc-oms never
            // builds the account one.
            cancel_all_account: Support::Native,
            cancel_all_instrument: Support::Native,
            // `order-cancel-on-disconnect`: orders placed or tracked during the connection are
            // cancelled "if the WebSocket disconnects unexpectedly", and the state "does not
            // persist across reconnects", so it is requested again on every connection.
            cancel_on_disconnect: CancelOnDisconnect::PerConnection {
                rearm_on_reconnect: true,
            },
            // "Create batch of orders": "Orders are queued for risk checking independently",
            // so an accepted order (status NEW) can still be closed by the risk check. The
            // window is undocumented: five seconds (decision 0054).
            ack: AckModel::TwoPhase {
                risk_reject_window: Duration::from_secs(5),
            },
            // order.create's client_id is a string of at most 64 characters; this library
            // writes its ids as UUIDs there (decision 0004).
            client_id: ClientIdFormat::Uuid,
            // The schema's OrderEvent and FillEvent carry `clientOrderId`.
            cid_echoed_on_events: true,
            // Orders are signed over a `signature_timestamp`, with no nonce.
            nonce_scope: NonceScope::None,
            // The schema's OrderEvent `seq` (`orders-market-symbol`: "Unique increasing number
            // (non-sequential) ... changes on every order update").
            ordering_key: OrderingKey::VenueSeq,
            // Undocumented whether the open-orders snapshot is consistent with the order
            // stream: declared untrustworthy (decision 0054), so an absence from it never
            // ends an order.
            snapshot_source: SnapshotSource::Untrustworthy,
            // The schema's OrderEvent carries `timeInForce` (POST_ONLY, RPI) and `flags`
            // (REDUCE_ONLY).
            events_echo_flags: true,
            // "Advanced API Trader Best Practices": signing in Rust averages 0.2ms.
            sign_cost_hint_us: 200,
        },
        fills: FillCaps {
            // The fills.{market} channel: the schema's FillEvent (template 21).
            source: FillSource::Native,
            // FillEvent `liquidity`: MAKER or TAKER.
            liquidity_flag: true,
            // FillEvent `realizedPnl` ("null if position not closed").
            realized_pnl: true,
            // FillEvent `realizedFunding`.
            realized_funding: true,
            // FillEvent `fee`: "Fee charged (positive = paid, negative = rebate)".
            fee_sign: VenueFeeSign::PositiveIsCost,
            // FillEvent `feeCurrency`, from schema version 2 (`ORDER_SBE_SCHEMA_VERSION`).
            fee_asset_reported: true,
            // FillEvent `fillId`.
            fill_id: true,
            // Undocumented: declared false (decision 0054). Nothing waits for a replay: a fill
            // missed across a reconnect comes from the resync, and a replayed one is a
            // duplicate its fill id drops (0005's I3).
            replays_fills_on_reconnect: false,
        },
    }
}
