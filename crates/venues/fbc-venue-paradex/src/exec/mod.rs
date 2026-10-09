//! Paradex order entry (BT-402, decision 0054): what it can do ([`exec_caps`], its rate limits
//! in [`ORDER_OPS`] and [`order_limits`]), the SBE schema version its order socket
//! negotiates ([`ORDER_SBE_SCHEMA_VERSION`]), and its private events decoded: `OrderEvent`
//! into order updates, a modify's request_info included, reported once per order and request
//! ([`decode_order_event`], [`ModifyRequests`]); `FillEvent`
//! into fills with the venue's realized P&L and funding ([`decode_fill_event`]);
//! `PositionEvent` into the account's position in a market ([`decode_position_event`]); and
//! `AccountEvent` into its balance ([`decode_account_event`]); and its REST reads, the resync of
//! open orders and positions and the order query by client id, as requests and plans whose
//! answers decode whole into the resync events and a query result ([`resync_plan`],
//! [`query_plan`]); and the read-only private-stream codec, which logs in, authenticates the
//! socket, subscribes the private channels and refuses every command ([`ReadOnlyExec`],
//! decision 0061). Funding reaches the account only
//! as each fill's realized funding (decision 0059). Order entry is WebSocket-only: every place,
//! amend and cancel is one of the socket's JSON-RPC methods, with no REST fallback.
//!
//! Commands are encoded as the socket's JSON-RPC frames by [`ParadexEncoder`] (place, batch
//! place, modify, cancel, batch cancel, cancel-all and cancel-on-disconnect), from the command
//! and the `EncodeCtx` only, and their JSON-RPC replies are decoded into one outcome per item
//! by [`ParadexReplies`], each venue error code mapped to a reject kind through
//! [`REJECT_CODES`] (decision 0069). [`ParadexExec`] is the order-entry codec that sends them
//! (decision 0071): the read-only codec's connection, which also carries the private channels,
//! with these frames, their replies, the REST resync and the order query added.
//! [`ParadexFactory`](crate::ParadexFactory) declares these caps
//! ([`caps`](crate::factory::caps)), plans the endpoint and builds this codec, or the read-only
//! one, from the consumer's configuration and credentials (decision 0072).

mod account;
mod caps;
mod codec;
mod encode;
mod errors;
mod fill;
mod order;
mod read_only;
mod reply;
mod rest;

pub use account::{
    TEMPLATE_ACCOUNT, TEMPLATE_POSITION, decode_account_event, decode_position_event,
};
pub use caps::{ORDER_OPS, exec_caps, order_limits};
pub use codec::{CONTROL_IDS, ParadexExec};
pub use encode::ParadexEncoder;
pub use errors::REJECT_CODES;
pub use fill::{TEMPLATE_FILL, decode_fill_event};
pub use order::{MODIFY_OUTCOMES_HELD, ModifyRequests, TEMPLATE_ORDER, decode_order_event};
pub use read_only::{LOGIN_REQUEST, PRIVATE_CHANNELS, REFRESH_TIMER, ReadOnlyExec};
pub use reply::{ParadexReplies, ReplyRead};
pub use rest::{
    RestAnswer, ResyncTags, decode_order_query, decode_resync, open_orders_request,
    order_history_request, positions_request, query_plan, query_request, resync_plan,
    resync_requests,
};

/// The SBE schema version the order socket negotiates (`sbeSchemaVersion=2`, schema id
/// [`sbe::SCHEMA_ID`](crate::md::sbe::SCHEMA_ID)): `OrderEvent`'s request_info
/// (`requestStatus`, `requestType`, `requestId`, `requestMessage`) and `FillEvent`'s
/// `feeCurrency` exist only from `sinceVersion="2"` in the schema (`paradex_1_0.xml` at the
/// paradex-py commit decision 0022 cites). Market data stays on
/// [`sbe::SCHEMA_VERSION`](crate::md::sbe::SCHEMA_VERSION), 1:1 (decision 0054).
pub const ORDER_SBE_SCHEMA_VERSION: u16 = 2;
