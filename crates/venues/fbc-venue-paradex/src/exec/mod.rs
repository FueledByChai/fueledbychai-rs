//! Paradex order entry (BT-402, decision 0054): what it can do ([`exec_caps`], its rate limits
//! in [`ORDER_OPS`] and [`order_limits`]), the SBE schema version its order socket
//! negotiates ([`ORDER_SBE_SCHEMA_VERSION`]), and its private events decoded: `OrderEvent`
//! into order updates, a modify's request_info included ([`decode_order_event`]); `FillEvent`
//! into fills with the venue's realized P&L and funding ([`decode_fill_event`]);
//! `PositionEvent` into the account's position in a market ([`decode_position_event`]); and
//! `AccountEvent` into its balance ([`decode_account_event`]). Funding reaches the account only
//! as each fill's realized funding (decision 0059). Order entry is WebSocket-only: every place,
//! amend and cancel is one of the socket's JSON-RPC methods, with no REST fallback.
//!
//! The order-entry codec is FBC-xvf's; the factory declares these caps, plans the endpoint and
//! builds the codec in FBC-xzp, until when [`ParadexFactory`](crate::ParadexFactory)'s caps stay
//! market data only and [`caps_with_order_entry`](crate::factory::caps_with_order_entry) is
//! what it will declare.

mod account;
mod caps;
mod fill;
mod order;

pub use account::{
    TEMPLATE_ACCOUNT, TEMPLATE_POSITION, decode_account_event, decode_position_event,
};
pub use caps::{ORDER_OPS, exec_caps, order_limits};
pub use fill::{TEMPLATE_FILL, decode_fill_event};
pub use order::{TEMPLATE_ORDER, decode_order_event};

/// The SBE schema version the order socket negotiates (`sbeSchemaVersion=2`, schema id
/// [`sbe::SCHEMA_ID`](crate::md::sbe::SCHEMA_ID)): `OrderEvent`'s request_info
/// (`requestStatus`, `requestType`, `requestId`, `requestMessage`) and `FillEvent`'s
/// `feeCurrency` exist only from `sinceVersion="2"` in the schema (`paradex_1_0.xml` at the
/// paradex-py commit decision 0022 cites). Market data stays on
/// [`sbe::SCHEMA_VERSION`](crate::md::sbe::SCHEMA_VERSION), 1:1 (decision 0054).
pub const ORDER_SBE_SCHEMA_VERSION: u16 = 2;
