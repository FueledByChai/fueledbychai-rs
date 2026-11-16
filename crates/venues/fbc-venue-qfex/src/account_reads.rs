//! The probe's account reads (decision 0132 item 7, FBC-h3z1): what the probe sends, outside an
//! order-entry session, to check, before every session, that each account it reads holds no
//! open order and a flat position; the account listing, which it no longer sends, since it
//! reads the accounts its configuration names (decision 0144); what its liveness hold sends and
//! reads on its one trade connection (decision 0146, FBC-39ks); and the reads its drop test and
//! report need (decision 0132 items 4 and 5, FBC-be57): a `get_user_trades` page and the ended
//! orders `GET /user/historic-orders` lists.
//!
//! - **The requests.** `GET /user/subaccounts` ([`subaccounts`]), `GET /user/positions`
//!   ([`positions`]) and `GET /user/historic-orders` ([`historic_orders`]) on `qfex.rest_url`;
//!   and, on `qfex.trade_url` ([`trade_url`]), the connection's authentication for one account
//!   ([`authentication`]) and then one `get_user_orders` page ([`user_orders`]) or one
//!   `get_user_trades` page ([`user_trades`]), or, on the hold's connection, the three
//!   subscriptions a session sends ([`subscriptions`]). Each takes its origin only from a
//!   configuration that a probe mode's guard admits (`preprod_probe`, FBC-mfey, or
//!   `production_probe`, decision 0143 item 2) and whose provider account and account key
//!   decision 0122 item 1 admits ([`probe::connections`]): without a probe mode, or under a
//!   configuration the guard refuses, nothing is built. Every signed part, and every account
//!   identifier, comes from `src/auth`'s [`ReadAuth`], which the caller builds for an account it
//!   names, the primary or a subaccount id (the probe's from its configuration, decision 0144;
//!   a listing's from [`Subaccounts::accounts`]), with that account's credentials; nothing here
//!   reads or holds a credential. `ReadAuth` signs and names its spans for this crate only, so
//!   these functions are the one way to them.
//! - **The decoders.** [`decode_subaccounts`], [`decode_positions`], [`decode_historic`],
//!   [`orders_page`], [`decode_trades_page`], [`authenticated`], and, for the hold,
//!   [`confirmation`] and [`socket_err`]. Every number is read from its own JSON text exactly
//!   (`exec::exact`, FBC-zeb0), never through `f64`. A REST decoder refuses a status outside
//!   2xx, naming the status only, malformed JSON, a member repeated, a required member missing
//!   and a top-level member QFEX does not document (every body is `additionalProperties:
//!   false`, and a cursor or a count added would make the answer partial). A frame is read only
//!   from the bytes the journal keeps of it ([`redact_inbound`]), as the order-entry codec reads
//!   its own (decision 0134 item 5). A trades page is read in either envelope, and its rows as
//!   each carries its fields, since QFEX's two sources disagree on both (decision 0132's
//!   context): the envelope it came in is kept, a row missing a field is read, not refused, and
//!   the names of the members it carries are kept.
//! - **The redaction** ([`redact_inbound`]): the spans `src/auth` names (every listed account
//!   id, every `user_id`, every balance value, and the rest, decisions 0028 and 0122 item 3), an
//!   err or an ack that echoes a request kept as decision 0134 says (0126 as it narrows it), and
//!   the whole body of a REST answer outside 2xx, which nothing decodes.
//!
//! QFEX's pages, read 2026-11-05: [list subaccounts](https://docs.qfex.com/api-reference/rest/user/list-subaccounts)
//! (`account_ids`, an array of strings or null, and an optional `$schema`; the primary account
//! is not listed), [user positions](https://docs.qfex.com/api-reference/rest/user/user-positions)
//! (`positions`, an array or null, each row's `symbol`, `position` and `open_orders`, and a
//! required `balance`), [get_user_orders](https://docs.qfex.com/websocket/channels/trade/get_user_orders)
//! (`limit`, default and largest 1000, and `offset`; the answer an `all_orders_response` of open
//! `orders` and active `twaps`), and the [rate page](https://docs.qfex.com/websocket/rate)
//! (`get_user_orders` weighs 5.0, so 50 in this crate's tenths). Read 2026-11-16:
//! [historic orders](https://docs.qfex.com/api-reference/rest/user/historic-orders) (`limit`, 1
//! to 1000, and `offset` in the query, the four signed headers and the optional
//! `x-qfex-requested-account-id`; the answer a required `count` and `data`, an array of orders
//! or null, each with a `client_order_id` and a `terminal_status` among its sixteen required
//! members, and an optional `$schema`), [get_user_trades](https://docs.qfex.com/websocket/channels/trade/get_user_trades)
//! (`start_ts`, `limit` and `offset`; the answer `user_trades` beside a `count`, its example
//! rows carrying `aggressor_side`, `client_order_id` and `timestamp`) and the [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml)
//! (`user_trades_response`, an array of `TradePayload` rows carrying `side` and none of those
//! three).
//!
//! FBC-l931 sends these but the listing (decision 0144) through fbc-runtime, charged to the
//! run's shared limiter, and judges the answers. The order-entry codec's resync (FBC-jxau,
//! decision 0132 item 5), inside this crate, reuses the positions request with its handle's
//! signed headers (`positions_request`), the positions decoder, the `get_user_orders` frame
//! (`user_orders_body`) and the page's shape (`page_rows`) on the bytes it keeps of a frame,
//! without a `ReadAuth`. Its trades pages it reads with its own reader, which this module's
//! [`decode_trades_page`] leaves as it is.

use core::fmt;
use core::ops::Range;
use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroU32;
use std::time::Duration;

use fbc_core::{
    CidMatch, ConfigError, DecodeScope, EncodeCtx, HttpMethod, HttpRequest, HttpResponse, Inbound,
    InboundSpans, OpKind, RateCharge, RawFrame, Side, SignError, TrafficClass, VenueConfig,
    VenueOrderId, VenueSymbol, WireSlice, WireUrl,
};
use rust_decimal::Decimal;

use crate::auth::{AccountSelection, ReadAuth, canonical_uuid};
use crate::exec::echo::{self, Envelope};
use crate::exec::json::{self, Kind, Member, Node};
use crate::exec::{self, ErrCode, TRADES_WEIGHT, exact, is_venue_order_id};
use crate::{probe, rate};

/// The account listing's path on the REST origin.
pub const SUBACCOUNTS_PATH: &str = "/user/subaccounts";
/// The positions read's path on the REST origin.
pub const POSITIONS_PATH: &str = "/user/positions";
/// The ended orders read's path on the REST origin.
pub const HISTORIC_ORDERS_PATH: &str = "/user/historic-orders";
/// The largest `get_user_orders` page QFEX documents, and the largest page size the probe sets
/// (decision 0132 item 6). `GET /user/historic-orders` documents the same largest `limit`;
/// `get_user_trades` states none, so its page is bounded as the resync bounds one.
pub const MAX_PAGE: u16 = 1000;
/// What one `get_user_orders` costs, in tenths of QFEX's weight unit: the rate page's 5.0.
pub const PAGE_WEIGHT: NonZeroU32 = NonZeroU32::new(50).expect("fifty is not zero");

/// What a frame that is not one JSON object is refused as.
const NOT_AN_OBJECT: &str = "not a JSON object";

/// One private REST read, as the probe sends it through fbc-runtime: the request, whose signed
/// headers and account selection are all `redact: true`, and what it is charged and sent as.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RestRead {
    pub req: HttpRequest,
    /// `OpKind::Rest`, weight one: QFEX documents no REST limit and no declared bucket counts it
    /// (decisions 0128 and 0132 item 5).
    pub charge: RateCharge,
    pub class: TrafficClass,
}

/// One frame the probe writes on a trade socket, and what it is charged and sent as.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SocketWrite {
    /// The frame, its credentials and account id redaction spans.
    pub frame: WireSlice,
    pub charge: RateCharge,
    pub class: TrafficClass,
}

/// Why a request was not built; never carries a value.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BuildError {
    /// The configuration is not one the probe's guard and decision 0122's check admit.
    Config(ConfigError),
    /// The read could not be signed: the wrong number of nonces, or a time before the epoch.
    Sign(SignError),
    /// A `get_user_orders` page size outside 1 to [`MAX_PAGE`].
    PageSize,
    /// A page size outside 1 to [`MAX_PAGE`] for the read it names: `get_user_trades` or
    /// `GET /user/historic-orders`.
    PageLimit(&'static str),
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::Config(err) => write!(f, "QFEX probe configuration refused: {err}"),
            BuildError::Sign(err) => write!(f, "QFEX account read not signed: {err}"),
            BuildError::PageSize => f.write_str("a get_user_orders page size outside 1 to 1000"),
            BuildError::PageLimit(read) => write!(f, "a {read} page size outside 1 to 1000"),
        }
    }
}

impl std::error::Error for BuildError {}

impl From<ConfigError> for BuildError {
    fn from(err: ConfigError) -> BuildError {
        BuildError::Config(err)
    }
}

impl From<SignError> for BuildError {
    fn from(err: SignError) -> BuildError {
        BuildError::Sign(err)
    }
}

/// The trade socket's URL for `reader`'s credentials, on the trade origin `cfg` names once the
/// probe's guard admits it: its public key in the URL's query, the URL's one redaction span.
pub fn trade_url(cfg: &VenueConfig, reader: &ReadAuth) -> Result<WireUrl, ConfigError> {
    probe::connections(cfg)?;
    reader.opening_url(cfg)
}

/// The authentication of `reader`'s account, the first frame on a trade socket opened with
/// `ctx`, as the order-entry codec authenticates its own: `account_id` only for a subaccount
/// (decision 0122 item 3), charged as one tenth of a weight unit under `Control`, Safety
/// traffic.
pub fn authentication(
    cfg: &VenueConfig,
    reader: &ReadAuth,
    ctx: &EncodeCtx,
) -> Result<SocketWrite, BuildError> {
    probe::connections(cfg)?;
    Ok(SocketWrite {
        frame: reader.frame(ctx)?,
        charge: RateCharge::one(OpKind::Control, None),
        class: TrafficClass::Safety,
    })
}

/// The three subscriptions an order-entry session sends once its authentication succeeded, one
/// frame per channel of `exec::CHANNELS` in that order, each built and charged exactly as the
/// codec builds and charges its own (`exec::subscription`: the rate page's 0.1 under
/// `Subscribe`, Safety traffic): what the probe's liveness hold sends after its authentication
/// succeeded ([`authenticated`]), and nothing after them (decision 0146 item 2). They name no
/// account and carry nothing to redact.
pub fn subscriptions(cfg: &VenueConfig) -> Result<Vec<SocketWrite>, BuildError> {
    probe::connections(cfg)?;
    let write = |channel: &&str| {
        let (frame, charge, class) = exec::subscription(channel);
        SocketWrite {
            frame,
            charge,
            class,
        }
    };
    Ok(exec::CHANNELS.iter().map(write).collect())
}

/// One `get_user_orders` page asked for: its page size, 1 to [`MAX_PAGE`], and its offset.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct PageAsk {
    limit: u16,
    offset: u32,
}

impl PageAsk {
    /// The page of `limit` orders and TWAPs from `offset`; refused, with nothing built, for a
    /// `limit` of zero or above [`MAX_PAGE`].
    pub fn new(limit: u16, offset: u32) -> Result<PageAsk, BuildError> {
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(BuildError::PageSize);
        }
        Ok(PageAsk { limit, offset })
    }

    pub fn limit(&self) -> u16 {
        self.limit
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
}

/// The `get_user_orders` frame for `ask`, for every symbol (none named), sent once the socket's
/// authentication succeeded ([`authenticated`]): `{"type":"get_user_orders","params":{"limit":N,
/// "offset":M}}`, charged [`PAGE_WEIGHT`] under `Query`, Normal traffic.
pub fn user_orders(cfg: &VenueConfig, ask: PageAsk) -> Result<SocketWrite, BuildError> {
    probe::connections(cfg)?;
    Ok(SocketWrite {
        frame: WireSlice::plain(user_orders_body(ask).into_bytes()),
        charge: RateCharge {
            op: OpKind::Query,
            inst: None,
            weight: PAGE_WEIGHT,
        },
        class: TrafficClass::Normal,
    })
}

/// The `get_user_orders` frame for `ask`, as [`user_orders`] and the order-entry codec's resync
/// both write it.
pub(crate) fn user_orders_body(ask: PageAsk) -> String {
    format!(
        r#"{{"type":"get_user_orders","params":{{"limit":{},"offset":{}}}}}"#,
        ask.limit, ask.offset
    )
}

/// One `get_user_trades` page asked for, as the order-entry codec's resync asks for one: from
/// `start_ts`, an inclusive Unix time in seconds, its page size, 1 to [`MAX_PAGE`], and its
/// offset.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TradesAsk {
    start_ts: u64,
    limit: u16,
    offset: u32,
}

impl TradesAsk {
    /// The page of `limit` trades from `offset`, of those at or after `start_ts`; refused, with
    /// nothing built, for a `limit` of zero or above [`MAX_PAGE`].
    pub fn new(start_ts: u64, limit: u16, offset: u32) -> Result<TradesAsk, BuildError> {
        page_limit(limit, "get_user_trades")?;
        Ok(TradesAsk {
            start_ts,
            limit,
            offset,
        })
    }

    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    pub fn limit(&self) -> u16 {
        self.limit
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
}

/// `limit` refused, naming `read`, outside 1 to [`MAX_PAGE`].
fn page_limit(limit: u16, read: &'static str) -> Result<(), BuildError> {
    match (1..=MAX_PAGE).contains(&limit) {
        true => Ok(()),
        false => Err(BuildError::PageLimit(read)),
    }
}

/// The `get_user_trades` frame for `ask`, for every order (none named), sent once the socket's
/// authentication succeeded ([`authenticated`]) on a connection that never arms: the frame the
/// resync writes, `{"type":"get_user_trades","params":{"start_ts":S,"limit":N,"offset":M}}`,
/// charged as the resync charges it, the rate page's 0.5 ([`TRADES_WEIGHT`]) under `Query`, and
/// Normal traffic, as [`user_orders`] is, since no resync sends it.
pub fn user_trades(cfg: &VenueConfig, ask: TradesAsk) -> Result<SocketWrite, BuildError> {
    probe::connections(cfg)?;
    let body = format!(
        r#"{{"type":"get_user_trades","params":{{"start_ts":{},"limit":{},"offset":{}}}}}"#,
        ask.start_ts, ask.limit, ask.offset
    );
    Ok(SocketWrite {
        frame: WireSlice::plain(body.into_bytes()),
        charge: RateCharge {
            op: OpKind::Query,
            inst: None,
            weight: TRADES_WEIGHT,
        },
        class: TrafficClass::Normal,
    })
}

/// `GET /user/subaccounts`, signed with `reader`'s credentials from the nonces and time `ctx`
/// gives: the four signed headers, each `redact: true`, and no account selection, so QFEX reads
/// the provider user from its primary account.
pub fn subaccounts(
    cfg: &VenueConfig,
    reader: &ReadAuth,
    ctx: &EncodeCtx,
) -> Result<RestRead, BuildError> {
    let rest = probe::connections(cfg)?.rest;
    Ok(rest_read(rest, SUBACCOUNTS_PATH, reader.user_headers(ctx)?))
}

/// `GET /user/positions` of `reader`'s account, signed from the nonces and time `ctx` gives:
/// the four signed headers, each `redact: true`, and `x-qfex-requested-account-id`, `redact:
/// true`, only for a subaccount, as decision 0132 item 5's resync read selects one.
pub fn positions(
    cfg: &VenueConfig,
    reader: &ReadAuth,
    ctx: &EncodeCtx,
) -> Result<RestRead, BuildError> {
    let rest = probe::connections(cfg)?.rest;
    Ok(positions_request(rest, reader.rest_headers(ctx)?))
}

/// The one `GET /user/positions` request: on `rest`, a REST origin that already passed the
/// probe's guard (`probe::mode`, which the factory applies to every plan too), with `headers`,
/// the signed headers of the account it reads from either `src/auth` handle, charged as
/// [`positions`] says. [`positions`] builds it for the probe's reads; the order-entry codec's
/// resync builds it for the traded account (FBC-jxau, decision 0132 item 5).
pub(crate) fn positions_request(rest: &str, headers: Vec<fbc_core::Header>) -> RestRead {
    rest_read(rest, POSITIONS_PATH, headers)
}

/// One `GET /user/historic-orders` page asked for: its page size, 1 to [`MAX_PAGE`], and its
/// offset.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct HistoricAsk {
    limit: u16,
    offset: u32,
}

impl HistoricAsk {
    /// The page of `limit` ended orders from `offset`; refused, with nothing built, for a
    /// `limit` of zero or above [`MAX_PAGE`].
    pub fn new(limit: u16, offset: u32) -> Result<HistoricAsk, BuildError> {
        page_limit(limit, "historic-orders")?;
        Ok(HistoricAsk { limit, offset })
    }

    pub fn limit(&self) -> u16 {
        self.limit
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
}

/// `GET /user/historic-orders?limit=N&offset=M` of `reader`'s account, every order it lists
/// (no filter named), signed from the nonces and time `ctx` gives, as [`positions`] is: the
/// four signed headers, each `redact: true`, and `x-qfex-requested-account-id`, `redact: true`,
/// only for a subaccount.
pub fn historic_orders(
    cfg: &VenueConfig,
    reader: &ReadAuth,
    ctx: &EncodeCtx,
    ask: HistoricAsk,
) -> Result<RestRead, BuildError> {
    let rest = probe::connections(cfg)?.rest;
    let path = format!(
        "{HISTORIC_ORDERS_PATH}?limit={}&offset={}",
        ask.limit, ask.offset
    );
    Ok(rest_read(rest, &path, reader.rest_headers(ctx)?))
}

fn rest_read(rest: &str, path: &str, headers: Vec<fbc_core::Header>) -> RestRead {
    RestRead {
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(format!("{rest}{path}")),
            headers,
            body: WireSlice::plain(Vec::new()),
        },
        charge: RateCharge::one(OpKind::Rest, None),
        class: TrafficClass::Normal,
    }
}

/// The spans of `input`, a frame off one of `reader`'s trade sockets or an answer to one of its
/// REST reads, that the journal and any evidence keep only as keyed hashes:
///
/// - what `src/auth` names (`ReadAuth::inbound_spans`): every listed account id, every
///   `user_id`, every value of a `balance` member (the one `GET /user/positions` answers), the
///   account's public key and subaccount id wherever they appear, and the rest (decisions 0028
///   and 0122 item 3);
/// - for a frame that can echo a request, decision 0134's spans in place of those, as the
///   order-entry codec redacts one on an epoch with no arm (0126, as 0134 narrows it);
/// - for a REST answer whose status is outside 2xx, its whole body: nothing decodes it.
///
/// The balance values covered are the positions answer's. A trade socket's `balance_response`
/// names them by other members (`deposit`, `available_balance` and the rest), which are not
/// spans, as the order-entry codec's journal does not span them either (FBC-77aa (e) names the
/// account's identifiers, not its amounts). The probe's read sockets subscribe to no channel,
/// so none is expected on one. The liveness hold's connection subscribes to the three a session
/// does (decision 0146), so one may arrive on it, and keeps only its `id` and `user_id` as keyed
/// hashes; the hold counts its frames and keeps, prints and writes none of them.
pub fn redact_inbound(reader: &ReadAuth, input: Inbound<'_>) -> InboundSpans {
    let account = reader.inbound_spans(input);
    match input {
        Inbound::Frame(frame) => InboundSpans::frame(exec::frame_spans(account.body(), frame)),
        Inbound::Http(_, resp) if !success(resp.status) => {
            InboundSpans::response(Vec::new(), whole(resp.body))
        }
        Inbound::Http(..) => account,
    }
}

pub(crate) fn success(status: u16) -> bool {
    (200..300).contains(&status)
}

/// One span over all of `body`, or none when it is empty.
pub(crate) fn whole(body: &[u8]) -> Vec<Range<u32>> {
    let end = u32::try_from(body.len()).unwrap_or(u32::MAX);
    (end > 0).then_some(0..end).into_iter().collect()
}

/// What the journal keeps of a text frame, as text; `None` for a binary frame.
fn kept(reader: &ReadAuth, frame: RawFrame<'_>) -> Option<String> {
    let spans = redact_inbound(reader, Inbound::Frame(frame));
    match frame {
        RawFrame::Text(_) => String::from_utf8(json::blank(frame.bytes(), spans.body())).ok(),
        RawFrame::Binary(_) => None,
    }
}

/// Whether `frame` is QFEX's reply that `reader`'s socket is authenticated: exactly where the
/// order-entry codec takes a reply as success (`exec::auth_success`), read from the bytes the
/// journal keeps. A binary frame, one that is not a JSON object or repeats a member, an err, an
/// ack, and every other frame are not success, so the page is asked only after success.
pub fn authenticated(reader: &ReadAuth, frame: RawFrame<'_>) -> bool {
    let Some(text) = kept(reader, frame) else {
        return false;
    };
    json::parse(&text).is_some_and(|root| {
        !root.repeats()
            && root.members().is_some_and(|top| {
                matches!(echo::envelope(top), Envelope::Other) && exec::auth_success(top)
            })
    })
}

/// Which of `exec::CHANNELS` `frame`, off `reader`'s trade socket, confirms: a
/// `{"subscribed":"<channel>"}` reply, the frame's one member, naming one of them, exactly where
/// the order-entry codec takes a frame as a confirmation (`exec::channel_reply`), read from the
/// bytes the journal keeps. A binary frame, one that is not a JSON object or repeats a member,
/// an err or an ack, an `{"unsubscribed":…}` reply, a channel the codec never subscribes, and
/// every other frame confirm nothing.
pub fn confirmation(reader: &ReadAuth, frame: RawFrame<'_>) -> Option<usize> {
    let text = kept(reader, frame)?;
    let root = json::parse(&text).filter(|root| !root.repeats())?;
    let top = root.members()?;
    match (echo::envelope(top), exec::channel_reply(top)) {
        (Envelope::Other, Some(("subscribed", at))) => Some(at),
        _ => None,
    }
}

/// An err off one of the probe's trade sockets, read as [`orders_page`] reads one.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SocketErr {
    /// A RateLimited err: the shared budget to hold.
    RateLimited(RateHold),
    /// Any other err: its documented code, or `None` for one absent, spanned or undocumented.
    Other(Option<ErrCode>),
}

impl fmt::Display for SocketErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SocketErr::RateLimited(_) => f.write_str("QFEX answered an err: RateLimited"),
            SocketErr::Other(Some(code)) => write!(f, "QFEX answered an err: {}", code.wire()),
            SocketErr::Other(None) => f.write_str("QFEX answered an undocumented err"),
        }
    }
}

impl std::error::Error for SocketErr {}

impl From<SocketErr> for NotAPage {
    fn from(err: SocketErr) -> NotAPage {
        match err {
            SocketErr::RateLimited(hold) => NotAPage::RateLimited(hold),
            SocketErr::Other(code) => NotAPage::Err(code),
        }
    }
}

/// The err `frame`, off `reader`'s trade socket, is, read from the bytes the journal keeps as
/// [`orders_page`] reads one: a RateLimited one's hold, which the caller applies to the shared
/// provider-user limiter (decision 0126), or its code. `None` for every frame in no err
/// envelope: a binary frame, one that is not a JSON object or repeats a member, an ack, and the
/// rest. An err the journal keeps whole (decision 0134 item 2) reads as no JSON object, so as
/// `None`, as it holds no budget in a session (0134 item 6).
pub fn socket_err(reader: &ReadAuth, frame: RawFrame<'_>) -> Option<SocketErr> {
    let text = kept(reader, frame)?;
    let root = json::parse(&text).filter(|root| !root.repeats())?;
    match echo::envelope(root.members()?) {
        Envelope::FlatErr(body) | Envelope::NestedErr(body) => Some(refusal(echo::err(body))),
        _ => None,
    }
}

// ----- The account listing -----

/// Why a REST answer was refused; names a member or a status, never a value.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AnswerError {
    /// A status outside 2xx; its body is not read.
    Status(u16),
    /// Not one JSON object in UTF-8, or a member named twice in an object.
    Malformed,
    /// A required member is missing.
    Missing(&'static str),
    /// A top-level member QFEX does not document.
    Undocumented,
    /// A member's value is not as documented.
    Invalid(&'static str),
    /// An account id or a symbol listed twice.
    Repeated(&'static str),
}

impl fmt::Display for AnswerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AnswerError::Status(status) => write!(f, "QFEX answered HTTP {status}"),
            AnswerError::Malformed => f.write_str("QFEX's answer is not one JSON object"),
            AnswerError::Missing(member) => write!(f, "QFEX's answer has no {member}"),
            AnswerError::Undocumented => f.write_str("QFEX's answer has an undocumented member"),
            AnswerError::Invalid(member) => write!(f, "QFEX's answer has an invalid {member}"),
            AnswerError::Repeated(member) => write!(f, "QFEX's answer repeats a {member}"),
        }
    }
}

impl std::error::Error for AnswerError {}

/// The subaccounts `GET /user/subaccounts` listed: none, or each a canonical lowercase UUID,
/// none repeated. The primary account is never listed. `Debug` shows how many, never an id.
#[derive(Clone, Eq, PartialEq)]
pub struct Subaccounts {
    ids: Vec<String>,
}

impl Subaccounts {
    /// How many subaccounts were listed.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether none was listed.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Every account of the provider user the listing shows (decision 0132 item 7 had the probe
    /// read each; it reads the accounts its configuration names, decision 0144): the primary
    /// account first, which the listing leaves out, then each listed subaccount in the listing's
    /// order.
    pub fn accounts(&self) -> impl Iterator<Item = AccountSelection<'_>> {
        core::iter::once(AccountSelection::Primary).chain(
            self.ids
                .iter()
                .map(|id| AccountSelection::Subaccount(id.as_str())),
        )
    }

    /// Whether `account`, decision 0122's selection of an account to trade, is the primary
    /// account or a listed subaccount (decision 0132 item 7; the probe no longer asks, decision
    /// 0144).
    pub fn admits(&self, account: AccountSelection<'_>) -> bool {
        match account {
            AccountSelection::Primary => true,
            AccountSelection::Subaccount(id) => self.ids.iter().any(|listed| listed == id),
        }
    }
}

impl fmt::Debug for Subaccounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subaccounts")
            .field("listed", &self.ids.len())
            .finish()
    }
}

/// The answer to [`subaccounts`]: `account_ids`, an array of strings or null (null and `[]`
/// both list none), beside an optional `$schema` string and nothing else. Each id must be a
/// canonical lowercase UUID, not the nil one, as decision 0122 selects a subaccount by; one
/// listed twice is refused.
///
/// It decodes the live answer only. The journal keeps each listed id as a keyed hash, read back
/// as 36 `2`s, dashes included, which is no UUID: a listing replayed from the journal is
/// refused, `AnswerError::Invalid("account_ids")`, where live it was decoded. The positions
/// answer reads no member the journal blanks, so it decodes the same from the journal as live.
pub fn decode_subaccounts(resp: &HttpResponse<'_>) -> Result<Subaccounts, AnswerError> {
    let root = body(resp)?;
    let top = documented(&root, &["account_ids"])?;
    let mut ids = Vec::new();
    for item in nullable_array(top, "account_ids")? {
        match item.str() {
            Some(id) if canonical_uuid(id) => ids.push(id.to_owned()),
            _ => return Err(AnswerError::Invalid("account_ids")),
        }
    }
    unique(&ids, "account id")?;
    Ok(Subaccounts { ids })
}

// ----- The positions -----

/// One symbol's row of `GET /user/positions`: its position, exactly as written, and its
/// `open_orders`, kept as evidence for decision 0132 item 5's cross-check and not judged here.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct PositionRow {
    pub symbol: VenueSymbol,
    pub position: Decimal,
    pub open_orders: u64,
}

/// What `GET /user/positions` showed of one account: its rows, no symbol twice. The balance QFEX
/// answers beside them is required, but none of its values is kept, so no report or error can
/// carry one (decision 0009).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Positions {
    rows: Vec<PositionRow>,
}

impl Positions {
    pub fn rows(&self) -> &[PositionRow] {
        &self.rows
    }

    /// Whether every row's position is exactly zero; no row is flat too.
    pub fn is_flat(&self) -> bool {
        self.rows.iter().all(|row| row.position.is_zero())
    }
}

/// The answer to [`positions`]: `positions`, an array of rows or null (null lists none), a
/// required `balance` object, whose values are not read, and an optional `$schema` string, and
/// nothing else. Each row names a `symbol`, listed once, its `position`, a JSON number read
/// exactly, and `open_orders`, a whole number of at least zero.
pub fn decode_positions(
    resp: &HttpResponse<'_>,
    scope: &DecodeScope<'_>,
) -> Result<Positions, AnswerError> {
    let root = body(resp)?;
    let top = documented(&root, &["balance", "positions"])?;
    if required(top, "balance")?.members().is_none() {
        return Err(AnswerError::Invalid("balance"));
    }
    let mut rows = Vec::new();
    for row in nullable_array(top, "positions")? {
        let row = row.members().ok_or(AnswerError::Invalid("positions"))?;
        let symbol = required(row, "symbol")?
            .str()
            .and_then(|symbol| scope.venue_symbol(symbol).ok())
            .ok_or(AnswerError::Invalid("symbol"))?;
        let position =
            number(required(row, "position")?).ok_or(AnswerError::Invalid("position"))?;
        let open_orders =
            count_of(required(row, "open_orders")?).ok_or(AnswerError::Invalid("open_orders"))?;
        rows.push(PositionRow {
            symbol,
            position,
            open_orders,
        });
    }
    let symbols: Vec<&VenueSymbol> = rows.iter().map(|row| &row.symbol).collect();
    unique(&symbols, "symbol")?;
    Ok(Positions { rows })
}

// ----- The ended orders -----

/// One order `GET /user/historic-orders` listed: its `client_order_id` and its
/// `terminal_status`, each exactly as written. QFEX lists no `terminal_status` value (decision
/// 0132's context), so it is kept as text, for evidence, and not judged here.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct HistoricOrder {
    pub client_order_id: String,
    pub terminal_status: String,
}

/// What one `GET /user/historic-orders` page showed: its orders, in its order, and the `count`
/// it states beside them.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct HistoricOrders {
    rows: Vec<HistoricOrder>,
    count: u64,
}

impl HistoricOrders {
    pub fn rows(&self) -> &[HistoricOrder] {
        &self.rows
    }

    /// The answer's `count`, as stated.
    pub fn count(&self) -> u64 {
        self.count
    }
}

/// The answer to [`historic_orders`]: a required `count`, a whole number of at least zero, a
/// required `data`, an array of orders or null (null lists none), and an optional `$schema`
/// string, and nothing else. Each order must carry `client_order_id` and `terminal_status` as
/// strings; its other members are not read. Nothing it reads is a member the journal blanks, so
/// it decodes the same from the journal as live.
pub fn decode_historic(resp: &HttpResponse<'_>) -> Result<HistoricOrders, AnswerError> {
    let root = body(resp)?;
    let top = documented(&root, &["count", "data"])?;
    let count = count_of(required(top, "count")?).ok_or(AnswerError::Invalid("count"))?;
    let mut rows = Vec::new();
    for row in nullable_array(top, "data")? {
        let row = row.members().ok_or(AnswerError::Invalid("data"))?;
        let text = |key: &'static str| match required(row, key)?.str() {
            Some(text) => Ok(text.to_owned()),
            None => Err(AnswerError::Invalid(key)),
        };
        rows.push(HistoricOrder {
            client_order_id: text("client_order_id")?,
            terminal_status: text("terminal_status")?,
        });
    }
    Ok(HistoricOrders { rows, count })
}

/// A 2xx answer's body as one JSON object naming no member twice.
fn body<'a>(resp: &HttpResponse<'a>) -> Result<Node<'a>, AnswerError> {
    if !success(resp.status) {
        return Err(AnswerError::Status(resp.status));
    }
    core::str::from_utf8(resp.body)
        .ok()
        .and_then(json::parse)
        .filter(|root| root.members().is_some() && !root.repeats())
        .ok_or(AnswerError::Malformed)
}

/// The top-level members of `root`, each one of `members` or an optional `$schema` string.
fn documented<'n, 'a>(
    root: &'n Node<'a>,
    members: &[&str],
) -> Result<&'n [Member<'a>], AnswerError> {
    let top = root.members().unwrap_or_default();
    for (name, value) in top {
        match name.as_ref() {
            "$schema" if value.str().is_none() => return Err(AnswerError::Invalid("$schema")),
            "$schema" => {}
            name if members.contains(&name) => {}
            _ => return Err(AnswerError::Undocumented),
        }
    }
    Ok(top)
}

fn required<'n, 'a>(
    members: &'n [Member<'a>],
    key: &'static str,
) -> Result<&'n Node<'a>, AnswerError> {
    json::get(members, key).ok_or(AnswerError::Missing(key))
}

/// The items of the array `key` names, none for null.
fn nullable_array<'n, 'a>(
    members: &'n [Member<'a>],
    key: &'static str,
) -> Result<&'n [Node<'a>], AnswerError> {
    let node = required(members, key)?;
    match node.kind {
        Kind::Literal("null") => Ok(&[]),
        _ => node.items().ok_or(AnswerError::Invalid(key)),
    }
}

fn unique<T: Eq + core::hash::Hash>(items: &[T], what: &'static str) -> Result<(), AnswerError> {
    let mut seen = HashSet::new();
    match items.iter().all(|item| seen.insert(item)) {
        true => Ok(()),
        false => Err(AnswerError::Repeated(what)),
    }
}

/// A JSON number, exactly as written.
fn number(node: &Node<'_>) -> Option<Decimal> {
    match node.kind {
        Kind::Num(raw) => exact(raw),
        _ => None,
    }
}

/// A JSON number that is a whole number of at least zero.
fn count_of(node: &Node<'_>) -> Option<u64> {
    number(node)
        .filter(|n| n.fract().is_zero())
        .and_then(|n| u64::try_from(n).ok())
}

/// `BUY` or `SELL`, the enums page's OrderDirection as the frames write it.
fn side_of(text: &str) -> Option<Side> {
    match text {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => None,
    }
}

// ----- The get_user_orders page -----

/// One order an `all_orders_response` page listed, read exactly.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct PageOrder {
    /// By the crate's one venue order-id rule (FBC-s8wq).
    pub order: VenueOrderId,
    pub symbol: VenueSymbol,
    pub side: Side,
    pub quantity: Decimal,
    /// At most `quantity`.
    pub remaining: Decimal,
    /// The `client_order_id`, decoded under the venue's declared format against our namespace.
    pub client: CidMatch,
}

/// One `all_orders_response` page: its orders, an order listed twice kept for the caller to
/// deduplicate (decision 0132 item 5), and how many TWAPs it listed beside them.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OrdersPage {
    orders: Vec<PageOrder>,
    twaps: usize,
    ask: PageAsk,
}

impl OrdersPage {
    pub fn orders(&self) -> &[PageOrder] {
        &self.orders
    }

    /// How many TWAPs the page listed.
    pub fn twaps(&self) -> usize {
        self.twaps
    }

    /// The page asked for.
    pub fn ask(&self) -> PageAsk {
        self.ask
    }

    /// Whether the page listed nothing: no order and no TWAP.
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty() && self.twaps == 0
    }

    /// Whether it listed as many orders and TWAPs together as its page size: another page may
    /// follow (decision 0132 item 5).
    pub fn is_full(&self) -> bool {
        self.orders.len() + self.twaps == usize::from(self.ask.limit)
    }
}

/// The hold a RateLimited err asks for, as the order-entry codec reads one (`src/rate.rs`,
/// decisions 0126 and 0134 item 5): the bucket of the echo's mapped `type`, `None` to hold every
/// frame budget, and the hint of a kept `message`, `None` to hold at least the declared window.
/// The caller applies it to the shared provider-user limiter and settles nothing.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateHold {
    pub charge: Option<RateCharge>,
    pub retry_after: Option<Duration>,
}

/// Why a frame is no page: never an empty one.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum NotAPage {
    /// A RateLimited err: the shared budget to hold.
    RateLimited(RateHold),
    /// Any other err: its documented code, or `None` for one absent, spanned or undocumented.
    Err(Option<ErrCode>),
    /// Any other frame; names what was wrong, never a value.
    Other(&'static str),
}

impl fmt::Display for NotAPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotAPage::RateLimited(_) => f.write_str("QFEX refused the page read: RateLimited"),
            NotAPage::Err(Some(code)) => write!(f, "QFEX answered an err: {}", code.wire()),
            NotAPage::Err(None) => f.write_str("QFEX answered an undocumented err"),
            NotAPage::Other(what) => write!(f, "not a get_user_orders page: {what}"),
        }
    }
}

impl std::error::Error for NotAPage {}

/// The page `frame`, off `reader`'s trade socket, answers `ask` with: one `all_orders_response`
/// holding exactly an `orders` and a `twaps` array, at most `ask`'s page size together, each
/// order row naming its `order_id` (by the crate's one rule), `symbol`, `side` (`BUY` or
/// `SELL`), a positive `quantity`, a `quantity_remaining` from zero to `quantity` and a string
/// `client_order_id`, each TWAP an object. It is read from the bytes the journal keeps, so a
/// row's `user_id` is never read. An err, a RateLimited one included, an ack, and any other frame
/// are no page.
pub fn orders_page(
    reader: &ReadAuth,
    frame: RawFrame<'_>,
    ask: PageAsk,
    scope: &DecodeScope<'_>,
) -> Result<OrdersPage, NotAPage> {
    let text = kept(reader, frame).ok_or(NotAPage::Other("not a text frame"))?;
    decode_page(&text, ask, scope)
}

/// The page `text` answers `ask` with, as [`orders_page`] reads it, where `text` is what the
/// journal keeps of a text frame off the trade socket, its spans blanked by whichever `src/auth`
/// handle's socket it came off. The order-entry codec's resync reads its pages' shape through
/// [`page_rows`] instead, since it reads more of each row.
pub(crate) fn decode_page(
    text: &str,
    ask: PageAsk,
    scope: &DecodeScope<'_>,
) -> Result<OrdersPage, NotAPage> {
    read_frame(text, |top| page(top, ask, scope)).map_err(|why| match why {
        NotRead::Err(err) => err.into(),
        NotRead::Other(what) => NotAPage::Other(what),
    })
}

/// Why a frame is not the answer a read on the trade socket asked for.
enum NotRead {
    Err(SocketErr),
    Other(&'static str),
}

/// What `read` makes of `text`, the bytes the journal keeps of a text frame, once it is one JSON
/// object naming no member twice in no err or ack envelope: an err is read as [`socket_err`]
/// reads one, and an ack, two envelopes and any other frame are refused, naming what was wrong.
fn read_frame<T>(
    text: &str,
    read: impl FnOnce(&[Member<'_>]) -> Result<T, &'static str>,
) -> Result<T, NotRead> {
    let root = json::parse(text).ok_or(NotRead::Other(NOT_AN_OBJECT))?;
    let top = root.members().ok_or(NotRead::Other(NOT_AN_OBJECT))?;
    if root.repeats() {
        return Err(NotRead::Other("a repeated member"));
    }
    match echo::envelope(top) {
        Envelope::FlatErr(body) | Envelope::NestedErr(body) => {
            Err(NotRead::Err(refusal(echo::err(body))))
        }
        Envelope::Ack(_) => Err(NotRead::Other("an ack")),
        Envelope::Ambiguous => Err(NotRead::Other("two envelopes")),
        Envelope::Other => read(top).map_err(NotRead::Other),
    }
}

/// What an err is: a RateLimited one's hold, read as `src/rate.rs` reads it, or its code.
fn refusal(err: echo::Err<'_>) -> SocketErr {
    match err.code {
        Some(ErrCode::RateLimited) => SocketErr::RateLimited(RateHold {
            charge: err
                .echo_type
                .and_then(rate::operation)
                .map(|op| RateCharge::one(op, None)),
            retry_after: err.message.and_then(rate::hint),
        }),
        code => SocketErr::Other(code),
    }
}

fn page(
    top: &[Member<'_>],
    ask: PageAsk,
    scope: &DecodeScope<'_>,
) -> Result<OrdersPage, &'static str> {
    let (rows, twaps) = page_rows(top, ask.limit)?;
    let orders = rows
        .iter()
        .map(|row| order(row, scope))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OrdersPage {
        orders,
        twaps: twaps.len(),
        ask,
    })
}

/// The rows of the page `top`, a frame's top-level members, lists: one `all_orders_response`
/// holding exactly an `orders` array and a `twaps` array of objects, at most `limit` together.
/// A `stop_orders` member, or any other, is not documented on the get_user_orders page and is
/// refused, as a page it would leave partial. The orders' rows and the TWAPs, each unread.
pub(crate) fn page_rows<'n, 'a>(
    top: &'n [Member<'a>],
    limit: u16,
) -> Result<(&'n [Node<'a>], &'n [Node<'a>]), &'static str> {
    let page = match top {
        [(name, body)] if name == "all_orders_response" => body.members(),
        _ => None,
    }
    .ok_or("not one all_orders_response")?;
    if page
        .iter()
        .any(|(name, _)| name != "orders" && name != "twaps")
    {
        return Err("an undocumented page member");
    }
    let rows = json::get(page, "orders")
        .and_then(Node::items)
        .ok_or("orders")?;
    let twaps = json::get(page, "twaps")
        .and_then(Node::items)
        .filter(|twaps| twaps.iter().all(|twap| twap.members().is_some()))
        .ok_or("twaps")?;
    if rows.len() + twaps.len() > usize::from(limit) {
        return Err("more orders and TWAPs than the page size");
    }
    Ok((rows, twaps))
}

fn order(row: &Node<'_>, scope: &DecodeScope<'_>) -> Result<PageOrder, &'static str> {
    let row = row.members().ok_or("an order row")?;
    let text = |key: &'static str| json::get(row, key).and_then(Node::str).ok_or(key);
    let order = Some(text("order_id")?)
        .filter(|id| is_venue_order_id(id))
        .and_then(|id| scope.venue_order_id(id).ok())
        .ok_or("order_id")?;
    let symbol = scope.venue_symbol(text("symbol")?).or(Err("symbol"))?;
    let side = side_of(text("side")?).ok_or("side")?;
    let amount = |key| json::get(row, key).and_then(number).ok_or(key);
    let quantity = Some(amount("quantity")?)
        .filter(|n| *n > Decimal::ZERO)
        .ok_or("quantity")?;
    let remaining = Some(amount("quantity_remaining")?)
        .filter(|n| *n >= Decimal::ZERO && *n <= quantity)
        .ok_or("quantity_remaining")?;
    Ok(PageOrder {
        order,
        symbol,
        side,
        quantity,
        remaining,
        client: scope.client_order_id(text("client_order_id")?),
    })
}

// ----- The get_user_trades page -----

/// Which envelope a trades page came in, since QFEX's two sources name it differently (decision
/// 0132's context), so the report says which one QFEX sent.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TradesEnvelope {
    /// `user_trades`, beside a whole `count`, as the get_user_trades page documents it.
    UserTrades,
    /// `user_trades_response`, a `count` beside it optional, as the trade AsyncAPI gives it.
    UserTradesResponse,
}

impl TradesEnvelope {
    /// The member that names it.
    pub fn name(self) -> &'static str {
        match self {
            TradesEnvelope::UserTrades => "user_trades",
            TradesEnvelope::UserTradesResponse => "user_trades_response",
        }
    }
}

/// One row of a `get_user_trades` page, as [`decode_trades_page`] reads it: each field below
/// as QFEX's pages show it when the row carries it so, `None` when the row leaves it out or
/// carries it otherwise, and the name of every member the row carries, so the caller tells the
/// two apart and the report says which fields a row carries (decision 0132's consequences). No
/// value of any other member is kept.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TradeRow {
    /// A string.
    pub trade_id: Option<String>,
    /// A string.
    pub order_id: Option<String>,
    /// A string, as the get_user_trades page's example rows carry it.
    pub client_order_id: Option<String>,
    /// `BUY` or `SELL`, as the AsyncAPI's `TradePayload` carries it.
    pub side: Option<Side>,
    /// `BUY` or `SELL`, as the get_user_trades page's example rows carry it.
    pub aggressor_side: Option<Side>,
    /// A number, exactly as written: Unix seconds in the page's example; no page states what
    /// kind of time it is.
    pub timestamp: Option<Decimal>,
    /// A number, exactly as written, of unstated currency and sign.
    pub fee: Option<Decimal>,
    /// Every member name the row carries, its value never.
    pub members: BTreeSet<String>,
}

impl TradeRow {
    /// Whether the row carries a member named `name`, whatever its value.
    pub fn carries(&self, name: &str) -> bool {
        self.members.contains(name)
    }
}

/// One `get_user_trades` page: the envelope it came in, its rows, in its order, a trade listed
/// twice kept for the caller to deduplicate, and the `count` it states beside them, when it
/// states one.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TradesPage {
    envelope: TradesEnvelope,
    rows: Vec<TradeRow>,
    count: Option<u64>,
}

impl TradesPage {
    pub fn envelope(&self) -> TradesEnvelope {
        self.envelope
    }

    pub fn rows(&self) -> &[TradeRow] {
        &self.rows
    }

    /// The page's `count`, as stated; `None` when it states none, as the AsyncAPI's need not.
    pub fn count(&self) -> Option<u64> {
        self.count
    }
}

/// Why a frame is no trades page: never one whose rows leave a field out.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum NotATradesPage {
    /// An err, as [`socket_err`] reads one: a RateLimited one's hold, or its code.
    Err(SocketErr),
    /// Any other frame; names what was wrong, never a value.
    Other(&'static str),
}

impl fmt::Display for NotATradesPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotATradesPage::Err(err) => err.fmt(f),
            NotATradesPage::Other(what) => write!(f, "not a get_user_trades page: {what}"),
        }
    }
}

impl std::error::Error for NotATradesPage {}

/// The page `frame`, off `reader`'s trade socket, is: exactly one of the two envelopes
/// [`TradesEnvelope`] names, as the resync reads both, holding an array of objects, with a
/// `count` beside it, a whole number of at least zero, which `user_trades` requires and
/// `user_trades_response` may leave out, and no other member. Each row is read as [`TradeRow`]
/// says, a row missing a field or carrying it otherwise read, not refused; the order-entry
/// codec's resync reads its own pages with its own reader, which this leaves as it is. It is
/// read from the bytes the journal keeps, so no member the journal blanks is read. An err, a
/// RateLimited one included, an ack, a frame naming both envelopes or neither, and any other
/// frame are no page.
pub fn decode_trades_page(
    reader: &ReadAuth,
    frame: RawFrame<'_>,
) -> Result<TradesPage, NotATradesPage> {
    let text = kept(reader, frame).ok_or(NotATradesPage::Other("not a text frame"))?;
    read_frame(&text, trades).map_err(|why| match why {
        NotRead::Err(err) => NotATradesPage::Err(err),
        NotRead::Other(what) => NotATradesPage::Other(what),
    })
}

fn trades(top: &[Member<'_>]) -> Result<TradesPage, &'static str> {
    let documented = json::get(top, TradesEnvelope::UserTrades.name());
    let asyncapi = json::get(top, TradesEnvelope::UserTradesResponse.name());
    let (envelope, rows) = match (documented, asyncapi) {
        (Some(rows), None) => (TradesEnvelope::UserTrades, rows),
        (None, Some(rows)) => (TradesEnvelope::UserTradesResponse, rows),
        (Some(_), Some(_)) => return Err("two trades envelopes"),
        (None, None) => return Err("no trades envelope"),
    };
    if top
        .iter()
        .any(|(name, _)| name != envelope.name() && name != "count")
    {
        return Err("a member beside the trades envelope");
    }
    let rows = rows
        .items()
        .ok_or("a trades envelope that is not an array")?;
    let count = match (json::get(top, "count"), envelope) {
        (None, TradesEnvelope::UserTrades) => return Err("count"),
        (None, TradesEnvelope::UserTradesResponse) => None,
        (Some(count), _) => Some(count_of(count).ok_or("count")?),
    };
    let rows = rows
        .iter()
        .map(|row| {
            row.members()
                .map(trade_row)
                .ok_or("a row that is not an object")
        })
        .collect::<Result<_, _>>()?;
    Ok(TradesPage {
        envelope,
        rows,
        count,
    })
}

fn trade_row(row: &[Member<'_>]) -> TradeRow {
    let text = |key: &str| json::get(row, key).and_then(Node::str);
    let owned = |key: &str| text(key).map(str::to_owned);
    let amount = |key: &str| json::get(row, key).and_then(number);
    TradeRow {
        trade_id: owned("trade_id"),
        order_id: owned("order_id"),
        client_order_id: owned("client_order_id"),
        side: text("side").and_then(side_of),
        aggressor_side: text("aggressor_side").and_then(side_of),
        timestamp: amount("timestamp"),
        fee: amount("fee"),
        members: row.iter().map(|(name, _)| name.to_string()).collect(),
    }
}
