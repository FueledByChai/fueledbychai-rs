//! The QFEX model the place stages' tests run against (FBC-c6kw, FBC-23ch, FBC-77r8; story
//! BT-702): QFEX's REST reads, its trade sockets, sessions and reads alike, and its MDS touch,
//! each answered as QFEX's pages show them, on the conformance stub's serving scripts (decision
//! 0150), since the place stages' connections, several at once, send what no timeline can know.
//! fbc-conformance stays venue-free: the model is this test's own. Later slices extend it with
//! crossings, IOCs and paging (FBC-su9i and after).
//!
//! - **State** ([`QfexModel`], behind `Arc<Mutex<..>>`): the instruments `/refdata` lists (or,
//!   with [`QfexModel::refdata`], the status and body it answers in their place), each
//!   account's positions with a lag knob (each account's first [`QfexModel::lag`] reads answer
//!   its stale body), the trade connections, orders and fills, the touch, and a log of every
//!   request and connection in arrival order, each with whether the probe had printed its `CAPS`
//!   line by then and, for a trade socket's authentication, whether it opened alone.
//! - **REST** ([`QfexModel::rest`]): `GET /refdata`, `GET /user/positions` by the
//!   `x-qfex-requested-account-id` header (`None` for the primary account),
//!   `GET /user/subaccounts` refused HTTP 403 as QFEX refused a key scoped to one account
//!   (FBC-291a's first production run; decision 0144), and `GET /user/historic-orders`, each
//!   order of the account that ended, with its `terminal_status`.
//! - **Trade sockets** ([`QfexModel::trade`], FBC-23ch): each connection's account learned from
//!   its authentication's `account_id` (`None` for the primary account), which is answered (or
//!   left unanswered with [`QfexModel::reads_silent`]); the three subscriptions confirmed, which
//!   make the connection a session's (a read socket subscribes to nothing); the
//!   cancel-on-disconnect arm acknowledged by its echo; `add_order` answered with an
//!   `order_response` `ACK` naming the order the model rests and echoing its client id byte for
//!   byte; `cancel_order` with a `CANCELLED` one, or `NO_SUCH_ORDER` for an order not open; and
//!   `get_user_orders` and `get_user_trades` pages of the connection's account, each from its
//!   offset. Any other frame is refused, failing that answer as the serving script reports it.
//!   An armed connection's account has every open order cancelled once the connection closed
//!   gracefully, with [`QfexModel::cod_on_close`], or dropped, with [`QfexModel::cod_on_drop`]
//!   (account-wide, decision 0121), seen at the model's next answer; with [`QfexModel::cod_reach`]
//!   at [`Reach::Own`], only the orders placed on that connection (FBC-77r8). Those three are
//!   the cod knob. The arm itself cancels every open order of its account with
//!   [`QfexModel::arm_cancels_open`], each reported by a `CANCELLED` `order_response` before
//!   the arm's ack, reported late with the next place's answer, or not reported at all
//!   ([`ArmCancels`], FBC-77r8; QFEX's note says enabling cancels, decision 0132 item 2).
//! - **Positions** follow the model's own orders and fills: an account they touched is answered
//!   its position on [`SYMBOL`] and its open orders, every other one its body in
//!   [`QfexModel::positions`].
//! - **Knobs** (FBC-23ch), each acting once, on the first `add_order` or `cancel_order` of the
//!   run, or on the reads made after it: [`QfexModel::ack_delay`], [`QfexModel::rate_limit`]
//!   ([`Limited`]), [`QfexModel::stray_order`], [`QfexModel::cid_absent_update`],
//!   [`QfexModel::stray_fill`], [`QfexModel::maker_fill`], [`QfexModel::server_close`],
//!   [`QfexModel::close_on_place`], [`QfexModel::arm_err`],
//!   [`QfexModel::rest_lag`], [`QfexModel::rest_fails`], [`QfexModel::cancel_delay`],
//!   [`QfexModel::foreign_order`], [`QfexModel::position_on_place`] and
//!   [`QfexModel::position_after_close`], [`QfexModel::foreign_after_close`] (FBC-77r8),
//!   [`QfexModel::cancel_place`],
//!   [`QfexModel::venue_cancel`],
//!   [`QfexModel::close_on_cancel`], [`QfexModel::stray_after`],
//!   [`QfexModel::part_fill_after`], [`QfexModel::close_after`],
//!   [`QfexModel::fill_on_trades`] and [`QfexModel::fill_on_trades_page`]; as the first graceful
//!   close of an armed connection is settled, [`QfexModel::fill_after_close`],
//!   [`QfexModel::part_fill_after_close`], [`QfexModel::trade_after_close`] and
//!   [`QfexModel::netting_after_close`]; as each drop's cancel-on-disconnect cancels,
//!   [`QfexModel::drop_lag`] (FBC-su9i); at the first
//!   trade-socket authentication, [`QfexModel::touch_moves`]; on every read socket,
//!   [`QfexModel::auth_fails`], [`QfexModel::stuck_pages`], [`QfexModel::stuck_trades`],
//!   [`QfexModel::trades_err`] and [`QfexModel::hold_trades`], and on every read socket's pages
//!   [`QfexModel::newest_first`], after the first `add_order` [`QfexModel::page_delay`] and,
//!   once an armed connection ended, [`QfexModel::blank_cid_after_session`] and, on its
//!   positions, [`QfexModel::stop_order_after_close`]; on one authentication,
//!   [`QfexModel::silent_auth`] (FBC-77r8). With [`QfexModel::stray_order`] or
//!   [`QfexModel::cid_absent_update`], a read socket's page waits from the first `add_order`
//!   until the end of the connection it came on is seen ([`QfexModel::knob_frames_unread`]),
//!   and with [`OnAck::MakerFill`] from its fill until its connection's client writes again or
//!   that connection's end is seen.
//!   FBC-77r8's on a later place, arm or drop:
//!   [`QfexModel::ack_delay_on`], [`QfexModel::fill_at_arm`], [`QfexModel::arm_err_on`],
//!   [`QfexModel::on_ack`] ([`OnAck`]), [`QfexModel::crossed_on_place`],
//!   [`QfexModel::fill_on_drop`], [`QfexModel::trade_on_drop`],
//!   [`QfexModel::historic_omits_last`],
//!   [`QfexModel::historic_fails`] and
//!   [`QfexModel::touch_moves_at_arm`]; for FBC-77r8's review,
//!   [`QfexModel::foreign_order_on`], [`QfexModel::position_on_place_on`],
//!   [`QfexModel::foreign_on_drop`], [`QfexModel::twap_after_close`],
//!   [`QfexModel::after_drop_historic`] ([`AfterHistoric`]), [`Limited::TradesPage`] and
//!   [`Limited::HistoricAfterDrop`].
//!   FBC-su9i's, on every place: [`QfexModel::ack_echo`]
//!   ([`Echo`]), how each `ACK` writes the order's client id, [`QfexModel::alo_cross`]
//!   ([`AloCross`]), how a post-only order priced through the touch is answered, `CANCELLED` by
//!   default, [`QfexModel::cancel_misses`], every cancel answered `NO_SUCH_ORDER`,
//!   [`QfexModel::cancel_lies`], every cancel answered `CANCELLED` with the order left open, and
//!   [`QfexModel::silent_after_cross`], the primary account's authentications left unanswered
//!   once a given post-only order through the touch arrived; on the run's first `cancel_order`,
//!   [`QfexModel::hold_on_cancel`]; once the crossing session's connection ended, for FBC-77r8's
//!   review, [`QfexModel::after_cross_end`]; for FBC-su9i's review, as a given drop is seen,
//!   [`QfexModel::rest_fails_after_drop`], and on a given place, [`QfexModel::stop_order_on`].
//! - **MDS** ([`QfexModel::mds`]), on a stub of its own at its own loopback origin: a `bbo`
//!   subscribe answered with the touch of each symbol it names, or, with [`QfexModel::mds_err`],
//!   with a `ServerError` err, which ends the connection's epoch (the MDS codec asks to
//!   reconnect), or, with [`QfexModel::mds_silent`], with nothing.
//!
//! Every symbol, key, id and value is made up.

use super::*;

use std::str::FromStr;
use std::sync::Mutex;

use fbc_conformance::{ConnectionsView, HttpRequest, Pusher, Responded};
use rust_decimal::Decimal;

/// A made-up QFEX symbol, which no account trades.
pub const SYMBOL: &str = "SYN-USD";

/// A flat account with no row: the documented members alone.
pub const FLAT: &str = r#"{"balance":{},"positions":[]}"#;

/// An account holding a position on [`SYMBOL`], with one open order counted.
pub const LONG: &str =
    r#"{"balance":{},"positions":[{"symbol":"SYN-USD","position":1,"open_orders":1}]}"#;

/// No ended order: `GET /user/historic-orders`'s documented members, empty.
pub const NO_HISTORIC: &str = r#"{"count":0,"data":[]}"#;

/// The made-up `user_id` every private frame names, which `src/auth` spans.
pub const USER_ID: &str = "5a5a5a5a-0000-4000-8000-0000000000a1";

/// The client id the model's stray order carries, which no run mints.
pub const STRAY_CID: &str = "someone-else-0001";

/// One trade connection, as the model saw it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Conn {
    /// The account its authentication named: `None` for the primary account.
    pub account: Option<String>,
    /// Whether it subscribed: a session's connection, or the hold's.
    pub session: bool,
    /// Whether it armed cancel-on-disconnect.
    pub armed: bool,
    /// Whether its end was seen, and its cancel-on-disconnect applied as the knobs say.
    pub settled: bool,
    /// Whether that end was a drop, with no close of the client's.
    pub dropped: bool,
}

/// Where an order the model rests stands.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    Open,
    Cancelled,
    Filled,
}

/// One order the model rests, as its `add_order` gave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub id: String,
    pub account: Option<String>,
    /// As the frame wrote it.
    pub cid: String,
    pub side: String,
    pub kind: String,
    pub tif: String,
    pub price: Value,
    pub quantity: Decimal,
    pub filled: Decimal,
    pub state: State,
}

/// One fill the model made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filled {
    pub account: Option<String>,
    pub trade_id: String,
    pub order: Order,
    /// What it filled: the order's whole quantity, or part of it ([`QfexModel::part_fill_after`]).
    pub quantity: Decimal,
    pub at: i64,
}

/// A trade on an account of an order the model never had ([`QfexModel::trade_after_close`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StrayTrade {
    /// Its row names a made-up order by its `order_id` and its `client_order_id`.
    Other,
    /// Its row names no order: it carries neither id, which a decoder reads, not refuses.
    Unnamed,
}

/// How every `ACK` `order_response` the model sends for a place writes the order's client id
/// ([`QfexModel::ack_echo`]; FBC-su9i, decision 0148 item 2's exact-echo gate).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Echo {
    /// Exactly as the `add_order` wrote it, byte for byte.
    #[default]
    Exact,
    /// In upper case: the same id to fbc-core's `decode_cid`, but not byte for byte.
    Upper,
    /// Its first character written as a JSON `\u00XX` escape: the same string once decoded, but
    /// not byte for byte.
    Escaped,
    /// No `client_order_id` member.
    Absent,
    /// `client_order_id` null.
    Null,
    /// `client_order_id` empty.
    Empty,
}

/// How the model answers a post-only (`ALO`) order priced through its touch, a buy at or above
/// the ask or a sell at or below the bid ([`QfexModel::alo_cross`]; FBC-su9i, decision 0132 item
/// 7: QFEX's pages disagree on how it refuses one).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AloCross {
    /// An `order_response` `CANCELLED` naming its client id: it never rests.
    Cancelled,
    /// An `order_response` whose status the adapter does not map, [`UNMAPPED`], naming its
    /// client id: it never rests.
    Unmapped,
    /// An `err` echoing its `add_order`: no order is made.
    Err,
    /// Its `ACK`, written as [`QfexModel::ack_echo`] says, then an `order_response` `CANCELLED`
    /// naming its client id: acknowledged, then ended before it could rest.
    AckedCancelled,
    /// It fills whole as a taker, its `fill_response` and `FILLED` `order_response` naming its
    /// client id.
    Fills,
}

/// The status [`AloCross::Unmapped`] answers: one the adapter does not map (FBC-jdj7).
pub const UNMAPPED: &str = "REJECTED";

/// What the cancel-on-disconnect arm does to its account's open orders
/// ([`QfexModel::arm_cancels_open`]; decision 0132 items 2 and 4).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ArmCancels {
    /// It cancels none.
    #[default]
    No,
    /// It cancels every one, each reported by an `order_response` `CANCELLED` naming its client
    /// id, sent before the arm's ack.
    Reported,
    /// It cancels every one, and no frame says so.
    Silent,
    /// It cancels every one, each reported by an `order_response` `CANCELLED` naming its client
    /// id that comes late: with the next `add_order`'s answer, before its `ACK`, so after the
    /// session's resync ended.
    Late,
}

/// What the model does to one order after its `ACK` ([`QfexModel::on_ack`]), on a connection
/// still open.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OnAck {
    /// QFEX cancels it itself, its `order_response` `CANCELLED` naming its client id.
    VenueCancel,
    /// It fills whole as a maker, its `fill_response` and `FILLED` `order_response` naming its
    /// client id.
    MakerFill,
}

/// What the model changes on an account as its first `GET /user/historic-orders` after an armed
/// connection's drop is answered ([`QfexModel::after_drop_historic`]): after the reads after the
/// drop, and before the read of every named account after them (FBC-77r8's review).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AfterHistoric {
    /// Its position on [`SYMBOL`] moves, with no order or fill of the model's, as a hand trade's
    /// would.
    Position,
    /// It holds an open order no run placed, under [`STRAY_CID`], which its pages list and no
    /// frame names.
    Foreign,
    /// Its read sockets' pages list one TWAP.
    Twap,
}

/// Which open orders an armed connection's end cancels ([`QfexModel::cod_reach`]).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Reach {
    /// Every open order of its account, placed on any connection (decision 0121's account-wide
    /// cancel-on-disconnect).
    #[default]
    Account,
    /// Only the orders placed on that connection: the drop's reach over an earlier connection's
    /// orders fails (decision 0132 item 4).
    Own,
    /// Only the orders placed on earlier connections: the order placed on it outlives it.
    Earlier,
}

/// A provider rate refusal the model answers in place of one request ([`QfexModel::rate_limit`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Limited {
    /// The run's first `add_order`, answered a `RateLimited` err echoing it; nothing rests.
    PlaceErr,
    /// The run's first `add_order`, answered an `order_response` whose status is `RATE_LIMITED`.
    PlaceOrder,
    /// The run's first `cancel_order`, answered a `RateLimited` err echoing it.
    Cancel,
    /// The `n`th `get_user_orders` page on a read socket after the first `add_order`, from 0,
    /// answered a `RateLimited` err echoing it.
    ReadSocket(usize),
    /// The `n`th `GET /user/positions` after the first `add_order`, from 0, answered HTTP 429.
    Rest(usize),
    /// The first `get_user_orders` page on a read socket once an armed connection ended,
    /// answered a `RateLimited` err echoing it: the after-session read's.
    ReadSocketAfterSession,
    /// The first `GET /user/positions` once an armed connection ended, answered HTTP 429.
    RestAfterSession,
    /// The first `GET /user/positions` once an armed connection dropped, with no close,
    /// answered HTTP 429: the first read's after the drop (FBC-77r8).
    RestAfterDrop,
    /// The `n`th trade-socket authentication after the first `add_order`, from 0, answered a
    /// `RateLimited` err that echoes nothing. A session's authentication is counted too, since
    /// a connection shows itself a session only by subscribing after it: aimed at a read socket
    /// after a session authenticated, `n` counts that session's authentication.
    ReadAuth(usize),
    /// The `n`th trade-socket authentication after the first `add_order`, from 0, counted as
    /// [`Limited::ReadAuth`] counts them, answered a `RateLimited` err echoing it, which the
    /// journal keeps whole (decision 0134 item 2), so the adapter reads no hold from it.
    ReadAuthEcho(usize),
    /// The `n`th `get_user_orders` page on a read socket after the first `add_order`, from 0,
    /// answered a `RateLimited` err echoing it whose `message` holds a JSON escape, which the
    /// journal keeps whole (decision 0134 item 2), so the adapter reads neither a hold nor a
    /// refusal from it.
    PageKeptWhole(usize),
    /// The first `get_user_orders` page on a read socket before any `add_order`, answered a
    /// `RateLimited` err echoing it: the reads' before drop session one, or those that clear a
    /// stop marker.
    ReadSocketBefore,
    /// The first `GET /user/positions` before any `add_order`, answered HTTP 429.
    RestBefore,
    /// The first `get_user_orders` page on a read socket before any `add_order`, answered as
    /// [`Limited::PageKeptWhole`] answers one.
    PageKeptWholeBefore,
    /// The first `get_user_trades` page on a read socket, answered a `RateLimited` err echoing
    /// it.
    Trades,
    /// The `n`th `get_user_trades` page of the run, from 0, as
    /// [`QfexModel::fill_on_trades_page`] counts them, answered a `RateLimited` err echoing it
    /// (FBC-77r8's review).
    TradesPage(usize),
    /// The first `GET /user/historic-orders` once an armed connection dropped, with no close,
    /// answered HTTP 429 (FBC-77r8's review).
    HistoricAfterDrop,
}

/// What an account's reads answer while [`QfexModel::rest_lag`] holds them back: its open orders
/// and positions before the change, and how many pages and REST reads are left to answer so.
#[derive(Clone, Debug)]
pub(super) struct Stale {
    orders: Vec<Order>,
    positions: String,
    pages: usize,
    rest: usize,
}

/// One instrument `/refdata` lists, in its REST reference's shape.
#[derive(Clone, Debug)]
pub struct Listed {
    pub symbol: &'static str,
    pub base: &'static str,
    /// `ACTIVE`, `INACTIVE` or `DELISTED`.
    pub status: &'static str,
    /// `order_types` and `order_time_in_force`, each an array of names or null.
    pub types: Value,
    pub tifs: Value,
    pub tick: &'static str,
    pub lot: &'static str,
    pub min: &'static str,
}

impl Listed {
    /// [`SYMBOL`], trading, listing both a post-only (`ALO`) GTC and a limit IOC, on a cent grid
    /// with a minimum of one lot of 0.01.
    pub fn trading() -> Listed {
        Listed {
            symbol: SYMBOL,
            base: "SYN",
            status: "ACTIVE",
            types: json(r#"["LIMIT","ALO"]"#),
            tifs: json(r#"["GTC","IOC"]"#),
            tick: "0.01",
            lot: "0.01",
            min: "0.01",
        }
    }

    /// Its `/refdata` entry.
    fn entry(&self) -> Value {
        serde_json::json!({
            "symbol": self.symbol, "clobPairId": "1", "tick_size": self.tick,
            "lot_size": self.lot, "min_quantity": self.min, "max_quantity": "1000",
            "base_asset": self.base, "quote_asset": "USD", "margin_asset": "USDC",
            "order_time_in_force": self.tifs, "order_types": self.types, "status": self.status
        })
    }
}

/// The touch the MDS answers a `bbo` subscribe with: each side's price and size, or none, and
/// how many seconds before its answer QFEX stamps it.
#[derive(Clone, Debug)]
pub struct Touch {
    pub bid: Option<(&'static str, &'static str)>,
    pub ask: Option<(&'static str, &'static str)>,
    /// Its age when answered: each frame is stamped this many seconds before the time it is
    /// answered at, so a fresh touch is fresh however long a test ran before it.
    pub age_secs: i64,
}

impl Touch {
    /// A fresh touch, 100.00 bid and 100.10 asked, stamped as it is answered.
    pub fn fresh() -> Touch {
        Touch {
            bid: Some(("100.00", "1.00")),
            ask: Some(("100.10", "2.00")),
            age_secs: 0,
        }
    }

    /// Its `bbo` frame for `symbol`, in the bbo channel page's shape, stamped
    /// [`Touch::age_secs`] before now.
    fn frame(&self, symbol: &str) -> String {
        let side = |level: Option<(&str, &str)>| match level {
            Some((px, qty)) => serde_json::json!([[px, qty]]),
            None => serde_json::json!([]),
        };
        let time = evidence::utc(now_secs() - self.age_secs).replacen(' ', "T", 1);
        serde_json::json!({
            "type": "bbo", "symbol": symbol, "bid": side(self.bid), "ask": side(self.ask),
            "sequence": 1, "time": time
        })
        .to_string()
    }
}

/// The seconds since the epoch now.
pub fn now_secs() -> i64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    i64::try_from(now.unwrap().as_secs()).unwrap()
}

/// One request or connection the model saw, in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// `GET <path>` with the selected account for positions, `trade <n> <type>`, or
    /// `mds <n> <type>`.
    pub what: String,
    /// Whether the probe had printed its `CAPS` line by then.
    pub caps_printed: bool,
    /// For a trade socket's authentication: whether every trade socket before it had closed, no
    /// MDS connection had opened and no `/refdata` been read. `None` for every other event.
    pub alone: Option<bool>,
}

/// QFEX, as the place stages' tests model it.
pub struct QfexModel {
    /// What `/refdata` lists.
    pub listed: Vec<Listed>,
    /// Each account's positions by its selection header, and what a lagging read answers.
    pub positions: HashMap<Option<String>, String>,
    pub stale: HashMap<Option<String>, String>,
    /// The lag knob: each account's first `lag` reads answer its stale body.
    pub lag: usize,
    /// Each account's reads so far.
    pub reads: HashMap<Option<String>, usize>,
    /// Whether a trade socket's authentication goes unanswered, so a read waits on it for
    /// `--read-wait-ms`, as a test pressing Ctrl-C during the reads needs.
    pub reads_silent: bool,
    /// What `/refdata` answers in place of the listing, when set: a status and a body.
    pub refdata: Option<(u16, &'static str)>,
    /// The touch, and whether the MDS answers a subscribe with an err instead, or with nothing
    /// (`mds_silent`: the preflight then waits for a touch, as a test pressing Ctrl-C during it
    /// needs).
    pub touch: Touch,
    pub mds_err: bool,
    pub mds_silent: bool,
    pub log: Vec<Event>,
    /// What the probe printed, to stamp each event with whether `CAPS` came first.
    pub printed: Printed,
    /// The trade and MDS stubs' connections, once both are up ([`Venue::start`]).
    pub views: Option<(ConnectionsView, ConnectionsView)>,
    /// The trade stub's push handle, once it is up: the knobs that answer later or close.
    pub pusher: Option<Pusher>,
    /// The MDS stub's push handle, once it is up ([`QfexModel::crossed_on_place`],
    /// [`QfexModel::touch_moves`], [`QfexModel::touch_moves_at_arm`]).
    pub mds_pusher: Option<Pusher>,
    /// Each trade connection seen, by its number.
    pub conns: HashMap<usize, Conn>,
    /// Every order rested and every fill made, in order.
    pub orders: Vec<Order>,
    pub fills: Vec<Filled>,
    /// Whether a session's connection subscribed while another session's was open.
    pub overlapped: bool,
    /// Graceful close and drop: whether an armed connection's end cancels its account's open
    /// orders (decision 0121's account-wide cancel-on-disconnect), and which of them it reaches.
    pub cod_on_close: bool,
    pub cod_on_drop: bool,
    pub cod_reach: Reach,
    /// Whether the arm cancels its account's open orders, and whether a frame reports it.
    pub arm_cancels_open: ArmCancels,
    /// The first `add_order`'s answer and its knobs' frames are pushed this long after it, past
    /// the request's deadline if need be, instead of answering it at once.
    pub ack_delay: Option<Duration>,
    /// Which `add_order` of the run, from 0, [`QfexModel::ack_delay`] delays (the first by
    /// default): its answer alone, pushed later.
    pub ack_delay_on: usize,
    /// The `n`th arm of the run, from 0, fills every open order of its account whole, as a
    /// maker, its `fill_response` and `FILLED` `order_response` pushed after the arm's ack.
    pub fill_at_arm: Option<usize>,
    /// An armed connection's drop fills the open orders placed on it whole, as a maker, before
    /// its cancel-on-disconnect; no frame says so, since nothing is open to hear it.
    pub fill_on_drop: bool,
    /// `GET /user/historic-orders` leaves out the run's last-placed order, ended or not.
    pub historic_omits_last: bool,
    /// The run's first `GET /user/historic-orders` answered HTTP 500, as a read that fails with
    /// no provider rate refusal.
    pub historic_fails: bool,
    /// How every place's `ACK` writes its client id (FBC-su9i).
    pub ack_echo: Echo,
    /// How a post-only order priced through the touch is answered, when set: `CANCELLED` by
    /// default, as [`QfexModel::cancel_place`] says QFEX ends a post-only order that would
    /// cross; with `None` it rests like any other (FBC-su9i).
    pub alo_cross: Option<AloCross>,
    /// Every `cancel_order` answered `NO_SUCH_ORDER`, the order it names left open, so it still
    /// rests after the session (FBC-su9i: an order left resting).
    pub cancel_misses: bool,
    /// Every `cancel_order` of an open order answered `CANCELLED` naming its client id, the order
    /// left open, so the registry holds it ended while the reads list it (FBC-su9i: an order left
    /// resting that only a read shows).
    pub cancel_lies: bool,
    /// Once the `n`th post-only order through the touch, from 0, arrived, every later
    /// authentication naming no subaccount (the primary account's) is left unanswered, so each
    /// read of the primary account after it waits out `--read-wait-ms` and fails (FBC-su9i).
    pub silent_after_cross: Option<usize>,
    pub rate_limit: Option<Limited>,
    /// After the first `ACK`: an `order_response` naming an order no run placed, with
    /// [`STRAY_CID`] (decision 0151 item 13's stray order). With it, as with
    /// [`QfexModel::cid_absent_update`], the read sockets' pages wait for the end of the
    /// placing connection ([`QfexModel::knob_frames_unread`]).
    pub stray_order: bool,
    /// After the first `ACK`: an `order_response` naming that order with no client id.
    pub cid_absent_update: bool,
    /// After the first `ACK`: that order filled whole, its `fill_response` naming no client id.
    pub stray_fill: bool,
    /// After the first `ACK`: that order filled whole as a maker, its `fill_response` and its
    /// `FILLED` `order_response` naming its client id.
    pub maker_fill: bool,
    /// After the first `add_order`'s answer: the server closes that connection.
    pub server_close: bool,
    /// With the first `add_order`, which it rests and leaves unanswered: the server closes that
    /// connection, so the session is still waiting for the answer as it closes.
    pub close_on_place: bool,
    /// Whether the cancel-on-disconnect arm is answered an err echoing it, which rejects it,
    /// instead of its ack.
    pub arm_err: bool,
    /// The `n`th arm of the run, from 0, answered such an err alone (FBC-77r8).
    pub arm_err_on: Option<usize>,
    /// The `n`th `add_order` of the run, from 0: what the model does to its order right after
    /// its `ACK`, in the same answer (FBC-77r8). With [`OnAck::MakerFill`], the read sockets'
    /// pages wait from then until that connection's client writes again or its end is seen
    /// ([`QfexModel::knob_frames_unread`]).
    pub on_ack: Option<(usize, OnAck)>,
    /// As the `n`th `add_order` of the run, from 0, arrives, the MDS pushes a crossed touch on
    /// every MDS connection open, so no later order is priced from it (FBC-77r8).
    pub crossed_on_place: Option<usize>,
    /// After each change to an account's orders or position, its next `rest_lag` read-socket
    /// pages and REST positions each answer what it was before the change.
    pub rest_lag: usize,
    /// `(n, lag)`: after the `n`th drop of an armed connection of the run, from 0, cancels an
    /// account's orders on its cancel-on-disconnect, the account's next `lag` read-socket pages
    /// and REST positions each answer what it was before the drop, as QFEX cancels on a drop
    /// only once it learns of it, which a read made at once can precede (FBC-su9i).
    pub drop_lag: Option<(usize, usize)>,
    /// The `GET /user/positions` reads after the first `add_order`, from 0, answered HTTP 500.
    pub rest_fails: Vec<usize>,
    /// The first `cancel_order`'s answer is pushed this long after it, instead of at once.
    pub cancel_delay: Option<Duration>,
    /// Once an armed connection ended, its account's positions show a position on [`SYMBOL`]
    /// that no order or fill of the model made, as a hand trade would.
    pub position_after_close: bool,
    /// From the first `add_order` on, its account's positions show that position too.
    pub position_on_place: bool,
    /// Once an armed connection ended, its account also holds an open order no run placed, under
    /// [`STRAY_CID`], which its pages list and no frame names, as another trader's would.
    pub foreign_after_close: bool,
    /// With the first `add_order`, its account also holds an open order it did not place, under
    /// [`STRAY_CID`], which its pages list and no frame names, as another trader's would.
    pub foreign_order: bool,
    /// With the `n`th `add_order` of the run, from 0, its account also holds such an order
    /// (FBC-77r8's review: `n` 1, beside Y, so the read before the drop lists it).
    pub foreign_order_on: Option<usize>,
    /// With the `n`th `add_order` of the run, from 0, its account's positions show a position on
    /// [`SYMBOL`] that no order or fill of the model made, and no frame says so (FBC-77r8's
    /// review: `n` 1, beside Y, so the read before the drop shows it).
    pub position_on_place_on: Option<usize>,
    /// As an armed connection's drop is settled, after its cancel-on-disconnect, its account
    /// holds such an order, so the reads after the drop list it (FBC-77r8's review).
    pub foreign_on_drop: bool,
    /// Once an armed connection ended, its account's read sockets' pages list one TWAP
    /// (FBC-77r8's review).
    pub twap_after_close: bool,
    /// What the first `GET /user/historic-orders` after an armed connection's drop changes on
    /// its account, once answered ([`AfterHistoric`]; FBC-77r8's review).
    pub after_drop_historic: Option<AfterHistoric>,
    /// The accounts whose read sockets' pages list a TWAP.
    pub(super) twaps: Vec<Option<String>>,
    /// As it reads the first `add_order`, before answering it, the model holds this limiter's
    /// every frame budget for the time given, as a provider refusal echoing nothing would: the
    /// run's shared limiter, under the run's provider user.
    pub hold_on_place: Option<(fbc_runtime::RateLimiter, Duration)>,
    /// As it reads the run's first `cancel_order`, before answering it, the model holds this
    /// limiter's every frame budget for the time given, as [`QfexModel::hold_on_place`] does:
    /// the cancel is still answered, so only what the probe sends after it is held.
    pub hold_on_cancel: Option<(fbc_runtime::RateLimiter, Duration)>,
    /// What the model changes on an account, each in turn, once the trade connection the run's
    /// first post-only through the touch arrived on ended, after its cancel-on-disconnect, so
    /// only the reads after the crossing session see it ([`AfterHistoric`]; FBC-su9i, carrying
    /// FBC-77r8's review).
    pub after_cross_end: Vec<AfterHistoric>,
    /// `(n, ok)`: once the `n`th drop of an armed connection of the run, from 0, is seen, its
    /// account's `GET /user/positions` answers its next `ok` reads, then HTTP 500 to every one
    /// after them, as a read that fails with no provider rate refusal (FBC-su9i's review: a
    /// later round of the reads after a crossing session's drop that cannot read the account).
    pub rest_fails_after_drop: Option<(usize, usize)>,
    /// With the `n`th `add_order` of the run, from 0, its account's `GET /user/positions`
    /// counts one more open order on [`SYMBOL`] than its pages list, as a stop order would, and
    /// no frame says so (FBC-su9i's review: an order REST alone counts after a crossing session).
    pub stop_order_on: Option<usize>,
    /// Whether every authentication is answered a failure.
    pub auth_fails: bool,
    /// The `n`th trade-socket authentication after the first `add_order`, from 0, a session's
    /// included (as [`Limited::ReadAuth`] counts), left unanswered, so its read waits on it for
    /// `--read-wait-ms`, as a test pressing Ctrl-C during that read needs.
    pub silent_auth: Option<usize>,
    /// Whether every `get_user_orders` page is answered from offset 0, whatever its offset, as
    /// a venue that ignored it would.
    pub stuck_pages: bool,
    /// The first `add_order` answered an `order_response` `CANCELLED` naming its client id, the
    /// order never resting, as QFEX ends a post-only order that would cross.
    pub cancel_place: bool,
    /// After the first `ACK`: that order cancelled by QFEX itself, its `order_response`
    /// `CANCELLED` naming its client id.
    pub venue_cancel: bool,
    /// The first `cancel_order` left unanswered, and the server closes its connection, so the
    /// session is still waiting for the answer as it ends.
    pub close_on_cancel: bool,
    /// Every `get_user_trades` page is held unanswered until the test releases it
    /// ([`Venue::release_held`]).
    pub hold_trades: bool,
    /// This long after the first `add_order`, an `order_response` naming an order no run placed
    /// is pushed on its connection ([`QfexModel::stray_order`]'s frame, later).
    pub stray_after: Option<Duration>,
    /// With the first `add_order`, half of that order fills as a maker, the rest left open, its
    /// `fill_response` and its `FILLED` `order_response` (`quantity_remaining` above zero, a
    /// partial fill) pushed on its connection this long after it, naming its client id; the
    /// model's positions and pages show the fill at once.
    pub part_fill_after: Option<Duration>,
    /// With the first `add_order`, the server closes that connection this long after it.
    pub close_after: Option<Duration>,
    /// As the first `get_user_trades` page is asked, the run's first order fills whole as a
    /// maker, no frame telling any socket: a fill after the reads that showed its account flat.
    pub fill_on_trades: bool,
    /// As the `n`th `get_user_trades` page of the run is asked, from 0, the run's first order
    /// fills whole as a maker, no frame telling any socket: [`QfexModel::fill_on_trades`] on a
    /// later read (FBC-77r8: `n` 1, the read of X's and Y's trades after a drop session two that
    /// failed, the first being X's after drop session one).
    pub fill_on_trades_page: Option<usize>,
    /// With the first trade-socket authentication, the touch becomes this one, pushed on every
    /// MDS connection: a touch that moved after the preflight read it.
    pub touch_moves: Option<Touch>,
    /// Once an armed connection ended, a read socket's page lists each order with an empty
    /// `client_order_id`, its `order_id` as before: a row naming X's venue id alone.
    pub blank_cid_after_session: bool,
    /// Whether a read socket's pages list the newest order first.
    pub newest_first: bool,
    /// Each read socket's `get_user_orders` page after the first `add_order` is answered this
    /// long after it, through the pusher, instead of at once: an answer still on its way as a
    /// comparison's wait ends.
    pub page_delay: Option<Duration>,
    /// Whether a read socket's `get_user_trades` pages are each answered as if asked from offset
    /// 0, whatever offset they name, as a venue ignoring it would.
    pub stuck_trades: bool,
    /// Once an armed connection of an account the model placed nothing on ended, that account's
    /// `GET /user/positions` counts one open order on [`SYMBOL`] that no page lists, as a stop
    /// order would (FBC-77r8: an order resting after a drop session one that failed).
    pub stop_order_after_close: bool,
    /// At the `n`th arm of the run, from 0, the touch becomes this one, pushed on every MDS
    /// connection before the arm is answered: a touch that moved before a later session's place
    /// (FBC-77r8: Y's, at drop session two's arm, `n` 1).
    pub touch_moves_at_arm: Option<(usize, Touch)>,
    /// Once an armed connection closed gracefully, as the model settles that close and before
    /// any answer after it, that connection's account's first open order fills whole as a
    /// maker, no frame telling any socket: a fill after the close and before the read after it.
    /// With [`QfexModel::part_fill_after_close`], half of it fills instead, and with
    /// [`QfexModel::cod_on_close`] the close then cancels the rest.
    pub fill_after_close: bool,
    pub part_fill_after_close: bool,
    /// Whether every read socket's `get_user_trades` page is answered an `InvalidRequest` err
    /// echoing it, which is no rate refusal: X's trades are not read.
    pub trades_err: bool,
    /// Once an armed connection closed gracefully, as the model settles that close, a trade on
    /// that connection's account of an order the model never had, the size of the account's
    /// first order, which moves its position: a trade after X's send naming another order, or
    /// none, as [`StrayTrade`] says.
    pub trade_after_close: Option<StrayTrade>,
    /// Once an armed connection closed gracefully, as the model settles that close, two trades
    /// on its account of an order the model never had, as [`QfexModel::trade_on_drop`]'s, so its
    /// position does not move: rows since X's send naming another order, or none, on an account
    /// still flat after drop session one (FBC-77r8).
    pub netting_after_close: Option<StrayTrade>,
    /// As an armed connection's drop is settled, two trades on its account of an order the model
    /// never had, one each way at the size of the account's first order, so its position does
    /// not move: rows since X's send naming another order, or none, as [`StrayTrade`] says, on
    /// an account still flat (FBC-77r8).
    pub trade_on_drop: Option<StrayTrade>,
    /// The trades whose `get_user_trades` rows carry neither `order_id` nor `client_order_id`
    /// ([`StrayTrade::Unnamed`]), by trade id.
    pub unnamed: Vec<String>,
    /// The answers [`QfexModel::hold_trades`] held: each connection and frame.
    pub(super) held: Vec<(usize, Frame)>,
    /// The connection each order was placed on, by its id ([`Reach::Own`]).
    pub(super) placed_on: HashMap<String, usize>,
    /// The connection the first `add_order` came on.
    pub(super) first_placed_on: Option<usize>,
    /// The connection [`OnAck::MakerFill`]'s fill came on, and how many data frames its client
    /// had sent by then, as the stub's record shows.
    pub(super) fill_after_ack_on: Option<(usize, usize)>,
    /// The read sockets' pages [`QfexModel::knob_frames_unread`] held: each connection and
    /// frame.
    pub(super) held_pages: Vec<(usize, Frame)>,
    pub(super) lagging: HashMap<Option<String>, Stale>,
    /// The accounts [`QfexModel::position_after_close`] moved.
    pub(super) moved: Vec<Option<String>>,
    pub(super) places: usize,
    pub(super) cancels: usize,
    /// The arms answered so far ([`QfexModel::fill_at_arm`]).
    pub(super) arms: usize,
    /// The `CANCELLED` frames [`ArmCancels::Late`] holds for the next `add_order`'s answer.
    pub(super) late_cancels: Vec<Frame>,
    /// Read-socket pages and REST positions answered since the first `add_order`.
    pub(super) pages_after: usize,
    /// How many drops of an armed connection were seen, for [`QfexModel::drop_lag`].
    pub(super) drops: usize,
    pub(super) rest_after: usize,
    /// Trade-socket authentications answered since the first `add_order`, a session's included
    /// ([`Limited::ReadAuth`]).
    pub(super) auths_after: usize,
    /// The `get_user_trades` pages asked so far ([`QfexModel::fill_on_trades_page`]).
    pub(super) trades_asked: usize,
    /// The post-only orders through the touch that arrived ([`QfexModel::silent_after_cross`]).
    pub(super) crossings: usize,
    /// The trade connections a post-only through the touch arrived on
    /// ([`QfexModel::after_cross_end`]).
    pub(super) crossed_on: Vec<usize>,
    /// The account [`QfexModel::rest_fails_after_drop`] fails, once its drop was seen, and how
    /// many of its reads are still answered.
    pub(super) failing: Option<(Option<String>, usize)>,
    /// The accounts [`QfexModel::stop_order_on`] gave a stop order.
    pub(super) stop_ordered: Vec<Option<String>>,
    pub(super) next_id: u64,
}

impl Default for QfexModel {
    fn default() -> QfexModel {
        let flat = HashMap::from([
            (None, FLAT.to_owned()),
            (Some(SUB.to_owned()), FLAT.to_owned()),
        ]);
        QfexModel {
            listed: vec![Listed::trading()],
            positions: flat.clone(),
            stale: flat,
            lag: 0,
            reads: HashMap::new(),
            reads_silent: false,
            refdata: None,
            touch: Touch::fresh(),
            mds_err: false,
            mds_silent: false,
            log: Vec::new(),
            printed: Printed::default(),
            views: None,
            pusher: None,
            mds_pusher: None,
            conns: HashMap::new(),
            orders: Vec::new(),
            fills: Vec::new(),
            overlapped: false,
            cod_on_close: false,
            cod_on_drop: true,
            cod_reach: Reach::Account,
            arm_cancels_open: ArmCancels::No,
            ack_delay: None,
            ack_delay_on: 0,
            fill_at_arm: None,
            fill_on_drop: false,
            historic_omits_last: false,
            historic_fails: false,
            ack_echo: Echo::Exact,
            alo_cross: Some(AloCross::Cancelled),
            cancel_misses: false,
            cancel_lies: false,
            silent_after_cross: None,
            rate_limit: None,
            stray_order: false,
            cid_absent_update: false,
            stray_fill: false,
            maker_fill: false,
            server_close: false,
            close_on_place: false,
            arm_err: false,
            arm_err_on: None,
            on_ack: None,
            crossed_on_place: None,
            rest_lag: 0,
            drop_lag: None,
            drops: 0,
            rest_fails: Vec::new(),
            cancel_delay: None,
            position_after_close: false,
            position_on_place: false,
            foreign_after_close: false,
            foreign_order_on: None,
            position_on_place_on: None,
            foreign_on_drop: false,
            twap_after_close: false,
            after_drop_historic: None,
            twaps: Vec::new(),
            foreign_order: false,
            hold_on_place: None,
            hold_on_cancel: None,
            after_cross_end: Vec::new(),
            rest_fails_after_drop: None,
            stop_order_on: None,
            failing: None,
            stop_ordered: Vec::new(),
            auth_fails: false,
            silent_auth: None,
            stuck_pages: false,
            cancel_place: false,
            venue_cancel: false,
            close_on_cancel: false,
            hold_trades: false,
            stray_after: None,
            part_fill_after: None,
            close_after: None,
            fill_on_trades: false,
            fill_on_trades_page: None,
            touch_moves: None,
            blank_cid_after_session: false,
            newest_first: false,
            page_delay: None,
            stuck_trades: false,
            stop_order_after_close: false,
            touch_moves_at_arm: None,
            fill_after_close: false,
            part_fill_after_close: false,
            trades_err: false,
            trade_after_close: None,
            netting_after_close: None,
            trade_on_drop: None,
            unnamed: Vec::new(),
            held: Vec::new(),
            placed_on: HashMap::new(),
            first_placed_on: None,
            fill_after_ack_on: None,
            held_pages: Vec::new(),
            lagging: HashMap::new(),
            moved: Vec::new(),
            places: 0,
            cancels: 0,
            arms: 0,
            late_cancels: Vec::new(),
            pages_after: 0,
            rest_after: 0,
            auths_after: 0,
            trades_asked: 0,
            crossings: 0,
            crossed_on: Vec::new(),
            next_id: 0,
        }
    }
}

impl QfexModel {
    /// Logs `what`, stamped with whether `CAPS` was printed by then.
    fn event(&mut self, what: String, alone: Option<bool>) {
        let caps_printed = self.printed.text().contains("CAPS ");
        self.log.push(Event {
            what,
            caps_printed,
            alone,
        });
    }

    /// The answer to a REST request.
    pub fn rest(&mut self, req: &HttpRequest) -> HttpReply {
        self.settle();
        let account = req.header("x-qfex-requested-account-id").map(str::to_owned);
        let shown = account
            .as_deref()
            .map_or(String::new(), |a| format!(" {a}"));
        self.event(format!("GET {}{shown}", req.path), None);
        match req.path.as_str() {
            "/refdata" if self.refdata.is_some() => {
                let (status, body) = self.refdata.expect("set");
                HttpReply {
                    status,
                    body: body.as_bytes().to_vec(),
                }
            }
            "/refdata" => {
                let data: Vec<Value> = self.listed.iter().map(Listed::entry).collect();
                ok(&serde_json::json!({"type": "refdata", "data": data}).to_string())
            }
            "/user/positions" => {
                let after = self.places > 0;
                let n = self.rest_after;
                self.rest_after += usize::from(after);
                if after && self.rate_limit == Some(Limited::Rest(n)) {
                    return status(429, r#"{"error":"Too Many Requests"}"#);
                }
                if !after && self.rate_limit == Some(Limited::RestBefore) {
                    self.rate_limit = None;
                    return status(429, r#"{"error":"Too Many Requests"}"#);
                }
                if self.rate_limit == Some(Limited::RestAfterSession) && self.session_ended() {
                    self.rate_limit = None;
                    return status(429, r#"{"error":"Too Many Requests"}"#);
                }
                if self.rate_limit == Some(Limited::RestAfterDrop) && self.dropped() {
                    self.rate_limit = None;
                    return status(429, r#"{"error":"Too Many Requests"}"#);
                }
                if after && self.rest_fails.contains(&n) {
                    return status(500, r#"{"error":"synthetic failure"}"#);
                }
                if let Some(failing) = self.failing.as_mut().filter(|f| f.0 == account) {
                    if failing.1 == 0 {
                        return status(500, r#"{"error":"synthetic failure"}"#);
                    }
                    failing.1 -= 1;
                }
                ok(&self.positions_of(account))
            }
            "/user/subaccounts" => listing_refused(),
            _ if std::mem::take(&mut self.historic_fails) => {
                status(500, r#"{"error":"synthetic failure"}"#)
            }
            _ if self.rate_limit == Some(Limited::HistoricAfterDrop) && self.dropped() => {
                self.rate_limit = None;
                status(429, r#"{"error":"Too Many Requests"}"#)
            }
            _ => {
                let body = self.historic(account.clone());
                let dropped = self.dropped();
                if let Some(change) = self.after_drop_historic.take_if(|_| dropped) {
                    self.after_historic(account, change);
                }
                ok(&body)
            }
        }
    }

    /// Makes `change` on `account` ([`QfexModel::after_drop_historic`]).
    fn after_historic(&mut self, account: Option<String>, change: AfterHistoric) {
        match change {
            AfterHistoric::Position => {
                self.before_change(&account);
                if !self.moved.contains(&account) {
                    self.moved.push(account);
                }
            }
            AfterHistoric::Foreign => {
                self.foreign_beside(&account, "33333333-3333-4333-8333-333333333333");
            }
            AfterHistoric::Twap => self.twaps.push(account),
        }
    }

    /// Rests an open order no run placed on `account`, under [`STRAY_CID`] and the venue id
    /// `id`, shaped as the account's first order: none when the account has none, or when `id`
    /// is held already.
    fn foreign_beside(&mut self, account: &Option<String>, id: &str) {
        let first = self.orders.iter().find(|o| o.account == *account).cloned();
        let foreign = first.map(|first| Order {
            id: id.to_owned(),
            cid: STRAY_CID.to_owned(),
            filled: Decimal::ZERO,
            state: State::Open,
            ..first
        });
        let new = |f: &Order| self.orders.iter().all(|o| o.id != f.id);
        let foreign = foreign.filter(new);
        if foreign.is_some() {
            self.before_change(account);
        }
        self.orders.extend(foreign);
    }

    /// `account`'s ended orders, as `GET /user/historic-orders` lists them.
    fn historic(&self, account: Option<String>) -> String {
        let last = (self.orders.last()).filter(|_| self.historic_omits_last);
        let shown = |o: &&Order| last.is_none_or(|last| last.id != o.id);
        let ended = (self.orders.iter()).filter(|o| o.account == account && o.state != State::Open);
        let ended = ended.filter(shown);
        let data: Vec<Value> = ended
            .map(|o| {
                let terminal = match o.state {
                    State::Filled => "FILLED",
                    _ => "CANCELLED",
                };
                serde_json::json!({
                    "order_id": o.id, "client_order_id": o.cid, "terminal_status": terminal,
                    "symbol": SYMBOL, "side": o.side, "type": o.kind, "time_in_force": o.tif,
                    "quantity": number(o.quantity), "price": o.price, "filled_qty": number(o.filled)
                })
            })
            .collect();
        match data.is_empty() {
            true => NO_HISTORIC.to_owned(),
            false => serde_json::json!({"count": data.len(), "data": data}).to_string(),
        }
    }

    /// `account`'s positions: its stale body for its first [`QfexModel::lag`] reads, then, while
    /// [`QfexModel::rest_lag`] holds them back, what they were before its last change, then its
    /// current ones ([`QfexModel::current`]).
    pub fn positions_of(&mut self, account: Option<String>) -> String {
        let read = self.reads.entry(account.clone()).or_default();
        *read += 1;
        if *read <= self.lag {
            return self.stale[&account].clone();
        }
        if let Some(stale) = self.stale_of(&account, |s| &mut s.rest) {
            return stale.positions;
        }
        let ended = (self.conns.values()).any(|c| c.armed && c.settled && c.account == account);
        let placed = self.orders.iter().any(|o| o.account == account);
        if self.stop_order_after_close && ended && !placed {
            return serde_json::json!({"balance": {}, "positions": [
                {"symbol": SYMBOL, "position": 0, "open_orders": 1}
            ]})
            .to_string();
        }
        self.current(&account)
    }

    /// `account`'s positions now: what its orders and fills make them once the model touched
    /// it, its body in [`QfexModel::positions`] otherwise.
    pub fn current(&self, account: &Option<String>) -> String {
        let touched = self.orders.iter().any(|o| o.account == *account);
        let moved = self.moved.contains(account);
        if !touched && !moved {
            return self.positions[account].clone();
        }
        let filled: Decimal = (self.fills.iter())
            .filter(|f| f.account == *account)
            .map(|f| match f.order.side.as_str() {
                "SELL" => -f.quantity,
                _ => f.quantity,
            })
            .sum();
        let position = filled + if moved { Decimal::ONE } else { Decimal::ZERO };
        let stop = usize::from(self.stop_ordered.contains(account));
        let open = self.open_of(account).len() + stop;
        serde_json::json!({"balance": {}, "positions": [
            {"symbol": SYMBOL, "position": number(position), "open_orders": open}
        ]})
        .to_string()
    }

    /// `account`'s open orders, oldest first.
    fn open_of(&self, account: &Option<String>) -> Vec<Order> {
        let open = (self.orders.iter()).filter(|o| o.account == *account && o.state == State::Open);
        open.cloned().collect()
    }

    /// What `account`'s reads still answer from before its last change, one read of the kind
    /// `left` counts spent; `None` once none is left.
    fn stale_of(
        &mut self,
        account: &Option<String>,
        left: impl Fn(&mut Stale) -> &mut usize,
    ) -> Option<Stale> {
        let stale = self.lagging.get_mut(account)?;
        let n = left(stale);
        if *n == 0 {
            return None;
        }
        *n -= 1;
        Some(stale.clone())
    }

    /// Keeps what `account`'s reads answer now, for its next [`QfexModel::rest_lag`] reads of
    /// each kind, as a change to it is made.
    fn before_change(&mut self, account: &Option<String>) {
        if self.rest_lag == 0 {
            return;
        }
        let stale = Stale {
            orders: self.open_of(account),
            positions: self.current(account),
            pages: self.rest_lag,
            rest: self.rest_lag,
        };
        self.lagging.insert(account.clone(), stale);
    }

    /// The answer to frame `frame` on trade socket `conn`: its authentication, a subscription,
    /// the arm, a place, a cancel and the two pages; anything else refused.
    pub fn trade(&mut self, conn: usize, frame: &Frame) -> Responded {
        self.settle();
        let v = parsed(frame)?;
        let kind = kind_of(&v);
        let alone = (kind == "auth").then(|| self.alone(conn));
        self.event(format!("trade {conn} {kind}"), alone);
        let Frame::Text(text) = frame else {
            unreachable!("parsed")
        };
        match kind.as_str() {
            "auth" => {
                let account = v["params"]["account_id"].as_str().map(str::to_owned);
                self.conns.entry(conn).or_default().account = account;
                let n = self.auths_after;
                self.auths_after += usize::from(self.places > 0);
                if self.places > 0 && self.silent_auth == Some(n) {
                    return Ok(Vec::new());
                }
                let crossed = self.silent_after_cross.is_some_and(|n| self.crossings > n);
                if crossed && v["params"]["account_id"].as_str().is_none() {
                    return Ok(Vec::new());
                }
                if self.places > 0 && self.rate_limit == Some(Limited::ReadAuth(n)) {
                    let err = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds"}"#;
                    return Ok(vec![Frame::text(err)]);
                }
                if self.places > 0 && self.rate_limit == Some(Limited::ReadAuthEcho(n)) {
                    return Ok(vec![Frame::text(limited(text, 3))]);
                }
                if let Some(touch) = self.touch_moves.take() {
                    self.move_touch(touch);
                }
                match (self.reads_silent, self.auth_fails) {
                    (true, _) => Ok(Vec::new()),
                    (false, true) => Ok(vec![Frame::text(r#"{"type":"auth","result":"failure"}"#)]),
                    (false, false) => Ok(vec![Frame::text(AUTH_OK)]),
                }
            }
            "subscribe" => {
                self.subscribed(conn);
                confirmation(frame)
            }
            "cancel_on_disconnect" if self.arm_err || self.arm_err_on == Some(self.arms) => {
                self.arms += 1;
                Ok(vec![Frame::text(format!(
                    concat!(
                        r#"{{"type":"Err","error_code":"InvalidRequest","message":"synthetic","#,
                        r#""incoming_message":{text}}}"#,
                    ),
                    text = text
                ))])
            }
            "cancel_on_disconnect" => {
                self.conns.entry(conn).or_default().armed = true;
                let mut frames = self.arm_cancels(conn);
                frames.push(Frame::text(format!(
                    r#"{{"ack":{{"incoming_message":{text}}}}}"#
                )));
                let n = self.arms;
                self.arms += 1;
                if let Some((at, touch)) = self.touch_moves_at_arm.take() {
                    match at == n {
                        true => self.move_touch(touch),
                        false => self.touch_moves_at_arm = Some((at, touch)),
                    }
                }
                if self.fill_at_arm == Some(n) {
                    let open = self.open_of(&self.account_of(conn));
                    for order in open {
                        let filled = self.fill(&order.id);
                        frames.push(fill_response(&filled, true));
                        frames.push(order_response(&filled.order, "FILLED", true));
                    }
                }
                Ok(frames)
            }
            "add_order" => Ok(self.add_order(conn, &v, text)),
            "cancel_order" => Ok(self.cancel_order(conn, &v, text)),
            "get_user_orders" => {
                let session = self.conns.get(&conn).is_some_and(|c| c.session);
                let page = Frame::text(self.orders_page(conn, &v));
                if !session && self.knob_frames_unread() {
                    self.held_pages.push((conn, page));
                    return Ok(Vec::new());
                }
                match self.page_delay.filter(|_| !session && self.places > 0) {
                    Some(after) => {
                        self.later(conn, vec![page], after, false);
                        Ok(Vec::new())
                    }
                    None => Ok(vec![page]),
                }
            }
            "get_user_trades" if self.trades_err => Ok(vec![Frame::text(format!(
                r#"{{"type":"Err","error_code":"InvalidRequest","message":"synthetic","incoming_message":{text}}}"#
            ))]),
            "get_user_trades"
                if self.rate_limit == Some(Limited::TradesPage(self.trades_asked)) =>
            {
                self.rate_limit = None;
                self.trades_asked += 1;
                let echo = serde_json::json!({"type": "get_user_trades", "params": v["params"]});
                Ok(vec![Frame::text(limited(&echo.to_string(), 1))])
            }
            "get_user_trades" if self.rate_limit == Some(Limited::Trades) => {
                self.rate_limit = None;
                let echo = serde_json::json!({"type": "get_user_trades", "params": v["params"]});
                Ok(vec![Frame::text(limited(&echo.to_string(), 1))])
            }
            "get_user_trades" if self.hold_trades => {
                let page = Frame::text(self.trades_page(conn, &v));
                self.held.push((conn, page));
                Ok(Vec::new())
            }
            "get_user_trades" => {
                let asked = self.trades_asked;
                self.trades_asked += 1;
                let later = self.fill_on_trades_page == Some(asked);
                if std::mem::take(&mut self.fill_on_trades) || later {
                    let id = self.orders.first().map(|o| o.id.clone());
                    self.fill(&id.expect("an order to fill"));
                }
                Ok(vec![Frame::text(self.trades_page(conn, &v))])
            }
            _ => Err(format!("the model answers no {kind}")),
        }
    }

    /// Whether an armed connection's end was seen.
    fn session_ended(&self) -> bool {
        self.conns.values().any(|c| c.armed && c.settled)
    }

    /// Whether a read socket's `get_user_orders` page waits: with [`QfexModel::stray_order`] or
    /// [`QfexModel::cid_absent_update`], from the first `add_order` until the end of the
    /// connection it came on is seen (its client's close frame read, or the connection ended).
    /// Neither knob changes what a read shows, so nothing else orders a comparison's reads after
    /// the knob's frame. The stub writing that frame does not put it on the session's socket:
    /// the stub's socket does not set `TCP_NODELAY`, so the kernel can hold a frame written right
    /// after the `ACK` until the client acknowledges the `ACK` (about 40 ms on Linux). The
    /// session ends its connection only after drop session one's steps end, so a comparison
    /// held this way ends by the stop the knob's frame raises or by the end of its wait, never
    /// by agreeing before the session read that frame.
    ///
    /// So, too, with [`OnAck::MakerFill`] (FBC-77r8), from its fill until the placing
    /// connection's client writes again or its end is seen: the read before drop session two's
    /// drop shows nothing of a fill of Y, and the stop the fill's frames raise is what sends the
    /// next frame, the cancels (0151 item 2), so a read held this way cannot end before the
    /// session heard the fill. The drop comes only after that read, so its end never releases
    /// the hold first.
    pub(super) fn knob_frames_unread(&self) -> bool {
        let knob = self.stray_order || self.cid_absent_update;
        let stray = knob && self.first_placed_on.is_some_and(|n| !self.conn_ended(n));
        let fill = (self.fill_after_ack_on).is_some_and(|(n, sent)| !self.conn_wrote(n, sent));
        stray || fill
    }

    /// Whether trade connection `n`'s client wrote a data frame past its `sent`th, as the
    /// stub's record shows, or the end of `n` was seen ([`QfexModel::conn_ended`]).
    fn conn_wrote(&self, n: usize, sent: usize) -> bool {
        let records = self.views.as_ref().map(|(trade, _)| trade.connections());
        let record = records.and_then(|r| r.get(n).cloned());
        self.conn_ended(n) || record.is_some_and(|r| r.received.len() > sent)
    }

    /// How many data frames trade connection `n`'s client has sent, as the stub's record shows.
    fn sent_on(&self, n: usize) -> usize {
        let records = self.views.as_ref().map(|(trade, _)| trade.connections());
        let record = records.and_then(|r| r.get(n).cloned());
        record.map_or(0, |r| r.received.len())
    }

    /// Whether the end of trade connection `n` was seen: its client's close frame read, or the
    /// connection ended, as the stub's record shows, or its cancel-on-disconnect settled.
    fn conn_ended(&self, n: usize) -> bool {
        let settled = self.conns.get(&n).is_some_and(|c| c.settled);
        let records = self.views.as_ref().map(|(trade, _)| trade.connections());
        let record = records.and_then(|r| r.get(n).cloned());
        settled || record.is_some_and(|r| r.client_closed || !r.open)
    }

    /// Answers each page [`QfexModel::knob_frames_unread`] held, in order, on its connection,
    /// once none waits. A comparison that asked for them has ended by then, so a push to a
    /// connection that is gone fails, and nothing reads that failure.
    fn release_pages(&mut self) {
        if self.held_pages.is_empty() || self.knob_frames_unread() || self.pusher.is_none() {
            return;
        }
        for (conn, page) in std::mem::take(&mut self.held_pages) {
            self.later(conn, vec![page], Duration::ZERO, false);
        }
    }

    /// Whether an armed connection's drop was seen: its end, with no close of the client's.
    fn dropped(&self) -> bool {
        self.conns
            .values()
            .any(|c| c.armed && c.settled && c.dropped)
    }

    /// `conn`'s account.
    fn account_of(&self, conn: usize) -> Option<String> {
        self.conns.get(&conn).and_then(|c| c.account.clone())
    }

    /// Marks `conn` a session's, noting whether another session's connection was still open.
    fn subscribed(&mut self, conn: usize) {
        let open = |n: usize| {
            let records = self.views.as_ref().map(|(trade, _)| trade.connections());
            let record = records.and_then(|r| r.get(n).cloned());
            record.is_some_and(|r| r.open && !r.client_closed)
        };
        let others = (self.conns.iter()).any(|(n, c)| *n != conn && c.session && open(*n));
        self.overlapped |= others;
        self.conns.entry(conn).or_default().session = true;
    }

    /// The arm on `conn`, as [`QfexModel::arm_cancels_open`] says: every open order of its
    /// account cancelled, logged, and, when reported, each one's `CANCELLED` `order_response`,
    /// which the arm's ack follows.
    fn arm_cancels(&mut self, conn: usize) -> Vec<Frame> {
        let account = self.account_of(conn);
        let open = self.open_of(&account);
        if self.arm_cancels_open == ArmCancels::No || open.is_empty() {
            return Vec::new();
        }
        self.before_change(&account);
        let how = self.arm_cancels_open;
        let mut frames = Vec::new();
        for order in &mut self.orders {
            if order.account == account && order.state == State::Open {
                order.state = State::Cancelled;
                let report =
                    (how != ArmCancels::Silent).then(|| order_response(order, "CANCELLED", true));
                frames.extend(report);
            }
        }
        let how = match how {
            ArmCancels::Reported => "reported",
            ArmCancels::Late => {
                self.late_cancels.append(&mut frames);
                "reported late"
            }
            _ => "silently",
        };
        self.event(format!("arm {conn} cancels {} {how}", open.len()), None);
        frames
    }

    /// Applies cancel-on-disconnect to each armed connection whose end the stub has seen since
    /// ([`QfexModel::ended`]), in the order the connections opened.
    fn settle(&mut self) {
        let Some((trade, _)) = &self.views else {
            return;
        };
        let records = trade.connections();
        let mut ended = Vec::new();
        for (n, conn) in &self.conns {
            let Some(record) = records.get(*n) else {
                continue;
            };
            if conn.armed && !conn.settled && (record.client_closed || !record.open) {
                ended.push((*n, record.client_closed));
            }
        }
        ended.sort_by_key(|(n, _)| *n);
        for (n, graceful) in ended {
            self.ended(n, graceful);
        }
        self.release_pages();
    }

    /// Trade connection `n`'s end, gracefully (the client's close frame read) or by a drop: once
    /// armed, a graceful close cancels its account's open orders with
    /// [`QfexModel::cod_on_close`], and a drop with [`QfexModel::cod_on_drop`], every one or only
    /// those placed on it, as [`QfexModel::cod_reach`] says; logged as `cod <n> <close|drop>
    /// <cancels|keeps>`. A connection that never armed, or whose end was seen, changes nothing.
    pub(super) fn ended(&mut self, n: usize, graceful: bool) {
        let Some(conn) = self.conns.get_mut(&n).filter(|c| c.armed && !c.settled) else {
            return;
        };
        conn.settled = true;
        conn.dropped = !graceful;
        let account = conn.account.clone();
        if self.fill_on_drop && !graceful {
            let own = (self.orders.iter())
                .filter(|o| o.state == State::Open && self.placed_on.get(&o.id) == Some(&n));
            let own: Vec<String> = own.map(|o| o.id.clone()).collect();
            for id in own {
                self.fill(&id);
            }
        }
        let (how, cancels) = match graceful {
            true => ("close", self.cod_on_close),
            false => ("drop", self.cod_on_drop),
        };
        let fills = self.fill_after_close || self.part_fill_after_close;
        if graceful && fills {
            let whole = !std::mem::take(&mut self.part_fill_after_close);
            self.fill_after_close = false;
            let open = (self.orders.iter())
                .find(|o| o.account == account && o.state == State::Open)
                .map(|o| (o.id.clone(), o.quantity - o.filled));
            if let Some((id, left)) = open {
                match whole {
                    true => self.fill(&id),
                    false => self.fill_part(&id, left / Decimal::TWO),
                };
            }
        }
        if let Some(stray) = self.trade_after_close.take_if(|_| graceful) {
            self.stray_trade(account.clone(), stray);
        }
        if let Some(stray) = self.netting_after_close.take_if(|_| graceful) {
            self.netting_trades(account.clone(), stray);
        }
        if let Some(stray) = self.trade_on_drop.take_if(|_| !graceful) {
            self.netting_trades(account.clone(), stray);
        }
        if cancels {
            self.before_change(&account);
            let lag = self
                .drop_lag
                .filter(|(on, _)| !graceful && *on == self.drops);
            if let Some((_, lag)) = lag {
                let stale = Stale {
                    orders: self.open_of(&account),
                    positions: self.current(&account),
                    pages: lag,
                    rest: lag,
                };
                self.lagging.insert(account.clone(), stale);
            }
            let reach = self.cod_reach;
            let placed_on = &self.placed_on;
            let reached = |o: &Order| match reach {
                Reach::Account => true,
                Reach::Own => placed_on.get(&o.id) == Some(&n),
                Reach::Earlier => placed_on.get(&o.id) != Some(&n),
            };
            let open = self.orders.iter_mut();
            let open = open.filter(|o| o.account == account && o.state == State::Open);
            open.filter(|o| reached(o))
                .for_each(|o| o.state = State::Cancelled);
        }
        if self.position_after_close && !self.moved.contains(&account) {
            self.moved.push(account.clone());
        }
        if self.foreign_on_drop && !graceful {
            self.foreign_beside(&account, "99999999-9999-4999-8999-999999999999");
        }
        if self.twap_after_close && !self.twaps.contains(&account) {
            self.twaps.push(account.clone());
        }
        if self.foreign_after_close {
            let first = self.orders.iter().find(|o| o.account == account).cloned();
            let foreign = first.map(|first| Order {
                id: "44444444-4444-4444-8444-444444444444".to_owned(),
                cid: STRAY_CID.to_owned(),
                filled: Decimal::ZERO,
                state: State::Open,
                ..first
            });
            let new = |f: &Order| self.orders.iter().all(|o| o.id != f.id);
            let foreign = foreign.filter(new);
            self.orders.extend(foreign);
        }
        if self.crossed_on.contains(&n) {
            for change in std::mem::take(&mut self.after_cross_end) {
                self.after_historic(account.clone(), change);
            }
        }
        let failing = self
            .rest_fails_after_drop
            .filter(|(on, _)| !graceful && *on == self.drops);
        if let Some((_, ok)) = failing {
            self.failing = Some((account.clone(), ok));
        }
        self.drops += usize::from(!graceful);
        let did = if cancels { "cancels" } else { "keeps" };
        self.event(format!("cod {n} {how} {did}"), None);
    }

    /// Rests the order `v`, whose frame is `text`, on `conn`'s account and answers it, with the
    /// knobs' frames after its `ACK` on the run's first.
    fn add_order(&mut self, conn: usize, v: &Value, text: &str) -> Vec<Frame> {
        self.places += 1;
        let first = self.places == 1;
        if first {
            self.first_placed_on = Some(conn);
        }
        if self.crossed_on_place == Some(self.places - 1) {
            self.push_crossed();
        }
        if let Some((limiter, held)) = self.hold_on_place.as_ref().filter(|_| first) {
            let now = tokio::time::Instant::now();
            let on = fbc_core::ConnKey {
                conn: u16::MAX,
                epoch: 0,
            };
            limiter.provider_rejected(now, on, None, Some(*held));
        }
        match self.rate_limit {
            Some(Limited::PlaceErr) if first => return vec![Frame::text(limited(text, 1))],
            Some(Limited::PlaceOrder) if first => {
                let order = r#"{"order_response":{"symbol":"SYN-USD","status":"RATE_LIMITED"}}"#;
                return vec![Frame::text(order)];
            }
            _ => {}
        }
        let account = self.account_of(conn);
        self.before_change(&account);
        let params = &v["params"];
        let text_of = |key: &str| params[key].as_str().unwrap_or_default().to_owned();
        let quantity = Decimal::from_str(&params["quantity"].to_string()).unwrap_or_default();
        let id = self.mint_id();
        self.placed_on.insert(id.clone(), conn);
        let order = Order {
            id,
            account: account.clone(),
            cid: text_of("client_order_id"),
            side: text_of("side"),
            kind: text_of("order_type"),
            tif: text_of("order_time_in_force"),
            price: params["price"].clone(),
            quantity,
            filled: Decimal::ZERO,
            state: State::Open,
        };
        self.crossings += usize::from(self.crosses(&order));
        if self.crosses(&order) {
            self.crossed_on.push(conn);
        }
        // The knobs on the nth place apply to a post-only through the touch too, whichever way it
        // is answered.
        let moved = self.position_on_place_on == Some(self.places - 1);
        if moved && !self.moved.contains(&account) {
            self.moved.push(account.clone());
        }
        if self.foreign_order_on == Some(self.places - 1) {
            self.foreign_beside(&account, "88888888-8888-4888-8888-888888888888");
        }
        if self.stop_order_on == Some(self.places - 1) && !self.stop_ordered.contains(&account) {
            self.before_change(&account);
            self.stop_ordered.push(account.clone());
        }
        if let Some(how) = self.alo_cross.filter(|_| self.crosses(&order)) {
            return self.cross(order, how, text);
        }
        if first && self.cancel_place {
            let ended = Order {
                state: State::Cancelled,
                ..order
            };
            self.orders.push(ended.clone());
            return vec![order_response(&ended, "CANCELLED", true)];
        }
        self.orders.push(order.clone());
        let mut frames = std::mem::take(&mut self.late_cancels);
        frames.push(echoed(&order, "ACK", self.ack_echo));
        match self.on_ack {
            Some((n, OnAck::VenueCancel)) if n + 1 == self.places => {
                let at = self.orders.len() - 1;
                self.before_change(&account);
                self.orders[at].state = State::Cancelled;
                frames.push(order_response(&self.orders[at], "CANCELLED", true));
            }
            Some((n, OnAck::MakerFill)) if n + 1 == self.places => {
                self.fill_after_ack_on = Some((conn, self.sent_on(conn)));
                let filled = self.fill(&order.id);
                frames.push(fill_response(&filled, true));
                frames.push(order_response(&filled.order, "FILLED", true));
            }
            _ => {}
        }
        if let Some(after) = self.stray_after.filter(|_| first) {
            let stray = Order {
                id: "77777777-7777-4777-8777-777777777777".to_owned(),
                cid: STRAY_CID.to_owned(),
                ..order.clone()
            };
            self.later(
                conn,
                vec![order_response(&stray, "ACK", true)],
                after,
                false,
            );
        }
        if first {
            frames.extend(self.knobs(&order));
            if self.position_on_place && !self.moved.contains(&account) {
                self.moved.push(account.clone());
            }
            if self.foreign_order {
                let foreign = Order {
                    id: "66666666-6666-4666-8666-666666666666".to_owned(),
                    cid: STRAY_CID.to_owned(),
                    ..order.clone()
                };
                self.orders.push(foreign);
            }
        }
        if let Some(after) = self.part_fill_after.filter(|_| first) {
            let filled = self.fill_part(&order.id, order.quantity / Decimal::TWO);
            let frames = vec![
                fill_response(&filled, true),
                order_response(&filled.order, "FILLED", true),
            ];
            self.later(conn, frames, after, false);
        }
        if let Some(after) = self.close_after.filter(|_| first) {
            self.later(conn, Vec::new(), after, true);
        }
        if first && self.close_on_place {
            self.later(conn, Vec::new(), Duration::ZERO, true);
            return Vec::new();
        }
        // A late answer, and a close, go through the pusher alone, so the close follows every
        // frame of the answer.
        let close = first && self.server_close;
        let delayed = self.places == self.ack_delay_on + 1;
        match (self.ack_delay.filter(|_| delayed), close) {
            (None, false) => frames,
            (after, _) => {
                self.later(conn, frames, after.unwrap_or_default(), close);
                Vec::new()
            }
        }
    }

    /// Whether `order` is a post-only order priced through the model's touch: a buy at or above
    /// its ask, or a sell at or below its bid.
    fn crosses(&self, order: &Order) -> bool {
        let price = Decimal::from_str(&order.price.to_string()).ok();
        let level = |side: Option<(&str, &str)>| side.and_then(|(px, _)| px.parse().ok());
        let through = match order.side.as_str() {
            "BUY" => price.zip(level(self.touch.ask)).map(|(p, ask)| p >= ask),
            _ => price.zip(level(self.touch.bid)).map(|(p, bid)| p <= bid),
        };
        order.kind == "ALO" && through == Some(true)
    }

    /// The answer to `order`, a post-only order through the touch whose frame is `text`, as `how`
    /// says, logged as `cross <how>`.
    fn cross(&mut self, order: Order, how: AloCross, text: &str) -> Vec<Frame> {
        self.event(format!("cross {how:?}"), None);
        let ended = |state| Order {
            state,
            ..order.clone()
        };
        match how {
            AloCross::Cancelled => {
                self.orders.push(ended(State::Cancelled));
                vec![order_response(&ended(State::Cancelled), "CANCELLED", true)]
            }
            AloCross::Unmapped => {
                self.orders.push(ended(State::Cancelled));
                vec![order_response(&ended(State::Cancelled), UNMAPPED, true)]
            }
            AloCross::AckedCancelled => {
                self.orders.push(ended(State::Cancelled));
                vec![
                    echoed(&order, "ACK", self.ack_echo),
                    order_response(&ended(State::Cancelled), "CANCELLED", true),
                ]
            }
            AloCross::Err => {
                self.placed_on.remove(&order.id);
                vec![Frame::text(format!(
                    concat!(
                        r#"{{"type":"Err","error_code":"InvalidOrder","message":"synthetic","#,
                        r#""incoming_message":{text}}}"#,
                    ),
                    text = text
                ))]
            }
            AloCross::Fills => {
                self.orders.push(order.clone());
                let filled = self.fill(&order.id);
                vec![
                    taker_fill(&filled),
                    order_response(&filled.order, "FILLED", true),
                ]
            }
        }
    }

    /// The frames the knobs send after the run's first `ACK`, `order`'s.
    fn knobs(&mut self, order: &Order) -> Vec<Frame> {
        let mut frames = Vec::new();
        if self.stray_order {
            let stray = Order {
                id: "77777777-7777-4777-8777-777777777777".to_owned(),
                cid: STRAY_CID.to_owned(),
                ..order.clone()
            };
            frames.push(order_response(&stray, "ACK", true));
        }
        if self.cid_absent_update {
            frames.push(order_response(order, "ACK", false));
        }
        if self.venue_cancel {
            let at = self.orders.iter().position(|o| o.id == order.id);
            let at = at.expect("the order");
            self.before_change(&order.account);
            self.orders[at].state = State::Cancelled;
            frames.push(order_response(&self.orders[at], "CANCELLED", true));
        }
        if self.stray_fill || self.maker_fill {
            let named = self.maker_fill;
            let filled = self.fill(&order.id);
            frames.push(fill_response(&filled, named));
            if named {
                frames.push(order_response(&filled.order, "FILLED", true));
            }
        }
        frames
    }

    /// Fills the open order `id` whole, as a maker.
    fn fill(&mut self, id: &str) -> Filled {
        let n = self
            .orders
            .iter()
            .position(|o| o.id == id)
            .expect("an order");
        let quantity = self.orders[n].quantity - self.orders[n].filled;
        self.fill_part(id, quantity)
    }

    /// A trade on `account` of an order the model never had, the size of the account's first
    /// order, its row naming that made-up order or none ([`QfexModel::trade_after_close`]).
    fn stray_trade(&mut self, account: Option<String>, stray: StrayTrade) {
        let first = self.orders.iter().find(|o| o.account == account).cloned();
        let template = first.expect("an order on the account");
        self.before_change(&account);
        let order = Order {
            id: "77777777-7777-4777-8777-777777777777".to_owned(),
            cid: STRAY_CID.to_owned(),
            filled: template.quantity,
            state: State::Filled,
            ..template
        };
        let filled = Filled {
            account,
            trade_id: format!("55555555-5555-4555-8555-{:012x}", self.fills.len() + 1),
            quantity: order.quantity,
            order,
            at: now_secs(),
        };
        if stray == StrayTrade::Unnamed {
            self.unnamed.push(filled.trade_id.clone());
        }
        self.fills.push(filled);
    }

    /// Fills `quantity` of the open order `id` as a maker, which ends it once nothing is left.
    fn fill_part(&mut self, id: &str, quantity: Decimal) -> Filled {
        let n = self
            .orders
            .iter()
            .position(|o| o.id == id)
            .expect("an order");
        let account = self.orders[n].account.clone();
        self.before_change(&account);
        let order = &mut self.orders[n];
        order.filled += quantity;
        if order.filled >= order.quantity {
            order.state = State::Filled;
        }
        let filled = Filled {
            account,
            trade_id: format!("55555555-5555-4555-8555-{:012x}", self.fills.len() + 1),
            order: order.clone(),
            quantity,
            at: now_secs(),
        };
        self.fills.push(filled.clone());
        filled
    }

    /// Makes `touch` the touch, pushed at once on every MDS connection open
    /// ([`QfexModel::touch_moves`]).
    fn move_touch(&mut self, touch: Touch) {
        let frame = Frame::text(touch.frame(SYMBOL));
        self.touch = touch;
        let pusher = self.mds_pusher.clone().expect("the MDS stub is up");
        let conns = self
            .views
            .as_ref()
            .map_or(0, |(_, mds)| mds.connections().len());
        tokio::spawn(async move {
            for conn in 0..conns {
                let _ = pusher.push(conn, frame.clone()).await;
            }
        });
    }

    /// Pushes a crossed touch of [`SYMBOL`] on every MDS connection open, logged as `mds
    /// crossed`.
    fn push_crossed(&mut self) {
        let crossed = Touch {
            bid: Some(("100.20", "1.00")),
            ask: Some(("100.10", "2.00")),
            age_secs: 0,
        };
        let frame = Frame::text(crossed.frame(SYMBOL));
        let mds = self.views.as_ref().map(|(_, mds)| mds.connections());
        let open: Vec<usize> = (mds.unwrap_or_default().iter().enumerate())
            .filter(|(_, c)| c.open)
            .map(|(n, _)| n)
            .collect();
        let pusher = self.mds_pusher.clone().expect("the MDS stub is up");
        self.event("mds crossed".to_owned(), None);
        tokio::spawn(async move {
            for n in open {
                let _ = pusher.push(n, frame.clone()).await;
            }
        });
    }

    /// Two trades on `account` of an order the model never had, one each way at the size of the
    /// account's first order, so its position does not move ([`QfexModel::trade_on_drop`],
    /// [`QfexModel::netting_after_close`]).
    fn netting_trades(&mut self, account: Option<String>, stray: StrayTrade) {
        self.stray_trade(account.clone(), stray);
        self.stray_trade(account, stray);
        let last = self.fills.last_mut().expect("the second stray trade");
        last.order.side = match last.order.side.as_str() {
            "SELL" => "BUY".to_owned(),
            _ => "SELL".to_owned(),
        };
    }

    /// Half of the last order placed fills as a maker now, the rest left open, its
    /// `fill_response` and its `FILLED` `order_response` pushed on the connection it was placed
    /// on: [`QfexModel::part_fill_after`]'s fill, on a later order and at a moment a test
    /// chooses by a line the probe prints, so no timer orders it (FBC-77r8).
    pub fn part_fill_last(&mut self) {
        let order = self.orders.last().cloned().expect("an order placed");
        let conn = *self
            .placed_on
            .get(&order.id)
            .expect("the connection it was placed on");
        let filled = self.fill_part(&order.id, order.quantity / Decimal::TWO);
        let frames = vec![
            fill_response(&filled, true),
            order_response(&filled.order, "FILLED", true),
        ];
        self.later(conn, frames, Duration::ZERO, false);
    }

    /// Pushes `frames` on `conn` after `after`, then, with `close`, closes it.
    fn later(&self, conn: usize, frames: Vec<Frame>, after: Duration, close: bool) {
        let pusher = self.pusher.clone().expect("the stub is up");
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            for frame in frames {
                let _ = pusher.push(conn, frame).await;
            }
            if close {
                let _ = pusher.close(conn).await;
            }
        });
    }

    /// Cancels the order `v` names, whose frame is `text`, when it is open on `conn`'s account.
    fn cancel_order(&mut self, conn: usize, v: &Value, text: &str) -> Vec<Frame> {
        self.cancels += 1;
        let first = self.cancels == 1;
        if let Some((limiter, held)) = self.hold_on_cancel.as_ref().filter(|_| first) {
            let on = fbc_core::ConnKey {
                conn: u16::MAX,
                epoch: 0,
            };
            limiter.provider_rejected(tokio::time::Instant::now(), on, None, Some(*held));
        }
        if self.cancels == 1 && self.rate_limit == Some(Limited::Cancel) {
            return vec![Frame::text(limited(text, 1))];
        }
        if self.cancels == 1 && self.close_on_cancel {
            self.later(conn, Vec::new(), Duration::ZERO, true);
            return Vec::new();
        }
        let account = self.account_of(conn);
        let id = v["params"]["order_id"].as_str().unwrap_or_default();
        let at = (self.orders.iter())
            .position(|o| o.id == id && o.account == account && o.state == State::Open);
        let Some(n) = at.filter(|_| !self.cancel_misses) else {
            let missing = serde_json::json!({"order_response": {
                "order_id": id, "status": "NO_SUCH_ORDER"
            }});
            return vec![Frame::text(missing.to_string())];
        };
        if self.cancel_lies {
            let told = Order {
                state: State::Cancelled,
                ..self.orders[n].clone()
            };
            return vec![order_response(&told, "CANCELLED", true)];
        }
        self.before_change(&account);
        self.orders[n].state = State::Cancelled;
        let answer = vec![order_response(&self.orders[n], "CANCELLED", true)];
        match self.cancel_delay.filter(|_| self.cancels == 1) {
            Some(after) => {
                self.later(conn, answer, after, false);
                Vec::new()
            }
            None => answer,
        }
    }

    /// The `get_user_orders` page `v` asks of `conn`'s account: a read socket's answered from
    /// before the account's last change while [`QfexModel::rest_lag`] holds it back, or refused
    /// as [`Limited::ReadSocket`] says.
    fn orders_page(&mut self, conn: usize, v: &Value) -> String {
        let session = self.conns.get(&conn).is_some_and(|c| c.session);
        let account = self.account_of(conn);
        if !session && self.places == 0 && self.rate_limit == Some(Limited::ReadSocketBefore) {
            self.rate_limit = None;
            let echo = serde_json::json!({"type": "get_user_orders", "params": v["params"]});
            return limited(&echo.to_string(), 3);
        }
        if !session && self.places == 0 && self.rate_limit == Some(Limited::PageKeptWholeBefore) {
            self.rate_limit = None;
            let echo = serde_json::json!({"type": "get_user_orders", "params": v["params"]});
            return limited(&echo.to_string(), 3).replace("exceeded,", "exceeded\\u002c");
        }
        if !session && self.places > 0 {
            let n = self.pages_after;
            self.pages_after += 1;
            if self.rate_limit == Some(Limited::ReadSocket(n)) {
                let echo = serde_json::json!({"type": "get_user_orders", "params": v["params"]});
                return limited(&echo.to_string(), 3);
            }
            if self.rate_limit == Some(Limited::PageKeptWhole(n)) {
                let echo = serde_json::json!({"type": "get_user_orders", "params": v["params"]});
                return limited(&echo.to_string(), 3).replace("exceeded,", "exceeded\\u002c");
            }
            if self.rate_limit == Some(Limited::ReadSocketAfterSession) && self.session_ended() {
                self.rate_limit = None;
                let echo = serde_json::json!({"type": "get_user_orders", "params": v["params"]});
                return limited(&echo.to_string(), 1);
            }
        }
        let stale = match session {
            true => None,
            false => self.stale_of(&account, |s| &mut s.pages),
        };
        let mut open = stale.map_or_else(|| self.open_of(&account), |s| s.orders);
        if !session && self.newest_first {
            open.reverse();
        }
        let mut asked = v.clone();
        if self.stuck_pages {
            asked["params"]["offset"] = serde_json::json!(0);
        }
        let mut rows: Vec<Value> = paged(&open, &asked).iter().map(row).collect();
        if !session && self.blank_cid_after_session && self.session_ended() {
            for row in &mut rows {
                row["client_order_id"] = Value::String(String::new());
            }
        }
        let twaps = match !session && self.twaps.contains(&account) {
            true => vec![serde_json::json!({"symbol": SYMBOL, "status": "ACTIVE"})],
            false => Vec::new(),
        };
        serde_json::json!({"all_orders_response": {"orders": rows, "twaps": twaps}}).to_string()
    }

    /// The `get_user_trades` page `v` asks of `conn`'s account: its fills from `start_ts`.
    fn trades_page(&self, conn: usize, v: &Value) -> String {
        let account = self.account_of(conn);
        let start = v["params"]["start_ts"].as_i64().unwrap_or(0);
        let fills: Vec<&Filled> = (self.fills.iter())
            .filter(|f| f.account == account && f.at >= start)
            .collect();
        let mut asked = v.clone();
        if self.stuck_trades {
            asked["params"]["offset"] = serde_json::json!(0);
        }
        let row = |f: &&Filled| trade_row(f, self.unnamed.contains(&f.trade_id));
        let rows: Vec<Value> = paged(&fills, &asked).iter().map(row).collect();
        serde_json::json!({"user_trades_response": rows}).to_string()
    }

    /// A made-up order UUID, unique in the run.
    fn mint_id(&mut self) -> String {
        self.next_id += 1;
        format!("11111111-1111-4111-8111-{:012x}", self.next_id)
    }

    /// Whether trade socket `conn` opened alone: every trade socket before it closed by the
    /// client, no MDS connection and no `/refdata` read.
    fn alone(&self, conn: usize) -> bool {
        let Some((trade, mds)) = self.views.as_ref() else {
            return false;
        };
        let before = trade.connections();
        let closed = before[..conn].iter().all(|c| c.client_closed);
        let refdata = self.log.iter().any(|e| e.what == "GET /refdata");
        closed && mds.connections().is_empty() && !refdata
    }

    /// The answer to frame `frame` on MDS connection `conn`: the touch of each symbol a
    /// subscribe names, or an err with [`QfexModel::mds_err`].
    pub fn mds(&mut self, conn: usize, frame: &Frame) -> Responded {
        let v = parsed(frame)?;
        self.event(format!("mds {conn} {}", kind_of(&v)), None);
        if self.mds_silent {
            return Ok(Vec::new());
        }
        if self.mds_err {
            let err = r#"{"err":{"error_code":"ServerError","message":"synthetic text"}}"#;
            return Ok(vec![Frame::text(err)]);
        }
        let symbols = v["symbols"].as_array().cloned().unwrap_or_default();
        let named = symbols.iter().filter_map(Value::as_str);
        Ok(named.map(|s| Frame::text(self.touch.frame(s))).collect())
    }
}

/// `quantity` as a JSON number, exactly as written.
fn number(quantity: Decimal) -> Value {
    Value::Number(serde_json::Number::from_str(&quantity.normalize().to_string()).unwrap())
}

/// An HTTP answer of `status` with `body`.
fn status(status: u16, body: &str) -> HttpReply {
    HttpReply {
        status,
        body: body.as_bytes().to_vec(),
    }
}

/// A `RateLimited` err echoing `request`, its hint `secs` seconds, as QFEX's rate page shows one.
fn limited(request: &str, secs: u32) -> String {
    format!(
        r#"{{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after {secs} seconds","incoming_message":{request}}}"#
    )
}

/// The items of `all` the page `v` asks for: from its `offset`, at most its `limit`.
fn paged<T: Clone>(all: &[T], v: &Value) -> Vec<T> {
    let offset = v["params"]["offset"].as_u64().unwrap_or(0);
    let limit = v["params"]["limit"].as_u64().unwrap_or(1000);
    let from = usize::try_from(offset).unwrap_or(usize::MAX);
    let take = usize::try_from(limit).unwrap_or(usize::MAX);
    all.iter().skip(from).take(take).cloned().collect()
}

/// `order`'s members as an `order_response` and a page's row both give them, its client id
/// among them with `cid`.
fn order_members(order: &Order, status: &str, cid: bool) -> Value {
    let mut members = serde_json::json!({
        "order_id": order.id, "symbol": SYMBOL, "status": status,
        "quantity": number(order.quantity), "price": order.price, "side": order.side,
        "type": order.kind, "time_in_force": order.tif, "user_id": USER_ID,
        "quantity_remaining": number(order.quantity - order.filled),
        "update_time": now_secs(), "trade_id": null
    });
    if cid {
        members["client_order_id"] = Value::String(order.cid.clone());
    }
    members
}

/// `order`'s `order_response` with `status`, naming its client id with `cid`.
fn order_response(order: &Order, status: &str, cid: bool) -> Frame {
    let mut members = order_members(order, status, cid);
    members["take_profit"] = serde_json::json!(0.0);
    members["stop_loss"] = serde_json::json!(0.0);
    Frame::text(serde_json::json!({ "order_response": members }).to_string())
}

/// `order`'s `order_response` with `status`, its client id written as `echo` says
/// ([`QfexModel::ack_echo`]).
fn echoed(order: &Order, status: &str, echo: Echo) -> Frame {
    let mut members = order_members(order, status, false);
    members["take_profit"] = serde_json::json!(0.0);
    members["stop_loss"] = serde_json::json!(0.0);
    let cid = &order.cid;
    let written = match echo {
        Echo::Exact | Echo::Escaped => Some(Value::String(cid.clone())),
        Echo::Upper => Some(Value::String(cid.to_uppercase())),
        Echo::Absent => None,
        Echo::Null => Some(Value::Null),
        Echo::Empty => Some(Value::String(String::new())),
    };
    if let Some(written) = written {
        members["client_order_id"] = written;
    }
    let text = serde_json::json!({ "order_response": members }).to_string();
    let first = cid.chars().next().filter(|_| echo == Echo::Escaped);
    let text = match first {
        Some(first) => {
            let plain = format!(r#""client_order_id":"{cid}""#);
            let rest = &cid[first.len_utf8()..];
            let escaped = format!(r#""client_order_id":"\u{:04x}{rest}""#, u32::from(first));
            text.replacen(&plain, &escaped, 1)
        }
        None => text,
    };
    Frame::text(text)
}

/// `order`'s row on a `get_user_orders` page.
fn row(order: &Order) -> Value {
    order_members(order, "ACK", true)
}

/// `filled`'s `fill_response` as a taker's ([`AloCross::Fills`]), naming its client id.
fn taker_fill(filled: &Filled) -> Frame {
    let Frame::Text(text) = fill_response(filled, true) else {
        unreachable!("a text frame")
    };
    let mut v: Value = serde_json::from_str(&text).expect("the model's own frame");
    v["fill_response"]["execution_type"] = Value::String("TAKER".to_owned());
    Frame::text(v.to_string())
}

/// `filled`'s `fill_response`, a maker's, naming the order's client id with `cid`.
fn fill_response(filled: &Filled, cid: bool) -> Frame {
    let order = &filled.order;
    let mut members = serde_json::json!({
        "trade_id": filled.trade_id, "user_id": USER_ID, "symbol": SYMBOL,
        "price": order.price, "quantity": number(filled.quantity), "side": order.side,
        "aggressor_side": if order.side == "BUY" { "SELL" } else { "BUY" },
        "order_id": order.id, "fee": 0.001, "order_type": order.kind, "tif": order.tif,
        "order_price": order.price, "remaining_quantity": number(order.quantity - order.filled),
        "take_profit": 0.0,
        "stop_loss": 0.0, "execution_type": "MAKER", "timestamp": filled.at, "realised_pnl": 0.0
    });
    if cid {
        members["client_order_id"] = Value::String(order.cid.clone());
    }
    Frame::text(serde_json::json!({ "fill_response": members }).to_string())
}

/// `filled`'s row on a `get_user_trades` page, with neither order id when `unnamed`.
fn trade_row(filled: &Filled, unnamed: bool) -> Value {
    let order = &filled.order;
    let mut row = serde_json::json!({
        "trade_id": filled.trade_id, "symbol": SYMBOL, "price": order.price,
        "quantity": number(filled.quantity), "side": order.side, "order_id": order.id,
        "client_order_id": order.cid, "fee": 0.001, "timestamp": filled.at,
        "order_type": order.kind, "tif": order.tif
    });
    if unnamed {
        let members = row.as_object_mut().expect("an object");
        members.remove("order_id");
        members.remove("client_order_id");
    }
    row
}

/// A frame as JSON, refused when it is not a JSON text frame.
fn parsed(frame: &Frame) -> Result<Value, String> {
    let Frame::Text(text) = frame else {
        return Err("a binary frame".to_owned());
    };
    serde_json::from_str(text).map_err(|e| e.to_string())
}

/// A frame's `type`, `none` when it has none.
fn kind_of(v: &Value) -> String {
    v["type"].as_str().unwrap_or("none").to_owned()
}

/// What the probe printed, shared with the model, which stamps each event with it.
#[derive(Clone, Default)]
pub struct Printed(Arc<Mutex<Vec<u8>>>);

impl Printed {
    /// Everything printed so far.
    pub fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for Printed {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The model, running: its trade and REST stub and its MDS stub, each on a serving script.
pub struct Venue {
    pub trade: StubServer,
    pub mds: StubServer,
    pub model: Arc<Mutex<QfexModel>>,
}

impl Venue {
    /// `model`, served.
    pub async fn start(model: QfexModel) -> Venue {
        let model = Arc::new(Mutex::new(model));
        let m = Arc::clone(&model);
        let (script, push) = WsScript::serving(move |conn| {
            let m = Arc::clone(&m);
            Responder::new(move |frame| m.lock().unwrap().trade(conn, frame))
        });
        model.lock().unwrap().pusher = Some(push);
        let mut router = HttpRouter::new();
        for path in [
            "/refdata",
            "/user/positions",
            "/user/subaccounts",
            "/user/historic-orders",
        ] {
            let m = Arc::clone(&model);
            let answer = move |req: &HttpRequest| m.lock().unwrap().rest(req);
            router = router.route_fn(Method::GET, PathPattern::exact(path), answer);
        }
        let trade = StubServer::start(script, router).await.unwrap();
        let m = Arc::clone(&model);
        let (script, push) = WsScript::serving(move |conn| {
            let m = Arc::clone(&m);
            Responder::new(move |frame| m.lock().unwrap().mds(conn, frame))
        });
        model.lock().unwrap().mds_pusher = Some(push);
        let mds = StubServer::start(script, HttpRouter::new()).await.unwrap();
        model.lock().unwrap().views = Some((trade.connections_view(), mds.connections_view()));
        Venue { trade, mds, model }
    }

    /// Every request and connection the model saw, in order.
    pub fn log(&self) -> Vec<Event> {
        self.model.lock().unwrap().log.clone()
    }

    /// Sends each answer [`QfexModel::hold_trades`] held, in order, on its connection.
    pub async fn release_held(&self) {
        let (held, pusher) = {
            let mut model = self.model.lock().unwrap();
            (std::mem::take(&mut model.held), model.pusher.clone())
        };
        let pusher = pusher.expect("the stub is up");
        for (conn, frame) in held {
            let _ = pusher.push(conn, frame).await;
        }
    }

    /// What the probe printed.
    pub fn printed(&self) -> Printed {
        self.model.lock().unwrap().printed.clone()
    }

    /// Asserts neither stub failed an answer: every frame the probe sent was one the model
    /// answers.
    pub fn assert_served(&self) {
        assert!(
            self.trade.failures().is_empty(),
            "{:?}",
            self.trade.failures()
        );
        assert!(self.mds.failures().is_empty(), "{:?}", self.mds.failures());
    }
}
