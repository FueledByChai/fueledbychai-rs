//! The QFEX model the place stages' tests run against (FBC-c6kw, FBC-23ch; story BT-702):
//! QFEX's REST reads, its trade sockets, sessions and reads alike, and its MDS touch, each
//! answered as QFEX's pages show them, on the conformance stub's serving scripts (decision
//! 0150), since the place stages' connections, several at once, send what no timeline can know.
//! fbc-conformance stays venue-free: the model is this test's own. Later slices extend it with
//! crossings, IOCs and paging (FBC-77r8 and after).
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
//!   (account-wide, decision 0121), seen at the model's next answer.
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
//!   [`QfexModel::position_after_close`], [`QfexModel::cancel_place`],
//!   [`QfexModel::venue_cancel`],
//!   [`QfexModel::close_on_cancel`], [`QfexModel::stray_after`],
//!   [`QfexModel::part_fill_after`], [`QfexModel::close_after`] and
//!   [`QfexModel::fill_on_trades`]; at the first trade-socket authentication,
//!   [`QfexModel::touch_moves`]; on every read socket, [`QfexModel::auth_fails`],
//!   [`QfexModel::stuck_pages`], [`QfexModel::stuck_trades`] and [`QfexModel::hold_trades`], and
//!   on every read socket's pages [`QfexModel::newest_first`], after the first `add_order`
//!   [`QfexModel::page_delay`] and, once an armed connection ended,
//!   [`QfexModel::blank_cid_after_session`]. With [`QfexModel::stray_order`] or
//!   [`QfexModel::cid_absent_update`], a read socket's page waits from the first `add_order`
//!   until the end of the connection it came on is seen ([`QfexModel::knob_frames_unread`]).
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
    /// Each trade connection seen, by its number.
    pub conns: HashMap<usize, Conn>,
    /// Every order rested and every fill made, in order.
    pub orders: Vec<Order>,
    pub fills: Vec<Filled>,
    /// Whether a session's connection subscribed while another session's was open.
    pub overlapped: bool,
    /// Graceful close and drop: whether an armed connection's end cancels its account's open
    /// orders (decision 0121's account-wide cancel-on-disconnect).
    pub cod_on_close: bool,
    pub cod_on_drop: bool,
    /// The first `add_order`'s answer and its knobs' frames are pushed this long after it, past
    /// the request's deadline if need be, instead of answering it at once.
    pub ack_delay: Option<Duration>,
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
    /// After each change to an account's orders or position, its next `rest_lag` read-socket
    /// pages and REST positions each answer what it was before the change.
    pub rest_lag: usize,
    /// The `GET /user/positions` reads after the first `add_order`, from 0, answered HTTP 500.
    pub rest_fails: Vec<usize>,
    /// The first `cancel_order`'s answer is pushed this long after it, instead of at once.
    pub cancel_delay: Option<Duration>,
    /// Once an armed connection ended, its account's positions show a position on [`SYMBOL`]
    /// that no order or fill of the model made, as a hand trade would.
    pub position_after_close: bool,
    /// From the first `add_order` on, its account's positions show that position too.
    pub position_on_place: bool,
    /// With the first `add_order`, its account also holds an open order it did not place, under
    /// [`STRAY_CID`], which its pages list and no frame names, as another trader's would.
    pub foreign_order: bool,
    /// As it reads the first `add_order`, before answering it, the model holds this limiter's
    /// every frame budget for the time given, as a provider refusal echoing nothing would: the
    /// run's shared limiter, under the run's provider user.
    pub hold_on_place: Option<(fbc_runtime::RateLimiter, Duration)>,
    /// Whether every authentication is answered a failure.
    pub auth_fails: bool,
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
    /// The MDS stub's push handle, once it is up ([`QfexModel::touch_moves`]).
    pub mds_pusher: Option<Pusher>,
    /// The answers [`QfexModel::hold_trades`] held: each connection and frame.
    pub(super) held: Vec<(usize, Frame)>,
    /// The connection the first `add_order` came on.
    pub(super) placed_on: Option<usize>,
    /// The read sockets' pages [`QfexModel::knob_frames_unread`] held: each connection and
    /// frame.
    pub(super) held_pages: Vec<(usize, Frame)>,
    pub(super) lagging: HashMap<Option<String>, Stale>,
    /// The accounts [`QfexModel::position_after_close`] moved.
    pub(super) moved: Vec<Option<String>>,
    pub(super) places: usize,
    pub(super) cancels: usize,
    /// Read-socket pages and REST positions answered since the first `add_order`.
    pub(super) pages_after: usize,
    pub(super) rest_after: usize,
    /// Trade-socket authentications answered since the first `add_order`, a session's included
    /// ([`Limited::ReadAuth`]).
    pub(super) auths_after: usize,
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
            conns: HashMap::new(),
            orders: Vec::new(),
            fills: Vec::new(),
            overlapped: false,
            cod_on_close: false,
            cod_on_drop: true,
            ack_delay: None,
            rate_limit: None,
            stray_order: false,
            cid_absent_update: false,
            stray_fill: false,
            maker_fill: false,
            server_close: false,
            close_on_place: false,
            arm_err: false,
            rest_lag: 0,
            rest_fails: Vec::new(),
            cancel_delay: None,
            position_after_close: false,
            position_on_place: false,
            foreign_order: false,
            hold_on_place: None,
            auth_fails: false,
            stuck_pages: false,
            cancel_place: false,
            venue_cancel: false,
            close_on_cancel: false,
            hold_trades: false,
            stray_after: None,
            part_fill_after: None,
            close_after: None,
            fill_on_trades: false,
            touch_moves: None,
            blank_cid_after_session: false,
            newest_first: false,
            page_delay: None,
            stuck_trades: false,
            mds_pusher: None,
            held: Vec::new(),
            placed_on: None,
            held_pages: Vec::new(),
            lagging: HashMap::new(),
            moved: Vec::new(),
            places: 0,
            cancels: 0,
            pages_after: 0,
            rest_after: 0,
            auths_after: 0,
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
                if after && self.rest_fails.contains(&n) {
                    return status(500, r#"{"error":"synthetic failure"}"#);
                }
                ok(&self.positions_of(account))
            }
            "/user/subaccounts" => listing_refused(),
            _ => ok(&self.historic(account)),
        }
    }

    /// `account`'s ended orders, as `GET /user/historic-orders` lists them.
    fn historic(&self, account: Option<String>) -> String {
        let ended = (self.orders.iter()).filter(|o| o.account == account && o.state != State::Open);
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
        let open = self.open_of(account).len();
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
            "cancel_on_disconnect" if self.arm_err => Ok(vec![Frame::text(format!(
                r#"{{"type":"Err","error_code":"InvalidRequest","message":"synthetic","incoming_message":{text}}}"#
            ))]),
            "cancel_on_disconnect" => {
                self.conns.entry(conn).or_default().armed = true;
                Ok(vec![Frame::text(format!(
                    r#"{{"ack":{{"incoming_message":{text}}}}}"#
                ))])
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
            "get_user_trades" if self.hold_trades => {
                let page = Frame::text(self.trades_page(conn, &v));
                self.held.push((conn, page));
                Ok(Vec::new())
            }
            "get_user_trades" => {
                if std::mem::take(&mut self.fill_on_trades) {
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
    pub(super) fn knob_frames_unread(&self) -> bool {
        let knob = self.stray_order || self.cid_absent_update;
        knob && self.placed_on.is_some_and(|n| !self.conn_ended(n))
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

    /// Applies cancel-on-disconnect to each armed connection whose end the stub has seen since:
    /// a graceful close (the client's close frame read) cancels every open order of its account
    /// with [`QfexModel::cod_on_close`], and a drop with [`QfexModel::cod_on_drop`].
    fn settle(&mut self) {
        let Some((trade, _)) = &self.views else {
            return;
        };
        let records = trade.connections();
        let mut ended = Vec::new();
        for (n, conn) in &mut self.conns {
            let Some(record) = records.get(*n) else {
                continue;
            };
            if conn.armed && !conn.settled && (record.client_closed || !record.open) {
                conn.settled = true;
                ended.push((*n, conn.account.clone(), record.client_closed));
            }
        }
        ended.sort_by_key(|(n, ..)| *n);
        for (n, account, graceful) in ended {
            let (how, cancels) = match graceful {
                true => ("close", self.cod_on_close),
                false => ("drop", self.cod_on_drop),
            };
            if cancels {
                self.before_change(&account);
                let open = self.orders.iter_mut();
                let open = open.filter(|o| o.account == account && o.state == State::Open);
                open.for_each(|o| o.state = State::Cancelled);
            }
            if self.position_after_close && !self.moved.contains(&account) {
                self.moved.push(account.clone());
            }
            let did = if cancels { "cancels" } else { "keeps" };
            self.event(format!("cod {n} {how} {did}"), None);
        }
        self.release_pages();
    }

    /// Rests the order `v`, whose frame is `text`, on `conn`'s account and answers it, with the
    /// knobs' frames after its `ACK` on the run's first.
    fn add_order(&mut self, conn: usize, v: &Value, text: &str) -> Vec<Frame> {
        self.places += 1;
        let first = self.places == 1;
        if first {
            self.placed_on = Some(conn);
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
        let order = Order {
            id: self.mint_id(),
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
        if first && self.cancel_place {
            let ended = Order {
                state: State::Cancelled,
                ..order
            };
            self.orders.push(ended.clone());
            return vec![order_response(&ended, "CANCELLED", true)];
        }
        self.orders.push(order.clone());
        let mut frames = vec![order_response(&order, "ACK", true)];
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
        match (self.ack_delay.filter(|_| first), close) {
            (None, false) => frames,
            (after, _) => {
                self.later(conn, frames, after.unwrap_or_default(), close);
                Vec::new()
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
        let Some(n) = at else {
            let missing = serde_json::json!({"order_response": {
                "order_id": id, "status": "NO_SUCH_ORDER"
            }});
            return vec![Frame::text(missing.to_string())];
        };
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
        serde_json::json!({"all_orders_response": {"orders": rows, "twaps": []}}).to_string()
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
        let rows: Vec<Value> = paged(&fills, &asked).iter().map(|f| trade_row(f)).collect();
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

/// `order`'s row on a `get_user_orders` page.
fn row(order: &Order) -> Value {
    order_members(order, "ACK", true)
}

/// `filled`'s `fill_response`, naming the order's client id with `cid`.
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

/// `filled`'s row on a `get_user_trades` page.
fn trade_row(filled: &Filled) -> Value {
    let order = &filled.order;
    serde_json::json!({
        "trade_id": filled.trade_id, "symbol": SYMBOL, "price": order.price,
        "quantity": number(filled.quantity), "side": order.side, "order_id": order.id,
        "client_order_id": order.cid, "fee": 0.001, "timestamp": filled.at,
        "order_type": order.kind, "tif": order.tif
    })
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
