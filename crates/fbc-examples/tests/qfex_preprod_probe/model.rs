//! The QFEX model the place stages' tests run against (FBC-c6kw; story BT-702), its first part:
//! QFEX's REST reads, its never-arming trade sockets and its MDS touch, each answered as QFEX's
//! pages show them, on the conformance stub's serving scripts (decision 0150), since the place
//! stages' connections, several at once, send what no timeline can know. fbc-conformance stays
//! venue-free: the model is this test's own. Later slices extend it with the trade sessions,
//! cancel-on-disconnect, crossings, fills and paging (FBC-23ch and after).
//!
//! - **State** ([`QfexModel`], behind `Arc<Mutex<..>>`): the instruments `/refdata` lists (or,
//!   with [`QfexModel::refdata`], the status and body it answers in their place), each
//!   account's positions with a lag knob (each account's first [`QfexModel::lag`] reads answer
//!   its stale body), the `get_user_orders` page, the touch, and a log of every request and
//!   connection in arrival order, each with whether the probe had printed its `CAPS` line by
//!   then and, for a trade socket's authentication, whether it opened alone.
//! - **REST** ([`QfexModel::rest`]): `GET /refdata`, `GET /user/positions` by the
//!   `x-qfex-requested-account-id` header (`None` for the primary account),
//!   `GET /user/subaccounts` refused HTTP 403 as QFEX refused a key scoped to one account
//!   (FBC-291a's first production run; decision 0144), and `GET /user/historic-orders`, empty.
//! - **Trade sockets** ([`QfexModel::trade`]): authentication, the three subscriptions and a
//!   `get_user_orders` page answered; nothing arms, and any other frame is refused, failing that
//!   answer as the serving script reports it.
//! - **MDS** ([`QfexModel::mds`]), on a stub of its own at its own loopback origin: a `bbo`
//!   subscribe answered with the touch of each symbol it names, or, with [`QfexModel::mds_err`],
//!   with a `ServerError` err, which ends the connection's epoch (the MDS codec asks to
//!   reconnect).
//!
//! Every symbol, key, id and value is made up.

use super::*;

use std::sync::Mutex;

use fbc_conformance::{ConnectionsView, HttpRequest, Responded};

/// A made-up QFEX symbol, which no account trades.
pub const SYMBOL: &str = "SYN-USD";

/// A flat account with no row: the documented members alone.
pub const FLAT: &str = r#"{"balance":{},"positions":[]}"#;

/// An account holding a position on [`SYMBOL`], with one open order counted.
pub const LONG: &str =
    r#"{"balance":{},"positions":[{"symbol":"SYN-USD","position":1,"open_orders":1}]}"#;

/// No ended order: `GET /user/historic-orders`'s documented members, empty.
pub const NO_HISTORIC: &str = r#"{"count":0,"data":[]}"#;

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
    /// The `get_user_orders` page every read socket is answered.
    pub page: String,
    /// What `/refdata` answers in place of the listing, when set: a status and a body.
    pub refdata: Option<(u16, &'static str)>,
    /// The touch, and whether the MDS answers a subscribe with an err instead.
    pub touch: Touch,
    pub mds_err: bool,
    pub log: Vec<Event>,
    /// What the probe printed, to stamp each event with whether `CAPS` came first.
    pub printed: Printed,
    /// The trade and MDS stubs' connections, once both are up ([`Venue::start`]).
    pub views: Option<(ConnectionsView, ConnectionsView)>,
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
            page: EMPTY_PAGE.to_owned(),
            refdata: None,
            touch: Touch::fresh(),
            mds_err: false,
            log: Vec::new(),
            printed: Printed::default(),
            views: None,
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
            "/user/positions" => ok(&self.positions_of(account)),
            "/user/subaccounts" => listing_refused(),
            _ => ok(NO_HISTORIC),
        }
    }

    /// `account`'s positions: its stale body for its first [`QfexModel::lag`] reads, then its
    /// current one.
    pub fn positions_of(&mut self, account: Option<String>) -> String {
        let read = self.reads.entry(account.clone()).or_default();
        *read += 1;
        let lagging = *read <= self.lag;
        let bodies = if lagging {
            &self.stale
        } else {
            &self.positions
        };
        bodies[&account].clone()
    }

    /// The answer to frame `frame` on trade socket `conn`: authentication, a subscription and a
    /// `get_user_orders` page; anything else refused.
    pub fn trade(&mut self, conn: usize, frame: &Frame) -> Responded {
        let kind = kind_of(&parsed(frame)?);
        let alone = (kind == "auth").then(|| self.alone(conn));
        self.event(format!("trade {conn} {kind}"), alone);
        match kind.as_str() {
            "auth" => Ok(vec![Frame::text(AUTH_OK)]),
            "subscribe" => confirmation(frame),
            "get_user_orders" => Ok(vec![Frame::text(self.page.clone())]),
            _ => Err(format!("the model answers no {kind} yet (FBC-23ch)")),
        }
    }

    /// Whether trade socket `conn` opened alone: every trade socket before it closed by the
    /// client, no MDS connection and no `/refdata` read.
    fn alone(&self, conn: usize) -> bool {
        let (trade, mds) = self.views.as_ref().expect("the stubs are up");
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
        if self.mds_err {
            let err = r#"{"err":{"error_code":"ServerError","message":"synthetic text"}}"#;
            return Ok(vec![Frame::text(err)]);
        }
        let symbols = v["symbols"].as_array().cloned().unwrap_or_default();
        let named = symbols.iter().filter_map(Value::as_str);
        Ok(named.map(|s| Frame::text(self.touch.frame(s))).collect())
    }
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
        let (script, _push) = WsScript::serving(move |conn| {
            let m = Arc::clone(&m);
            Responder::new(move |frame| m.lock().unwrap().trade(conn, frame))
        });
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
        let (script, _push) = WsScript::serving(move |conn| {
            let m = Arc::clone(&m);
            Responder::new(move |frame| m.lock().unwrap().mds(conn, frame))
        });
        let mds = StubServer::start(script, HttpRouter::new()).await.unwrap();
        model.lock().unwrap().views = Some((trade.connections_view(), mds.connections_view()));
        Venue { trade, mds, model }
    }

    /// Every request and connection the model saw, in order.
    pub fn log(&self) -> Vec<Event> {
        self.model.lock().unwrap().log.clone()
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
