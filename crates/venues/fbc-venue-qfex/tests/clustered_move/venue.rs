//! The stub QFEX the clustered-move benchmark's order entry runs against: fbc-conformance's stub
//! server, started on a thread and runtime of its own, answering each frame it reads on the
//! trade socket by its `type`, in the shapes `tests/liveness.rs` and `tests/probe_harness/` give
//! (QFEX's trade AsyncAPI, the authenticate page and FBC-291a's capture of the arm's answer):
//!
//! - `auth`: the authenticate page's success reply;
//! - `subscribe`: the AsyncAPI's `{"subscribed":…}` confirmation of the channel it names;
//! - `cancel_on_disconnect`: the arm's echo in the AsyncAPI's generic `ack`, as QFEX production
//!   answered it (`fixtures/qfex/exec-recorded/arm-production/README.md`), which the codec
//!   accepts at Final (decision 0132 item 1);
//! - `get_user_orders`: one empty `all_orders_response` page, short at any size, so the resync's
//!   open-order reads end (decision 0132 item 5);
//! - `add_order`: an `order_response` `ACK` echoing the order's client id under a venue order
//!   UUID of the stub's, which the codec decodes into the order's update, `Open` (item 8);
//! - `cancel_order`: an `order_response` `CANCELLED` naming the order, nothing filled, which
//!   settles the cancel at Final before the order's update (item 8).
//!
//! Any other type fails the script. Its HTTP route answers the resync's `GET /user/positions`
//! flat. Every id and value is made up; `user_id` is the harness's synthetic one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, ScriptError, Step, StubServer, WsScript,
};
use fbc_runtime::http::Method;
use serde_json::Value;

use super::probe_harness::{order_frame, quoted};

/// The authenticate page's success reply.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;
/// The arm's echo in the AsyncAPI's generic `ack`, as QFEX production answered the arm.
const ARM_ACK: &str = r#"{"ack":{"incoming_message":{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}}}"#;
/// The resync's one page: empty, so short at any page size, and its last.
const PAGE: &str = r#"{"all_orders_response":{"orders":[],"twaps":[]}}"#;
/// The account's positions on the REST route: flat, holding none.
const FLAT: &str = r#"{"balance":{},"positions":[]}"#;

/// One order the stub holds.
#[derive(Clone, Debug)]
pub struct Order {
    pub cid: String,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub open: bool,
}

/// What the stub's venue holds: its orders by venue order id, and the counters.
#[derive(Default)]
pub struct Book {
    pub orders: BTreeMap<String, Order>,
    next_vid: u64,
    /// How many orders the stub has been asked to place.
    pub creates: u64,
    /// Called with [`Book::creates`] each time the stub reads an `add_order`, once it has built
    /// its answer and before it sends it: how the benchmark sends its reference frames then.
    pub on_create: Option<Box<dyn FnMut(u64) + Send>>,
}

impl Book {
    /// A venue order UUID of the stub's, version 4 in form.
    fn vid(&mut self) -> String {
        self.next_vid += 1;
        format!("0000c1a5-0000-4000-8000-{:012x}", self.next_vid)
    }
}

pub type Shared = Arc<Mutex<Book>>;

pub fn lock(book: &Shared) -> MutexGuard<'_, Book> {
    book.lock().unwrap_or_else(|e| e.into_inner())
}

/// `params`' member `name`, a JSON number or string, as JSON text.
fn raw(params: &Value, name: &str) -> Result<String, String> {
    match &params[name] {
        Value::Number(n) => Ok(n.to_string()),
        Value::String(s) => Ok(s.clone()),
        other => Err(format!("{name} is {other}")),
    }
}

/// `order` as an `order_response` with `status` and `remaining` left, under `vid`.
fn order_response(vid: &str, order: &Order, status: &str, remaining: &str) -> Frame {
    let (vid, cid) = (quoted(vid), quoted(&order.cid));
    let (symbol, side, status) = (quoted(&order.symbol), quoted(&order.side), quoted(status));
    Frame::text(order_frame(&[
        ("order_id", Some(&vid)),
        ("symbol", Some(&symbol)),
        ("status", Some(&status)),
        ("quantity", Some(&order.quantity)),
        ("price", Some(&order.price)),
        ("side", Some(&side)),
        ("type", Some("\"ALO\"")),
        ("client_order_id", Some(&cid)),
        ("quantity_remaining", Some(remaining)),
    ]))
}

/// Answers each trade-socket frame by its `type` (module documentation).
pub fn responder(book: Shared) -> Responder {
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary frame from the client".to_owned());
        };
        let req: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let params = &req["params"];
        let mut book = lock(&book);
        match req["type"].as_str().unwrap_or("") {
            "auth" => Ok(vec![Frame::text(AUTH_OK)]),
            "subscribe" => {
                let channel = params["channels"][0].as_str().ok_or("no channel")?;
                Ok(vec![Frame::text(format!(
                    r#"{{"subscribed":"{channel}"}}"#
                ))])
            }
            "cancel_on_disconnect" => Ok(vec![Frame::text(ARM_ACK)]),
            "get_user_orders" => Ok(vec![Frame::text(PAGE)]),
            "add_order" => {
                let order = Order {
                    cid: params["client_order_id"]
                        .as_str()
                        .ok_or("no client_order_id")?
                        .to_owned(),
                    symbol: params["symbol"].as_str().ok_or("no symbol")?.to_owned(),
                    side: params["side"].as_str().ok_or("no side")?.to_owned(),
                    price: raw(params, "price")?,
                    quantity: raw(params, "quantity")?,
                    open: true,
                };
                let vid = book.vid();
                let ack = order_response(&vid, &order, "ACK", &order.quantity);
                book.orders.insert(vid, order);
                book.creates += 1;
                let created = book.creates;
                if let Some(hook) = book.on_create.as_mut() {
                    hook(created);
                }
                Ok(vec![ack])
            }
            "cancel_order" => {
                if params["cancel_order_id_type"] != "order_id" {
                    return Err(format!("a cancel not by order id: {params}"));
                }
                let vid = params["order_id"].as_str().ok_or("no order_id")?;
                let order = book
                    .orders
                    .get_mut(vid)
                    .filter(|o| o.open)
                    .ok_or("a cancel of no order resting at the stub")?;
                order.open = false;
                let order = order.clone();
                Ok(vec![order_response(
                    vid,
                    &order,
                    "CANCELLED",
                    &order.quantity,
                )])
            }
            other => Err(format!("an unexpected type {other}")),
        }
    })
}

/// The stub's REST route: [`FLAT`] for every positions read.
fn routes() -> HttpRouter {
    HttpRouter::new().route_fn(
        Method::GET,
        PathPattern::exact("/user/positions"),
        move |_| HttpReply {
            status: 200,
            body: FLAT.as_bytes().to_vec(),
        },
    )
}

/// The stub, running on a thread of its own until dropped: one connection, whose first
/// `frames` frames it answers.
pub struct Stub {
    stub: Option<StubServer>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Stub {
    pub fn start(book: &Shared, frames: usize) -> Stub {
        let with = responder(book.clone());
        let mut steps = vec![Step::Accept];
        steps.extend((0..frames).map(|_| Step::Respond {
            conn: 0,
            with: with.clone(),
        }));
        let script = WsScript::new(steps);
        let (started_tx, started) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let stub = StubServer::start(script, routes()).await.unwrap();
                started_tx.send(stub).unwrap();
                // Its tasks run on this runtime until the test is done with it.
                let _ = stopped.await;
            });
        });
        let stub = started.recv().unwrap();
        Stub {
            stub: Some(stub),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn server(&self) -> &StubServer {
        self.stub.as_ref().unwrap()
    }

    /// `ws://127.0.0.1:<port>`, a loopback origin the probe's guard admits.
    pub fn ws_url(&self) -> String {
        self.server().ws_url("")
    }

    /// `http://127.0.0.1:<port>`.
    pub fn rest_url(&self) -> String {
        self.server().http_url("")
    }

    /// How many connections the socket endpoint accepted.
    pub fn connections(&self) -> usize {
        self.server().connections().len()
    }

    /// The JSON requests received on the one connection, in order.
    pub fn requests(&self) -> Vec<Value> {
        self.server()
            .connections()
            .first()
            .map(|c| {
                c.received
                    .iter()
                    .filter_map(|f| match f {
                        Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
                        Frame::Binary(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Waits until the script has played to its end or failed: its outcome.
    pub async fn finished(&self) -> Result<(), ScriptError> {
        self.server().finished().await
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        drop(self.stub.take());
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
