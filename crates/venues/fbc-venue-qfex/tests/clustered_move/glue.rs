//! The glue between the clustered-move benchmark's order-entry session and fbc-oms, as a
//! consumer writes it (the shape of Paradex's `tests/rehearsal/glue.rs`, cut to what this run
//! meets): an [`ExecHandler`] that routes every event into the account's [`Registry`] (order
//! updates by client id or venue id under the ingest order, the probe caps declaring no venue
//! ordering key; outcomes by the request they name; the `Resync*` events collected into one
//! [`ResyncSnapshot`] applied at `ResyncEnd`), and the one path commands take to the session, so
//! the registry knows which request each answer names. Anything it cannot apply, or that this
//! run never sends (a fill, an asynchronous reject, an error naming no request), is kept in
//! [`Glue::problems`], which the benchmark holds empty.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fbc_core::{
    ClientOrderId, ConnKey, Envelope, ExecEvent, ItemRef, MonoNs, OrderCaps, RpcId, SubmitHandle,
    SubmitOutcome, VenueCommand, WallNs,
};
use fbc_oms::{Authorization, LadderConfig, OrderKey, OrderOp, Registry, ResyncSnapshot, Routed};
use fbc_runtime::{ExecHandler, ExecOrders};

/// The one item of a command this run submits.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Item {
    Place(ClientOrderId),
    Cancel(ClientOrderId),
}

pub struct Glue {
    pub reg: Registry,
    caps: OrderCaps,
    ladder: LadderConfig,
    sent: HashMap<RpcId, Item>,
    /// The items of the commands planned on the shard host's ladder ([`Glue::planned`]), in the
    /// order pushed, not yet reported submitted: the host submits them in that order, so a
    /// submission reported for a request [`Glue::submit`] did not name is the next of these
    /// (decision 0100).
    planned: VecDeque<Item>,
    collecting: Option<ResyncSnapshot>,
    authenticated_at: Option<MonoNs>,
    origin: Instant,
    /// The commands authorized by fbc-oms and handed to the session: the ladder's places and
    /// the handler's cancels.
    pub authorized: Vec<Item>,
    /// The resyncs applied.
    pub resyncs: u32,
    pub problems: Vec<String>,
}

impl Glue {
    pub fn new(reg: Registry, caps: OrderCaps, ladder: LadderConfig) -> Glue {
        Glue {
            reg,
            caps,
            ladder,
            sent: HashMap::new(),
            planned: VecDeque::new(),
            collecting: None,
            authenticated_at: None,
            origin: Instant::now(),
            authorized: Vec::new(),
            resyncs: 0,
            problems: Vec::new(),
        }
    }

    /// Now, on the monotonic clock from the glue's origin and on the wall clock.
    fn now(&self) -> (MonoNs, WallNs) {
        let mono = u64::try_from(self.origin.elapsed().as_nanos()).unwrap();
        (MonoNs(mono), wall_now())
    }

    /// Submits `auth` through `orders` at once, remembering the order it names.
    pub fn submit(&mut self, orders: &ExecOrders, auth: Authorization) -> RpcId {
        let item = self.item_of(auth.command());
        let rpc = orders.submit(auth).unwrap();
        self.sent.insert(rpc, item);
        self.authorized.push(item);
        rpc
    }

    /// Remembers the order `auth` names, as it is pushed onto the shard host's ladder.
    pub fn planned(&mut self, auth: &Authorization) {
        let item = self.item_of(auth.command());
        self.planned.push_back(item);
        self.authorized.push(item);
    }

    fn item_of(&self, cmd: &VenueCommand) -> Item {
        match cmd {
            VenueCommand::Place(o) => Item::Place(o.cid),
            VenueCommand::Cancel(c) => {
                let cid = c
                    .target
                    .client()
                    .or_else(|| c.target.venue().and_then(|v| self.reg.cid_of(v)));
                Item::Cancel(cid.expect("a cancel of an order the registry holds"))
            }
            other => panic!("not sent by this benchmark: {other:?}"),
        }
    }

    fn op(item: Item, rpc: RpcId) -> (ClientOrderId, OrderOp) {
        match item {
            Item::Place(cid) => (cid, OrderOp::Place),
            Item::Cancel(cid) => (cid, OrderOp::Cancel(rpc)),
        }
    }

    /// The item request `rpc` carried: `None` for the session's own arm.
    fn on_submitted(&mut self, handle: SubmitHandle) -> Option<Item> {
        let SubmitHandle { rpc, receipt } = handle;
        let item = match self.sent.get(&rpc) {
            Some(item) => *item,
            None => {
                let item = self.planned.pop_front()?;
                self.sent.insert(rpc, item);
                item
            }
        };
        let (mono, wall) = self.now();
        let (cid, op) = Glue::op(item, rpc);
        let done = match &receipt {
            Ok(_) => match item {
                Item::Place(_) => self.reg.placement_sent(cid, mono, wall),
                Item::Cancel(_) => self.reg.cancel_sent(cid, rpc, mono).map(|_| ()),
            },
            Err(reason) => {
                self.problems
                    .push(format!("request {} not sent: {reason:?}", rpc.0));
                let named = ItemRef {
                    idx: 0,
                    cid: Some(cid),
                    vid: None,
                };
                let outcome = SubmitOutcome::NotSent(*reason);
                self.reg
                    .on_outcome(cid, op, &named, &outcome, mono)
                    .map(|_| ())
            }
        };
        if let Err(e) = done {
            self.problems.push(format!("request {}: {e:?}", rpc.0));
        }
        Some(item)
    }

    fn on_outcome(
        &mut self,
        rpc: RpcId,
        item: Option<ItemRef>,
        outcome: SubmitOutcome,
        now: MonoNs,
    ) {
        // The session's own arm, which the registry did not build.
        let Some(&it) = self.sent.get(&rpc) else {
            if !matches!(outcome, SubmitOutcome::Accepted { .. }) {
                self.problems.push(format!("the arm: {outcome:?}"));
            }
            return;
        };
        let (cid, op) = Glue::op(it, rpc);
        let named = item.unwrap_or(ItemRef {
            idx: 0,
            cid: Some(cid),
            vid: None,
        });
        if let Err(e) = self.reg.on_outcome(cid, op, &named, &outcome, now) {
            self.problems.push(format!("request {}: {e:?}", rpc.0));
        }
    }

    fn on_event(&mut self, env: Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        let key = OrderKey {
            venue: env.venue_seq,
            ingest: env.stamp.ingest_seq,
        };
        match env.body {
            ExecEvent::Outcome { rpc, item, outcome } => self.on_outcome(rpc, item, outcome, now),
            // Every order the stub holds is ours.
            ExecEvent::Order(u) => match self.reg.apply_update(&u, key) {
                Routed::Ours(..) => {}
                other => self
                    .problems
                    .push(format!("an order update routed {other:?}: {u:?}")),
            },
            ExecEvent::ResyncBegin { watermark } => {
                self.collecting = Some(ResyncSnapshot {
                    watermark,
                    requested_at: self.authenticated_at.unwrap_or(now),
                    orders: Vec::new(),
                    positions: Vec::new(),
                });
            }
            ExecEvent::ResyncOrder(o) => match self.collecting.as_mut() {
                Some(snap) => snap.orders.push(o),
                None => self
                    .problems
                    .push("ResyncOrder outside a resync".to_owned()),
            },
            ExecEvent::ResyncPosition { inst, qty, .. } => match self.collecting.as_mut() {
                Some(snap) => snap.positions.push((inst, qty)),
                None => self
                    .problems
                    .push("ResyncPosition outside a resync".to_owned()),
            },
            ExecEvent::ResyncEnd => {
                let Some(snapshot) = self.collecting.take() else {
                    return self
                        .problems
                        .push("ResyncEnd with no ResyncBegin".to_owned());
                };
                match self.reg.resync(&self.ladder, &self.caps, &snapshot, key) {
                    Ok(_) => self.resyncs += 1,
                    Err(e) => self.problems.push(format!("resync refused: {e:?}")),
                }
            }
            ExecEvent::Conn { state, .. } => {
                if state == fbc_core::ConnState::Authenticated {
                    self.authenticated_at = Some(now);
                }
            }
            // What the account holds: the registry reads none of it.
            ExecEvent::Position { .. } | ExecEvent::Balance { .. } => {}
            other => self
                .problems
                .push(format!("an event this run does not send: {other:?}")),
        }
    }
}

/// The wall clock now.
pub fn wall_now() -> WallNs {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    WallNs(i64::try_from(ns).unwrap())
}

/// The session's handler, the glue's part: hands each event to the [`Glue`], and says which
/// item each submission reported carried.
pub struct GlueHandler(pub Rc<RefCell<Glue>>);

impl GlueHandler {
    pub fn submitted(&mut self, handle: SubmitHandle) -> Option<Item> {
        self.0.borrow_mut().on_submitted(handle)
    }
}

impl ExecHandler for GlueHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().on_event(env);
    }

    fn on_epoch_end(&mut self, _key: ConnKey) {}

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.submitted(handle);
    }
}
