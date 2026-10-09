//! The glue between the order-entry session and fbc-oms, as a consumer writes it: an
//! [`ExecHandler`] that routes every event into the account's [`Registry`] (order updates under
//! the venue's sequence, fills through the [`FillLedger`], the `Resync*` events collected into
//! one [`ResyncSnapshot`] applied at `ResyncEnd`), and the one path commands take to the
//! session, so the registry knows which request each answer names. What the session reported is
//! kept in order in [`Glue::notes`], which the tests read, with each change of whether the
//! session takes places ([`ExecOrders::may_place`]) as it is handed the event that made it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fbc_core::{
    ClientOrderId, ConnKey, ConnState, Envelope, ExecEvent, ItemRef, MonoNs, NotSentReason,
    OrderCaps, RpcId, SubmitHandle, SubmitOutcome, VenueCommand, WallNs,
};
use fbc_oms::{
    Admission, Authorization, FillLedger, FillRouted, FillTime, LadderConfig, OrderKey, OrderOp,
    Registry, ResyncReport, ResyncSnapshot, Routed,
};
use fbc_runtime::{ExecHandler, ExecOrders};
use tokio::sync::Notify;

/// Something the session reported, as the glue saw it. Every field shows in a failing test's
/// notes, read or not.
#[allow(dead_code)]
#[derive(Debug)]
pub enum Note {
    /// The stream's state changed.
    Conn(ConnState),
    /// The outcome of a request: `ours` when submitted through [`Glue::submit`], otherwise the
    /// session's own (its cancel-on-disconnect arm).
    Outcome {
        rpc: RpcId,
        outcome: SubmitOutcome,
        ours: bool,
    },
    /// A submission was written, or not and why.
    Submitted {
        rpc: RpcId,
        sent: Result<(), NotSentReason>,
    },
    /// A fill: applied to the order and the inventory, or why not.
    Fill { applied: bool, what: String },
    /// A resync was applied.
    Resynced {
        report: ResyncReport,
        snapshot: ResyncSnapshot,
    },
    /// Something the glue could not apply, or a refusal no outcome carries.
    Problem(String),
    /// A connection epoch ended.
    EpochEnd(ConnKey),
    /// Whether the session takes places and amends changed ([`ExecOrders::may_place`]), read
    /// after each event and each epoch's end the handler is handed: it begins to once the
    /// epoch's arm is accepted and its resync has ended, whichever comes last.
    Placing(bool),
}

/// One item of a submitted command.
#[derive(Copy, Clone, Debug)]
enum Item {
    Place(ClientOrderId),
    Cancel(ClientOrderId),
}

pub struct Glue {
    pub reg: Registry,
    ledger: FillLedger,
    caps: OrderCaps,
    ladder: LadderConfig,
    sent: HashMap<RpcId, Vec<Item>>,
    collecting: Option<ResyncSnapshot>,
    authenticated_at: Option<MonoNs>,
    origin: Instant,
    /// The session's orders, watched for [`Note::Placing`] once [`Glue::watch`] hands them over.
    orders: Option<ExecOrders>,
    placing: bool,
    pub notes: Vec<Note>,
    /// Woken each time the session has handed the handler something, so a test waits on what
    /// happened rather than on a clock.
    pub changed: Rc<Notify>,
}

impl Glue {
    pub fn new(reg: Registry, ledger: FillLedger, caps: OrderCaps, ladder: LadderConfig) -> Glue {
        Glue {
            reg,
            ledger,
            caps,
            ladder,
            sent: HashMap::new(),
            collecting: None,
            authenticated_at: None,
            origin: Instant::now(),
            orders: None,
            placing: false,
            notes: Vec::new(),
            changed: Rc::new(Notify::new()),
        }
    }

    /// Watches `orders` (the session's) for whether the session takes places.
    pub fn watch(&mut self, orders: ExecOrders) {
        self.placing = orders.may_place();
        self.orders = Some(orders);
    }

    /// Notes a change of whether the session takes places.
    fn note_placing(&mut self) {
        let Some(placing) = self.orders.as_ref().map(ExecOrders::may_place) else {
            return;
        };
        if placing != self.placing {
            self.placing = placing;
            self.notes.push(Note::Placing(placing));
        }
    }

    /// Now, on the monotonic clock from the glue's origin and on the wall clock.
    pub fn now(&self) -> (MonoNs, WallNs) {
        let mono = u64::try_from(self.origin.elapsed().as_nanos()).unwrap();
        (MonoNs(mono), wall_now())
    }

    /// Submits `auth` through `orders`, remembering the order each item names.
    pub fn submit(&mut self, orders: &ExecOrders, auth: Authorization) -> RpcId {
        let items = self.items_of(auth.command());
        let rpc = orders.submit(auth).unwrap();
        self.sent.insert(rpc, items);
        rpc
    }

    fn items_of(&self, cmd: &VenueCommand) -> Vec<Item> {
        let cancel_of = |c: &fbc_core::CancelOrder| {
            let cid = c
                .target
                .client()
                .or_else(|| c.target.venue().and_then(|v| self.reg.cid_of(v)));
            Item::Cancel(cid.expect("a cancel of an order the registry holds"))
        };
        match cmd {
            VenueCommand::Place(o) => vec![Item::Place(o.cid)],
            VenueCommand::PlaceBatch(orders) => orders.iter().map(|o| Item::Place(o.cid)).collect(),
            VenueCommand::Cancel(c) => vec![cancel_of(c)],
            VenueCommand::CancelMany(cs) => cs.iter().map(cancel_of).collect(),
            other => panic!("not modelled by this glue: {other:?}"),
        }
    }

    fn op(item: Item, rpc: RpcId) -> (ClientOrderId, OrderOp) {
        match item {
            Item::Place(cid) => (cid, OrderOp::Place),
            Item::Cancel(cid) => (cid, OrderOp::Cancel(rpc)),
        }
    }

    /// Whether request `rpc` is one submitted through [`Glue::submit`].
    pub fn is_ours(&self, rpc: RpcId) -> bool {
        self.sent.contains_key(&rpc)
    }

    fn problem(&mut self, what: String) {
        self.notes.push(Note::Problem(what));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let SubmitHandle { rpc, receipt } = handle;
        let items = self.sent.get(&rpc).cloned().unwrap_or_default();
        let (mono, wall) = self.now();
        for (idx, item) in items.into_iter().enumerate() {
            let (cid, op) = Glue::op(item, rpc);
            let done = match &receipt {
                Ok(_) => match item {
                    Item::Place(_) => self.reg.placement_sent(cid, mono, wall),
                    Item::Cancel(_) => self.reg.cancel_sent(cid, rpc, mono).map(|_| ()),
                },
                Err(reason) => {
                    let item = ItemRef {
                        idx: u16::try_from(idx).unwrap(),
                        cid: Some(cid),
                        vid: None,
                    };
                    let outcome = SubmitOutcome::NotSent(*reason);
                    self.reg
                        .on_outcome(cid, op, &item, &outcome, mono)
                        .map(|_| ())
                }
            };
            if let Err(e) = done {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
        }
        self.notes.push(Note::Submitted {
            rpc,
            sent: receipt.map(|_| ()),
        });
    }

    fn on_outcome(
        &mut self,
        rpc: RpcId,
        item: Option<ItemRef>,
        outcome: SubmitOutcome,
        now: MonoNs,
    ) {
        let Some(items) = self.sent.get(&rpc).cloned() else {
            self.notes.push(Note::Outcome {
                rpc,
                outcome,
                ours: false,
            });
            return;
        };
        let targets: Vec<(Item, ItemRef)> = match &item {
            Some(named) => items
                .get(usize::from(named.idx))
                .map(|it| vec![(*it, named.clone())])
                .unwrap_or_default(),
            None => items
                .iter()
                .enumerate()
                .map(|(idx, it)| {
                    let named = ItemRef {
                        idx: u16::try_from(idx).unwrap(),
                        cid: Some(Glue::op(*it, rpc).0),
                        vid: None,
                    };
                    (*it, named)
                })
                .collect(),
        };
        for (it, named) in targets {
            let (cid, op) = Glue::op(it, rpc);
            if let Err(e) = self.reg.on_outcome(cid, op, &named, &outcome, now) {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
        }
        self.notes.push(Note::Outcome {
            rpc,
            outcome,
            ours: true,
        });
    }

    fn on_event(&mut self, env: Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        let key = OrderKey {
            venue: env.venue_seq,
            ingest: env.stamp.ingest_seq,
        };
        match env.body {
            ExecEvent::Outcome { rpc, item, outcome } => self.on_outcome(rpc, item, outcome, now),
            // An update the registry routes anywhere but to an order of ours is a problem here:
            // the rehearsal's venue holds no other engine's or system's order (Reviewer B
            // RB-8mv-7 on PR #119).
            ExecEvent::Order(u) => match self.reg.apply_update(&u, key) {
                Routed::Ours(..) => {}
                other => self.problem(format!("an order update routed {other:?}: {u:?}")),
            },
            ExecEvent::Fill(f) => {
                let time = env.exch_ts.map(|exch| FillTime {
                    exch,
                    kind: env.exch_ts_kind,
                    aligned: WallNs(exch.0),
                });
                let note = match self.ledger.admit(&f, time, now) {
                    Admission::Apply(accepted) => match self.reg.apply_fill(accepted) {
                        Ok(routed @ FillRouted::Ours(..)) => Note::Fill {
                            applied: true,
                            what: format!("{routed:?}"),
                        },
                        Ok(other) => Note::Fill {
                            applied: false,
                            what: format!("{other:?}"),
                        },
                        Err(e) => Note::Fill {
                            applied: false,
                            what: format!("refused by the registry: {e:?}"),
                        },
                    },
                    other => Note::Fill {
                        applied: false,
                        what: format!("{other:?}"),
                    },
                };
                self.notes.push(note);
            }
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
                None => self.problem("ResyncOrder outside a resync".to_owned()),
            },
            ExecEvent::ResyncPosition { inst, qty, .. } => match self.collecting.as_mut() {
                Some(snap) => snap.positions.push((inst, qty)),
                None => self.problem("ResyncPosition outside a resync".to_owned()),
            },
            ExecEvent::ResyncEnd => {
                let Some(snapshot) = self.collecting.take() else {
                    return self.problem("ResyncEnd with no ResyncBegin".to_owned());
                };
                let key = OrderKey {
                    venue: env.venue_seq,
                    ingest: env.stamp.ingest_seq,
                };
                match self.reg.resync(&self.ladder, &self.caps, &snapshot, key) {
                    Ok(report) => self.notes.push(Note::Resynced { report, snapshot }),
                    Err(e) => self.problem(format!("resync refused: {e:?}")),
                }
            }
            ExecEvent::Conn { state, .. } => {
                if state == ConnState::Authenticated {
                    self.authenticated_at = Some(now);
                }
                self.notes.push(Note::Conn(state));
            }
            ExecEvent::AsyncReject { op, reject, .. } => {
                self.problem(format!(
                    "asynchronous reject of a {op:?}: {:?}",
                    reject.kind
                ));
            }
            // Shown as a consumer shows it: its kind, the venue's code and the codec's text
            // (a refused login's status, code and message, FBC-3f8z).
            ExecEvent::UncorrelatedError(reject) => {
                self.problem(format!(
                    "venue error naming no request: {:?} {}: {}",
                    reject.kind,
                    reject.venue_code.as_deref().unwrap_or("no code"),
                    reject.raw
                ));
            }
            // What the account holds: the registry reads none of it.
            ExecEvent::Position { .. }
            | ExecEvent::Balance { .. }
            | ExecEvent::FundingPaid { .. }
            | ExecEvent::FeeRates { .. } => {}
            other => self.problem(format!("an event this glue does not model: {other:?}")),
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

/// The session's handler: hands each event to the [`Glue`].
pub struct GlueHandler(pub Rc<RefCell<Glue>>);

impl ExecHandler for GlueHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut glue = self.0.borrow_mut();
        glue.on_event(env);
        glue.note_placing();
        glue.changed.notify_waiters();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        let mut glue = self.0.borrow_mut();
        glue.notes.push(Note::EpochEnd(key));
        glue.note_placing();
        glue.changed.notify_waiters();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let mut glue = self.0.borrow_mut();
        glue.on_submitted(handle);
        glue.changed.notify_waiters();
    }
}
