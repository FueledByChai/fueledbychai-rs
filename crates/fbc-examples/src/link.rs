//! The glue between one account's order-entry session and fbc-oms (FBC-x69b): an
//! [`ExecHandler`] that routes every event the session hands it into the account's
//! [`Registry`], and the one path commands take to the session, so the registry knows which
//! request each answer names. It depends on fbc-core, fbc-oms and fbc-runtime only. Shared by
//! the samples that trade (`testnet_trade`, and FBC-elg7's `testnet_quote`), which include it
//! by path with their tests; it is not a module of this crate's library target.
//!
//! - **Submitting.** [`Link::submit`] hands an fbc-oms [`Authorization`] to the session's
//!   [`ExecOrders`] and remembers, under the request id it was given, the order each item of
//!   the command names: a placement by its client id, a cancel by its client id (or the one the
//!   registry holds for its venue id), an amend by the order it names with its new price and
//!   total (FBC-xji8's acceptance program amends once; testnet_trade and testnet_quote never
//!   do, and each treats an amend of an order of ours as trouble).
//! - **What became of a submission** ([`ExecHandler::on_submitted`]): sent, the placement's send
//!   time ([`Registry::placement_sent`]), the cancel in flight ([`Registry::cancel_sent`]) or the
//!   amend in flight ([`Registry::amend_sent`]) is recorded; not sent, every item's order is told
//!   `NotSent` for its reason (an amend's after it is recorded in flight, as fbc-oms asks of an
//!   amend a gateway took).
//! - **Events.** A request's outcome goes to the order each of its items names
//!   ([`Registry::on_outcome`]); an outcome naming a request not submitted here is the session's
//!   own (its cancel-on-disconnect arm) and is only noted. An order event is applied under the
//!   venue's sequence with the ingest order as tiebreak ([`Registry::apply_update`]); a fill goes
//!   through the [`FillLedger`] and, accepted, to [`Registry::apply_fill`]. The `Resync*` events
//!   are collected into one [`ResyncSnapshot`] and applied at `ResyncEnd`
//!   ([`Registry::resync`]); its `requested_at` is the receive time of the event reporting the
//!   socket authenticated, which comes before the session asks for the resync. A snapshot the
//!   registry refuses is noted as refused ([`Note::ResyncRefused`]): the session opened its
//!   gate at `ResyncEnd` all the same, so the driver must not place on it.
//!   An order event of an order in our namespace that the registry does not hold is noted as
//!   such ([`Note::Orphan`]): Stop would not cancel it nor the run count its fills.
//!   The account's position events are noted as they come, for the driver to compare with
//!   the position it seeded.
//! - **Asynchronous rejects** (a two-phase venue's risk check refusing what it accepted) are
//!   noted as they come ([`Note::AsyncReject`]) for the driver to judge.
//! - **Refusals and connection ends.** A venue error naming no request
//!   ([`ExecEvent::UncorrelatedError`]) that comes before the connection authenticated is a
//!   refused login ([`Note::LoginRefused`]: the REST login refused or unanswered, or the auth
//!   frame refused); after it, a problem. Either is kept as the reason its connection ended,
//!   which [`Note::EpochEnd`] carries; with none, the end names what the session saw: its codec
//!   closed the connection, or the socket ended and fbc-runtime does not say why (FBC-dmlw).
//!   A venue error is shown through [`shown`]: Paradex's codec withholds a login refusal's
//!   echoed words itself, but an auth frame's refusal is the venue's message as received, and
//!   that frame carries the session token.
//! - **Unprotected orders** ([`ExecHandler::on_unprotected`], decision 0080): the orders of
//!   ours a connection's resync showed resting from an earlier connection or run, which its
//!   cancel-on-disconnect may not cover, are cancelled at once: a Safety cancel per order, built
//!   by the registry's [`Registry::cancel_many`] through each order's cancel permit, authorized
//!   and submitted through the session's [`ExecOrders`] ([`Link::attach`]), and the request
//!   remembered for the order ([`Link::safety_cancel_of`]). One the registry holds ended or
//!   not at all is not cancelled: decision 0080 queries it, and no order query is built here
//!   yet (FBC-m8vm), so its market stays held, which is safe. [`Note::Unprotected`] says what
//!   was done.
//! - **Undecodable input** ([`ExecHandler::on_decode_error`]): noted as a problem as it is
//!   read, so a driver stops trading on what it may have missed.
//! - **Cancels not ours** (FBC-aoh5, the owner's 2026-10-18 mainnet session: a Java instance on
//!   the same account cancelled every order on its ticker it did not place). An order of ours
//!   that an order event ends `Canceled(Requested)` (a user cancel: Paradex's USER_CANCELED,
//!   user_cancel_ws or user_cancel_batch_ws, which do not say who sent it) while no cancel of
//!   ours covers it is noted ([`Note::CancelNotOurs`]) after its [`Note::Order`]. A cancel of
//!   ours covers an order from the moment it is submitted here, in flight or answered, whatever
//!   its answer: a cancel or cancel-many naming it (a replace's, a sweep's, Stop's, or a Safety
//!   cancel of [`ExecHandler::on_unprotected`]), or a cancel-all of its market (or of the
//!   account) submitted while the order was ours and not ended; the registry's own count of
//!   cancels sent for it is read as well. Only the event that ends the order counts (a late
//!   duplicate of it is not), so one order is noted at most once. Each driver decides what it
//!   means.
//! - **Notes.** Everything the driver may wait on is appended to [`Link::notes`], in order.
//!
//! Times on the shard's monotonic clock are taken from each event's stamp; a submission's send
//! time, which carries no stamp, from [`Link::now`], a clock started with the session's
//! [`IngestClock`](fbc_runtime::IngestClock). A fill's venue time is taken as the local wall
//! time too (no clock-skew estimate yet), which only a replayed fill's admission reads.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fbc_core::{
    AccountKey, CancelReason, CancelScope, CidMatch, ClientOrderId, ConnKey, ConnState, Envelope,
    ExecEvent, InstrumentId, ItemRef, Lots, MonoNs, NotSentReason, OpKind, OrderCaps, OrderRef,
    Reject, RpcId, Side, SignedLots, SubmitHandle, SubmitOutcome, Ticks, VenueCommand,
    VenueOrderId, VenueOrderSnapshot, WallNs,
};
use fbc_oms::{
    Admission, Applied, Authorization, FillLedger, FillRouted, FillTime, LadderConfig, OrdState,
    OrderKey, OrderOp, Registry, ResyncReport, ResyncSnapshot, Routed, TerminalKind,
};
use fbc_runtime::{ExecHandler, ExecOrders};
use tokio::sync::Notify;

/// Something the session reported, as the glue saw it.
#[derive(Debug)]
pub enum Note {
    /// A stream's state changed.
    Conn(ConnState),
    /// The outcome of a request: `ours` when submitted through [`Link::submit`], otherwise the
    /// session's own request (its cancel-on-disconnect arm).
    Outcome {
        rpc: RpcId,
        /// The item it answers, `None` for the whole request.
        item: Option<u16>,
        outcome: SubmitOutcome,
        ours: bool,
    },
    /// A submission was sent, or not and why.
    Submitted {
        rpc: RpcId,
        sent: Result<(), NotSentReason>,
    },
    /// An order event, where it went: anything but an order in our namespace that the
    /// registry does not hold ([`Note::Orphan`]).
    Order(Routed),
    /// An order event of an order in our namespace that the registry does not hold (an
    /// earlier run's the resync did not show): its client id and venue id.
    Orphan {
        cid: ClientOrderId,
        vid: Option<VenueOrderId>,
    },
    /// A fill, where it went (or why the ledger did not apply it): `unexplained` unless it was
    /// counted on an order of ours, held by the resync's position, or one the ledger already
    /// held (a duplicate, or a replay the starting position holds); `ours` when it was applied,
    /// counted on an order of ours and the inventory.
    Fill {
        seen: String,
        unexplained: bool,
        ours: bool,
    },
    /// The account's position in an instrument, as the venue's position stream reported it.
    Position { inst: InstrumentId, qty: SignedLots },
    /// A resync was applied: its report, and the positions and open orders it showed.
    Resynced {
        report: ResyncReport,
        snapshot: ResyncSnapshot,
    },
    /// A resync the registry refused, and why: nothing of it was applied.
    ResyncRefused(String),
    /// A refusal that came after the venue accepted the request (a two-phase venue's risk
    /// check): of what, which order, and why. Each driver decides what it means (RB-elg7-1 on
    /// PR #149): one of its own post-only places closed POST_ONLY_WOULD_CROSS is an ordinary
    /// end for a quoting loop, a reason to Stop for a one-order round trip.
    AsyncReject {
        target: OrderRef,
        op: OpKind,
        reject: Reject,
    },
    /// A refusal from the venue that no outcome carries, or something the glue could not apply.
    Problem(String),
    /// The login was refused before the connection authenticated (module documentation): the
    /// venue's error as [`described`] shows it.
    LoginRefused(String),
    /// A connection epoch ended, and why, as far as the session reported it.
    EpochEnd { key: ConnKey, why: String },
    /// The session told the orders of ours its connection's cancel-on-disconnect may not cover
    /// (decision 0080): each with its market, the Safety cancel requests submitted for them,
    /// and why any was not cancelled.
    Unprotected {
        told: Vec<(ClientOrderId, InstrumentId)>,
        rpcs: Vec<RpcId>,
        left: Vec<String>,
    },
    /// Our order `cid` (on `side`) was just ended by a user cancel (`Canceled(Requested)`) that
    /// no cancel of ours covers (module documentation, FBC-aoh5): another program on the
    /// account, or a person in the venue's UI. Noted right after its [`Note::Order`].
    CancelNotOurs { cid: ClientOrderId, side: Side },
}

/// Why a connection ended when the session reported no venue error with it: its codec
/// closed it ([`CODEC_CLOSED`]), or nothing the handler is told says ([`ENDED_UNSAID`]: the
/// codec asks for a reconnect with no event when a login cannot be signed; FBC-dmlw).
pub const CODEC_CLOSED: &str = "the session closed it to log in again, with no venue error";
pub const ENDED_UNSAID: &str = concat!(
    "the socket closed or failed, a write stalled, or the login could not be signed ",
    "(fbc-runtime does not say which yet, FBC-dmlw)"
);

/// The longest word of a venue's text [`shown`] shows: a longer one could hold an echoed
/// credential (a session token, a key, a signature, an account), so it is withheld, as
/// Paradex's codec withholds a login refusal's words.
const LONGEST_SHOWN_WORD: usize = 15;
/// The longest number [`shown`] shows (an HTTP status, a JSON-RPC code).
const LONGEST_NUMBER: usize = 6;
/// The most characters of a venue's text [`shown`] shows.
const MOST_SHOWN: usize = 200;
/// The longest error code [`described`] shows.
const LONGEST_CODE: usize = 64;

/// Whether `word` could hold a secret: longer than [`LONGEST_SHOWN_WORD`], holding anything but
/// printable ASCII, or holding a digit unless it is a short number (an optional '-' and at most
/// [`LONGEST_NUMBER`] digits), or a run of six or more hex digits. A word of capitals joined by
/// '_' (an error code's shape, `STARKNET_SIGNATURE_VERIFICATION_FAILED`) is judged part by
/// part: no key, signature or token is written in capitals alone.
fn withheld(word: &str) -> bool {
    let code = word.contains('_')
        && word.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
        && !word.split('_').any(str::is_empty);
    if code {
        return word.split('_').any(withheld);
    }
    let digits = word.strip_prefix('-').unwrap_or(word);
    let number = !digits.is_empty()
        && digits.len() <= LONGEST_NUMBER
        && digits.bytes().all(|b| b.is_ascii_digit());
    word.len() > LONGEST_SHOWN_WORD
        || word.bytes().any(|b| !b.is_ascii_graphic())
        || (word.bytes().any(|b| b.is_ascii_digit()) && !number)
        || (word.len() >= 6 && word.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// `text` as the run prints a venue's words: each word that could hold a secret ([`withheld`])
/// replaced by `<withheld>`, control characters and whitespace as spaces, cut at [`MOST_SHOWN`]
/// characters. Words are split at whitespace and at punctuation that cannot sit inside a
/// token; a token's '.', '-' and '_' keep it one (long) word.
pub fn shown(text: &str) -> String {
    let (mut out, mut word) = (String::new(), String::new());
    let flush = |word: &mut String, out: &mut String| {
        if !word.is_empty() {
            out.push_str(if withheld(word) { "<withheld>" } else { word });
        }
        word.clear();
    };
    for c in text.chars() {
        if c.is_whitespace() || c.is_control() {
            flush(&mut word, &mut out);
            out.push(' ');
        } else if matches!(
            c,
            ',' | ';'
                | ':'
                | '"'
                | '\''
                | '`'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '<'
                | '>'
                | '='
                | '|'
        ) {
            flush(&mut word, &mut out);
            out.push(c);
        } else {
            word.push(c);
        }
    }
    flush(&mut word, &mut out);
    match out.char_indices().nth(MOST_SHOWN) {
        Some((at, _)) => format!("{}...", &out[..at]),
        None => out,
    }
}

/// An asynchronous reject ([`Note::AsyncReject`]) as the samples print it: the operation, the
/// kind, and the venue's reason as [`described`] shows it.
pub fn async_reject(op: OpKind, reject: &Reject) -> String {
    format!(
        "asynchronous reject of a {op:?}: {:?} ({})",
        reject.kind,
        described(reject)
    )
}

/// A venue error as the run prints it: its code (when plain: letters, digits, '_' and '-', no
/// part withheld; `<withheld>` otherwise) unless its text already names it, then its text,
/// both through [`shown`].
pub fn described(reject: &Reject) -> String {
    let raw = shown(&reject.raw);
    let Some(code) = reject.venue_code.as_deref() else {
        return raw;
    };
    let plain = code.len() <= LONGEST_CODE
        && !code.is_empty()
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && !code.split('_').any(withheld);
    if !plain {
        return format!("<withheld>: {raw}");
    }
    if raw.contains(code) {
        raw
    } else {
        format!("{code}: {raw}")
    }
}

/// One item of a submitted command, as the registry knows it.
#[derive(Copy, Clone, Debug)]
enum Item {
    Place(ClientOrderId),
    Cancel(ClientOrderId),
    /// An amend of the order to a new price and total quantity.
    Amend {
        cid: ClientOrderId,
        px: Ticks,
        qty: Lots,
    },
}

/// The registry, the fill ledger and what was submitted (module documentation).
pub struct Link {
    reg: Registry,
    ledger: FillLedger,
    caps: OrderCaps,
    ladder: LadderConfig,
    /// The items of each request submitted here.
    sent: HashMap<RpcId, Vec<Item>>,
    /// The resync being collected, from `ResyncBegin` to `ResyncEnd`.
    collecting: Option<ResyncSnapshot>,
    /// When the socket was last reported authenticated.
    authenticated_at: Option<MonoNs>,
    /// The current connection: whether it authenticated, whether the codec closed it, and the
    /// last venue error naming no request it reported (its end's reason).
    epoch_authenticated: bool,
    epoch_closed: bool,
    epoch_error: Option<String>,
    origin: Instant,
    notes: Vec<Note>,
    /// The places and cancels sent that notes [`Link::forget`] took counted.
    forgotten: (usize, usize),
    /// The session's orders and the account, where Safety cancels go ([`Link::attach`]).
    session: Option<(ExecOrders, AccountKey)>,
    /// The Safety cancel request last submitted for each unprotected order.
    safety: HashMap<ClientOrderId, RpcId>,
    /// The orders of ours that have not ended that the link knows (placed here, or shown by a
    /// resync), with their market: what a cancel-all of ours covers.
    live: HashMap<ClientOrderId, InstrumentId>,
    /// The orders of ours a cancel of ours submitted here covers (module documentation): an
    /// order that ends `Canceled(Requested)` outside it was cancelled by someone else.
    covered: HashSet<ClientOrderId>,
}

impl Link {
    /// The glue over `reg`, its fills deduplicated by `ledger`, resyncs applied under the venue's
    /// order caps `caps` and `ladder`. Build it just before the session's `IngestClock`, so
    /// [`Link::now`] reads the same monotonic origin to within the build.
    pub fn new(reg: Registry, ledger: FillLedger, caps: OrderCaps, ladder: LadderConfig) -> Link {
        Link {
            reg,
            ledger,
            caps,
            ladder,
            sent: HashMap::new(),
            collecting: None,
            authenticated_at: None,
            epoch_authenticated: false,
            epoch_closed: false,
            epoch_error: None,
            origin: Instant::now(),
            notes: Vec::new(),
            forgotten: (0, 0),
            session: None,
            safety: HashMap::new(),
            live: HashMap::new(),
            covered: HashSet::new(),
        }
    }

    /// Where the Safety cancels of unprotected orders go: the session's `orders`, under the
    /// account `acct`. Attached before the session runs, so before it tells any.
    pub fn attach(&mut self, orders: ExecOrders, acct: AccountKey) {
        self.session = Some((orders, acct));
    }

    /// The Safety cancel request submitted for our order `cid` when the session told it
    /// unprotected, if one was.
    pub fn safety_cancel_of(&self, cid: ClientOrderId) -> Option<RpcId> {
        self.safety.get(&cid).copied()
    }

    /// How many orders of ours on `inst` the session told unprotected so far.
    pub fn told_on(&self, inst: InstrumentId) -> usize {
        let mut cids: Vec<ClientOrderId> = self
            .notes
            .iter()
            .filter_map(|n| match n {
                Note::Unprotected { told, .. } => Some(told),
                _ => None,
            })
            .flatten()
            .filter(|(_, i)| *i == inst)
            .map(|(cid, _)| *cid)
            .collect();
        cids.sort_unstable();
        cids.dedup();
        cids.len()
    }

    /// The registry, for building commands and arming markets.
    pub fn reg(&mut self) -> &mut Registry {
        &mut self.reg
    }

    /// The registry, to read.
    pub fn registry(&self) -> &Registry {
        &self.reg
    }

    /// The venue's order caps.
    pub fn caps(&self) -> &OrderCaps {
        &self.caps
    }

    /// Everything noted so far, in order.
    pub fn notes(&self) -> &[Note] {
        &self.notes
    }

    /// How many places (requests whose items are placements) and cancels (cancels,
    /// cancel-manys, cancel-alls) submitted here the session reported sent, those in notes
    /// [`Link::forget`] took included; an amend is neither.
    pub fn sent_counts(&self) -> (usize, usize) {
        let (places, cancels) = self.counted(&self.notes);
        (self.forgotten.0 + places, self.forgotten.1 + cancels)
    }

    /// Forgets request `rpc`'s items, for a driver that has every answer to it it waits for and
    /// whose notes [`Link::forget`] took (testnet_quote): the map of submitted requests then
    /// holds those in flight, not the run's history. An answer to it that comes later is noted
    /// as the session's own ([`Note::Outcome`] with `ours` false).
    pub fn release(&mut self, rpc: RpcId) {
        self.sent.remove(&rpc);
        self.safety.retain(|_, r| *r != rpc);
    }

    /// How many submitted requests the link still holds the items of.
    pub fn requests_held(&self) -> usize {
        self.sent.len()
    }

    /// What [`Link::sent_counts`] counts in `notes`.
    fn counted(&self, notes: &[Note]) -> (usize, usize) {
        let mut counts = (0, 0);
        for note in notes {
            let Note::Submitted { rpc, sent: Ok(()) } = note else {
                continue;
            };
            match self.sent.get(rpc).and_then(|items| items.first()) {
                Some(Item::Place(_)) => counts.0 += 1,
                Some(Item::Cancel(_)) | None if self.sent.contains_key(rpc) => counts.1 += 1,
                Some(Item::Amend { .. }) => {}
                _ => {}
            }
        }
        counts
    }

    /// Takes the first `upto` notes out and hands them over, for a driver that reads each note
    /// once and runs for long (testnet_quote): the list then never grows with the run. What
    /// [`Link::sent_counts`] counts of them is kept; [`Link::outcome_of`],
    /// [`Link::login_refusals`] and [`Link::told_on`] see only the notes left.
    pub fn forget(&mut self, upto: usize) -> Vec<Note> {
        let upto = upto.min(self.notes.len());
        let taken: Vec<Note> = self.notes.drain(..upto).collect();
        self.prune_ended();
        let (places, cancels) = self.counted(&taken);
        self.forgotten.0 += places;
        self.forgotten.1 += cancels;
        taken
    }

    /// What became of request `rpc`, submitted here, once every item is answered: not sent;
    /// the whole request's outcome; or, item by item, `Accepted` (at the first item's level)
    /// when every item was accepted and otherwise the first item's outcome that was not. `None`
    /// while an item is unanswered, so a batch is never judged by its first item alone.
    pub fn outcome_of(&self, rpc: RpcId) -> Option<SubmitOutcome> {
        let items = self.sent.get(&rpc)?.len().max(1);
        let mut by_item: HashMap<u16, &SubmitOutcome> = HashMap::new();
        for note in &self.notes {
            match note {
                Note::Submitted {
                    rpc: r,
                    sent: Err(reason),
                } if *r == rpc => return Some(SubmitOutcome::NotSent(*reason)),
                Note::Outcome {
                    rpc: r,
                    item,
                    outcome,
                    ours: true,
                } if *r == rpc => match item {
                    None => return Some(outcome.clone()),
                    Some(idx) => {
                        by_item.entry(*idx).or_insert(outcome);
                    }
                },
                _ => {}
            }
        }
        if by_item.len() < items {
            return None;
        }
        let mut answered: Vec<(&u16, &&SubmitOutcome)> = by_item.iter().collect();
        answered.sort_by_key(|(idx, _)| **idx);
        let refused = answered
            .iter()
            .find(|(_, o)| !matches!(o, SubmitOutcome::Accepted { .. }));
        Some(match refused {
            Some((_, o)) => (**o).clone(),
            None => (*answered[0].1).clone(),
        })
    }

    /// The orders request `rpc`, submitted here, names.
    pub fn cids_of(&self, rpc: RpcId) -> Vec<ClientOrderId> {
        let items = self.sent.get(&rpc).map_or(&[][..], Vec::as_slice);
        items.iter().map(|it| Link::op(*it, rpc).0).collect()
    }

    /// How many logins were refused in a row since a connection last authenticated, and the
    /// last refusal: `None` when none was.
    pub fn login_refusals(&self) -> Option<(usize, &str)> {
        let since = self
            .notes
            .iter()
            .rposition(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
            .map_or(0, |at| at + 1);
        let refused: Vec<&str> = self.notes[since..]
            .iter()
            .filter_map(|n| match n {
                Note::LoginRefused(why) => Some(why.as_str()),
                _ => None,
            })
            .collect();
        refused.last().map(|last| (refused.len(), *last))
    }

    /// Now, on the monotonic clock from the glue's origin and on the wall clock.
    pub fn now(&self) -> (MonoNs, WallNs) {
        let mono = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        (MonoNs(mono), WallNs(wall))
    }

    /// Submits `auth` through `orders`, remembering the order each item names: its request id,
    /// or why it was not submitted (nothing was then remembered). A cancel covers the orders it
    /// names, and a cancel-all every live order of ours on its market, from here on (module
    /// documentation): the venue's end of one may come before the session reports it sent.
    pub fn submit(&mut self, orders: &ExecOrders, auth: Authorization) -> Result<RpcId, String> {
        let items = self.items_of(auth.command())?;
        let scope = match auth.command() {
            VenueCommand::CancelAll(scope) => Some(*scope),
            _ => None,
        };
        let placed: Vec<(ClientOrderId, InstrumentId)> = match auth.command() {
            VenueCommand::Place(o) => vec![(o.cid, o.inst)],
            VenueCommand::PlaceBatch(os) => os.iter().map(|o| (o.cid, o.inst)).collect(),
            _ => Vec::new(),
        };
        let rpc = orders.submit(auth).map_err(|e| format!("{e:?}"))?;
        for item in &items {
            if let Item::Cancel(cid) = item {
                self.covered.insert(*cid);
            }
        }
        if let Some(scope) = scope {
            self.prune_ended();
            let all: Vec<ClientOrderId> = self
                .live
                .iter()
                .filter(|(_, inst)| match scope {
                    CancelScope::Instrument(market) => **inst == market,
                    CancelScope::Account => true,
                })
                .map(|(cid, _)| *cid)
                .collect();
            self.covered.extend(all);
        }
        self.live.extend(placed);
        self.sent.insert(rpc, items);
        Ok(rpc)
    }

    /// Forgets the orders of ours the registry holds ended (or not at all) from what a cancel
    /// of ours covers and from the live orders: only the event that ends an order is judged, so
    /// neither keeps the run's history.
    fn prune_ended(&mut self) {
        let reg = &self.reg;
        let live = |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| !r.state().is_terminal());
        self.live.retain(|cid, _| live(cid));
        self.covered.retain(live);
    }

    /// Whether a cancel of ours covers our order `cid` (module documentation).
    fn cancel_of_ours(&self, cid: ClientOrderId) -> bool {
        self.covered.contains(&cid) || self.reg.get(cid).is_some_and(|r| r.cancels_sent() > 0)
    }

    /// The items of `cmd`, each naming one of our orders.
    fn items_of(&self, cmd: &VenueCommand) -> Result<Vec<Item>, String> {
        let cancel_of = |c: &fbc_core::CancelOrder| {
            c.target
                .client()
                .or_else(|| c.target.venue().and_then(|v| self.reg.cid_of(v)))
                .map(Item::Cancel)
                .ok_or_else(|| "a cancel of no order the registry holds".to_owned())
        };
        match cmd {
            VenueCommand::Place(o) => Ok(vec![Item::Place(o.cid)]),
            VenueCommand::PlaceBatch(orders) => {
                Ok(orders.iter().map(|o| Item::Place(o.cid)).collect())
            }
            VenueCommand::Cancel(c) => Ok(vec![cancel_of(c)?]),
            VenueCommand::CancelMany(cs) => cs.iter().map(cancel_of).collect(),
            VenueCommand::CancelAll(_) => Ok(Vec::new()),
            VenueCommand::Amend(a) => {
                let cid = a
                    .target
                    .client()
                    .or_else(|| a.target.venue().and_then(|v| self.reg.cid_of(v)))
                    .ok_or_else(|| "an amend of no order the registry holds".to_owned())?;
                Ok(vec![Item::Amend {
                    cid,
                    px: a.px,
                    qty: a.qty,
                }])
            }
            other => Err(format!("not modelled by this glue: {}", kind(other))),
        }
    }

    /// The order op an item of request `rpc` is.
    fn op(item: Item, rpc: RpcId) -> (ClientOrderId, OrderOp) {
        match item {
            Item::Place(cid) => (cid, OrderOp::Place),
            Item::Cancel(cid) => (cid, OrderOp::Cancel(rpc)),
            Item::Amend { cid, .. } => (cid, OrderOp::Amend(rpc)),
        }
    }

    fn problem(&mut self, what: String) {
        self.notes.push(Note::Problem(what));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let SubmitHandle { rpc, receipt } = handle;
        let items = self.sent.get(&rpc).cloned().unwrap_or_default();
        let (mono, wall) = self.now();
        for (idx, item) in items.into_iter().enumerate() {
            let (cid, op) = Link::op(item, rpc);
            // An amend the session took is in flight until its outcome resolves it, a not-sent
            // one included (fbc-oms's `amend_not_submitted` is for one never handed over).
            if let Item::Amend { px, qty, .. } = item
                && let Err(e) = self.reg.amend_sent(cid, px, qty, rpc, mono)
            {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
            let done = match &receipt {
                Ok(_) => match item {
                    Item::Place(_) => self.reg.placement_sent(cid, mono, wall),
                    Item::Cancel(_) => self.reg.cancel_sent(cid, rpc, mono).map(|_| ()),
                    Item::Amend { .. } => Ok(()),
                },
                Err(reason) => {
                    let item = ItemRef {
                        idx: u16::try_from(idx).unwrap_or(u16::MAX),
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
        let sent = receipt.map(|_| ());
        self.notes.push(Note::Submitted { rpc, sent });
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
                item: item.map(|i| i.idx),
                outcome,
                ours: false,
            });
            return;
        };
        // One item's outcome, or the whole request's (`item: None`).
        let targets: Vec<(Item, ItemRef)> = match &item {
            Some(named) => match items.get(usize::from(named.idx)) {
                Some(it) => vec![(*it, named.clone())],
                None => Vec::new(),
            },
            None => items
                .iter()
                .enumerate()
                .map(|(idx, it)| {
                    let cid = Link::op(*it, rpc).0;
                    let named = ItemRef {
                        idx: u16::try_from(idx).unwrap_or(u16::MAX),
                        cid: Some(cid),
                        vid: None,
                    };
                    (*it, named)
                })
                .collect(),
        };
        for (it, named) in targets {
            let (cid, op) = Link::op(it, rpc);
            if let Err(e) = self.reg.on_outcome(cid, op, &named, &outcome, now) {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
        }
        self.notes.push(Note::Outcome {
            rpc,
            item: item.map(|i| i.idx),
            outcome,
            ours: true,
        });
    }

    fn on_resync_end(&mut self, ingest: u64, venue: Option<u64>) {
        let Some(snapshot) = self.collecting.take() else {
            self.problem("ResyncEnd with no ResyncBegin".to_owned());
            return;
        };
        let key = OrderKey { venue, ingest };
        for o in &snapshot.orders {
            if let Some(CidMatch::Ours(cid)) = o.cid {
                self.live.insert(cid, o.inst);
            }
        }
        match self.reg.resync(&self.ladder, &self.caps, &snapshot, key) {
            Ok(report) => self.notes.push(Note::Resynced { report, snapshot }),
            Err(e) => self.notes.push(Note::ResyncRefused(e.to_string())),
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
            ExecEvent::Order(u) => {
                // Our order this event names, and whether it had not ended before it.
                let named = match u.cid {
                    Some(CidMatch::Ours(cid)) => Some(cid),
                    _ => u.vid.as_ref().and_then(|v| self.reg.cid_of(v)),
                };
                let was_live = named
                    .and_then(|cid| self.reg.get(cid))
                    .is_some_and(|r| !r.state().is_terminal());
                let routed = self.reg.apply_update(&u, key);
                let not_ours = match routed {
                    Routed::Ours(cid, Applied::Advanced)
                        if was_live
                            && named == Some(cid)
                            && u.state
                                == fbc_core::VenueOrderState::Canceled(CancelReason::Requested)
                            && !self.cancel_of_ours(cid) =>
                    {
                        self.reg
                            .get(cid)
                            .filter(|r| {
                                r.state()
                                    == OrdState::Terminal(TerminalKind::Canceled(
                                        CancelReason::Requested,
                                    ))
                            })
                            .map(|r| Note::CancelNotOurs {
                                cid,
                                side: r.placed().side,
                            })
                    }
                    _ => None,
                };
                let note = match (routed, u.cid) {
                    (Routed::Untracked, Some(CidMatch::Ours(cid))) => {
                        Note::Orphan { cid, vid: u.vid }
                    }
                    (routed, _) => Note::Order(routed),
                };
                self.notes.push(note);
                self.notes.extend(not_ours);
            }
            ExecEvent::Fill(f) => {
                let time = env.exch_ts.map(|exch| FillTime {
                    exch,
                    kind: env.exch_ts_kind,
                    aligned: WallNs(exch.0),
                });
                let (seen, unexplained, ours) = match self.ledger.admit(&f, time, now) {
                    Admission::Apply(accepted) => match self.reg.apply_fill(accepted) {
                        Ok(routed) => (
                            describe_fill(&routed),
                            !matches!(routed, FillRouted::Ours(..) | FillRouted::InSnapshot(_)),
                            matches!(routed, FillRouted::Ours(..)),
                        ),
                        Err(e) => (format!("refused by the registry: {e:?}"), true, false),
                    },
                    held @ (Admission::Duplicate | Admission::BeforeWatermark) => {
                        (format!("not applied: {held:?}"), false, false)
                    }
                    other => (format!("not applied: {other:?}"), true, false),
                };
                self.notes.push(Note::Fill {
                    seen,
                    unexplained,
                    ours,
                });
            }
            ExecEvent::ResyncBegin { watermark } => {
                let requested_at = self.authenticated_at.unwrap_or(now);
                self.collecting = Some(ResyncSnapshot {
                    watermark,
                    requested_at,
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
            ExecEvent::ResyncEnd => self.on_resync_end(env.stamp.ingest_seq, env.venue_seq),
            ExecEvent::Conn { state, .. } => {
                match state {
                    ConnState::Authenticated => {
                        self.authenticated_at = Some(now);
                        self.epoch_authenticated = true;
                    }
                    ConnState::Closed => self.epoch_closed = true,
                    _ => {}
                }
                self.notes.push(Note::Conn(state));
            }
            ExecEvent::Position { inst, qty, .. } => {
                self.notes.push(Note::Position { inst, qty });
            }
            ExecEvent::AsyncReject { target, op, reject } => {
                self.notes.push(Note::AsyncReject { target, op, reject });
            }
            ExecEvent::UncorrelatedError(reject) => {
                let why = described(&reject);
                self.epoch_error = Some(why.clone());
                if self.epoch_authenticated {
                    self.problem(format!(
                        "venue error naming no request: {:?}, {why}",
                        reject.kind
                    ));
                } else {
                    self.notes.push(Note::LoginRefused(why));
                }
            }
            // The account's balances, funding, modes, query answers and fee rates move nothing
            // the registry holds here.
            _ => {}
        }
    }

    /// The orders of ours the session told unprotected (module documentation): each the
    /// registry holds open gets a Safety cancel at once.
    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        let told: Vec<(ClientOrderId, InstrumentId)> = orders
            .iter()
            .filter_map(|o| match o.cid {
                Some(CidMatch::Ours(cid)) => Some((cid, o.inst)),
                _ => None,
            })
            .collect();
        self.live.extend(told.iter().copied());
        let (open, ended): (Vec<ClientOrderId>, Vec<ClientOrderId>) = told
            .iter()
            .map(|(cid, _)| *cid)
            .partition(|cid| self.reg.get(*cid).is_some_and(|r| !r.state().is_terminal()));
        let mut left: Vec<String> = ended
            .iter()
            .map(|cid| {
                format!(
                    "client id sequence {}: the registry holds it ended or not at all, and no \
                     order query is built here (FBC-m8vm), so its market stays held",
                    cid.seq()
                )
            })
            .collect();
        let plan = self.reg.cancel_many(&open, &self.caps);
        let unbuilt = plan.refused.iter().map(|(cid, r)| (*cid, format!("{r:?}")));
        let waiting = plan
            .awaiting_ack
            .iter()
            .map(|cid| (*cid, "awaits its ack".to_owned()));
        for (cid, why) in unbuilt.chain(waiting) {
            left.push(format!(
                "client id sequence {}: no cancel built: {why}",
                cid.seq()
            ));
        }
        let mut rpcs = Vec::new();
        let session = self.session.clone();
        for cmd in plan.commands {
            let submitted = session
                .as_ref()
                .ok_or_else(|| "no session attached".to_owned())
                .and_then(|(orders, acct)| {
                    let auth = self
                        .reg
                        .authorize(*acct, cmd)
                        .map_err(|e| format!("{e:?}"))?;
                    self.submit(orders, auth)
                });
            match submitted {
                Ok(rpc) => {
                    for cid in self.cids_of(rpc) {
                        self.safety.insert(cid, rpc);
                    }
                    rpcs.push(rpc);
                }
                Err(e) => left.push(format!("a Safety cancel was not submitted: {e}")),
            }
        }
        self.notes.push(Note::Unprotected { told, rpcs, left });
    }

    /// Connection epoch `key` ended: noted with why ([`Note::EpochEnd`]), and the next starts
    /// unauthenticated with nothing said.
    fn on_epoch_end(&mut self, key: ConnKey) {
        let why = match (self.epoch_error.take(), self.epoch_closed) {
            (Some(error), _) => error,
            (None, true) => CODEC_CLOSED.to_owned(),
            (None, false) => ENDED_UNSAID.to_owned(),
        };
        self.epoch_authenticated = false;
        self.epoch_closed = false;
        self.notes.push(Note::EpochEnd { key, why });
    }
}

/// A fill's routing, without its amounts.
fn describe_fill(routed: &FillRouted) -> String {
    match routed {
        FillRouted::Ours(..) => "ours: counted on the order and the inventory".to_owned(),
        other => format!("{other:?}"),
    }
}

/// A command's kind, without its contents.
fn kind(cmd: &VenueCommand) -> &'static str {
    match cmd {
        VenueCommand::Place(_) => "place",
        VenueCommand::PlaceBatch(_) => "batch",
        VenueCommand::Amend(_) => "amend",
        VenueCommand::Cancel(_) => "cancel",
        VenueCommand::CancelMany(_) => "cancel-many",
        VenueCommand::CancelAll(_) => "cancel-all",
        VenueCommand::ArmCancelOnDisconnect(_) => "cancel-on-disconnect",
        VenueCommand::RefreshDeadMan => "dead-man refresh",
        VenueCommand::Query(_) => "query",
        VenueCommand::FeeQuery => "fee query",
    }
}

/// The session's handler: hands each event to the [`Link`] and wakes whoever waits on it.
pub struct LinkHandler {
    pub link: Rc<RefCell<Link>>,
    pub wake: Rc<Notify>,
}

impl ExecHandler for LinkHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.link.borrow_mut().on_event(env);
        self.wake.notify_waiters();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_epoch_end(key);
        self.wake.notify_waiters();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.link.borrow_mut().on_submitted(handle);
        self.wake.notify_waiters();
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.link.borrow_mut().on_unprotected(orders);
        self.wake.notify_waiters();
    }

    /// A frame or HTTP answer the codec could not decode: a problem, which ends the driver's
    /// trading (it may have held a fill, a position or an order event never seen).
    fn on_decode_error(&mut self, key: ConnKey) {
        self.link.borrow_mut().problem(format!(
            "connection {key:?}: a frame or HTTP answer the codec could not decode (it may have \
             held a fill, a position or an order event never seen)"
        ));
        self.wake.notify_waiters();
    }
}
