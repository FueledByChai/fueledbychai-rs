//! The place stages' tee on the run's journal (FBC-23ch, a slice of FBC-9sk2; decisions 0006,
//! 0009, 0132 item 7, 0148 item 8 and 0151 item 13): a [`JournalSink`] that wraps the run's queue
//! sink, delegates every call to it unchanged, `open_gap` included, so decision 0139's trip
//! still reads the queue's gaps, and reads each record on its way through for what the place
//! stages must see as it happens, which the codec and fbc-oms do not report: an order event or a
//! fill without our client id, an order event naming no order of ours, and a provider rate
//! refusal on a session.
//!
//! - **What it reads.** Each record exactly as the journal keeps it: its redaction spans blanked
//!   first, as [`refusals::kept`] blanks an inbound frame's, the whole frame when its spans do not
//!   fit it, and an outbound frame's spans as its `WireSlice` names them. Only the blanked text is
//!   parsed, with two exceptions, each read in memory and kept as a verdict alone. An inbound
//!   frame whose blanked text is no JSON object (one decision 0134 item 2 keeps whole among them)
//!   is parsed once more as it arrived, for whether it was a provider rate refusal. And an `ack`
//!   or `err` echo of an `add_order`, whose echoed `client_order_id` is a span of its own
//!   (decision 0134 item 4): the tee finds that span in the blanked text and compares the bytes
//!   the frame had there with each client id the run sent, keeping only whether they were exact,
//!   differed or were absent ([`EchoCid`]), never the bytes. It never formats a frame into an
//!   error or a line, and keeps no frame.
//! - **What it keeps** ([`Kept`], read through a [`Tap`]):
//!   - from each outbound `add_order`, its `client_order_id` as written between its quotes, when
//!     it was written, and whether the frame carried `take_profit`, `stop_loss` or `reduce_only`
//!     (FBC-blcw: the codec writes none of them); from each outbound `cancel_order`, that the
//!     order it names is ours;
//!   - from each inbound `order_response`, its status, as a status name or not one, and whether
//!     its `client_order_id` is absent (or null), empty, exactly one we sent (the raw bytes
//!     between its quotes: a JSON escape never matches), one we sent written otherwise (another
//!     case or an escape), or none of ours ([`Cid`]), and whether it names an order of ours;
//!   - from each inbound `fill_response`, the same, whether it names its `trade_id`, and its fee's
//!     sign ([`Sign`]), never its amount;
//!   - from each `ack` or `err` echo of an `add_order`, its envelope and its echoed client id
//!     ([`EchoCid`]), for the report alone: an echo never opens FBC-su9i's exact-echo gate;
//!   - every frame read, in arrival order with its receive time ([`Heard`]), so the order of an
//!     order's frames is kept too.
//! - **The orders of ours.** An order is ours once a frame names it with one of our client ids,
//!   exact or written otherwise, once a `cancel_order` of ours names it, or once the driver says
//!   so ([`Tap::ours`]).
//! - **Its stops** ([`Tap::stop`], [`Tap::stopped`]), the first raised being the run's:
//!   - an `order_response` or a `fill_response` naming an order of ours whose client id is not
//!     ours (absent, null, empty or another's): [`Stop::NotOurCid`] (decision 0132 item 7),
//!     except a `NO_SUCH_ORDER` with no client id naming an order of ours, which answers a
//!     cancel of ours that found no order (a cancel reject, decision 0132 item 8) and is kept
//!     with no stop;
//!   - an `order_response` naming no order of ours, on accounts `--sole-trader` declares ours
//!     alone: [`Stop::StrayOrder`] (0151 item 13), and so is a `fill_response` naming no order of
//!     ours, which shows as much that something else trades. An order is known ours only from a
//!     frame naming one of our client ids, a cancel of ours or the driver, so a first frame of X
//!     with no client id of ours, before X's venue id is known, is this stop too: it reads as
//!     another's order;
//!   - an `err` whose code is `RateLimited`, or an `order_response` whose status is
//!     `RATE_LIMITED`: a provider rate refusal on the session, [`Stop::RateRefused`] on
//!     [`RefusedOn::Session`] (decision 0148 item 8), a frame the journal keeps whole (decision
//!     0134 item 2) read once more in memory as it arrived for that alone, as a read socket's is;
//!     and an HTTP answer of the session's resync with status 429 or 418, the same on
//!     [`RefusedOn::Rest`].
//!
//!   A fill of ours, with or without our client id, is noted ([`Kept::fill_of_ours`]): in a drop
//!   session it is what lets a stop cancel (0151 item 2, `places::Stop::cancels_in_drop`).
//!
//! No `Secrets` passes through it: it reads what the journal keeps, and `src/auth_qfex.rs` is
//! unchanged. The run's refusal counts after the run still come from the journal
//! ([`crate::refusals`]).

use std::cell::{Ref, RefCell};
use std::ops::Range;
use std::rc::Rc;

use fbc_core::{InboundSpans, MonoNs, RawFrame, TrafficClass, WallNs};
use fbc_journal::{BLANK, JournalSink, Opcode, Record, RecordRef, Recorded};
use serde_json::{Map, Value};
use tokio::sync::Notify;

use crate::capture;
use crate::places::{RefusedOn, Stop};
use crate::refusals;

/// The run's journal sink, read on its way through ([`Tee`]'s module).
pub struct Tee<S> {
    inner: S,
    tap: Tap,
    /// A test's alone ([`Tee::refusing_normal`]): every Normal record answered dropped.
    #[cfg(test)]
    refuse_normal: bool,
}

impl<S: JournalSink> Tee<S> {
    /// Wraps `inner`, which every call is handed to unchanged.
    pub fn new(inner: S) -> Tee<S> {
        Tee {
            inner,
            tap: Tap::default(),
            #[cfg(test)]
            refuse_normal: false,
        }
    }

    /// The tee, but answering every Normal record dropped and counted without handing it on,
    /// as a queue past its soft limit does: a test's way to reach decision 0139's soft limit on
    /// the run's own journal. The program never builds it.
    #[cfg(test)]
    pub fn refusing_normal(self) -> Tee<S> {
        Tee {
            refuse_normal: true,
            ..self
        }
    }

    /// Whether `class` is refused ([`Tee::refusing_normal`]).
    fn refuses(&self, _class: TrafficClass) -> bool {
        #[cfg(test)]
        {
            self.refuse_normal && _class == TrafficClass::Normal
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    /// A handle on what it kept, shared with it.
    pub fn tap(&self) -> Tap {
        self.tap.clone()
    }

    /// The sink it wraps, whose drop counts the run reads once its writer closed.
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: JournalSink> JournalSink for Tee<S> {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.tap.observe(record);
        if self.refuses(class) {
            return Recorded::DroppedCounted;
        }
        self.inner.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        self.tap.observe_ref(record);
        if self.refuses(class) {
            return Recorded::DroppedCounted;
        }
        self.inner.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.inner.omit(class, now)
    }

    fn open_gap(&self) -> Option<u64> {
        self.inner.open_gap()
    }
}

/// How an inbound `order_response` or `fill_response` names its client id.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Cid {
    /// No `client_order_id` member, or null.
    Absent,
    /// An empty string.
    Empty,
    /// Exactly the client id of the `n`th `add_order` the run sent, byte for byte between its
    /// quotes.
    Exact(usize),
    /// The client id of the `n`th `add_order`, written otherwise: in another case, or with a
    /// JSON escape. Ours, but not byte for byte.
    Differs(usize),
    /// A client id that is none of ours.
    Foreign,
}

impl Cid {
    /// The `add_order` it names, exactly or not.
    pub fn sent(self) -> Option<usize> {
        match self {
            Cid::Exact(n) | Cid::Differs(n) => Some(n),
            _ => None,
        }
    }

    /// As the report names it: never the id.
    pub fn name(self) -> &'static str {
        match self {
            Cid::Absent => "absent",
            Cid::Empty => "empty",
            Cid::Exact(_) => "exact",
            Cid::Differs(_) => "ours, not byte for byte",
            Cid::Foreign => "not ours",
        }
    }
}

/// How an `ack` or `err` echo of an `add_order` gave back the client id it echoes, against each
/// client id the run sent: for the report alone.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EchoCid {
    /// The bytes at the echo's client-id span are exactly one the run sent.
    Exact,
    /// They are none the run sent, or the echo names the member more than once.
    Differs,
    /// The echo names no client id, null, or an empty one.
    Absent,
}

/// A fee's sign, never its amount.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Sign {
    Positive,
    Zero,
    Negative,
}

/// One outbound `add_order`: its client id as written, when it was written, and what else it
/// carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    /// Kept in memory only, to compare with what QFEX sends back: never written anywhere.
    cid: String,
    pub at: MonoNs,
    pub take_profit: bool,
    pub stop_loss: bool,
    pub reduce_only: bool,
}

/// One inbound frame the tee read, in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Heard {
    Order {
        at: MonoNs,
        /// The status as QFEX wrote it when it is a status name (capitals and underscores, at
        /// most 32), `(not a status name)` otherwise, `(none)` when absent.
        status: String,
        cid: Cid,
        ours: bool,
    },
    Fill {
        at: MonoNs,
        cid: Cid,
        ours: bool,
        trade_id: bool,
        fee: Option<Sign>,
    },
    Echo {
        at: MonoNs,
        /// `ack` or `err`.
        envelope: &'static str,
        cid: EchoCid,
    },
}

/// What the tee kept ([`Tee`]'s module): never a frame.
#[derive(Debug, Default)]
pub struct Kept {
    sent: Vec<Sent>,
    /// The venue ids of the orders of ours, as QFEX wrote them, kept in memory only.
    ours: Vec<String>,
    pub heard: Vec<Heard>,
    /// Every stop raised, in order; the first is the run's.
    pub stops: Vec<Stop>,
    /// Whether a fill of ours reached a session, with or without our client id.
    pub fill_of_ours: bool,
}

impl Kept {
    /// The `add_order`s the run sent, in order.
    pub fn sent(&self) -> &[Sent] {
        &self.sent
    }

    /// Whether QFEX sent an `order_response` whose status is `ACK` and whose client id is
    /// exactly one we sent, byte for byte (decision 0148 item 2's gate, which FBC-su9i opens).
    #[cfg_attr(not(test), allow(dead_code))] // FBC-su9i's per-run gate reads it
    pub fn exact_ack(&self) -> bool {
        self.exact_ack_from(0)
    }

    /// [`Kept::exact_ack`] among the frames heard from the `from`th on: one session's.
    pub fn exact_ack_from(&self, from: usize) -> bool {
        self.heard.iter().skip(from).any(
            |h| matches!(h, Heard::Order { status, cid: Cid::Exact(_), .. } if status == "ACK"),
        )
    }

    /// Whether QFEX sent an `order_response` whose status is `ACK` naming an order of ours, among
    /// the frames heard from the `from`th on: an order that rested, whether or not it still does.
    pub fn acked_ours_from(&self, from: usize) -> bool {
        (self.heard.iter().skip(from))
            .any(|h| matches!(h, Heard::Order { status, ours: true, .. } if status == "ACK"))
    }

    /// Whether QFEX sent an `order_response` whose status is `CANCELLED` naming the order whose
    /// `add_order` carried `cid`, exactly as written, by that client id, among the frames heard
    /// from the `from`th on. One naming it with no client id of ours is no report of it: that
    /// frame raises [`Stop::NotOurCid`].
    pub fn cancelled_of_from(&self, from: usize, cid: &str) -> bool {
        let Some(n) = self.sent.iter().position(|s| s.cid == cid) else {
            return false;
        };
        (self.heard.iter().skip(from)).any(|h| {
            matches!(h, Heard::Order { status, cid, .. }
                if status == "CANCELLED" && cid.sent() == Some(n))
        })
    }

    fn raise(&mut self, stop: Stop) -> bool {
        let first = self.stops.is_empty();
        self.stops.push(stop);
        first
    }

    fn is_ours(&self, order: Option<&str>) -> bool {
        order.is_some_and(|o| self.ours.iter().any(|ours| ours == o))
    }

    fn learn(&mut self, order: Option<&str>) {
        if let Some(order) = order
            && !self.is_ours(Some(order))
        {
            self.ours.push(order.to_owned());
        }
    }
}

/// A handle on what the tee kept, shared with it: the driver reads its stop and the report its
/// notes.
#[derive(Clone, Default)]
pub struct Tap {
    kept: Rc<RefCell<Kept>>,
    raised: Rc<Notify>,
}

impl Tap {
    /// What it kept so far.
    pub fn kept(&self) -> Ref<'_, Kept> {
        self.kept.borrow()
    }

    /// The first stop it raised, if any.
    pub fn stop(&self) -> Option<Stop> {
        self.kept.borrow().stops.first().copied()
    }

    /// Resolves with the first stop it raised: at once when one was, or when it raises one.
    pub async fn stopped(&self) -> Stop {
        loop {
            if let Some(stop) = self.stop() {
                return stop;
            }
            self.raised.notified().await;
        }
    }

    /// Says the order `venue_id` names is ours, as the registry learned it: its frames are then
    /// read as ours whatever client id they carry.
    pub fn ours(&self, venue_id: &str) {
        self.kept.borrow_mut().learn(Some(venue_id));
    }

    fn raise(&self, stop: Stop) {
        if self.kept.borrow_mut().raise(stop) {
            self.raised.notify_one();
        }
    }

    /// Reads `record`, an owned record offered to the journal.
    fn observe(&self, record: &Record) {
        match record {
            Record::Inbound {
                stamp,
                opcode: Opcode::Text,
                bytes,
                redact,
            } => {
                if let Ok(text) = std::str::from_utf8(&bytes.0) {
                    self.inbound(stamp.recv_mono, RawFrame::Text(text), redact);
                }
            }
            Record::Outbound {
                at,
                opcode: Opcode::Text,
                frame,
                ..
            } => self.outbound(*at, frame.bytes(), frame.redactions()),
            Record::HttpResult {
                result: Ok(resp), ..
            } => self.status(resp.status),
            _ => {}
        }
    }

    /// Reads `record`, a record offered to the journal by reference.
    fn observe_ref(&self, record: RecordRef<'_>) {
        match record {
            RecordRef::Owned(record) => self.observe(record),
            RecordRef::Inbound {
                stamp,
                frame: frame @ RawFrame::Text(_),
                redact,
            } => self.inbound(stamp.recv_mono, frame, redact),
            RecordRef::HttpResult {
                result: Ok(resp), ..
            } => self.status(resp.status),
            _ => {}
        }
    }

    /// An HTTP answer of a session's resync: 429 and 418 are provider rate refusals.
    fn status(&self, status: u16) {
        if matches!(status, 429 | 418) {
            self.raise(Stop::RateRefused {
                on: RefusedOn::Rest,
            });
        }
    }

    /// An outbound text frame, `spans` blanked first.
    fn outbound(&self, at: MonoNs, bytes: &[u8], spans: &[Range<u32>]) {
        let kept = blanked(bytes, spans);
        let Some(top) = object(&kept) else { return };
        let params = top.get("params").and_then(Value::as_object);
        match top.get("type").and_then(Value::as_str) {
            Some("add_order") => {
                let Some(params) = params else { return };
                let raw = string_members(kept.as_bytes(), b"client_order_id");
                let cid = match (raw.as_slice(), params.get("client_order_id")) {
                    ([one], _) => kept[one.clone()].to_owned(),
                    (_, Some(Value::String(cid))) => cid.clone(),
                    _ => return,
                };
                self.kept.borrow_mut().sent.push(Sent {
                    cid,
                    at,
                    take_profit: params.contains_key("take_profit"),
                    stop_loss: params.contains_key("stop_loss"),
                    reduce_only: params.contains_key("reduce_only"),
                });
            }
            Some("cancel_order") => {
                let order = params
                    .and_then(|p| p.get("order_id"))
                    .and_then(Value::as_str);
                self.kept.borrow_mut().learn(order);
            }
            _ => {}
        }
    }

    /// An inbound text frame `frame`, as the journal keeps it with `spans`.
    fn inbound(&self, at: MonoNs, frame: RawFrame<'_>, spans: &[Range<u32>]) {
        let kept = refusals::kept(frame, &InboundSpans::frame(spans.to_vec()));
        let Some(top) = object(&kept) else {
            self.whole(frame);
            return;
        };
        if let Some((envelope, body)) = capture::envelope(&top) {
            self.echo(at, frame.bytes(), &kept, envelope, body);
            return;
        }
        if top.len() != 1 {
            return;
        }
        match top.iter().next() {
            Some((name, Value::Object(order))) if name == "order_response" => {
                self.order(at, &kept, order);
            }
            Some((name, Value::Object(fill))) if name == "fill_response" => {
                self.fill(at, &kept, fill);
            }
            _ => {}
        }
    }

    /// A frame whose kept bytes are no JSON object, one decision 0134 item 2 keeps whole among
    /// them: read once more in memory, as it arrived, for a provider rate refusal alone, as a
    /// read socket's frame is ([`refusals::Refusals::hear`]). The codec, which reads only what
    /// the journal keeps, holds no budget for it (0134 item 6), so the session would go on.
    fn whole(&self, frame: RawFrame<'_>) {
        let arrived = match frame {
            RawFrame::Text(text) => text,
            RawFrame::Binary(_) => return,
        };
        if refusals::refusal(arrived).is_some() {
            self.raise(Stop::RateRefused {
                on: RefusedOn::Session,
            });
        }
    }

    /// An `ack` or `err` envelope: a `RateLimited` err is a refusal, and an echo of an
    /// `add_order` has its client id compared, raw, with each one sent.
    fn echo(
        &self,
        at: MonoNs,
        raw: &[u8],
        kept: &str,
        envelope: &'static str,
        body: &Map<String, Value>,
    ) {
        let code = body.get("error_code").and_then(Value::as_str);
        if envelope == "err" && code == Some("RateLimited") {
            self.raise(Stop::RateRefused {
                on: RefusedOn::Session,
            });
        }
        let echoed = body.get("incoming_message").and_then(Value::as_object);
        let Some(echoed) = echoed.filter(|m| m.get("type") == Some(&json_str("add_order"))) else {
            return;
        };
        let named = echoed.get("params").and_then(|p| p.get("client_order_id"));
        let cid = match named {
            Some(Value::String(s)) if !s.is_empty() => {
                match string_members(kept.as_bytes(), b"client_order_id").as_slice() {
                    [one] => {
                        let bytes = raw.get(one.clone()).unwrap_or_default();
                        let kept = self.kept.borrow();
                        match kept.sent.iter().any(|s| s.cid.as_bytes() == bytes) {
                            true => EchoCid::Exact,
                            false => EchoCid::Differs,
                        }
                    }
                    _ => EchoCid::Differs,
                }
            }
            _ => EchoCid::Absent,
        };
        let heard = Heard::Echo { at, envelope, cid };
        self.kept.borrow_mut().heard.push(heard);
    }

    /// An `order_response`.
    fn order(&self, at: MonoNs, kept: &str, order: &Map<String, Value>) {
        let status = match order.get("status") {
            None => "(none)".to_owned(),
            Some(Value::String(s)) if is_status_name(s) => s.clone(),
            Some(_) => "(not a status name)".to_owned(),
        };
        if status == "RATE_LIMITED" {
            self.raise(Stop::RateRefused {
                on: RefusedOn::Session,
            });
            let heard = Heard::Order {
                at,
                status,
                cid: Cid::Absent,
                ours: false,
            };
            self.kept.borrow_mut().heard.push(heard);
            return;
        }
        let (cid, ours, known) = self.judge(kept, order);
        // QFEX's answer to a cancel of ours that found no order, its order id and status alone:
        // a cancel reject, which the adapter reads as no order update (decision 0132 item 8), so
        // no order event without our client id.
        let rejected = status == "NO_SUCH_ORDER" && known && cid == Cid::Absent;
        let heard = Heard::Order {
            at,
            status,
            cid,
            ours,
        };
        self.kept.borrow_mut().heard.push(heard);
        if !rejected {
            self.stop_on(cid, ours, known);
        }
    }

    /// A `fill_response`.
    fn fill(&self, at: MonoNs, kept: &str, fill: &Map<String, Value>) {
        let (cid, ours, known) = self.judge(kept, fill);
        let trade_id = matches!(fill.get("trade_id"), Some(Value::String(s)) if !s.is_empty());
        let heard = Heard::Fill {
            at,
            cid,
            ours,
            trade_id,
            fee: fill.get("fee").and_then(sign),
        };
        let mut kept = self.kept.borrow_mut();
        kept.heard.push(heard);
        kept.fill_of_ours |= ours;
        drop(kept);
        self.stop_on(cid, ours, known);
    }

    /// `frame`'s client id, whether it names an order of ours, and whether its order was known
    /// ours before it; an order it names with one of our client ids becomes ours.
    fn judge(&self, kept: &str, frame: &Map<String, Value>) -> (Cid, bool, bool) {
        let order = frame.get("order_id").and_then(Value::as_str);
        let mut kept_by = self.kept.borrow_mut();
        let cid = cid_of(kept, frame, &kept_by.sent);
        let known = kept_by.is_ours(order);
        if cid.sent().is_some() {
            kept_by.learn(order);
        }
        (cid, known || cid.sent().is_some(), known)
    }

    /// The stop a frame of `cid` raises: none for one of our client ids; [`Stop::NotOurCid`] when
    /// it names an order known ours; [`Stop::StrayOrder`] when it names no order of ours.
    fn stop_on(&self, cid: Cid, ours: bool, known: bool) {
        if cid.sent().is_some() {
            return;
        }
        match (known, ours) {
            (true, _) => self.raise(Stop::NotOurCid),
            (false, false) => self.raise(Stop::StrayOrder),
            (false, true) => {}
        }
    }
}

/// `"text"` as a JSON value.
fn json_str(text: &str) -> Value {
    Value::String(text.to_owned())
}

/// Whether `status` reads as a status name: capitals, digits and underscores, 1 to 32 of them.
fn is_status_name(status: &str) -> bool {
    let named = |c: char| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_';
    (1..=32).contains(&status.len()) && status.chars().all(named)
}

/// `bytes` with each of `spans` blanked to [`BLANK`], the whole when one does not fit them, read
/// as text.
fn blanked(bytes: &[u8], spans: &[Range<u32>]) -> String {
    let mut out = bytes.to_vec();
    let fits = spans
        .iter()
        .all(|s| s.start < s.end && (s.end as usize) <= out.len());
    let end = u32::try_from(out.len()).unwrap_or(u32::MAX);
    let whole: Vec<Range<u32>> = std::iter::once(0..end).collect();
    for span in if fits { spans } else { &whole[..] } {
        let end = (span.end as usize).min(out.len());
        if let Some(part) = out.get_mut(span.start as usize..end) {
            part.fill(BLANK);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `text` as one JSON object, or `None`.
fn object(text: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(top)) => Some(top),
        _ => None,
    }
}

/// How `frame`, read from `kept`, names its client id against `sent`.
fn cid_of(kept: &str, frame: &Map<String, Value>, sent: &[Sent]) -> Cid {
    let decoded = match frame.get("client_order_id") {
        None | Some(Value::Null) => return Cid::Absent,
        Some(Value::String(s)) if s.is_empty() => return Cid::Empty,
        Some(Value::String(s)) => s,
        Some(_) => return Cid::Foreign,
    };
    if let [one] = string_members(kept.as_bytes(), b"client_order_id").as_slice() {
        let raw = &kept.as_bytes()[one.clone()];
        if let Some(n) = sent.iter().position(|s| s.cid.as_bytes() == raw) {
            return Cid::Exact(n);
        }
    }
    let same = |s: &Sent| s.cid.eq_ignore_ascii_case(decoded);
    sent.iter()
        .position(same)
        .map_or(Cid::Foreign, Cid::Differs)
}

/// A fee's sign: a JSON number, or a string holding a decimal number.
fn sign(fee: &Value) -> Option<Sign> {
    let text = match fee {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return None,
    };
    let decimal = |c: char| c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '+' | '-');
    let digits = text.trim_start_matches(['-', '+']);
    let leads = digits.starts_with(|c: char| c.is_ascii_digit() || c == '.');
    if !leads || !text.chars().all(decimal) {
        return None;
    }
    let mantissa = digits.split(['e', 'E']).next().unwrap_or("");
    if !mantissa.chars().any(|c| c.is_ascii_digit() && c != '0') {
        return Some(Sign::Zero);
    }
    match text.starts_with('-') {
        true => Some(Sign::Negative),
        false => Some(Sign::Positive),
    }
}

/// The byte ranges, between their quotes, of every string value of a member named `name`
/// (compared byte for byte between its quotes, so an escaped name never matches) in `text`, a
/// JSON text: a small scan, not a parser, which the caller has already parsed `text` with.
fn string_members(text: &[u8], name: &[u8]) -> Vec<Range<usize>> {
    let mut found = Vec::new();
    let mut at = 0;
    while let Some(open) = text[at.min(text.len())..].iter().position(|b| *b == b'"') {
        let Some(key) = string_at(text, at + open) else {
            break;
        };
        at = key.end + 1;
        let colon = skip_space(text, at);
        if text.get(colon) != Some(&b':') {
            continue;
        }
        let value = skip_space(text, colon + 1);
        if &text[key.clone()] == name
            && text.get(value) == Some(&b'"')
            && let Some(content) = string_at(text, value)
        {
            at = content.end + 1;
            found.push(content);
        }
    }
    found
}

/// The content of the string whose opening quote is at `quote`, up to its closing quote, or
/// `None` when it never closes.
fn string_at(text: &[u8], quote: usize) -> Option<Range<usize>> {
    let mut at = quote + 1;
    while at < text.len() {
        match text[at] {
            b'\\' => at += 2,
            b'"' => return Some(quote + 1..at),
            _ => at += 1,
        }
    }
    None
}

/// The first index at or after `at` that is not JSON white space.
fn skip_space(text: &[u8], mut at: usize) -> usize {
    while text
        .get(at)
        .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
    {
        at += 1;
    }
    at
}

#[cfg(test)]
mod tests {
    use super::*;

    use fbc_core::{ConnKey, Stamp, WireSlice};
    use fbc_journal::Opaque;

    /// A sink that counts what it is handed and names a gap, so the tee's delegation shows.
    #[derive(Default)]
    struct Counting {
        records: usize,
        refs: usize,
        omitted: usize,
        gap: Option<u64>,
    }

    impl JournalSink for Counting {
        fn record(&mut self, _: TrafficClass, _: WallNs, _: &Record) -> Recorded {
            self.records += 1;
            Recorded::Ok
        }

        fn record_ref(&mut self, _: TrafficClass, _: WallNs, _: RecordRef<'_>) -> Recorded {
            self.refs += 1;
            Recorded::Ok
        }

        fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
            self.omitted += 1;
            Recorded::DroppedCounted
        }

        fn open_gap(&self) -> Option<u64> {
            self.gap
        }
    }

    const CID: &str = "0007abcd00000001";
    const ORDER: &str = "11111111-1111-4111-8111-111111111111";
    const OTHER: &str = "99999999-9999-4999-8999-999999999999";

    fn stamp(mono: u64) -> Stamp {
        Stamp {
            ingest_seq: 0,
            kernel_rx: None,
            recv_mono: MonoNs(mono),
            recv_wall: WallNs(0),
            conn: ConnKey { conn: 0, epoch: 1 },
        }
    }

    fn teed() -> (Tee<Counting>, Tap) {
        let tee = Tee::new(Counting::default());
        let tap = tee.tap();
        (tee, tap)
    }

    /// Offers `text` inbound, by reference, with `spans`.
    fn inbound(tee: &mut Tee<Counting>, text: &str, spans: &[Range<u32>]) {
        let record = RecordRef::Inbound {
            stamp: stamp(5),
            frame: RawFrame::Text(text),
            redact: spans,
        };
        let _ = tee.record_ref(TrafficClass::Safety, WallNs(1), record);
    }

    /// Offers `text` inbound, owned.
    fn inbound_owned(tee: &mut Tee<Counting>, text: &str) {
        let record = Record::Inbound {
            stamp: stamp(6),
            opcode: Opcode::Text,
            bytes: Opaque(text.as_bytes().to_vec()),
            redact: Vec::new(),
        };
        let _ = tee.record(TrafficClass::Safety, WallNs(1), &record);
    }

    /// Offers `text` outbound, owned, through `record_ref` as the runtime does.
    fn outbound(tee: &mut Tee<Counting>, text: &str, spans: Vec<Range<u32>>) {
        let record = Record::Outbound {
            at: MonoNs(3),
            conn: ConnKey { conn: 0, epoch: 1 },
            rpc: None,
            opcode: Opcode::Text,
            frame: WireSlice::redacted(text.as_bytes().to_vec(), spans).unwrap(),
        };
        let _ = tee.record_ref(TrafficClass::Safety, WallNs(1), RecordRef::Owned(&record));
    }

    fn add_order(cid: &str) -> String {
        format!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"BUY","order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"client_order_id":"{cid}"}}}}"#
        )
    }

    fn order_response(order: &str, status: &str, cid: Option<&str>) -> String {
        let cid = cid.map_or(String::new(), |c| format!(r#","client_order_id":{c}"#));
        format!(
            r#"{{"order_response":{{"order_id":"{order}","symbol":"SYN-USD","status":"{status}","quantity":0.01,"price":99.5,"side":"BUY"{cid}}}}}"#
        )
    }

    fn fill_response(order: &str, cid: Option<&str>, fee: &str) -> String {
        let cid = cid.map_or(String::new(), |c| format!(r#","client_order_id":{c}"#));
        format!(
            r#"{{"fill_response":{{"trade_id":"55555555-5555-4555-8555-555555555555","order_id":"{order}","fee":{fee}{cid}}}}}"#
        )
    }

    /// One span alone.
    fn one(span: Range<u32>) -> Vec<Range<u32>> {
        std::iter::once(span).collect()
    }

    fn quoted(cid: &str) -> String {
        format!("\"{cid}\"")
    }

    #[test]
    fn every_call_reaches_the_sink_it_wraps_open_gap_included() {
        let (mut tee, _) = teed();
        tee.inner.gap = Some(7);
        inbound(&mut tee, "{}", &[]);
        inbound_owned(&mut tee, "{}");
        outbound(&mut tee, "{}", Vec::new());
        let _ = tee.omit(TrafficClass::Normal, WallNs(1));
        assert_eq!(tee.open_gap(), Some(7));
        let inner = tee.inner();
        assert_eq!((inner.records, inner.refs, inner.omitted), (1, 2, 1));
    }

    #[test]
    fn an_add_order_keeps_its_client_id_and_whether_it_carried_the_three_members() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        let with = r#"{"type":"add_order","params":{"client_order_id":"x","take_profit":0,"stop_loss":0,"reduce_only":false}}"#;
        outbound(&mut tee, with, Vec::new());
        // A frame with spans is read blanked: an authentication's key never reaches it.
        let auth = r#"{"type":"auth","params":{"hmac":{"public_key":"secret"}}}"#;
        let at = u32::try_from(auth.find("secret").unwrap()).unwrap();
        outbound(&mut tee, auth, one(at..at + 6));
        let kept = tap.kept();
        let sent = kept.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].cid, CID);
        assert_eq!(sent[0].at, MonoNs(3));
        assert!(!(sent[0].take_profit || sent[0].stop_loss || sent[0].reduce_only));
        assert!(sent[1].take_profit && sent[1].stop_loss && sent[1].reduce_only);
        assert!(kept.heard.is_empty() && kept.stops.is_empty());
    }

    #[test]
    fn an_ack_echoing_our_client_id_exactly_makes_its_order_ours_and_stops_nothing() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        inbound(
            &mut tee,
            &order_response(ORDER, "ACK", Some(&quoted(CID))),
            &[],
        );
        let kept = tap.kept();
        let heard = Heard::Order {
            at: MonoNs(5),
            status: "ACK".to_owned(),
            cid: Cid::Exact(0),
            ours: true,
        };
        assert_eq!(kept.heard, [heard]);
        assert!(kept.exact_ack());
        assert!(kept.ours.iter().any(|o| o == ORDER));
        assert_eq!(tap.stop(), None);
    }

    #[test]
    fn a_sessions_acks_and_cancels_are_read_from_its_first_frame_on() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        inbound(
            &mut tee,
            &order_response(ORDER, "ACK", Some(&quoted(CID))),
            &[],
        );
        inbound(
            &mut tee,
            &order_response(ORDER, "CANCELLED", Some(&quoted(CID))),
            &[],
        );
        // A second order of ours, sent and acknowledged, never cancelled.
        let other = "0007abcd00000002";
        let other_order = "22222222-2222-4222-8222-222222222222";
        outbound(&mut tee, &add_order(other), Vec::new());
        inbound(
            &mut tee,
            &order_response(other_order, "ACK", Some(&quoted(other))),
            &[],
        );
        let kept = tap.kept();
        // From the first frame: the ACK, exact and ours, and the CANCELLED of the first order.
        assert!(kept.exact_ack_from(0) && kept.acked_ours_from(0));
        assert!(kept.cancelled_of_from(0, CID) && kept.cancelled_of_from(1, CID));
        // The CANCELLED names the first order alone, and a client id never sent names none.
        assert!(!kept.cancelled_of_from(0, other) && !kept.cancelled_of_from(0, "never-sent"));
        // From the second: a later session's, which heard the CANCELLED and the second ACK.
        assert!(kept.exact_ack_from(1) && kept.acked_ours_from(1));
        assert!(!kept.exact_ack_from(3) && !kept.acked_ours_from(3));
        // From past every frame: nothing.
        assert!(!kept.cancelled_of_from(3, CID) && !kept.exact_ack_from(9));
        drop(kept);
        // A CANCELLED naming it by its client id written otherwise is still a report of it.
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        let upper = CID.to_uppercase();
        inbound(
            &mut tee,
            &order_response(ORDER, "CANCELLED", Some(&quoted(&upper))),
            &[],
        );
        assert!(tap.kept().cancelled_of_from(0, CID));
    }

    #[test]
    fn an_upper_cased_or_escaped_client_id_is_ours_but_not_byte_for_byte() {
        for written in [CID.to_uppercase(), CID.replacen('a', "\\u0061", 1)] {
            let (mut tee, tap) = teed();
            outbound(&mut tee, &add_order(CID), Vec::new());
            inbound(
                &mut tee,
                &order_response(ORDER, "ACK", Some(&quoted(&written))),
                &[],
            );
            let kept = tap.kept();
            assert!(matches!(
                kept.heard[0],
                Heard::Order {
                    cid: Cid::Differs(0),
                    ours: true,
                    ..
                }
            ));
            assert!(!kept.exact_ack());
            assert_eq!(tap.stop(), None);
        }
    }

    #[test]
    fn an_order_event_of_ours_without_our_client_id_stops_not_our_cid() {
        for cid in [None, Some("null"), Some(r#""""#), Some(r#""someone-else""#)] {
            let (mut tee, tap) = teed();
            outbound(&mut tee, &add_order(CID), Vec::new());
            inbound(
                &mut tee,
                &order_response(ORDER, "ACK", Some(&quoted(CID))),
                &[],
            );
            inbound(&mut tee, &order_response(ORDER, "CANCELLED", cid), &[]);
            assert_eq!(tap.stop(), Some(Stop::NotOurCid), "{cid:?}");
            let kept = tap.kept();
            let Heard::Order {
                cid: seen, ours, ..
            } = &kept.heard[1]
            else {
                panic!("an order");
            };
            assert!(*ours);
            let want = match cid {
                None | Some("null") => Cid::Absent,
                Some(r#""""#) => Cid::Empty,
                _ => Cid::Foreign,
            };
            assert_eq!(*seen, want);
        }
    }

    #[test]
    fn an_order_named_ours_by_a_cancel_or_the_driver_is_ours() {
        let (mut tee, tap) = teed();
        let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":"{ORDER}"}}}}"#);
        outbound(&mut tee, &cancel, Vec::new());
        tap.ours(OTHER);
        inbound(&mut tee, &order_response(OTHER, "CANCELLED", None), &[]);
        assert_eq!(tap.stop(), Some(Stop::NotOurCid));
        let (mut tee, tap) = teed();
        outbound(&mut tee, &cancel, Vec::new());
        inbound(&mut tee, &order_response(ORDER, "CANCELLED", None), &[]);
        assert_eq!(tap.stop(), Some(Stop::NotOurCid));
    }

    /// QFEX's `NO_SUCH_ORDER`, its order id and status alone, as the adapter reads it.
    fn no_such(order: &str) -> String {
        format!(r#"{{"order_response":{{"order_id":"{order}","status":"NO_SUCH_ORDER"}}}}"#)
    }

    #[test]
    fn a_no_such_order_answering_a_cancel_of_ours_is_a_cancel_reject_and_stops_nothing() {
        let (mut tee, tap) = teed();
        let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":"{ORDER}"}}}}"#);
        outbound(&mut tee, &cancel, Vec::new());
        inbound(&mut tee, &no_such(ORDER), &[]);
        assert_eq!(tap.stop(), None);
        let heard = Heard::Order {
            at: MonoNs(5),
            status: "NO_SUCH_ORDER".to_owned(),
            cid: Cid::Absent,
            ours: true,
        };
        assert_eq!(tap.kept().heard, [heard]);
        // An order of ours placed and acknowledged, then a cancel that raced its end: the same.
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        inbound(
            &mut tee,
            &order_response(ORDER, "ACK", Some(&quoted(CID))),
            &[],
        );
        outbound(&mut tee, &cancel, Vec::new());
        inbound(&mut tee, &no_such(ORDER), &[]);
        assert_eq!(tap.stop(), None);
        // One naming no order of ours is another's cancel on the account: a stray order.
        let (mut tee, tap) = teed();
        outbound(&mut tee, &cancel, Vec::new());
        inbound(&mut tee, &no_such(OTHER), &[]);
        assert_eq!(tap.stop(), Some(Stop::StrayOrder));
        // One naming an order of ours with a client id not ours is still not ours.
        let (mut tee, tap) = teed();
        outbound(&mut tee, &cancel, Vec::new());
        let foreign = order_response(ORDER, "NO_SUCH_ORDER", Some(r#""someone-else""#));
        inbound(&mut tee, &foreign, &[]);
        assert_eq!(tap.stop(), Some(Stop::NotOurCid));
    }

    #[test]
    fn an_order_response_naming_no_order_of_ours_stops_stray_order() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        let stray = order_response(OTHER, "ACK", Some(r#""someone-else""#));
        inbound(&mut tee, &stray, &[]);
        assert_eq!(tap.stop(), Some(Stop::StrayOrder));
        assert!(matches!(
            tap.kept().heard[0],
            Heard::Order {
                cid: Cid::Foreign,
                ours: false,
                ..
            }
        ));
    }

    #[test]
    fn a_fill_of_ours_is_noted_and_one_without_our_client_id_stops_not_our_cid() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        inbound(
            &mut tee,
            &fill_response(ORDER, Some(&quoted(CID)), "0.002"),
            &[],
        );
        assert_eq!(tap.stop(), None);
        assert!(tap.kept().fill_of_ours);
        let heard = Heard::Fill {
            at: MonoNs(5),
            cid: Cid::Exact(0),
            ours: true,
            trade_id: true,
            fee: Some(Sign::Positive),
        };
        assert_eq!(tap.kept().heard, [heard]);
        // The next fill of the order, its client id absent: ours, so noted, and a stop.
        inbound(&mut tee, &fill_response(ORDER, None, r#""-1e-4""#), &[]);
        assert_eq!(tap.stop(), Some(Stop::NotOurCid));
        assert!(matches!(
            tap.kept().heard[1],
            Heard::Fill {
                cid: Cid::Absent,
                ours: true,
                fee: Some(Sign::Negative),
                ..
            }
        ));
    }

    #[test]
    fn a_fill_naming_no_order_of_ours_stops_stray_order_and_is_not_ours() {
        let (mut tee, tap) = teed();
        inbound(&mut tee, &fill_response(OTHER, None, "0"), &[]);
        assert_eq!(tap.stop(), Some(Stop::StrayOrder));
        let kept = tap.kept();
        assert!(!kept.fill_of_ours);
        assert!(matches!(
            kept.heard[0],
            Heard::Fill {
                fee: Some(Sign::Zero),
                ours: false,
                ..
            }
        ));
    }

    #[test]
    fn a_fee_keeps_only_its_sign() {
        let signs = [
            ("0.002", Some(Sign::Positive)),
            ("-0.5", Some(Sign::Negative)),
            ("0", Some(Sign::Zero)),
            ("0.000", Some(Sign::Zero)),
            (r#""1e-4""#, Some(Sign::Positive)),
            (r#""-0.0""#, Some(Sign::Zero)),
            (r#""a lot""#, None),
            (r#""""#, None),
            ("null", None),
        ];
        for (fee, want) in signs {
            let value: Value = serde_json::from_str(fee).unwrap();
            assert_eq!(sign(&value), want, "{fee}");
        }
    }

    #[test]
    fn a_rate_limited_err_or_order_response_or_a_429_stops_rate_refused() {
        let limited = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 1 seconds","incoming_message":{"type":"get_order"}}"#;
        let nested =
            r#"{"err":{"error_code":"RateLimited","incoming_message":{"type":"get_order"}}}"#;
        let order = r#"{"order_response":{"symbol":"SYN-USD","status":"RATE_LIMITED"}}"#;
        let session = Stop::RateRefused {
            on: RefusedOn::Session,
        };
        for frame in [limited, nested, order] {
            let (mut tee, tap) = teed();
            inbound(&mut tee, frame, &[]);
            assert_eq!(tap.stop(), Some(session), "{frame}");
        }
        for status in [429, 418] {
            let (mut tee, tap) = teed();
            let failure = fbc_journal::HttpResponseRec {
                status,
                headers: Vec::new(),
                body: Opaque(Vec::new()),
                body_redact: Vec::new(),
            };
            let record = Record::HttpResult {
                stamp: stamp(1),
                tag: fbc_core::HttpTag(0),
                result: Ok(failure),
            };
            let _ = tee.record(TrafficClass::Safety, WallNs(1), &record);
            let rest = Stop::RateRefused {
                on: RefusedOn::Rest,
            };
            assert_eq!(tap.stop(), Some(rest));
            // The same answer offered by reference, as the runtime offers a session's.
            let (mut tee, tap) = teed();
            let answer = fbc_journal::ResponseRef {
                status,
                headers: &[],
                body: b"",
                marks: &[],
                body_redact: &[],
            };
            let record = RecordRef::HttpResult {
                stamp: stamp(1),
                tag: fbc_core::HttpTag(0),
                result: Ok(answer),
            };
            let _ = tee.record_ref(TrafficClass::Safety, WallNs(1), record);
            assert_eq!(tap.stop(), Some(rest));
            assert_eq!(tee.inner().refs, 1);
        }
        // Any other err, and a 500, raise nothing.
        let (mut tee, tap) = teed();
        let other =
            r#"{"type":"Err","error_code":"InvalidOrder","incoming_message":{"type":"get_order"}}"#;
        inbound(&mut tee, other, &[]);
        tap.status(500);
        assert_eq!(tap.stop(), None);
    }

    #[test]
    fn a_rate_limited_err_the_journal_keeps_whole_is_read_in_memory_and_stops_rate_refused() {
        // An echo holding a JSON escape, which decision 0134 item 2 keeps whole: its kept bytes
        // are all blank, so only the frame as it arrived shows the refusal.
        let escaped = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 1 seconds","incoming_message":{"type":"get_order","params":{"order_id":"a\u0062"}}}"#;
        let whole = u32::try_from(escaped.len()).unwrap();
        let (mut tee, tap) = teed();
        inbound(&mut tee, escaped, &one(0..whole));
        let session = Stop::RateRefused {
            on: RefusedOn::Session,
        };
        assert_eq!(tap.stop(), Some(session));
        assert!(tap.kept().heard.is_empty());
        // A frame kept whole that is no refusal raises nothing.
        let other = r#"{"type":"Err","error_code":"InvalidOrder","incoming_message":{"type":"get_order","params":{"order_id":"a\u0062"}}}"#;
        let (mut tee, tap) = teed();
        inbound(
            &mut tee,
            other,
            &one(0..u32::try_from(other.len()).unwrap()),
        );
        assert_eq!(tap.stop(), None);
    }

    /// An ack echoing `add_order(cid)`, its client id at the span the codec names for it.
    fn echo_of(envelope: &str, cid: &str) -> (String, Vec<Range<u32>>) {
        let frame = match envelope {
            "ack" => format!(r#"{{"ack":{{"incoming_message":{}}}}}"#, add_order(cid)),
            _ => format!(
                r#"{{"type":"Err","error_code":"InvalidOrder","incoming_message":{}}}"#,
                add_order(cid)
            ),
        };
        let at = frame.find(&quoted(cid)).unwrap() + 1;
        let span = u32::try_from(at).unwrap()..u32::try_from(at + cid.len()).unwrap();
        (frame, vec![span])
    }

    #[test]
    fn an_echo_of_an_add_order_compares_its_client_id_span_raw_for_the_report_alone() {
        for (envelope, written, want) in [
            ("ack", CID.to_owned(), EchoCid::Exact),
            ("err", CID.to_owned(), EchoCid::Exact),
            ("ack", CID.to_uppercase(), EchoCid::Differs),
            ("ack", "someone-else".to_owned(), EchoCid::Differs),
        ] {
            let (mut tee, tap) = teed();
            outbound(&mut tee, &add_order(CID), Vec::new());
            let (frame, spans) = echo_of(envelope, &written);
            inbound(&mut tee, &frame, &spans);
            let kept = tap.kept();
            let Heard::Echo {
                cid, envelope: e, ..
            } = kept.heard[0]
            else {
                panic!("an echo");
            };
            assert_eq!((cid, e), (want, envelope));
            // An echo never opens the gate and raises nothing.
            assert!(!kept.exact_ack());
            assert_eq!(tap.stop(), None);
        }
        // No client id, an empty one, or none echoed at all.
        let (mut tee, tap) = teed();
        let none = r#"{"ack":{"incoming_message":{"type":"add_order","params":{"symbol":"2"}}}}"#;
        inbound(&mut tee, none, &[]);
        let empty =
            r#"{"ack":{"incoming_message":{"type":"add_order","params":{"client_order_id":""}}}}"#;
        inbound(&mut tee, empty, &[]);
        let other = r#"{"ack":{"incoming_message":{"type":"get_order"}}}"#;
        inbound(&mut tee, other, &[]);
        let cids: Vec<EchoCid> = (tap.kept().heard.iter())
            .filter_map(|h| match h {
                Heard::Echo { cid, .. } => Some(*cid),
                _ => None,
            })
            .collect();
        assert_eq!(cids, [EchoCid::Absent, EchoCid::Absent]);
    }

    #[test]
    fn a_frame_whose_spans_do_not_fit_or_that_is_no_object_is_read_as_nothing() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        // Spans past the frame blank all of it, as the journal would.
        let frame = order_response(OTHER, "ACK", None);
        inbound(&mut tee, &frame, &one(0..10_000));
        inbound(&mut tee, "not json", &[]);
        inbound(&mut tee, r#"{"order_response":{},"extra":1}"#, &[]);
        inbound(&mut tee, r#"{"position_response":{}}"#, &[]);
        let binary = RecordRef::Inbound {
            stamp: stamp(1),
            frame: RawFrame::Binary(frame.as_bytes()),
            redact: &[],
        };
        let _ = tee.record_ref(TrafficClass::Safety, WallNs(1), binary);
        let kept = tap.kept();
        assert!(kept.heard.is_empty() && kept.stops.is_empty());
        assert_eq!(blanked(b"abcd", &[1..2, 2..9]), "2222");
        assert_eq!(blanked(b"abcd", &one(1..3)), "a22d");
    }

    #[test]
    fn a_status_is_kept_only_as_a_status_name() {
        let (mut tee, tap) = teed();
        outbound(&mut tee, &add_order(CID), Vec::new());
        let cid = quoted(CID);
        inbound(&mut tee, &order_response(ORDER, "ACK", Some(&cid)), &[]);
        inbound(
            &mut tee,
            &order_response(ORDER, "not a status", Some(&cid)),
            &[],
        );
        let unnamed =
            format!(r#"{{"order_response":{{"order_id":"{ORDER}","client_order_id":{cid}}}}}"#);
        inbound(&mut tee, &unnamed, &[]);
        let statuses: Vec<String> = (tap.kept().heard.iter())
            .filter_map(|h| match h {
                Heard::Order { status, .. } => Some(status.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(statuses, ["ACK", "(not a status name)", "(none)"]);
        assert!(is_status_name("IOC_PARTIALLY_FILLED"));
        assert!(!is_status_name(&"A".repeat(33)));
        assert!(!is_status_name(""));
    }

    #[test]
    fn only_the_first_stop_wakes_the_driver_and_every_stop_is_kept() {
        let (mut tee, tap) = teed();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        // Nothing raised yet: the wait is pending.
        let waited = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_millis(10), tap.stopped()).await
        });
        assert!(waited.is_err());
        inbound(&mut tee, &order_response(OTHER, "ACK", None), &[]);
        let order = r#"{"order_response":{"status":"RATE_LIMITED"}}"#;
        inbound(&mut tee, order, &[]);
        assert_eq!(rt.block_on(tap.stopped()), Stop::StrayOrder);
        let session = Stop::RateRefused {
            on: RefusedOn::Session,
        };
        assert_eq!(tap.kept().stops, [Stop::StrayOrder, session]);
    }

    #[test]
    fn string_members_finds_each_value_of_a_member_by_its_raw_name() {
        let text = r#"{"a":"x","client_order_id" : "one","n":{"client_order_id":"t\"wo"},"v":"client_order_id","c"#;
        let text = [
            text,
            "\\u006c",
            r#"ient_order_id":"no","client_order_id":5}"#,
        ]
        .concat();
        let text = text.as_bytes();
        let found = string_members(text, b"client_order_id");
        let values: Vec<&[u8]> = found.iter().map(|r| &text[r.clone()]).collect();
        assert_eq!(values, [&b"one"[..], &br#"t\"wo"#[..]]);
        assert_eq!(
            string_members(br#"{"client_order_id":"open"#, b"client_order_id"),
            []
        );
        assert_eq!(string_at(br#""a\"#, 0), None);
        assert_eq!(skip_space(b" \t\n\rx", 0), 4);
    }
}
