//! What the toy's codec holds between frames (FBC-sal): the requests still waiting for their
//! answers, a resync being read, and an authentication asked for. Records, one per frame:
//!
//! - `item|rpc|i|res|vid?|code?|msg?|ts?|seq?`: the answer to item `i` of request `rpc`, `res`
//!   `ok` (accepted, with the venue id `vid` it assigned) or `rej` (refused, `code` read
//!   through [`REJECT_CODES`](super::REJECT_CODES)). A request's item outcomes are held until
//!   every item is answered and then pushed in one call, since the first answer clears the
//!   whole request's deadline (record 0014 item 3); [`Answers::timed_out`] pushes those held
//!   and `Unknown` only for the items still unanswered.
//! - `qres|rpc|found|...|ts?|seq?`: the answer to order query `rpc`, `found` `0` or `1` with
//!   the order's fields as a resync's (r4172456271). It carries the query's rpc and its target
//!   (r4172835741), and a snapshot of another order than the one queried is refused.
//! - `rsbegin|wm`, `rsorder|cid|vid|sym|side|st|px?|qty|cum|po|ro`, `rspos|sym|qty|avg?`,
//!   `rsend`: a resync answered in frames, `wm` echoing the instant it was asked for, which is
//!   its watermark (0014's alternatives). Its events are held until `rsend` and then pushed in
//!   one call, so nothing is pushed from a resync cut short (0014 item 2): a reconnect, a new
//!   resync, or any frame of it refused drops it whole.
//! - A resync over REST (FBC-ja3, Codex r4172917335), for a codec built with
//!   [`ToyExec::rest_resync`](super::ToyExec::rest_resync): `GET <base>/resync?ts=<wall>`, its
//!   response body those same records one per line, `rsbegin` first and `rsend` last. The whole
//!   body is decoded, its envelope included, before anything is pushed, all in one call. An
//!   [`HttpFailure`], a status other than 200 or a body it cannot decode whole pushes nothing
//!   and asks again after [`RESYNC_RETRY`], on a timer carrying the failed request's tag (Codex
//!   r4219602910: a superseded resync's timer retries nothing), at the instant of the retry,
//!   which is the new resync's watermark. A response to a request superseded (a new resync, a reconnect) or
//!   already answered is ignored.
//! - `auth|ok|token?|code?|msg?`: the answer to the authentication `on_open` sends, `ok` `1`
//!   echoing the toy's token (whose span [`token_spans`] names, 0028) or `0` with the
//!   venue's code. Only an acknowledgement of an authentication asked for reports the stream
//!   [`ConnState::Authenticated`].

use std::collections::HashMap;
use std::ops::Range;

use fbc_core::{
    AckLevel, ClientOrderId, ConnState, DecodeError, DecodeScope, Effect, Effects, EncodeCtx,
    ExecEvent, ExecSink, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, Inbound,
    InboundSpans, ItemRef, OpKind, OrderRef, QueryAnswer, RateCharge, RawFrame, RpcId, SpecTable,
    StreamId, SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueMeta, WallNs, WireSlice,
    WireUrl,
};

use super::decode::{self, Record};
use super::url::join;
use super::{RESYNC_RETRY, RESYNC_TIMEOUT, TOY_TOKEN};

use DecodeError::Malformed;

/// The field of an `auth` record, sent and echoed, that carries the toy's token.
const TOKEN_FIELD: &str = "token=";

/// What one sent request still waits for.
enum Waiting {
    /// An order-entry request's items, in order.
    Items(Vec<Item>),
    /// An order query, for the order it names.
    Query(OrderRef),
}

/// One item of a request: our client id where its command names one, and its answer once
/// decoded.
struct Item {
    cid: Option<ClientOrderId>,
    answer: Option<(VenueMeta, ItemRef, SubmitOutcome)>,
}

/// A resync asked for at `watermark`; once begun, the events of its frames read so far.
struct Resync {
    watermark: WallNs,
    held: Option<Vec<(VenueMeta, ExecEvent)>>,
}

/// A resync asked for over REST at `watermark` under `tag`; `retrying` once its request failed
/// or came back unreadable, until its retry timer fires.
struct RestResync {
    tag: HttpTag,
    watermark: WallNs,
    retrying: bool,
}

/// The session state the toy's codec answers requests from.
#[derive(Default)]
pub(super) struct Answers {
    waiting: HashMap<RpcId, Waiting>,
    resync: Option<Resync>,
    rest: Option<RestResync>,
    /// The tag of the last HTTP request asked for.
    last_tag: u64,
    auth_asked: bool,
}

impl Answers {
    /// Request `rpc`, `cmd`, was encoded: it waits for its answer. The toy answers no fee
    /// query, so that one only ever times out.
    pub(super) fn sent(&mut self, rpc: RpcId, cmd: &VenueCommand) {
        let item = |cid| Item { cid, answer: None };
        let waiting = match cmd {
            VenueCommand::Query(q) => Waiting::Query(q.target.clone()),
            VenueCommand::FeeQuery => return,
            VenueCommand::Place(o) => Waiting::Items(vec![item(Some(o.cid))]),
            VenueCommand::PlaceBatch(orders) => {
                Waiting::Items(orders.iter().map(|o| item(Some(o.cid))).collect())
            }
            VenueCommand::Amend(a) => Waiting::Items(vec![item(a.target.client())]),
            VenueCommand::Cancel(c) => Waiting::Items(vec![item(c.target.client())]),
            VenueCommand::CancelMany(cancels) => {
                Waiting::Items(cancels.iter().map(|c| item(c.target.client())).collect())
            }
            VenueCommand::CancelAll(_)
            | VenueCommand::ArmCancelOnDisconnect(_)
            | VenueCommand::RefreshDeadMan => Waiting::Items(vec![item(None)]),
        };
        self.waiting.insert(rpc, waiting);
    }

    /// Request `rpc` was answered as a whole (a refusal of every item): nothing of it waits.
    pub(super) fn answered(&mut self, rpc: RpcId) {
        self.waiting.remove(&rpc);
    }

    /// Request `rpc`'s deadline passed unanswered: the outcomes held for it, each with its
    /// venue id, then `Unknown` for every item still unanswered, by index; `Unknown` for the
    /// whole request when none was answered.
    pub(super) fn timed_out(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let unknown = SubmitOutcome::Unknown;
        let items = match self.waiting.remove(&rpc) {
            Some(Waiting::Items(items)) if items.iter().any(|i| i.answer.is_some()) => items,
            _ => {
                let (item, outcome) = (None, unknown);
                sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
                return;
            }
        };
        for (idx, it) in (0..).zip(items) {
            let (meta, item, outcome) = it.answer.unwrap_or_else(|| {
                let (cid, vid) = (it.cid, None);
                (VenueMeta::NONE, ItemRef { idx, cid, vid }, unknown.clone())
            });
            let item = Some(item);
            sink.push(meta, ExecEvent::Outcome { rpc, item, outcome });
        }
    }

    /// An `item` record: held, and with the request's last item every outcome pushed.
    pub(super) fn item(
        &mut self,
        r: &Record<'_>,
        scope: &DecodeScope<'_>,
        sink: &mut dyn ExecSink,
    ) -> Result<(), DecodeError> {
        let rpc = RpcId(r.num("rpc")?);
        let Some(Waiting::Items(items)) = self.waiting.get_mut(&rpc) else {
            return Err(Malformed("rpc"));
        };
        let idx: u16 = r.num("i")?;
        let slot = items.get_mut(usize::from(idx));
        let slot = slot
            .filter(|it| it.answer.is_none())
            .ok_or(Malformed("i"))?;
        let outcome = match r.get("res")? {
            "ok" => SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            },
            "rej" => SubmitOutcome::Rejected(decode::refusal(r)?),
            _ => return Err(Malformed("res")),
        };
        let vid = r.opt("vid").map(|v| scope.venue_order_id(v)).transpose()?;
        let meta = r.meta(false)?;
        let item = ItemRef {
            idx,
            cid: slot.cid,
            vid,
        };
        slot.answer = Some((meta, item, outcome));
        if items.iter().all(|it| it.answer.is_some()) {
            self.timed_out(rpc, sink);
        }
        Ok(())
    }

    /// A `qres` record: the answer to a query waiting for one, about the order it named.
    pub(super) fn query(
        &mut self,
        r: &Record<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
    ) -> Result<(), DecodeError> {
        let rpc = RpcId(r.num("rpc")?);
        let Some(Waiting::Query(target)) = self.waiting.get(&rpc) else {
            return Err(Malformed("rpc"));
        };
        let found = match r.get("found")? {
            "1" => Some(decode::snapshot(r, scope, specs)?),
            "0" => None,
            _ => return Err(Malformed("found")),
        };
        let meta = r.meta(false)?;
        let answer = QueryAnswer::new(rpc, target.clone(), found);
        let answer = answer.ok_or(Malformed("another order"))?;
        self.waiting.remove(&rpc);
        sink.push(meta, ExecEvent::QueryResult(answer));
        Ok(())
    }

    /// Asks for the venue's open orders and positions as of `ctx.wall`, dropping any resync
    /// still being read.
    pub(super) fn ask_resync(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        let watermark = ctx.wall;
        self.resync = Some(Resync {
            watermark,
            held: None,
        });
        fx.push(Effect::Send {
            stream,
            frame: WireSlice::plain(format!("resync|ts={}", watermark.0).into_bytes()),
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Query, None),
        });
    }

    /// One frame of a resync: held, and with `rsend` the whole resync pushed. A frame refused
    /// drops the resync, so none of it is ever pushed.
    pub(super) fn resync_frame(
        &mut self,
        r: &Record<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
    ) -> Result<(), DecodeError> {
        let read = self.read_resync(r, scope, specs);
        if !matches!(read, Ok(None)) {
            self.resync = None;
        }
        for (meta, ev) in read?.unwrap_or_default() {
            sink.push(meta, ev);
        }
        Ok(())
    }

    /// Reads one resync frame into the resync asked for: the whole resync once it ends.
    fn read_resync(
        &mut self,
        r: &Record<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
    ) -> Result<Option<Vec<(VenueMeta, ExecEvent)>>, DecodeError> {
        let resync = self.resync.as_mut().ok_or(Malformed("no resync asked for"));
        if r.kind == "rsbegin" {
            let resync = resync?;
            if resync.held.is_some() {
                return Err(Malformed("resync begun twice"));
            }
            if WallNs(r.num("wm")?) != resync.watermark {
                return Err(Malformed("resync for another request"));
            }
            let watermark = resync.watermark;
            let begin = (r.meta(false)?, ExecEvent::ResyncBegin { watermark });
            resync.held = Some(vec![begin]);
            return Ok(None);
        }
        let held = resync.ok().and_then(|resync| resync.held.as_mut());
        let held = held.ok_or(Malformed("no resync begun"))?;
        held.push(resync_event(r, scope, specs)?);
        Ok((r.kind == "rsend").then(|| core::mem::take(held)))
    }

    /// Asks over REST, under `base`, for the venue's open orders and positions as of
    /// `ctx.wall`, dropping any resync still being read or asked for.
    pub(super) fn ask_rest_resync(&mut self, base: &WireUrl, ctx: &EncodeCtx, fx: &mut Effects) {
        let watermark = ctx.wall;
        self.resync = None;
        self.last_tag += 1;
        let tag = HttpTag(self.last_tag);
        self.rest = Some(RestResync {
            tag,
            watermark,
            retrying: false,
        });
        fx.push(Effect::Http {
            tag,
            req: HttpRequest {
                method: HttpMethod::Get,
                url: join(base, &format!("/resync?ts={}", watermark.0)),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: RESYNC_TIMEOUT,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Rest, None),
        });
    }

    /// The response to the HTTP request tagged `tag`: a resync over REST decoded whole and
    /// pushed in one call, or, failed or unreadable, nothing pushed and asked again after
    /// [`RESYNC_RETRY`]. One to a request no longer waited for is ignored.
    pub(super) fn rest_answer(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let waiting = self.rest.as_mut().filter(|r| r.tag == tag && !r.retrying);
        let Some(asked) = waiting else {
            return Ok(());
        };
        let read = match resp {
            Ok(resp) if resp.status == 200 => {
                Some(rest_body(resp.body, asked.watermark, scope, specs))
            }
            _ => None,
        };
        match read {
            Some(Ok(events)) => {
                self.rest = None;
                for (meta, ev) in events {
                    sink.push(meta, ev);
                }
                Ok(())
            }
            unread => {
                asked.retrying = true;
                fx.push(Effect::Timer {
                    tag: TimerTag(tag.0),
                    after: RESYNC_RETRY,
                });
                unread.map_or(Ok(()), |refused| refused.map(drop))
            }
        }
    }

    /// Timer `tag` fired: the resync over REST whose request carried that tag, failed, is asked
    /// again under `base` at `ctx.wall`, unless it was superseded meanwhile.
    pub(super) fn retry_rest(
        &mut self,
        tag: TimerTag,
        base: &WireUrl,
        ctx: &EncodeCtx,
        fx: &mut Effects,
    ) {
        let due = |r: &RestResync| r.retrying && r.tag.0 == tag.0;
        if self.rest.as_ref().is_some_and(due) {
            self.ask_rest_resync(base, ctx, fx);
        }
    }

    /// Authenticates on `stream` with the toy's token, in a redaction span; a resync still
    /// being read was cut short by the reconnect.
    pub(super) fn ask_auth(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.auth_asked = true;
        self.resync = None;
        self.rest = None;
        let head = format!("auth|ts={}|{TOKEN_FIELD}", ctx.wall.0);
        let text = format!("{head}{TOY_TOKEN}");
        let span = span(head.len()..text.len()).expect("an authentication shorter than 4 GiB");
        let frame = WireSlice::redacted(text.into_bytes(), vec![span]);
        fx.push(Effect::Send {
            stream,
            frame: frame.expect("the token's span is inside the frame"),
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Control, None),
        });
    }

    /// An `auth` record: the stream authenticated, or the venue's refusal, once per
    /// authentication asked for.
    pub(super) fn auth(
        &mut self,
        stream: StreamId,
        r: &Record<'_>,
        sink: &mut dyn ExecSink,
    ) -> Result<(), DecodeError> {
        if !self.auth_asked {
            return Err(Malformed("no authentication asked"));
        }
        let event = match r.get("ok")? {
            "1" => {
                // The token is echoed and never read: replay hands it back blanked (0028).
                r.opt("token")
                    .filter(|t| !t.is_empty())
                    .ok_or(Malformed("token"))?;
                let state = ConnState::Authenticated;
                ExecEvent::Conn { stream, state }
            }
            "0" => ExecEvent::UncorrelatedError(decode::refusal(r)?),
            _ => return Err(Malformed("ok")),
        };
        let meta = r.meta(false)?;
        self.auth_asked = false;
        sink.push(meta, event);
        Ok(())
    }
}

/// One record of a resync after its `rsbegin`: an order, a position or its end.
fn resync_event(
    r: &Record<'_>,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<(VenueMeta, ExecEvent), DecodeError> {
    let event = match r.kind {
        "rsorder" => ExecEvent::ResyncOrder(decode::snapshot(r, scope, specs)?),
        "rspos" => decode::position(r, specs)?,
        "rsend" => ExecEvent::ResyncEnd,
        "rsbegin" => return Err(Malformed("resync begun twice")),
        _ => return Err(Malformed("kind")),
    };
    Ok((r.meta(false)?, event))
}

/// A resync over REST's response body, decoded whole: `rsbegin` echoing `watermark` first,
/// orders and positions, `rsend` last, one record per line.
fn rest_body(
    body: &[u8],
    watermark: WallNs,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<Vec<(VenueMeta, ExecEvent)>, DecodeError> {
    let text = core::str::from_utf8(body).map_err(|_| Malformed("body"))?;
    let mut lines = text.lines().map(Record::parse);
    let begin = lines.next().transpose()?;
    let begin = begin.filter(|r| r.kind == "rsbegin");
    let begin = begin.ok_or(Malformed("a resync begins with rsbegin"))?;
    if WallNs(begin.num("wm")?) != watermark {
        return Err(Malformed("resync for another request"));
    }
    let mut events = vec![(begin.meta(false)?, ExecEvent::ResyncBegin { watermark })];
    for r in lines {
        if matches!(events.last(), Some((_, ExecEvent::ResyncEnd))) {
            return Err(Malformed("a record after rsend"));
        }
        events.push(resync_event(&r?, scope, specs)?);
    }
    match events.last() {
        Some((_, ExecEvent::ResyncEnd)) => Ok(events),
        _ => Err(Malformed("a resync ends with rsend")),
    }
}

/// The spans of every toy token in a text frame: the value of each `token` field of each of
/// its records, wherever it stands, so a frame the codec would refuse is still journaled
/// without one. A binary frame and an HTTP response carry none of the toy's.
pub(super) fn token_spans(input: Inbound<'_>) -> InboundSpans {
    let Inbound::Frame(RawFrame::Text(text)) = input else {
        return InboundSpans::NONE;
    };
    let mut spans = Vec::new();
    let mut at = 0;
    for line in text.split('\n') {
        let mut fields = line.split('|');
        // The record's kind is not a field.
        at += fields.next().map_or(0, str::len) + 1;
        for field in fields {
            let value = field.strip_prefix(TOKEN_FIELD).filter(|v| !v.is_empty());
            let start = at + TOKEN_FIELD.len();
            spans.extend(value.and_then(|v| span(start..start + v.len())));
            at += field.len() + 1;
        }
    }
    InboundSpans::frame(spans)
}

/// `range` as a redaction span, or `None` past what a span can name.
fn span(range: Range<usize>) -> Option<Range<u32>> {
    Some(u32::try_from(range.start).ok()?..u32::try_from(range.end).ok()?)
}
