//! Paradex's JSON-RPC replies to the order methods decoded into one [`ExecEvent::Outcome`] per
//! item (BT-402; decisions 0005, 0014, 0054, 0069). [`ParadexReplies`] holds, for each request
//! [`ParadexEncoder`](super::ParadexEncoder) sent, what its reply must name; the order-entry
//! codec (FBC-xvf) records each request it sends ([`ParadexReplies::sent`]), hands it every
//! text frame ([`ParadexReplies::on_reply`]) and every deadline that passes
//! ([`ParadexReplies::on_rpc_timeout`]).
//!
//! A reply is `{"jsonrpc":"2.0","result":{..},"id":<rpc>}` or `{.., "error":{"code":..,
//! "message":..,"data":..},"id":<rpc>}` (docs.paradex.trade WebSocket "Error Handling"), the
//! `result` of each method as its page (`ws/web-socket-channels/<method>`) documents it:
//!
//! | Method | Result | Outcome per item |
//! | --- | --- | --- |
//! | `order.create` | `{"order":{..}}` (or a bare `{"id":..}`, as the Java library tolerates) | Accepted, provisional, under the order's `id` |
//! | `order.create_batch` | `{"results":[..]}` in request order, each an `order` or an `error` message | per item: Accepted, provisional; or `Unknown` for an error (a message, no code) |
//! | `order.modify` | `{"order":{..}}` | Accepted, provisional: the amend is final only on its order event (0054) |
//! | `order.cancel` | `{"order_id":..,"status":"QUEUED_FOR_CANCELLATION"}` | Accepted, provisional: queued is not done |
//! | `order.cancel_batch` | `{"results":[{"id","status"}..]}` in request order | QUEUED_FOR_CANCELLATION accepted, provisional; ALREADY_CLOSED and NOT_FOUND refused through [`REJECT_CODES`](super::REJECT_CODES) |
//! | `order.cancel_all` | `{"status":"ok"}` | Accepted, provisional |
//! | `order.cancel_on_disconnect` | `{"enabled":<bool>}` | Accepted, final, when it is the state asked for |
//!
//! Each acceptance but cancel-on-disconnect's is provisional: Paradex queues an order for its
//! risk check (0054's `AckModel::TwoPhase`), confirms an amend on the order event that reports
//! SUCCESS for MODIFY_ORDER (`AmendAck::ReplacedEvent`), and queues a cancel; the order events
//! settle each. The reply to cancel-on-disconnect states the connection's new state, so it is
//! final (0058).
//!
//! An error with the request's id refuses the whole request (`item: None`) as the kind its
//! code maps to; a code [`REJECT_CODES`](super::REJECT_CODES) does not hold (an internal error,
//! or one no page documents) is `Unknown` instead, since the venue may have acted, and so are an
//! item status no page documents and a batch item's error, a message with no code. An error with no id (absent or null) answers no request: it
//! is an [`ExecEvent::UncorrelatedError`] keeping its code. A frame with an id no request
//! waits on (the auth frame's, a subscription's) or with neither id nor error (a channel
//! message) is not this tracker's ([`ReplyRead::NotOurs`]).
//!
//! A reply is read whole before anything is pushed, and a batch's item outcomes are pushed in
//! one call (0014 item 3). A reply that names another order or client id than the request's,
//! or that is not in its method's shape, is refused with nothing pushed: the request waits for
//! its deadline, whose timeout is `Unknown`. A batch reply with fewer results than items
//! answers the items it has and is `Unknown` for the rest, in the same call. A timeout is
//! `Unknown` for every item of a request no reply answered, and adds nothing to one whose every
//! item was already reported `Unknown`. Replies state no venue time or sequence of their own,
//! so their events carry [`VenueMeta::NONE`].

use std::collections::HashMap;
use std::fmt;

use fbc_core::{
    AckLevel, ChosenRef, ClientOrderId, DecodeError, DecodeScope, ExecEvent, ExecSink, ItemRef,
    OrderCaps, Reject, RejectKind, RpcId, SubmitOutcome, VenueCommand, VenueMeta, VenueOrderId,
};
use serde_json::Value;

use super::errors::reject_kind;
use super::exec_caps;

use DecodeError::Malformed;

/// What [`ParadexReplies::on_reply`] made of a text frame.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ReplyRead {
    /// The reply to a request this tracker holds, or an error frame with no id: its events
    /// were pushed.
    Decoded,
    /// Not this tracker's: a reply to a request it does not hold (the auth frame's, a
    /// subscription's) or a frame that is neither a reply nor an error. Nothing was pushed.
    NotOurs,
}

/// One request sent, with what its reply must name.
#[derive(Debug)]
enum Request {
    /// `order.create` of our client id.
    Create(ClientOrderId),
    /// `order.create_batch`, its items' client ids in order.
    CreateBatch(Vec<ClientOrderId>),
    /// `order.modify` of the order `vid`, ours as `cid` when the command says so.
    Modify {
        cid: Option<ClientOrderId>,
        vid: VenueOrderId,
    },
    /// `order.cancel` by venue id, or by client id (`vid` none).
    Cancel {
        cid: Option<ClientOrderId>,
        vid: Option<VenueOrderId>,
    },
    /// `order.cancel_batch`, each item's client id (when the command names one) and venue id.
    CancelBatch(Vec<(Option<ClientOrderId>, VenueOrderId)>),
    /// `order.cancel_all`, for a market or the account.
    CancelAll,
    /// `order.cancel_on_disconnect`, turning it on or off.
    CancelOnDisconnect(bool),
}

/// A request's place in the tracker.
#[derive(Debug)]
enum Slot {
    /// Sent and unanswered.
    Waiting(Request),
    /// Answered with every item `Unknown`: no answer cleared its deadline, and its timeout
    /// adds nothing.
    ReportedUnknown,
}

/// The order requests sent on one order-entry connection and not yet answered, by JSON-RPC id
/// (module documentation). Its `Debug` shows how many it holds.
pub struct ParadexReplies {
    order: OrderCaps,
    slots: HashMap<RpcId, Slot>,
}

impl fmt::Debug for ParadexReplies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParadexReplies")
            .field("requests", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl Default for ParadexReplies {
    fn default() -> ParadexReplies {
        ParadexReplies::new()
    }
}

impl ParadexReplies {
    /// A tracker holding no request, choosing references as the caps [`exec_caps`] declares,
    /// as the encoder does.
    pub fn new() -> ParadexReplies {
        ParadexReplies {
            order: exec_caps().order,
            slots: HashMap::new(),
        }
    }

    /// Request `rpc`, `cmd`, was sent ([`ParadexEncoder::encode`](super::ParadexEncoder::encode)
    /// returned `Ok`): it waits for its reply. A command the encoder refuses (a query, a fee
    /// query, a dead-man refresh, an amend or cancel without the reference it needs) is never
    /// sent, and is not held.
    pub fn sent(&mut self, rpc: RpcId, cmd: &VenueCommand) {
        let request = match cmd {
            VenueCommand::Place(o) => Request::Create(o.cid),
            VenueCommand::PlaceBatch(orders) => {
                Request::CreateBatch(orders.iter().map(|o| o.cid).collect())
            }
            VenueCommand::Amend(a) => {
                let caps = self.order.amend;
                match caps.as_ref().and_then(|caps| a.reference(caps)) {
                    Some(ChosenRef::Venue(vid)) => Request::Modify {
                        cid: a.target.client(),
                        vid: vid.clone(),
                    },
                    _ => return,
                }
            }
            VenueCommand::Cancel(c) => match c.reference(self.order.cancel_refs) {
                Some(ChosenRef::Venue(vid)) => Request::Cancel {
                    cid: c.target.client(),
                    vid: Some(vid.clone()),
                },
                Some(ChosenRef::Client(cid)) => Request::Cancel {
                    cid: Some(cid),
                    vid: None,
                },
                _ => return,
            },
            VenueCommand::CancelMany(cancels) => {
                let items = cancels
                    .iter()
                    .map(|c| Some((c.target.client(), c.target.venue()?.clone())))
                    .collect::<Option<Vec<_>>>();
                match items {
                    Some(items) => Request::CancelBatch(items),
                    None => return,
                }
            }
            VenueCommand::CancelAll(_) => Request::CancelAll,
            VenueCommand::ArmCancelOnDisconnect(on) => Request::CancelOnDisconnect(*on),
            VenueCommand::RefreshDeadMan | VenueCommand::FeeQuery | VenueCommand::Query(_) => {
                return;
            }
        };
        self.slots.insert(rpc, Slot::Waiting(request));
    }

    /// Reads one text frame: the reply to a request this tracker holds, decoded into its item
    /// outcomes and pushed in one call; an error frame with no id, pushed as an
    /// [`ExecEvent::UncorrelatedError`]; or not this tracker's ([`ReplyRead::NotOurs`]).
    /// `Err` means nothing was pushed and the request still waits.
    pub fn on_reply(
        &mut self,
        text: &str,
        scope: &DecodeScope<'_>,
        sink: &mut dyn ExecSink,
    ) -> Result<ReplyRead, DecodeError> {
        let reply: Value =
            serde_json::from_str(text).map_err(|_| Malformed("text frame is not JSON"))?;
        let (result, error) = (reply.get("result"), reply.get("error"));
        let id = reply.get("id").filter(|id| !id.is_null());
        let Some(id) = id else {
            let Some(error) = error else {
                return Ok(ReplyRead::NotOurs);
            };
            let (code, raw) = error_of(error)?;
            let kind = reject_kind(&code).unwrap_or(RejectKind::Other);
            let reject = Reject {
                kind,
                venue_code: Some(code.into()),
                raw: raw.into(),
            };
            sink.push(VenueMeta::NONE, ExecEvent::UncorrelatedError(reject));
            return Ok(ReplyRead::Decoded);
        };
        let Some(rpc) = id.as_u64().map(RpcId) else {
            return Ok(ReplyRead::NotOurs);
        };
        let Some(Slot::Waiting(request)) = self.slots.get(&rpc) else {
            return Ok(ReplyRead::NotOurs);
        };
        let outcomes = match (result, error) {
            (Some(result), None) if !result.is_null() => answer(request, result, scope)?,
            (None, Some(error)) => {
                let (code, raw) = error_of(error)?;
                let outcome = match reject_kind(&code) {
                    Some(kind) => SubmitOutcome::Rejected(Reject {
                        kind,
                        venue_code: Some(code.into()),
                        raw: raw.into(),
                    }),
                    None => SubmitOutcome::Unknown,
                };
                vec![(None, outcome)]
            }
            _ => return Err(Malformed("a reply is a result or an error")),
        };
        let unanswered = outcomes
            .iter()
            .all(|(_, outcome)| *outcome == SubmitOutcome::Unknown);
        if unanswered {
            self.slots.insert(rpc, Slot::ReportedUnknown);
        } else {
            self.slots.remove(&rpc);
        }
        for (item, outcome) in outcomes {
            let event = ExecEvent::Outcome { rpc, item, outcome };
            sink.push(VenueMeta::NONE, event);
        }
        Ok(ReplyRead::Decoded)
    }

    /// Request `rpc`'s deadline passed with no answer: `Unknown` for every item, unless every
    /// item was already reported `Unknown` from its reply, which this adds nothing to.
    pub fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        if let Some(Slot::ReportedUnknown) = self.slots.remove(&rpc) {
            return;
        }
        let (item, outcome) = (None, SubmitOutcome::Unknown);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }
}

/// An item and its outcome; `None` for the whole request.
type Answer = (Option<ItemRef>, SubmitOutcome);

const PROVISIONAL: SubmitOutcome = SubmitOutcome::Accepted {
    ack: AckLevel::Provisional,
};

/// A JSON-RPC `error` object's code, as text, and its message.
fn error_of(error: &Value) -> Result<(String, &str), DecodeError> {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .ok_or(Malformed("error code"))?;
    let message = error.get("message").and_then(Value::as_str).unwrap_or("");
    Ok((code.to_string(), message))
}

/// The item outcomes `result` states for `request`.
fn answer(
    request: &Request,
    result: &Value,
    scope: &DecodeScope<'_>,
) -> Result<Vec<Answer>, DecodeError> {
    let one = |cid, vid, outcome| vec![(Some(ItemRef { idx: 0, cid, vid }), outcome)];
    Ok(match request {
        Request::Create(cid) => {
            let vid = created(result, *cid, scope)?;
            one(Some(*cid), Some(vid), PROVISIONAL)
        }
        Request::CreateBatch(cids) => {
            let results = results(result, cids.len())?;
            let mut out = Vec::with_capacity(cids.len());
            for (idx, cid) in (0u16..).zip(cids) {
                let (vid, outcome) = match results.get(usize::from(idx)) {
                    Some(item) => match (item.get("order"), item.get("error")) {
                        (Some(_), None) => (Some(created(item, *cid, scope)?), PROVISIONAL),
                        // A message with no code: no page says the venue left the item
                        // undone, so it is Unknown, as an undocumented code is (0066).
                        (None, Some(error)) => {
                            error.as_str().ok_or(Malformed("item error"))?;
                            (None, SubmitOutcome::Unknown)
                        }
                        _ => return Err(Malformed("a result is an order or an error")),
                    },
                    None => (None, SubmitOutcome::Unknown),
                };
                let item = ItemRef {
                    idx,
                    cid: Some(*cid),
                    vid,
                };
                out.push((Some(item), outcome));
            }
            out
        }
        Request::Modify { cid, vid } => {
            let order = result.get("order").unwrap_or(result);
            let named = order_id(order, scope)?;
            if named != *vid {
                return Err(Malformed("the reply names another order"));
            }
            if let Some(cid) = cid {
                same_client(order, *cid, scope)?;
            }
            one(*cid, Some(named), PROVISIONAL)
        }
        Request::Cancel { cid, vid } => {
            let status = str_field(result, "status")?;
            let named = match result.get("order_id") {
                Some(_) => Some(id_field(result, "order_id", scope)?),
                None => None,
            };
            let vid = match (vid, named) {
                (Some(sent), Some(named)) if *sent != named => {
                    return Err(Malformed("the reply names another order"));
                }
                (Some(sent), _) => Some(sent.clone()),
                (None, named) => named,
            };
            let outcome = match status {
                "QUEUED_FOR_CANCELLATION" => PROVISIONAL,
                _ => SubmitOutcome::Unknown,
            };
            one(*cid, vid, outcome)
        }
        Request::CancelBatch(items) => {
            let results = results(result, items.len())?;
            let mut out = Vec::with_capacity(items.len());
            for (idx, (cid, vid)) in (0u16..).zip(items) {
                let outcome = match results.get(usize::from(idx)) {
                    Some(item) => {
                        if id_field(item, "id", scope)? != *vid {
                            return Err(Malformed("the reply names another order"));
                        }
                        cancel_status(str_field(item, "status")?)
                    }
                    None => SubmitOutcome::Unknown,
                };
                let item = ItemRef {
                    idx,
                    cid: *cid,
                    vid: Some(vid.clone()),
                };
                out.push((Some(item), outcome));
            }
            out
        }
        Request::CancelAll => {
            let outcome = match str_field(result, "status")? {
                "ok" => PROVISIONAL,
                _ => SubmitOutcome::Unknown,
            };
            one(None, None, outcome)
        }
        Request::CancelOnDisconnect(asked) => {
            let enabled = result.get("enabled").and_then(Value::as_bool);
            if enabled != Some(*asked) {
                return Err(Malformed("cancel-on-disconnect is not as asked"));
            }
            let fin = SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            };
            one(None, None, fin)
        }
    })
}

/// A batch's `results`, at most `items` long.
fn results(result: &Value, items: usize) -> Result<&Vec<Value>, DecodeError> {
    let results = result
        .get("results")
        .and_then(Value::as_array)
        .ok_or(Malformed("batch results"))?;
    if results.len() > items {
        return Err(Malformed("more results than items"));
    }
    Ok(results)
}

/// A cancel_batch item's status as its outcome: QUEUED_FOR_CANCELLATION accepted, provisional;
/// a refusal [`REJECT_CODES`](super::REJECT_CODES) holds; `Unknown` for any other.
fn cancel_status(status: &str) -> SubmitOutcome {
    if status == "QUEUED_FOR_CANCELLATION" {
        return PROVISIONAL;
    }
    match reject_kind(status) {
        Some(kind) => SubmitOutcome::Rejected(Reject {
            kind,
            venue_code: Some(status.into()),
            raw: status.into(),
        }),
        None => SubmitOutcome::Unknown,
    }
}

/// The venue id of the order a create reply (or a batch item) states for our `cid`: its
/// `order`, or the reply itself when it is a bare order or id.
fn created(
    reply: &Value,
    cid: ClientOrderId,
    scope: &DecodeScope<'_>,
) -> Result<VenueOrderId, DecodeError> {
    let order = reply.get("order").unwrap_or(reply);
    let vid = order_id(order, scope)?;
    same_client(order, cid, scope)?;
    Ok(vid)
}

/// An order object's `id`.
fn order_id(order: &Value, scope: &DecodeScope<'_>) -> Result<VenueOrderId, DecodeError> {
    id_field(order, "id", scope)
}

/// Refuses an order whose `client_id`, when it states one, is not `cid`.
fn same_client(
    order: &Value,
    cid: ClientOrderId,
    scope: &DecodeScope<'_>,
) -> Result<(), DecodeError> {
    match order.get("client_id").and_then(Value::as_str) {
        None | Some("") => Ok(()),
        Some(wire) if scope.client_order_id(wire) == fbc_core::CidMatch::Ours(cid) => Ok(()),
        Some(_) => Err(Malformed("the reply names another client id")),
    }
}

/// The venue id in string field `name`.
fn id_field(
    object: &Value,
    name: &'static str,
    scope: &DecodeScope<'_>,
) -> Result<VenueOrderId, DecodeError> {
    let wire = str_field(object, name)?;
    scope.venue_order_id(wire).map_err(DecodeError::IdRefused)
}

/// String field `name`.
fn str_field<'a>(object: &'a Value, name: &'static str) -> Result<&'a str, DecodeError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or(Malformed(name))
}
