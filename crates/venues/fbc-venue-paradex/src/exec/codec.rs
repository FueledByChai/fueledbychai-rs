//! Paradex's order-entry codec (FBC-xvf, decision 0071): one [`ExecCodec`] on one WebSocket
//! connection, which carries both the order methods and the account's private channels. It is
//! the read-only codec ([`ReadOnlyExec`], decision 0061) with commands added:
//!
//! - **The connection.** `on_open`, the login, the auth frame, the private channels, the token
//!   refresh and the decoding of the private events are the read-only codec's, unchanged. Its
//!   JSON-RPC ids are counted from [`CONTROL_IDS`], so a reply to the auth frame or a
//!   subscription can never be taken for the reply to an order request, whose id is the
//!   request's [`RpcId`].
//! - **Commands.** Every command but the order query is encoded by [`ParadexEncoder`] (FBC-xe1)
//!   as one JSON-RPC frame on the authenticated connection, cancel-on-disconnect's arm included
//!   (the runtime sends it, FBC-w19), and recorded with [`ParadexReplies`] (FBC-0l9), which
//!   decodes its reply into one outcome per item and reports `Unknown` at its deadline for what
//!   went unanswered. Order entry is WebSocket only: a frame command while no connection is
//!   authenticated is `NotSent(Disconnected)`, as the runtime reports it, with no REST
//!   fallback; one whose rpc is not below [`CONTROL_IDS`] is `NotSent(Unencodable)`, since its
//!   reply could not be told from the codec's own.
//! - **The query.** The Unknown ladder's order query is FBC-0sc's REST read of
//!   `GET /orders-history` by our client id ([`query_request`]), carrying the current token in a
//!   redacted header and nowhere else, under a tag of its own; its answer decodes into the
//!   `QueryResult` for its rpc ([`decode_order_query`]). As `on_http`'s contract asks, a read
//!   that was never sent is `NotSent(Disconnected)` for its rpc, and one that failed afterwards,
//!   was answered with an error status or does not decode is `Unknown`. With no token yet it is
//!   `NotSent(Disconnected)`.
//! - **The resync** is the read-only codec's: `GET /orders` and `GET /positions` with the
//!   current token in a redacted header, decoded whole at the watermark `ctx.wall`.
//! - **The token** in every REST read is the one the latest login gave, read when the request is
//!   built, so a read built after a refresh carries the new token; the refresh timer follows the
//!   configured interval alone, never the token's bytes (0028).
//!
//! Paradex signs a timestamp, never a nonce: no call asks for one.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use fbc_core::{
    CtxCall, DecodeError, DecodeScope, Effects, EncodeCtx, EncodeReceipt, ExecCodec, ExecEvent,
    ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, NotSentReason,
    OrderSigner, PathStamps, QueryOrder, RawFrame, RpcId, Secrets, SpecTable, StreamId,
    SubmitOutcome, TimerTag, VenueCommand, VenueConfig, VenueError, VenueMeta,
};
use serde_json::Value;

use super::{
    ParadexEncoder, ParadexReplies, ReadOnlyExec, ReplyRead, decode_order_query, query_request,
};

/// The first JSON-RPC id of the codec's own frames (auth and subscribe), 2^52: every id from it
/// up is the codec's, every id below it an order request's [`RpcId`]. The runtime counts rpcs
/// up from one per account, so they stay below it; JSON numbers hold every integer below 2^53
/// exactly, so the venue echoes either kind as sent.
pub const CONTROL_IDS: u64 = 1 << 52;

/// Paradex's order-entry codec (module documentation). Its `Debug` shows no account, key,
/// signature or token.
pub struct ParadexExec {
    session: ReadOnlyExec,
    encoder: ParadexEncoder,
    replies: ParadexReplies,
    /// The stream the encoder writes to: commands go out only once it is authenticated.
    stream: StreamId,
    /// The order queries sent and not yet answered, by their read's tag.
    queries: BTreeMap<HttpTag, (RpcId, QueryOrder)>,
}

impl fmt::Debug for ParadexExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParadexExec")
            .field("session", &self.session)
            .field("encoder", &self.encoder)
            .field("replies", &self.replies)
            .field("queries", &self.queries.len())
            .finish_non_exhaustive()
    }
}

impl ParadexExec {
    /// The codec for the account `creds` hold, under `cfg` (src/auth's keys: the REST base,
    /// chain id, signature lifetime, refresh interval and request timeout), signing orders with
    /// `signer` and writing them to `stream`, each awaiting its reply for `rpc_timeout` (the
    /// consumer's configuration). Refused naming a missing or invalid key, never a value.
    pub fn new(
        cfg: &VenueConfig,
        creds: Secrets,
        signer: Box<dyn OrderSigner>,
        stream: StreamId,
        rpc_timeout: Duration,
    ) -> Result<ParadexExec, VenueError> {
        Ok(ParadexExec {
            session: ReadOnlyExec::with_first_id(cfg, creds, CONTROL_IDS)?,
            encoder: ParadexEncoder::new(signer, stream, rpc_timeout),
            replies: ParadexReplies::new(),
            stream,
            queries: BTreeMap::new(),
        })
    }

    /// The order query `query` as request `rpc`: the REST read under a new tag, the current
    /// token in its redacted header.
    fn query(
        &mut self,
        query: &QueryOrder,
        rpc: RpcId,
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        if specs.get(query.inst).is_none() {
            return Err(NotSentReason::Unencodable);
        }
        let header = self
            .session
            .token()
            .map(|token| token.header())
            .ok_or(NotSentReason::Disconnected)?;
        let tag = self.session.read_tag();
        let (base, timeout) = self.session.rest();
        for effect in query_request(query, rpc, base, &[header], timeout, tag)?.take() {
            fx.push(effect);
        }
        self.queries.insert(tag, (rpc, query.clone()));
        Ok(EncodeReceipt::new())
    }

    /// The answer to query `rpc`: its `QueryResult`, or `NotSent`/`Unknown` for a read that
    /// gave none (module documentation).
    fn on_query_answer(
        rpc: RpcId,
        query: &QueryOrder,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
    ) {
        let outcome = match resp {
            Ok(resp) if (200..300).contains(&resp.status) => {
                match decode_order_query(rpc, query, resp.body, scope, specs) {
                    Ok(answer) => return answer.push_into(sink),
                    Err(_) => SubmitOutcome::Unknown,
                }
            }
            Err(HttpFailure::NotSent) => SubmitOutcome::NotSent(NotSentReason::Disconnected),
            Ok(_) | Err(HttpFailure::TimedOut | HttpFailure::Lost) => SubmitOutcome::Unknown,
        };
        let event = ExecEvent::Outcome {
            rpc,
            item: None,
            outcome,
        };
        sink.push(VenueMeta::NONE, event);
    }
}

impl ExecCodec for ParadexExec {
    /// None: Paradex signs a timestamp, never a nonce.
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// The read-only codec's: the login, or the auth frame with a token already held. The
    /// queries of an earlier connection are dropped, as its resync is: their answers come back
    /// only to the epoch that asked (0027).
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.queries.clear();
        self.session.on_open(stream, ctx, fx);
    }

    /// The order query as its REST read; every other command as one frame on the
    /// authenticated connection, awaiting its reply (module documentation).
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        if let VenueCommand::Query(query) = cmd {
            return self.query(query, rpc, specs, fx);
        }
        if self.session.authenticated() != Some(self.stream) {
            return Err(NotSentReason::Disconnected);
        }
        if rpc.0 >= CONTROL_IDS {
            return Err(NotSentReason::Unencodable);
        }
        let receipt = self.encoder.encode(cmd, rpc, specs, ctx, t, fx)?;
        self.replies.sent(rpc, cmd);
        Ok(receipt)
    }

    /// A binary frame is a private event (the read-only codec's); a text frame is the reply
    /// to an order request, an error with no id, or, from [`CONTROL_IDS`] up, the reply to the
    /// codec's own auth or subscribe frame.
    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let RawFrame::Text(text) = f else {
            return self.session.on_frame(stream, f, scope, specs, sink, fx);
        };
        let reply: Value = serde_json::from_str(text)
            .map_err(|_| DecodeError::Malformed("text frame is not JSON"))?;
        let id = reply.get("id").and_then(Value::as_u64);
        if id.is_some_and(|id| id >= CONTROL_IDS) {
            return self.session.on_text(text, sink, fx);
        }
        match self.replies.on_reply(text, scope, sink)? {
            ReplyRead::Decoded => Ok(()),
            ReplyRead::NotOurs => Err(DecodeError::Malformed("a reply to no request sent")),
        }
    }

    /// A query's answer, a resync read's or a login's.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        match self.queries.remove(&tag) {
            Some((rpc, query)) => {
                ParadexExec::on_query_answer(rpc, &query, resp, scope, specs, sink);
                Ok(())
            }
            None => self.session.on_http(tag, resp, scope, specs, sink, fx),
        }
    }

    /// The refresh timer: the next login (the read-only codec's).
    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.session.on_timer(tag, ctx, fx);
    }

    /// `Unknown` for every item of request `rpc` no reply answered ([`ParadexReplies`]).
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.queries.retain(|_, (sent, _)| *sent != rpc);
        self.replies.on_rpc_timeout(rpc, sink);
    }

    /// The read-only codec's REST resync, with the current token.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.session.resync(ctx, fx);
    }

    /// The read-only codec's: a login answer's token, nothing else.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.session.redact_inbound(input)
    }
}
