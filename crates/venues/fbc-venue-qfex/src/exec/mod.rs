//! Offline QFEX private entry point. The factory can build and inspect this codec, but
//! `caps.exec` stays `None` and `Readiness::Record` until order, fill and resync truth have
//! complete conformance and owner-authorized provider evidence.

use fbc_core::{
    ConfigError, ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx,
    EncodeReceipt, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, HttpFailure, HttpResponse,
    HttpTag, Inbound, InboundSpans, NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, RpcId,
    Secret, Secrets, SpecTable, StreamId, TimerTag, TrafficClass, VenueCommand, VenueConfig,
    VenueError, VenueMeta, WireSlice,
};
use serde_json::Value;
use std::time::Duration;

use crate::auth::{self, AccountSelection};
use crate::valid_host_port;

const TRADE: StreamId = StreamId(0);
const RPC_TIMEOUT: &str = "qfex.rpc_timeout";

mod order;

pub const RPC_TIMEOUT_KEY: &str = RPC_TIMEOUT;

fn rpc_timeout(cfg: &VenueConfig) -> Result<Duration, ConfigError> {
    let raw = cfg
        .get(RPC_TIMEOUT)
        .ok_or(ConfigError::Missing(RPC_TIMEOUT))?;
    let (number, unit) = if let Some(n) = raw.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1000u64)
    } else {
        ("", 0)
    };
    let ms = number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(unit))
        .ok_or(ConfigError::Invalid {
            key: RPC_TIMEOUT,
            reason: "expected positive whole <n>ms or <n>s",
        })?;
    Ok(Duration::from_millis(ms))
}

fn trade_origin(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let raw = cfg
        .get(auth::TRADE_URL)
        .ok_or(ConfigError::Missing(auth::TRADE_URL))?;
    let origin = raw.strip_suffix('/').unwrap_or(raw);
    if !origin.strip_prefix("wss://").is_some_and(valid_host_port) {
        return Err(ConfigError::Invalid {
            key: auth::TRADE_URL,
            reason: "expected a WSS origin without path, query or fragment",
        });
    }
    Ok(origin)
}

pub fn plan(cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
    auth::account_key(cfg)?;
    let origin = trade_origin(cfg)?;
    let public = cfg
        .get(auth::PUBLIC_KEY)
        .ok_or(ConfigError::Missing(auth::PUBLIC_KEY))?;
    Ok(vec![ExecEndpoint {
        stream: TRADE,
        url: auth::opening_url(origin, public)?,
        handshake: Vec::new(),
    }])
}

pub fn codec(cfg: &VenueConfig, mut creds: Secrets) -> Result<QfexExec, VenueError> {
    auth::account_key(cfg)?;
    trade_origin(cfg)?;
    let rpc_timeout = rpc_timeout(cfg)?;
    let configured = cfg
        .get(auth::PUBLIC_KEY)
        .ok_or(ConfigError::Missing(auth::PUBLIC_KEY))?;
    let public = creds
        .take(auth::PUBLIC_KEY)
        .ok_or(ConfigError::Missing(auth::PUBLIC_KEY))?;
    auth::matching_public_key(configured, &public)?;
    let secret = creds
        .take(auth::SECRET_KEY)
        .ok_or(ConfigError::Missing(auth::SECRET_KEY))?;
    let subaccount = match auth::account_selection(cfg)? {
        AccountSelection::Primary => None,
        AccountSelection::Subaccount(id) => Some(id.to_owned()),
    };
    Ok(QfexExec {
        public,
        secret,
        subaccount,
        rpc_timeout,
        waiting_auth: false,
        authenticated: false,
    })
}

pub struct QfexExec {
    public: Secret,
    secret: Secret,
    subaccount: Option<String>,
    rpc_timeout: Duration,
    waiting_auth: bool,
    authenticated: bool,
}

impl QfexExec {
    fn selection(&self) -> AccountSelection<'_> {
        self.subaccount
            .as_deref()
            .map_or(AccountSelection::Primary, AccountSelection::Subaccount)
    }

    fn send_subscriptions(&self, fx: &mut Effects) {
        for channel in ["order_responses", "fills"] {
            let body =
                format!("{{\"type\":\"subscribe\",\"params\":{{\"channels\":[\"{channel}\"]}}}}");
            fx.push(Effect::Send {
                stream: TRADE,
                frame: WireSlice::plain(body.into_bytes()),
                rpc: None,
                class: TrafficClass::Safety,
                // One whole unit conservatively exceeds QFEX's currently documented 0.1.
                charge: RateCharge::one(OpKind::Subscribe, None),
            });
        }
    }

    fn reconnect(fx: &mut Effects) {
        fx.push(Effect::Reconnect {
            stream: TRADE,
            reason: "QFEX private state unverified",
        });
    }
}

impl ExecCodec for QfexExec {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        u16::from(matches!(call, CtxCall::Open(TRADE))) * 2
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.waiting_auth = false;
        self.authenticated = false;
        if stream != TRADE {
            return;
        }
        match auth::frame(&self.public, &self.secret, ctx, self.selection()) {
            Ok(frame) => {
                self.waiting_auth = true;
                fx.push(Effect::Send {
                    stream: TRADE,
                    frame,
                    rpc: None,
                    class: TrafficClass::Safety,
                    charge: RateCharge::one(OpKind::Control, None),
                });
            }
            Err(_) => Self::reconnect(fx),
        }
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        if !self.authenticated {
            return Err(NotSentReason::Disconnected);
        }
        let (frame, charge) = order::frame(cmd, specs)?;
        fx.push(Effect::Send {
            stream: TRADE,
            frame: WireSlice::plain(frame),
            rpc: Some(fbc_core::RpcCall {
                id: rpc,
                timeout: self.rpc_timeout,
            }),
            class: cmd.traffic_class(),
            charge,
        });
        Ok(EncodeReceipt::new())
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        frame: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if stream != TRADE {
            self.authenticated = false;
            Self::reconnect(fx);
            return Err(DecodeError::Malformed("unexpected QFEX trade stream"));
        }
        let RawFrame::Text(text) = frame else {
            self.authenticated = false;
            Self::reconnect(fx);
            return Err(DecodeError::Malformed("QFEX private frame is not text"));
        };
        let value: Value = serde_json::from_str(text).map_err(|_| {
            self.authenticated = false;
            Self::reconnect(fx);
            DecodeError::Malformed("invalid QFEX private JSON")
        })?;
        if self.waiting_auth {
            if value.get("type").and_then(Value::as_str) == Some("auth")
                && value.get("result").and_then(Value::as_str) == Some("success")
                && value.get("err").is_none()
            {
                self.waiting_auth = false;
                self.authenticated = true;
                sink.push(
                    VenueMeta::NONE,
                    ExecEvent::Conn {
                        stream: TRADE,
                        state: ConnState::Authenticated,
                    },
                );
                self.send_subscriptions(fx);
            } else {
                self.waiting_auth = false;
                Self::reconnect(fx);
            }
            return Ok(());
        }
        if !self.authenticated {
            Self::reconnect(fx);
            return Ok(());
        }
        if value.get("subscribed").and_then(Value::as_str).is_some() {
            return Ok(());
        }
        // No unsolicited order or fill is normalized until its identifiers, fee sign and
        // cumulative quantities have been established by private conformance fixtures.
        // A DecodeError alone only increments the runtime's error counter, so explicitly
        // end this authenticated epoch rather than leave it running on unknown private truth.
        self.authenticated = false;
        Self::reconnect(fx);
        Ok(())
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "unexpected QFEX private HTTP response",
        ))
    }

    fn on_timer(&mut self, _tag: TimerTag, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Outcome {
                rpc,
                item: None,
                outcome: fbc_core::SubmitOutcome::Unknown,
            },
        );
    }

    fn resync(&mut self, _ctx: &EncodeCtx, fx: &mut Effects) {
        // A partial or untrustworthy snapshot must never make the session ready.
        Self::reconnect(fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        let Inbound::Frame(frame) = input else {
            return InboundSpans::NONE;
        };
        auth::inbound_spans(frame.bytes(), &self.public, self.subaccount.as_deref())
    }
}
