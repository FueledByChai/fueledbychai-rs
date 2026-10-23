use fbc_core::{
    AccountKey, ConnState, CtxCall, DecodeError, Effect, Effects, EncodeCtx, ExecEvent, ExecSink,
    HttpFailure, HttpResponse, HttpTag, Inbound, MonoNs, NonceBlock, NotSentReason, PathStamps,
    RawFrame, Readiness, RpcId, Secret, Secrets, SpecTable, StreamId, TimerTag, VenueCommand,
    VenueConfig, VenueFactory, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use serde_json::Value;

const PUBLIC: &str = "qfex_pub_synthetic_1";
const SUBACCOUNT: &str = "11111111-1111-1111-1111-111111111111";

fn config(account: &str, key: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert("qfex.trade_url", "wss://trade.qfex.com");
    cfg.insert("qfex.public_key", PUBLIC);
    cfg.insert("qfex.account_id", account);
    cfg.insert("qfex.account_key", key);
    cfg.insert("qfex.rpc_timeout", "2500ms");
    cfg
}

fn credentials(public: &str) -> Secrets {
    let mut creds = Secrets::new();
    creds.insert("qfex.public_key", Secret::new(public.to_owned()));
    creds.insert(
        "qfex.secret_key",
        Secret::new("synthetic-qfex-secret".to_owned()),
    );
    creds
}

fn context() -> EncodeCtx {
    context_with_nonces(0xc0fe, 0xbabe)
}

fn context_with_nonces(first: u64, second: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_545_414_000_000_000),
        mono: MonoNs(0),
        nonces: NonceBlock::new(vec![first, second]),
    }
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

#[test]
fn factory_keeps_record_only_and_refuses_implicit_or_mismatched_accounts() {
    let cfg = config("primary", "4");
    assert_eq!(
        Qfex.caps(&cfg).unwrap().readiness_ceiling,
        Readiness::Record
    );
    assert!(Qfex.caps(&cfg).unwrap().exec.is_none());
    assert_eq!(
        Qfex.exec_account_key(&cfg).unwrap(),
        Some(AccountKey::new(4))
    );
    let endpoint = Qfex.plan_exec(&cfg).unwrap().remove(0);
    assert!(endpoint.url.as_str().contains(PUBLIC));
    assert!(!format!("{:?}", endpoint.url).contains(PUBLIC));
    assert_eq!(endpoint.url.redactions().len(), 1);
    assert!(
        Qfex.exec_codec(&cfg, credentials("wrong-key"))
            .unwrap()
            .is_err()
    );
    let mut missing = cfg.clone();
    missing.insert("qfex.account_id", "");
    assert!(Qfex.plan_exec(&missing).is_err());
    assert!(
        Qfex.exec_codec(&missing, credentials(PUBLIC))
            .unwrap()
            .is_err()
    );
    let mut bad_url = cfg.clone();
    bad_url.insert("qfex.trade_url", "ws://trade.qfex.com");
    assert!(Qfex.plan_exec(&bad_url).is_err());
    bad_url.insert("qfex.trade_url", "wss://[::1]suffix");
    assert!(Qfex.plan_exec(&bad_url).is_err());
    bad_url.insert("qfex.trade_url", "wss://[::1");
    assert!(Qfex.plan_exec(&bad_url).is_err());
    bad_url.insert("qfex.trade_url", "wss://[::1]");
    assert!(Qfex.plan_exec(&bad_url).is_ok());
    let mut bad_key = cfg.clone();
    bad_key.insert("qfex.public_key", "bad/key");
    assert!(Qfex.plan_exec(&bad_key).is_err());
    let mut bad_account_key = cfg.clone();
    bad_account_key.insert("qfex.account_key", "not-decimal");
    assert!(Qfex.exec_account_key(&bad_account_key).is_err());
}

#[test]
fn selected_subaccount_is_signed_redacted_and_reauthenticated_on_reconnect() {
    let cfg = config(SUBACCOUNT, "9");
    assert_eq!(
        Qfex.exec_account_key(&cfg).unwrap(),
        Some(AccountKey::new(9))
    );
    let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
    assert_eq!(codec.nonces_for(CtxCall::Open(StreamId(0))), 2);
    let mut effects = Effects::new();
    codec.on_open(StreamId(0), &context(), &mut effects);
    let [Effect::Send { frame, .. }] = effects.as_slice() else {
        panic!("expected one auth frame");
    };
    let body: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(body["params"]["account_id"], SUBACCOUNT);
    assert_eq!(frame.redactions().len(), 3);
    assert!(!format!("{frame:?}").contains(SUBACCOUNT));
    assert!(!format!("{frame:?}").contains(PUBLIC));

    let mut sink = Sink::default();
    effects.take();
    let caps = Qfex.caps(&cfg).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"authenticated":true}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    });
    result.unwrap();
    assert_eq!(
        sink.0,
        [ExecEvent::Conn {
            stream: StreamId(0),
            state: ConnState::Authenticated
        }]
    );
    assert_eq!(effects.len(), 2);
    for effect in effects.as_slice() {
        assert!(matches!(effect, Effect::Send { .. }));
    }

    effects.take();
    codec.resync(&context(), &mut effects);
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    effects.take();
    codec.on_open(
        StreamId(0),
        &context_with_nonces(0x1234, 0x5678),
        &mut effects,
    );
    let [
        Effect::Send {
            frame: next_auth, ..
        },
    ] = effects.as_slice()
    else {
        panic!("expected a fresh auth frame");
    };
    let next: Value = serde_json::from_slice(next_auth.bytes()).unwrap();
    assert_ne!(
        body["params"]["hmac"]["nonce"],
        next["params"]["hmac"]["nonce"]
    );
    let echoed = format!("{{\"signature\":\"synthetic\",\"account_id\":\"{SUBACCOUNT}\"}}");
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(&echoed)));
    assert_eq!(spans.body().len(), 1);
    assert_eq!(spans.body()[0], 0..echoed.len() as u32);
}

#[test]
fn primary_auth_omits_subaccount_and_private_events_stay_unsettled() {
    let cfg = config("primary", "4");
    let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
    let mut effects = Effects::new();
    codec.on_open(StreamId(0), &context(), &mut effects);
    let [Effect::Send { frame, .. }] = effects.as_slice() else {
        panic!("expected auth frame");
    };
    let body: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert!(body["params"].get("account_id").is_none());
    effects.take();
    let caps = Qfex.caps(&cfg).unwrap();
    let mut sink = Sink::default();
    dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    })
    .unwrap();
    effects.take();
    let decoded = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"order_response":{"status":"ACK"}}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    });
    assert_eq!(decoded, Ok(()));
    assert_eq!(sink.0.len(), 1, "no guessed order outcome or fill");
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
}

#[test]
fn documented_fill_and_partial_resync_frames_never_emit_unproven_truth() {
    let cfg = config("primary", "4");
    let caps = Qfex.caps(&cfg).unwrap();
    for raw in [
        include_str!("../../../../fixtures/qfex/private/fill_response.json"),
        include_str!("../../../../fixtures/qfex/private/open_orders_page.json"),
        include_str!("../../../../fixtures/qfex/private/user_trades_page.json"),
        include_str!("../../../../fixtures/qfex/private/position_pulse.json"),
    ] {
        let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
        let mut effects = Effects::new();
        let mut sink = Sink::default();
        codec.on_open(StreamId(0), &context(), &mut effects);
        effects.take();
        dispatch_market_data(&caps, |scope| {
            codec.on_frame(
                StreamId(0),
                RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
                scope,
                &SpecTable::new(),
                &mut sink,
                &mut effects,
            )
        })
        .unwrap();
        effects.take();
        dispatch_market_data(&caps, |scope| {
            codec.on_frame(
                StreamId(0),
                RawFrame::Text(raw),
                scope,
                &SpecTable::new(),
                &mut sink,
                &mut effects,
            )
        })
        .unwrap();
        assert_eq!(sink.0.len(), 1, "only auth may be emitted");
        assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    }
}

#[test]
fn malformed_documented_fill_reconnects_with_generic_diagnostic() {
    let cfg = config("primary", "4");
    let caps = Qfex.caps(&cfg).unwrap();
    let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
    let mut effects = Effects::new();
    let mut sink = Sink::default();
    codec.on_open(StreamId(0), &context(), &mut effects);
    effects.take();
    dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    })
    .unwrap();
    effects.take();
    let malformed = r#"{"fill_response":{"trade_id":"secret","fee":3}}"#;
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(malformed),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    });
    assert_eq!(
        result,
        Err(DecodeError::Malformed("invalid QFEX private state"))
    );
    assert!(!format!("{result:?}").contains("secret"));
    assert_eq!(sink.0.len(), 1);
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
}

#[test]
fn rejected_or_missing_auth_never_emits_authenticated() {
    for reply in [
        r#"{"type":"auth","result":"failure","err":"bad signature"}"#,
        r#"{"authenticated":false}"#,
        r#"{"authenticated":true,"order_response":{"status":"ACK"}}"#,
        r#"{"order_response":{"status":"ACK"}}"#,
    ] {
        let cfg = config("primary", "4");
        let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
        let mut effects = Effects::new();
        codec.on_open(StreamId(0), &context(), &mut effects);
        effects.take();
        let mut sink = Sink::default();
        let caps = Qfex.caps(&cfg).unwrap();
        dispatch_market_data(&caps, |scope| {
            codec.on_frame(
                StreamId(0),
                RawFrame::Text(reply),
                scope,
                &SpecTable::new(),
                &mut sink,
                &mut effects,
            )
        })
        .unwrap();
        assert!(sink.0.is_empty());
        assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    }
}

#[test]
fn missing_nonce_and_malformed_or_unexpected_private_frames_end_the_epoch() {
    let cfg = config("primary", "4");
    let caps = Qfex.caps(&cfg).unwrap();
    let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
    let mut effects = Effects::new();
    let missing_nonce = EncodeCtx {
        nonces: NonceBlock::EMPTY,
        ..context()
    };
    codec.on_open(StreamId(0), &missing_nonce, &mut effects);
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    effects.take();

    let mut sink = Sink::default();
    dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"subscribed":"fills"}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    })
    .unwrap();
    assert!(sink.0.is_empty());
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    effects.take();

    for (stream, frame, expected) in [
        (
            StreamId(1),
            RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
            "unexpected QFEX trade stream",
        ),
        (
            StreamId(0),
            RawFrame::Binary(b"private"),
            "QFEX private frame is not text",
        ),
        (
            StreamId(0),
            RawFrame::Text("not-json"),
            "invalid QFEX private JSON",
        ),
    ] {
        codec.on_open(StreamId(0), &context(), &mut effects);
        effects.take();
        let mut sink = Sink::default();
        let result = dispatch_market_data(&caps, |scope| {
            codec.on_frame(
                stream,
                frame,
                scope,
                &SpecTable::new(),
                &mut sink,
                &mut effects,
            )
        });
        assert_eq!(result, Err(DecodeError::Malformed(expected)));
        assert!(sink.0.is_empty());
        assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
        effects.take();
    }
}

#[test]
fn unverified_private_control_and_rpc_timeout_never_accept_an_order() {
    let cfg = config("primary", "4");
    let caps = Qfex.caps(&cfg).unwrap();
    let mut codec = Qfex.exec_codec(&cfg, credentials(PUBLIC)).unwrap().unwrap();
    assert_eq!(codec.nonces_for(CtxCall::Open(StreamId(1))), 0);
    let mut effects = Effects::new();
    codec.on_open(StreamId(1), &context(), &mut effects);
    assert!(effects.is_empty());
    codec.on_open(StreamId(0), &context(), &mut effects);
    effects.take();

    let mut sink = Sink::default();
    dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    })
    .unwrap();
    effects.take();
    dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"subscribed":"order_responses"}"#),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    })
    .unwrap();
    assert!(effects.is_empty());
    let result = codec.encode(
        &VenueCommand::ArmCancelOnDisconnect(true),
        RpcId(7),
        &SpecTable::new(),
        &context(),
        &mut PathStamps::off(),
        &mut effects,
    );
    assert_eq!(result, Err(NotSentReason::Unsupported));
    assert!(effects.is_empty());
    codec.on_timer(TimerTag(1), &context(), &mut effects);
    assert!(effects.is_empty());
    let unexpected_http = dispatch_market_data(&caps, |scope| {
        codec.on_http(
            HttpTag(1),
            Err(HttpFailure::NotSent),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut effects,
        )
    });
    assert_eq!(
        unexpected_http,
        Err(DecodeError::Malformed(
            "unexpected QFEX private HTTP response"
        ))
    );
    codec.on_rpc_timeout(RpcId(7), &mut sink);
    assert_eq!(sink.0.len(), 2);
    assert!(matches!(
        &sink.0[1],
        ExecEvent::Outcome {
            rpc: RpcId(7),
            outcome: fbc_core::SubmitOutcome::Unknown,
            ..
        }
    ));
    let no_sensitive_http = codec.redact_inbound(Inbound::Http(
        HttpTag(1),
        HttpResponse {
            status: 200,
            headers: &[],
            body: b"empty",
        },
    ));
    assert_eq!(no_sensitive_http, fbc_core::InboundSpans::NONE);
}
