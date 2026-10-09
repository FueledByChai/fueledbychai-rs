//! FBC-648o's done line: a conformance-toy variant that refuses every command but a query as
//! `NotSent(Disconnected)` until its session has authenticated, as Paradex's codec does, passes
//! `caps_truthful`, `commands_selfcontained` and `signing_golden` with the setup's
//! [`Bootstrap`] and fails each without it. Two variants: one authenticated by the toy's own
//! acknowledgement frame, one that also needs an HTTP login answered first (Paradex's shape).
//! Around them: the bootstrap reaches every codec the suite builds (the frame-reading checks
//! and the other encoding checks pass with it), and a bootstrap the codec refuses, or whose
//! HTTP response answers no request, fails naming `Setup.bootstrap` without showing its bytes.

mod toy_setup;

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use fbc_conformance::suite::{
    self, BootReply, Bootstrap, Failure, Setup, Subject, Verdict, caps_truthful,
    commands_selfcontained, signing_golden,
};
use fbc_conformance::toy::{TOY_TOKEN, ToyFactory};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConnState, CtxCall, DecodeError, DecodeScope, Effect,
    Effects, EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint, ExecEvent, ExecSink,
    FieldSpec, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse, HttpTag, Inbound,
    InboundSpans, InstrumentSpecDraft, MdCodec, NotSentReason, OpKind, PathStamps, RateCharge,
    RawFrame, RpcId, Secrets, SpecTable, StreamId, Subscription, SymbolError, TimerTag,
    TrafficClass, VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta,
    WireSlice, WireUrl,
};
use toy_setup::{FIXTURES, assumed};

/// The tag of the gated variant's login request.
const LOGIN: HttpTag = HttpTag(900);

// ---------------------------------------------------------------------------------------------
// The variants.
// ---------------------------------------------------------------------------------------------

/// The toy, its order-entry codec gated on authentication ([`GatedExec`]); with `login`, its
/// session also logs in over HTTP before it counts as authenticated.
struct Gated {
    login: bool,
}

/// The toy authenticated by its acknowledgement frame alone.
const BY_FRAME: Gated = Gated { login: false };
/// The toy that logs in over HTTP, then is authenticated by its acknowledgement frame.
const BY_LOGIN: Gated = Gated { login: true };

impl VenueFactory for Gated {
    fn id(&self) -> &'static str {
        "TOY-GATED"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        ToyFactory.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        ToyFactory.caps(cfg)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        ToyFactory.parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        ToyFactory.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        ToyFactory.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        ToyFactory.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        ToyFactory.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let inner = match ToyFactory.exec_codec(cfg, creds)? {
            Ok(inner) => inner,
            Err(e) => return Some(Err(e)),
        };
        let gated: Box<dyn ExecCodec> = Box::new(GatedExec {
            inner,
            login: self.login,
            logged_in: false,
            opened: None,
            acknowledged: false,
        });
        Some(Ok(gated))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The toy's codec, refusing every command but a query as `NotSent(Disconnected)` until its
/// session has authenticated on the stream it opened, as Paradex's does: the toy's
/// acknowledgement of its authentication frame, and, for a codec that logs in, a login answered
/// 200 before it. Opening the stream again starts over.
struct GatedExec {
    inner: Box<dyn ExecCodec>,
    login: bool,
    logged_in: bool,
    opened: Option<StreamId>,
    acknowledged: bool,
}

impl GatedExec {
    fn authenticated(&self) -> bool {
        self.opened.is_some() && self.acknowledged && (self.logged_in || !self.login)
    }
}

/// A sink passing events on, noting whether the stream the codec opened authenticated.
struct Watch<'a> {
    sink: &'a mut dyn ExecSink,
    opened: Option<StreamId>,
    acknowledged: &'a mut bool,
}

impl ExecSink for Watch<'_> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        if let ExecEvent::Conn {
            stream,
            state: ConnState::Authenticated,
        } = &ev
            && Some(*stream) == self.opened
        {
            *self.acknowledged = true;
        }
        self.sink.push(meta, ev);
    }
}

impl ExecCodec for GatedExec {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    /// The login first, for a codec that logs in; then the toy's authentication frame.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opened = Some(stream);
        self.acknowledged = false;
        self.logged_in = false;
        if self.login {
            fx.push(Effect::Http {
                tag: LOGIN,
                req: HttpRequest {
                    method: HttpMethod::Post,
                    url: WireUrl::plain("https://toy.invalid/login"),
                    headers: Vec::new(),
                    body: WireSlice::plain(Vec::new()),
                },
                rpc: None,
                timeout: Duration::from_secs(5),
                class: TrafficClass::Safety,
                charge: RateCharge::one(OpKind::Rest, None),
            });
        }
        self.inner.on_open(stream, ctx, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        if !matches!(cmd, VenueCommand::Query(_)) && !self.authenticated() {
            return Err(NotSentReason::Disconnected);
        }
        self.inner.encode(cmd, rpc, specs, ctx, t, fx)
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let mut watch = Watch {
            sink,
            opened: self.opened,
            acknowledged: &mut self.acknowledged,
        };
        self.inner.on_frame(stream, f, scope, specs, &mut watch, fx)
    }

    /// The login's answer: 200 logs in, anything else is refused; the toy's otherwise.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if self.login && tag == LOGIN {
            return match resp {
                Ok(resp) if resp.status == 200 => {
                    self.logged_in = true;
                    Ok(())
                }
                _ => Err(DecodeError::Malformed("login refused")),
            };
        }
        self.inner.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// The setups.
// ---------------------------------------------------------------------------------------------

/// The toy's acknowledgement of its authentication, its synthetic token echoed.
fn acknowledged() -> BootReply {
    BootReply::Text(format!("auth|ok=1|token={TOY_TOKEN}"))
}

/// A login answered 200.
fn logged_in() -> BootReply {
    BootReply::Http {
        status: 200,
        headers: vec![("content-type".into(), "application/json".into())],
        body: br#"{"jwt_token":"synthetic"}"#.to_vec(),
    }
}

/// The toy's setup with a bootstrap of `replies`.
fn booting(replies: Vec<BootReply>) -> Setup {
    Setup {
        bootstrap: Some(Bootstrap { replies }),
        ..assumed()
    }
}

/// The bootstrap that authenticates [`BY_FRAME`].
fn by_frame() -> Setup {
    booting(vec![acknowledged()])
}

/// The bootstrap that authenticates [`BY_LOGIN`].
fn by_login() -> Setup {
    booting(vec![logged_in(), acknowledged()])
}

type Check = fn(&Subject<'_>) -> Result<Verdict, Failure>;

/// The three checks the done line names.
const THREE: [(&str, Check); 3] = [
    ("caps_truthful", caps_truthful),
    ("commands_selfcontained", commands_selfcontained),
    ("signing_golden", signing_golden),
];

fn run(check: Check, venue: &Gated, setup: fn() -> Setup) -> Result<Verdict, Failure> {
    check(&Subject::new(venue, Path::new(FIXTURES), setup).unwrap())
}

fn passed(name: &str, outcome: Result<Verdict, Failure>) {
    match outcome {
        Ok(Verdict::Passed { .. }) => {}
        other => panic!("{name}: expected a pass, got {other:?}"),
    }
}

fn failed(name: &str, outcome: Result<Verdict, Failure>) -> Failure {
    match outcome {
        Err(failure) => failure,
        Ok(verdict) => panic!("{name}: expected a failure, got {verdict:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_toy_gated_on_its_authentication_frame_passes_the_three_checks_with_its_bootstrap() {
    for (name, check) in THREE {
        passed(name, run(check, &BY_FRAME, by_frame));
    }
}

#[test]
fn a_toy_gated_on_a_login_and_its_authentication_frame_passes_the_three_checks_with_its_bootstrap()
{
    for (name, check) in THREE {
        passed(name, run(check, &BY_LOGIN, by_login));
    }
}

#[test]
fn a_gated_toy_fails_each_of_the_three_checks_without_a_bootstrap() {
    for venue in [&BY_FRAME, &BY_LOGIN] {
        for (name, check) in THREE {
            let failure = failed(name, run(check, venue, assumed));
            assert_eq!(failure.check, name);
            let shown = failure.to_string();
            assert!(shown.contains("Disconnected"), "{name}: {shown}");
        }
    }
}

#[test]
fn a_toy_gated_on_a_login_fails_the_three_checks_with_its_frame_alone() {
    for (name, check) in THREE {
        let failure = failed(name, run(check, &BY_LOGIN, by_frame));
        assert!(failure.to_string().contains("Disconnected"), "{failure}");
    }
}

// ---------------------------------------------------------------------------------------------
// Every codec the suite builds is bootstrapped.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_other_encoding_checks_pass_a_gated_toy_with_its_bootstrap() {
    passed(
        "encode_deterministic",
        run(suite::encode_deterministic, &BY_LOGIN, by_login),
    );
    passed("price_grid", run(suite::price_grid, &BY_LOGIN, by_login));
    // price_grid's orders must be sent; encode_deterministic compares two refusals as equal.
    let failure = failed("price_grid", run(suite::price_grid, &BY_LOGIN, assumed));
    assert!(failure.to_string().contains("Disconnected"), "{failure}");
}

#[test]
fn the_checks_reading_fixture_frames_pass_a_gated_toy_with_its_bootstrap() {
    let frames: [(&str, Check); 5] = [
        ("fee_sign", suite::fee_sign),
        ("liquidity_reported", suite::liquidity_reported),
        ("position_signed", suite::position_signed),
        ("decoder_deterministic", suite::decoder_deterministic),
        ("restart_cid", suite::restart_cid),
    ];
    for (name, check) in frames {
        passed(name, run(check, &BY_LOGIN, by_login));
    }
}

#[test]
fn a_bootstrap_with_no_reply_only_opens_the_codec() {
    // Opened but never acknowledged: still refused.
    let failure = failed(
        "caps_truthful",
        run(caps_truthful, &BY_FRAME, || booting(vec![])),
    );
    assert!(failure.to_string().contains("Disconnected"), "{failure}");
}

// ---------------------------------------------------------------------------------------------
// A bootstrap that does not take.
// ---------------------------------------------------------------------------------------------

/// The one `Setup.bootstrap` breach of `failure`, as shown.
fn bootstrap_breach(failure: &Failure) -> String {
    assert!(failure.names("Setup.bootstrap"), "{failure}");
    assert_eq!(failure.breaches.len(), 1, "{failure}");
    failure.breaches[0].what.clone()
}

#[test]
fn an_http_reply_with_no_request_to_answer_fails_naming_its_position() {
    // The frame-authenticated toy asks for no HTTP.
    let setup = || booting(vec![acknowledged(), logged_in()]);
    for (name, check) in THREE {
        let what = bootstrap_breach(&failed(name, run(check, &BY_FRAME, setup)));
        assert!(what.starts_with("reply 1 is an HTTP response"), "{what}");
    }
}

#[test]
fn a_second_http_reply_for_the_one_login_fails() {
    let setup = || booting(vec![logged_in(), logged_in(), acknowledged()]);
    let what = bootstrap_breach(&failed(
        "signing_golden",
        run(signing_golden, &BY_LOGIN, setup),
    ));
    assert!(what.starts_with("reply 1 is an HTTP response"), "{what}");
}

#[test]
fn a_reply_the_codec_refuses_fails_naming_its_position_and_kind_never_its_bytes() {
    let refused_login = || {
        booting(vec![BootReply::Http {
            status: 401,
            headers: Vec::new(),
            body: b"secret-401-body".to_vec(),
        }])
    };
    let what = bootstrap_breach(&failed(
        "caps_truthful",
        run(caps_truthful, &BY_LOGIN, refused_login),
    ));
    assert!(
        what.starts_with("reply 0 (HTTP response) refused by the codec"),
        "{what}"
    );
    assert!(!what.contains("secret-401-body"), "{what}");

    let garbage = || booting(vec![BootReply::Text("secret-text-garbage".into())]);
    let what = bootstrap_breach(&failed(
        "caps_truthful",
        run(caps_truthful, &BY_FRAME, garbage),
    ));
    assert!(
        what.starts_with("reply 0 (text frame) refused by the codec"),
        "{what}"
    );
    assert!(!what.contains("secret-text-garbage"), "{what}");

    let binary = || booting(vec![acknowledged(), BootReply::Binary(vec![0xff, 0xfe])]);
    let what = bootstrap_breach(&failed(
        "commands_selfcontained",
        run(commands_selfcontained, &BY_FRAME, binary),
    ));
    assert!(
        what.starts_with("reply 1 (binary frame) refused by the codec"),
        "{what}"
    );
}

#[test]
fn a_bootstrap_shows_its_replies_by_kind_only() {
    let setup = by_login();
    let shown = format!("{setup:?}");
    assert!(
        shown.contains("replies: [HTTP response, text frame]"),
        "{shown}"
    );
    assert!(!shown.contains(TOY_TOKEN), "{shown}");
    assert!(!shown.contains("jwt_token"), "{shown}");
    let binary = format!("{:?}", BootReply::Binary(vec![1]));
    assert_eq!(binary, "binary frame");
}
