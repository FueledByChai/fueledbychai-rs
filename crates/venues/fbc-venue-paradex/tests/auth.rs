//! FBC-mz1: Paradex authentication (`src/auth`, a review path, 0009). The login is an
//! `Effect::Http` built from synthetic `Secrets` and an `EncodeCtx`, its signature the Java
//! signer's `auth_request` vector; the session token read from its answer is named for
//! `redact_inbound` (0028) and carried only inside a redaction span or a redacted header; the
//! next login follows the configured refresh interval, whatever the token's bytes; and Test
//! Connection is a plan of two rounds, login then account (0035, 0043, 0048).
//!
//! Every key and account here is the synthetic one in `fixtures/paradex/signing` (its
//! `SYNTHETIC` file says where each comes from), read from the vectors file's header, and the
//! tokens are made-up text that never looks like a JWT.

mod common;

use std::time::Duration;

use common::Vectors;
use fbc_core::{
    AccountSummary, AssetSym, ConfigError, ConfigScope, DecodeError, Effect, Effects, EncodeCtx,
    FieldUnit, Header, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse, HttpTag,
    Inbound, InboundSpans, Money, MonoNs, NonceBlock, OpKind, PlanError, PlanStep, RateCharge,
    Secret, Secrets, SignError, TimerTag, TrafficClass, VenueConfig, VenueError, VenueFactory,
    WallNs, WireSlice, WireUrl, check_redactions, dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, LOGIN_TAG, Login, LoginCycle, LoginError, REFRESH, REST_URL,
    SIGNATURE_LIFETIME, SIGNING_KEY, SessionToken, TIMEOUT, token_spans,
};
use fbc_venue_paradex::factory::{MD_URL, caps};

const REST: &str = "https://api.prod.paradex.trade/v1";
/// Made-up session tokens: the JWT alphabet, never a JWT's shape.
const TOKEN: &str = "SYNTHETIC.session-token.one";
const OTHER_TOKEN: &str = "SYNTHETIC.session-token.two_2";
const LOGIN: HttpTag = HttpTag(7);
const REFRESH_TIMER: TimerTag = TimerTag(9);

fn cfg() -> VenueConfig {
    let vectors = Vectors::read();
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    cfg.insert(REST_URL, REST);
    cfg.insert(CHAIN_ID, &vectors.header["chain_id"]);
    cfg.insert(SIGNATURE_LIFETIME, "3600s");
    cfg.insert(REFRESH, "60s");
    cfg.insert(TIMEOUT, "5000ms");
    cfg
}

/// The synthetic account and key, as the consumer hands them over.
fn creds() -> Secrets {
    let vectors = Vectors::read();
    let mut creds = Secrets::new();
    creds.insert(
        ACCOUNT_ADDRESS,
        Secret::new(vectors.header["account"].clone()),
    );
    creds.insert(SIGNING_KEY, Secret::new(vectors.header["key"].clone()));
    creds
}

fn login() -> Login {
    Login::new(&cfg(), creds()).unwrap()
}

fn ctx_at(secs: i64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(secs * 1_000_000_000 + 123_456_789),
        mono: MonoNs(1),
        nonces: NonceBlock::EMPTY,
    }
}

fn ok(body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status: 200,
        headers: &[],
        body,
    })
}

fn login_body(token: &str) -> String {
    format!(r#"{{"jwt_token": "{token}"}}"#)
}

/// The value of header `name` in `req`.
fn header<'a>(req: &'a HttpRequest, name: &str) -> &'a Header {
    req.headers
        .iter()
        .find(|h| h.name == name)
        .unwrap_or_else(|| panic!("no header {name}"))
}

/// What every diagnostic in these tests must not show.
fn assert_hidden(shown: &str, secrets: &[&str]) {
    for secret in secrets {
        assert!(!shown.contains(secret), "{shown}");
    }
}

#[test]
fn the_login_is_the_documented_post_with_the_java_vectors_signature_in_a_redacted_header() {
    // docs.paradex.trade "Get JWT": POST /auth with PARADEX-STARKNET-ACCOUNT,
    // PARADEX-STARKNET-SIGNATURE, PARADEX-TIMESTAMP and PARADEX-SIGNATURE-EXPIRATION, both in
    // seconds. The timestamp is EncodeCtx.wall in whole seconds, the expiry that plus the
    // configured lifetime, and the signature the auth_request vector the Java signer wrote.
    let vectors = Vectors::read();
    let row = vectors.row("auth_request");
    let (timestamp, expiration) = (row.u64("timestamp"), row.u64("expiration"));
    let effect = login().request(&ctx_at(timestamp as i64), LOGIN).unwrap();
    let Effect::Http {
        tag,
        req,
        rpc,
        timeout,
        class,
        charge,
    } = &effect
    else {
        panic!("the login is an HTTP request")
    };
    assert_eq!(*tag, LOGIN);
    assert_eq!(req.method, HttpMethod::Post);
    assert_eq!(req.url, format!("{REST}/auth").as_str());
    assert_eq!(req.body, WireSlice::plain(Vec::new()));
    assert_eq!(*rpc, None);
    assert_eq!(*timeout, Duration::from_millis(5000));
    assert_eq!(*class, TrafficClass::Safety);
    assert_eq!(*charge, RateCharge::one(OpKind::Rest, None));
    let names: Vec<_> = req.headers.iter().map(|h| h.name).collect();
    assert_eq!(
        names,
        [
            "PARADEX-STARKNET-ACCOUNT",
            "PARADEX-STARKNET-SIGNATURE",
            "PARADEX-TIMESTAMP",
            "PARADEX-SIGNATURE-EXPIRATION",
        ]
    );
    let sig = format!(r#"["{}","{}"]"#, row.felt("r"), row.felt("s"));
    let signature = header(req, "PARADEX-STARKNET-SIGNATURE");
    assert_eq!(signature.value, sig);
    assert!(signature.redact, "anyone holding it can mint a token");
    let account = header(req, "PARADEX-STARKNET-ACCOUNT");
    assert_eq!(account.value, vectors.account().to_hex_string());
    assert!(account.redact, "an account address is private (0009)");
    let ts = header(req, "PARADEX-TIMESTAMP");
    assert_eq!((ts.value.as_str(), ts.redact), ("1759400000", false));
    let exp = header(req, "PARADEX-SIGNATURE-EXPIRATION");
    assert_eq!((exp.value.as_str(), exp.redact), ("1759403600", false));
    assert_eq!(exp.value, expiration.to_string());

    // The effect's Debug, and the login's, show neither the signature nor the account.
    for shown in [format!("{effect:?}"), format!("{:?}", login())] {
        assert_hidden(
            &shown,
            &[&sig, &vectors.header["account"], &vectors.header["key"]],
        );
        let r = row.felt("r").to_string();
        assert!(!shown.contains(&r), "{shown}");
    }
}

#[test]
fn the_signature_expiry_follows_its_lifetime_and_the_refresh_its_interval_apart() {
    // The signed request's lifetime and the token's refresh are separate settings: changing
    // one moves only what it names.
    let mut short = cfg();
    short.insert(SIGNATURE_LIFETIME, "120s");
    let at = ctx_at(1_759_400_000);
    let headers = |cfg: &VenueConfig| {
        let effect = Login::new(cfg, creds())
            .unwrap()
            .request(&at, LOGIN)
            .unwrap();
        let Effect::Http { req, .. } = effect else {
            unreachable!()
        };
        header(&req, "PARADEX-SIGNATURE-EXPIRATION").value.clone()
    };
    assert_eq!(headers(&cfg()), "1759403600");
    assert_eq!(headers(&short), "1759400120");
    let (long, quick) = (
        Login::new(&cfg(), creds()).unwrap(),
        Login::new(&short, creds()).unwrap(),
    );
    assert_eq!(long.signature_lifetime(), Duration::from_secs(3600));
    assert_eq!(quick.signature_lifetime(), Duration::from_secs(120));
    assert_eq!(long.refresh_interval(), quick.refresh_interval());

    let mut faster = cfg();
    faster.insert(REFRESH, "15000ms");
    let refreshed = Login::new(&faster, creds()).unwrap();
    assert_eq!(refreshed.refresh_interval(), Duration::from_secs(15));
    assert_eq!(headers(&faster), headers(&cfg()));
}

#[test]
fn the_token_is_read_from_the_documented_answer_and_its_span_named_for_redact_inbound() {
    // The answer's jwt_token field carries the token (docs.paradex.trade "Get JWT").
    let body = login_body(TOKEN);
    let token = SessionToken::read(body.as_bytes()).unwrap();
    assert_eq!(token.len(), TOKEN.len());
    assert!(!token.is_empty());
    let resp = HttpResponse {
        status: 200,
        headers: &[("content-type", "application/json")],
        body: body.as_bytes(),
    };
    let spans = token_spans(&resp);
    let at = u32::try_from(body.find(TOKEN).unwrap()).unwrap();
    let span = at..at + u32::try_from(TOKEN.len()).unwrap();
    assert_eq!(spans, InboundSpans::response(vec![], vec![span.clone()]));
    spans.check(Inbound::Http(LOGIN, resp)).unwrap();
    assert_eq!(&body[span.start as usize..span.end as usize], TOKEN);

    // Replay hands the answer back with its span blanked (0028): it still reads, as a token
    // of the same length.
    let mut blanked = body.clone().into_bytes();
    blanked[span.start as usize..span.end as usize].fill(b'2');
    let replayed = SessionToken::read(&blanked).unwrap();
    assert_eq!(replayed.len(), TOKEN.len());

    // A token the answer repeats, unquoted or not, is named wherever it stands.
    let echoed = format!(r#"{{"jwt_token":"{TOKEN}","note":"x{TOKEN}y"}}"#);
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: echoed.as_bytes(),
    };
    let spans = token_spans(&resp);
    assert_eq!(spans.body().len(), 2);
    for span in spans.body() {
        assert_eq!(&echoed[span.start as usize..span.end as usize], TOKEN);
    }
    // Overlapping copies are named as one span covering both.
    let overlapping = r#"{"jwt_token":"aa","x":"aaa"}"#;
    let spans = token_spans(&HttpResponse {
        status: 200,
        headers: &[],
        body: overlapping.as_bytes(),
    });
    let named: Vec<_> = spans
        .body()
        .iter()
        .map(|s| &overlapping[s.start as usize..s.end as usize])
        .collect();
    assert_eq!(named, ["aa", "aaa"]);
    check_redactions(overlapping.as_bytes(), spans.body()).unwrap();
}

#[test]
fn an_answer_whose_token_cannot_be_located_is_redacted_whole_and_one_without_a_token_not_at_all() {
    let spans_of = |status, body: &[u8]| {
        token_spans(&HttpResponse {
            status,
            headers: &[],
            body,
        })
    };
    // A refusal names no token: nothing to redact.
    let refusal = br#"{"error":"INVALID_SIGNATURE","message":"bad signature"}"#;
    assert_eq!(spans_of(401, refusal), InboundSpans::NONE);
    assert_eq!(spans_of(200, b""), InboundSpans::NONE);
    // Bytes that are not JSON, a token that is not a string, and a token written with
    // escapes (so its bytes are not the token's) are redacted whole, since where a token
    // stands in them cannot be told.
    // Codex r4184896990: a parsed answer keeps only the last of repeated keys, so an answer
    // that names jwt_token more than once (repeated, nested, or in a value), or holds any
    // escape that could spell the key another way, is redacted whole: an earlier token is not
    // left verbatim. A refusal that mentions no token and escapes nothing is left as it is.
    let repeated = br#"{"jwt_token":"SYNTHETIC-A","jwt_token":"SYNTHETIC-B"}"#;
    let nested = br#"{"data":{"jwt_token":"SYNTHETIC-A"},"jwt_token":"SYNTHETIC-B"}"#;
    let nested_only = br#"{"data":{"jwt_token":"SYNTHETIC-A"}}"#;
    let mentioned = br#"{"error":"no jwt_token"}"#;
    let escaped_refusal = br#"{"error":"a "quoted" reason"}"#;
    for body in [
        &repeated[..],
        nested,
        nested_only,
        mentioned,
        escaped_refusal,
    ] {
        let whole = 0..u32::try_from(body.len()).unwrap();
        let spans = spans_of(200, body);
        assert_eq!(
            spans,
            InboundSpans::response(vec![], vec![whole]),
            "{body:?}"
        );
    }
    let escaped = br#"{"jwt_token":"\u0053YNTHETIC"}"#;
    for body in [
        &b"not json"[..],
        br#"{"jwt_token":7}"#,
        escaped,
        b"\xff\xfe",
    ] {
        let whole = 0..u32::try_from(body.len()).unwrap();
        assert_eq!(
            spans_of(200, body),
            InboundSpans::response(vec![], vec![whole])
        );
    }
}

#[test]
fn a_token_outside_the_jwt_alphabet_or_missing_is_refused_naming_the_field_only() {
    let refused = |body: &[u8]| SessionToken::read(body).err();
    let malformed = Some(DecodeError::Malformed("jwt_token"));
    assert_eq!(
        refused(b"not json"),
        Some(DecodeError::Malformed("login answer is not JSON"))
    );
    assert_eq!(refused(br#"{"other":"x"}"#), malformed);
    assert_eq!(refused(br#"["jwt_token"]"#), malformed);
    assert_eq!(refused(br#"{"jwt_token":""}"#), malformed);
    assert_eq!(refused(br#"{"jwt_token":5}"#), malformed);
    for bad in ["a b", "a\\\"b", "a\u{e9}b", "a/b"] {
        let body = format!(r#"{{"jwt_token":"{bad}"}}"#);
        assert_eq!(refused(body.as_bytes()), malformed, "{bad}");
    }
}

#[test]
fn the_token_travels_only_inside_a_redaction_span_or_a_redacted_header() {
    let token = SessionToken::read(login_body(TOKEN).as_bytes()).unwrap();
    // The WebSocket's JSON-RPC auth method carries it as params.bearer.
    let frame = token.ws_frame(4);
    let text = std::str::from_utf8(frame.bytes()).unwrap();
    let doc: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(doc["jsonrpc"], "2.0");
    assert_eq!(doc["method"], "auth");
    assert_eq!(doc["params"]["bearer"], TOKEN);
    assert_eq!(doc["id"], 4);
    let spans = frame.redactions();
    assert_eq!(spans.len(), 1);
    assert_eq!(&text[spans[0].start as usize..spans[0].end as usize], TOKEN);
    // Every copy of the token in the frame is inside the span.
    assert_eq!(text.matches(TOKEN).count(), 1);

    // REST requests carry it as a bearer Authorization header, marked redacted.
    let header = token.header();
    assert_eq!(header.name, "Authorization");
    assert_eq!(header.value, format!("Bearer {TOKEN}"));
    assert!(header.redact);

    // Neither the token, the frame nor the header shows it.
    for shown in [
        format!("{token:?}"),
        format!("{frame:?}"),
        format!("{header:?}"),
    ] {
        assert_hidden(&shown, &[TOKEN]);
    }
    assert_eq!(
        format!("{token:?}"),
        format!("SessionToken(<{} bytes>)", TOKEN.len())
    );
}

#[test]
fn the_next_login_follows_the_refresh_interval_whatever_the_token_holds() {
    // Java's paradex.jwt.refresh.seconds: a login again after the configured interval, driven
    // by a timer, never by reading the token (replay blanks it, 0028).
    let mut cycle = LoginCycle::new(login(), LOGIN, REFRESH_TIMER);
    assert_eq!(
        (cycle.login_tag(), cycle.refresh_tag()),
        (LOGIN, REFRESH_TIMER)
    );
    assert!(cycle.token().is_none());
    let mut fx = Effects::new();
    cycle.start(&ctx_at(1_759_400_000), &mut fx).unwrap();
    let first = fx.take();
    assert_eq!(
        first,
        [login().request(&ctx_at(1_759_400_000), LOGIN).unwrap()]
    );

    let refresh = Effect::Timer {
        tag: REFRESH_TIMER,
        after: Duration::from_secs(60),
    };
    let blanked = "2".repeat(TOKEN.len());
    for token in [TOKEN, OTHER_TOKEN, blanked.as_str()] {
        let body = login_body(token);
        cycle.on_answer(ok(body.as_bytes()), &mut fx).unwrap();
        assert_eq!(fx.take(), std::slice::from_ref(&refresh), "{token}");
        assert_eq!(cycle.token().map(SessionToken::len), Some(token.len()));
    }
    // A failed login keeps the token it had and still schedules the next one.
    let failures: [(Result<HttpResponse<'_>, HttpFailure>, LoginError); 3] = [
        (
            Err(HttpFailure::TimedOut),
            LoginError::Failed(HttpFailure::TimedOut),
        ),
        (
            Ok(HttpResponse {
                status: 401,
                headers: &[],
                body: TOKEN.as_bytes(),
            }),
            LoginError::Status(401),
        ),
        (
            ok(b"{}"),
            LoginError::Decode(DecodeError::Malformed("jwt_token")),
        ),
    ];
    for (answer, err) in failures {
        assert_eq!(cycle.on_answer(answer, &mut fx), Err(err));
        assert_eq!(fx.take(), std::slice::from_ref(&refresh), "{err}");
        assert_eq!(cycle.token().map(SessionToken::len), Some(blanked.len()));
        assert_hidden(&err.to_string(), &[TOKEN]);
    }
    // The timer's firing logs in again, signed at the time it fired.
    cycle.on_timer(&ctx_at(1_759_400_060), &mut fx).unwrap();
    assert_eq!(
        fx.take(),
        [login().request(&ctx_at(1_759_400_060), LOGIN).unwrap()]
    );
    // The cycle's Debug shows no token, signature, account or key.
    let shown = format!("{cycle:?}");
    let vectors = Vectors::read();
    assert_hidden(
        &shown,
        &[&blanked, &vectors.header["account"], &vectors.header["key"]],
    );
}

#[test]
fn login_errors_say_what_failed_and_never_what_the_venue_sent() {
    let cases = [
        (
            LoginError::Failed(HttpFailure::Lost),
            "login got no response: Lost",
        ),
        (LoginError::Status(403), "login refused with status 403"),
        (
            LoginError::Decode(DecodeError::Malformed("jwt_token")),
            "login answer refused: malformed frame: jwt_token",
        ),
    ];
    for (err, text) in cases {
        assert_eq!(err.to_string(), text);
    }
    // A login signed before 1970 is not signed.
    let mut fx = Effects::new();
    let mut cycle = LoginCycle::new(login(), LOGIN, REFRESH_TIMER);
    let before = EncodeCtx {
        wall: WallNs(-1),
        ..ctx_at(0)
    };
    let refused = Err(SignError::Unsignable("timestamp before 1970"));
    assert_eq!(cycle.start(&before, &mut fx), refused);
    assert_eq!(cycle.on_timer(&before, &mut fx), refused);
    assert!(fx.is_empty());
    // The latest wall time there is still signs: a lifetime of at most a week cannot carry
    // the expiry past u64 seconds.
    let last = EncodeCtx {
        wall: WallNs(i64::MAX),
        ..ctx_at(0)
    };
    assert!(login().request(&last, LOGIN).is_ok());
}

#[test]
fn the_configuration_is_read_and_refused_by_key_never_by_value() {
    // Chain id as hex, decimal or Paradex's short string name gives the same login.
    let at = ctx_at(1_759_400_000);
    let expected = login().request(&at, LOGIN).unwrap();
    let vectors = Vectors::read();
    let chain = vectors.chain_id();
    for form in [
        chain.to_string(),
        "PRIVATE_SN_PARACLEAR_MAINNET".to_owned(),
        vectors.header["chain_id"].clone(),
    ] {
        let mut cfg = cfg();
        cfg.insert(CHAIN_ID, &form);
        let effect = Login::new(&cfg, creds())
            .unwrap()
            .request(&at, LOGIN)
            .unwrap();
        assert_eq!(effect, expected, "{form}");
    }
    // A REST base with a trailing slash is the same base.
    let mut slashed = cfg();
    slashed.insert(REST_URL, &format!("{REST}/"));
    assert_eq!(
        Login::new(&slashed, creds()).unwrap().request(&at, LOGIN),
        Ok(expected)
    );

    let invalid = |key: &'static str, value: &str| {
        let mut cfg = cfg();
        cfg.insert(key, value);
        match Login::new(&cfg, creds()) {
            Err(VenueError::Config(ConfigError::Invalid {
                key: refused,
                reason,
            })) => {
                assert_eq!(refused, key, "{value}");
                reason
            }
            other => panic!("{key}={value}: {other:?}"),
        }
    };
    for value in [
        "wss://api.prod.paradex.trade/v1",
        "https://api.prod.paradex.trade",
        "https://api.prod.paradex.trade/v1?x=1",
        "https://user@api.prod.paradex.trade/v1",
    ] {
        invalid(REST_URL, value);
    }
    assert_eq!(
        invalid(REST_URL, "http://127.0.0.1:1/v1#a"),
        "the adapter writes the path; give the API base with no query, fragment or user"
    );
    for value in [
        "0xZZ",
        "0x",
        &format!("0x{}", "f".repeat(65)),
        "1x",
        "",
        "WAY_TOO_LONG_FOR_A_SHORT_STRING_NAME_X",
    ] {
        invalid(CHAIN_ID, value);
    }
    for key in [SIGNATURE_LIFETIME, REFRESH, TIMEOUT] {
        for value in [
            "0s",
            "0ms",
            "60",
            "s",
            "ms",
            "-1s",
            "1.5s",
            "1m",
            "18446744073709551616s",
        ] {
            invalid(key, value);
        }
    }
    assert_eq!(invalid(SIGNATURE_LIFETIME, "1500ms"), "not whole seconds");
    // Codex r4184897007: Paradex takes a signature valid for at most one week ("Get JWT").
    assert_eq!(
        invalid(SIGNATURE_LIFETIME, "604801s"),
        "longer than Paradex's one-week maximum (604800s)"
    );
    let mut week = cfg();
    week.insert(SIGNATURE_LIFETIME, "604800s");
    assert!(Login::new(&week, creds()).is_ok());
    // A missing setting is refused by its key.
    for key in [REST_URL, CHAIN_ID, SIGNATURE_LIFETIME, REFRESH, TIMEOUT] {
        let mut cfg = VenueConfig::new();
        for (k, v) in [
            (REST_URL, REST),
            (CHAIN_ID, "1"),
            (SIGNATURE_LIFETIME, "1s"),
            (REFRESH, "1s"),
            (TIMEOUT, "1s"),
        ] {
            if k != key {
                cfg.insert(k, v);
            }
        }
        let missing = Login::new(&cfg, creds()).err();
        assert_eq!(missing, Some(VenueError::Config(ConfigError::Missing(key))));
    }

    // Credentials missing or unreadable are refused by their key, never their value.
    for key in [ACCOUNT_ADDRESS, SIGNING_KEY] {
        let mut partial = creds();
        drop(partial.take(key));
        let missing = Login::new(&cfg(), partial).err();
        assert_eq!(missing, Some(VenueError::Config(ConfigError::Missing(key))));
        let mut bad = creds();
        let secret_text = "0xSYNTHETICnothex";
        bad.insert(key, Secret::new(secret_text.to_owned()));
        let err = Login::new(&cfg(), bad).err().unwrap();
        assert!(
            matches!(err, VenueError::Config(ConfigError::Invalid { key: k, .. }) if k == key),
            "{err:?}"
        );
        assert_hidden(&format!("{err} {err:?}"), &[secret_text]);
    }
    let mut zero = creds();
    zero.insert(SIGNING_KEY, Secret::new("0x0".to_owned()));
    let err = Login::new(&cfg(), zero).err();
    assert_eq!(
        err,
        Some(VenueError::Config(ConfigError::Invalid {
            key: SIGNING_KEY,
            reason: "not a Stark key in range"
        }))
    );
}

#[test]
fn the_factory_names_every_auth_setting_in_its_schema_at_account_scope() {
    let schema = ParadexFactory.config_schema();
    let keys: Vec<_> = schema.iter().map(|f| f.key).collect();
    assert_eq!(
        keys,
        [
            MD_URL,
            REST_URL,
            CHAIN_ID,
            SIGNATURE_LIFETIME,
            REFRESH,
            TIMEOUT,
            ACCOUNT_ADDRESS,
            SIGNING_KEY
        ]
    );
    assert!(schema.iter().all(|f| f.scope == ConfigScope::Account));
    let unit = |key| schema.iter().find(|f| f.key == key).unwrap().unit;
    for key in [SIGNATURE_LIFETIME, REFRESH, TIMEOUT] {
        assert_eq!(unit(key), FieldUnit::Duration, "{key}");
    }
    // Market data alone is still all the venue declares (order entry is FBC-xzp).
    assert!(ParadexFactory.caps(&cfg()).unwrap().exec.is_none());
    assert_eq!(ParadexFactory.caps(&cfg()).unwrap(), caps());
}

// ---------------------------------------------------------------------------------------------
// Test Connection: login, then the account with the token (0043, 0048).
// ---------------------------------------------------------------------------------------------

const ACCOUNT_BODY: &str = r#"{"account":"0xSYNTHETICACCOUNT","account_value":"1234.567890123","free_collateral":"1","settlement_asset":"USDC","status":"ACTIVE"}"#;

fn connection_plan() -> HttpPlan<AccountSummary> {
    ParadexFactory
        .test_connection(&cfg(), creds())
        .expect("Paradex proves credentials")
        .unwrap()
}

fn run<T>(
    plan: HttpPlan<T>,
    answers: &[(HttpTag, Result<HttpResponse<'_>, HttpFailure>)],
) -> Result<PlanStep<T>, PlanError> {
    dispatch_market_data(&caps(), |scope| plan.parse(answers, scope))
}

/// The plan's next round, built at `secs`.
fn next<T>(step: Result<PlanStep<T>, PlanError>, secs: i64) -> HttpPlan<T> {
    let Ok(PlanStep::Next(round)) = step else {
        panic!("another round follows")
    };
    assert_eq!(round.nonces(), 0, "Paradex's login and reads take no nonce");
    round.build(&ctx_at(secs)).unwrap()
}

#[test]
fn test_connection_logs_in_then_reads_the_account_with_the_token() {
    let vectors = Vectors::read();
    let row = vectors.row("auth_request");
    let secs = row.u64("timestamp") as i64;
    // Round one is built from the context the runtime sends it under: the login, signed.
    let plan = connection_plan();
    assert_eq!(plan.requests(), []);
    let first = next(run(plan, &[]), secs);
    assert_eq!(
        first.requests(),
        [login().request(&ctx_at(secs), LOGIN_TAG).unwrap()]
    );
    // Round two carries the token the login's answer gave, in a redacted header.
    let body = login_body(TOKEN);
    let second = next(run(first, &[(LOGIN_TAG, ok(body.as_bytes()))]), secs + 1);
    let [
        Effect::Http {
            tag,
            req,
            rpc,
            timeout,
            class,
            charge,
        },
    ] = second.requests()
    else {
        panic!("one account read")
    };
    let account_tag = *tag;
    assert_ne!(account_tag, LOGIN_TAG);
    assert_eq!(req.method, HttpMethod::Get);
    assert_eq!(req.url, WireUrl::plain(format!("{REST}/account")));
    assert_eq!(
        req.headers,
        [SessionToken::read(body.as_bytes()).unwrap().header()]
    );
    assert_eq!(req.body, WireSlice::plain(Vec::new()));
    assert_eq!((*rpc, *timeout), (None, Duration::from_millis(5000)));
    assert_eq!(
        (*class, *charge),
        (TrafficClass::Normal, RateCharge::one(OpKind::Query, None))
    );
    assert_hidden(&format!("{second:?}"), &[TOKEN]);

    let summary = run(second, &[(account_tag, ok(ACCOUNT_BODY.as_bytes()))])
        .unwrap()
        .done()
        .unwrap();
    let usdc = AssetSym::new("USDC").unwrap();
    assert_eq!(
        summary,
        AccountSummary {
            account: "0xSYNTHETICACCOUNT".to_owned(),
            equity: Some(Money::new(1_234_567_890_123, usdc)),
        }
    );
}

#[test]
fn test_connection_ends_at_a_refused_login_and_reads_no_account() {
    let secs = 1_759_400_000;
    let first = || next(run(connection_plan(), &[]), secs);
    let tag = LOGIN_TAG;
    let refused = run(
        first(),
        &[(
            tag,
            Ok(HttpResponse {
                status: 401,
                headers: &[],
                body: br#"{"error":"INVALID_SIGNATURE"}"#,
            }),
        )],
    );
    assert_eq!(refused.err(), Some(PlanError::Status { tag, status: 401 }));
    let failure = HttpFailure::NotSent;
    let lost = run(first(), &[(tag, Err(failure))]);
    assert_eq!(lost.err(), Some(PlanError::Http { tag, failure }));
    let unreadable = run(first(), &[(tag, ok(b"{}"))]);
    assert_eq!(
        unreadable.err(),
        Some(PlanError::Decode(DecodeError::Malformed("jwt_token")))
    );
    // A login that cannot be signed ends the plan before any request.
    let Ok(PlanStep::Next(round)) = run(connection_plan(), &[]) else {
        panic!("the login round follows")
    };
    let before = EncodeCtx {
        wall: WallNs(-1),
        ..ctx_at(0)
    };
    assert_eq!(
        round.build(&before).err(),
        Some(PlanError::Sign(SignError::Unsignable(
            "timestamp before 1970"
        )))
    );
}

#[test]
fn test_connection_refuses_an_account_answer_missing_a_field_and_shows_no_account() {
    let secs = 1_759_400_000;
    let body = login_body(TOKEN);
    let read = |answer: &[u8]| {
        let first = next(run(connection_plan(), &[]), secs);
        let second = next(run(first, &[(LOGIN_TAG, ok(body.as_bytes()))]), secs);
        let Effect::Http { tag, .. } = &second.requests()[0] else {
            unreachable!()
        };
        let tag = *tag;
        run(second, &[(tag, ok(answer))]).map(|step| step.done().unwrap())
    };
    let missing = |field| Err(PlanError::Missing(field));
    assert_eq!(
        read(br#"{"account_value":"1","settlement_asset":"USDC"}"#),
        missing("account")
    );
    assert_eq!(
        read(br#"{"account":"0x1","settlement_asset":"USDC"}"#),
        missing("account_value")
    );
    assert_eq!(
        read(br#"{"account":"0x1","account_value":"1"}"#),
        missing("settlement_asset")
    );
    let malformed = |part| Err(PlanError::Decode(DecodeError::Malformed(part)));
    assert_eq!(
        read(b"[]"),
        malformed("account answer is not a JSON object")
    );
    assert_eq!(
        read(b"nope"),
        malformed("account answer is not a JSON object")
    );
    assert_eq!(
        read(br#"{"account":"0x1","account_value":"x","settlement_asset":"USDC"}"#),
        malformed("account_value")
    );
    assert_eq!(
        read(br#"{"account":"0x1","account_value":"1e400","settlement_asset":"USDC"}"#),
        malformed("account_value")
    );
    assert_eq!(
        read(br#"{"account":"0x1","account_value":"1","settlement_asset":"TOOLONGASSET"}"#),
        malformed("settlement_asset")
    );
    // Equity below a nanounit is truncated toward zero.
    let fine =
        read(br#"{"account":"0x1","account_value":"-0.0000000019","settlement_asset":"USDC"}"#);
    assert_eq!(fine.unwrap().equity.unwrap().nanos, -1);
    let summary = read(ACCOUNT_BODY.as_bytes()).unwrap();
    assert_hidden(&format!("{summary:?}"), &["0xSYNTHETICACCOUNT", "1234"]);

    // Credentials missing or a configuration refused: no plan, and the error names the key.
    let mut partial = creds();
    drop(partial.take(SIGNING_KEY));
    let refused = ParadexFactory.test_connection(&cfg(), partial);
    let Some(Err(err)) = refused else {
        panic!("no plan without a key")
    };
    assert_eq!(err, VenueError::Config(ConfigError::Missing(SIGNING_KEY)));
}
