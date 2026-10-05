//! Paradex authentication (decision 0009: a review path; the owner reviews every change here).
//!
//! Paradex's documented flow (docs.paradex.trade "Get JWT"; the Java library's
//! `ParadexRestApi.getJwtToken`): `POST /auth` under the REST base URL with the headers
//! `PARADEX-STARKNET-ACCOUNT`, `PARADEX-STARKNET-SIGNATURE` (the auth `Request` signed by
//! [`ParadexSigner::sign_auth_request`]), `PARADEX-TIMESTAMP` and
//! `PARADEX-SIGNATURE-EXPIRATION`, both in seconds. The answer's `jwt_token` is the session
//! token ([`SessionToken`]); the WebSocket's JSON-RPC `auth` method carries it as
//! `params.bearer`, and REST requests as a bearer `Authorization` header.
//!
//! - [`Login`] holds what a login is built from: the account and Stark key, taken from the
//!   consumer's [`Secrets`], the chain id and REST base from its configuration, and the two
//!   intervals the flow keeps apart. The signature's expiry is the login's timestamp plus the
//!   configured lifetime (Java uses one hour): how long the signed request could be replayed
//!   to mint a token. The refresh interval (Java's `paradex.jwt.refresh.seconds`, 60 s) is how
//!   often a login is made again, because the token itself lapses long before the signed
//!   request does.
//! - [`LoginCycle`] makes the logins as effects: one when started, and one each time the
//!   refresh timer it sets after every answer fires. The timer follows the configured interval
//!   alone, never the token's bytes, which replay blanks (0028).
//! - The login is safety traffic, its signature and account in redacted headers: anyone
//!   holding the signature can mint a token until it expires, and an account address is
//!   private (0009). The token is named for the journal ([`token_spans`]) and goes out only
//!   inside a redaction span ([`SessionToken::ws_frame`]) or a redacted header
//!   ([`SessionToken::header`]).
//! - [`connection_plan`] is the factory's Test Connection: the login, then the account read
//!   with the token it gave, as a plan of two rounds (decision 0048).
//!
//! Exec code calls these by name; the words the credential placement check looks for stay in
//! this module (0009).

mod token;

use core::fmt;
use core::time::Duration;

use fbc_core::{
    AccountSummary, AssetSym, ConfigError, ConfigScope, DecodeError, Effect, Effects, EncodeCtx,
    FieldSpec, FieldUnit, Header, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse,
    HttpTag, Money, NextRound, OpKind, PlanError, PlanStep, RateCharge, Secret, Secrets, SignError,
    TimerTag, TrafficClass, VenueConfig, VenueError, WallNs, WireSlice, WireUrl,
};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::sign::{Felt, ParadexSigner, StarkKey, message_felt, short_string};

pub use token::{SessionToken, token_spans};

/// The credential key of the Starknet account address (`0x` and hex digits), as the Java
/// library names it.
pub const ACCOUNT_ADDRESS: &str = "paradex.account.address";
/// The credential key of the account's Stark private key (`0x` and hex digits), as the Java
/// library names it.
pub const SIGNING_KEY: &str = "paradex.private.key";
/// The configuration key of the REST API base, ending in `/v1`:
/// `https://api.prod.paradex.trade/v1` on mainnet, `https://api.testnet.paradex.trade/v1` on
/// testnet. The login is `POST /auth` under it, signed as `/v1/auth`.
pub const REST_URL: &str = "paradex.rest.url";
/// The configuration key of the Starknet chain id the login is signed for: `0x` and hex
/// digits, decimal digits, or the chain's name (`PRIVATE_SN_PARACLEAR_MAINNET`).
pub const CHAIN_ID: &str = "paradex.chain.id";
/// The configuration key of the login signature's lifetime, in whole seconds (`3600s`), at
/// most Paradex's one week (`604800s`).
pub const SIGNATURE_LIFETIME: &str = "paradex.auth.signature.lifetime";
/// The configuration key of the interval between logins (`60s`), the token's refresh.
pub const REFRESH: &str = "paradex.jwt.refresh";
/// The configuration key of how long a login or account request waits for its answer
/// (`5000ms` or `5s`).
pub const TIMEOUT: &str = "paradex.rest.timeout";

/// The REST base's schema entry.
pub const REST_URL_FIELD: FieldSpec = FieldSpec {
    key: REST_URL,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "REST API base (http:// or https://) ending in /v1, without query or user, e.g. \
          https://api.prod.paradex.trade/v1; the login is POST /auth under it",
};
/// The chain id's schema entry.
pub const CHAIN_ID_FIELD: FieldSpec = FieldSpec {
    key: CHAIN_ID,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "Starknet chain id the login is signed for: 0x and hex digits, decimal digits, or the \
          chain's name (PRIVATE_SN_PARACLEAR_MAINNET)",
};
/// The signature lifetime's schema entry.
pub const SIGNATURE_LIFETIME_FIELD: FieldSpec = FieldSpec {
    key: SIGNATURE_LIFETIME,
    scope: ConfigScope::Account,
    unit: FieldUnit::Duration,
    doc: "how long a login's signature stays valid, in whole seconds (e.g. 3600s): the \
          PARADEX-SIGNATURE-EXPIRATION header is the login's timestamp plus this",
};
/// The refresh interval's schema entry.
pub const REFRESH_FIELD: FieldSpec = FieldSpec {
    key: REFRESH,
    scope: ConfigScope::Account,
    unit: FieldUnit::Duration,
    doc: "how long after each login answer the next login is made (e.g. 60s or 60000ms), \
          whatever the session token says",
};
/// The request timeout's schema entry.
pub const TIMEOUT_FIELD: FieldSpec = FieldSpec {
    key: TIMEOUT,
    scope: ConfigScope::Account,
    unit: FieldUnit::Duration,
    doc: "how long a login or account request waits for its answer (e.g. 5000ms or 5s)",
};
/// The account address's schema entry; the consumer hands its value over as a credential.
pub const ACCOUNT_ADDRESS_FIELD: FieldSpec = FieldSpec {
    key: ACCOUNT_ADDRESS,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "credential: the Starknet account address (0x and hex digits), handed over in Secrets",
};
/// The signing key's schema entry; the consumer hands its value over as a credential.
pub const SIGNING_KEY_FIELD: FieldSpec = FieldSpec {
    key: SIGNING_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "credential: the account's Stark private key (0x and hex digits), handed over in \
          Secrets",
};

/// The tag of the login request in [`connection_plan`].
pub const LOGIN_TAG: HttpTag = HttpTag(1);
/// The tag of the account read in [`connection_plan`].
pub const ACCOUNT_TAG: HttpTag = HttpTag(2);

/// The longest signature lifetime Paradex takes: "Max 1 week" (docs.paradex.trade "Get JWT",
/// `PARADEX-SIGNATURE-EXPIRATION`).
const MAX_SIGNATURE_LIFETIME: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The path a login is signed under; the REST base must end in its `/v1`.
const SIGNED_PREFIX: &str = "/v1";

/// What a login is built from: the account and its signer, the REST base, and the signature
/// lifetime, refresh interval and request timeout. Its `Debug` shows the base and the
/// intervals only: never the account, the key or a signature.
pub struct Login {
    /// The account address as the header carries it, `0x` and lowercase hex.
    account: Secret,
    signer: ParadexSigner,
    /// The REST base without a trailing slash.
    rest: String,
    lifetime: Duration,
    refresh: Duration,
    timeout: Duration,
}

impl fmt::Debug for Login {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Login")
            .field("rest", &self.rest)
            .field("lifetime", &self.lifetime)
            .field("refresh", &self.refresh)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// `key` refused for `reason`.
fn invalid(key: &'static str, reason: &'static str) -> ConfigError {
    ConfigError::Invalid { key, reason }
}

/// `0x` then 1 to 64 hex digits, as a field element below the prime.
fn hex_felt(text: &str) -> Option<Felt> {
    let digits = text.strip_prefix("0x")?;
    let hex = !digits.is_empty() && digits.len() <= 64;
    if !hex || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    message_felt(text).ok()
}

/// A chain id: hex, decimal, or a name of capitals, digits and underscores (a short string).
fn chain_felt(text: &str) -> Option<Felt> {
    if text.starts_with("0x") {
        return hex_felt(text);
    }
    let bytes = text.as_bytes();
    if !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit) {
        return message_felt(text).ok();
    }
    let name = |b: &u8| b.is_ascii_uppercase() || b.is_ascii_digit() || *b == b'_';
    let named = bytes.first().is_some_and(u8::is_ascii_uppercase) && bytes.iter().all(name);
    named.then(|| short_string(text).ok()).flatten()
}

/// A positive, whole number of seconds (`<n>s`) or milliseconds (`<n>ms`).
fn duration(text: &str) -> Option<Duration> {
    let (digits, unit): (&str, fn(u64) -> Duration) = match text.strip_suffix("ms") {
        Some(digits) => (digits, Duration::from_millis),
        None => (text.strip_suffix('s')?, Duration::from_secs),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    (n > 0).then(|| unit(n))
}

/// The REST base from `text`, without a trailing slash.
fn rest_base(text: &str) -> Result<String, ConfigError> {
    let base = text.trim_end_matches('/');
    let after = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .ok_or(invalid(REST_URL, "not an http:// or https:// URL"))?;
    if after.contains(['?', '#', '@']) {
        return Err(invalid(
            REST_URL,
            "the adapter writes the path; give the API base with no query, fragment or user",
        ));
    }
    if !base.ends_with(SIGNED_PREFIX) || !after.contains('/') {
        return Err(invalid(
            REST_URL,
            "the login is signed as /v1/auth: give the API base ending in /v1",
        ));
    }
    Ok(base.to_owned())
}

impl Login {
    /// The login for the account `creds` hold, under `cfg`. The account address and the key
    /// are moved out of `creds`; the rest of `creds`, and the key's text once it is read, are
    /// dropped here, and so zeroed. Refused naming a missing or invalid key, never a value.
    pub fn new(cfg: &VenueConfig, mut creds: Secrets) -> Result<Login, VenueError> {
        let get = |key| cfg.get(key).ok_or(ConfigError::Missing(key));
        let rest = rest_base(get(REST_URL)?)?;
        let chain_id = chain_felt(get(CHAIN_ID)?).ok_or(invalid(
            CHAIN_ID,
            "not 0x and hex digits, decimal digits or a chain name",
        ))?;
        let interval =
            |key| duration(get(key)?).ok_or(invalid(key, "not a positive whole <n>s or <n>ms"));
        let lifetime = interval(SIGNATURE_LIFETIME)?;
        if lifetime.subsec_nanos() != 0 {
            return Err(invalid(SIGNATURE_LIFETIME, "not whole seconds").into());
        }
        if lifetime > MAX_SIGNATURE_LIFETIME {
            let reason = "longer than Paradex's one-week maximum (604800s)";
            return Err(invalid(SIGNATURE_LIFETIME, reason).into());
        }
        let (refresh, timeout) = (interval(REFRESH)?, interval(TIMEOUT)?);

        let mut take = |key| creds.take(key).ok_or(ConfigError::Missing(key));
        let (address, key) = (take(ACCOUNT_ADDRESS)?, take(SIGNING_KEY)?);
        let account = hex_felt(address.expose()).ok_or(invalid(
            ACCOUNT_ADDRESS,
            "not 0x and hex digits below the prime",
        ))?;
        let key = StarkKey::from_hex(key.expose()).map_err(|err| match err {
            crate::sign::KeyError::NotHex => invalid(SIGNING_KEY, "not 0x and hex digits"),
            crate::sign::KeyError::OutOfRange => invalid(SIGNING_KEY, "not a Stark key in range"),
        })?;
        Ok(Login {
            account: Secret::new(account.to_hex_string()),
            signer: ParadexSigner::new(account, chain_id, key),
            rest,
            lifetime,
            refresh,
            timeout,
        })
    }

    /// How long a login's signature stays valid.
    pub fn signature_lifetime(&self) -> Duration {
        self.lifetime
    }

    /// How long after each login answer the next login is made.
    pub fn refresh_interval(&self) -> Duration {
        self.refresh
    }

    /// The login as request `tag`: `POST /auth` signed at `ctx.wall` in whole seconds, its
    /// signature expiring the configured lifetime later. Refused for a time before 1970.
    pub fn request(&self, ctx: &EncodeCtx, tag: HttpTag) -> Result<Effect, SignError> {
        let timestamp = seconds(ctx.wall)?;
        let expiration = timestamp
            .checked_add(self.lifetime.as_secs())
            .ok_or(SignError::Unsignable("signature expiration"))?;
        let signature = self
            .signer
            .sign_auth_request(timestamp, expiration)?
            .to_sig();
        let signature = String::from_utf8_lossy(signature.as_bytes()).into_owned();
        let header = |name, value, redact| Header {
            name,
            value,
            redact,
        };
        let headers = vec![
            header(
                "PARADEX-STARKNET-ACCOUNT",
                self.account.expose().to_owned(),
                true,
            ),
            header("PARADEX-STARKNET-SIGNATURE", signature, true),
            header("PARADEX-TIMESTAMP", timestamp.to_string(), false),
            header(
                "PARADEX-SIGNATURE-EXPIRATION",
                expiration.to_string(),
                false,
            ),
        ];
        Ok(Effect::Http {
            tag,
            req: HttpRequest {
                method: HttpMethod::Post,
                url: WireUrl::plain(format!("{}/auth", self.rest)),
                headers,
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: self.timeout,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Rest, None),
        })
    }
}

/// The account read (`GET /account` under `rest`) as request `tag`, carrying `token`.
fn account_request(rest: &str, timeout: Duration, token: &SessionToken, tag: HttpTag) -> Effect {
    Effect::Http {
        tag,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(format!("{rest}/account")),
            headers: vec![token.header()],
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Query, None),
    }
}

/// `wall` in whole seconds since the epoch.
fn seconds(wall: WallNs) -> Result<u64, SignError> {
    u64::try_from(wall.0)
        .map(|ns| ns / 1_000_000_000)
        .map_err(|_| SignError::Unsignable("timestamp before 1970"))
}

/// Why a login gave no token.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum LoginError {
    /// The login got no response.
    Failed(HttpFailure),
    /// The venue answered with a status other than 2xx.
    Status(u16),
    /// The answer holds no readable token; names the part, never a byte of it.
    Decode(DecodeError),
}

impl fmt::Display for LoginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoginError::Failed(failure) => write!(f, "login got no response: {failure:?}"),
            LoginError::Status(status) => write!(f, "login refused with status {status}"),
            LoginError::Decode(err) => write!(f, "login answer refused: {err}"),
        }
    }
}

impl std::error::Error for LoginError {}

/// Logins as effects, for an order-entry codec: one when started, then one each time the
/// refresh timer fires, which is set after every login answer, a failed one included, for the
/// configured refresh interval. The token is kept until a later login gives another, so a
/// failed login leaves the last one in place. The codec routes the answer to
/// [`login_tag`](LoginCycle::login_tag) and the firing of
/// [`refresh_tag`](LoginCycle::refresh_tag) here.
#[derive(Debug)]
pub struct LoginCycle {
    login: Login,
    login_tag: HttpTag,
    refresh_tag: TimerTag,
    token: Option<SessionToken>,
}

impl LoginCycle {
    /// Logins built by `login`, each request tagged `login_tag`, the refresh timer
    /// `refresh_tag`.
    pub fn new(login: Login, login_tag: HttpTag, refresh_tag: TimerTag) -> LoginCycle {
        LoginCycle {
            login,
            login_tag,
            refresh_tag,
            token: None,
        }
    }

    pub fn login_tag(&self) -> HttpTag {
        self.login_tag
    }

    pub fn refresh_tag(&self) -> TimerTag {
        self.refresh_tag
    }

    /// The token the last successful login gave, if one has.
    pub fn token(&self) -> Option<&SessionToken> {
        self.token.as_ref()
    }

    /// Asks for the first login, signed at `ctx`'s time. `Err` pushes nothing.
    pub fn start(&mut self, ctx: &EncodeCtx, fx: &mut Effects) -> Result<(), SignError> {
        fx.push(self.login.request(ctx, self.login_tag)?);
        Ok(())
    }

    /// The refresh timer fired: asks for the next login, signed at `ctx`'s time. `Err` pushes
    /// nothing.
    pub fn on_timer(&mut self, ctx: &EncodeCtx, fx: &mut Effects) -> Result<(), SignError> {
        self.start(ctx, fx)
    }

    /// The login's answer, or why none came: keeps the token it gives, and sets the refresh
    /// timer for the configured interval whatever the answer was or held. `Err` says why no
    /// token came; the token from an earlier login is kept.
    pub fn on_answer(
        &mut self,
        answer: Result<HttpResponse<'_>, HttpFailure>,
        fx: &mut Effects,
    ) -> Result<(), LoginError> {
        fx.push(Effect::Timer {
            tag: self.refresh_tag,
            after: self.login.refresh,
        });
        let resp = answer.map_err(LoginError::Failed)?;
        if !(200..300).contains(&resp.status) {
            return Err(LoginError::Status(resp.status));
        }
        let token = SessionToken::read(resp.body).map_err(LoginError::Decode)?;
        self.token = Some(token);
        Ok(())
    }
}

/// Test Connection (decisions 0043, 0048): a plan of two rounds. The first, built from the
/// context the runtime sends it under, is the login ([`LOGIN_TAG`]); the second, built once the
/// login's answer gave a token, reads the account (`GET /account`, [`ACCOUNT_TAG`]) with it.
/// The account read answers the summary: `account`, and `account_value` in `settlement_asset`
/// as the equity, truncated toward zero at a nanounit. A refused login ends the plan before
/// the read is built. The credentials are moved into the plan and dropped, and so zeroed, once
/// the login is built; the token once the read is (its bytes in the read's redacted header are
/// the request's, as 0043 says of every request).
pub fn connection_plan(
    cfg: &VenueConfig,
    creds: Secrets,
) -> Result<HttpPlan<AccountSummary>, VenueError> {
    let login = Login::new(cfg, creds)?;
    Ok(HttpPlan::later(0, move |ctx: &EncodeCtx| {
        let mut fx = Effects::new();
        fx.push(login.request(ctx, LOGIN_TAG)?);
        let (rest, timeout) = (login.rest.clone(), login.timeout);
        // The account, key and signer are no longer needed: dropped, and so zeroed, here.
        drop(login);
        HttpPlan::then(fx, move |responses, _scope| {
            let token = SessionToken::read(responses[0].body)?;
            Ok(PlanStep::Next(NextRound::new(
                0,
                move |_ctx: &EncodeCtx| {
                    let mut fx = Effects::new();
                    fx.push(account_request(&rest, timeout, &token, ACCOUNT_TAG));
                    HttpPlan::new(fx, |responses, _scope| account_summary(responses[0].body))
                },
            )))
        })
    }))
}

/// The summary in an account answer (docs.paradex.trade "Get account information").
fn account_summary(body: &[u8]) -> Result<AccountSummary, PlanError> {
    const NOT_OBJECT: DecodeError = DecodeError::Malformed("account answer is not a JSON object");
    let doc: Value = serde_json::from_slice(body).map_err(|_| NOT_OBJECT)?;
    let doc = doc.as_object().ok_or(NOT_OBJECT)?;
    let text = |field| {
        doc.get(field)
            .and_then(Value::as_str)
            .ok_or(PlanError::Missing(field))
    };
    let account = text("account")?.to_owned();
    let value = text("account_value")?;
    let asset = text("settlement_asset")?;
    let malformed = |part| PlanError::Decode(DecodeError::Malformed(part));
    let nanos = value
        .parse::<Decimal>()
        .ok()
        .and_then(|d| d.checked_mul(Decimal::from(1_000_000_000)))
        .and_then(|d| i128::try_from(d.trunc()).ok())
        .ok_or(malformed("account_value"))?;
    let asset = AssetSym::new(asset).ok_or(malformed("settlement_asset"))?;
    Ok(AccountSummary {
        account,
        equity: Some(Money::new(nanos, asset)),
    })
}
