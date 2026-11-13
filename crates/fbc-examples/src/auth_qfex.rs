//! `qfex_preprod_probe`'s credentials (decision 0009: a review path; the owner reviews every
//! change here). Included by path by `qfex_preprod_probe` and its test; not a module of this
//! crate's library target, which stays empty with no dependency.
//!
//! Everything in the probe that touches a credential's value is here, and nothing else in it
//! reads one:
//!
//! - **The environment.** The provider user's primary credential, [`PUBLIC_VAR`] and
//!   [`SECRET_VAR`], required: it reads the primary account, and the subaccount [`SUB_ID_VAR`]
//!   names when it has no credential of its own (decision 0144). The subaccount a session arms
//!   on, [`SUB_ID_VAR`] (a lowercase UUID, as decision 0122 selects one), and optionally a
//!   credential QFEX scoped to that subaccount alone, [`SUB_PUBLIC_VAR`] and [`SUB_SECRET_VAR`]
//!   (both or neither). Each value moves straight into fbc-core's [`Secret`] or the venue's
//!   configuration; a refusal names variables only, never a value.
//! - **A session's configuration** ([`session`]): `qfex.account_id` and `qfex.public_key` from
//!   the environment, beside the consumer's settings, and the credentials the order-entry codec
//!   takes. The QFEX adapter's `src/auth` checks both, naming keys and never values.
//! - **A read's credentials** ([`reader`]): the adapter's `ReadAuth` for the account named, the
//!   primary or the subaccount [`SUB_ID_VAR`] names, built from the credential that reaches it.
//! - **What carries a credential to the wire**: a signed REST read ([`send`]), the trade
//!   socket's URL whose query holds the public key ([`open`]), and a frame the adapter signed
//!   ([`write`]). Each request and frame is built, signed and spanned by the adapter
//!   (`fbc_venue_qfex::account_reads`); here it is only handed to fbc-runtime's connector, so
//!   the run's proxy applies (decision 0002). A failure names the step and never a header, a
//!   URL or a frame.

use std::time::Duration;

use fbc_core::{HttpRequest, Secret, Secrets, VenueConfig, WireSlice, WireUrl};
use fbc_runtime::Connector;
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::ws::{Message, WebSocket};
use fbc_venue_qfex::auth::{ACCOUNT_ID, AccountSelection, PUBLIC_KEY, ReadAuth, SECRET_KEY};
use futures_util::SinkExt;

/// The primary credential's public half.
pub const PUBLIC_VAR: &str = "QFEX_PUBLIC_KEY";
/// The primary credential's secret.
pub const SECRET_VAR: &str = "QFEX_SECRET_KEY";
/// The subaccount a session arms on: lowercase 8-4-4-4-12 hex, as QFEX writes one.
pub const SUB_ID_VAR: &str = "QFEX_SUBACCOUNT_ID";
/// A credential QFEX scoped to that subaccount: its public half, optional.
pub const SUB_PUBLIC_VAR: &str = "QFEX_SUBACCOUNT_PUBLIC_KEY";
/// Its secret, set exactly when [`SUB_PUBLIC_VAR`] is.
pub const SUB_SECRET_VAR: &str = "QFEX_SUBACCOUNT_SECRET_KEY";

/// The most bytes of a REST answer read: QFEX's account answers are far smaller.
const MAX_BODY: usize = 1 << 20;

/// Where variables are read from: the process environment when run, a map under test.
pub type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// The account a session arms on.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Armed {
    /// The provider user's primary account.
    Primary,
    /// The subaccount [`SUB_ID_VAR`] names.
    Subaccount,
}

/// The value of `var`, refused naming `var` alone when it is missing or empty.
fn var(lookup: Lookup<'_>, var: &str) -> Result<String, String> {
    let value = lookup(var).filter(|value| !value.is_empty());
    value.ok_or_else(|| format!("{var} is not set: export it before running the probe"))
}

/// The subaccount's own credential variables, when both are set; refused when one is.
fn sub_slot(lookup: Lookup<'_>) -> Result<Option<(&'static str, &'static str)>, String> {
    let set = |name: &str| lookup(name).is_some_and(|value| !value.is_empty());
    match (set(SUB_PUBLIC_VAR), set(SUB_SECRET_VAR)) {
        (true, true) => Ok(Some((SUB_PUBLIC_VAR, SUB_SECRET_VAR))),
        (false, false) => Ok(None),
        _ => Err(format!(
            "set both {SUB_PUBLIC_VAR} and {SUB_SECRET_VAR}, or neither"
        )),
    }
}

/// The variables of the credential that reaches `account`: the subaccount's own for the
/// subaccount [`SUB_ID_VAR`] names, when they are set; the primary credential's otherwise.
fn vars_for(
    lookup: Lookup<'_>,
    account: AccountSelection<'_>,
) -> Result<(&'static str, &'static str), String> {
    let named = lookup(SUB_ID_VAR);
    match (account, sub_slot(lookup)?) {
        (AccountSelection::Subaccount(id), Some(slot)) if named.as_deref() == Some(id) => Ok(slot),
        _ => Ok((PUBLIC_VAR, SECRET_VAR)),
    }
}

/// The credentials the variables `(public, secret)` hold, keyed as the QFEX adapter reads them.
fn secrets(lookup: Lookup<'_>, (public, secret): (&str, &str)) -> Result<Secrets, String> {
    let mut creds = Secrets::new();
    creds.insert(PUBLIC_KEY, Secret::new(var(lookup, public)?));
    creds.insert(SECRET_KEY, Secret::new(var(lookup, secret)?));
    Ok(creds)
}

/// Every variable a run arming on `arms` reads, checked once before anything connects: the
/// primary credential, [`SUB_ID_VAR`] when a session arms on the subaccount or its own
/// credential is set, and that credential's two variables together or not at all.
pub fn check(lookup: Lookup<'_>, arms: &[Armed]) -> Result<(), String> {
    var(lookup, PUBLIC_VAR)?;
    var(lookup, SECRET_VAR)?;
    if sub_slot(lookup)?.is_some() || arms.contains(&Armed::Subaccount) {
        var(lookup, SUB_ID_VAR)?;
    }
    Ok(())
}

/// The configuration of a session arming on `armed`, and the credentials its codec takes:
/// `base` with `qfex.account_id` (`primary`, or [`SUB_ID_VAR`]'s value) and `qfex.public_key`
/// (the public half of the credential that reaches it). Nothing here checks them: the probe's
/// guard and decision 0122 item 1's check do, before anything connects.
pub fn session(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, Secrets), String> {
    let id = match armed {
        Armed::Primary => "primary".to_owned(),
        Armed::Subaccount => var(lookup, SUB_ID_VAR)?,
    };
    let account = match armed {
        Armed::Primary => AccountSelection::Primary,
        Armed::Subaccount => AccountSelection::Subaccount(&id),
    };
    let vars = vars_for(lookup, account)?;
    let mut cfg = base.clone();
    cfg.insert(ACCOUNT_ID, &id);
    cfg.insert(PUBLIC_KEY, &var(lookup, vars.0)?);
    Ok((cfg, secrets(lookup, vars)?))
}

/// The adapter's reader of `account`, the primary or the subaccount [`SUB_ID_VAR`] names, with
/// the credential that reaches it; refused naming that credential's variable and the
/// adapter's key, never a value.
pub fn reader(lookup: Lookup<'_>, account: AccountSelection<'_>) -> Result<ReadAuth, String> {
    let vars = vars_for(lookup, account)?;
    ReadAuth::new(account, secrets(lookup, vars)?).map_err(|e| format!("{}: {e}", vars.0))
}

/// Sends `req`, one of the adapter's signed account reads (each a GET), through `connector`,
/// waiting at most `wait`: its status and body. A failure names the read's path only.
pub async fn send(
    connector: &Connector,
    req: &HttpRequest,
    wait: Duration,
) -> Result<(u16, Vec<u8>), String> {
    let url = req.url.as_str();
    let what = format!("GET {}", url.find("/user/").map_or("", |at| &url[at..]));
    let mut builder = Request::builder().method(Method::GET).uri(url);
    for header in &req.headers {
        builder = builder.header(header.name, header.value.as_str());
    }
    let request = builder
        .body(Bytes::new())
        .map_err(|_| format!("{what}: not built"))?;
    let answer = tokio::time::timeout(wait, connector.http(request, MAX_BODY)).await;
    let answer = answer.map_err(|_| format!("{what}: no answer within {} ms", wait.as_millis()))?;
    let answer = answer.map_err(|e| format!("{what}: {e}"))?;
    Ok((answer.status().as_u16(), answer.body().to_vec()))
}

/// Opens the trade socket at `url`, whose query holds the reading credential's public key,
/// through `connector`, waiting at most `wait`.
pub async fn open(
    connector: &Connector,
    url: &WireUrl,
    wait: Duration,
) -> Result<WebSocket, String> {
    let opened = tokio::time::timeout(wait, connector.websocket(url.as_str())).await;
    let opened = opened.map_err(|_| format!("no read socket within {} ms", wait.as_millis()))?;
    opened.map_err(|e| format!("the read socket: {e}"))
}

/// Writes `frame`, built and signed by the adapter, on `ws` as text.
pub async fn write(ws: &mut WebSocket, frame: &WireSlice) -> Result<(), String> {
    let text = String::from_utf8_lossy(frame.bytes()).into_owned();
    let sent = ws.send(Message::text(text)).await;
    sent.map_err(|_| "the read socket refused a frame".to_owned())
}
