//! The acceptance program's own REST reads of the Paradex account (decision 0009: a review
//! path; the owner reviews every change here). Included by path by `paradex_testnet_acceptance`
//! and its test; it is not a module of this crate's library target, which stays empty.
//!
//! The order-entry session reads `GET /orders` and `GET /positions` only for its resync, as
//! each connection opens. FBC-8xr's comparison (owner answer C to RB-olg-3) needs them read on
//! demand after every step, beside the orders stream, so the program makes its own reads:
//! [`RestReader`] logs in through the Paradex adapter's own [`Login`] (the same signed
//! `POST /auth` the session's codec makes, built in that crate's `src/auth`), keeps the session
//! token it answers with as the adapter's [`SessionToken`] (a `Secret`, never shown), and sends
//! each read the adapter builds (`open_orders_request`, `positions_request`,
//! `order_history_request`) with the token's header ([`SessionToken::header`]) through
//! fbc-runtime's [`Connector`], so the run's SOCKS5 proxy applies.
//!
//! Nothing here prints, logs or keeps a credential: a refused login is reported by its HTTP
//! status alone, and an answer is handed back as its status, its body and only the response
//! headers whose names speak of a rate limit ([`RestAnswer::limits`], for the per-IP check of
//! design §13.3), each value shown only when it is short plain text. The login's answer body,
//! which holds the token, is never handed back.

use std::time::{Duration, Instant};

use fbc_core::{
    EncodeCtx, HttpMethod, HttpRequest, MonoNs, NonceBlock, Secrets, VenueConfig, WallNs,
};
use fbc_runtime::Connector;
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_venue_paradex::auth::{Login, SessionToken, TIMEOUT};

/// The tag the login goes out under: the program reads its answer itself, so any tag serves.
const LOGIN: fbc_core::HttpTag = fbc_core::HttpTag(1);

/// The most bytes of an answer read: Paradex's lists are far smaller.
const MAX_BODY: usize = 1 << 22;

/// The longest rate-limit header value shown: a count, a window or a time.
const LONGEST_LIMIT_VALUE: usize = 32;

/// An authenticated read's answer: its status, the rate-limit headers it carried
/// ([`RestReader`]'s module documentation) and its body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestAnswer {
    pub status: u16,
    /// Each response header whose lowercased name contains `ratelimit`, `rate-limit` or
    /// `retry-after`, with its value when that is at most 32 printable ASCII characters
    /// without spaces, `<withheld>` otherwise.
    pub limits: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Logs in and reads the account over REST (module documentation).
pub struct RestReader {
    login: Login,
    connector: Connector,
    /// The adapter's configured request timeout (`paradex.rest.timeout`).
    timeout: Duration,
    /// The token the last login gave and when; a login older than the refresh interval is
    /// made again before the next read.
    token: Option<(SessionToken, Instant)>,
    origin: Instant,
    /// How many requests this reader sent, logins included ([`RestReader::requests`]).
    sent: u64,
}

impl std::fmt::Debug for RestReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestReader")
            .field("login", &self.login)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl RestReader {
    /// A reader for the account `creds` hold under `cfg` (the adapter's REST base, chain id,
    /// signature lifetime, refresh interval and request timeout), connecting through
    /// `connector`. The credentials are moved into the adapter's [`Login`]; refused naming a
    /// missing or invalid key, never a value.
    pub fn new(
        cfg: &VenueConfig,
        creds: Secrets,
        connector: Connector,
    ) -> Result<RestReader, String> {
        let login = Login::new(cfg, creds).map_err(|e| format!("the REST login: {e}"))?;
        // `Login::new` has checked the value is a whole `<n>s` or `<n>ms`.
        let timeout = cfg
            .get(TIMEOUT)
            .and_then(duration)
            .ok_or("the REST timeout is not configured")?;
        Ok(RestReader {
            login,
            connector,
            timeout,
            token: None,
            origin: Instant::now(),
            sent: 0,
        })
    }

    /// How many requests this reader has sent, its logins included: what a rate-limit count
    /// moved by is judged against (design §13.3).
    pub fn requests(&self) -> u64 {
        self.sent
    }

    /// `req` (one of the adapter's account reads) sent with the session token's header, logging
    /// in first when no token is held or the one held is older than the refresh interval. Each
    /// request waits at most the configured timeout and never past `by`.
    pub async fn get(&mut self, mut req: HttpRequest, by: Instant) -> Result<RestAnswer, String> {
        let fresh = self
            .token
            .as_ref()
            .is_some_and(|(_, at)| at.elapsed() < self.login.refresh_interval());
        if !fresh {
            self.token = None;
            let token = self.log_in(by).await?;
            self.token = Some((token, Instant::now()));
        }
        let (token, _) = self.token.as_ref().expect("a token was just kept");
        req.headers.push(token.header());
        self.send(&req, by).await
    }

    /// The login: the adapter's signed `POST /auth`, its answer read for the token only.
    async fn log_in(&mut self, by: Instant) -> Result<SessionToken, String> {
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        let mono = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let ctx = EncodeCtx {
            wall: WallNs(wall),
            mono: MonoNs(mono),
            nonces: NonceBlock::EMPTY,
        };
        let effect = self
            .login
            .request(&ctx, LOGIN)
            .map_err(|e| format!("the REST login could not be signed: {e:?}"))?;
        let fbc_core::Effect::Http { req, .. } = effect else {
            return Err("the REST login is not an HTTP request".to_owned());
        };
        let (status, body) = self.exchange(&req, by).await?;
        if !(200..300).contains(&status) {
            return Err(format!("the REST login was refused with status {status}"));
        }
        SessionToken::read(&body).map_err(|e| format!("the REST login's answer: {e}"))
    }

    /// Sends `req` and reads its answer, keeping the rate-limit headers.
    async fn send(&mut self, req: &HttpRequest, by: Instant) -> Result<RestAnswer, String> {
        let response = self.call(req, by).await?;
        let limits = response
            .headers()
            .iter()
            .filter(|(name, _)| {
                let name = name.as_str().to_ascii_lowercase();
                name.contains("ratelimit")
                    || name.contains("rate-limit")
                    || name.contains("retry-after")
            })
            .map(|(name, value)| {
                let shown = value
                    .to_str()
                    .ok()
                    .filter(|v| {
                        v.len() <= LONGEST_LIMIT_VALUE && v.bytes().all(|b| b.is_ascii_graphic())
                    })
                    .map_or_else(|| "<withheld>".to_owned(), str::to_owned);
                (name.as_str().to_owned(), shown)
            })
            .collect();
        Ok(RestAnswer {
            status: response.status().as_u16(),
            limits,
            body: response.body().to_vec(),
        })
    }

    /// Sends `req` and reads its status and body.
    async fn exchange(&mut self, req: &HttpRequest, by: Instant) -> Result<(u16, Vec<u8>), String> {
        let response = self.call(req, by).await?;
        Ok((response.status().as_u16(), response.body().to_vec()))
    }

    /// `req` made through the connector within the timeout, and never past `by` (counted as
    /// sent once it is handed to the connector). A failure names the request's method and path
    /// only: its headers carry the credentials.
    async fn call(
        &mut self,
        req: &HttpRequest,
        by: Instant,
    ) -> Result<fbc_runtime::http::Response<Bytes>, String> {
        let path = path_of(req.url.as_str());
        let method = match req.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Delete => Method::DELETE,
        };
        let what = format!("{method} {path}");
        let mut builder = Request::builder().method(method).uri(req.url.as_str());
        for header in &req.headers {
            builder = builder.header(header.name, header.value.as_str());
        }
        let request = builder
            .body(Bytes::copy_from_slice(req.body.bytes()))
            .map_err(|_| format!("{what}: the request could not be built"))?;
        let wait = self
            .timeout
            .min(by.saturating_duration_since(Instant::now()));
        if wait.is_zero() {
            return Err(format!("{what}: not sent, the wait had passed"));
        }
        self.sent += 1;
        tokio::time::timeout(wait, self.connector.http(request, MAX_BODY))
            .await
            .map_err(|_| format!("{what}: no answer within {} ms", wait.as_millis()))?
            .map_err(|e| format!("{what}: {e}"))
    }
}

/// A configured `<n>s` or `<n>ms`.
fn duration(text: &str) -> Option<Duration> {
    match text.strip_suffix("ms") {
        Some(ms) => ms.parse().ok().map(Duration::from_millis),
        None => text
            .strip_suffix('s')?
            .parse()
            .ok()
            .map(Duration::from_secs),
    }
}

/// The path and query of `url` (after its host), for naming a request.
fn path_of(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.find('/').map_or("/", |at| &rest[at..])
}

#[cfg(test)]
mod tests {
    use super::{duration, path_of};
    use std::time::Duration;

    #[test]
    fn the_configured_timeout_is_read_in_seconds_or_milliseconds() {
        assert_eq!(duration("5000ms"), Some(Duration::from_millis(5000)));
        assert_eq!(duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(duration("5"), None);
    }

    #[test]
    fn a_request_is_named_by_its_path_and_query() {
        assert_eq!(path_of("https://h.example/v1/orders?x=1"), "/v1/orders?x=1");
        assert_eq!(path_of("http://127.0.0.1:9/"), "/");
        assert_eq!(path_of("http://127.0.0.1:9"), "/");
    }
}
