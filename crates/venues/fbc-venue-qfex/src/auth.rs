//! QFEX credentials and trade WebSocket authentication (decision 0009: a review path; the owner
//! reviews every change here).
//!
//! Every QFEX credential is read, held and applied here or in `sign`, and nowhere else
//! (decisions 0009, 0043):
//!
//! - [`TradeAuth`] is the one thing an order-entry codec holds. It is built once from the
//!   account's configuration and the consumer's [`Secrets`], which it takes by value: the
//!   public key and the secret move into it (the secret inside `sign`'s [`HmacKey`]), and the
//!   rest is dropped, and so zeroed. Its fields are private and it gives none of them back: a
//!   codec asks it for the authentication frame ([`TradeAuth::frame`]) and for the spans of an
//!   inbound frame to redact ([`TradeAuth::inbound_spans`]), and gets bytes whose credentials
//!   are already marked for the journal; for a private REST read, it gives the signed headers
//!   ([`TradeAuth::rest_headers`], decision 0132 item 5).
//! - [`opening_url`] builds the trade socket's URL, whose query carries the public key
//!   (`?api_key=`), with that key marked as a redaction span.
//! - [`account_selection`] and [`account_key`] read the explicit provider account and the
//!   consumer's account key it is bound to (decision 0122), which the factory's
//!   `exec_account_key` returns so the runtime checks it before planning.
//!
//! QFEX's pages this follows: [authentication](https://docs.qfex.com/websocket/channels/trade/authenticate)
//! (the frame, `account_id`, and the `api_key` query parameter), the
//! [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml) (which messages carry
//! `user_id`), and [API keys](https://docs.qfex.com/api-reference/api-keys) (a key may cover
//! one account or all of them, so the selection is the configuration's, never the key's).
//!
//! The public key and the subaccount id are not secrets in the way the HMAC secret is, but they
//! identify the account, so they are handled as credentials: held in a [`Secret`], never shown
//! by `Debug`, and written to the wire only inside redaction spans (decision 0122 item 3).

mod redact;

use core::fmt;
use core::ops::Range;
use std::net::Ipv4Addr;

use fbc_core::{
    AccountKey, ConfigError, ConfigScope, EncodeCtx, FieldSpec, FieldUnit, Header, Inbound,
    InboundSpans, Secret, Secrets, SignError, VenueConfig, VenueError, WireSlice, WireUrl,
};

use crate::sign::{HmacKey, KeyError};
use crate::valid_host_port;

/// The configuration and credential key of the API credential's public half. The consumer
/// sets it in the account's configuration, for the trade socket's URL, and hands the same value
/// over in [`Secrets`]; [`TradeAuth::new`] refuses the two when they differ (decision 0122).
pub const PUBLIC_KEY: &str = "qfex.public_key";
/// The credential key of the API credential's secret, the HMAC key. Only in [`Secrets`].
pub const SECRET_KEY: &str = "qfex.secret_key";
/// The configuration key of the trade WebSocket's origin, e.g. `wss://trade.qfex.com`.
pub const TRADE_URL: &str = "qfex.trade_url";
/// The configuration key of the provider account to trade: `primary`, or a subaccount's UUID in
/// lowercase. Required, so a missing or mistyped subaccount never trades the primary account
/// (decision 0122).
pub const ACCOUNT_ID: &str = "qfex.account_id";
/// The configuration key of the consumer's account key for that provider account, in decimal.
/// The consumer attests the mapping; the runtime checks it against the session's account
/// before anything is planned (decision 0122).
pub const ACCOUNT_KEY: &str = "qfex.account_key";

/// The provider account's schema entry.
pub const ACCOUNT_ID_FIELD: FieldSpec = FieldSpec {
    key: ACCOUNT_ID,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "the provider account to trade: primary, or a lowercase subaccount UUID; required, \
          never defaulted to primary",
};

/// The account key's schema entry.
pub const ACCOUNT_KEY_FIELD: FieldSpec = FieldSpec {
    key: ACCOUNT_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "the consumer's decimal AccountKey for that provider account, checked against the \
          order-entry session's account before planning",
};

/// The public key's schema entry, which the factory lists for the probe mode (decision 0132
/// item 6).
pub const PUBLIC_KEY_FIELD: FieldSpec = FieldSpec {
    key: PUBLIC_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "the API credential's public half, for the trade socket's URL; also handed over in \
          Secrets, and the two must match",
};

/// The secret's schema entry; the consumer hands its value over as a credential. The factory
/// lists it for the probe mode (decision 0132 item 6).
pub const SECRET_KEY_FIELD: FieldSpec = FieldSpec {
    key: SECRET_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "credential: the API credential's secret, handed over in Secrets; it signs each \
          trade socket's authentication",
};

/// The trade socket's schema entry, which the factory lists for the probe mode (decision 0132
/// item 6), whose guard admits only its pre-production origin or a loopback address.
pub const TRADE_URL_FIELD: FieldSpec = FieldSpec {
    key: TRADE_URL,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "the trade WebSocket origin: wss:// with a host and optional port, or ws:// on \
          loopback for a test stub",
};

/// The private REST reads' headers, from QFEX's REST reference (for instance
/// [user positions](https://docs.qfex.com/api-reference/rest/user/user-positions)): the public
/// key, the HMAC signature, the nonce and the Unix time in seconds it signs, all four required,
/// and the optional account selection.
pub const REST_PUBLIC_KEY: &str = "x-qfex-public-key";
/// The hex HMAC-SHA256 signature of `${nonce}:${unix_ts}`.
pub const REST_SIGNATURE: &str = "x-qfex-hmac-signature";
/// The hex nonce, unique within 15 minutes.
pub const REST_NONCE: &str = "x-qfex-nonce";
/// The Unix time, in whole seconds.
pub const REST_TIMESTAMP: &str = "x-qfex-timestamp";
/// The subaccount a read acts on; omitted, it acts on the primary account.
pub const REST_ACCOUNT: &str = "x-qfex-requested-account-id";

/// The longest public key accepted. QFEX documents no length; this only keeps a frame and a
/// URL that carry one far below what a span can address.
const MAX_PUBLIC_KEY: usize = 256;

/// The primary account's spelling in [`ACCOUNT_ID`].
const PRIMARY: &str = "primary";

/// The provider account a trade connection authenticates as.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum AccountSelection<'a> {
    /// The primary account: the authentication frame carries no `account_id`.
    Primary,
    /// The subaccount with this UUID, sent as the frame's `account_id`.
    Subaccount(&'a str),
}

impl fmt::Debug for AccountSelection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountSelection::Primary => f.write_str("Primary"),
            AccountSelection::Subaccount(_) => f.write_str("Subaccount(<redacted>)"),
        }
    }
}

/// The provider account `cfg` selects: exactly `primary`, or a subaccount's UUID written as
/// QFEX writes one, lowercase 8-4-4-4-12 hex. Refused when missing, and when anything else,
/// an uppercase UUID, a padded one, `Primary` or the nil UUID included: no value falls back to
/// the primary account.
pub fn account_selection(cfg: &VenueConfig) -> Result<AccountSelection<'_>, ConfigError> {
    match cfg.get(ACCOUNT_ID) {
        None => Err(ConfigError::Missing(ACCOUNT_ID)),
        Some(PRIMARY) => Ok(AccountSelection::Primary),
        Some(id) if canonical_uuid(id) => Ok(AccountSelection::Subaccount(id)),
        Some(_) => Err(ConfigError::Invalid {
            key: ACCOUNT_ID,
            reason: "expected primary or a lowercase subaccount UUID",
        }),
    }
}

/// The consumer's account key `cfg` binds to its provider account: the factory's
/// `exec_account_key` (decision 0122). Refused unless the provider account is selected
/// explicitly ([`account_selection`]) and the key is a decimal number from 0 to 65535 written
/// as it prints, with no sign, padding or leading zero.
pub fn account_key(cfg: &VenueConfig) -> Result<AccountKey, ConfigError> {
    account_selection(cfg)?;
    let raw = cfg
        .get(ACCOUNT_KEY)
        .ok_or(ConfigError::Missing(ACCOUNT_KEY))?;
    let invalid = ConfigError::Invalid {
        key: ACCOUNT_KEY,
        reason: "expected a decimal AccountKey with no sign, padding or leading zero",
    };
    match raw.parse::<u16>() {
        Ok(n) if n.to_string() == raw => Ok(AccountKey::new(n)),
        _ => Err(invalid),
    }
}

/// Whether `id` is a UUID as QFEX writes one: 8-4-4-4-12 lowercase hex digits, not all zero
/// (the nil UUID is a placeholder, never an account).
fn canonical_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(at, byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
        && id.bytes().any(|byte| byte != b'0' && byte != b'-')
}

/// The public key `cfg` names, checked to be one that can sit in a URL query and a JSON string
/// as it is: 1 to 256 ASCII letters, digits, `_` or `-`. QFEX's pages show keys like
/// `qfex_pub_...`, but state no format, so only this safe alphabet is checked.
fn configured_public_key(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let key = cfg
        .get(PUBLIC_KEY)
        .ok_or(ConfigError::Missing(PUBLIC_KEY))?;
    if !safe_public_key(key) {
        return Err(ConfigError::Invalid {
            key: PUBLIC_KEY,
            reason: "expected 1 to 256 ASCII letters, digits, _ or -",
        });
    }
    Ok(key)
}

fn safe_public_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_PUBLIC_KEY
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The trade WebSocket's origin `cfg` names, without a trailing `/`: `wss://` with a valid host
/// and optional port, or `ws://` on a loopback host for a test stub; never a path, query, user
/// or fragment.
fn trade_origin(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let raw = cfg.get(TRADE_URL).ok_or(ConfigError::Missing(TRADE_URL))?;
    let bad = || ConfigError::Invalid {
        key: TRADE_URL,
        reason: "expected a wss:// origin, or a ws:// loopback origin",
    };
    let origin = raw.strip_suffix('/').unwrap_or(raw);
    let (authority, plain) = if let Some(rest) = origin.strip_prefix("wss://") {
        (rest, false)
    } else if let Some(rest) = origin.strip_prefix("ws://") {
        (rest, true)
    } else {
        return Err(bad());
    };
    if !valid_host_port(authority) {
        return Err(bad());
    }
    let host = authority
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']').map(|(host, _)| host))
        .unwrap_or_else(|| authority.split(':').next().unwrap_or(""));
    let loopback = host == "localhost"
        || host == "::1"
        || host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback());
    if plain && !loopback {
        return Err(bad());
    }
    Ok(origin)
}

/// The URL the trade socket opens: the configured origin with the public key as its `api_key`
/// query parameter, as QFEX's authentication page shows, the key marked as the URL's one
/// redaction span so no endpoint plan, log line or journal record shows it.
pub fn opening_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
    let origin = trade_origin(cfg)?;
    let key = configured_public_key(cfg)?;
    let mut text = Spanned::default();
    text.plain(origin);
    text.plain("/?api_key=");
    text.redacted(key);
    let (text, spans) = text.finish();
    Ok(WireUrl::redacted(text, spans).expect("one span, inside the URL, on ASCII boundaries"))
}

/// The opaque authentication handle an order-entry codec holds: one account's public key,
/// HMAC key and selected provider account, and the consumer's account key it is bound to.
/// Nothing in it can be read back out; `Debug` shows the account key and whether a subaccount
/// is selected, nothing else. Not `Clone`.
pub struct TradeAuth {
    public_key: Secret,
    key: HmacKey,
    /// `None` only for the explicitly selected primary account.
    subaccount: Option<Secret>,
    account: AccountKey,
}

impl TradeAuth {
    /// How many nonces one authentication frame takes from its `EncodeCtx`: the two halves of
    /// its 128-bit nonce, item 0 first. A codec's `nonces_for` asks this many when its socket
    /// opens.
    pub const NONCES: u16 = 2;

    /// The handle for the account `cfg` configures, holding the credentials `creds` hold. It
    /// takes `creds`: the public key and the secret move in, anything else is dropped. Refused,
    /// naming the key and never a value, when the provider account or account key is missing
    /// or invalid ([`account_key`]), when the configured public key is missing or unsafe, when
    /// `creds` lack the public key or the secret, when the two public keys differ, or when the
    /// secret is empty.
    pub fn new(cfg: &VenueConfig, mut creds: Secrets) -> Result<TradeAuth, VenueError> {
        let account = account_key(cfg)?;
        let subaccount = match account_selection(cfg)? {
            AccountSelection::Primary => None,
            AccountSelection::Subaccount(id) => Some(Secret::new(id.to_owned())),
        };
        let configured = configured_public_key(cfg)?;
        let public_key = creds
            .take(PUBLIC_KEY)
            .ok_or(ConfigError::Missing(PUBLIC_KEY))?;
        if public_key.expose() != configured {
            return Err(ConfigError::Invalid {
                key: PUBLIC_KEY,
                reason: "the credential's public key is not the configured one",
            }
            .into());
        }
        let secret = creds
            .take(SECRET_KEY)
            .ok_or(ConfigError::Missing(SECRET_KEY))?;
        let key = HmacKey::new(secret).map_err(|err| ConfigError::Invalid {
            key: SECRET_KEY,
            reason: match err {
                KeyError::Empty => "an empty secret",
            },
        })?;
        Ok(TradeAuth {
            public_key,
            key,
            subaccount,
            account,
        })
    }

    /// The consumer's account key this handle is bound to.
    pub fn account(&self) -> AccountKey {
        self.account
    }

    /// Whether it authenticates as a subaccount rather than the primary account.
    pub fn is_subaccount(&self) -> bool {
        self.subaccount.is_some()
    }

    /// The authentication frame for a socket that opened with `ctx`, whose nonces must be
    /// exactly the [`NONCES`](TradeAuth::NONCES) the runtime reserved for it:
    ///
    /// ```json
    /// {"type":"auth","params":{"hmac":{"public_key":"…","nonce":"…","unix_ts":…,"signature":"…"},"account_id":"…"}}
    /// ```
    ///
    /// with `account_id` only for a subaccount. The public key, the signature and the
    /// subaccount id are its redaction spans, so the journal keeps each only as a keyed hash;
    /// the nonce and the time are written as they are, as the journal already holds them.
    /// Refused, building nothing, for any other number of nonces or a wall time before the
    /// Unix epoch.
    pub fn frame(&self, ctx: &EncodeCtx) -> Result<WireSlice, SignError> {
        let nonce = match ctx.nonces.as_slice() {
            &[high, low] => [high, low],
            _ => {
                return Err(SignError::Unsignable(
                    "QFEX authentication takes two nonces",
                ));
            }
        };
        let signed = self.key.sign(nonce, ctx.wall)?;
        let mut body = Spanned::default();
        body.plain(r#"{"type":"auth","params":{"hmac":{"public_key":""#);
        body.redacted(self.public_key.expose());
        body.plain(r#"","nonce":""#);
        body.plain(signed.nonce());
        body.plain(r#"","unix_ts":"#);
        body.plain(&signed.unix_ts().to_string());
        body.plain(r#","signature":""#);
        body.redacted(signed.signature());
        body.plain(r#""}"#);
        if let Some(id) = &self.subaccount {
            body.plain(r#","account_id":""#);
            body.redacted(id.expose());
            body.plain(r#"""#);
        }
        body.plain("}}");
        let (text, spans) = body.finish();
        Ok(
            WireSlice::redacted(text.into_bytes(), spans)
                .expect("spans in order, inside the frame"),
        )
    }

    /// How many nonces one signed REST read takes from its `EncodeCtx`: the two halves of its
    /// 128-bit nonce, as [`frame`](TradeAuth::frame)'s.
    pub const REST_NONCES: u16 = 2;

    /// The headers of one private REST read (decision 0132 item 5) made with `ctx`, whose
    /// nonces must be exactly the [`REST_NONCES`](TradeAuth::REST_NONCES) reserved for it.
    /// QFEX's REST reference signs a read as it signs the trade socket's authentication: an
    /// HMAC-SHA256 of `${nonce}:${unix_ts}`, hex encoded, keyed with the secret (`sign`). So:
    ///
    /// - [`REST_PUBLIC_KEY`], the public key, and [`REST_SIGNATURE`], the signature, each
    ///   `redact: true`, so the journal keeps them only as keyed hashes (decisions 0024 and
    ///   0122 item 3);
    /// - [`REST_NONCE`] and [`REST_TIMESTAMP`] as they are, as the authentication frame writes
    ///   them, the journal already holding the nonces;
    /// - [`REST_ACCOUNT`], the subaccount's UUID, `redact: true`, only when the handle selects
    ///   a subaccount: omitted, QFEX reads the primary account, which the configuration
    ///   selected explicitly (decision 0122).
    ///
    /// Refused, building nothing, for any other number of nonces or a wall time before the
    /// Unix epoch.
    pub fn rest_headers(&self, ctx: &EncodeCtx) -> Result<Vec<Header>, SignError> {
        let nonce = match ctx.nonces.as_slice() {
            &[high, low] => [high, low],
            _ => {
                return Err(SignError::Unsignable("a QFEX REST read takes two nonces"));
            }
        };
        let signed = self.key.sign(nonce, ctx.wall)?;
        let header = |name, value: &str, redact| Header {
            name,
            value: value.to_owned(),
            redact,
        };
        let mut headers = vec![
            header(REST_PUBLIC_KEY, self.public_key.expose(), true),
            header(REST_SIGNATURE, signed.signature(), true),
            header(REST_NONCE, signed.nonce(), false),
            header(REST_TIMESTAMP, &signed.unix_ts().to_string(), false),
        ];
        if let Some(id) = &self.subaccount {
            headers.push(header(REST_ACCOUNT, id.expose(), true));
        }
        Ok(headers)
    }

    /// The spans of `input`, a frame off the trade socket or a response to one of its codec's
    /// requests, that identify the account or carry a credential, for the codec's
    /// `redact_inbound` (decision 0028). Each is a span of its own, never the whole input
    /// unless the input is not JSON, so what a codec decodes reads the same in replay as live:
    ///
    /// - the value of every member QFEX names an account, a user or a credential by (`user_id`,
    ///   which `order_response`, `all_orders_response` rows, `fill_response` and others carry;
    ///   `account_id`, `id`, `public_key`, `api_key`, `signature`, `jwt`, `secret_key`), at any
    ///   depth: a string's characters (its quotes kept) and a number's digits;
    /// - any string, a member name included, that holds this handle's public key or subaccount
    ///   id, or a quoted name of such a member (a request echoed as a string);
    /// - the whole input, but a leading `{`, `[` or other character that keeps it unparseable,
    ///   when it is not JSON that `serde_json` parses: nothing in it can be decoded, so
    ///   nothing is lost.
    ///
    /// Blanked, a string stays a string of its length and a number a number, and every byte
    /// outside the spans is the input's own; a codec must never read the members above. A user
    /// id written into free text rather than a `user_id` member is not known here and is not
    /// found. This names the account's material only: how much more of a frame that can echo a
    /// request the journal keeps is the codec's rule to add (`rate`, decisions 0126 and 0132).
    pub fn inbound_spans(&self, input: Inbound<'_>) -> InboundSpans {
        let marks = redact::Marks {
            public_key: self.public_key.expose(),
            subaccount: self.subaccount.as_ref().map(Secret::expose),
        };
        match input {
            Inbound::Frame(frame) => InboundSpans::frame(redact::spans(frame.bytes(), &marks)),
            Inbound::Http(_, resp) => {
                InboundSpans::response(Vec::new(), redact::spans(resp.body, &marks))
            }
        }
    }
}

impl fmt::Debug for TradeAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TradeAuth")
            .field("account", &self.account)
            .field("subaccount", &self.is_subaccount())
            .finish_non_exhaustive()
    }
}

/// Text built in order, with the byte ranges of the parts that must be redacted.
#[derive(Default)]
struct Spanned {
    text: String,
    spans: Vec<Range<u32>>,
}

impl Spanned {
    fn plain(&mut self, part: &str) {
        self.text.push_str(part);
    }

    /// Appends `part` as a redaction span of its own; an empty part makes no span.
    fn redacted(&mut self, part: &str) {
        let start = self.at();
        self.text.push_str(part);
        let end = self.at();
        if start < end {
            self.spans.push(start..end);
        }
    }

    /// Where the text ends. Every part is bounded (a public key at most 256 bytes, the rest
    /// fixed or checked), so a frame or URL is far below what a `u32` span addresses.
    fn at(&self) -> u32 {
        u32::try_from(self.text.len()).expect("a frame or URL far below 4 GiB")
    }

    fn finish(self) -> (String, Vec<Range<u32>>) {
        (self.text, self.spans)
    }
}
