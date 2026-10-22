//! Credential handling for QFEX trade WebSocket authentication. Both the
//! account-identifying public key and the short-lived signature are redacted.

use core::fmt;

use fbc_core::{
    AccountKey, ConfigError, EncodeCtx, InboundSpans, Secret, SignError, VenueConfig, WireSlice,
    WireUrl,
};

use crate::sign;

/// Refuse JSON metacharacters and URL delimiters before interpolating a key.
/// QFEX's documented example uses `qfex_pub_`, but its prefix is not specified
/// as a validation rule, so this checks the safe alphabet alone.
pub fn valid_public_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The account-identifying key required in QFEX's opening URL and auth frame.
pub const PUBLIC_KEY: &str = "qfex.public_key";
/// HMAC secret, carried only in `Secrets`.
pub const SECRET_KEY: &str = "qfex.secret_key";
/// The trade WebSocket origin. `wss://` is required outside a loopback test.
pub const TRADE_URL: &str = "qfex.trade_url";
/// Explicit provider account for private trading: `primary` or a subaccount UUID.
/// Requiring a value prevents a missing subaccount setting from trading the primary account.
pub const ACCOUNT_ID: &str = "qfex.account_id";
/// The consumer's local AccountKey for the selected provider account. This mapping is
/// caller-attested and checked against ExecSessionConfig before endpoint planning.
pub const ACCOUNT_KEY: &str = "qfex.account_key";

/// Require a canonical local key alongside the explicit provider account selection.
pub fn account_key(cfg: &VenueConfig) -> Result<AccountKey, ConfigError> {
    account_selection(cfg)?;
    let raw = cfg
        .get(ACCOUNT_KEY)
        .ok_or(ConfigError::Missing(ACCOUNT_KEY))?;
    let number = raw.parse::<u16>().map_err(|_| ConfigError::Invalid {
        key: ACCOUNT_KEY,
        reason: "expected a canonical decimal AccountKey",
    })?;
    if raw != number.to_string() {
        return Err(ConfigError::Invalid {
            key: ACCOUNT_KEY,
            reason: "expected a canonical decimal AccountKey",
        });
    }
    Ok(AccountKey::new(number))
}

/// The provider account selected for one authenticated trade connection.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum AccountSelection<'a> {
    Primary,
    Subaccount(&'a str),
}

impl fmt::Debug for AccountSelection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary => f.write_str("Primary"),
            Self::Subaccount(_) => f.write_str("Subaccount(<redacted>)"),
        }
    }
}

/// Parse the account-scoped selection; never default to the primary account.
pub fn account_selection(cfg: &VenueConfig) -> Result<AccountSelection<'_>, ConfigError> {
    match cfg.get(ACCOUNT_ID) {
        Some("primary") => Ok(AccountSelection::Primary),
        Some(id) if valid_account_id(id) => Ok(AccountSelection::Subaccount(id)),
        Some(_) => Err(ConfigError::Invalid {
            key: ACCOUNT_ID,
            reason: "expected primary or a canonical subaccount UUID",
        }),
        None => Err(ConfigError::Missing(ACCOUNT_ID)),
    }
}

fn valid_account_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, byte)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

/// Check the URL's configured public key against the credential the codec will
/// authenticate with, before any private connection or authenticated write.
pub fn matching_public_key<'a>(
    configured: &str,
    credential: &'a Secret,
) -> Result<&'a Secret, ConfigError> {
    if !valid_public_key(configured) || configured != credential.expose() {
        return Err(ConfigError::Invalid {
            key: PUBLIC_KEY,
            reason: "does not match the account credential",
        });
    }
    Ok(credential)
}

/// Build the credential-bearing opening URL with an exact redaction span.
/// The caller validates `origin` as a WebSocket origin with no path/query.
pub fn opening_url(origin: &str, configured: &str) -> Result<WireUrl, ConfigError> {
    if !valid_public_key(configured) {
        return Err(ConfigError::Invalid {
            key: PUBLIC_KEY,
            reason: "invalid public key alphabet",
        });
    }
    let base = origin.strip_suffix('/').unwrap_or(origin);
    let mut url = String::with_capacity(base.len() + configured.len() + 9);
    url.push_str(base);
    url.push_str("/?api_key=");
    let start = u32::try_from(url.len()).map_err(|_| ConfigError::Invalid {
        key: TRADE_URL,
        reason: "URL too long",
    })?;
    url.push_str(configured);
    let end = u32::try_from(url.len()).map_err(|_| ConfigError::Invalid {
        key: TRADE_URL,
        reason: "URL too long",
    })?;
    WireUrl::redacted(url, std::iter::once(start..end).collect()).map_err(|_| {
        ConfigError::Invalid {
            key: TRADE_URL,
            reason: "invalid URL redaction",
        }
    })
}

/// Build one authenticated frame from the codec's journaled context. A source
/// providing cryptographically random, unique `u64` nonces is required.
pub fn frame(
    public_key: &Secret,
    signing_key: &Secret,
    ctx: &EncodeCtx,
    account: AccountSelection<'_>,
) -> Result<WireSlice, SignError> {
    let key = public_key.expose();
    if !valid_public_key(key) {
        return Err(SignError::Unsignable("QFEX public key"));
    }
    let nonce = [
        ctx.nonce(0)
            .ok_or(SignError::Unsignable("QFEX authentication nonce"))?,
        ctx.nonce(1)
            .ok_or(SignError::Unsignable("QFEX authentication nonce"))?,
    ];
    let signed = sign::sign(signing_key, nonce, ctx.wall)?;
    let mut body = String::from("{\"type\":\"auth\",\"params\":{\"hmac\":{\"public_key\":\"");
    let key_start = body.len();
    body.push_str(key);
    let key_end = body.len();
    body.push_str("\",\"nonce\":\"");
    body.push_str(signed.nonce());
    body.push_str("\",\"unix_ts\":");
    body.push_str(&signed.unix_ts().to_string());
    body.push_str(",\"signature\":\"");
    let sig_start = body.len();
    body.push_str(signed.signature());
    let sig_end = body.len();
    body.push_str("\"}");
    let account_span = match account {
        AccountSelection::Primary => None,
        AccountSelection::Subaccount(id) => {
            if !valid_account_id(id) {
                return Err(SignError::Unsignable("QFEX subaccount id"));
            }
            body.push_str(",\"account_id\":\"");
            let start = body.len();
            body.push_str(id);
            let end = body.len();
            body.push('"');
            Some(start..end)
        }
    };
    body.push_str("}}");
    let spans = [
        Some(key_start..key_end),
        Some(sig_start..sig_end),
        account_span,
    ]
    .into_iter()
    .flatten()
    .map(|span| {
        Ok(
            u32::try_from(span.start).map_err(|_| SignError::Unsignable("auth frame"))?
                ..u32::try_from(span.end).map_err(|_| SignError::Unsignable("auth frame"))?,
        )
    })
    .collect::<Result<Vec<_>, _>>()?;
    WireSlice::redacted(body.into_bytes(), spans)
        .map_err(|_| SignError::Backend("auth frame redaction"))
}

/// An error may echo the authentication payload. Keep any frame carrying a known account
/// identifier or a signature field out of the journal. Such a frame replays as undecodable,
/// leaving the connection unready.
pub fn inbound_spans(bytes: &[u8], public: &Secret, subaccount: Option<&str>) -> InboundSpans {
    let contains = |needle: &[u8]| {
        !needle.is_empty() && bytes.windows(needle.len()).any(|window| window == needle)
    };
    let sensitive = contains(public.expose().as_bytes())
        || subaccount.is_some_and(|id| contains(id.as_bytes()))
        || contains(b"\"signature\"");
    if sensitive
        && let Ok(end) = u32::try_from(bytes.len())
        && end > 0
    {
        return InboundSpans::frame(std::iter::once(0..end).collect());
    }
    InboundSpans::NONE
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{MonoNs, NonceBlock, WallNs};

    #[test]
    fn unsafe_auth_inputs_refuse_and_diagnostics_hide_selected_account() {
        let mut cfg = VenueConfig::new();
        assert!(account_selection(&cfg).is_err());
        cfg.insert(ACCOUNT_ID, "11111111-1111-1111-1111-111111111111");
        assert!(!format!("{:?}", account_selection(&cfg).unwrap()).contains("11111111"));
        cfg.insert(ACCOUNT_ID, "abcdefab-cdef-abcd-efab-cdefabcdefab");
        assert!(matches!(
            account_selection(&cfg),
            Ok(AccountSelection::Subaccount(_))
        ));
        cfg.insert(ACCOUNT_ID, "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB");
        assert!(
            account_selection(&cfg).is_err(),
            "uppercase UUIDs are not canonical"
        );
        cfg.insert(ACCOUNT_ID, "primary");
        assert_eq!(format!("{:?}", account_selection(&cfg).unwrap()), "Primary");
        cfg.insert(ACCOUNT_KEY, "bad");
        assert!(account_key(&cfg).is_err());
        cfg.insert(ACCOUNT_KEY, "04");
        assert!(account_key(&cfg).is_err());

        let public = Secret::new("qfex_pub_synthetic_1".to_owned());
        let secret = Secret::new("synthetic-secret".to_owned());
        let ctx = EncodeCtx {
            wall: WallNs(1_760_545_414_000_000_000),
            mono: MonoNs(0),
            nonces: NonceBlock::new(vec![1, 2]),
        };
        assert!(opening_url("wss://trade.qfex.com", "bad/key").is_err());
        assert!(
            frame(
                &Secret::new("bad/key".to_owned()),
                &secret,
                &ctx,
                AccountSelection::Primary
            )
            .is_err()
        );
        assert!(
            frame(
                &public,
                &secret,
                &ctx,
                AccountSelection::Subaccount("bad-id")
            )
            .is_err()
        );
        let one_nonce = EncodeCtx {
            nonces: NonceBlock::new(vec![1]),
            ..ctx.clone()
        };
        assert!(frame(&public, &secret, &one_nonce, AccountSelection::Primary).is_err());
        let bad_wall = EncodeCtx {
            wall: WallNs(-1),
            ..ctx
        };
        assert!(frame(&public, &secret, &bad_wall, AccountSelection::Primary).is_err());
    }

    #[test]
    fn echoed_auth_material_is_redacted_without_treating_plain_replies_as_secret() {
        let public = Secret::new("qfex_pub_synthetic_1".to_owned());
        let echoed = br#"{"error":{"signature":"synthetic-value"}}"#;
        assert_eq!(
            inbound_spans(echoed, &public, None).body()[0],
            0..echoed.len() as u32
        );
        let echoed_key = br#"{"error":{"public_key":"qfex_pub_synthetic_1"}}"#;
        assert_eq!(
            inbound_spans(echoed_key, &public, None).body()[0],
            0..echoed_key.len() as u32
        );
        let subaccount = "abcdefab-cdef-abcd-efab-cdefabcdefab";
        let echoed_account = format!("{{\"account_id\":\"{subaccount}\"}}");
        assert_eq!(
            inbound_spans(echoed_account.as_bytes(), &public, Some(subaccount)).body()[0],
            0..echoed_account.len() as u32
        );
        let plain = br#"{"type":"auth","result":"success"}"#;
        assert!(inbound_spans(plain, &public, None).body().is_empty());
    }
}
