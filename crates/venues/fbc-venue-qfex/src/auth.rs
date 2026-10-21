//! Credential handling for QFEX trade WebSocket authentication. Both the
//! account-identifying public key and the short-lived signature are redacted.

use fbc_core::{ConfigError, EncodeCtx, Secret, SignError, WireSlice, WireUrl};

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
/// The trade WebSocket origin. `wss://` is required outside a loopback test.
pub const TRADE_URL: &str = "qfex.trade_url";

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
    body.push_str("\"}}}");
    let spans = [key_start..key_end, sig_start..sig_end]
        .into_iter()
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
