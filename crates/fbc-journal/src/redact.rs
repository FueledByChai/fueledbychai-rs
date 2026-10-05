//! Keyed hashes in place of redacted bytes (FBC-apz, decision 0024).
//!
//! The journal never writes a redaction span's bytes (0006, 0009). In their place it writes
//! the span's HMAC-SHA-256 under a [`RedactionKey`] the consumer supplies: the same secret
//! under the same key hashes the same way every time, so replay compares outbound bytes modulo
//! spans and still sees whether a credential changed. The spans are the ones the journal
//! blanks: the spans of a [`WireSlice`](fbc_core::WireSlice) or
//! [`WireUrl`](fbc_core::WireUrl), header names and values a codec marked redacted, the values
//! of the [`SECRET_HEADERS`](crate::SECRET_HEADERS) by name, in requests and results alike,
//! and the spans a codec named in an inbound frame or a response body
//! ([`InboundSpans`](fbc_core::InboundSpans), FBC-7lm, decision 0028). Bytes outside spans,
//! order signatures included, are written verbatim.
//!
//! [`Record::digests`] gives a record's hashes in the order the format writes them, which is
//! the order [`Entry::digests`](crate::Entry) reads them back in:
//!
//! - an inbound or outbound frame: its spans in order;
//! - an HTTP request: its URL's spans, then its secret headers in header order, then its body's
//!   spans;
//! - an HTTP result with a response: its secret headers in header order, then its body's
//!   spans;
//! - every other record: none.
//!
//! A secret header gives its name's hash and then its value's when its name is redacted
//! (`HeaderRec::redact_name`), and its value's alone otherwise.

use core::fmt;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::JournalError;
use crate::record::{HeaderRec, Record};

/// The fewest key bytes [`RedactionKey::new`] takes: SHA-256's output length, the shortest key
/// RFC 2104 recommends for HMAC.
pub const MIN_KEY_LEN: usize = 32;

/// The length of a [`SpanDigest`].
pub const DIGEST_LEN: usize = 32;

/// The key the journal hashes redacted spans under. The consumer supplies its bytes and keeps
/// them; the journal holds only the HMAC state derived from them.
///
/// It is not `Clone`: share one through an `Arc` between a [`JournalWriter`](crate::JournalWriter)
/// and a [`journal_queue`](crate::journal_queue). Its `Debug` and `Display` show none of its
/// bytes, and the journal never writes it.
pub struct RedactionKey {
    /// HMAC-SHA-256 with the key absorbed; each span's hash starts from a copy of it.
    mac: Hmac<Sha256>,
}

impl RedactionKey {
    /// A key from the consumer's secret bytes, at least [`MIN_KEY_LEN`] of them. A shorter
    /// key is refused with [`JournalError::Config`].
    pub fn new(bytes: &[u8]) -> Result<RedactionKey, JournalError> {
        if bytes.len() < MIN_KEY_LEN {
            return Err(JournalError::Config(
                "a redaction key shorter than 32 bytes",
            ));
        }
        let mac = Hmac::<Sha256>::new_from_slice(bytes).expect("HMAC takes a key of any length");
        Ok(RedactionKey { mac })
    }

    /// HMAC-SHA-256 of `bytes` under this key.
    pub fn digest(&self, bytes: &[u8]) -> SpanDigest {
        self.digest_chunks([bytes])
    }

    /// HMAC-SHA-256 of `chunks` in order, as of their concatenation, under this key.
    pub(crate) fn digest_chunks<'a>(
        &self,
        chunks: impl IntoIterator<Item = &'a [u8]>,
    ) -> SpanDigest {
        #[cfg(test)]
        TAKEN.with(|n| n.set(n.get() + 1));
        let mut mac = self.mac.clone();
        for chunk in chunks {
            mac.update(chunk);
        }
        SpanDigest(mac.finalize().into_bytes().into())
    }
}

#[cfg(test)]
std::thread_local! {
    /// The digests this thread has taken, so a test can tell a record was refused unhashed.
    static TAKEN: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

/// The digests this test thread has taken.
#[cfg(test)]
pub(crate) fn digests_taken() -> usize {
    TAKEN.with(core::cell::Cell::get)
}

impl fmt::Debug for RedactionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RedactionKey(..)")
    }
}

impl fmt::Display for RedactionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redaction key>")
    }
}

/// The HMAC-SHA-256 of one redacted span under a [`RedactionKey`]: what the journal writes in
/// the span's place. Without the key it says nothing about the span's bytes.
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct SpanDigest(pub [u8; DIGEST_LEN]);

impl fmt::Debug for SpanDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SpanDigest(")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        f.write_str(")")
    }
}

impl Record {
    /// The keyed hash of each of this record's redacted spans under `key`, in the order the
    /// journal writes them (see the [module](self) docs): what the reader returns beside the
    /// record in an [`Entry`](crate::Entry).
    pub fn digests(&self, key: &RedactionKey) -> Vec<SpanDigest> {
        let spans = |bytes: &[u8], spans: &[core::ops::Range<u32>]| {
            spans
                .iter()
                // A span past the end (a record the writer refuses) hashes what the bytes hold.
                .map(|s| key.digest(bytes.get(s.start as usize..s.end as usize).unwrap_or(&[])))
                .collect::<Vec<_>>()
        };
        let headers = |headers: &[HeaderRec]| {
            let mut out = Vec::new();
            for h in headers.iter().filter(|h| h.secret()) {
                if h.redact_name {
                    out.push(key.digest(h.name.as_bytes()));
                }
                out.push(key.digest(h.value.as_bytes()));
            }
            out
        };
        match self {
            Record::Inbound { bytes, redact, .. } => spans(&bytes.0, redact),
            Record::Outbound { frame, .. } => spans(frame.bytes(), frame.redactions()),
            Record::HttpRequest { req, .. } => {
                let mut out = spans(req.url.as_str().as_bytes(), req.url.redactions());
                out.extend(headers(&req.headers));
                out.extend(spans(req.body.bytes(), req.body.redactions()));
                out
            }
            Record::HttpResult {
                result: Ok(resp), ..
            } => {
                let mut out = headers(&resp.headers);
                out.extend(spans(&resp.body.0, &resp.body_redact));
                out
            }
            // Written whole as one hash, when there is anything to write (decision 0041).
            Record::InboundControl { frame, .. } => {
                let whole = frame.content();
                (!whole.is_empty())
                    .then(|| key.digest(whole))
                    .into_iter()
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 test case 6: a 131-byte key, longer than SHA-256's block.
    #[test]
    fn digest_is_hmac_sha256() {
        let key = RedactionKey::new(&[0xaa; 131]).unwrap();
        let got = key.digest(b"Test Using Larger Than Block-Size Key - Hash Key First");
        let want = [
            0x60, 0xe4, 0x31, 0x59, 0x1e, 0xe0, 0xb6, 0x7f, 0x0d, 0x8a, 0x26, 0xaa, 0xcb, 0xf5,
            0xb7, 0x7f, 0x8e, 0x0b, 0xc6, 0x21, 0x37, 0x28, 0xc5, 0x14, 0x05, 0x46, 0x04, 0x0f,
            0x0e, 0xe3, 0x7f, 0x54,
        ];
        assert_eq!(got, SpanDigest(want));
        // A hash starts from the keyed state each time: the same bytes hash the same.
        assert_eq!(
            key.digest(b"Test Using Larger Than Block-Size Key - Hash Key First"),
            got
        );
    }

    #[test]
    fn records_with_nothing_to_redact_have_no_digest() {
        use crate::record::Marker;
        use fbc_core::{ConnKey, HttpFailure, HttpTag, MonoNs, Stamp, WallNs};
        let key = RedactionKey::new(&[1; 32]).unwrap();
        let failed = Record::HttpResult {
            stamp: Stamp {
                ingest_seq: 1,
                kernel_rx: None,
                recv_mono: MonoNs(1),
                recv_wall: WallNs(1),
                conn: ConnKey { conn: 1, epoch: 1 },
            },
            tag: HttpTag(1),
            result: Err(HttpFailure::Lost),
        };
        assert!(failed.digests(&key).is_empty());
        assert!(Record::Marker(Marker::Recovered).digests(&key).is_empty());
    }

    #[test]
    fn a_digest_shows_as_hex() {
        let shown = format!("{:?}", SpanDigest([0xab; DIGEST_LEN]));
        assert_eq!(shown, format!("SpanDigest({})", "ab".repeat(DIGEST_LEN)));
    }
}
