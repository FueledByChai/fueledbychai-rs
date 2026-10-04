# 0024 — The journal hashes each redacted span with HMAC-SHA-256 under a consumer key, using hmac 0.12.1 and sha2 0.10.9 pinned

Status: accepted
Date: 2026-10-04

## Context

0006 keeps authorization headers, bearer tokens, JWTs, cookies and API keys out of the
journal except as keyed hashes, and keeps order signatures, because exact replay compares
signed payloads. FBC-aen built the journal with every redaction span blanked: the spans of a
`WireSlice` or `WireUrl`, header values a codec marked redacted, and the Authorization,
Proxy-Authorization, Cookie and Set-Cookie headers by name, in requests and results alike.
FBC-apz puts a keyed hash in each span's place, so a value hashed under one key is replaced
the same way every time and replay compares bytes modulo spans while still seeing whether a
credential changed. The owner decided on 2026-10-03 that this is ordinary journal code under
`crates/fbc-journal/src/redact*`, not a review path. The key is a secret the consumer holds
(0009); nothing here may choose or store it.

## Decision

Each redaction span is written as HMAC-SHA-256 (RFC 2104 over SHA-256, 32 bytes) of its bytes
under a `RedactionKey` built from at least 32 bytes the consumer supplies; shorter keys are
refused. The format moves to version 2: a span's descriptor is its start, its end and its
hash, and a secret header's value is its length and its hash; bytes outside spans, signatures
included, are written verbatim. The reader returns the record with its spans blanked, as
before, and the hashes beside it (`Entry`), in the order `Record::digests` gives. The hash is
of the span's bytes alone, so equal secrets hash equally wherever they sit. The crates are
`hmac` `=0.12.1` and `sha2` `=0.10.9`, default features off, pinned exactly in the workspace
manifest; both were already in the lockfile through the Paradex signer's tree, so the licence
gate (0017) sees no new crate. `RedactionKey` keeps only the HMAC state derived from the key,
is not `Clone` (writer and sink share it through an `Arc`), and its `Debug` and `Display`
show none of its bytes.

## Alternatives

- `ring::hmac` (ring is already in the tree through rustls, 0020): not taken because ring is
  pulled in by the runtime's TLS stack, and the journal depends only on `fbc-core`; adding
  ring to it brings a C and assembly build into a crate that otherwise has none.
- A plain SHA-256 of each span, with no key: a short or guessable credential (a cookie, a
  short API key) could be found by hashing candidates against the journal.
- Truncated hashes (16 bytes) to save space: credentials are short and few per record, so 16
  bytes a span saves little, and a full HMAC-SHA-256 is the form the design names.
- Domain-separating the hash by where the span sits (header name, URL, frame): equal secrets
  would then hash differently in different places, which replay and investigation want to
  see as equal.
- Keeping version 1 and adding hashes in a side record: two records to keep in step for one
  fact, and version 1 journals were never recorded from a venue (no recording has run).

## Consequences

- Replay and investigation can tell whether the same credential was sent twice, and whether
  it changed, without the journal holding it; anyone without the key learns nothing from a
  hash.
- The consumer must keep the key for as long as it wants to compare hashes across journals;
  journals written under different keys hash the same secret differently.
- A version 1 segment is refused by the reader as an unsupported version.
- Each span costs 32 more bytes on disk and one HMAC on the shard's thread when it is
  journaled (the sink encodes on the caller's thread, 0021).
- Moving either crate is a ticket that says why, since a change could change what a recorded
  span hashes to.

## What would show this was wrong

- A profile showing span hashing a material part of a shard cycle: then a cheaper keyed hash
  (or hashing on the writer thread, with the span bytes kept out of the queue some other way)
  replaces it in a new record.
- A need to compare spans across journals written under different keys, which a per-consumer
  key cannot serve.
- A credential that turns up in a journal file, which the done-line test of FBC-apz would have
  had to miss.
