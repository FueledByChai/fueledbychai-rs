# 0006 — The journal records everything that crosses the shard boundary and keeps a reserve so safety traffic never blocks

Status: accepted
Date: 2026-10-02

## Context

Research and incident analysis need to replay a recorded day through the same code that ran
live, byte for byte, and the Java stack cannot (design §10.1). A journal that records only
market data cannot reproduce decisions; a journal that blocks the trading thread when the disk
stalls can stop the cancels that a market move requires (design Appendix A item 24).

## Decision

`fbc-journal` defines the journal format, its writer and its reader (design D7, §4.8, §5.4).
It records everything that crosses the shard boundary: raw inbound frames with their stamps,
outbound bytes, HTTP results with headers, timers, nonces, the wall time used for each encode,
decide-cycle boundaries, write results, and control and marker events. Segments are
length-prefixed and compressed, and grouped by day.

**Safety traffic never blocks on the journal.** The writer has a soft limit for normal records
and a reserve for safety records (cancels, reducing orders, acks and fills). The `JournalSink`
never blocks: at the soft limit normal records are dropped and counted; if the reserve is also
exhausted, safety traffic is still sent and counted, and a `Degraded` marker is written once
space returns.

**Redaction.** Authorization headers, bearer tokens and JWTs, cookies and API keys are stored
only as keyed hashes (0009). Order signatures are kept: replay needs them to compare bytes, and
they are not secret.

Where the files live, how long they are kept and where they are copied is the consumer's
choice; the library writes and reads them.

## Alternatives

- Journal market data only: rejected. Decisions and outbound bytes could not be reproduced.
- Fail closed when the journal is full: rejected. It would stop the cancels the stall makes
  urgent.
- Redact signatures too: rejected. Exact replay compares signed payloads.

## Consequences

- Exact replay of a recorded journal reproduces outbound bytes modulo redaction spans; this
  becomes a golden test once the journal and a venue codec exist.
- Every clock value a codec puts in a payload comes from a journaled encode context, so codecs
  read no clocks.
- Degraded spans are excluded from the golden test and are always counted.

## What would show this was wrong

- Exact replay of a recorded journal produces different outbound bytes outside declared
  redaction spans.
- A disk stall or full journal delays a cancel or a reducing order.
- A journal file is found to contain a live token, cookie, API key or private key.
