# Journal fixtures

Journals written by `fbc-journal` itself, kept so a later format can be shown to still read
them. Nothing here was recorded from a venue: every record is synthetic, built in the test that
reads it, and every credential in those records is a synthetic value the writer stored only as
its keyed hash.

| Directory | What it holds | How it was written |
| --- | --- | --- |
| `v2/` | one shard's journal (shard 1) in format version 2, the version before the `Nonce`, `EncodeCtx` and `Cycle` kinds: one record of every version 2 kind across two UTC hours of 2026-10-03, so `20261003/1-000000.fbcj.zst` is a closed, compressed segment and `20261003/1-000001.fbcj` the open one | by `JournalWriter` at commit `59fc783` (format version 2), from `v2_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |
| `v3/` | one shard's journal (shard 1) in format version 3, the version before inbound redaction spans (FBC-7lm): the `v2/` records with one decide cycle's `Cycle`, `Nonce`, `EncodeCtx` and outbound frame after the second inbound frame, across the same two UTC hours | by `JournalWriter` at commit `9347240` (format version 3), from `v3_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |
| `v4/` | one shard's journal (shard 1) in format version 4, the version before outbound frames kept the kind they were sent as (FBC-q7b): the `v3/` records with, after the decide cycle's outbound frame, an inbound frame with a credential span its codec named, a binary outbound frame with bytes that are not UTF-8 outside its span, and one whose only such bytes lie inside its span, across the same two UTC hours | by `JournalWriter` at commit `da9127c` (format version 4), from `v4_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |
| `v5/` | one shard's journal (shard 1) in format version 5, the version before the stale-authorization reason (FBC-j5bw): the `v4/` records, the binary frame whose only bytes that are not UTF-8 lie inside its span written as binary, with, after the first write result, a ping and a close frame received (each payload a synthetic credential) and a write result not sent for `SignFailed`, across the same two UTC hours | by `JournalWriter` at commit `4112975` (format version 5), from `v5_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |
| `v6/` | one shard's journal (shard 1) in format version 6, the version before a request deadline's firing was a record (FBC-0hfl): the `v5/` records with, after the `SignFailed` write result, a write result not sent for `StaleAuthorization`, across the same two UTC hours | by `JournalWriter` at commit `ae61f2c` (format version 6), from `v6_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |

`crates/fbc-journal/tests/cycle_records.rs` reads `v2/`, `v3/`, `v4/`, `v5/` and `v6/` with the
current reader and checks every record and keyed hash against `v2_records()`, `v3_records()`,
`v4_records()`, `v5_records()` and `v6_records()`; an outbound frame of versions 2 to 4 reads back with the
kind its blanked bytes imply, so `v4/`'s second binary frame reads back as text, while `v5/`
kept each frame's kind. Never rewrite
these files with a newer writer: they stand for journals already on disk.
