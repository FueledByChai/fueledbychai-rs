# Journal fixtures

Journals written by `fbc-journal` itself, kept so a later format can be shown to still read
them. Nothing here was recorded from a venue: every record is synthetic, built in the test that
reads it, and every credential in those records is a synthetic value the writer stored only as
its keyed hash.

| Directory | What it holds | How it was written |
| --- | --- | --- |
| `v2/` | one shard's journal (shard 1) in format version 2, the version before the `Nonce`, `EncodeCtx` and `Cycle` kinds: one record of every version 2 kind across two UTC hours of 2026-10-03, so `20261003/1-000000.fbcj.zst` is a closed, compressed segment and `20261003/1-000001.fbcj` the open one | by `JournalWriter` at commit `59fc783` (format version 2), from `v2_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |
| `v3/` | one shard's journal (shard 1) in format version 3, the version before inbound redaction spans (FBC-7lm): the `v2/` records with one decide cycle's `Cycle`, `Nonce`, `EncodeCtx` and outbound frame after the second inbound frame, across the same two UTC hours | by `JournalWriter` at commit `9347240` (format version 3), from `v3_records()` in `crates/fbc-journal/tests/cycle_records.rs` with that file's `key()` |

`crates/fbc-journal/tests/cycle_records.rs` reads `v2/` and `v3/` with the current reader and
checks every record and keyed hash against `v2_records()` and `v3_records()`. Never rewrite
these files with a newer writer: they stand for journals already on disk.
