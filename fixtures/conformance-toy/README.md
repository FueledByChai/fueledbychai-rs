# Conformance toy fixtures

The fixture directory the named conformance suite (`fbc_conformance::suite!`) is given for the
conformance toy venue (`crates/fbc-conformance/src/toy/`, decision 0044), in
`crates/fbc-conformance/tests/suite_toy.rs`. The toy describes no real venue, so nothing here
is recorded from one.

A check that reads recorded data reads it from a subdirectory named after the check
(`<check>/`), described in that check's documentation in `crates/fbc-conformance/src/suite/`:

- `signing_golden/`: one `<name>.golden` per golden command the suite's setup lists
  (`crates/fbc-conformance/tests/toy_setup/mod.rs`, `goldens()`), holding the exact bytes of
  the frame the toy encodes it to, signature included, and the `SYNTHETIC` marker decision 0009
  asks of golden signing vectors. The toy's signer holds no key; its signature is a hash.
- `legacy_symbols/tickers.txt`: Java-era ticker values the toy's own made-up rule reads
  (`X/USDT`); the Java stack never traded the toy, so these stand in for the values a real
  venue's fixtures list from exported Java configurations.

- `fee_sign/`, `liquidity_reported/`, `position_signed/` and `decoder_deterministic/`: case
  files (`<case>.frames`, one frame the toy sends per `text` line, in the format
  `crates/fbc-conformance/src/suite/frames.rs` documents), written by hand in the toy's own
  protocol (`crates/fbc-conformance/src/toy/decode.rs`): maker fills with a rebate and a
  taker fill paying a fee (`fee_sign/rebate`, `fee_sign/paid`), maker and taker fills
  (`liquidity_reported/maker`, `liquidity_reported/taker`), long and short positions in a
  resync's answer, asked for by the `resync` line before them (`position_signed/long`,
  `position_signed/short`), and order updates, a refused request, a venue mode and a frame the
  toy refuses, which `decoder_deterministic` decodes with all the others
  (`decoder_deterministic/orders`). Client ids in them are arbitrary text, never one the suite
  minted.

- `ids_roundtrip/java_era.txt`: client ids another system would send the toy, each of which
  `ids_roundtrip` must read as `Unparseable`: Java-style millisecond counters, a random UUID,
  a typed-in label and one of the suite's own ids with its last character changed. The Java
  stack never traded the toy, so these stand in for a real venue's Java-era ids.
- `restart_cid/resting.frames`: a case in the same format of what the toy shows a process
  restarted after the suite's mint issued our first eight ids: order updates for two of them
  (filled, canceled) and a resync's answer with two resting, the newest among them, beside an
  order whose client id is not ours. The client ids are the toy's spelling (base 62, 21
  characters) of the suite's sequence numbers 5 to 8 in its namespace 1, computed with
  `fbc_core::encode_cid`; nothing here is a real account's.

- `continuity/`, `no_exch_ts_synthesized/` and `book_channels/`: market-data case files, one per
  book channel the suite drives (`<channel>.frames`, in the format
  `crates/fbc-conformance/src/suite/book_cases.rs` documents), written by hand in the toy's own
  protocol (`crates/fbc-conformance/src/toy/md.rs`), each line tagged with what the fixture
  knows of it. The toy's `book` channel: its sequence breaks marked `gap=<symbol>`
  (`continuity/book`), its frames carrying a timestamp marked `ts`
  (`no_exch_ts_synthesized/book`), and its frames marked with the order channels whose
  liquidity they show, public only (`book_channels/book`). The toy's `rpi_book`, anchored on
  REST (FBC-fhk4): its snapshots are the anchors' answers, `anchor|sym|seq|<levels>` bodies in
  `http` lines answering the requests the toy asks for under the setup's anchor base
  (`crates/fbc-conformance/tests/toy_setup/mod.rs`, `ANCHOR_URL`, never opened), TOYA-PERP's
  as request tag 1 and TOYB-PERP's as tag 2 when subscribed, each later one under the next
  tag; its breaks marked `gap=<symbol>`, with a delta held without a second gap while its
  re-anchor is awaited, a break of one instrument while the other's re-anchor is awaited, and
  an anchor that mends nothing (`continuity/rpi_book`), its frames and answers carrying a
  timestamp marked `ts`, and a delta held while an anchor is awaited marked `ts=<ns>` with the
  timestamp it carries, which the anchor's answer, carrying none, releases and names with
  `held=<ns>:1`, its one level the one event carrying it (`no_exch_ts_synthesized/rpi_book`,
  Codex r4489870341), and its frames and answers marked with the order channels whose
  liquidity they show, public and RPI, as the toy declares (`book_channels/rpi_book`). The
  toy's market data is text, so `continuity` has no `longer_block/` sub-case here. Nothing in
  them is key- or address-shaped.
- `reject_coverage/table.txt`: the toy's own made-up reject codes (its `REJECT_CODES`, in
  `crates/fbc-conformance/src/toy/decode.rs`), each followed by the `RejectKind` it maps to as
  the kind's `Debug` spells it, and one code the toy does not know, which maps to `Other`
  (decision 0085).

`caps_truthful`, `commands_selfcontained`, `encode_deterministic`, `price_grid` and
`subscriptions_idempotent` read no file: they need only the toy's factory and the setup the
suite's test gives (the toy's spec table, a configuration naming only the anchored channel's
REST base, and no credentials). Nor do
`amend_ack`, `mixed_batch`, `unknown_on_timeout`, `resync_after_reconnect` and `two_phase_ack`:
they run the order-entry session against the stub server, which answers as the setup's
order-entry stub states (`crates/fbc-conformance/tests/toy_setup/mod.rs`, decisions 0083 and
0085).

One subdirectory here belongs to no suite check: `exact_replay/` holds exact replay's golden
(FBC-qwq, decisions 0006 and 0142). It is a conformance-toy order-entry session's journal, which
`crates/fbc-runtime/tests/exact_replay.rs` replays to every outbound byte it holds outside its
redaction spans. It is rewritten only under `FBC_REFRESH_GOLDENS=1`, which
`scripts/check.sh --refresh-goldens` sets for that one test and the check otherwise unsets. Its
own README says what it holds and how it was made.
