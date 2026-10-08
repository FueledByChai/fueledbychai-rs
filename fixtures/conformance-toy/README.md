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
  protocol (`crates/fbc-conformance/src/toy/md.rs`), each frame tagged with what the fixture
  knows of it: the toy's `book` channel with its sequence breaks marked `gap`
  (`continuity/book`), its frames carrying a timestamp marked `ts`
  (`no_exch_ts_synthesized/book`), and its frames marked with the order channels whose
  liquidity they show, public only (`book_channels/book`). The toy's `rpi_book` is anchored on
  REST, which the suite does not drive yet (FBC-fhk4), so it has no case; the toy's market
  data is text, so `continuity` has no `longer_block/` sub-case here.

`caps_truthful`, `commands_selfcontained`, `encode_deterministic`, `price_grid` and
`subscriptions_idempotent` read no file: they need only the toy's factory and the setup the
suite's test gives (the toy's spec table, an empty configuration and no credentials).
