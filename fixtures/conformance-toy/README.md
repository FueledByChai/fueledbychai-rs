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

`caps_truthful` and `commands_selfcontained` read no file: they need only the toy's factory
and the setup the suite's test gives (the toy's spec table, an empty configuration and no
credentials).
