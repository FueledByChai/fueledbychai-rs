# Paradex conformance fixtures

The fixture directory the named conformance suite (`fbc_conformance::suite!`) is given for
fbc-venue-paradex, in `crates/venues/fbc-venue-paradex/tests/conformance.rs` (FBC-6oj). What
the fixtures assume (the spec table, configuration, synthetic credentials, golden commands,
bootstrap and stub answers) is `crates/venues/fbc-venue-paradex/tests/conformance/setup.rs`.
Every frame is hand-built; nothing here is recorded from a Paradex account (decision 0009).

- `signing_golden/`: the exact frame of each golden command, signed ones carrying the Java
  signer's signature for a row of `fixtures/paradex/signing/paradex-vectors.tsv`
  (`SYNTHETIC` says which).
- `legacy_symbols/tickers.txt`: Java-era `X/USDT` tickers, read as `X-USD-PERP` (FBC-l5o).
- `ids_roundtrip/java_era.txt`: client ids the Java Paradex brokers sent (random UUIDs, the
  resilient broker's `<symbol>-<millis>-<nanos>`, a typed-in label), all made up.
- `fee_sign/`, `liquidity_reported/`, `position_signed/`, `decoder_deterministic/`: case files
  whose `hex` lines are the hand-built SBE frames of `fixtures/paradex/exec/` (each preceded by
  its title), one per line.
- `restart_cid/resting.frames`: what a restarted process is shown of the suite's ids 1 to 8,
  two closing order events (from `fixtures/paradex/exec/`, our ids put in) and a resync
  answered over REST (`http` lines, FBC-2905) with our ids 8 and 5 resting beside a Java-era
  order.
- `reject_coverage/table.txt`: the JSON-RPC error codes `REJECT_CODES` maps, each with its kind.
- `continuity/`, `no_exch_ts_synthesized/`, `book_channels/`: one case per book channel
  (`deltas`, `interactive_deltas`), the hand-built `BookEvent` frames of `fixtures/paradex/md/`
  tagged with what each shows; `continuity/longer_block/` holds the same frames with root
  blocks 8 bytes longer than schema 1:1's, and group entries longer than known.
