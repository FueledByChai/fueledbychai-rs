# Hibachi conformance fixtures

The fixture directory the named conformance suite (`fbc_conformance::suite!`) is given for
fbc-venue-hibachi, in `crates/venues/fbc-venue-hibachi/tests/conformance.rs` (FBC-2kmp). What
the fixtures assume (the spec table and configuration) is
`crates/venues/fbc-venue-hibachi/tests/conformance/setup.rs`. Hibachi declares no order entry
(`exec: None`), so the suite's order-entry checks skip by name and this directory holds only
what the market-data checks and `legacy_symbols` read. Every frame is hand-built; nothing here
is recorded from Hibachi (decision 0009, `SYNTHETIC`).

- `book_channels/orderbook.frames`, `no_exch_ts_synthesized/orderbook.frames`: the `orderbook`
  channel's cases, frames of `fixtures/hibachi/md/` (whose README cites each shape) tagged with
  what each shows: `public` on each frame that pushes a level, `ts` on the one frame carrying a
  root `timestamp_ms`. A frame of one-side prunes, clears or a gap shows no liquidity and carries
  no channel tag.
- `legacy_symbols/tickers.txt`: Java-era `X/USDT` tickers, read as `X/USDT-P`.

No `continuity/` case: the orderbook is `Continuity::Unsequenced`, so the suite has no sequence
to break and skips it by name; the bounds check that detects Hibachi's gaps is proven by the
crate's own tests (decision 0129).
