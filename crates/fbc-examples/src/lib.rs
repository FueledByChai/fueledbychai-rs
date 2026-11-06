//! Sample programs the owner runs by hand; CI builds them but never runs one against a venue.
//!
//! This library target is empty. Each sample is an example target of this crate, with its
//! wiring in a module its integration test includes by path and runs against the conformance
//! kit's stub server on 127.0.0.1:
//!
//! - `md_watch` (`examples/md_watch/`): subscribes to a Paradex market (bbo, trades and the
//!   `deltas` book) and a Binance USD-M symbol (`bookTicker` and the diff-depth book) through
//!   fbc-runtime, keeps one fbc-book book per channel, and prints a line per update. It never
//!   sends an order and never reads a credential. `cargo run -p fbc-examples --example md_watch
//!   -- --help` lists its arguments.
//! - `testnet_trade` (`examples/testnet_trade/`): Paradex TESTNET only. Logs in, arms
//!   cancel-on-disconnect, resyncs, seeds the market by hand as a declared owner-assisted testnet
//!   run (decision 0067), places one post-only order of the size given on its command line
//!   (bounded by the resting cap), well away from the touch, through an fbc-oms authorization,
//!   cancels it, and Stops. Its credentials are read from the
//!   environment by `src/auth.rs` (a review path), which the sample and its test include by
//!   path: it is not a module of this empty library target.
//! - `paradex_testnet_acceptance` (`examples/paradex_testnet_acceptance/`): Paradex TESTNET only,
//!   FBC-8xr's acceptance run (FBC-xji8). testnet_trade's flags and helpers, reused by path;
//!   one run each of place, amend, cancel, batch create, cancel-many, an IOC expiry and
//!   cancel-on-disconnect on a dropped socket and on a graceful close, each through an fbc-oms
//!   authorization; after every step GET /orders and GET /positions compared with the order
//!   stream through its own authenticated REST reads (`src/auth_rest.rs`, a review path). It
//!   writes an evidence report, its machine-readable form, its journal and a scrubbed fixture
//!   slice. The crate's `README.md` says how the owner runs it.
//! - `testnet_quote` (`examples/testnet_quote/`, FBC-elg7): Paradex TESTNET only, a PLUMBING
//!   TEST, NOT A STRATEGY. Keeps one post-only order of a fixed size on each side a fixed number
//!   of basis points behind the touch and requotes them through fbc-oms's `ExecutionPlanner`
//!   (cancel-and-replace only, the caps checked on every place), cancelling every order of ours
//!   still open after each reconnect before it places, until Ctrl-C, `--duration` or
//!   `--cycles`; then Stop and a summary. With `--mainnet` (FBC-tyft), the same loop on Paradex
//!   MAINNET for a short supervised session behind `--mainnet` and `--sole-trader`: mainnet's
//!   guard (`src/mainnet.rs`, shared with `mainnet_smoke`), its credentials
//!   (`src/auth_mainnet.rs`), the operator's own positive caps (required, no default, no
//!   ceiling), offsets of at least 20 bps, at most an hour, never seeded by hand.
//! - `mainnet_smoke` (`examples/mainnet_smoke/`, FBC-p0aa): Paradex MAINNET, the owner's first
//!   prod order: testnet_trade's round trip (its `trade.rs`, by path), never seeded by hand, behind
//!   `--mainnet` and `--sole-trader`, the operator's own positive caps (required, no default, no
//!   ceiling), mainnet's URLs and chain id (checked against `GET /system/config`), the order sized
//!   to the minimum notional at the mark price. Its credentials come from mainnet-only variables
//!   (`src/auth_mainnet.rs`, a review path). It Starts only after a trustworthy resync seeded the
//!   market (Paradex's snapshot source is trustworthy, decision 0094).
//! - `qfex_preprod_probe` (`examples/qfex_preprod_probe/`, FBC-l931): QFEX PRE-PRODUCTION only,
//!   stage one of decision 0132 item 7's probe. Before anything connects it refuses any origin
//!   but QFEX's pre-production ones (or, for a stub, all loopback), an account decision 0122
//!   refuses, missing credentials and a proxied loopback origin. It reads every account of the
//!   provider user through the adapter's `account_reads` and starts a session only when each
//!   holds no open order and a flat position; then one `ExecSession` per account, one after the
//!   other, arms cancel-on-disconnect once and is stopped at the arm's outcome with no second
//!   attempt. It places nothing. It writes a report, its evidence, its journal and a scrubbed
//!   fixture slice of what answered the arm. Its credentials come from the environment through
//!   `src/auth_qfex.rs` (a review path), which also hands each signed read to the connector.
//!
//! The samples that trade share, by path as well, `src/link.rs` (the ExecHandler-to-Registry
//! glue) and `src/testnet.rs` (the testnet guard, the market's spec, the session's wiring, the
//! client-id high-water mark, the touch and mark reads); the mainnet ones `src/mainnet.rs` (the
//! mainnet guard); `testnet_trade` (and so `mainnet_smoke`) and `testnet_quote` also
//! share `src/positions.rs` (FBC-hpg6: `GET /positions` compared with the registry's position
//! after the first fill and at Stop, a mismatch the re-read confirms Stopping the run), which
//! reads through `src/auth_rest.rs`; none is a module of this library target.
