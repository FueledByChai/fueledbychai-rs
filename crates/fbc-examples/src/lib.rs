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
//!   `--cycles`; then Stop and a summary.
//!
//! The samples that trade share, by path as well, `src/link.rs` (the ExecHandler-to-Registry
//! glue) and `src/testnet.rs` (the testnet guard, the market's spec, the session's wiring, the
//! client-id high-water mark); neither is a module of this library target.
