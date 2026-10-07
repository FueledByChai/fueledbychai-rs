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
