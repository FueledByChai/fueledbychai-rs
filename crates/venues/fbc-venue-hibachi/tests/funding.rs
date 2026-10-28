//! FBC-jyyl's done line, the funding half, and FBC-204c's discovery half.
//!
//! FBC-jyyl: the crate's settlement interval is one hour, and the hand-built SYNTHETIC
//! funding-rates fixture, in the shape of Hibachi's settled history
//! (`GET /market/data/funding-rates`: `{"data": [{contractId, fundingTimestamp, fundingRate,
//! indexPrice}, ...]}`, `fundingTimestamp` in epoch seconds), holds consecutive settlements each
//! on the UTC hour and spaced by that interval. Its order is not relied on: the owner's read of
//! the live history found its first page not ending at the latest settlement.
//!
//! FBC-204c, on the owner's conclusion of 2026-10-28 (decision 0131): a settled `fundingRate` is
//! a rate per one-hour settlement and the streamed estimate an eight-hour rate, so discovery
//! states every perpetual's funding `Known` at the interval the settled history is spaced by,
//! capped per interval at the docs' -10% to 10% read for eight hours and rescaled to one: 125
//! bps. The codec's half, the estimate's eighth per hour, is in `tests/md_topics.rs`.

mod common;

use std::time::Duration;

use fbc_core::{Bps, FundingSpec, VenueFactory, dispatch_market_data};
use fbc_venue_hibachi::discover::decode_exchange_info;
use fbc_venue_hibachi::{
    FUNDING_ESTIMATE_PERIOD, FUNDING_RATE_CAP, FUNDING_SETTLEMENT_INTERVAL, Hibachi,
};
use serde_json::Value;

const FUNDING_RATES: &str = include_str!("../../../../fixtures/hibachi/rest/funding_rates.json");
const EXCHANGE_INFO: &str = include_str!("../../../../fixtures/hibachi/rest/exchange_info.json");

/// Each settlement's `fundingTimestamp`, in epoch seconds, sorted.
fn settlements() -> Vec<u64> {
    let doc: Value = serde_json::from_str(FUNDING_RATES).unwrap();
    let mut times: Vec<u64> = doc["data"]
        .as_array()
        .expect("a data list")
        .iter()
        .map(|record| {
            // Each record carries the four fields Hibachi's history is read for.
            for field in ["contractId", "fundingRate", "indexPrice"] {
                assert!(!record[field].is_null(), "{field}");
            }
            record["fundingTimestamp"]
                .as_u64()
                .expect("epoch seconds as a whole number")
        })
        .collect();
    times.sort_unstable();
    times
}

/// The one spacing of the settled history, in seconds.
fn spacing() -> u64 {
    let times = settlements();
    let gaps: Vec<u64> = times.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert!(!gaps.is_empty(), "at least one spacing to derive");
    assert!(gaps.iter().all(|gap| *gap == gaps[0]), "{gaps:?}");
    gaps[0]
}

#[test]
fn the_settlement_interval_is_one_hour() {
    assert_eq!(FUNDING_SETTLEMENT_INTERVAL, Duration::from_secs(3_600));
}

#[test]
fn consecutive_settlements_are_spaced_by_the_settlement_interval() {
    assert_eq!(spacing(), FUNDING_SETTLEMENT_INTERVAL.as_secs());
    // Each on the UTC hour, in epoch seconds (a count of milliseconds would not be).
    for t in &settlements() {
        assert_eq!(t % 3_600, 0, "{t}");
        assert!(*t < 4_102_444_800, "{t} reads as seconds before 2100");
    }
}

#[test]
fn the_streamed_estimate_is_a_rate_for_eight_settlement_intervals() {
    // The owner's conclusion: the estimate's hourly equivalent is the estimate divided by 8.
    assert_eq!(FUNDING_ESTIMATE_PERIOD, Duration::from_secs(8 * 3_600));
    assert_eq!(FUNDING_ESTIMATE_PERIOD, FUNDING_SETTLEMENT_INTERVAL * 8);
}

#[test]
fn the_cap_per_interval_is_the_docs_ten_percent_per_eight_hours_rescaled_to_one_hour() {
    // 10% is 1000 bps, stated for the eight-hour period; per one-hour interval, an eighth.
    let ten_percent_bps = 1_000.0;
    let intervals = FUNDING_ESTIMATE_PERIOD.as_secs() / FUNDING_SETTLEMENT_INTERVAL.as_secs();
    assert_eq!(intervals, 8);
    assert_eq!(FUNDING_RATE_CAP, Bps(125.0));
    assert_eq!(FUNDING_RATE_CAP.0 * intervals as f64, ten_percent_bps);
}

#[test]
fn discovery_states_funding_known_at_the_settled_history_s_spacing_with_the_cap_per_interval() {
    let caps = Hibachi.caps(&common::config()).unwrap();
    let drafts = dispatch_market_data(&caps, |scope| {
        decode_exchange_info(EXCHANGE_INFO.as_bytes(), scope)
    })
    .unwrap();
    assert_eq!(drafts.len(), 11);
    let known = FundingSpec::Known {
        interval: Duration::from_secs(spacing()),
        cap: Some(Bps(125.0)),
    };
    for draft in &drafts {
        assert_eq!(draft.funding, known, "{}", draft.venue_symbol.as_wire());
    }
}
