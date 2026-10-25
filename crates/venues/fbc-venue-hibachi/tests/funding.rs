//! FBC-jyyl's done line, the funding half: the crate's settlement interval is one hour, and the
//! hand-built SYNTHETIC funding-rates fixture, in the shape of Hibachi's settled history
//! (`GET /market/data/funding-rates`: `{"data": [{contractId, fundingTimestamp, fundingRate,
//! indexPrice}, ...]}`, `fundingTimestamp` in epoch seconds), holds consecutive settlements each
//! on the UTC hour and spaced by that interval. Its order is not relied on: the owner's read of
//! the live history found its first page not ending at the latest settlement.

use std::time::Duration;

use fbc_venue_hibachi::FUNDING_SETTLEMENT_INTERVAL;
use serde_json::Value;

const FUNDING_RATES: &str = include_str!("../../../../fixtures/hibachi/rest/funding_rates.json");

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

#[test]
fn the_settlement_interval_is_one_hour() {
    assert_eq!(FUNDING_SETTLEMENT_INTERVAL, Duration::from_secs(3_600));
}

#[test]
fn consecutive_settlements_are_spaced_by_the_settlement_interval() {
    let times = settlements();
    assert!(times.len() >= 2, "at least one spacing to derive");
    let interval = FUNDING_SETTLEMENT_INTERVAL.as_secs();
    for pair in times.windows(2) {
        assert_eq!(pair[1] - pair[0], interval, "{pair:?}");
    }
    // Each on the UTC hour, in epoch seconds (a count of milliseconds would not be).
    for t in &times {
        assert_eq!(t % 3_600, 0, "{t}");
        assert!(*t < 4_102_444_800, "{t} reads as seconds before 2100");
    }
}
