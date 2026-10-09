//! FBC-6oj: fbc-venue-paradex runs the named conformance suite.

mod common;
mod md;
#[path = "conformance/setup.rs"]
mod setup;

use fbc_venue_paradex::ParadexFactory;
use setup::assumed;

fbc_conformance::suite! {
    factory: ParadexFactory,
    fixtures: "../../../fixtures/paradex/conformance",
    setup: assumed,
}

/// The done line's "golden encodings whose signatures are the Java vectors'": each signed golden
/// file is the frame written by hand around the Java signer's r and s for its row of
/// `fixtures/paradex/signing/paradex-vectors.tsv`, and `signing_golden` holds the codec to
/// those bytes.
#[test]
fn each_signed_golden_carries_its_java_vectors_signature() {
    use fbc_venue_paradex::sign::Felt;
    let v = common::Vectors::read();
    let dir = std::path::Path::new(setup::FIXTURES).join("signing_golden");
    let sig = |row: &str| {
        let r = v.row(row);
        let (rr, ss) = (
            Felt::from_hex(r.get("r")).unwrap(),
            Felt::from_hex(r.get("s")).unwrap(),
        );
        serde_json::Value::from(format!(r#"["{rr}","{ss}"]"#)).to_string()
    };
    let rpc = |method: &str, params: String, id: u64| {
        format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{id}}}"#)
    };
    let files = [
        (
            "place",
            rpc(
                "order.create",
                format!(
                    r#"{{"client_id":"{}","market":"ETH-USD-PERP","side":"SELL","signature_timestamp":1759400000124,"size":"1.25","type":"LIMIT","price":"2450.7","instruction":"GTC","signature":{}}}"#,
                    setup::uuid(0),
                    sig("sell_limit")
                ),
                21,
            ),
        ),
        (
            "place-market",
            rpc(
                "order.create",
                format!(
                    r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"SELL","signature_timestamp":1759400000126,"size":"0.5","type":"MARKET","price":"0","instruction":"IOC","signature":{}}}"#,
                    setup::uuid(1),
                    sig("market_sell")
                ),
                22,
            ),
        ),
        (
            "place-batch",
            rpc(
                "order.create_batch",
                format!(
                    r#"{{"orders":[{{"client_id":"{}","market":"ETH-USD-PERP","side":"BUY","signature_timestamp":1759400000130,"size":"2.5","type":"LIMIT","price":"2450.7","instruction":"GTC","signature":{}}}]}}"#,
                    setup::uuid(2),
                    sig("edge_trailing_zeros")
                ),
                23,
            ),
        ),
        (
            "amend",
            rpc(
                "order.modify",
                format!(
                    r#"{{"id":"1759400000123201703010000","market":"BTC-USD-PERP","price":"65200.1","side":"SELL","signature":{},"signature_timestamp":1759400000127,"size":"0.02","type":"LIMIT"}}"#,
                    sig("modify")
                ),
                24,
            ),
        ),
        (
            "cancel",
            rpc(
                "order.cancel",
                format!(
                    r#"{{"client_id":"{}","market":"BTC-USD-PERP"}}"#,
                    setup::uuid(0)
                ),
                25,
            ),
        ),
        (
            "cancel-batch",
            rpc(
                "order.cancel_batch",
                r#"{"order_ids":["1759500000000000001","1759500000000000002"]}"#.to_owned(),
                26,
            ),
        ),
    ];
    for (name, frame) in files {
        let golden = std::fs::read_to_string(dir.join(format!("{name}.golden"))).unwrap();
        assert_eq!(golden, frame, "{name}.golden");
    }
}
