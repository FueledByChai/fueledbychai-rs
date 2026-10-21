#[path = "../src/exec/correlation.rs"]
mod correlation;

use correlation::{Correlation, Evidence, Operation, Pending, correlate};
use fbc_core::RpcId;

#[test]
fn exact_identity_yields_candidate_only() {
    let pending = [Pending {
        rpc: RpcId(7),
        symbol: "AAPL-USD",
        operation: Operation::Place {
            client_id: "synthetic-1",
        },
    }];
    let reply = include_bytes!("../../../../fixtures/qfex/private/order_ack.json");
    assert_eq!(
        correlate(reply, &pending).unwrap(),
        Correlation::Candidate {
            rpc: RpcId(7),
            evidence: Evidence::OrderIdentity,
            status: Some("ACK".to_owned()),
        }
    );
    assert_eq!(
        correlate(
            br#"{"order_response":{"symbol":"AAPL-USD","status":"ACK"}}"#,
            &pending
        )
        .unwrap(),
        Correlation::None
    );
}

#[test]
fn documented_style_ack_without_a_client_id_cannot_settle_a_place() {
    let pending = [Pending {
        rpc: RpcId(8),
        symbol: "AAPL-USD",
        operation: Operation::Place {
            client_id: "synthetic-8",
        },
    }];
    let reply =
        include_bytes!("../../../../fixtures/qfex/private/order_ack_without_client_id.json");
    assert_eq!(correlate(reply, &pending).unwrap(), Correlation::None);
}

#[test]
fn duplicate_client_id_is_ambiguous_and_cancel_needs_venue_uuid() {
    let duplicate = [
        Pending {
            rpc: RpcId(1),
            symbol: "AAPL-USD",
            operation: Operation::Place {
                client_id: "synthetic-1",
            },
        },
        Pending {
            rpc: RpcId(2),
            symbol: "AAPL-USD",
            operation: Operation::Place {
                client_id: "synthetic-1",
            },
        },
    ];
    let reply = br#"{"order_response":{"symbol":"AAPL-USD","client_order_id":"synthetic-1","status":"ACK"}}"#;
    assert_eq!(
        correlate(reply, &duplicate).unwrap(),
        Correlation::Ambiguous
    );
    let cancel = [Pending {
        rpc: RpcId(3),
        symbol: "AAPL-USD",
        operation: Operation::Cancel {
            venue_id: "11111111-1111-4111-8111-111111111111",
        },
    }];
    let by_client = br#"{"order_response":{"symbol":"AAPL-USD","client_order_id":"synthetic-1","status":"CANCELLED"}}"#;
    assert_eq!(correlate(by_client, &cancel).unwrap(), Correlation::None);
    let exact = br#"{"order_response":{"symbol":"AAPL-USD","order_id":"11111111-1111-4111-8111-111111111111","status":"CANCELLED"}}"#;
    assert!(matches!(
        correlate(exact, &cancel).unwrap(),
        Correlation::Candidate { rpc: RpcId(3), .. }
    ));
}

#[test]
fn command_echo_requires_type_symbol_and_target() {
    let pending = [Pending {
        rpc: RpcId(4),
        symbol: "AAPL-USD",
        operation: Operation::Modify {
            venue_id: "11111111-1111-4111-8111-111111111111",
        },
    }];
    let echo = br#"{"err":{"error_code":"InvalidOrder","incoming_message":{"type":"modify_order","params":{"symbol":"AAPL-USD","order_id":"11111111-1111-4111-8111-111111111111"}}}}"#;
    assert!(matches!(
        correlate(echo, &pending).unwrap(),
        Correlation::Candidate {
            evidence: Evidence::CommandEcho,
            ..
        }
    ));
    let text_echo = br#"{"err":{"error_code":"InvalidOrder","incoming_message":"modify_order"}}"#;
    assert_eq!(correlate(text_echo, &pending).unwrap(), Correlation::None);
    assert!(correlate(b"not json", &pending).is_err());
}
