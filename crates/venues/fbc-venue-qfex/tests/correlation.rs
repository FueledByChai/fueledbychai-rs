//! FBC-zeb0 (f): draft PR #196's reply correlation, kept as test code only.
//!
//! On the draft branch `src/exec/correlation.rs` sat outside the crate's module tree and was
//! compiled only where this test included it by path. It is test code here, and nothing in the
//! crate uses it, for two reasons:
//!
//! - No reply settles a request: each sent command ends `Unknown` at its deadline (FBC-blcw,
//!   decisions 0005 and 0132), so a correlation candidate decides nothing yet.
//! - Its command-echo evidence reads an echo's `params`, which the journal keeps only as spans
//!   (decision 0134 item 4). The codec may decide only from bytes the journal keeps in clear
//!   (item 5), and the last test here shows a candidate found live is gone from the journaled
//!   frame. A later ticket that matches replies must work from what is kept: an
//!   `order_response`'s identity, which no redaction touches.
//!
//! What it shows of QFEX's published `order_response`, `ack` and `err` shapes: an identity
//! narrows a reply to a pending request, at most one, and never proves that request caused it.

use fbc_core::ExecCodec;
use fbc_core::{Inbound, RawFrame, RpcId, Secret, Secrets, VenueConfig};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::QfexExec;
use serde_json::Value;

/// The draft's correlation, unchanged but for its comments.
mod correlation {
    use fbc_core::RpcId;
    use serde_json::Value;

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    pub enum Operation<'a> {
        Place { client_id: &'a str },
        Modify { venue_id: &'a str },
        Cancel { venue_id: &'a str },
    }

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    pub struct Pending<'a> {
        pub rpc: RpcId,
        pub symbol: &'a str,
        pub operation: Operation<'a>,
    }

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    pub enum Evidence {
        /// The order response repeated the exact symbol and order identity.
        OrderIdentity,
        /// An acknowledgement or error echoed the exact original command.
        CommandEcho,
    }

    #[derive(Clone, Eq, PartialEq, Debug)]
    pub enum Correlation {
        None,
        Ambiguous,
        Candidate {
            rpc: RpcId,
            evidence: Evidence,
            status: Option<String>,
        },
    }

    /// At most one pending request a reply names, never guessed from the order replies come in.
    /// Malformed JSON is refused; a frame that names no request matches none.
    pub fn correlate(frame: &[u8], pending: &[Pending<'_>]) -> Result<Correlation, &'static str> {
        let value: Value = serde_json::from_slice(frame).map_err(|_| "malformed QFEX JSON")?;
        let (body, evidence, status) = if let Some(order) = value.get("order_response") {
            (
                order,
                Evidence::OrderIdentity,
                order.get("status").and_then(Value::as_str),
            )
        } else if let Some(command) = value
            .get("ack")
            .or_else(|| value.get("err"))
            .and_then(|v| v.get("incoming_message"))
        {
            (command, Evidence::CommandEcho, None)
        } else {
            return Ok(Correlation::None);
        };
        let mut found = None;
        for request in pending {
            if matches(body, evidence, status, request) {
                if found.is_some() {
                    return Ok(Correlation::Ambiguous);
                }
                found = Some(request.rpc);
            }
        }
        Ok(match found {
            Some(rpc) => Correlation::Candidate {
                rpc,
                evidence,
                status: status.map(str::to_owned),
            },
            None => Correlation::None,
        })
    }

    fn matches(
        body: &Value,
        evidence: Evidence,
        status: Option<&str>,
        request: &Pending<'_>,
    ) -> bool {
        let (params, kind) = if evidence == Evidence::CommandEcho {
            let Some(kind) = body.get("type").and_then(Value::as_str) else {
                return false;
            };
            let Some(params) = body.get("params") else {
                return false;
            };
            (params, Some(kind))
        } else {
            (body, None)
        };
        if params.get("symbol").and_then(Value::as_str) != Some(request.symbol) {
            return false;
        }
        match request.operation {
            Operation::Place { client_id } => {
                // A live order update may repeat a place's client id without the place causing
                // it, as an unsolicited MODIFIED does when QFEX converts a resting order to
                // reduce-only: only an ACK or an exact add_order echo is a candidate.
                (kind == Some("add_order") || (kind.is_none() && status == Some("ACK")))
                    && params.get("client_order_id").and_then(Value::as_str) == Some(client_id)
            }
            Operation::Modify { venue_id } => {
                // A modify replaces the order's UUID, and its client id may be empty: only an
                // exact echo names the old target.
                evidence == Evidence::CommandEcho
                    && kind == Some("modify_order")
                    && params.get("order_id").and_then(Value::as_str) == Some(venue_id)
            }
            Operation::Cancel { venue_id } => {
                ((kind == Some("cancel_order")
                    && params.get("cancel_order_id_type").and_then(Value::as_str)
                        == Some("order_id"))
                    || status.is_some_and(|status| {
                        matches!(status, "CANCELLED" | "CANCELLED_STP" | "NO_SUCH_ORDER")
                    }))
                    && params.get("order_id").and_then(Value::as_str) == Some(venue_id)
            }
        }
    }
}

use correlation::{Correlation, Evidence, Operation, Pending, correlate};

const CID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const ORDER: &str = "11111111-1111-4111-8111-111111111111";

fn place(rpc: u64, client_id: &'static str) -> Pending<'static> {
    Pending {
        rpc: RpcId(rpc),
        symbol: "AAPL-USD",
        operation: Operation::Place { client_id },
    }
}

#[test]
fn an_exact_identity_is_a_candidate_only() {
    let pending = [place(7, "synthetic-1")];
    let ack = format!(
        r#"{{"order_response":{{"symbol":"AAPL-USD","client_order_id":"synthetic-1","order_id":"{ORDER}","status":"ACK"}}}}"#
    );
    assert_eq!(
        correlate(ack.as_bytes(), &pending).unwrap(),
        Correlation::Candidate {
            rpc: RpcId(7),
            evidence: Evidence::OrderIdentity,
            status: Some("ACK".to_owned()),
        }
    );
    let anonymous = br#"{"order_response":{"symbol":"AAPL-USD","status":"ACK"}}"#;
    assert_eq!(correlate(anonymous, &pending).unwrap(), Correlation::None);
    // An ACK with an empty client id, as QFEX's add_order example answers, settles no place.
    let empty = br#"{"order_response":{"symbol":"AAPL-USD","order_id":"22222222-2222-4222-8222-222222222222","status":"ACK","client_order_id":""}}"#;
    assert_eq!(
        correlate(empty, &[place(8, "synthetic-8")]).unwrap(),
        Correlation::None
    );
}

#[test]
fn an_unsolicited_change_is_no_place_candidate_and_an_exact_echo_is() {
    let pending = [place(9, CID)];
    // The order responses page's lifecycle example: QFEX converting a resting order.
    let conversion = format!(
        r#"{{"order_response":{{"order_id":"{ORDER}","client_order_id":"{CID}","symbol":"AAPL-USD","status":"MODIFIED","order_origin":"SET_REDUCE_ONLY_ALL","reduce_only":true}}}}"#
    );
    assert_eq!(
        correlate(conversion.as_bytes(), &pending).unwrap(),
        Correlation::None
    );
    let cancelled = format!(
        r#"{{"order_response":{{"symbol":"AAPL-USD","client_order_id":"{CID}","status":"CANCELLED"}}}}"#
    );
    assert_eq!(
        correlate(cancelled.as_bytes(), &pending).unwrap(),
        Correlation::None
    );
    let echo = format!(
        r#"{{"ack":{{"incoming_message":{{"type":"add_order","params":{{"symbol":"AAPL-USD","client_order_id":"{CID}"}}}}}}}}"#
    );
    assert_eq!(
        correlate(echo.as_bytes(), &pending).unwrap(),
        Correlation::Candidate {
            rpc: RpcId(9),
            evidence: Evidence::CommandEcho,
            status: None,
        }
    );
}

#[test]
fn a_repeated_client_id_is_ambiguous_and_a_cancel_needs_the_venue_uuid() {
    let twice = [place(1, "synthetic-1"), place(2, "synthetic-1")];
    let ack = br#"{"order_response":{"symbol":"AAPL-USD","client_order_id":"synthetic-1","status":"ACK"}}"#;
    assert_eq!(correlate(ack, &twice).unwrap(), Correlation::Ambiguous);
    let cancel = [Pending {
        rpc: RpcId(3),
        symbol: "AAPL-USD",
        operation: Operation::Cancel { venue_id: ORDER },
    }];
    let by_client = br#"{"order_response":{"symbol":"AAPL-USD","client_order_id":"synthetic-1","status":"CANCELLED"}}"#;
    assert_eq!(correlate(by_client, &cancel).unwrap(), Correlation::None);
    let exact = format!(
        r#"{{"order_response":{{"symbol":"AAPL-USD","order_id":"{ORDER}","status":"CANCELLED"}}}}"#
    );
    assert!(matches!(
        correlate(exact.as_bytes(), &cancel).unwrap(),
        Correlation::Candidate { rpc: RpcId(3), .. }
    ));
    let echoed = format!(
        r#"{{"err":{{"error_code":"InvalidOrderId","incoming_message":{{"type":"cancel_order","params":{{"symbol":"AAPL-USD","cancel_order_id_type":"order_id","order_id":"{ORDER}"}}}}}}}}"#
    );
    assert!(matches!(
        correlate(echoed.as_bytes(), &cancel).unwrap(),
        Correlation::Candidate {
            evidence: Evidence::CommandEcho,
            ..
        }
    ));
}

#[test]
fn a_command_echo_needs_its_type_symbol_and_target() {
    let pending = [Pending {
        rpc: RpcId(4),
        symbol: "AAPL-USD",
        operation: Operation::Modify { venue_id: ORDER },
    }];
    let echo = format!(
        r#"{{"err":{{"error_code":"InvalidOrder","incoming_message":{{"type":"modify_order","params":{{"symbol":"AAPL-USD","order_id":"{ORDER}"}}}}}}}}"#
    );
    assert!(matches!(
        correlate(echo.as_bytes(), &pending).unwrap(),
        Correlation::Candidate {
            evidence: Evidence::CommandEcho,
            ..
        }
    ));
    for unmatched in [
        r#"{"err":{"error_code":"InvalidOrder","incoming_message":"modify_order"}}"#.to_owned(),
        r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"type":"modify_order"}}}"#
            .to_owned(),
        r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"params":{}}}}"#.to_owned(),
        r#"{"subscribed":"fills"}"#.to_owned(),
        // A modify reports the replacement's UUID, never the old target.
        r#"{"order_response":{"symbol":"AAPL-USD","order_id":"22222222-2222-4222-8222-222222222222","client_order_id":"","status":"MODIFIED"}}"#.to_owned(),
        // Even the old id in an order's status is order truth, not a command's echo.
        format!(r#"{{"order_response":{{"symbol":"AAPL-USD","order_id":"{ORDER}","status":"MODIFIED"}}}}"#),
        format!(r#"{{"order_response":{{"symbol":"NVDA-USD","order_id":"{ORDER}","status":"MODIFIED"}}}}"#),
    ] {
        assert_eq!(correlate(unmatched.as_bytes(), &pending).unwrap(), Correlation::None, "{unmatched}");
    }
    assert!(correlate(b"not json", &pending).is_err());
}

/// Why this stays test code: what the journal keeps of an echoing ack or err names no request,
/// so a candidate found live would be gone in replay (decision 0134 items 4 and 5).
#[test]
fn an_echo_candidate_found_live_is_gone_from_the_journaled_frame() {
    let mut cfg = VenueConfig::new();
    cfg.insert(auth::ACCOUNT_ID, "primary");
    cfg.insert(auth::ACCOUNT_KEY, "4");
    cfg.insert(auth::PUBLIC_KEY, "qfex_pub_SYNTHETIC_0001");
    let mut secrets = Secrets::new();
    secrets.insert(
        auth::PUBLIC_KEY,
        Secret::new("qfex_pub_SYNTHETIC_0001".to_owned()),
    );
    secrets.insert(
        auth::SECRET_KEY,
        Secret::new("synthetic-qfex-secret-0001".to_owned()),
    );
    let codec = QfexExec::new(TradeAuth::new(&cfg, secrets).unwrap());
    let pending = [place(9, CID)];
    let echo = format!(
        r#"{{"ack":{{"incoming_message":{{"type":"add_order","params":{{"symbol":"AAPL-USD","client_order_id":"{CID}"}}}}}}}}"#
    );
    assert!(matches!(
        correlate(echo.as_bytes(), &pending).unwrap(),
        Correlation::Candidate { .. }
    ));
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(&echo)));
    let mut kept = echo.clone().into_bytes();
    for span in spans.body() {
        kept[span.start as usize..span.end as usize].fill(b'2');
    }
    let replayed: Value = serde_json::from_slice(&kept).unwrap();
    assert_eq!(replayed["ack"]["incoming_message"]["type"], "add_order");
    assert_eq!(correlate(&kept, &pending).unwrap(), Correlation::None);
    // An order's identity is kept, so a candidate from it survives the journal.
    let ack = format!(
        r#"{{"order_response":{{"symbol":"AAPL-USD","client_order_id":"{CID}","order_id":"{ORDER}","status":"ACK"}}}}"#
    );
    assert!(
        codec
            .redact_inbound(Inbound::Frame(RawFrame::Text(&ack)))
            .is_empty()
    );
    assert!(matches!(
        correlate(ack.as_bytes(), &pending).unwrap(),
        Correlation::Candidate {
            evidence: Evidence::OrderIdentity,
            ..
        }
    ));
}
