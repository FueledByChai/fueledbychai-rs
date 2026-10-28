//! Conservative correlation candidates for QFEX trade replies.
//!
//! QFEX's published `order_response`, `ack`, and `err` envelopes have no
//! universal request id. An order identity can narrow a response to a pending
//! request, but an unsolicited order update may have the same identity. Thus a
//! `Candidate` is evidence for a later state machine, not an accepted outcome.

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

/// Identify at most one pending request without guessing from response order.
/// Malformed JSON is refused; unrecognised or unidentifiable frames match none.
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
    let mut match_found = None;
    for request in pending {
        if matches_request(body, evidence, status, request) {
            if match_found.is_some() {
                return Ok(Correlation::Ambiguous);
            }
            match_found = Some(request.rpc);
        }
    }
    Ok(match match_found {
        Some(rpc) => Correlation::Candidate {
            rpc,
            evidence,
            status: status.map(str::to_owned),
        },
        None => Correlation::None,
    })
}

fn matches_request(
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
            // A live order update may repeat the place's client id without being
            // caused by the place. In particular, QFEX can send an unsolicited
            // MODIFIED update when it converts a resting order to reduce-only.
            // Only an ACK or an exact add_order command echo is a candidate.
            (kind == Some("add_order") || (kind.is_none() && status == Some("ACK")))
                && params.get("client_order_id").and_then(Value::as_str) == Some(client_id)
        }
        Operation::Modify { venue_id } => {
            // A successful modify replaces the old UUID with a new order_id, and its
            // client_order_id may be empty. Only an exact echo names the old target.
            evidence == Evidence::CommandEcho
                && kind == Some("modify_order")
                && params.get("order_id").and_then(Value::as_str) == Some(venue_id)
        }
        Operation::Cancel { venue_id } => {
            ((kind == Some("cancel_order")
                && params.get("cancel_order_id_type").and_then(Value::as_str) == Some("order_id"))
                || status.is_some_and(|status| {
                    matches!(status, "CANCELLED" | "CANCELLED_STP" | "NO_SUCH_ORDER")
                }))
                && params.get("order_id").and_then(Value::as_str) == Some(venue_id)
        }
    }
}
