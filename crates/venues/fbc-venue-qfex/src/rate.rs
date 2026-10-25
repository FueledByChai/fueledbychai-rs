//! Classification of QFEX trade WebSocket rate errors. The echo is untrusted and may contain
//! credentials; only its message type is used, and no echoed bytes enter diagnostics.

use std::time::Duration;

use fbc_core::{Effect, InboundSpans, OpKind, RateCharge, StreamId};
use serde_json::Value;

/// A provider refusal and its unambiguous operation, if known. This is budget feedback only:
/// it is never evidence that a sent order did or did not take effect.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct RateRefusal {
    /// The operation, only when its echoed type has one documented bucket.
    pub charge: Option<RateCharge>,
    /// The provider's parsed retry-after duration, when valid.
    pub retry_after: Option<Duration>,
}

/// Parse a QFEX rate refusal without retaining its echoed message.
pub fn refusal(value: &Value) -> Option<RateRefusal> {
    let fields = value.as_object()?;
    if fields.get("type")?.as_str()? != "Err"
        || fields.get("error_code")?.as_str()? != "RateLimited"
    {
        return None;
    }
    let operation = fields
        .get("incoming_message")
        .and_then(Value::as_object)
        .and_then(|echo| echo.get("type"))
        .and_then(Value::as_str)
        .and_then(|kind| match kind {
            "add_order" => Some(OpKind::Place),
            "modify_order" | "modify_stop_order" => Some(OpKind::Amend),
            "get_order"
            | "get_user_orders"
            | "get_user_trades"
            | "get_user_leverage"
            | "get_available_leverage_levels" => Some(OpKind::Query),
            "subscribe" | "unsubscribe" => Some(OpKind::Subscribe),
            "set_user_leverage" => Some(OpKind::Control),
            "cancel_order" | "cancel_stop_order" => Some(OpKind::Cancel),
            "cancel_all_orders" => Some(OpKind::CancelAll),
            // Published rate documentation does not name COD's bucket. Hold both.
            _ => None,
        });
    Some(RateRefusal {
        charge: operation.map(|op| RateCharge::one(op, None)),
        retry_after: fields
            .get("message")
            .and_then(Value::as_str)
            .and_then(parse_retry),
    })
}

/// Translate a rate refusal into budget feedback for its WebSocket stream. The effect carries
/// neither an RPC id nor the echoed request, so it cannot settle or resend a sent command.
pub fn effect(stream: StreamId, value: &Value) -> Option<Effect> {
    let parsed = refusal(value)?;
    Some(Effect::RateLimited {
        stream,
        charge: parsed.charge,
        retry_after: parsed.retry_after,
    })
}

fn parse_retry(message: &str) -> Option<Duration> {
    let tail = message.strip_prefix("Rate limit exceeded, retry after ")?;
    let number = tail.strip_suffix(" seconds")?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .map(Duration::from_secs)
}

/// Redact the entire inbound frame whenever a QFEX rate error may echo the original request.
/// Parsing catches escaped JSON strings; the byte check also catches malformed envelopes.
pub fn echoed_frame_spans(bytes: &[u8]) -> Option<InboundSpans> {
    let contains = |needle: &[u8]| bytes.windows(needle.len()).any(|part| part == needle);
    let parsed_echo = serde_json::from_slice::<Value>(bytes)
        .ok()
        .is_some_and(|value| refusal(&value).is_some() && value.get("incoming_message").is_some());
    if parsed_echo || contains(b"RateLimited") && contains(b"incoming_message") {
        let end = u32::try_from(bytes.len()).ok()?;
        return Some(InboundSpans::frame(std::iter::once(0..end).collect()));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{ConnKey, LimitScope, RateLimit, TagSet, TrafficClass, Via};
    use fbc_runtime::{RateLimiter, RateUserKey, Request, SafetyReserve};
    use serde_json::json;
    use tokio::time::Instant;

    #[test]
    fn classifies_only_exact_echoed_operations_and_preserves_hint() {
        let frame = json!({"type":"Err", "error_code":"RateLimited",
            "message":"Rate limit exceeded, retry after 7 seconds",
            "incoming_message":{"type":"cancel_order", "signature":"secret"}});
        let got = refusal(&frame).unwrap();
        assert_eq!(got.charge.unwrap().op, OpKind::Cancel);
        assert_eq!(got.retry_after, Some(Duration::from_secs(7)));
        assert!(matches!(
            effect(StreamId(0), &frame),
            Some(Effect::RateLimited {
                charge: Some(_),
                retry_after: Some(_),
                ..
            })
        ));
        for echo in [
            json!({}),
            json!({"type":"cancel_on_disconnect"}),
            json!({"type":"cancel_order,add_order"}),
        ] {
            let mut unknown = frame.clone();
            unknown["incoming_message"] = echo;
            assert!(refusal(&unknown).unwrap().charge.is_none());
        }
        assert!(refusal(&json!({"type":"Err", "error_code":"Other"})).is_none());
    }

    #[test]
    fn malformed_envelopes_and_hints_fail_closed() {
        for value in [
            json!(null),
            json!({"error_code":"RateLimited"}),
            json!({"type":"Err"}),
            json!({"type":"err", "error_code":"RateLimited"}),
        ] {
            assert!(effect(StreamId(0), &value).is_none());
        }
        for hint in [
            "Rate limit exceeded, retry after x seconds",
            "Rate limit exceeded, retry after 0 seconds",
            "Rate limit exceeded, retry after 2 second",
            "Rate limit exceeded, retry after  seconds",
        ] {
            let value = json!({"type":"Err", "error_code":"RateLimited",
                "message":hint, "incoming_message":{"type":"add_order"}});
            assert_eq!(refusal(&value).unwrap().retry_after, None);
        }
        assert!(echoed_frame_spans(br#"{"type":"Ok"}"#).is_none());
        let malformed = br#"{"error_code":"RateLimited","incoming_message":{"signature":"secret"}"#;
        assert_eq!(
            echoed_frame_spans(malformed).unwrap().body()[0],
            0..malformed.len() as u32
        );
    }

    #[test]
    fn known_echoed_types_select_only_their_declared_bucket() {
        for (kind, operation) in [
            ("add_order", OpKind::Place),
            ("modify_order", OpKind::Amend),
            ("modify_stop_order", OpKind::Amend),
            ("get_order", OpKind::Query),
            ("get_user_orders", OpKind::Query),
            ("get_user_trades", OpKind::Query),
            ("get_user_leverage", OpKind::Query),
            ("get_available_leverage_levels", OpKind::Query),
            ("subscribe", OpKind::Subscribe),
            ("unsubscribe", OpKind::Subscribe),
            ("set_user_leverage", OpKind::Control),
            ("cancel_order", OpKind::Cancel),
            ("cancel_stop_order", OpKind::Cancel),
            ("cancel_all_orders", OpKind::CancelAll),
        ] {
            let value = json!({"type":"Err", "error_code":"RateLimited",
                "incoming_message":{"type":kind}});
            assert_eq!(refusal(&value).unwrap().charge.unwrap().op, operation);
        }
    }

    #[test]
    fn echoed_credentials_are_whole_frame_redacted_without_diagnostics() {
        let raw = br#"{"type":"Err","error_code":"RateLimited","incoming_message":{"type":"auth","signature":"secret"}}"#;
        assert_eq!(
            echoed_frame_spans(raw).unwrap().body()[0],
            0..raw.len() as u32
        );
        let refusal = refusal(&serde_json::from_slice(raw).unwrap()).unwrap();
        assert!(refusal.charge.is_none());
        assert!(!format!("{refusal:?}").contains("secret"));
        let escaped = br#"{"type":"Err","error_code":"RateLimit\u0065d","incoming_message":{"signature":"secret"}}"#;
        assert_eq!(
            echoed_frame_spans(escaped).unwrap().body()[0],
            0..escaped.len() as u32
        );
    }

    #[test]
    fn synthetic_primary_and_subaccount_faults_share_general_and_cancel_holds() {
        let limits = [
            RateLimit {
                scope: LimitScope::User,
                ops: TagSet::of(&[OpKind::Place, OpKind::Query, OpKind::Subscribe]),
                per: Duration::from_secs(60),
                units: 10,
            },
            RateLimit {
                scope: LimitScope::User,
                ops: TagSet::of(&[OpKind::Cancel, OpKind::CancelAll]),
                per: Duration::from_secs(60),
                units: 10,
            },
        ];
        let reserve = SafetyReserve::percent(50).unwrap();
        let primary =
            RateLimiter::for_user(RateUserKey::new(0x7a00_1001), &limits, reserve).unwrap();
        let sub = RateLimiter::for_user(RateUserKey::new(0x7a00_1001), &limits, reserve).unwrap();
        let conn = ConnKey { conn: 1, epoch: 1 };
        let now = Instant::now();
        let request = |op, class| Request {
            charge: RateCharge::one(op, None),
            via: Via::Frame,
            class,
        };
        let place = request(OpKind::Place, TrafficClass::Normal);
        let cancel = request(OpKind::Cancel, TrafficClass::Safety);
        let fault = |kind: &str, hint: &str| {
            json!({"type":"Err", "error_code":"RateLimited",
            "message":hint, "incoming_message":{"type":kind}})
        };

        // A general fault seen by the subaccount gates the primary too. It does not spend
        // the independent cancel bucket or create a new send effect.
        let general = effect(
            StreamId(0),
            &fault("add_order", "Rate limit exceeded, retry after 4 seconds"),
        )
        .unwrap();
        let Effect::RateLimited {
            charge,
            retry_after,
            ..
        } = general
        else {
            panic!("feedback only")
        };
        sub.provider_rejected(now, conn, charge, retry_after);
        assert!(primary.charge(now, conn, &[place]).is_err());
        assert!(primary.charge(now, conn, &[cancel]).is_ok());
        assert!(
            primary
                .charge(now + Duration::from_secs(5), conn, &[place])
                .is_ok()
        );

        let cancel_fault = effect(
            StreamId(0),
            &fault("cancel_order", "Rate limit exceeded, retry after 6 seconds"),
        )
        .unwrap();
        let Effect::RateLimited {
            charge,
            retry_after,
            ..
        } = cancel_fault
        else {
            panic!("feedback only")
        };
        primary.provider_rejected(now + Duration::from_secs(5), conn, charge, retry_after);
        assert!(
            sub.charge(now + Duration::from_secs(5), conn, &[cancel])
                .is_err()
        );
        assert!(
            sub.charge(now + Duration::from_secs(5), conn, &[place])
                .is_ok()
        );

        // An echo without a trustworthy type gates both budgets, even Safety cancel traffic.
        let unknown = effect(StreamId(0), &fault("auth", "unparseable retry hint")).unwrap();
        let Effect::RateLimited {
            charge,
            retry_after,
            ..
        } = unknown
        else {
            panic!("feedback only")
        };
        assert!(charge.is_none() && retry_after.is_none());
        sub.provider_rejected(now + Duration::from_secs(12), conn, charge, retry_after);
        for req in [place, cancel] {
            assert!(
                primary
                    .charge(now + Duration::from_secs(13), conn, &[req])
                    .is_err()
            );
        }
    }
}
