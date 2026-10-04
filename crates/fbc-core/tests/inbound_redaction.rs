//! FBC-7lm's done line, fbc-core's half (decision 0028): a toy venue marks the token span of a
//! synthetic authentication response body and its `Set-Cookie` header, and a key a frame
//! echoes; the spans pass `InboundSpans::check`; and the same input with those spans blanked
//! gives the toy the same spans and decodes to the same events and effects, which is what lets
//! replay compare modulo spans. fbc-journal's `inbound_redaction` test journals the same
//! responses.
//!
//! Every credential is synthetic and assembled at run time, so no credential-shaped literal
//! sits in the source.

mod auth_toy;

use core::ops::Range;

use auth_toy::{AUTH_TAG, AuthToy, EXEC_STREAM, REFRESH_TAG, decode_frame, decode_http};
use fbc_core::{
    ConnState, Effect, ExecCodec, ExecEvent, HeaderMark, HttpFailure, HttpResponse, HttpTag,
    Inbound, InboundSpans, RawFrame,
};

fn secret(what: &str) -> String {
    format!("SYNTH{}{}SECRET", what.to_uppercase(), "-i7b")
}

/// The byte fbc-journal reads a redaction span back as (its `BLANK`).
const BLANK: u8 = b'2';

/// `bytes` with every span filled with the journal's blank byte.
fn blanked(bytes: &[u8], spans: &[Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for s in spans {
        out[s.start as usize..s.end as usize].fill(BLANK);
    }
    out
}

fn blank_text(len: usize) -> String {
    char::from(BLANK).to_string().repeat(len)
}

#[test]
fn the_toy_marks_the_token_and_set_cookie_of_an_auth_response_and_decodes_it_blanked() {
    let token = secret("token");
    let body = format!("auth|token={token}|refresh=300");
    let cookie = format!("sid={}; Path=/", secret("cookie"));
    let headers = [
        ("Content-Type", "text/plain"),
        ("Set-Cookie", cookie.as_str()),
        ("X-Request-Id", "req-1"),
    ];
    let resp = HttpResponse {
        status: 200,
        headers: &headers,
        body: body.as_bytes(),
    };
    let input = Inbound::Http(AUTH_TAG, resp);
    let spans = AuthToy.redact_inbound(input);
    assert_eq!(spans.headers(), [(1, HeaderMark::Value)]);
    let [span] = spans.body() else {
        panic!("{spans:?}")
    };
    assert_eq!(
        &body.as_bytes()[span.start as usize..span.end as usize],
        token.as_bytes()
    );
    spans.check(input).unwrap();

    // The same response with the spans blanked, as the journal reads it back.
    let body_blank = blanked(body.as_bytes(), spans.body());
    let cookie_blank = blank_text(cookie.len());
    let headers_blank = [
        headers[0],
        ("Set-Cookie", cookie_blank.as_str()),
        headers[2],
    ];
    let resp_blank = HttpResponse {
        status: 200,
        headers: &headers_blank,
        body: &body_blank,
    };
    assert!(!String::from_utf8_lossy(&body_blank).contains(&token));
    assert_eq!(
        AuthToy.redact_inbound(Inbound::Http(AUTH_TAG, resp_blank)),
        spans
    );

    let live = decode_http(AUTH_TAG, Ok(resp));
    assert_eq!(live.result, Ok(()));
    assert_eq!(
        live.events,
        [ExecEvent::Conn {
            stream: EXEC_STREAM,
            state: ConnState::Authenticated
        }]
    );
    assert!(matches!(
        live.effects[..],
        [Effect::Timer { tag: REFRESH_TAG, after }] if after.as_secs() == 300
    ));
    assert_eq!(decode_http(AUTH_TAG, Ok(resp_blank)), live);
}

#[test]
fn the_toy_marks_a_key_a_frame_echoes_and_a_header_naming_one() {
    let key = secret("key");
    let frame = format!("hello|key={key}|v=1");
    let input = Inbound::Frame(RawFrame::Text(&frame));
    let spans = AuthToy.redact_inbound(input);
    assert_eq!(spans.headers(), []);
    let at = 10..10 + key.len() as u32;
    assert_eq!(spans.body(), std::slice::from_ref(&at));
    spans.check(input).unwrap();
    let frame_blank = String::from_utf8(blanked(frame.as_bytes(), spans.body())).unwrap();
    let blank_input = Inbound::Frame(RawFrame::Text(&frame_blank));
    assert_eq!(AuthToy.redact_inbound(blank_input), spans);
    let live = decode_frame(RawFrame::Text(&frame));
    assert_eq!(
        live.events,
        [ExecEvent::Conn {
            stream: EXEC_STREAM,
            state: ConnState::Open
        }]
    );
    assert_eq!(decode_frame(RawFrame::Text(&frame_blank)), live);

    // A header whose name echoes a key is marked whole; a response to another request, and a
    // frame with no key, hold nothing.
    let name = format!("X-Echo-{key}");
    let headers = [(name.as_str(), "1")];
    let resp = HttpResponse {
        status: 200,
        headers: &headers,
        body: b"auth|refresh=1",
    };
    let spans = AuthToy.redact_inbound(Inbound::Http(AUTH_TAG, resp));
    assert_eq!(
        spans,
        InboundSpans::response(vec![(0, HeaderMark::NameAndValue)], Vec::new())
    );
    assert!(
        AuthToy
            .redact_inbound(Inbound::Http(HttpTag(9), resp))
            .is_empty()
    );
    let plain = Inbound::Frame(RawFrame::Text("hello|v=1"));
    assert_eq!(AuthToy.redact_inbound(plain), InboundSpans::NONE);
}

#[test]
fn the_toy_refuses_what_it_cannot_decode() {
    let ok = |status, body: &'static [u8]| HttpResponse {
        status,
        headers: &[],
        body,
    };
    let refused = |tag, resp| decode_http(tag, Ok(resp)).result.unwrap_err().to_string();
    assert!(refused(AUTH_TAG, ok(500, b"auth|token=t|refresh=1")).contains("status"));
    assert!(refused(HttpTag(9), ok(200, b"auth|token=t|refresh=1")).contains("tag"));
    assert!(refused(AUTH_TAG, ok(200, b"auth|refresh=1")).contains("token"));
    assert!(refused(AUTH_TAG, ok(200, b"auth|token=t|refresh=x")).contains("refresh"));
    assert!(refused(AUTH_TAG, ok(200, b"bye|token=t|refresh=1")).contains("kind"));
    assert!(refused(AUTH_TAG, ok(200, &[0xff])).contains("body"));
    // A login that got no response reconnects.
    let lost = decode_http(AUTH_TAG, Err(HttpFailure::Lost));
    assert_eq!(lost.result, Ok(()));
    assert!(matches!(
        lost.effects[..],
        [Effect::Reconnect {
            stream: EXEC_STREAM,
            ..
        }]
    ));
    let bad = |f| decode_frame(f).result.unwrap_err().to_string();
    assert!(bad(RawFrame::Binary(&[0xff])).contains("text"));
    assert!(bad(RawFrame::Text("bye|key=k")).contains("kind"));
}
