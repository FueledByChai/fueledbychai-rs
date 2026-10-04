// Decision 0018: every frame and HTTP request carries its rate charge, so the runtime can charge
// what a codec sends on its own (a resync, a pong, a token refresh) to the right limit. A frame
// or request asked for without one does not compile.
use core::time::Duration;

use fbc_core::{Effect, HttpMethod, HttpRequest, HttpTag, StreamId, TrafficClass, WireSlice, WireUrl};

fn main() {
    let _send = Effect::Send {
        stream: StreamId(0),
        frame: WireSlice::plain(Vec::new()),
        rpc: None,
        class: TrafficClass::Safety,
    };
    let _http = Effect::Http {
        tag: HttpTag(1),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(""),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: Duration::from_secs(1),
        class: TrafficClass::Normal,
    };
}
