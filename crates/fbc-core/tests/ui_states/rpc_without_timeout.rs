// An order-entry request always has a deadline: an RPC without a timeout, and an HTTP request
// without one, do not compile, so the OMS never waits forever to move an order to Unknown.
use fbc_core::{Effect, HttpMethod, HttpRequest, HttpTag, OpKind, RateCharge, RpcCall, RpcId, StreamId, TrafficClass, WireSlice, WireUrl};

fn main() {
    let _send = Effect::Send {
        stream: StreamId(0),
        frame: WireSlice::plain(Vec::new()),
        rpc: Some(RpcCall { id: RpcId(1) }),
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Place, None),
    };
    let _http = Effect::Http {
        tag: HttpTag(1),
        req: HttpRequest {
            method: HttpMethod::Post,
            url: WireUrl::plain(""),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: Some(RpcId(1)),
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Place, None),
    };
}
