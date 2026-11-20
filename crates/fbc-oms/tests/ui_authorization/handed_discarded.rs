// Decision 0152: a handed authorization's drop releases nothing, so only what the gateway reports
// of it settles its orders; a gateway that discards one unreported is warned, here denied.
#![deny(unused_must_use)]
use fbc_oms::Authorization;

fn discarded(auth: Authorization) {
    auth.hand_over();
}

fn main() {}
