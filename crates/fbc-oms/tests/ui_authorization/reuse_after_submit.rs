// Submit consumes the authorization, so it cannot be submitted again.
use fbc_core::{EncodeCtx, PathStamps};
use fbc_oms::{Authorization, OrderGateway};

fn resend(
    gateway: &mut impl OrderGateway,
    auth: Authorization,
    ctx: &EncodeCtx,
    t: &mut PathStamps<'_>,
) {
    gateway.submit(auth, ctx, t);
    gateway.submit(auth, ctx, t);
}

fn main() {}
