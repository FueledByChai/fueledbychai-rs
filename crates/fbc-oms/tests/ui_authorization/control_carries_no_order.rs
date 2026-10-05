// The unauthorized control path carries no order-affecting command: it has no place, amend,
// cancel or cancel-all, and no conversion from a venue command.
use fbc_core::{AccountKey, CancelOrder, EncodeCtx, NewOrder, PathStamps, VenueCommand};
use fbc_oms::{ControlCommand, OrderGateway};

fn bypass(
    gateway: &mut impl OrderGateway,
    acct: AccountKey,
    order: NewOrder,
    cancel: CancelOrder,
    ctx: &EncodeCtx,
    t: &mut PathStamps<'_>,
) {
    gateway.submit_control(acct, ControlCommand::Place(order), ctx, t);
    gateway.submit_control(acct, ControlCommand::Cancel(cancel), ctx, t);
    gateway.submit_control(acct, VenueCommand::FeeQuery.into(), ctx, t);
}

fn main() {}
