//! The gateway traits (design §4.8, as decision 0045 refines it): what submits commands to a
//! venue, whether the live gateway (runtime, exec codec and signer), the simulated venue, or a
//! [`ManagedGateway`] for a venue reachable only through a vendor SDK that owns its own socket
//! (journaled at the event level; using one needs a decision record first, 0002).
//!
//! An order-affecting command reaches a gateway only as an [`Authorization`] this crate issued
//! after its caps and the kill switch (0013 rule 2, 0012), which [`OrderGateway::submit`]
//! consumes. A command that affects no order (a query, a fee query, a dead-man refresh, turning
//! cancel-on-disconnect on) goes through [`OrderGateway::submit_control`] as a
//! [`ControlCommand`], which cannot carry an order-affecting one. The traits live here, not in
//! `fbc-core`, because `fbc-core` cannot name this crate's types; a gateway's crate depends on
//! `fbc-oms`, never the reverse.

use fbc_core::{AccountKey, EncodeCtx, PathStamps, QueryOrder, SubmitHandle, VenueCommand};

use crate::Authorization;

/// A command that affects no order, which a gateway takes without an authorization.
///
/// There is no variant for turning cancel-on-disconnect off: that removes the protection every
/// resting order relies on, and no record yet says who may ask for it (decision 0045).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ControlCommand {
    /// Query one order, as the Unknown ladder does (0005).
    Query(QueryOrder),
    /// Ask for the account's fee rates.
    FeeQuery,
    /// Refresh the venue's dead-man timer.
    RefreshDeadMan,
    /// Turn the venue's cancel-on-disconnect protection on.
    ArmCancelOnDisconnect,
}

impl ControlCommand {
    /// The venue command a codec encodes for it.
    pub fn into_command(self) -> VenueCommand {
        match self {
            ControlCommand::Query(query) => VenueCommand::Query(query),
            ControlCommand::FeeQuery => VenueCommand::FeeQuery,
            ControlCommand::RefreshDeadMan => VenueCommand::RefreshDeadMan,
            ControlCommand::ArmCancelOnDisconnect => VenueCommand::ArmCancelOnDisconnect(true),
        }
    }
}

/// Submits commands for accounts: the live gateway, the simulated venue, a managed gateway.
///
/// `t` carries the command's path marks (0034): a live gateway marks
/// [`PathStage::Encode`](fbc_core::PathStage::Encode) around its call to
/// [`ExecCodec::encode`](fbc_core::ExecCodec::encode), which it hands `t` to mark its signer
/// calls, and its runtime marks [`PathStage::Write`](fbc_core::PathStage::Write) around the
/// socket write.
pub trait OrderGateway {
    /// Submits the order-affecting command `fbc-oms` authorized, for the account and market the
    /// authorization names. The authorization is spent: it is never submitted twice.
    fn submit(
        &mut self,
        auth: Authorization,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> SubmitHandle;

    /// Submits a command that affects no order.
    fn submit_control(
        &mut self,
        acct: AccountKey,
        cmd: ControlCommand,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> SubmitHandle;
}

/// A gateway for a venue reachable only through a vendor SDK that owns its own socket. It emits
/// the same execution events and is journaled at the event level, so its replay is event-level,
/// not frame-level. Using one needs its own decision record first (0002).
pub trait ManagedGateway: OrderGateway + Send {}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{InstrumentId, OrderRef};

    use crate::common;

    #[test]
    fn a_control_command_encodes_as_the_venue_command_it_names() {
        let query = QueryOrder {
            target: OrderRef::Client(common::cid()),
            inst: InstrumentId::new(1),
            placement_nonce: Some(4),
        };
        let cases = [
            (
                ControlCommand::Query(query.clone()),
                VenueCommand::Query(query),
            ),
            (ControlCommand::FeeQuery, VenueCommand::FeeQuery),
            (ControlCommand::RefreshDeadMan, VenueCommand::RefreshDeadMan),
            (
                ControlCommand::ArmCancelOnDisconnect,
                VenueCommand::ArmCancelOnDisconnect(true),
            ),
        ];
        for (control, venue) in cases {
            assert_eq!(control.into_command(), venue);
        }
    }
}
