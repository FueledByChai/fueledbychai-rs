use std::time::Duration;

use fbc_core::{
    AckModel, CancelOnDisconnect, Channel, ClientIdFormat, NonceScope, OrderCaps, OrderKindTag,
    OrderingKey, SnapshotSource, Support, TagSet, TifTag,
};

fn main() {
    // Order capabilities that do not say whether the resync reads the open orders and the
    // positions at one instant do not compile: the seed's shown-cumulative-fill rule is never
    // assumed sound (decision 0095, FBC-k7t7).
    let _ = OrderCaps {
        kinds: TagSet::of(&[OrderKindTag::Limit]),
        tifs: TagSet::of(&[TifTag::Gtc]),
        channels: TagSet::of(&[Channel::Public]),
        post_only: true,
        reduce_only: true,
        flag_conflicts: Vec::new(),
        amend: None,
        cancel_refs: TagSet::none(),
        query_refs: TagSet::none(),
        cancel_before_ack: false,
        cancel_is_signed: false,
        batch_place: None,
        batch_cancel: None,
        cancel_all_account: Support::Unsupported,
        cancel_all_instrument: Support::Unsupported,
        cancel_on_disconnect: CancelOnDisconnect::DeadMan {
            max_ttl: Duration::from_secs(10),
        },
        ack: AckModel::SinglePhase,
        client_id: ClientIdFormat::Uuid,
        cid_echoed_on_events: true,
        nonce_scope: NonceScope::None,
        ordering_key: OrderingKey::VenueSeq,
        snapshot_source: SnapshotSource::Trustworthy,
        snapshot_lag_bound: Duration::ZERO,
        events_echo_flags: true,
        sign_cost_hint_us: 0,
    };
}
