//! FBC-z6v's done line: the audit that the six slices that built the conformance toy (FBC-7lx,
//! FBC-7ce, FBC-sal, FBC-u1d, FBC-z2s and FBC-ja3) left nothing it declares untested. [`CAPS`]
//! maps every capability the toy's [`caps`](toy::caps) declares true or non-empty to the named
//! tests in this directory that exercise it, [`THREADS`] maps every Codex thread the ticket
//! lists ([`LISTED`]) the same way, and
//! `the_toys_declared_capabilities_and_the_listed_threads_each_map_to_tests_that_exist` fails
//! when a declared capability or a listed thread has no entry, or an entry names a test that
//! does not exist.
//!
//! **What is declared** is read from the caps value at run time ([`declared`]), not kept by hand:
//! the toy's `VenueCaps` is printed with its derived `Debug` and each of its leaves that declares
//! something becomes one key, so a flag, a `Some`, a list's element, a set's member or a choice
//! the toy comes to declare, in a field `VenueCaps` has today or one it grows, is a key with no
//! entry until a test is named for it. A number is a key only where the rules below make it one;
//! elsewhere it is a parameter of what holds it and is not audited on its own, so a number a
//! field grows there (in a `Batch`, a `RateLimit` or a list's `BookCaps`, say) is not flagged.
//! A key is the field's path from `VenueCaps` (`exec.order.post_only`), a list's element by its
//! index (`md.books[1]`) and a set's member after a dot (`exec.order.tifs.Ioc`); an enum's choice
//! follows an `=` (`md.stats=Poll`), as does a tuple in a list
//! (`exec.order.flag_conflicts=(PostOnly, Ioc)`). Declared are a `true` flag, a `Some` (then
//! what it holds), each element of a list and each member of a set, and each enum choice but the
//! ones that declare nothing ([`ABSENT`]: `None`, `Unsupported`, `Unknown`), a struct variant's
//! fields after its choice.
//!
//! A struct that is a field's value is an enum's struct variant, a choice, unless its name ends
//! in `Caps`: then it is a part of the declaration (every plain struct `VenueCaps` holds as a
//! field's value is so named; one that is not reads as a choice and needs an entry, so the audit
//! errs toward asking). A struct inside a `Some` or a list is a part whatever its name (`Batch`,
//! `RateLimit`), the `Some` or the element being its key: an enum's struct variant held there
//! would be read the same, its choice not a key (no field of `VenueCaps` holds one today).
//!
//! A number or a duration other than zero held directly by a `*Caps` part that is not a list's
//! element is a key of its own: by a part whose presence declares nothing (`OrderCaps`, `MdCaps`:
//! `md.ts_precision`, `exec.order.snapshot_lag_bound`) or by one a `Some` holds (`ExecCaps`,
//! `AmendCaps`, which hold none today). One held by a list's element (a book channel's depth, a
//! limit's window and units), by a struct that is not a `*Caps` part (a batch's size), by a
//! struct variant (a client id's length) or directly by a `Some` (a connection's lifetime), and a
//! name, is a parameter of the capability that holds it and is exercised with it. `false`, an
//! empty set or list and a zero declare nothing.
//!
//! **How a test is named**: `<file>::<fn>`, the function `fn <fn>` at the top level of
//! `tests/<file>.rs`, as `cargo test --test <file> <fn>` runs it. The audit reads that file with
//! its comments taken out and its string and character literals emptied ([`code_only`]), so a
//! `fn` in a comment, a doc comment or a string is none, and finds the function outside every
//! brace, bracket and parenthesis (so in no module, block or macro's call), under a `#[test]` or
//! `#[tokio::test(..)]` attribute among the attributes directly above it; a function that is not
//! a test, one marked `#[ignore]`, which the check never runs, or one under a `#[cfg(..)]` or a
//! `#[cfg_attr(..)]`, which a build may leave out or ignore, names no test, nor does any function
//! of a file that opens with a `#![cfg(..)]` or a `#![cfg_attr(..)]` ([`run_by_the_check`]). The
//! crate's manifest declares no `[[test]]` target and sets no `autotests`, either of which can
//! change which of these files run as tests ([`manifest_problems`]). Tests the suite macro
//! generates are not written in these files, so an entry names the test in `suite_toy.rs` that
//! runs the same check on the toy and asserts what it probed.
//!
//! **What an entry is** ([`Entry`]): the tests that exercise the key; or why no test here can
//! exercise it, only for the choice or number of a field that describes the venue rather than
//! anything the toy does ([`DESCRIPTIVE`]), which no code of this crate but the toy's
//! declaration names ([`descriptive_readers`]), or for a set's member beside members that are
//! exercised (never for a flag, a `Some`, a list's element, any other choice or number, or a set
//! none of whose members is exercised, which the done line demands a test for); or the ticket
//! that brings the test still missing, which the audit reports as missing.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use fbc_conformance::toy::{self, FillIds};
use fbc_core::{
    AckModel, Cadence, CancelOnDisconnect, Channel, ExchTsKind, FeedSource, SeqDomain, SpeedBump,
    SpeedBumpScope, Support, TagSet, TifTag, TouchSourceCaps, VenueCaps,
};

// ---------------------------------------------------------------------------------------------
// The tests the table names.
// ---------------------------------------------------------------------------------------------

const ENCODES: &str =
    "toy_order_entry::every_command_kind_encodes_through_the_toy_with_a_fixed_encode_ctx";
const SIGNER_SEES: &str =
    "toy_order_entry::the_signer_is_shown_only_the_reference_the_request_carries";
const REMAINING: &str =
    "toy_order_entry::a_remaining_quantity_amend_sends_the_total_less_the_filled_quantity";
const UNDECLARED: &str =
    "toy_order_entry::an_undeclared_reference_tif_or_channel_is_refused_unsupported_with_no_effect";
const MAX_ITEMS: &str = "toy_order_entry::a_batch_over_max_items_is_refused";
const CHARGED: &str =
    "toy_order_entry::every_frame_carries_its_rpc_its_commands_traffic_class_and_its_rate_charge";
const SIGN_STAGES: &str = "toy_order_entry::\
     encode_marks_each_signer_call_as_a_sign_stage_and_its_bytes_do_not_depend_on_the_marks";
const NONCES: &str = "toy_order_entry::nonces_and_time_come_only_from_the_encode_ctx";
const UNSIGNED: &str = "toy_order_entry::what_cannot_be_written_or_signed_is_not_sent";

const ECHOED: &str = "toy_events::an_order_update_decodes_with_the_flags_the_venue_echoes";
const AMENDED: &str =
    "toy_events::an_amended_update_names_the_new_venue_id_and_states_the_new_price_and_total_once";
const FILL_ID: &str = "toy_events::a_fill_is_keyed_by_the_venue_fill_id";
const DERIVED: &str = "toy_events::a_fill_without_a_fill_id_is_keyed_by_order_and_\
                       cumulative_quantity_with_realized_pnl_and_funding";
const REPLAYED: &str = "toy_events::a_replayed_fill_is_marked_replay";
const NEGATIVE_QTY: &str =
    "toy_events::a_negative_optional_quantity_refuses_the_frame_with_nothing_pushed";
const FILL_IDS_ONLY: &str =
    "toy_events::the_toy_declared_without_fill_ids_differs_only_in_its_fill_id";

const QUERIES: &str = "toy_answers::\
     queries_by_venue_id_and_by_placement_nonce_are_answered_carrying_the_querys_rpc";
const QUERY_REFUSED: &str = "toy_answers::\
     a_query_answer_naming_another_order_or_no_query_is_refused_and_the_query_still_waits";
const TWO_FRAMES: &str = "toy_answers::a_batch_answered_in_two_frames_is_pushed_in_one_call";
const UNANSWERED: &str =
    "toy_answers::on_rpc_timeout_reports_unknown_only_for_the_items_still_unanswered";
const RESYNC: &str = "toy_answers::\
     a_resync_asks_once_and_is_decoded_into_begin_orders_positions_and_end_pushed_at_its_end";

const TWO_BOOKS: &str = "toy_books::two_book_channels_on_one_connection_keep_their_levels_apart";
const WHOLE_SNAPSHOT: &str = "toy_books::a_snapshot_frame_it_cannot_decode_whole_pushes_nothing";
const BOOK_REFUSED: &str =
    "toy_books::a_book_id_outside_the_declared_channels_is_refused_with_nothing_sent";
const ANCHORED: &str =
    "toy_books::a_rest_anchored_book_is_anchored_on_its_http_snapshot_retrying_until_it_decodes";
const ANCHOR_AGAIN: &str =
    "toy_books::an_anchor_older_than_a_held_delta_or_one_past_the_held_bound_is_asked_again";
const GAP: &str =
    "toy_books::a_gap_reports_health_naming_the_book_that_broke_and_leaves_the_other_alone";
const MD_COUNTED: &str = "toy_books::every_market_data_request_is_counted_by_a_declared_limit";

const FEEDS: &str = "toy_feeds::trades_funding_mark_and_index_decode_into_their_md_events";
const FEED_WHOLE: &str = "toy_feeds::a_feed_frame_it_cannot_decode_whole_pushes_nothing";
const TRANSPORTS: &str =
    "toy_feeds::a_poll_feed_is_refused_on_the_socket_codec_and_a_stream_feed_on_the_poll_codec";
const PLANNED: &str =
    "toy_feeds::the_factory_plans_streamed_feeds_on_the_socket_and_stats_on_a_poll_endpoint";
const POLLED: &str = "toy_feeds::\
     stats_are_polled_under_the_codecs_own_timer_until_the_last_subscription_is_removed";
const NEGATIVE_STATS: &str =
    "toy_feeds::negative_open_interest_or_volume_is_refused_with_nothing_pushed";
const POLLED_LIVE: &str = "toy_stats_poll::\
     the_toys_stats_are_polled_under_its_own_timer_until_the_last_subscription_is_removed";

const REST_WHOLE: &str =
    "toy_rest_resync::a_resync_over_rest_keeps_the_bases_spans_and_is_pushed_whole_in_one_call";
const REST_RETRIED: &str = "toy_rest_resync::\
     a_resync_over_rest_is_retried_on_an_http_failure_or_another_status_at_the_retrys_instant";
const REST_UNREAD: &str =
    "toy_rest_resync::a_resync_over_rest_pushes_nothing_from_a_body_it_cannot_decode_whole";
const PINGED: &str =
    "toy_rest_resync::a_pinging_codec_arms_its_ping_on_open_and_again_on_each_firing";
const FILL_FIELDS: &str =
    "toy_rest_resync::a_fill_frame_missing_any_field_its_fill_caps_declare_is_refused";

const URL_SPANS: &str = "toy_urls::configured_urls_keep_the_spans_their_configuration_marks";
const URL_REFUSED: &str =
    "toy_urls::a_url_with_user_information_a_query_or_spans_it_cannot_keep_is_refused";

const FACTORY: &str =
    "toy_factory::the_toy_factory_declares_the_toys_caps_and_builds_its_codec_without_credentials";

const CAPS_TRUTHFUL: &str =
    "suite_toy::caps_truthful_probes_every_absence_and_the_declared_flag_conflict_on_the_toy";
const SELFCONTAINED: &str =
    "suite_toy::commands_selfcontained_encodes_every_declared_reference_on_the_toy";
const FEE_SIGN: &str = "suite_toy::fee_sign_judges_the_fee_of_every_fill_in_both_cases_on_the_toy";
const LIQUIDITY: &str = "suite_toy::liquidity_reported_judges_every_fill_in_both_cases_on_the_toy";
const IDS: &str = "suite_toy::\
     ids_roundtrip_round_trips_minted_ids_and_reads_every_java_era_id_as_unparseable";
const CONTINUITY: &str = "suite_toy::\
     continuity_detects_every_marked_break_on_both_toy_books_and_skips_the_longer_block_by_name";
const BOOK_CHANNELS: &str = "suite_toy::\
     book_channels_holds_the_toys_book_to_public_and_its_rpi_book_to_public_and_rpi_liquidity";
const ANCHORED_CHANNELS: &str =
    "suite_md::book_channels_fails_the_anchored_rpi_book_declared_without_public_or_rpi";
const AMEND_ACK: &str =
    "suite_toy::amend_ack_sees_the_toys_replaced_order_event_with_its_new_venue_id";
const MIXED_BATCH: &str = "suite_toy::mixed_batch_gives_each_item_of_the_toys_batch_its_outcome";
const SINGLE_PHASE: &str =
    "suite_toy::two_phase_ack_sees_no_provisional_acceptance_on_the_single_phase_toy";

const DISARM: &str = "suite_broken::the_toy_declaring_a_disarm_sends_it_as_a_control_and_passes";

const REJECT_KEPT: &str = "amend_reject_keeps_original::refused_amend_leaves_order_resting";

const STORM: &str =
    "order_storm::the_340_reconnect_storm_resends_nothing_and_arms_and_resyncs_before_every_order";
const LATE_NEW: &str =
    "order_faults::an_order_stays_terminal_after_a_late_new_and_pending_cancel_in_either_order";
const EQUAL_KEY: &str =
    "order_faults::an_equal_key_partial_fill_and_cancel_end_alike_in_both_arrival_orders";

// ---------------------------------------------------------------------------------------------
// The table.
// ---------------------------------------------------------------------------------------------

/// What the audit holds of one declared capability.
#[derive(Copy, Clone, Debug)]
enum Entry {
    /// The tests that exercise it, each `<file>::<fn>`.
    Tested(&'static [&'static str]),
    /// Why no test here can exercise it: allowed only for the choice or number of a
    /// [`DESCRIPTIVE`] field, and for a set's member when another member of that set is
    /// [`Tested`](Entry::Tested).
    Described(&'static str),
    /// No test exercises it yet; the ticket named brings one. The audit reports it missing.
    Awaiting(&'static str),
}

use Entry::{Awaiting, Described, Tested};

/// Why a book channel's cadence is described, not tested.
const CADENCE: &str = "how often the venue publishes the channel: the toy's codec decodes each \
                       frame as it comes and times nothing by it, and no code in this crate reads \
                       it";
/// Why a book channel's queue-model quality is described, not tested.
const QUEUE_MODEL: &str = "how well a queue-position model calibrates on the channel's data: the \
                           toy's codec builds no queue model, and no code in this crate reads it";
/// Why the readiness ceiling is described, not tested.
const READINESS: &str = "the furthest the consumer may promote the adapter, which the consumer \
                         reads: nothing in this crate promotes an adapter or reads it";
/// Why the self-trade prevention scope is described, not tested.
const STP: &str = "across which orders the venue prevents a self-trade: the toy matches no \
                   orders, so it prevents none, and no code in this crate reads it";
/// Why the timestamp resolution is described, not tested.
const TS_PRECISION: &str = "the resolution of the venue's timestamps: the toy's codec reads each \
                            timestamp as its frame states it, and no code in this crate reads it";
/// Why a single cancel's placement-nonce reference is described, not tested.
const UNREACHABLE_NONCE: &str = "no single cancel can be named by it: every OrderRef carries the \
                                 venue's id or ours, both declared and tried before the placement \
                                 nonce, so the suite's commands_selfcontained names none either";

/// The fields whose choice or number describes the venue rather than anything the toy does, the
/// only ones an entry may describe rather than test (a list's index written `[]`).
const DESCRIPTIVE: [&str; 5] = [
    "matching.stp_scope",
    "md.books[].cadence",
    "md.books[].queue_model",
    "md.ts_precision",
    "readiness_ceiling",
];

/// The field `key` is declared by, its choice dropped and each list index written `[]`.
fn field_of(key: &str) -> String {
    let field = key.split_once('=').map_or(key, |(field, _)| field);
    let mut out = String::new();
    let mut index = false;
    for c in field.chars() {
        match c {
            '[' | ']' => {
                index = c == '[';
                out.push(c);
            }
            _ if index => {}
            _ => out.push(c),
        }
    }
    out
}

/// Rows of a table, one `key => fields;` a line: a macro, so each row stays on its line.
macro_rules! rows {
    ($($key:literal => $($field:expr),+;)*) => {
        &[$(($key, $($field),+)),*]
    };
}

/// Every capability the toy declares, by its key, with the tests that exercise it.
const CAPS: &[(&str, Entry)] = rows! {
    // Order entry: every command kind encoded through `ExecCodec` alone (FBC-7lx).
    "exec" => Tested(&[ENCODES, FACTORY]);
    "exec.order.kinds.Limit" => Tested(&[ENCODES, UNDECLARED]);
    "exec.order.tifs.Gtc" => Tested(&[ENCODES, UNDECLARED]);
    "exec.order.tifs.Ioc" => Tested(&[ENCODES, CAPS_TRUTHFUL]);
    "exec.order.channels.Public" => Tested(&[ENCODES, UNDECLARED]);
    "exec.order.post_only" => Tested(&[ENCODES, ECHOED]);
    "exec.order.reduce_only" => Tested(&[ENCODES, ECHOED]);
    "exec.order.flag_conflicts=(PostOnly, Ioc)" => Tested(&[CAPS_TRUTHFUL]);
    "exec.order.amend" => Tested(&[ENCODES, AMEND_ACK]);
    "exec.order.amend.refs.Venue" => Tested(&[SIGNER_SEES, SELFCONTAINED]);
    // The amend's frame carries its price and flags; amend_ack amends the price a step.
    "exec.order.amend.price" => Tested(&[ENCODES, AMEND_ACK]);
    "exec.order.amend.qty" => Tested(&[REMAINING]);
    "exec.order.amend.flags" => Tested(&[ENCODES, CAPS_TRUTHFUL]);
    // Amended with 4 of its 10 lots filled.
    "exec.order.amend.when_partially_filled" => Tested(&[REMAINING]);
    // A resting order amended once under each code of the toy's table that refuses an amend
    // stays Open at its price, quantity and venue id, the refusal its amend's outcome alone.
    "exec.order.amend.reject_keeps_original" => Tested(&[REJECT_KEPT]);
    "exec.order.amend.ack=ReplacedEvent" => Tested(&[AMEND_ACK, AMENDED]);
    "exec.order.amend.qty_semantics=Remaining" => Tested(&[REMAINING]);
    "exec.order.cancel_refs.Venue" => Tested(&[SIGNER_SEES, SELFCONTAINED]);
    "exec.order.cancel_refs.Client" => Tested(&[SIGNER_SEES, SELFCONTAINED, LATE_NEW]);
    "exec.order.cancel_refs.PlacementNonce" => Described(UNREACHABLE_NONCE);
    "exec.order.query_refs.Venue" => Tested(&[QUERIES, SELFCONTAINED]);
    "exec.order.query_refs.PlacementNonce" => Tested(&[QUERIES, SELFCONTAINED]);
    // Our cancel by client id while the placement is unanswered; a batch cancel of an order
    // with no venue id yet by its placement nonce.
    "exec.order.cancel_before_ack" => Tested(&[LATE_NEW, SIGNER_SEES]);
    "exec.order.cancel_is_signed" => Tested(&[UNSIGNED, SIGN_STAGES]);
    "exec.order.batch_place" => Tested(&[MAX_ITEMS, TWO_FRAMES, MIXED_BATCH]);
    "exec.order.batch_cancel" => Tested(&[MAX_ITEMS, SIGNER_SEES]);
    "exec.order.batch_cancel.refs.Venue" => Tested(&[SIGNER_SEES, SELFCONTAINED]);
    "exec.order.batch_cancel.refs.PlacementNonce" => Tested(&[SIGNER_SEES, SELFCONTAINED]);
    "exec.order.cancel_all_instrument=Native" => Tested(&[ENCODES, CAPS_TRUTHFUL]);
    "exec.order.cancel_on_disconnect=PerConnection" => Tested(&[ENCODES, CAPS_TRUTHFUL, STORM]);
    // Armed again on each of the storm's 341 connections.
    "exec.order.cancel_on_disconnect.rearm_on_reconnect" => Tested(&[STORM]);
    // Its codec writes the disarm, which caps_truthful sends as a control.
    "exec.order.cancel_on_disconnect.offers_disarm" => Tested(&[ENCODES, CAPS_TRUTHFUL, DISARM]);
    "exec.order.ack=SinglePhase" => Tested(&[SINGLE_PHASE]);
    "exec.order.client_id=Alnum" => Tested(&[IDS]);
    "exec.order.client_id.charset=Alphanumeric" => Tested(&[IDS]);
    "exec.order.cid_echoed_on_events" => Tested(&[ECHOED]);
    // The toy's nonces are only those the session reserves from the account's one sequence
    // (the encode context's), and a registry built from these caps arms with the account lease.
    "exec.order.nonce_scope=PerAccountMonotonic" => Tested(&[NONCES, STORM]);
    // Every order event carries its venue sequence, which orders late events behind newer ones.
    "exec.order.ordering_key=VenueSeq" => Tested(&[ECHOED, LATE_NEW, EQUAL_KEY]);
    // An order the venue never got is Lost once a resync of the toy's leaves it out.
    "exec.order.snapshot_source=Trustworthy" => Tested(&[STORM]);
    // One answer holds the open orders and the positions, in frames or one request.
    "exec.order.snapshot_reads=OneInstant" => Tested(&[RESYNC, REST_WHOLE]);
    "exec.order.events_echo_flags" => Tested(&[ECHOED]);
    // Fills (FBC-7ce, FBC-ja3).
    "exec.fills.source=Native" => Tested(&[FILL_ID, DERIVED]);
    "exec.fills.liquidity_flag" => Tested(&[FILL_ID, LIQUIDITY, FILL_FIELDS]);
    "exec.fills.realized_pnl" => Tested(&[DERIVED, FILL_FIELDS]);
    "exec.fills.realized_funding" => Tested(&[DERIVED, FILL_FIELDS]);
    "exec.fills.fee_sign=PositiveIsCost" => Tested(&[FEE_SIGN]);
    "exec.fills.fee_asset_reported" => Tested(&[FILL_ID, FILL_FIELDS]);
    "exec.fills.fill_id" => Tested(&[FILL_ID, FILL_FIELDS]);
    "exec.fills.replays_fills_on_reconnect" => Tested(&[REPLAYED]);
    "matching.stp_scope=Account" => Described(STP);
    // Market data (FBC-u1d, FBC-z2s).
    "md.encoding=Text" => Tested(&[CONTINUITY, TWO_BOOKS]);
    "md.books[0]" => Tested(&[TWO_BOOKS, BOOK_CHANNELS]);
    "md.books[0].cadence=Realtime" => Described(CADENCE);
    "md.books[0].continuity=PlusOne" => Tested(&[GAP, CONTINUITY]);
    "md.books[0].includes_channels.Public" => Tested(&[BOOK_CHANNELS]);
    "md.books[0].queue_model=BracketOnly" => Described(QUEUE_MODEL);
    "md.books[1]" => Tested(&[TWO_BOOKS, ANCHORED]);
    "md.books[1].cadence=Realtime" => Described(CADENCE);
    "md.books[1].continuity=PlusOne" => Tested(&[GAP, ANCHOR_AGAIN, CONTINUITY]);
    // The suite's book_channels drives the anchored channel from a case whose HTTP lines answer
    // its anchors, a line showing each, and names those lines where either is not declared.
    "md.books[1].includes_channels.Public" => Tested(&[BOOK_CHANNELS, ANCHORED_CHANNELS]);
    "md.books[1].includes_channels.Rpi" => Tested(&[BOOK_CHANNELS, ANCHORED_CHANNELS]);
    "md.books[1].queue_model=BracketOnly" => Described(QUEUE_MODEL);
    "md.books[1].rest_anchor" => Tested(&[ANCHORED]);
    "md.trades.source=Stream" => Tested(&[FEEDS, TRANSPORTS]);
    "md.trades.aggressor" => Tested(&[FEEDS, FEED_WHOLE]);
    "md.trades.trade_id" => Tested(&[FEEDS, FEED_WHOLE]);
    "md.funding.source=Stream" => Tested(&[FEEDS, TRANSPORTS]);
    "md.funding.interval_reported" => Tested(&[FEEDS, FEED_WHOLE]);
    "md.funding.next_time_reported" => Tested(&[FEEDS, FEED_WHOLE]);
    "md.stats=Poll" => Tested(&[POLLED_LIVE, POLLED, TRANSPORTS]);
    "md.mark=Stream" => Tested(&[FEEDS, TRANSPORTS]);
    "md.index=Stream" => Tested(&[FEEDS, TRANSPORTS]);
    "md.ts_precision" => Described(TS_PRECISION);
    // Both book channels of one instrument, and every streamed feed, on one connection.
    "md.topology=Shared" => Tested(&[TWO_BOOKS, PLANNED]);
    // One account budget counts every request either codec writes.
    "limits[0]" => Tested(&[CHARGED, MD_COUNTED]);
    "limits[0].scope=Account" => Tested(&[CHARGED, MD_COUNTED]);
    "limits[0].ops.Place" => Tested(&[CHARGED]);
    "limits[0].ops.Amend" => Tested(&[CHARGED]);
    "limits[0].ops.Cancel" => Tested(&[CHARGED]);
    "limits[0].ops.CancelAll" => Tested(&[CHARGED]);
    "limits[0].ops.Query" => Tested(&[CHARGED]);
    "limits[0].ops.Subscribe" => Tested(&[MD_COUNTED]);
    "limits[0].ops.Rest" => Tested(&[MD_COUNTED]);
    "limits[0].ops.Control" => Tested(&[CHARGED, MD_COUNTED]);
    "readiness_ceiling=Record" => Described(READINESS);
};

/// The Codex threads FBC-z6v lists, in its order.
const LISTED: [&str; 24] = [
    "r4172231010",
    "r4172835744",
    "r4172917339",
    "r4172835757",
    "r4172917344",
    "r4172649862",
    "r4172456290",
    "r4172752101",
    "r4172917328",
    "r4172917318",
    "r4172917335",
    "r4172835747",
    "r4172586688",
    "r4172649856",
    "r4172456284",
    "r4172586686",
    "r4172456271",
    "r4172835741",
    "r4172917321",
    "r4172835753",
    "r4172917294",
    "r4172752096",
    "r4172917309",
    "r4173243266",
];

/// Every listed thread, what it asked for, and the tests that show it.
const THREADS: &[(&str, &str, &[&str])] = rows! {
    "r4172231010" => "two book channels on one connection", &[TWO_BOOKS];
    "r4172835744" => "a snapshot decoded whole or not at all", &[WHOLE_SNAPSHOT];
    "r4172917339" => "a book id outside the declared channels refused", &[BOOK_REFUSED];
    "r4172835757" => "a REST anchor where BookCaps.rest_anchor", &[ANCHORED];
    "r4172917344" => "Stream feeds only on a socket codec, Poll feeds only on a poll codec",
        &[TRANSPORTS, PLANNED];
    "r4172649862" => "MdCaps sources checked against the codec's transport",
        &[TRANSPORTS, PLANNED];
    "r4172456290" => "a connectionless poll endpoint (MdTransport::Poll)",
        &[PLANNED, POLLED_LIVE];
    "r4172752101" => "stats polled over HTTP under the codec's own timer", &[POLLED, POLLED_LIVE];
    "r4172917328" => "polling stops when the last subscription is removed",
        &[POLLED, POLLED_LIVE];
    "r4172917318" => "a negative optional quantity refused, never read as absent",
        &[NEGATIVE_STATS, NEGATIVE_QTY];
    "r4172917335" => "a resync over REST retried on HttpFailure, its envelope read before a push",
        &[REST_RETRIED, REST_UNREAD, REST_WHOLE];
    "r4172835747" => "an exec ping armed on open and again on each firing", &[PINGED];
    "r4172586688" => "the signer shown only the CancelRef the request carries", &[SIGNER_SEES];
    "r4172649856" => "the signer shown only the AmendRef the request carries", &[SIGNER_SEES];
    "r4172456284" => "an amend whose only reference is our id refused", &[UNDECLARED];
    "r4172586686" => "batch placement within max_items", &[MAX_ITEMS];
    "r4172456271" => "order queries by venue id and by placement nonce", &[QUERIES];
    "r4172835741" => "a QueryResult carrying the query's rpc", &[QUERIES];
    "r4172917321" => "the library's QueryResult fix (FBC-5): it names its query and its order",
        &[QUERIES, QUERY_REFUSED];
    "r4172835753" => "fills without a fill id keyed FillKey::Derived", &[DERIVED, FILL_IDS_ONLY];
    "r4172917294" => "configured URLs keep their redaction spans, or are refused",
        &[URL_SPANS, URL_REFUSED];
    "r4172752096" => "time in force and channel checked against the caps before encode",
        &[UNDECLARED];
    "r4172917309" => "every declared FillCaps field required on the wire", &[FILL_FIELDS];
    "r4173243266" => "a batch acked in separate frames pushed in one call, Unknown for the rest",
        &[TWO_FRAMES, UNANSWERED];
};

// ---------------------------------------------------------------------------------------------
// What is declared.
// ---------------------------------------------------------------------------------------------

/// The enum choices that declare nothing.
const ABSENT: [&str; 3] = ["None", "Unsupported", "Unknown"];

/// What a declared key is.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Kind {
    /// A flag that is `true`.
    Flag,
    /// An option that is `Some`.
    Present,
    /// An element of a list.
    Element,
    /// A member of the set at this path.
    Member(String),
    /// An enum's choice.
    Choice,
    /// A number or a duration other than zero that a part holds directly.
    Number,
}

/// Every capability `caps` declares, by key, read from its `Debug` form (see the module's
/// documentation).
fn declared(caps: &VenueCaps) -> BTreeMap<String, Kind> {
    declared_text(&format!("{caps:?}"))
}

/// Every capability the derived `Debug` form `text` declares, by key.
fn declared_text(text: &str) -> BTreeMap<String, Kind> {
    let mut parser = Parser {
        chars: text.chars().collect(),
        at: 0,
    };
    let root = parser.value();
    assert_eq!(parser.peek(), None, "trailing text in {text}");
    let mut out = BTreeMap::new();
    walk("", &root, false, &mut out);
    out
}

/// One value of a derived `Debug` form.
#[derive(Debug)]
enum Node {
    /// An identifier, `true` or `false`, a number or a duration.
    Atom(String),
    /// A quoted string.
    Text,
    /// `Name { field: value, .. }`: a struct, or an enum's struct variant.
    Struct(String, Vec<(String, Node)>),
    /// `Name(value, ..)`: `Some`, or an enum's tuple variant.
    Call(String, Vec<Node>),
    /// `{a, b}`.
    Set(Vec<Node>),
    /// `[a, b]`.
    List(Vec<Node>),
    /// `(a, b)`.
    Tuple(Vec<Node>),
}

struct Parser {
    chars: Vec<char>,
    at: usize,
}

impl Parser {
    /// The next character that is not whitespace, which is not taken.
    fn peek(&mut self) -> Option<char> {
        while self.chars.get(self.at).is_some_and(|c| c.is_whitespace()) {
            self.at += 1;
        }
        self.chars.get(self.at).copied()
    }

    fn eat(&mut self, c: char) {
        assert_eq!(self.peek(), Some(c), "at {}", self.at);
        self.at += 1;
    }

    fn value(&mut self) -> Node {
        match self.peek() {
            Some('"') => {
                self.at += 1;
                while let Some(&c) = self.chars.get(self.at) {
                    self.at += if c == '\\' { 2 } else { 1 };
                    if c == '"' {
                        break;
                    }
                }
                Node::Text
            }
            Some('{') => Node::Set(self.items('{', '}')),
            Some('[') => Node::List(self.items('[', ']')),
            Some('(') => Node::Tuple(self.items('(', ')')),
            _ => {
                let word = self.word();
                match self.peek() {
                    Some('{') => Node::Struct(word, self.fields()),
                    Some('(') => Node::Call(word, self.items('(', ')')),
                    _ => Node::Atom(word),
                }
            }
        }
    }

    fn word(&mut self) -> String {
        self.peek();
        let start = self.at;
        let stop = |c: &char| c.is_whitespace() || "{}[](),:\"".contains(*c);
        while self.chars.get(self.at).is_some_and(|c| !stop(c)) {
            self.at += 1;
        }
        assert!(self.at > start, "a word at {start}");
        self.chars[start..self.at].iter().collect()
    }

    fn items(&mut self, open: char, close: char) -> Vec<Node> {
        self.eat(open);
        let mut items = Vec::new();
        while self.peek() != Some(close) {
            items.push(self.value());
            if self.peek() == Some(',') {
                self.at += 1;
            }
        }
        self.eat(close);
        items
    }

    fn fields(&mut self) -> Vec<(String, Node)> {
        self.eat('{');
        let mut fields = Vec::new();
        while self.peek() != Some('}') {
            let name = self.word();
            self.eat(':');
            fields.push((name, self.value()));
            if self.peek() == Some(',') {
                self.at += 1;
            }
        }
        self.eat('}');
        fields
    }
}

/// Adds what `node`, at `path`, declares to `out`; `field` when it is a field's own value.
fn walk(path: &str, node: &Node, field: bool, out: &mut BTreeMap<String, Kind>) {
    match node {
        Node::Atom(word) if word == "true" => {
            out.insert(path.to_owned(), Kind::Flag);
        }
        Node::Atom(word) if ABSENT.contains(&word.as_str()) => {}
        Node::Atom(word) if word.starts_with(|c: char| c.is_ascii_uppercase()) => {
            out.insert(format!("{path}={word}"), Kind::Choice);
        }
        // `false`, a number or a duration; a name.
        Node::Atom(_) | Node::Text => {}
        Node::Struct(name, fields) => {
            if field && !name.ends_with("Caps") {
                out.insert(format!("{path}={name}"), Kind::Choice);
            }
            // A `*Caps` part that is no list's element, whether its presence declares nothing or
            // a `Some` holds it: what number it holds directly is no other capability's
            // parameter.
            let part = name.ends_with("Caps") && out.get(path) != Some(&Kind::Element);
            for (name, value) in fields {
                let at = match path {
                    "" => name.clone(),
                    _ => format!("{path}.{name}"),
                };
                match value {
                    Node::Atom(word) if part && nonzero(word) => {
                        out.insert(at, Kind::Number);
                    }
                    _ => walk(&at, value, true, out),
                }
            }
        }
        Node::Call(name, args) => {
            match name.as_str() {
                "Some" => out.insert(path.to_owned(), Kind::Present),
                _ => out.insert(format!("{path}={name}"), Kind::Choice),
            };
            for arg in args {
                walk(path, arg, false, out);
            }
        }
        Node::Set(members) => {
            for member in members {
                let Node::Atom(member) = member else {
                    panic!("{path}: a set of {member:?}");
                };
                out.insert(format!("{path}.{member}"), Kind::Member(path.to_owned()));
            }
        }
        Node::List(items) => {
            for (i, item) in items.iter().enumerate() {
                if let Node::Tuple(_) = item {
                    out.insert(format!("{path}={}", render(item)), Kind::Element);
                } else {
                    let at = format!("{path}[{i}]");
                    out.insert(at.clone(), Kind::Element);
                    walk(&at, item, false, out);
                }
            }
        }
        Node::Tuple(_) => {
            out.insert(format!("{path}={}", render(node)), Kind::Choice);
        }
    }
}

/// Whether `word` is a number or a duration (`250ms`) other than zero.
fn nonzero(word: &str) -> bool {
    word.starts_with(|c: char| c.is_ascii_digit() || c == '-')
        && word.chars().any(|c| c.is_ascii_digit() && c != '0')
}

/// A tuple of identifiers as its `Debug` form spells it.
fn render(node: &Node) -> String {
    match node {
        Node::Atom(word) => word.clone(),
        Node::Tuple(items) => {
            let items: Vec<String> = items.iter().map(render).collect();
            format!("({})", items.join(", "))
        }
        other => panic!("not a tuple of identifiers: {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Which tests exist.
// ---------------------------------------------------------------------------------------------

/// Every test source of this directory, by file stem, as [`code_only`] leaves it.
fn sources() -> BTreeMap<String, String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut out = BTreeMap::new();
    for entry in fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
            out.insert(stem, code_only(&fs::read_to_string(&path).unwrap()));
        }
    }
    out
}

/// `source` with its comments taken out and every string and character literal emptied, so
/// what is left to scan is code: a `fn` or a brace in a comment, a doc comment or a literal is
/// none.
fn code_only(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let at = |i: usize| chars.get(i).copied();
    let word_char = |c: char| c.is_alphanumeric() || c == '_';
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    while let Some(c) = at(i) {
        match (c, at(i + 1)) {
            ('/', Some('/')) => {
                while at(i).is_some_and(|c| c != '\n') {
                    i += 1;
                }
            }
            ('/', Some('*')) => {
                let mut depth = 0;
                while let Some(c) = at(i) {
                    match (c, at(i + 1)) {
                        ('/', Some('*')) => (depth, i) = (depth + 1, i + 2),
                        ('*', Some('/')) => (depth, i) = (depth - 1, i + 2),
                        _ => i += 1,
                    }
                    if depth == 0 {
                        break;
                    }
                }
                out.push(' ');
            }
            ('"', _) => {
                i = literal_end(&chars, i + 1, false, 0);
                out.push_str("\"\"");
            }
            // A character literal, escaped or not; otherwise a lifetime or a label.
            ('\'', Some('\\')) => {
                i += 3;
                while at(i).is_some_and(|c| c != '\'') {
                    i += 1;
                }
                i += 1;
                out.push_str("'_'");
            }
            ('\'', _) if at(i + 2) == Some('\'') => {
                i += 3;
                out.push_str("'_'");
            }
            (c, _) if word_char(c) => {
                let start = i;
                while at(i).is_some_and(word_char) {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let hashes = chars[i..].iter().take_while(|c| **c == '#').count();
                if ["r", "br", "cr"].contains(&word.as_str()) && at(i + hashes) == Some('"') {
                    i = literal_end(&chars, i + hashes + 1, true, hashes);
                    out.push_str("\"\"");
                } else {
                    out.push_str(&word);
                }
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// Where the string literal whose contents start at `i` ends, just past its closing quote and
/// the `hashes` that close it with; `raw` when no backslash in it escapes anything.
fn literal_end(chars: &[char], mut i: usize, raw: bool, hashes: usize) -> usize {
    while let Some(&c) = chars.get(i) {
        i += 1;
        if c == '\\' && !raw {
            i += 1;
        } else if c == '"' && (0..hashes).all(|k| chars.get(i + k) == Some(&'#')) {
            return i + hashes;
        }
    }
    i
}

/// Why `name` names no test the check runs, or `None` when it names one: `name` is
/// `<file>::<fn>`, `tests/<file>.rs` (in `sources`, as [`code_only`] leaves it) opens with no
/// `#![cfg(..)]` or `#![cfg_attr(..)]`, and `fn <fn>` at its top level has a `#[test]` or
/// `#[tokio::test(..)]` attribute and no `#[ignore]`, `#[cfg(..)]` or `#[cfg_attr(..)]` among
/// the attributes directly above it.
fn run_by_the_check(sources: &BTreeMap<String, String>, name: &str) -> Option<String> {
    let Some((file, func)) = name.split_once("::") else {
        return Some(format!("{name}: not <file>::<fn>"));
    };
    let Some(code) = sources.get(file) else {
        return Some(format!("{name}: there is no tests/{file}.rs"));
    };
    let configured = |a: &&str| a.starts_with("cfg(") || a.starts_with("cfg_attr(");
    if inner_attributes(code).iter().any(configured) {
        return Some(format!(
            "{name}: tests/{file}.rs opens with a #![cfg] or #![cfg_attr], which can leave it out"
        ));
    }
    let mut why = format!("{name}: there is no fn {func} in tests/{file}.rs");
    for (at, _) in code.match_indices(&format!("fn {func}(")) {
        let before = &code[..at];
        let attrs = attributes(before);
        let test = attrs
            .iter()
            .any(|a| *a == "test" || a.starts_with("tokio::test"));
        let open = [('{', '}'), ('(', ')'), ('[', ']')]
            .iter()
            .any(|&(open, close)| before.matches(open).count() != before.matches(close).count());
        if open {
            why = format!(
                "{name}: fn {func} is not at the top level of tests/{file}.rs, so no test is \
                 named {name}"
            );
        } else if !test {
            why = format!("{name}: fn {func} is not a test");
        } else if attrs.iter().any(|a| a.starts_with("ignore")) {
            why = format!("{name}: fn {func} is ignored, so the check never runs it");
        } else if attrs.iter().any(|a| a.starts_with("cfg(")) {
            why = format!("{name}: fn {func} is built only where its cfg holds");
        } else if attrs.iter().any(|a| a.starts_with("cfg_attr(")) {
            why = format!("{name}: fn {func} has a cfg_attr, which can ignore it or leave it out");
        } else {
            return None;
        }
    }
    Some(why)
}

/// The inner attributes a file's code (as [`code_only`] leaves it) opens with, inside their
/// `#![..]`, first first.
fn inner_attributes(code: &str) -> Vec<&str> {
    let mut rest = code.trim_start();
    let mut attrs = Vec::new();
    while let Some(inner) = rest
        .strip_prefix("#!")
        .and_then(|r| r.trim_start().strip_prefix('['))
    {
        let mut depth = 1;
        let Some(close) = inner.char_indices().find_map(|(i, c)| {
            depth += match c {
                '[' => 1,
                ']' => -1,
                _ => 0,
            };
            (depth == 0).then_some(i)
        }) else {
            break;
        };
        attrs.push(inner[..close].trim());
        rest = inner[close + 1..].trim_start();
    }
    attrs
}

/// What in the crate's `manifest` can make the tests the check runs differ from the top-level
/// files of `tests/` that [`run_by_the_check`] reads: a `[[test]]` target, which can rename a
/// test file, leave it out or gate it by `required-features`, or an `autotests` key.
fn manifest_problems(manifest: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for line in manifest.lines() {
        let line: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        if line.starts_with("[[test]]") {
            problems.push(
                "Cargo.toml declares a [[test]] target, which can change which files in tests/ \
                 run as tests"
                    .to_owned(),
            );
        } else if line.starts_with("autotests=") {
            problems.push(
                "Cargo.toml sets autotests, which can change which files in tests/ run as tests"
                    .to_owned(),
            );
        }
    }
    problems
}

/// This crate's manifest.
fn manifest() -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap()
}

/// This file, which names the [`DESCRIPTIVE`] fields to build its cases and reads none.
const THIS_FILE: &str = "tests/toy_caps_audit.rs";

/// The toy's declaration, which names each [`DESCRIPTIVE`] field to declare it.
const DECLARATION: &str = "src/toy/mod.rs";

/// Every source of this crate under `src/` and `tests/` but [`THIS_FILE`], by its path from the
/// crate's directory, as [`code_only`] leaves it.
fn crate_sources() -> BTreeMap<String, String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = BTreeMap::new();
    let mut dirs = vec![root.join("src"), root.join("tests")];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let at = path.strip_prefix(root).unwrap().to_string_lossy();
                let at = at.replace('\\', "/");
                if at != THIS_FILE {
                    out.insert(at, code_only(&fs::read_to_string(&path).unwrap()));
                }
            }
        }
    }
    out
}

/// Where code in `sources` (as [`crate_sources`] reads them) names a [`DESCRIPTIVE`] field, other
/// than as `<field>:` in the toy's [`DECLARATION`]: code that may act on the field, so that an
/// entry's reason that no code here reads it is no longer true.
fn descriptive_readers(sources: &BTreeMap<String, String>) -> Vec<String> {
    let word_char = |c: char| c.is_alphanumeric() || c == '_';
    let mut problems = Vec::new();
    for field in DESCRIPTIVE {
        let name = field.rsplit('.').next().unwrap();
        for (path, code) in sources {
            for (at, _) in code.match_indices(name) {
                let after = &code[at + name.len()..];
                let whole = !code[..at].ends_with(word_char) && !after.starts_with(word_char);
                let declares = path == DECLARATION
                    && after
                        .trim_start()
                        .strip_prefix(':')
                        .is_some_and(|r| !r.starts_with(':'));
                if whole && !declares {
                    problems.push(format!(
                        "{field}: described as read by no code here, but {path} names {name}"
                    ));
                }
            }
        }
    }
    problems
}

/// The attributes directly above a function whose `fn` follows `before` (code, as [`code_only`]
/// leaves it), inside their `#[..]`, last first; qualifiers (`pub`, `async`) are passed over.
fn attributes(before: &str) -> Vec<&str> {
    let mut before = before.trim_end();
    for qualifier in ["async", "pub"] {
        before = before.strip_suffix(qualifier).unwrap_or(before).trim_end();
    }
    let mut attrs = Vec::new();
    while let Some(inner) = before.strip_suffix(']') {
        let mut depth = 1;
        let Some(open) = inner.char_indices().rev().find_map(|(i, c)| {
            depth += match c {
                ']' => 1,
                '[' => -1,
                _ => 0,
            };
            (depth == 0).then_some(i)
        }) else {
            break;
        };
        let Some(rest) = inner[..open].strip_suffix('#') else {
            break;
        };
        attrs.push(inner[open + 1..].trim());
        before = rest.trim_end();
    }
    attrs
}

// ---------------------------------------------------------------------------------------------
// The audit.
// ---------------------------------------------------------------------------------------------

/// Everything wrong with the table for what is `declared`: a declared key with no entry or an
/// entry that does not answer it, an entry for nothing declared, a listed thread with no entry or
/// an entry for one not listed, and every test named that the check does not run (`sources` as
/// [`sources`] reads them).
fn audit(
    declared: &BTreeMap<String, Kind>,
    caps: &[(&str, Entry)],
    listed: &[&str],
    threads: &[(&str, &str, &[&str])],
    sources: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let mut entries = BTreeMap::new();
    for &(key, entry) in caps {
        if entries.insert(key, entry).is_some() {
            problems.push(format!("{key}: two entries"));
        }
    }
    let named = |problems: &mut Vec<String>, what: &str, tests: &[&str]| {
        if tests.is_empty() {
            problems.push(format!("{what}: an entry naming no test"));
        }
        let missing = tests.iter().filter_map(|t| run_by_the_check(sources, t));
        problems.extend(missing.map(|why| format!("{what}: {why}")));
    };
    let tested = |key: &str| matches!(entries.get(key), Some(Tested(_)));
    for (key, kind) in declared {
        match (entries.get(key.as_str()), kind) {
            (None, _) => problems.push(format!("{key}: declared, with no entry")),
            (Some(Tested(tests)), _) => named(&mut problems, key, tests),
            (Some(Described(why)), _) if why.trim().is_empty() => {
                problems.push(format!("{key}: described with no reason"));
            }
            (Some(Described(_)), Kind::Choice | Kind::Number) => {
                let field = field_of(key);
                if !DESCRIPTIVE.contains(&field.as_str()) {
                    problems.push(format!(
                        "{key}: {field} does not describe the venue alone, so it needs a test"
                    ));
                }
            }
            (Some(Described(_)), Kind::Member(set)) => {
                let siblings = declared.iter().filter(|(k, m)| {
                    k != &key && *m == &Kind::Member(set.clone()) && tested(k.as_str())
                });
                if siblings.count() == 0 {
                    problems.push(format!("{key}: no member of {set} is tested"));
                }
            }
            (Some(Described(_)), kind) => {
                let what = match kind {
                    Kind::Flag => "a true flag",
                    Kind::Present => "a Some",
                    _ => "a list's element",
                };
                problems.push(format!("{key}: {what} needs a test, not a description"));
            }
            (Some(Awaiting(ticket)), _) => {
                problems.push(format!(
                    "{key}: no test exercises it yet; {ticket} brings one"
                ));
            }
        }
    }
    for key in entries.keys().filter(|k| !declared.contains_key(**k)) {
        problems.push(format!("{key}: an entry for nothing the toy declares"));
    }
    let rows: BTreeMap<&str, &[&str]> = threads.iter().map(|(t, _, tests)| (*t, *tests)).collect();
    if rows.len() != threads.len() {
        problems.push("a thread with two entries".to_owned());
    }
    for thread in listed.iter().filter(|t| !rows.contains_key(*t)) {
        problems.push(format!("{thread}: listed, with no entry"));
    }
    for (thread, tests) in &rows {
        if !listed.contains(thread) {
            problems.push(format!("{thread}: not a thread FBC-z6v lists"));
        }
        named(&mut problems, thread, tests);
    }
    problems
}

/// What [`audit`] finds wrong with the table for `caps`, with the code of this crate that names a
/// field the table describes ([`descriptive_readers`]) and what in its manifest can run other
/// tests than those the audit reads ([`manifest_problems`]).
fn audit_of(caps: &VenueCaps) -> Vec<String> {
    let mut problems = audit(&declared(caps), CAPS, &LISTED, THREADS, &sources());
    problems.extend(descriptive_readers(&crate_sources()));
    problems.extend(manifest_problems(&manifest()));
    problems
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_toys_declared_capabilities_and_the_listed_threads_each_map_to_tests_that_exist() {
    let problems = audit_of(&toy::caps());
    assert!(problems.is_empty(), "{problems:#?}");
}

/// What `audit_of` finds for `changed` that it does not find for the toy as declared.
fn found_for(changed: impl FnOnce(&mut VenueCaps)) -> BTreeSet<String> {
    let base: BTreeSet<String> = audit_of(&toy::caps()).into_iter().collect();
    let mut caps = toy::caps();
    changed(&mut caps);
    let found: BTreeSet<String> = audit_of(&caps).into_iter().collect();
    found.difference(&base).cloned().collect()
}

fn set(problems: &[&str]) -> BTreeSet<String> {
    problems.iter().map(|p| (*p).to_owned()).collect()
}

#[test]
fn a_capability_the_toy_comes_to_declare_has_no_entry_and_fails_the_audit() {
    let found = found_for(|caps| {
        let exec = caps.exec.as_mut().unwrap();
        let order = &mut exec.order;
        order.tifs = TagSet::of(&[TifTag::Gtc, TifTag::Ioc, TifTag::Fok]);
        order.cancel_all_account = Support::Native;
        order.amend.as_mut().unwrap().keeps_venue_id = true;
        order.amend.as_mut().unwrap().keeps_priority = Some(false);
        order.ack = AckModel::TwoPhase {
            risk_reject_window: Duration::from_millis(10),
        };
        order.cancel_on_disconnect = CancelOnDisconnect::AccountWide {
            offers_disarm: true,
        };
        order.snapshot_lag_bound = Duration::from_millis(250);
        order.sign_cost_hint_us = 40;
        // A parameter that changes declares nothing new.
        order.batch_place.as_mut().unwrap().max_items = 7;
        caps.md.ts_precision = Duration::from_millis(1);
        caps.matching.speed_bump = Some(SpeedBump {
            delay: Duration::from_millis(50),
            applies_to: SpeedBumpScope::TakersOnly,
        });
        caps.md.status = FeedSource::Poll;
        caps.md.max_conn_lifetime = Some(Duration::from_secs(3_600));
        caps.md.touch_sources.push(TouchSourceCaps {
            channel: "bbo",
            cadence: Cadence::Realtime,
            seq_domain: SeqDomain::Own,
            ts_kind: ExchTsKind::MatchingEngine,
            includes_channels: TagSet::of(&[Channel::Public]),
        });
    });
    let new = [
        "exec.order.tifs.Fok: declared, with no entry",
        "exec.order.cancel_all_account=Native: declared, with no entry",
        "exec.order.amend.keeps_venue_id: declared, with no entry",
        "exec.order.amend.keeps_priority: declared, with no entry",
        "exec.order.ack=TwoPhase: declared, with no entry",
        "exec.order.cancel_on_disconnect=AccountWide: declared, with no entry",
        "exec.order.snapshot_lag_bound: declared, with no entry",
        "exec.order.sign_cost_hint_us: declared, with no entry",
        "matching.speed_bump: declared, with no entry",
        "matching.speed_bump.applies_to=TakersOnly: declared, with no entry",
        "md.status=Poll: declared, with no entry",
        "md.max_conn_lifetime: declared, with no entry",
        "md.touch_sources[0]: declared, with no entry",
        "md.touch_sources[0].cadence=Realtime: declared, with no entry",
        "md.touch_sources[0].seq_domain=Own: declared, with no entry",
        "md.touch_sources[0].ts_kind=MatchingEngine: declared, with no entry",
        "md.touch_sources[0].includes_channels.Public: declared, with no entry",
        // What the changed choices no longer declare.
        "exec.order.ack=SinglePhase: an entry for nothing the toy declares",
        "exec.order.cancel_on_disconnect=PerConnection: an entry for nothing the toy declares",
        "exec.order.cancel_on_disconnect.rearm_on_reconnect: an entry for nothing the toy \
         declares",
    ];
    assert_eq!(found, set(&new));
}

#[test]
fn an_entry_for_nothing_declared_fails_the_audit() {
    // The toy declared without fill ids declares no fill id.
    let without = audit_of(&toy::caps_for(FillIds::Derived));
    let base = audit_of(&toy::caps());
    let found: Vec<&String> = without.iter().filter(|p| !base.contains(p)).collect();
    assert_eq!(
        found,
        ["exec.fills.fill_id: an entry for nothing the toy declares"]
    );
}

#[test]
fn an_entry_naming_a_test_that_does_not_exist_fails_the_audit() {
    let declared = declared(&toy::caps());
    let sources = sources();
    let key = "exec.order.post_only";
    let entry = |tests: &'static [&'static str]| {
        let mut caps: Vec<(&str, Entry)> =
            CAPS.iter().filter(|(k, _)| *k != key).copied().collect();
        caps.push((key, Tested(tests)));
        audit(&declared, &caps, &LISTED, THREADS, &sources)
    };
    let base = entry(&[ENCODES]);
    let found = |tests: &'static [&'static str]| -> Vec<String> {
        let all = entry(tests);
        all.into_iter().filter(|p| !base.contains(p)).collect()
    };
    assert_eq!(
        found(&["toy_order_entry::no_such_test"]),
        [
            "exec.order.post_only: toy_order_entry::no_such_test: there is no fn no_such_test in \
          tests/toy_order_entry.rs"
        ]
    );
    assert_eq!(
        found(&["no_such_file::a_test"]),
        ["exec.order.post_only: no_such_file::a_test: there is no tests/no_such_file.rs"]
    );
    // A helper of toy_books.rs, which is no test.
    assert_eq!(
        found(&["toy_books::sub"]),
        ["exec.order.post_only: toy_books::sub: fn sub is not a test"]
    );
    assert_eq!(
        found(&["a_test"]),
        ["exec.order.post_only: a_test: not <file>::<fn>"]
    );
    assert_eq!(
        found(&[]),
        ["exec.order.post_only: an entry naming no test"]
    );
    // A thread's entry likewise.
    let mut threads: Vec<(&str, &str, &[&str])> = THREADS.to_vec();
    threads[0].2 = &["toy_books::two_book_channels"];
    let found = audit(&declared, CAPS, &LISTED, &threads, &sources);
    assert!(
        found.contains(
            &"r4172231010: toy_books::two_book_channels: there is no fn two_book_channels in \
              tests/toy_books.rs"
                .to_owned()
        ),
        "{found:#?}"
    );
}

#[test]
fn a_listed_thread_with_no_entry_or_an_entry_for_one_not_listed_fails_the_audit() {
    let declared = declared(&toy::caps());
    let sources = sources();
    let base = audit(&declared, CAPS, &LISTED, THREADS, &sources);
    let mut threads: Vec<(&str, &str, &[&str])> = THREADS[1..].to_vec();
    threads.push(("r1", "a thread FBC-z6v does not list", &[TWO_BOOKS]));
    threads.push(("r4172835744", "the same thread twice", &[TWO_BOOKS]));
    let found = audit(&declared, CAPS, &LISTED, &threads, &sources);
    let found: Vec<String> = found.into_iter().filter(|p| !base.contains(p)).collect();
    assert_eq!(
        found,
        [
            "a thread with two entries",
            "r4172231010: listed, with no entry",
            "r1: not a thread FBC-z6v lists",
        ]
    );
}

#[test]
fn only_a_describing_fields_choice_or_number_or_a_member_beside_a_tested_one_may_be_described() {
    let declared = declared(&toy::caps());
    let sources = sources();
    let base = audit(&declared, CAPS, &LISTED, THREADS, &sources);
    // The table's own descriptions, each book channel's among them, pass, and nothing else in
    // the table is wrong: no entry awaits its test.
    assert!(base.is_empty(), "{base:#?}");
    let described = |keys: &[&'static str]| -> Vec<String> {
        let mut caps: Vec<(&str, Entry)> = CAPS
            .iter()
            .filter(|(k, _)| !keys.contains(k))
            .copied()
            .collect();
        caps.extend(keys.iter().map(|k| (*k, Described("described"))));
        let found = audit(&declared, &caps, &LISTED, THREADS, &sources);
        found.into_iter().filter(|p| !base.contains(p)).collect()
    };
    assert_eq!(
        described(&["exec.order.post_only", "exec.order.amend", "md.books[0]"]),
        [
            "exec.order.amend: a Some needs a test, not a description",
            "exec.order.post_only: a true flag needs a test, not a description",
            "md.books[0]: a list's element needs a test, not a description",
        ]
    );
    // A set none of whose members is tested.
    assert_eq!(
        described(&[
            "exec.order.query_refs.Venue",
            "exec.order.query_refs.PlacementNonce"
        ]),
        [
            "exec.order.query_refs.PlacementNonce: no member of exec.order.query_refs is tested",
            "exec.order.query_refs.Venue: no member of exec.order.query_refs is tested",
        ]
    );
    // One member beside a tested one may be, with its reason.
    assert!(described(&["exec.order.query_refs.Venue"]).is_empty());
    // A choice of a field that is not a description of the venue may not: a feed's source, an
    // order capability's choice.
    assert_eq!(
        described(&["md.stats=Poll", "exec.order.ack=SinglePhase"]),
        [
            "exec.order.ack=SinglePhase: exec.order.ack does not describe the venue alone, so it \
             needs a test",
            "md.stats=Poll: md.stats does not describe the venue alone, so it needs a test",
        ]
    );
    // Nor a number of one.
    let mut lagging = toy::caps();
    lagging.exec.as_mut().unwrap().order.snapshot_lag_bound = Duration::from_millis(250);
    let lag = "exec.order.snapshot_lag_bound";
    let mut caps: Vec<(&str, Entry)> = CAPS.to_vec();
    caps.push((lag, Described("described")));
    let found = audit(
        &crate::declared(&lagging),
        &caps,
        &LISTED,
        THREADS,
        &sources,
    );
    let found: Vec<String> = found.into_iter().filter(|p| !base.contains(p)).collect();
    assert_eq!(
        found,
        [format!(
            "{lag}: {lag} does not describe the venue alone, so it needs a test"
        )]
    );
    let mut caps: Vec<(&str, Entry)> = CAPS
        .iter()
        .filter(|(k, _)| *k != "readiness_ceiling=Record")
        .copied()
        .collect();
    caps.push(("readiness_ceiling=Record", Described(" ")));
    let found = audit(&declared, &caps, &LISTED, THREADS, &sources);
    let found: Vec<String> = found.into_iter().filter(|p| !base.contains(p)).collect();
    assert_eq!(
        found,
        ["readiness_ceiling=Record: described with no reason"]
    );
    // An entry awaiting its test is reported missing.
    let mut caps: Vec<(&str, Entry)> = CAPS
        .iter()
        .filter(|(k, _)| *k != "md.mark=Stream")
        .copied()
        .collect();
    caps.push(("md.mark=Stream", Awaiting("FBC-0")));
    let found = audit(&declared, &caps, &LISTED, THREADS, &sources);
    let found: Vec<String> = found.into_iter().filter(|p| !base.contains(p)).collect();
    assert_eq!(
        found,
        ["md.mark=Stream: no test exercises it yet; FBC-0 brings one"]
    );
}

#[test]
fn what_the_toy_declares_is_read_from_its_caps_by_kind_and_nothing_absent_is_read() {
    let declared = declared(&toy::caps());
    let member = |set: &str| Kind::Member(set.to_owned());
    for (key, kind) in [
        ("exec", Kind::Present),
        ("exec.order.post_only", Kind::Flag),
        ("exec.order.tifs.Ioc", member("exec.order.tifs")),
        ("exec.order.flag_conflicts=(PostOnly, Ioc)", Kind::Element),
        (
            "exec.order.cancel_on_disconnect=PerConnection",
            Kind::Choice,
        ),
        (
            "exec.order.cancel_on_disconnect.rearm_on_reconnect",
            Kind::Flag,
        ),
        ("exec.order.client_id=Alnum", Kind::Choice),
        ("md.books[1]", Kind::Element),
        ("md.books[1].rest_anchor", Kind::Flag),
        (
            "md.books[1].includes_channels.Rpi",
            member("md.books[1].includes_channels"),
        ),
        ("md.stats=Poll", Kind::Choice),
        ("md.ts_precision", Kind::Number),
        ("limits[0].ops.Control", member("limits[0].ops")),
    ] {
        assert_eq!(declared.get(key), Some(&kind), "{key}");
    }
    // Nothing false, absent, empty, zero or a parameter is a key.
    for absent in [
        "exec.order.amend.keeps_venue_id",
        "exec.order.amend.keeps_priority",
        "exec.order.cancel_all_account=Unsupported",
        "exec.order.cancel_on_disconnect.covers_open_orders",
        "exec.order.snapshot_lag_bound",
        "exec.order.sign_cost_hint_us",
        "exec.order.batch_place.max_items",
        "matching.speed_bump",
        "md.status=None",
        "md.touch_sources",
        "md.books[0].rest_anchor",
        "md.books[0].max_depth",
        "md.books[0].channel",
        "md.topology.max_subscriptions",
        "limits[0].per",
        "limits[0].units",
        "exec.order.client_id.max_len",
        "md.max_conn_lifetime",
        "exec_limits",
    ] {
        let prefixed = declared.keys().find(|k| k.starts_with(absent));
        assert_eq!(prefixed, None, "{absent}");
    }
    // Every struct of the caps is a part, never a choice.
    assert!(
        declared.keys().all(|k| !k.contains("Caps")),
        "{declared:#?}"
    );
    // The table answers exactly what is declared.
    let keys: BTreeSet<&str> = CAPS.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, declared.keys().map(String::as_str).collect());
}

#[test]
fn a_test_is_found_at_the_top_under_its_test_attribute_unless_it_is_ignored_or_configured() {
    let source = r##"
use x;

/// A doc comment, with [`a link`] and fn in_a_doc() {
#[test]
fn plain() {}

#[tokio::test(start_paused = true)]
// A comment.
async fn paused() {}

#[test]
#[should_panic(
    expected = "a [bracket] and a } brace"
)]
fn panics() {}

#[test]
#[ignore = "slow"]
fn ignored() {}

fn helper() {}

#[cfg(test)]
fn configured() {}

#[test]
#[cfg(target_os = "none")]
fn gated() {}

mod nested {
    #[test]
    fn inner() {}
}

/* #[test]
fn commented() {} /* nested */ { */

const QUOTED: &str = "#[test]
fn quoted() { \" {";
const RAW: &str = r#"#[test] fn raw() { "{"#;
const BRACE: char = '{';
const ESCAPED: char = '\'';
fn lifetime<'a>(x: &'a str) -> &'a str { x }

#[test]
#[cfg_attr(miri, ignore)]
fn sometimes_ignored() {}

tests! [
    #[test]
    fn in_brackets() {}
];

tests!(
    #[test]
    fn in_parens() {}
);

#[test]
fn after_them() {}
"##;
    let sources = BTreeMap::from([("t".to_owned(), code_only(source))]);
    for found in ["t::plain", "t::paused", "t::panics", "t::after_them"] {
        assert_eq!(run_by_the_check(&sources, found), None, "{found}");
    }
    for (missing, why) in [
        (
            "t::ignored",
            "fn ignored is ignored, so the check never runs it",
        ),
        ("t::helper", "fn helper is not a test"),
        ("t::configured", "fn configured is not a test"),
        ("t::gated", "fn gated is built only where its cfg holds"),
        (
            "t::sometimes_ignored",
            "fn sometimes_ignored has a cfg_attr, which can ignore it or leave it out",
        ),
        (
            "t::inner",
            "fn inner is not at the top level of tests/t.rs, so no test is named t::inner",
        ),
        (
            "t::in_brackets",
            "fn in_brackets is not at the top level of tests/t.rs, so no test is named \
             t::in_brackets",
        ),
        (
            "t::in_parens",
            "fn in_parens is not at the top level of tests/t.rs, so no test is named \
             t::in_parens",
        ),
        ("t::in_a_doc", "there is no fn in_a_doc in tests/t.rs"),
        ("t::commented", "there is no fn commented in tests/t.rs"),
        ("t::quoted", "there is no fn quoted in tests/t.rs"),
        ("t::raw", "there is no fn raw in tests/t.rs"),
        ("t::pla", "there is no fn pla in tests/t.rs"),
    ] {
        let found = run_by_the_check(&sources, missing);
        assert_eq!(found, Some(format!("{missing}: {why}")));
    }
    // This file's fixture above, read from the directory as every entry is, names no test.
    let sources = crate::sources();
    for func in ["plain", "paused", "panics", "after_them"] {
        let name = format!("toy_caps_audit::{func}");
        assert_eq!(
            run_by_the_check(&sources, &name),
            Some(format!(
                "{name}: there is no fn {func} in tests/toy_caps_audit.rs"
            ))
        );
    }
}

#[test]
fn a_file_that_opens_with_a_cfg_names_no_test_and_one_with_another_inner_attribute_does() {
    let sources = BTreeMap::from([
        (
            "gated".to_owned(),
            code_only(
                r#"//! A doc comment.
#![cfg(feature = "x")]
#[test]
fn plain() {}
"#,
            ),
        ),
        (
            "marked".to_owned(),
            code_only(
                r#"#![allow(dead_code)]
#![cfg_attr(miri, allow(unused))]
#[test]
fn plain() {}
"#,
            ),
        ),
        (
            "allowed".to_owned(),
            code_only(
                r#"#![allow(dead_code)]

#[test]
fn plain() {}
"#,
            ),
        ),
    ]);
    assert_eq!(run_by_the_check(&sources, "allowed::plain"), None);
    for file in ["gated", "marked"] {
        let name = format!("{file}::plain");
        assert_eq!(
            run_by_the_check(&sources, &name),
            Some(format!(
                "{name}: tests/{file}.rs opens with a #![cfg] or #![cfg_attr], which can leave it \
                 out"
            ))
        );
    }
}

#[test]
fn a_test_target_or_autotests_in_the_manifest_fails_the_audit() {
    assert_eq!(manifest_problems(&manifest()), Vec::<String>::new());
    let manifest = r#"[package]
name = "x"
# [[test]] in a comment
autotests = false

[[ test ]]
name = "y"
required-features = ["z"]
"#;
    assert_eq!(
        manifest_problems(manifest),
        [
            "Cargo.toml sets autotests, which can change which files in tests/ run as tests",
            "Cargo.toml declares a [[test]] target, which can change which files in tests/ run as \
             tests",
        ]
    );
}

#[test]
fn code_of_this_crate_naming_a_described_field_fails_the_audit_but_the_toys_declaration_does_not() {
    // The crate's code (the toy's declaration, the suite and the tests among it, but not this
    // file) names no described field.
    let all = crate_sources();
    for path in [DECLARATION, "src/suite/mod.rs", "tests/suite_toy.rs"] {
        assert!(all.contains_key(path), "{path}: {:?}", all.keys());
    }
    assert!(!all.contains_key(THIS_FILE));
    assert_eq!(descriptive_readers(&all), Vec::<String>::new());
    let sources = BTreeMap::from([
        (
            DECLARATION.to_owned(),
            code_only(
                "BookCaps { cadence: Cadence::Realtime, queue_model : Q::BracketOnly }; \
                 let lag = caps.md.ts_precision;",
            ),
        ),
        (
            "src/suite/reads.rs".to_owned(),
            code_only(
                r#"// The venue's cadence and stp_scope.
let s = "stp_scope";
let my_cadence_x = 1;
if caps.readiness_ceiling == Readiness::Record {}
let BookCaps { queue_model, .. } = book;
"#,
            ),
        ),
        (
            "tests/declares.rs".to_owned(),
            code_only("let caps = BookCaps { cadence: Cadence::Snapshot };"),
        ),
    ]);
    assert_eq!(
        descriptive_readers(&sources),
        [
            "md.books[].cadence: described as read by no code here, but tests/declares.rs names \
             cadence",
            "md.books[].queue_model: described as read by no code here, but src/suite/reads.rs \
             names queue_model",
            "md.ts_precision: described as read by no code here, but src/toy/mod.rs names \
             ts_precision",
            "readiness_ceiling: described as read by no code here, but src/suite/reads.rs names \
             readiness_ceiling",
        ]
    );
}

#[test]
fn a_number_is_a_key_where_a_caps_part_holds_it_and_otherwise_a_parameter() {
    let declared = declared_text(
        "VenueCaps { a: Some(AmendCaps { n: 3, z: 0 }), b: [BookCaps { d: 5 }], \
         c: Some(Batch { m: 2 }), d: Some(250ms), e: Alnum { max_len: 36 }, \
         f: PartCaps { lag: 1ms, off: 0ns } }",
    );
    let keys: Vec<(&str, &Kind)> = declared.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(
        keys,
        [
            ("a", &Kind::Present),
            ("a.n", &Kind::Number),
            ("b[0]", &Kind::Element),
            ("c", &Kind::Present),
            ("d", &Kind::Present),
            ("e=Alnum", &Kind::Choice),
            ("f.lag", &Kind::Number),
        ]
    );
}
