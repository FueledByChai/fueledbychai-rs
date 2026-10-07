//! FBC-0l9 (decisions 0005, 0009, 0014, 0054, 0069): Paradex's JSON-RPC replies to the order
//! methods decoded into one `ExecEvent::Outcome` per item, through the core's `DecodeScope`
//! only. A create's order (or bare id) reply, create_batch's results (an order or an error per
//! item, a mixed batch answered in one call), modify's order (provisional: the amend is final
//! on its order event, as 0054 decides), cancel's and cancel_batch's statuses (ALREADY_CLOSED
//! and NOT_FOUND refusals that leave the order as it was, never terminal), cancel_all's and
//! cancel_on_disconnect's answers. Each error code docs.paradex.trade's WebSocket "Error
//! Handling" page documents as a refusal maps to its `RejectKind` by code; an internal error
//! and an undocumented code are `Unknown`, as is the timeout of a request no reply answered,
//! and an error frame with no id is an `UncorrelatedError`.
//!
//! The frames are the hand-built ones in `fixtures/paradex/exec/` (SYNTHETIC).

mod md;

use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use fbc_core::{
    AccountKey, AckLevel, CancelOrder, CancelScope, Channel, CidMint, ClientOrderId, DecodeError,
    ExecEvent, ExecSink, ItemRef, Lots, Namespace, NamespaceLease, NewOrder, OrderKind, OrderRef,
    Reject, RejectKind, RpcId, Side, SubmitOutcome, TerminalHint, TerminalReject, Ticks, Tif,
    VenueCommand, VenueMeta, VenueOrderId, WallNs, dispatch,
};
use fbc_venue_paradex::exec::{ParadexReplies, REJECT_CODES, ReplyRead};
use fbc_venue_paradex::factory::caps_with_order_entry;
use md::BTC;

const OWN: Namespace = Namespace::new(7);
const OID: &str = "1759500000000000001";
const OID2: &str = "1759500000000000002";
const OID3: &str = "1759500000000000003";

/// The text of exec fixture `name`.
fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/exec")
        .join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The first two client ids minted in [`OWN`]: `01000700-199b-81ab-8200-00054d0aa3f5` and
/// `...-00099b9bf518`, the ones the fixtures carry.
fn cid(n: usize) -> ClientOrderId {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-paradex-replies-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(3), OWN).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_622_400_000_000_000));
        let ids = (0..2).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        ids
    })[n]
}

fn vid(wire: &str) -> VenueOrderId {
    dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.venue_order_id(wire)
    })
    .unwrap()
}

fn order(n: usize) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst: BTC,
        side: Side::Buy,
        qty: Lots::new(150).unwrap(),
        kind: OrderKind::Limit { px: Ticks(620_000) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

fn cancel(target: OrderRef) -> CancelOrder {
    CancelOrder {
        target,
        inst: BTC,
        side: Side::Buy,
        placement_nonce: None,
    }
}

fn amend(target: OrderRef) -> fbc_core::AmendOrder {
    fbc_core::AmendOrder {
        target,
        inst: BTC,
        side: Side::Buy,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(619_995),
        qty: Lots::new(200).unwrap(),
        cum_filled: Lots::ZERO,
    }
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        assert_eq!(
            meta,
            VenueMeta::NONE,
            "a reply states no venue time or sequence"
        );
        self.0.push(ev);
    }
}

/// `replies` reading `text`: what it returned and every event it pushed.
fn read(
    replies: &mut ParadexReplies,
    text: &str,
) -> (Result<ReplyRead, DecodeError>, Vec<ExecEvent>) {
    let mut sink = Sink::default();
    let result = dispatch(&caps_with_order_entry(), OWN, |scope| {
        replies.on_reply(text, scope, &mut sink)
    });
    (result, sink.0)
}

/// The events `replies` decodes `text` into, asserting it read the frame as an answer.
fn answered(replies: &mut ParadexReplies, text: &str) -> Vec<ExecEvent> {
    let (result, events) = read(replies, text);
    assert_eq!(result, Ok(ReplyRead::Decoded), "{text}");
    events
}

/// Asserts `text` is refused with nothing pushed.
fn refused(replies: &mut ParadexReplies, text: &str) -> DecodeError {
    let (result, events) = read(replies, text);
    assert!(events.is_empty(), "a refused reply pushed {events:?}");
    result.expect_err("the reply is refused")
}

/// What `replies` pushes when request `rpc`'s deadline passes.
fn timed_out(replies: &mut ParadexReplies, rpc: u64) -> Vec<ExecEvent> {
    let mut sink = Sink::default();
    replies.on_rpc_timeout(RpcId(rpc), &mut sink);
    sink.0
}

/// A tracker that has sent `cmd` as request `rpc`.
fn sent(rpc: u64, cmd: VenueCommand) -> ParadexReplies {
    let mut replies = ParadexReplies::new();
    replies.sent(RpcId(rpc), &cmd);
    replies
}

fn item(idx: u16, cid: Option<ClientOrderId>, vid: Option<VenueOrderId>) -> Option<ItemRef> {
    Some(ItemRef { idx, cid, vid })
}

fn outcome(rpc: u64, item: Option<ItemRef>, outcome: SubmitOutcome) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item,
        outcome,
    }
}

const PROVISIONAL: SubmitOutcome = SubmitOutcome::Accepted {
    ack: AckLevel::Provisional,
};

fn rejected(kind: RejectKind, code: Option<&str>, raw: &str) -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind,
        venue_code: code.map(Into::into),
        raw: raw.into(),
    })
}

fn unknown_whole(rpc: u64) -> Vec<ExecEvent> {
    vec![outcome(rpc, None, SubmitOutcome::Unknown)]
}

#[test]
fn a_create_reply_is_one_provisional_acceptance_naming_the_order_s_venue_id() {
    let mut replies = sent(11, VenueCommand::Place(order(0)));
    let events = answered(&mut replies, &fixture("reply-create.json"));
    // Accepted, not final: Paradex queues an order for its risk check (0054's TwoPhase).
    assert_eq!(
        events,
        vec![outcome(
            11,
            item(0, Some(cid(0)), Some(vid(OID))),
            PROVISIONAL
        )]
    );
}

#[test]
fn a_create_reply_with_a_bare_id_is_accepted_under_that_id() {
    let mut replies = sent(12, VenueCommand::Place(order(0)));
    let events = answered(&mut replies, &fixture("reply-create-bare-id.json"));
    assert_eq!(
        events,
        vec![outcome(
            12,
            item(0, Some(cid(0)), Some(vid(OID))),
            PROVISIONAL
        )]
    );
}

#[test]
fn a_create_reply_naming_another_client_id_or_no_order_id_is_refused() {
    let text = fixture("reply-create.json");
    // Request 11 was the second order: the reply names the first.
    let mut replies = sent(11, VenueCommand::Place(order(1)));
    assert_eq!(
        refused(&mut replies, &text),
        DecodeError::Malformed("the reply names another client id")
    );
    // Refused, the request still waits: its timeout is Unknown.
    assert_eq!(timed_out(&mut replies, 11), unknown_whole(11));

    let mut replies = sent(12, VenueCommand::Place(order(0)));
    let no_id = fixture("reply-create-bare-id.json")
        .replace(r#""id":"1759500000000000001""#, r#""idx":"1""#);
    assert!(matches!(
        refused(&mut replies, &no_id),
        DecodeError::Malformed(_)
    ));
}

#[test]
fn a_mixed_batch_is_answered_item_by_item_in_one_call() {
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    let events = answered(&mut replies, &fixture("reply-create-batch-mixed.json"));
    assert_eq!(
        events,
        vec![
            outcome(13, item(0, Some(cid(0)), Some(vid(OID))), PROVISIONAL),
            // The item's error is a message without a code: no page says the venue left the
            // item undone, so it is Unknown, as an undocumented code is (DeepSeek DS-2).
            outcome(13, item(1, Some(cid(1)), None), SubmitOutcome::Unknown),
        ]
    );
}

#[test]
fn a_batch_whose_every_item_errors_is_unknown_item_by_item_and_its_timeout_adds_nothing() {
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    let text = r#"{"jsonrpc":"2.0","result":{"results":[{"error":"synthetic a"},{"error":"synthetic b"}]},"id":13}"#;
    assert_eq!(
        answered(&mut replies, text),
        vec![
            outcome(13, item(0, Some(cid(0)), None), SubmitOutcome::Unknown),
            outcome(13, item(1, Some(cid(1)), None), SubmitOutcome::Unknown),
        ]
    );
    assert_eq!(timed_out(&mut replies, 13), vec![]);
}

#[test]
fn a_batch_reply_short_of_items_is_unknown_only_for_the_items_it_leaves_unanswered() {
    let mut replies = sent(14, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    let events = answered(&mut replies, &fixture("reply-create-batch-short.json"));
    assert_eq!(
        events,
        vec![
            outcome(14, item(0, Some(cid(0)), Some(vid(OID))), PROVISIONAL),
            outcome(14, item(1, Some(cid(1)), None), SubmitOutcome::Unknown),
        ]
    );
}

#[test]
fn a_batch_reply_with_more_results_than_items_or_an_item_neither_order_nor_error_is_refused() {
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0)]));
    let text = fixture("reply-create-batch-mixed.json");
    assert_eq!(
        refused(&mut replies, &text),
        DecodeError::Malformed("more results than items")
    );
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    let neither = text.replace(r#"{"error":"synthetic: order rejected"}"#, "{}");
    assert!(matches!(
        refused(&mut replies, &neither),
        DecodeError::Malformed(_)
    ));
    assert_eq!(timed_out(&mut replies, 13), unknown_whole(13));
}

#[test]
fn an_accepted_modify_is_provisional_and_confirms_no_amend() {
    // 0054: `AmendAck::ReplacedEvent`: the reply is not the amend's confirmation; the order
    // event reporting SUCCESS for MODIFY_ORDER is. No order update comes from the reply.
    let target = OrderRef::Both(cid(0), vid(OID));
    let mut replies = sent(15, VenueCommand::Amend(amend(target)));
    let events = answered(&mut replies, &fixture("reply-modify.json"));
    assert_eq!(
        events,
        vec![outcome(
            15,
            item(0, Some(cid(0)), Some(vid(OID))),
            PROVISIONAL
        )]
    );
}

#[test]
fn a_modify_reply_naming_another_order_is_refused() {
    let mut replies = sent(15, VenueCommand::Amend(amend(OrderRef::Venue(vid(OID2)))));
    assert_eq!(
        refused(&mut replies, &fixture("reply-modify.json")),
        DecodeError::Malformed("the reply names another order")
    );
}

#[test]
fn a_queued_cancel_is_provisional_by_venue_id_or_client_id() {
    // QUEUED_FOR_CANCELLATION is not the cancel done: the order event that closes the order is.
    let mut replies = sent(16, VenueCommand::Cancel(cancel(OrderRef::Venue(vid(OID)))));
    let events = answered(&mut replies, &fixture("reply-cancel.json"));
    assert_eq!(
        events,
        vec![outcome(16, item(0, None, Some(vid(OID))), PROVISIONAL)]
    );

    // By client id, the reply's order id is the item's venue id.
    let mut replies = sent(16, VenueCommand::Cancel(cancel(OrderRef::Client(cid(0)))));
    let events = answered(&mut replies, &fixture("reply-cancel.json"));
    assert_eq!(
        events,
        vec![outcome(
            16,
            item(0, Some(cid(0)), Some(vid(OID))),
            PROVISIONAL
        )]
    );

    // A reply naming another order than the one cancelled by venue id is refused.
    let mut replies = sent(16, VenueCommand::Cancel(cancel(OrderRef::Venue(vid(OID2)))));
    assert_eq!(
        refused(&mut replies, &fixture("reply-cancel.json")),
        DecodeError::Malformed("the reply names another order")
    );
}

#[test]
fn a_cancel_reply_with_a_status_no_page_documents_is_unknown_and_its_timeout_adds_nothing() {
    let mut replies = sent(16, VenueCommand::Cancel(cancel(OrderRef::Venue(vid(OID)))));
    let text = fixture("reply-cancel.json").replace("QUEUED_FOR_CANCELLATION", "SOMETHING_ELSE");
    let events = answered(&mut replies, &text);
    assert_eq!(
        events,
        vec![outcome(
            16,
            item(0, None, Some(vid(OID))),
            SubmitOutcome::Unknown
        )]
    );
    // Every item was reported Unknown already: none is left unanswered.
    assert_eq!(timed_out(&mut replies, 16), vec![]);
}

#[test]
fn a_cancel_batch_reports_each_order_s_status_and_its_refusals_leave_the_order_as_it_was() {
    let cancels = vec![
        cancel(OrderRef::Both(cid(0), vid(OID))),
        cancel(OrderRef::Venue(vid(OID2))),
        cancel(OrderRef::Venue(vid(OID3))),
    ];
    let mut replies = sent(17, VenueCommand::CancelMany(cancels));
    let events = answered(&mut replies, &fixture("reply-cancel-batch.json"));
    let closed = RejectKind::AlreadyTerminal(TerminalHint::Unspecified);
    assert_eq!(
        events,
        vec![
            outcome(17, item(0, Some(cid(0)), Some(vid(OID))), PROVISIONAL),
            outcome(
                17,
                item(1, None, Some(vid(OID2))),
                rejected(closed, Some("ALREADY_CLOSED"), "ALREADY_CLOSED")
            ),
            outcome(
                17,
                item(2, None, Some(vid(OID3))),
                rejected(RejectKind::NotFound, Some("NOT_FOUND"), "NOT_FOUND")
            ),
        ]
    );
    // Neither refusal can end an order (0014 item 6): NotFound sends it to the Unknown ladder,
    // AlreadyTerminal waits for its terminal event.
    assert_eq!(TerminalReject::new(closed), None);
    assert_eq!(TerminalReject::new(RejectKind::NotFound), None);
}

#[test]
fn a_cancel_batch_reply_naming_another_order_at_an_index_is_refused() {
    let cancels = vec![
        cancel(OrderRef::Venue(vid(OID2))),
        cancel(OrderRef::Venue(vid(OID))),
        cancel(OrderRef::Venue(vid(OID3))),
    ];
    let mut replies = sent(17, VenueCommand::CancelMany(cancels));
    assert_eq!(
        refused(&mut replies, &fixture("reply-cancel-batch.json")),
        DecodeError::Malformed("the reply names another order")
    );
}

#[test]
fn a_cancel_all_is_provisional_and_cancel_on_disconnect_is_final_only_as_asked() {
    let mut replies = sent(18, VenueCommand::CancelAll(CancelScope::Instrument(BTC)));
    let events = answered(&mut replies, &fixture("reply-cancel-all.json"));
    assert_eq!(events, vec![outcome(18, item(0, None, None), PROVISIONAL)]);

    let mut replies = sent(19, VenueCommand::ArmCancelOnDisconnect(true));
    let events = answered(&mut replies, &fixture("reply-cancel-on-disconnect.json"));
    let fin = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    assert_eq!(events, vec![outcome(19, item(0, None, None), fin)]);

    // Asked to turn it off, a reply that it is on answers another request: refused, and the
    // request's timeout is Unknown, so a protection never counts as changed when it was not.
    let mut replies = sent(19, VenueCommand::ArmCancelOnDisconnect(false));
    let text = fixture("reply-cancel-on-disconnect.json");
    assert_eq!(
        refused(&mut replies, &text),
        DecodeError::Malformed("cancel-on-disconnect is not as asked")
    );
    assert_eq!(timed_out(&mut replies, 19), unknown_whole(19));
}

#[test]
fn a_cancel_all_answered_other_than_ok_is_unknown() {
    let mut replies = sent(18, VenueCommand::CancelAll(CancelScope::Instrument(BTC)));
    let text = fixture("reply-cancel-all.json").replace(r#""ok""#, r#""later""#);
    let events = answered(&mut replies, &text);
    assert_eq!(
        events,
        vec![outcome(18, item(0, None, None), SubmitOutcome::Unknown)]
    );
}

/// Each code docs.paradex.trade's WebSocket "Error Handling" page lists that refuses a
/// request, with the kind it maps to, written out here apart from the table; the
/// cancel_batch statuses ALREADY_CLOSED and NOT_FOUND are its method page's.
const DOCUMENTED: [(&str, RejectKind); 10] = [
    ("-32700", RejectKind::Other),
    ("-32600", RejectKind::Other),
    ("-32601", RejectKind::Unsupported),
    ("-32602", RejectKind::Other),
    ("100", RejectKind::Other),
    ("40110", RejectKind::Other),
    ("40111", RejectKind::Other),
    ("40112", RejectKind::Other),
    (
        "ALREADY_CLOSED",
        RejectKind::AlreadyTerminal(TerminalHint::Unspecified),
    ),
    ("NOT_FOUND", RejectKind::NotFound),
];

#[test]
fn the_reject_table_maps_each_documented_code_to_its_kind() {
    assert_eq!(REJECT_CODES, DOCUMENTED);
}

#[test]
fn each_documented_error_code_refuses_the_whole_request_as_its_reject_kind() {
    for (code, kind) in DOCUMENTED {
        // The two statuses are cancel_batch's per item, never a JSON-RPC error code.
        let Ok(number) = code.parse::<i64>() else {
            continue;
        };
        let mut replies = sent(31, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
        let text = format!(
            r#"{{"jsonrpc":"2.0","error":{{"code":{number},"message":"synthetic {code}"}},"id":31}}"#
        );
        let events = answered(&mut replies, &text);
        let raw = format!("synthetic {code}");
        assert_eq!(
            events,
            vec![outcome(31, None, rejected(kind, Some(code), &raw))],
            "{code}"
        );
    }
}

#[test]
fn a_method_error_refuses_a_cancel_and_leaves_the_order_as_it_was() {
    let mut replies = sent(20, VenueCommand::Cancel(cancel(OrderRef::Venue(vid(OID)))));
    let events = answered(&mut replies, &fixture("error-method.json"));
    assert_eq!(
        events,
        vec![outcome(
            20,
            None,
            rejected(RejectKind::Other, Some("100"), "method error")
        )]
    );
    // A refusal is the command's outcome: no order update comes of it.
    assert!(!events.iter().any(|e| matches!(e, ExecEvent::Order(_))));
}

#[test]
fn an_internal_error_or_an_undocumented_code_is_unknown_and_its_timeout_adds_nothing() {
    for (rpc, name) in [(21, "error-internal.json"), (22, "error-undocumented.json")] {
        let mut replies = sent(rpc, VenueCommand::Place(order(0)));
        let events = answered(&mut replies, &fixture(name));
        // The venue may have acted: Unknown, resolved by the Unknown ladder, never resent.
        assert_eq!(events, unknown_whole(rpc), "{name}");
        assert_eq!(timed_out(&mut replies, rpc), vec![], "{name}");
    }
}

#[test]
fn an_error_frame_with_no_id_is_an_uncorrelated_error_and_answers_no_request() {
    let mut replies = sent(11, VenueCommand::Place(order(0)));
    let events = answered(&mut replies, &fixture("error-no-id.json"));
    let parse = Reject {
        kind: RejectKind::Other,
        venue_code: Some("-32700".into()),
        raw: "Parse error".into(),
    };
    assert_eq!(events, vec![ExecEvent::UncorrelatedError(parse)]);

    let events = answered(&mut replies, &fixture("error-null-id.json"));
    let token = Reject {
        kind: RejectKind::Other,
        venue_code: Some("40111".into()),
        raw: "Invalid Bearer Token".into(),
    };
    assert_eq!(events, vec![ExecEvent::UncorrelatedError(token)]);

    // An undocumented code with no id keeps its code, as Other.
    let text = fixture("error-no-id.json").replace("-32700", "4290");
    match answered(&mut replies, &text).as_slice() {
        [ExecEvent::UncorrelatedError(r)] => {
            assert_eq!(r.kind, RejectKind::Other);
            assert_eq!(r.venue_code.as_deref(), Some("4290"));
        }
        other => panic!("{other:?}"),
    }

    // The order's request still waits: its own reply is still read.
    let events = answered(&mut replies, &fixture("reply-create.json"));
    assert_eq!(events.len(), 1);
}

#[test]
fn the_timeout_of_an_unanswered_request_is_unknown_for_every_item() {
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    assert_eq!(timed_out(&mut replies, 13), unknown_whole(13));
    // A request the tracker never sent is Unknown too: it holds no outcome for it.
    assert_eq!(timed_out(&mut replies, 99), unknown_whole(99));
}

#[test]
fn a_reply_to_no_order_request_is_not_this_tracker_s() {
    let mut replies = sent(11, VenueCommand::Place(order(0)));
    for text in [
        // The auth frame's reply (id 0), a subscription's, a channel message.
        r#"{"jsonrpc":"2.0","result":{},"id":0}"#,
        r#"{"jsonrpc":"2.0","result":{"channel":"orders.ALL"},"id":12}"#,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"orders.ALL","data":{}}}"#,
        r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid parameters"},"id":"sub-1"}"#,
    ] {
        let (result, events) = read(&mut replies, text);
        assert_eq!(result, Ok(ReplyRead::NotOurs), "{text}");
        assert!(events.is_empty(), "{text}");
    }
    // A query, a fee query and a dead-man refresh are never this socket's requests.
    replies.sent(RpcId(40), &VenueCommand::FeeQuery);
    let (result, _) = read(&mut replies, r#"{"jsonrpc":"2.0","result":{},"id":40}"#);
    assert_eq!(result, Ok(ReplyRead::NotOurs));
}

#[test]
fn a_reply_that_is_not_a_json_rpc_answer_is_refused_with_nothing_pushed() {
    let mut replies = sent(11, VenueCommand::Place(order(0)));
    for text in [
        "not json",
        r#"{"jsonrpc":"2.0","result":{"id":"1"},"error":{"code":100,"message":"x"},"id":11}"#,
        r#"{"jsonrpc":"2.0","result":null,"id":11}"#,
        r#"{"jsonrpc":"2.0","error":{"message":"no code"},"id":11}"#,
        r#"{"jsonrpc":"2.0","error":{"code":"100","message":"a string code"}}"#,
        r#"{"jsonrpc":"2.0","result":{"order":{"id":""}},"id":11}"#,
    ] {
        assert!(
            matches!(
                refused(&mut replies, text),
                DecodeError::Malformed(_) | DecodeError::IdRefused(_)
            ),
            "{text}"
        );
    }
    // Still waiting: the real reply is read.
    assert_eq!(
        answered(&mut replies, &fixture("reply-create.json")).len(),
        1
    );
}

#[test]
fn a_cancel_batch_or_cancel_reply_missing_its_fields_is_refused() {
    let cancels = vec![cancel(OrderRef::Venue(vid(OID)))];
    let mut replies = sent(17, VenueCommand::CancelMany(cancels));
    for text in [
        r#"{"jsonrpc":"2.0","result":{},"id":17}"#,
        r#"{"jsonrpc":"2.0","result":{"results":[{"status":"NOT_FOUND"}]},"id":17}"#,
        r#"{"jsonrpc":"2.0","result":{"results":[{"id":"1759500000000000001"}]},"id":17}"#,
    ] {
        assert!(
            matches!(refused(&mut replies, text), DecodeError::Malformed(_)),
            "{text}"
        );
    }
    let mut replies = sent(16, VenueCommand::Cancel(cancel(OrderRef::Client(cid(0)))));
    let text = r#"{"jsonrpc":"2.0","result":{"order_id":"1759500000000000001"},"id":16}"#;
    assert!(matches!(
        refused(&mut replies, text),
        DecodeError::Malformed(_)
    ));
    // Without an order id, a cancel by client id is still answered: no venue id is learned.
    let text = r#"{"jsonrpc":"2.0","result":{"status":"QUEUED_FOR_CANCELLATION"},"id":16}"#;
    assert_eq!(
        answered(&mut replies, text),
        vec![outcome(16, item(0, Some(cid(0)), None), PROVISIONAL)]
    );
    let mut replies = sent(19, VenueCommand::ArmCancelOnDisconnect(true));
    let text = r#"{"jsonrpc":"2.0","result":{},"id":19}"#;
    assert!(matches!(
        refused(&mut replies, text),
        DecodeError::Malformed(_)
    ));
    let mut replies = sent(18, VenueCommand::CancelAll(CancelScope::Account));
    let text = r#"{"jsonrpc":"2.0","result":{},"id":18}"#;
    assert!(matches!(
        refused(&mut replies, text),
        DecodeError::Malformed(_)
    ));
}

#[test]
fn a_command_the_encoder_refuses_is_never_held() {
    let mut replies = ParadexReplies::default();
    let query = fbc_core::QueryOrder {
        target: OrderRef::Client(cid(0)),
        inst: BTC,
        placement_nonce: None,
    };
    let refused_cmds = [
        // An amend by client id only, and a batch cancel with a client-id item: no reference
        // the encoder writes for them.
        VenueCommand::Amend(amend(OrderRef::Client(cid(0)))),
        VenueCommand::CancelMany(vec![
            cancel(OrderRef::Venue(vid(OID))),
            cancel(OrderRef::Client(cid(1))),
        ]),
        VenueCommand::Query(query),
        VenueCommand::RefreshDeadMan,
        VenueCommand::FeeQuery,
    ];
    for (rpc, cmd) in (50..).zip(refused_cmds) {
        replies.sent(RpcId(rpc), &cmd);
        let text = format!(r#"{{"jsonrpc":"2.0","result":{{"status":"ok"}},"id":{rpc}}}"#);
        assert_eq!(
            read(&mut replies, &text).0,
            Ok(ReplyRead::NotOurs),
            "{cmd:?}"
        );
    }
    assert_eq!(format!("{replies:?}"), "ParadexReplies { requests: 0, .. }");
}

#[test]
fn a_modify_reply_naming_another_client_id_or_a_batch_error_that_is_not_text_is_refused() {
    let target = OrderRef::Both(cid(1), vid(OID));
    let mut replies = sent(15, VenueCommand::Amend(amend(target)));
    assert_eq!(
        refused(&mut replies, &fixture("reply-modify.json")),
        DecodeError::Malformed("the reply names another client id")
    );
    let mut replies = sent(13, VenueCommand::PlaceBatch(vec![order(0), order(1)]));
    let text = fixture("reply-create-batch-mixed.json")
        .replace(r#""synthetic: order rejected""#, r#"{"code":1}"#);
    assert_eq!(
        refused(&mut replies, &text),
        DecodeError::Malformed("item error")
    );
}
