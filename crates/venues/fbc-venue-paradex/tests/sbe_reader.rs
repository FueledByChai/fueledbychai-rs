//! FBC-jlr: the SBE reader reads group entries by their own stated length and count, so
//! fields a later version appends to an entry are skipped, and refuses a group that runs past
//! the frame.

mod md;

use fbc_core::DecodeError;
use fbc_venue_paradex::md::sbe::{Message, NULL_I64};
use md::frame;

#[test]
fn group_entries_are_read_by_their_own_length_and_appended_fields_skipped() {
    // A BookEvent-shaped frame whose entries are 24 bytes: price and size, then 8 more.
    let bytes = frame("book-longer-entries.sbe.txt");
    let msg = Message::parse(&bytes).unwrap();
    let header = msg.header();
    assert_eq!((header.template_id, header.block_length), (3, 89));
    assert_eq!((header.schema_id, header.version), (1, 1));
    assert_eq!(msg.block().bytes().len(), 89);
    assert_eq!(msg.block().u8_at(16), Some(1));
    assert_eq!(msg.block().i64_at(17), Some(NULL_I64));
    assert_eq!(msg.block().i64_at(82), None, "past the block: absent");

    let mut tail = msg.tail();
    let read = |group: fbc_venue_paradex::md::sbe::Group<'_>| {
        assert_eq!(group.len(), group.entries().count());
        assert!(!group.is_empty());
        group
            .entries()
            .map(|e| (e.i64_at(0).unwrap(), e.i64_at(8).unwrap(), e.i64_at(24)))
            .collect::<Vec<_>>()
    };
    let bids = read(tail.group().unwrap());
    assert_eq!(
        bids,
        [
            (6_200_050_000_000, 25_000_000, None),
            (6_200_040_000_000, 0, None)
        ]
    );
    let asks = read(tail.group().unwrap());
    assert_eq!(asks, [(6_200_100_000_000, 150_000_000, None)]);
    assert_eq!(tail.var_str(), Ok(Some("BTC-USD-PERP")));
    assert_eq!(tail.var_str(), Ok(None), "appended var data is absent");
}

#[test]
fn a_group_that_runs_past_the_frame_is_refused() {
    let bytes = frame("book-longer-entries.sbe.txt");
    let runs_past = Err(DecodeError::Malformed("SBE group runs past the frame"));
    // Cut inside the bids' entries, then inside the group header itself.
    for cut in [8 + 89 + 4 + 30, 8 + 89 + 2] {
        let msg = Message::parse(&bytes[..cut]).unwrap();
        assert_eq!(
            msg.tail().group().map(|g| g.len()),
            runs_past,
            "cut at {cut}"
        );
    }
}
