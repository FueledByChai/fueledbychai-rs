//! The drop test's verdict on one account (FBC-77r8, a slice of FBC-9sk2; decisions 0132 items 4
//! and 7, 0148 item 2 and 0151 items 4, 5, 9 and 13): what the reads after drop session two's
//! abrupt drop show of X and Y, the drop test's two orders, judged in the run that made them and
//! kept in memory alone, never carried to another account or run.
//!
//! - **The orders** ([`DropOrder`]): X or Y by its client id, its venue id once the streams named
//!   it, and its client id's text as QFEX's `client_order_id` carries it. A read lists it when an
//!   order it lists has either id ([`DropOrder::listed_in`]); a `get_user_trades` row names it by
//!   either ([`DropOrder::named_by`]); a `GET /user/historic-orders` row names it by its client
//!   id, whatever its `terminal_status`, which is kept as a status name
//!   ([`DropOrder::terminal_in`], 0151 item 5).
//! - **The reads after the drop** ([`until_absent`]): never-arming reads ([`compare::Reader`]:
//!   `get_user_orders` from offset 0 until a page that is not full, and REST positions, every
//!   frame and request charged first) at the comparisons' cadence ([`compare::Cadence`]: one at
//!   once, then each `--read-interval-ms` after the one before began, at most ten, none beginning
//!   at or past the end of `--agree-wait-ms` nor waited for there; 0151 item 4), until one lists
//!   neither X nor Y or the wait ends. A read still running at that end sends nothing more
//!   ([`Reader::until`]) and is not judged ([`Shown::Unfinished`]), though the answer to what it
//!   already sent is still read, so a provider rate refusal in it is heard. A provider rate
//!   refusal ends them at once (decision 0148 item 8).
//! - **The verdict** ([`DropVerdict::judge`]): the drop test passes only when Y rested before
//!   the drop, was still open as the connection dropped and was listed by the read before the
//!   drop (made after Y's `ACK`, so a `CANCELLED` of Y still on its way is no pass), the last
//!   read after it lists neither X nor Y and shows the position flat, every
//!   `get_user_trades` page since X's send was read and no row there names X, Y, another order or
//!   none (nothing else trades on the account, decision 0132 item 4), and `GET
//!   /user/historic-orders` lists both (0151 item 13), and no provider rate refusal cut its reads
//!   short (decision 0148 item 8). Beside it, the drop's reach over X, an order of an earlier
//!   connection: measured only when the read before the drop listed X and no `CANCELLED` of X
//!   came in drop session two before the drop, so a cancel the arm or QFEX made is never
//!   credited to the drop, and otherwise recorded as not measured, never failed; and what the
//!   arm did to X: not measured when X was not open as drop session two armed (whatever ended
//!   it: the report gives whether drop session one's graceful close did,
//!   [`crate::places::close_verdict`]), and otherwise reported cancelled (a `CANCELLED` of X in
//!   the arm's window, which ends with the read before the drop), survived, or cancelled with no
//!   report. X's absence counts as a cancel, the drop's or the arm's, only as
//!   [`crate::places::close_verdict`] counts it after drop session one (decision 0132 item 4):
//!   the trades since X's send read whole, no row naming X, none naming Y, another order or
//!   none, and the position flat after the drop; otherwise it is not measured, its reason given
//!   ([`Unmeasured`]).
//! - **Its stop** ([`DropVerdict::stop`]): a provider rate refusal in the reads after the drop,
//!   of the trades or of historic-orders stops the run as the refusal it is (decision 0148 item
//!   8), as the reads after drop session one do; then a fill of X or Y a trades row shows stops
//!   the run not flat, the position the owner's (0151 item 9), and so does a position the last
//!   read shows; then an arm-time cancel QFEX did not report (decision 0132 item 4: recorded, and
//!   stopped on after the drop); then a failed test (decision 0132 item 7). A failed drop test
//!   places nothing more on its account, since the run stops.

use std::time::Duration;

use fbc_core::{CidMatch, ClientOrderId, VenueOrderId};
use fbc_venue_qfex::account_reads::{HistoricOrder, TradeRow};
use tokio::time::Instant;

use crate::compare::{self, Attempt, Cadence, Reader};
use crate::places::{self, RefusedOn, Stop};

/// One of the drop test's orders, X or Y: its client id, its venue id once the streams named it,
/// and the client id's text as the codec writes it (`exec::CLIENT_ID_FORMAT`), which no output
/// shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropOrder {
    pub cid: ClientOrderId,
    pub venue: Option<VenueOrderId>,
    cid_text: String,
}

impl DropOrder {
    /// The order `cid`, named `venue` by the streams when they did; refused, by the encoder's
    /// refusal's variant alone, when its client id cannot be written.
    pub fn new(cid: ClientOrderId, venue: Option<VenueOrderId>) -> Result<DropOrder, String> {
        let text = fbc_core::encode_cid(&fbc_venue_qfex::exec::CLIENT_ID_FORMAT, cid);
        let text = text.map_err(|e| places::variant_name(&e))?.to_string();
        Ok(DropOrder {
            cid,
            venue,
            cid_text: text,
        })
    }

    /// Its client id's text as the codec writes it, which the tee matches with what was sent:
    /// kept in memory, never shown.
    pub fn cid_text(&self) -> &str {
        &self.cid_text
    }

    /// Whether `read` lists it: an order listed under its venue id, or under its client id, as
    /// the read decodes it under our namespace. Either one is enough, so an order listed is
    /// never taken for absent.
    pub fn listed_in(&self, read: &compare::Read) -> bool {
        let by = |l: &compare::Listed| {
            self.venue.as_ref() == Some(&l.order) || l.client == CidMatch::Ours(self.cid)
        };
        read.listed.iter().any(by)
    }

    /// Whether the `get_user_trades` row `row` names it: by its client id's text, or by its
    /// venue id once the streams named one.
    pub fn named_by(&self, row: &TradeRow) -> bool {
        let venue = self.venue.as_ref().map(VenueOrderId::as_str);
        row.client_order_id.as_deref() == Some(self.cid_text.as_str())
            || (venue.is_some() && row.order_id.as_deref() == venue)
    }

    /// Its `terminal_status` on a `GET /user/historic-orders` page's `rows`, as a status name
    /// ([`status_name`]), when a row names its client id's text: the first such row's. Terminal
    /// there means listed, whatever the status (0151 item 5).
    pub fn terminal_in(&self, rows: &[HistoricOrder]) -> Option<String> {
        let named = rows.iter().find(|r| r.client_order_id == self.cid_text);
        named.map(|row| status_name(&row.terminal_status))
    }
}

/// `status` as the report keeps it: a status name (capitals, digits and underscores, at most
/// 32), or `(not a status name)`, so no other text QFEX writes there reaches an output.
pub fn status_name(status: &str) -> String {
    let named = |c: char| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_';
    match (1..=32).contains(&status.len()) && status.chars().all(named) {
        true => status.to_owned(),
        false => "(not a status name)".to_owned(),
    }
}

/// What one read after the drop showed: whether it listed X and Y, and whether the REST position
/// on the traded symbol was flat.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    pub x: bool,
    pub y: bool,
    pub flat: bool,
}

impl Listing {
    /// What `read` shows of `x` and `y`.
    pub fn of(read: &compare::Read, x: &DropOrder, y: &DropOrder) -> Listing {
        Listing {
            x: x.listed_in(read),
            y: y.listed_in(read),
            flat: read.position.is_zero(),
        }
    }

    /// Whether it lists neither.
    pub fn absent(&self) -> bool {
        !self.x && !self.y
    }
}

/// What one read after the drop came to.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    /// Made, listing neither X nor Y.
    Absent,
    /// Made, listing X, Y or both.
    Listed,
    /// The shared limiter had no room for it, so it was not made (decision 0135 item 1).
    Skipped,
    Failed,
    /// A provider rate refusal.
    Refused,
    /// Begun within the wait and not finished by its end: cut there, sending nothing more, and
    /// not judged, so none is taken as listing neither after the wait (0109, 0151 item 4).
    Unfinished,
}

/// The reads after the drop ([`until_absent`]): each with when it began from their start and
/// what it showed, the last one made, when the first read listing neither began, and the
/// provider rate refusal that ended them, if one did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AfterDrop {
    pub reads: Vec<(Duration, Shown)>,
    pub last: Option<Listing>,
    /// From their start to the end of the first read listing neither X nor Y.
    pub absent_after: Option<Duration>,
    pub refused: Option<RefusedOn>,
}

/// The reads after the drop: `reader`'s reads at `cadence` (one at once, then each one interval
/// after the one before began, at most `cadence.reads()`, none beginning at or past the end of
/// its wait nor waited for when it would be due there; 0151 item 4), until one lists neither `x`
/// nor `y` or the wait ends. `reader` is told that end ([`Reader::until`]): a read still running
/// there sends nothing more, and one finished past it is not judged ([`Shown::Unfinished`]),
/// the last read finished within the wait the one judged. A provider rate refusal ends them at
/// once, one heard past the end included (decision 0148 item 8).
pub async fn until_absent<R: Reader>(
    reader: &mut R,
    (x, y): (&DropOrder, &DropOrder),
    cadence: Cadence,
) -> AfterDrop {
    let start = Instant::now();
    let mut after = AfterDrop::default();
    let mut before: Option<Instant> = None;
    let end = start + cadence.wait;
    reader.until(end);
    for _ in 0..cadence.reads() {
        if let Some(before) = before {
            // A read due at or past the end of the wait never begins, so it is not waited for.
            let due = before + cadence.interval;
            if due >= end {
                break;
            }
            tokio::time::sleep_until(due).await;
        }
        let now = Instant::now();
        let began = now.duration_since(start);
        if before.is_some() && began >= cadence.wait {
            break;
        }
        before = Some(now);
        let attempt = reader.read().await;
        // A read finished past the wait is not judged, so none is taken as listing neither after
        // it (0109, 0151 item 4); a provider rate refusal it heard still ends them.
        let late = Instant::now() > end;
        let shown = match attempt {
            Attempt::Cut => Shown::Unfinished,
            Attempt::Read(_) if late => Shown::Unfinished,
            Attempt::Read(read) => {
                let listing = Listing::of(&read, x, y);
                after.last = Some(listing);
                match listing.absent() {
                    true => Shown::Absent,
                    false => Shown::Listed,
                }
            }
            Attempt::Skipped => Shown::Skipped,
            Attempt::Failed(_) => Shown::Failed,
            Attempt::Refused(on) => {
                after.reads.push((began, Shown::Refused));
                after.refused = Some(on);
                return after;
            }
        };
        after.reads.push((began, shown));
        if shown == Shown::Unfinished {
            return after;
        }
        if shown == Shown::Absent {
            after.absent_after = Some(Instant::now().duration_since(start));
            return after;
        }
    }
    after
}

/// Which of the drop test's orders.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Which {
    X,
    Y,
}

impl Which {
    fn name(self) -> &'static str {
        match self {
            Which::X => "X",
            Which::Y => "Y",
        }
    }
}

/// What the reads of the drop test showed, for [`DropVerdict::judge`]: flags, counts and status
/// names alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Facts {
    /// Whether the read before drop session two listed X: X open as that session armed.
    pub x_before_two: bool,
    /// Whether an `order_response` reporting X `CANCELLED` arrived in drop session two's arm
    /// window, to the end of the read before the drop (decision 0132 item 4).
    pub arm_reported_x: bool,
    /// Whether one arrived in drop session two at any time before the drop: then the drop did
    /// not end X.
    pub x_cancelled_before_drop: bool,
    /// Whether the read before the drop listed X; `None` when it was not made.
    pub x_before_drop: Option<bool>,
    /// Whether the read before the drop listed Y; `None` when it was not made. Made after Y's
    /// `ACK`, it orders Y's end before the drop by a read, whatever the session had heard of it
    /// by then (0151 item 13).
    pub y_before_drop: Option<bool>,
    /// Whether an order event showed Y resting before the drop.
    pub y_rested: bool,
    /// Whether Y was still open as the connection dropped: open in the registry, with no
    /// `CANCELLED` of it heard in the session.
    pub y_open_at_drop: bool,
    /// The last read after the drop ([`AfterDrop::last`]); `None` when none was made.
    pub after: Option<Listing>,
    /// The `get_user_trades` rows since X's send naming X and naming Y; `None` when the trades
    /// were not read whole.
    pub traded: Option<[usize; 2]>,
    /// The `get_user_trades` rows since X's send naming neither X nor Y, another order or none,
    /// on the pages read, whole or not: something else traded on the account (decision 0132
    /// item 4).
    pub other_trades: usize,
    /// X's and Y's `terminal_status` on `GET /user/historic-orders`, each when a row names it;
    /// `None` when the pages were not read whole.
    pub historic: Option<[Option<String>; 2]>,
    /// The provider rate refusal that ended the reads after the drop ([`AfterDrop::refused`]) or
    /// the reads of the trades or of historic-orders, if one did (decision 0148 item 8).
    pub refused: Option<RefusedOn>,
}

/// What drop session two's arm did to X (decision 0132 item 4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ArmCancel {
    /// X was not open as the arm was sent (drop session one's graceful close or anything else
    /// ended it), or the read before the drop was not made: whether enabling cancels open
    /// orders is not measured.
    NotMeasured,
    /// The read before the drop still listed X, and no report of its cancel came.
    Survived,
    /// A `CANCELLED` report of X came in the arm's window: enabling cancels the account's open
    /// orders, and QFEX reports it.
    Reported,
    /// The read before the drop no longer listed X, and no report of its cancel came: enabling
    /// cancels open orders, and QFEX does not report it, a stop after the drop. Counted only when
    /// X's absence is shown a cancel ([`Unmeasured`]).
    Unreported,
    /// The read before the drop no longer listed X, and no report of its cancel came, but X's
    /// absence is not shown a cancel, for the reason given: what the arm did is not measured.
    Unmeasured(Unmeasured),
}

impl ArmCancel {
    /// What the report says of it.
    pub fn says(self) -> String {
        match self {
            ArmCancel::NotMeasured => "not measured: X was not open as drop session two armed, \
                                       or the read before the drop was not made"
                .to_owned(),
            ArmCancel::Survived => "no: X survived drop session two's arm".to_owned(),
            ArmCancel::Reported => {
                "yes, reported: a CANCELLED of X came in the arm's window".to_owned()
            }
            ArmCancel::Unreported => "yes, not reported: the read before the drop no longer \
                                      listed X and no CANCELLED of it came (decision 0132 item \
                                      4), a stop after the drop"
                .to_owned(),
            ArmCancel::Unmeasured(why) => format!(
                "not measured: the read before the drop no longer listed X and no CANCELLED of it \
                 came, but {}",
                why.says()
            ),
        }
    }
}

/// Why an order's absence is not counted a cancel, the drop's or the arm's: decision 0132 item
/// 4 reads each order shown cancelled, not filled, only with no `get_user_trades` row naming it
/// and nothing else trading on the account, as [`crate::places::close_verdict`] reads X after
/// drop session one.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Unmeasured {
    /// A `get_user_trades` row since X's send names it: a fill, whole or in part.
    Traded,
    /// A row since X's send names another order, Y included, or none: something traded on the
    /// account, so a fill its row does not name is not ruled out.
    OtherTrade,
    /// The last read after the drop showed a position that is not flat.
    NotFlat,
    /// No read after the drop was made, so the position is not known.
    NotReadAfter,
    /// The trades since X's send were not read whole.
    TradesUnread,
}

impl Unmeasured {
    /// Why, in the report's and the evidence's words.
    pub fn says(self) -> &'static str {
        match self {
            Unmeasured::Traded => {
                "a get_user_trades row names X: a fill, so its absence is not a cancel (decision \
                 0132 item 4)"
            }
            Unmeasured::OtherTrade => {
                "a get_user_trades row since X's send names another order or none: something \
                 traded on the account, so a fill of X is not ruled out (decision 0132 item 4)"
            }
            Unmeasured::NotFlat => {
                "the position was not flat after the drop: something traded on the account, so a \
                 fill of X is not ruled out (decision 0132 item 4)"
            }
            Unmeasured::NotReadAfter => {
                "no read after the drop was made, so the position is not known (decision 0132 \
                 item 4)"
            }
            Unmeasured::TradesUnread => {
                "X's trades since its send were not read whole, so a fill of X is not ruled out \
                 (decision 0132 item 4)"
            }
        }
    }
}

/// Whether X's absence counts as a cancel (decision 0132 item 4): the trades since X's send
/// read whole, no row naming X, none naming Y, another order or none, and the last read after
/// the drop flat; checked in that order of reasons, as [`crate::places::close_verdict`] checks
/// X's after drop session one: a row naming X, then any other row, then the position, then the
/// trades read whole.
fn x_absence(facts: &Facts) -> Result<(), Unmeasured> {
    if facts.traded.is_some_and(|[x, _]| x > 0) {
        return Err(Unmeasured::Traded);
    }
    if facts.other_trades > 0 || facts.traded.is_some_and(|[_, y]| y > 0) {
        return Err(Unmeasured::OtherTrade);
    }
    match facts.after {
        None => return Err(Unmeasured::NotReadAfter),
        Some(last) if !last.flat => return Err(Unmeasured::NotFlat),
        Some(_) => {}
    }
    match facts.traded {
        Some(_) => Ok(()),
        None => Err(Unmeasured::TradesUnread),
    }
}

/// The drop's reach over X, an order of an earlier connection (decision 0132 item 4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Reach {
    /// The read before the drop did not list X, or a `CANCELLED` of X came in drop session two
    /// before the drop (the arm or QFEX, not the drop, cancelled X): not measured, never failed.
    NotMeasured,
    /// X, listed before the drop, was shown ended by every read after it.
    Cancelled,
    /// X, listed before the drop, was not shown ended by every read after it: still listed by
    /// the last, no read made, or no `GET /user/historic-orders` row naming it.
    NotShown,
    /// X, listed before the drop, was absent after it, but its absence is not shown a cancel,
    /// for the reason given.
    Unmeasured(Unmeasured),
}

impl Reach {
    /// What the report says of it.
    pub fn says(self) -> String {
        match self {
            Reach::NotMeasured => "not measured: the read before the drop did not list X, or a \
                                   CANCELLED of X came before the drop (decision 0132 item 4)"
                .to_owned(),
            Reach::Cancelled => {
                "shown: the drop cancelled X, an order of drop session one's".to_owned()
            }
            Reach::NotShown => {
                "not shown: the reads after the drop did not show X ended".to_owned()
            }
            Reach::Unmeasured(why) => format!(
                "not measured: the reads after the drop no longer listed X, but {}",
                why.says()
            ),
        }
    }
}

/// Why the drop test did not pass, each named with no id and no value.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    /// Y never rested before the drop, so the drop had nothing of its own to cancel.
    YNotRested,
    /// Y rested, then ended before the drop, so the drop had nothing of its own to cancel.
    YEndedBeforeDrop,
    /// Y rested, but the read before the drop did not list it: Y had ended before the drop, or
    /// the read did not show it, so the drop's cancel of Y is not shown (decision 0132 item 4).
    YNotListedBeforeDrop,
    /// The read before the drop was not made, so the arm's cancels are not told from the drop's.
    NotReadBeforeDrop,
    /// A provider rate refusal cut the reads short (decision 0148 item 8).
    RateRefused,
    /// No read after the drop was made.
    NotReadAfterDrop,
    /// The last read after the drop still listed it.
    Listed(Which),
    /// The last read after the drop showed a position that is not flat.
    NotFlat,
    /// The trades since X's send were not read whole.
    TradesNotRead,
    /// A `get_user_trades` row names it: it filled.
    Traded(Which),
    /// A `get_user_trades` row since X's send names another order or none: something else
    /// traded on the account (decision 0132 item 4).
    OtherTrade,
    /// `GET /user/historic-orders` was not read whole.
    HistoricNotRead,
    /// No `GET /user/historic-orders` row names it (0151 item 13).
    NotTerminal(Which),
}

impl Failure {
    /// What the report says of it.
    pub fn says(self) -> String {
        match self {
            Failure::YNotRested => "Y never rested before the drop".to_owned(),
            Failure::YEndedBeforeDrop => {
                "Y rested, then ended before the drop, so the drop did not cancel it".to_owned()
            }
            Failure::YNotListedBeforeDrop => "Y rested, but the read before the drop did not \
                                              list it, so the drop's cancel of Y is not shown"
                .to_owned(),
            Failure::NotReadBeforeDrop => "the read before the drop was not made".to_owned(),
            Failure::RateRefused => {
                "a provider rate refusal cut the reads short (decision 0148 item 8)".to_owned()
            }
            Failure::NotReadAfterDrop => "no read after the drop was made".to_owned(),
            Failure::Listed(o) => format!(
                "{} still listed by get_user_orders when the wait ended",
                o.name()
            ),
            Failure::NotFlat => "a position that is not flat after the drop".to_owned(),
            Failure::TradesNotRead => "the trades since X's send were not read whole".to_owned(),
            Failure::Traded(o) => format!("a get_user_trades row names {}", o.name()),
            Failure::OtherTrade => "a get_user_trades row since X's send names another order or \
                                    none: something else traded on the account (decision 0132 \
                                    item 4)"
                .to_owned(),
            Failure::HistoricNotRead => "GET /user/historic-orders was not read whole".to_owned(),
            Failure::NotTerminal(o) => {
                format!("no GET /user/historic-orders row names {}", o.name())
            }
        }
    }
}

/// The drop test's verdict on one account (decision 0132 item 4, 0151 item 13).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropVerdict {
    pub passed: bool,
    /// Each reason it did not pass, in the order judged; empty when it passed.
    pub failures: Vec<Failure>,
    pub reach: Reach,
    pub arm: ArmCancel,
    /// X's and Y's `terminal_status` on `GET /user/historic-orders`, as status names, when a row
    /// named each.
    pub terminal: [Option<String>; 2],
    /// The provider rate refusal that cut the reads short ([`Facts::refused`]), if one did.
    pub refused: Option<RefusedOn>,
}

impl DropVerdict {
    /// The verdict `facts` give ([`crate::verdict`]'s module).
    pub fn judge(facts: &Facts) -> DropVerdict {
        let mut failures = Vec::new();
        match (facts.y_rested, facts.y_open_at_drop) {
            (false, _) => failures.push(Failure::YNotRested),
            (true, false) => failures.push(Failure::YEndedBeforeDrop),
            (true, true) => {}
        }
        // What the session heard of Y can lag a frame still on its way; the read before the drop,
        // made after Y's ACK, does not.
        if facts.y_rested && facts.y_before_drop == Some(false) {
            failures.push(Failure::YNotListedBeforeDrop);
        }
        if facts.x_before_drop.is_none() {
            failures.push(Failure::NotReadBeforeDrop);
        }
        if facts.refused.is_some() {
            failures.push(Failure::RateRefused);
        }
        match facts.after {
            None => failures.push(Failure::NotReadAfterDrop),
            Some(last) => {
                failures.extend(last.x.then_some(Failure::Listed(Which::X)));
                failures.extend(last.y.then_some(Failure::Listed(Which::Y)));
                failures.extend((!last.flat).then_some(Failure::NotFlat));
            }
        }
        match facts.traded {
            None => failures.push(Failure::TradesNotRead),
            Some([x, y]) => {
                failures.extend((x > 0).then_some(Failure::Traded(Which::X)));
                failures.extend((y > 0).then_some(Failure::Traded(Which::Y)));
            }
        }
        failures.extend((facts.other_trades > 0).then_some(Failure::OtherTrade));
        let terminal = facts.historic.clone().unwrap_or_default();
        match &facts.historic {
            None => failures.push(Failure::HistoricNotRead),
            Some([x, y]) => {
                failures.extend(x.is_none().then_some(Failure::NotTerminal(Which::X)));
                failures.extend(y.is_none().then_some(Failure::NotTerminal(Which::Y)));
            }
        }
        // X absent after the drop is the drop's cancel only once its absence is shown a cancel
        // (decision 0132 item 4), and X shown ended (0151 item 5) only with a historic-orders row.
        let absent = facts.after.is_some_and(|l| !l.x);
        let shown = |why: Result<(), Unmeasured>| match why {
            Ok(()) if terminal[0].is_some() => Reach::Cancelled,
            Ok(()) => Reach::NotShown,
            Err(why) => Reach::Unmeasured(why),
        };
        // A CANCELLED of X before the drop is the arm's cancel or QFEX's own, whatever a lagging
        // read before the drop still listed: the drop's reach over X is then not measured.
        let cancelled = facts.arm_reported_x || facts.x_cancelled_before_drop;
        let reach = match (facts.x_before_drop, cancelled) {
            (Some(true), false) if absent => shown(x_absence(facts)),
            (Some(true), false) => Reach::NotShown,
            _ => Reach::NotMeasured,
        };
        let arm = match (
            facts.x_before_two,
            facts.arm_reported_x,
            facts.x_before_drop,
        ) {
            (false, _, _) => ArmCancel::NotMeasured,
            (true, true, _) => ArmCancel::Reported,
            (true, false, Some(true)) => ArmCancel::Survived,
            (true, false, Some(false)) => match x_absence(facts) {
                Ok(()) => ArmCancel::Unreported,
                Err(why) => ArmCancel::Unmeasured(why),
            },
            (true, false, None) => ArmCancel::NotMeasured,
        };
        DropVerdict {
            passed: failures.is_empty(),
            failures,
            reach,
            arm,
            terminal,
            refused: facts.refused,
        }
    }

    /// The stop it makes, the first that applies: a provider rate refusal in its reads
    /// ([`Stop::RateRefused`], decision 0148 item 8); a fill of X or Y shown by a trades row
    /// ([`Stop::UnexpectedFill`]) or a position the last read showed ([`Stop::NotFlat`]), either
    /// leaving the run not flat with the position the owner's (0151 item 9); an arm-time cancel
    /// QFEX did not report ([`Stop::ArmCancelUnreported`], decision 0132 item 4); then a test
    /// that did not pass ([`Stop::DropFailed`], decision 0132 item 7). None when it passed with
    /// none of them.
    pub fn stop(&self) -> Option<Stop> {
        if let Some(on) = self.refused {
            return Some(Stop::RateRefused { on });
        }
        let has = |f: Failure| self.failures.contains(&f);
        if has(Failure::Traded(Which::X)) || has(Failure::Traded(Which::Y)) {
            return Some(Stop::UnexpectedFill);
        }
        if has(Failure::NotFlat) {
            return Some(Stop::NotFlat);
        }
        if self.arm == ArmCancel::Unreported {
            return Some(Stop::ArmCancelUnreported);
        }
        (!self.passed).then_some(Stop::DropFailed)
    }

    /// What the report says of it: passed or failed, with each failure.
    pub fn says(&self) -> String {
        match self.passed {
            true => "passed: X and Y absent after the drop, named by no trades row, and listed \
                     by GET /user/historic-orders"
                .to_owned(),
            false => {
                let why: Vec<String> = self.failures.iter().map(|f| f.says()).collect();
                format!("failed: {}", why.join("; "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeSet, VecDeque};
    use std::str::FromStr;
    use std::sync::OnceLock;

    use fbc_core::{
        AccountKey, CidMint, Namespace, NamespaceLease, VenueCaps, VenueConfig, VenueFactory,
        WallNs, dispatch,
    };
    use rust_decimal::Decimal;

    use crate::compare::{Listed, Read};
    use crate::probe_args::MAX_READS;

    /// The caps the probe's reads decode under, on loopback origins nothing listens on.
    fn caps() -> VenueCaps {
        let mut cfg = VenueConfig::new();
        cfg.insert(
            fbc_venue_qfex::probe::KEY_EXEC_MODE,
            fbc_venue_qfex::probe::PRODUCTION_PROBE,
        );
        cfg.insert(fbc_venue_qfex::auth::TRADE_URL, "ws://127.0.0.1:1");
        cfg.insert(fbc_venue_qfex::KEY_REST_URL, "http://127.0.0.1:2");
        cfg.insert(fbc_venue_qfex::KEY_MDS_URL, "ws://127.0.0.1:3");
        cfg.insert(fbc_venue_qfex::probe::KEY_PAGE_SIZE, "1000");
        cfg.insert(fbc_venue_qfex::probe::KEY_RESYNC_TIMEOUT, "30s");
        cfg.insert(fbc_venue_qfex::probe::KEY_RPC_TIMEOUT, "600ms");
        fbc_venue_qfex::Qfex.caps(&cfg).unwrap()
    }

    /// The `seq`th of three client ids minted under a lease of the tests' own, from 1.
    fn cid(seq: usize) -> ClientOrderId {
        static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
        let minted = MINTED.get_or_init(|| {
            let dir = std::env::temp_dir().join(format!("fbc-verdict-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), Namespace::new(7));
            let mut mint = CidMint::new(lease.unwrap(), 0, 0, WallNs(0));
            let cids = (0..3).map(|_| mint.mint().unwrap()).collect();
            let _ = std::fs::remove_dir_all(&dir);
            cids
        });
        minted[seq - 1]
    }

    fn vid(n: u8) -> VenueOrderId {
        let id = format!("{n:08x}-2222-4222-8222-222222222222");
        dispatch(&caps(), Namespace::new(7), |scope| {
            scope.venue_order_id(&id)
        })
        .unwrap()
    }

    /// X (client id 1, venue id 1) and Y (client id 2, its venue id `y_venue` when known).
    fn orders(y_venue: Option<u8>) -> (DropOrder, DropOrder) {
        let x = DropOrder::new(cid(1), Some(vid(1))).unwrap();
        let y = DropOrder::new(cid(2), y_venue.map(vid)).unwrap();
        (x, y)
    }

    fn listed(n: u8, client: CidMatch) -> Listed {
        Listed {
            order: vid(n),
            client,
        }
    }

    fn row(client: Option<&str>, order: Option<&str>) -> TradeRow {
        TradeRow {
            trade_id: Some("synthetic".to_owned()),
            order_id: order.map(str::to_owned),
            client_order_id: client.map(str::to_owned),
            side: None,
            aggressor_side: None,
            timestamp: None,
            fee: None,
            members: BTreeSet::new(),
        }
    }

    #[test]
    fn an_order_is_listed_named_and_found_ended_by_either_of_its_ids() {
        let (x, y) = orders(None);
        // Listed by its venue id, whatever client id the row decodes to, or by its client id.
        let read = Read {
            listed: vec![listed(1, CidMatch::Unparseable)],
            ..Read::default()
        };
        assert!(x.listed_in(&read) && !y.listed_in(&read));
        let read = Read {
            listed: vec![listed(9, CidMatch::Ours(cid(2)))],
            ..Read::default()
        };
        assert!(!x.listed_in(&read) && y.listed_in(&read));
        // A foreign order, or one of ours that is neither, lists neither.
        let read = Read {
            listed: vec![
                listed(9, CidMatch::Foreign(Namespace::new(3))),
                listed(8, CidMatch::Ours(cid(3))),
            ],
            ..Read::default()
        };
        assert!(!x.listed_in(&read) && !y.listed_in(&read));
        // A trades row names it by its client id's text, or by its venue id once known; Y's
        // venue id unknown, a row naming no client id names it by nothing.
        let x_text = x.cid_text.clone();
        assert!(x.named_by(&row(Some(&x_text), None)));
        assert!(x.named_by(&row(None, Some(vid(1).as_str()))));
        assert!(!x.named_by(&row(Some("synthetic"), Some(vid(2).as_str()))));
        assert!(!y.named_by(&row(None, Some(vid(2).as_str()))));
        assert!(!y.named_by(&row(None, None)));
        // A historic-orders row names it by its client id alone, its status kept as a name.
        let ended = |cid: &str, status: &str| HistoricOrder {
            client_order_id: cid.to_owned(),
            terminal_status: status.to_owned(),
        };
        let rows = [
            ended("synthetic", "FILLED"),
            ended(&x_text, "CANCELLED"),
            ended(&y.cid_text, "a status, with words"),
        ];
        assert_eq!(x.terminal_in(&rows).as_deref(), Some("CANCELLED"));
        assert_eq!(y.terminal_in(&rows).as_deref(), Some("(not a status name)"));
        assert_eq!(x.terminal_in(&rows[..1]), None);
        // A client id's text is what the codec writes.
        let written = fbc_core::encode_cid(&fbc_venue_qfex::exec::CLIENT_ID_FORMAT, cid(1));
        assert_eq!(x_text, written.unwrap().to_string());
    }

    #[test]
    fn a_status_is_kept_as_a_name_or_not_at_all() {
        assert_eq!(status_name("CANCELLED"), "CANCELLED");
        assert_eq!(status_name("IOC_2_DONE"), "IOC_2_DONE");
        for odd in ["", "cancelled", "CANCELLED ", &"A".repeat(33)] {
            assert_eq!(status_name(odd), "(not a status name)", "{odd}");
        }
        assert_eq!(status_name(&"A".repeat(32)), "A".repeat(32));
    }

    /// Facts of a drop test that passed: X open through the arm, listed before the drop, and X
    /// and Y absent, untraded and listed ended after it.
    fn passing() -> Facts {
        Facts {
            x_before_two: true,
            arm_reported_x: false,
            x_cancelled_before_drop: false,
            x_before_drop: Some(true),
            y_before_drop: Some(true),
            y_rested: true,
            y_open_at_drop: true,
            after: Some(Listing {
                x: false,
                y: false,
                flat: true,
            }),
            traded: Some([0, 0]),
            other_trades: 0,
            historic: Some([Some("CANCELLED".to_owned()), Some("CANCELLED".to_owned())]),
            refused: None,
        }
    }

    #[test]
    fn the_drop_cancelling_x_and_y_passes_with_the_reach_over_x_shown() {
        let verdict = DropVerdict::judge(&passing());
        assert!(verdict.passed, "{verdict:?}");
        assert!(verdict.failures.is_empty());
        assert_eq!(verdict.reach, Reach::Cancelled);
        assert_eq!(verdict.arm, ArmCancel::Survived);
        assert_eq!(verdict.stop(), None);
        let cancelled = Some("CANCELLED".to_owned());
        assert_eq!(verdict.terminal, [cancelled.clone(), cancelled]);
        assert!(verdict.says().starts_with("passed:"));
    }

    #[test]
    fn a_graceful_close_that_cancelled_x_leaves_the_arm_and_the_reach_not_measured() {
        // X ended by drop session one's close: not open as session two armed, and not listed
        // before the drop. Y is still judged, and the test passes on it.
        let facts = Facts {
            x_before_two: false,
            x_before_drop: Some(false),
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert!(verdict.passed);
        assert_eq!(
            (verdict.reach, verdict.arm),
            (Reach::NotMeasured, ArmCancel::NotMeasured)
        );
        assert_eq!(verdict.stop(), None);
        // Y still listed after the wait fails it, whatever became of X.
        let open = Facts {
            after: Some(Listing {
                x: false,
                y: true,
                flat: true,
            }),
            ..facts
        };
        let verdict = DropVerdict::judge(&open);
        assert_eq!(verdict.failures, [Failure::Listed(Which::Y)]);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
    }

    #[test]
    fn an_arm_that_cancels_x_is_recorded_reported_or_not() {
        // Reported: a CANCELLED of X in the arm's window; the reach over X is not measured, and
        // the test passes on Y.
        let reported = Facts {
            arm_reported_x: true,
            x_before_drop: Some(false),
            ..passing()
        };
        let verdict = DropVerdict::judge(&reported);
        assert!(verdict.passed);
        assert_eq!(verdict.arm, ArmCancel::Reported);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert_eq!(verdict.stop(), None);
        // Silent: X gone before the drop with no report. Recorded, and a stop after the drop,
        // though the test passed on Y.
        let silent = Facts {
            arm_reported_x: false,
            ..reported
        };
        let verdict = DropVerdict::judge(&silent);
        assert!(verdict.passed);
        assert_eq!(verdict.arm, ArmCancel::Unreported);
        assert_eq!(verdict.stop(), Some(Stop::ArmCancelUnreported));
        // A failed test with it stops on the unreported cancel, which came first.
        let failed = Facts {
            historic: Some([Some("CANCELLED".to_owned()), None]),
            ..silent
        };
        let verdict = DropVerdict::judge(&failed);
        assert!(!verdict.passed);
        assert_eq!(verdict.stop(), Some(Stop::ArmCancelUnreported));
        // With no read before the drop, what the arm did is not measured, and the test fails.
        let unread = Facts {
            x_before_drop: None,
            ..passing()
        };
        let verdict = DropVerdict::judge(&unread);
        assert_eq!(verdict.arm, ArmCancel::NotMeasured);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert_eq!(verdict.failures, [Failure::NotReadBeforeDrop]);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
    }

    #[test]
    fn x_absent_before_the_drop_leaves_the_reach_not_measured_never_failed() {
        // X absent before the drop with no CANCELLED of it heard: its absence alone leaves the
        // reach not measured, though the reads after the drop show X ended.
        let facts = Facts {
            x_before_drop: Some(false),
            arm_reported_x: false,
            x_cancelled_before_drop: false,
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert!(verdict.passed);
        // Listed before the drop and still listed after it, the reach is not shown, and fails.
        let kept = Facts {
            after: Some(Listing {
                x: true,
                y: false,
                flat: true,
            }),
            ..passing()
        };
        let verdict = DropVerdict::judge(&kept);
        assert_eq!(verdict.reach, Reach::NotShown);
        assert_eq!(verdict.failures, [Failure::Listed(Which::X)]);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
    }

    #[test]
    fn a_reported_arm_cancel_of_x_still_listed_before_the_drop_leaves_the_reach_not_measured() {
        // A CANCELLED of X came in the arm's window, but the read before the drop, lagging, still
        // listed X: the arm cancelled X, so the drop is not credited with it.
        let facts = Facts {
            arm_reported_x: true,
            x_before_drop: Some(true),
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert_eq!(verdict.arm, ArmCancel::Reported);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert!(verdict.passed, "{verdict:?}");
        assert_eq!(verdict.stop(), None);
        // X still listed after the wait is still a failure, and the reach still not measured.
        let kept = Facts {
            after: Some(Listing {
                x: true,
                y: false,
                flat: true,
            }),
            ..facts
        };
        let verdict = DropVerdict::judge(&kept);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert_eq!(verdict.failures, [Failure::Listed(Which::X)]);
    }

    #[test]
    fn a_cancelled_of_x_after_the_read_before_the_drop_leaves_the_reach_not_measured() {
        // The read before the drop listed X and no CANCELLED of it came in the arm's window, but
        // one came after that read, before the drop: QFEX, not the drop, ended X.
        let facts = Facts {
            x_cancelled_before_drop: true,
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert_eq!(verdict.reach, Reach::NotMeasured);
        assert_eq!(verdict.arm, ArmCancel::Survived);
        assert!(verdict.passed, "{verdict:?}");
        assert_eq!(verdict.stop(), None);
    }

    #[test]
    fn y_ended_before_the_drop_fails_the_test() {
        // Y rested, then ended before the drop (QFEX cancelled it on a live connection): the
        // reads after the drop show it absent and ended, but the drop did not cancel it.
        let facts = Facts {
            y_open_at_drop: false,
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert_eq!(verdict.failures, [Failure::YEndedBeforeDrop]);
        assert_eq!(verdict.reach, Reach::Cancelled);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
        assert_eq!(
            verdict.says(),
            "failed: Y rested, then ended before the drop, so the drop did not cancel it"
        );
        // Never rested at all is the other failure, and not this one.
        let never = Facts {
            y_rested: false,
            ..facts
        };
        assert_eq!(DropVerdict::judge(&never).failures, [Failure::YNotRested]);
    }

    #[test]
    fn y_not_listed_by_the_read_before_the_drop_fails_the_test() {
        // Y rested and the session heard no end of it before the drop, but the read before the
        // drop, made after Y's ACK, did not list it: a CANCELLED of Y may still have been on its
        // way, so the drop's cancel of Y is not shown, and the test fails, everything else
        // passing (0151 item 13: a stricter verdict only places less).
        let facts = Facts {
            y_before_drop: Some(false),
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        assert_eq!(verdict.failures, [Failure::YNotListedBeforeDrop]);
        assert!(!verdict.passed);
        assert_eq!(verdict.reach, Reach::Cancelled);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
        assert_eq!(
            verdict.says(),
            "failed: Y rested, but the read before the drop did not list it, so the drop's \
             cancel of Y is not shown"
        );
        // With the session's own end of Y heard too, both are given.
        let heard = Facts {
            y_open_at_drop: false,
            ..facts.clone()
        };
        assert_eq!(
            DropVerdict::judge(&heard).failures,
            [Failure::YEndedBeforeDrop, Failure::YNotListedBeforeDrop]
        );
        // A read not made is the failure it already was, and Y never rested is its own alone.
        let unread = Facts {
            x_before_drop: None,
            y_before_drop: None,
            ..passing()
        };
        assert_eq!(
            DropVerdict::judge(&unread).failures,
            [Failure::NotReadBeforeDrop]
        );
        let never = Facts {
            y_rested: false,
            ..facts
        };
        assert_eq!(DropVerdict::judge(&never).failures, [Failure::YNotRested]);
    }

    #[test]
    fn a_provider_refusal_after_the_drop_stops_rate_refused() {
        for on in [RefusedOn::Rest, RefusedOn::ReadSocket, RefusedOn::Pager] {
            // The reads otherwise whole and clean: the refusal alone fails the test and stops it.
            let facts = Facts {
                refused: Some(on),
                ..passing()
            };
            let verdict = DropVerdict::judge(&facts);
            assert!(!verdict.passed);
            assert_eq!(verdict.failures, [Failure::RateRefused]);
            assert_eq!(verdict.refused, Some(on));
            assert_eq!(verdict.stop(), Some(Stop::RateRefused { on }));
            assert_eq!(
                verdict.says(),
                "failed: a provider rate refusal cut the reads short (decision 0148 item 8)"
            );
            // It comes first: before a fill, a position, an unreported arm-time cancel and Y
            // still listed after the wait.
            let worst = Facts {
                arm_reported_x: false,
                x_before_drop: Some(false),
                after: Some(Listing {
                    x: false,
                    y: true,
                    flat: false,
                }),
                traded: Some([0, 1]),
                ..facts
            };
            let verdict = DropVerdict::judge(&worst);
            // X absent before the drop with no report, but a row naming Y, another order, and
            // not flat after it: not shown a cancel, so what the arm did is not measured
            // (decision 0132 item 4), the row the first reason, as close_verdict orders them.
            assert_eq!(verdict.arm, ArmCancel::Unmeasured(Unmeasured::OtherTrade));
            assert_eq!(verdict.failures[0], Failure::RateRefused);
            assert_eq!(verdict.stop(), Some(Stop::RateRefused { on }));
        }
    }

    #[test]
    fn a_trade_row_or_a_position_after_the_drop_stops_the_run_not_flat() {
        for (traded, which) in [([0, 1], Which::Y), ([1, 0], Which::X)] {
            let facts = Facts {
                traded: Some(traded),
                ..passing()
            };
            let verdict = DropVerdict::judge(&facts);
            assert_eq!(verdict.failures, [Failure::Traded(which)]);
            assert_eq!(verdict.stop(), Some(Stop::UnexpectedFill));
            // X traded is a fill, not the drop's cancel, so the reach over it is not measured
            // (decision 0132 item 4); Y traded is another order's row, which leaves X's absence
            // not measured too, as close_verdict counts any row since X's send.
            let reach = match which {
                Which::X => Reach::Unmeasured(Unmeasured::Traded),
                Which::Y => Reach::Unmeasured(Unmeasured::OtherTrade),
            };
            assert_eq!(verdict.reach, reach);
        }
        // A position the last read shows, with no trades row naming ours, stops not flat too,
        // before an unreported arm-time cancel.
        let moved = Facts {
            after: Some(Listing {
                x: false,
                y: false,
                flat: false,
            }),
            arm_reported_x: false,
            x_before_drop: Some(false),
            ..passing()
        };
        let verdict = DropVerdict::judge(&moved);
        assert_eq!(verdict.failures, [Failure::NotFlat]);
        // Not flat, X's absence before the drop is not shown a cancel (decision 0132 item 4).
        assert_eq!(verdict.arm, ArmCancel::Unmeasured(Unmeasured::NotFlat));
        assert_eq!(verdict.stop(), Some(Stop::NotFlat));
        // A fill comes before a position: a trades row and a position, a fill.
        let both = Facts {
            traded: Some([0, 1]),
            ..moved
        };
        assert_eq!(DropVerdict::judge(&both).stop(), Some(Stop::UnexpectedFill));
    }

    #[test]
    fn x_absent_counts_as_a_cancel_only_with_its_trades_read_whole_naming_nothing_and_flat() {
        // Carried from FBC-23ch's fourth review fix: X absent after the drop is the drop's
        // cancel only as places::close_verdict counts it after drop session one (decision 0132
        // item 4). A row naming X, then any other row, then a position, then trades not read
        // whole, each leaves the reach not measured, its reason given.
        assert_eq!(DropVerdict::judge(&passing()).reach, Reach::Cancelled);
        let not_flat = Listing {
            x: false,
            y: false,
            flat: false,
        };
        for (facts, why) in [
            (
                Facts {
                    traded: Some([1, 0]),
                    other_trades: 1,
                    after: Some(not_flat),
                    ..passing()
                },
                Unmeasured::Traded,
            ),
            // A row naming Y is another order's: X's absence is not shown a cancel either.
            (
                Facts {
                    traded: Some([0, 1]),
                    ..passing()
                },
                Unmeasured::OtherTrade,
            ),
            (
                Facts {
                    other_trades: 1,
                    after: Some(not_flat),
                    traded: None,
                    ..passing()
                },
                Unmeasured::OtherTrade,
            ),
            (
                Facts {
                    after: Some(not_flat),
                    traded: None,
                    ..passing()
                },
                Unmeasured::NotFlat,
            ),
            (
                Facts {
                    traded: None,
                    ..passing()
                },
                Unmeasured::TradesUnread,
            ),
        ] {
            let verdict = DropVerdict::judge(&facts);
            assert_eq!(verdict.reach, Reach::Unmeasured(why), "{facts:?}");
            assert!(!verdict.passed, "{facts:?}");
            assert!(verdict.reach.says().ends_with(why.says()), "{why:?}");
        }
        // Another order's row, or one naming none, fails the test too, as something else that
        // traded on the account, and stops it DropFailed.
        let other = Facts {
            other_trades: 2,
            ..passing()
        };
        let verdict = DropVerdict::judge(&other);
        assert_eq!(verdict.failures, [Failure::OtherTrade]);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
        assert_eq!(
            verdict.says(),
            "failed: a get_user_trades row since X's send names another order or none: \
             something else traded on the account (decision 0132 item 4)"
        );
        // X absent with its absence shown a cancel but no historic-orders row naming it is not
        // shown ended (0151 item 5): the reach is not shown, not unmeasured.
        let unlisted = Facts {
            historic: Some([None, Some("CANCELLED".to_owned())]),
            ..passing()
        };
        assert_eq!(DropVerdict::judge(&unlisted).reach, Reach::NotShown);
        // X still listed after the drop is not shown cancelled, whatever its trades read.
        let kept = Facts {
            after: Some(Listing {
                x: true,
                y: false,
                flat: true,
            }),
            traded: None,
            ..passing()
        };
        assert_eq!(DropVerdict::judge(&kept).reach, Reach::NotShown);
    }

    #[test]
    fn x_absent_before_the_drop_is_an_unreported_arm_cancel_only_when_shown_a_cancel() {
        // Carried from FBC-23ch's fourth review fix: X absent from the read before the drop with
        // no report is the arm's unreported cancel only once its absence is shown a cancel by the
        // reads after the drop (decision 0132 item 4); otherwise what the arm did is not
        // measured, its reason given, and the run stops on the verdict's other stops, never on
        // ArmCancelUnreported.
        let silent = Facts {
            x_before_drop: Some(false),
            ..passing()
        };
        let verdict = DropVerdict::judge(&silent);
        assert_eq!(verdict.arm, ArmCancel::Unreported);
        assert_eq!(verdict.stop(), Some(Stop::ArmCancelUnreported));
        for (facts, why, stop) in [
            (
                Facts {
                    traded: Some([1, 0]),
                    ..silent.clone()
                },
                Unmeasured::Traded,
                Stop::UnexpectedFill,
            ),
            (
                Facts {
                    other_trades: 1,
                    ..silent.clone()
                },
                Unmeasured::OtherTrade,
                Stop::DropFailed,
            ),
            (
                Facts {
                    after: None,
                    ..silent.clone()
                },
                Unmeasured::NotReadAfter,
                Stop::DropFailed,
            ),
            (
                Facts {
                    traded: None,
                    ..silent.clone()
                },
                Unmeasured::TradesUnread,
                Stop::DropFailed,
            ),
        ] {
            let verdict = DropVerdict::judge(&facts);
            assert_eq!(verdict.arm, ArmCancel::Unmeasured(why), "{facts:?}");
            assert_eq!(verdict.stop(), Some(stop), "{facts:?}");
            assert!(verdict.arm.says().ends_with(why.says()), "{why:?}");
        }
    }

    #[test]
    fn historic_orders_not_listing_y_or_x_fails_the_test() {
        for (historic, which) in [
            ([Some("CANCELLED".to_owned()), None], Which::Y),
            ([None, Some("EXPIRED".to_owned())], Which::X),
        ] {
            let facts = Facts {
                historic: Some(historic),
                ..passing()
            };
            let verdict = DropVerdict::judge(&facts);
            assert_eq!(verdict.failures, [Failure::NotTerminal(which)]);
            assert_eq!(verdict.stop(), Some(Stop::DropFailed));
            let says = verdict.says();
            let want = format!(
                "failed: no GET /user/historic-orders row names {}",
                which.name()
            );
            assert_eq!(says, want);
        }
        // Whatever the status: a row naming it is terminal (0151 item 5).
        let odd = Facts {
            historic: Some([
                Some("(not a status name)".to_owned()),
                Some("FILLED".to_owned()),
            ]),
            ..passing()
        };
        assert!(DropVerdict::judge(&odd).passed);
    }

    #[test]
    fn a_read_not_made_fails_the_test_and_names_each_gap() {
        let facts = Facts {
            y_rested: false,
            after: None,
            traded: None,
            historic: None,
            ..passing()
        };
        let verdict = DropVerdict::judge(&facts);
        let want = [
            Failure::YNotRested,
            Failure::NotReadAfterDrop,
            Failure::TradesNotRead,
            Failure::HistoricNotRead,
        ];
        assert_eq!(verdict.failures, want);
        assert_eq!(verdict.reach, Reach::NotShown);
        assert_eq!(verdict.terminal, [None, None]);
        assert_eq!(verdict.stop(), Some(Stop::DropFailed));
        assert_eq!(
            verdict.says(),
            "failed: Y never rested before the drop; no read after the drop was made; the trades \
             since X's send were not read whole; GET /user/historic-orders was not read whole"
        );
        // Every failure, every arm and every reach is said with no id and no value.
        let said = [
            Failure::NotReadBeforeDrop.says(),
            Failure::RateRefused.says(),
            Failure::Listed(Which::X).says(),
            Failure::NotFlat.says(),
            Failure::Traded(Which::Y).says(),
        ];
        assert_eq!(
            said,
            [
                "the read before the drop was not made",
                "a provider rate refusal cut the reads short (decision 0148 item 8)",
                "X still listed by get_user_orders when the wait ended",
                "a position that is not flat after the drop",
                "a get_user_trades row names Y",
            ]
        );
        for arm in [
            ArmCancel::NotMeasured,
            ArmCancel::Survived,
            ArmCancel::Reported,
            ArmCancel::Unreported,
            ArmCancel::Unmeasured(Unmeasured::NotFlat),
        ] {
            assert!(!arm.says().is_empty());
        }
        for reach in [
            Reach::NotMeasured,
            Reach::Cancelled,
            Reach::NotShown,
            Reach::Unmeasured(Unmeasured::TradesUnread),
        ] {
            assert!(!reach.says().is_empty());
        }
    }

    /// A reader playing `attempts` in order, recording when each read began and the end it was
    /// told ([`Reader::until`]); each read takes the next of `lengths`, then 30 ms.
    struct Played {
        attempts: VecDeque<Attempt>,
        at: Vec<Instant>,
        lengths: VecDeque<Duration>,
        until: Option<Instant>,
    }

    impl Reader for Played {
        async fn read(&mut self) -> Attempt {
            self.at.push(Instant::now());
            let length = self.lengths.pop_front();
            tokio::time::sleep(length.unwrap_or(Duration::from_millis(30))).await;
            self.attempts.pop_front().unwrap_or(Attempt::Skipped)
        }

        fn until(&mut self, end: Instant) {
            self.until = Some(end);
        }
    }

    fn played(attempts: Vec<Attempt>) -> Played {
        Played {
            attempts: attempts.into(),
            at: Vec::new(),
            lengths: VecDeque::new(),
            until: None,
        }
    }

    const CADENCE: Cadence = Cadence {
        interval: Duration::from_millis(1_000),
        wait: Duration::from_millis(5_000),
        cap: MAX_READS,
    };

    /// A read listing `ids`, each by its venue id, and the position `position`.
    fn read(ids: &[u8], position: &str) -> Attempt {
        let listed = ids.iter().map(|n| listed(*n, CidMatch::Unparseable));
        Attempt::Read(Read {
            listed: listed.collect(),
            twaps: 0,
            position: Decimal::from_str(position).unwrap(),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn the_reads_after_the_drop_end_at_the_first_listing_neither_at_the_read_interval() {
        let (x, y) = orders(Some(2));
        let mut reader = played(vec![
            read(&[1, 2], "0"),
            Attempt::Skipped,
            read(&[2], "0"),
            Attempt::Failed("synthetic".to_owned()),
            read(&[], "0"),
            read(&[1], "0"),
        ]);
        let after = until_absent(&mut reader, (&x, &y), CADENCE).await;
        let shown: Vec<Shown> = after.reads.iter().map(|(_, s)| *s).collect();
        let want = [
            Shown::Listed,
            Shown::Skipped,
            Shown::Listed,
            Shown::Failed,
            Shown::Absent,
        ];
        assert_eq!(shown, want);
        // One read at once, then one each interval after the one before began.
        let began: Vec<Duration> = after.reads.iter().map(|(at, _)| *at).collect();
        let want = [0, 1_000, 2_000, 3_000, 4_000].map(Duration::from_millis);
        assert_eq!(began, want);
        let gaps: Vec<Duration> = reader.at.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps.iter().all(|g| *g >= CADENCE.interval), "{gaps:?}");
        assert_eq!(after.absent_after, Some(Duration::from_millis(4_030)));
        let last = Listing {
            x: false,
            y: false,
            flat: true,
        };
        assert_eq!((after.last, after.refused), (Some(last), None));
    }

    #[tokio::test(start_paused = true)]
    async fn the_reads_after_the_drop_stop_at_the_wait_or_a_refusal() {
        let (x, y) = orders(None);
        // Y named by its client id alone, listed at every read: the wait ends them, none
        // beginning past it, the last read the one judged.
        let y_listed = Attempt::Read(Read {
            listed: vec![listed(7, CidMatch::Ours(cid(2)))],
            twaps: 0,
            position: Decimal::from_str("0.01").unwrap(),
        });
        let mut reader = played(vec![y_listed; 10]);
        let after = until_absent(&mut reader, (&x, &y), CADENCE).await;
        // One read at once and one each second before the 5 s wait ends: a read due at the end
        // itself is not made (0151 item 4).
        assert_eq!(after.reads.len(), 5);
        assert!(after.reads.iter().all(|(at, _)| *at < CADENCE.wait));
        assert!(after.reads.iter().all(|(_, s)| *s == Shown::Listed));
        let last = Listing {
            x: false,
            y: true,
            flat: false,
        };
        assert_eq!((after.last, after.absent_after), (Some(last), None));
        // At most the cap, however long the wait.
        let long = Cadence {
            wait: Duration::from_millis(60_000),
            ..CADENCE
        };
        let mut reader = played(Vec::new());
        let after = until_absent(&mut reader, (&x, &y), long).await;
        assert_eq!(reader.at.len(), 10);
        assert_eq!(after.last, None);
        // A provider rate refusal ends them at once.
        let mut reader = played(vec![read(&[1], "0"), Attempt::Refused(RefusedOn::Rest)]);
        let after = until_absent(&mut reader, (&x, &y), CADENCE).await;
        assert_eq!(reader.at.len(), 2);
        assert_eq!(after.refused, Some(RefusedOn::Rest));
        let shown: Vec<Shown> = after.reads.iter().map(|(_, s)| *s).collect();
        assert_eq!(shown, [Shown::Listed, Shown::Refused]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_after_the_drop_still_running_at_the_end_of_the_wait_is_cut_and_not_judged() {
        // FBC-23ch's second review fix carried to the reads after the drop: the third read
        // begins inside a 2.5 s wait and lists neither X nor Y a second later, past it. It is not
        // judged, so the reads end on the last one finished within the wait, which lists Y.
        let (x, y) = orders(Some(2));
        let short = Cadence {
            wait: Duration::from_millis(2_500),
            ..CADENCE
        };
        let mut reader = played(vec![read(&[2], "0"), read(&[2], "0"), read(&[], "0")]);
        reader.lengths = [30, 30, 1_000].map(Duration::from_millis).into();
        let start = Instant::now();
        let after = until_absent(&mut reader, (&x, &y), short).await;
        assert_eq!(reader.until, Some(start + short.wait));
        let shown: Vec<Shown> = after.reads.iter().map(|(_, s)| *s).collect();
        assert_eq!(shown, [Shown::Listed, Shown::Listed, Shown::Unfinished]);
        assert_eq!(after.absent_after, None);
        let last = Listing {
            x: false,
            y: true,
            flat: true,
        };
        assert_eq!((after.last, after.refused), (Some(last), None));
        // The cut read's answer is still read, so they end with it, not at the wait's end.
        assert_eq!(start.elapsed(), Duration::from_millis(3_000));
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_a_read_after_the_drop_hears_past_the_end_of_the_wait_still_ends_them() {
        // The refusal answering a read begun inside the wait comes past its end: it is heard,
        // not thrown away with the read, so it ends the reads as a refusal (decision 0148 item
        // 8).
        let (x, y) = orders(Some(2));
        let short = Cadence {
            wait: Duration::from_millis(2_500),
            ..CADENCE
        };
        for on in [RefusedOn::ReadSocket, RefusedOn::Rest] {
            let attempts = vec![read(&[2], "0"), read(&[2], "0"), Attempt::Refused(on)];
            let mut reader = played(attempts);
            reader.lengths = [30, 30, 600].map(Duration::from_millis).into();
            let after = until_absent(&mut reader, (&x, &y), short).await;
            let shown: Vec<Shown> = after.reads.iter().map(|(_, s)| *s).collect();
            assert_eq!(shown, [Shown::Listed, Shown::Listed, Shown::Refused]);
            assert_eq!(after.refused, Some(on));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_reads_after_the_drop_wait_for_no_read_due_past_the_end_of_the_wait() {
        // The first read runs 4.5 s of a 5 s wait, so the second begins at once and the third
        // would be due at 5.5 s, past the wait: they end after the second, not by waiting for a
        // slot no read may begin in (0151 item 4).
        let (x, y) = orders(Some(2));
        let mut reader = played(Vec::new());
        reader.lengths = [4_500].map(Duration::from_millis).into();
        let start = Instant::now();
        let after = until_absent(&mut reader, (&x, &y), CADENCE).await;
        assert_eq!(after.reads.len(), 2, "{after:?}");
        assert_eq!(start.elapsed(), Duration::from_millis(4_530));
    }
}
