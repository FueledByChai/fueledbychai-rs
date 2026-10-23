//! When an instrument trades, as data (decision 0125, design §4.4).
//!
//! A [`SessionCalendar`] is the venue's timetable for one instrument: an earliest open, a close
//! that never reopens, dated [`ClosedWindow`]s (published maintenance) and [`WeeklyClosure`]s
//! in a local wall time (an FX weekend). It only ever closes: [`SessionCalendar::is_open`] is
//! true when no part of it closes the instrument at an instant, and whether the instrument
//! trades then is still its [`TradingStatus`](crate::TradingStatus) too.
//!
//! It reads no clock and holds no time-zone database: the caller supplies the instant, and a
//! weekly closure's zone is a [`UtcOffsetRule`], a standard offset and an optional daylight
//! rule in POSIX `Mm.w.d/time` form with each transition's time of day below 24:00, evaluated
//! here by integer civil-date arithmetic. The same calendar answers the same instant alike
//! live, in replay and on every host.
//!
//! Every interval is half-open: closed at a closing instant, open at a reopening one. A weekly
//! closure's occurrence runs from the first instant the zone's wall clock reads its start to
//! the last instant it reads its end, an edge the clock skips taking effect at the jump. So a
//! daylight-saving change fails closed: an edge in a repeated hour, an end at the very wall
//! time the clock falls back to among them, holds the instrument closed through both passes,
//! and an occurrence wholly inside one skipped stretch closes nothing that week.

use crate::time::WallNs;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const SECS_PER_DAY: i64 = 86_400;
const SECS_PER_WEEK: i64 = 7 * SECS_PER_DAY;

/// A day of the week.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

/// A month of the year.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Month {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

/// Which of a month's weeks a [`Transition`] falls in: the first to fourth such weekday, or the
/// last (POSIX's week 5).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum WeekOfMonth {
    First,
    Second,
    Third,
    Fourth,
    Last,
}

/// A local wall time of day: whole seconds after local midnight, below 86 400.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct LocalTime(u32);

impl LocalTime {
    /// `h:m:s`, or `None` when the hour is 24 or more or a minute or second 60 or more.
    pub const fn hms(h: u8, m: u8, s: u8) -> Option<LocalTime> {
        if h >= 24 || m >= 60 || s >= 60 {
            return None;
        }
        Some(LocalTime(h as u32 * 3_600 + m as u32 * 60 + s as u32))
    }

    /// Seconds after local midnight.
    pub const fn seconds(self) -> u32 {
        self.0
    }
}

/// A local wall time in the week: a weekday and a time of day.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WeekTime {
    pub weekday: Weekday,
    pub at: LocalTime,
}

impl WeekTime {
    /// Seconds after the week's Monday 00:00.
    const fn secs(self) -> i64 {
        self.weekday as i64 * SECS_PER_DAY + self.at.0 as i64
    }

    const fn same(self, other: WeekTime) -> bool {
        self.weekday as u8 == other.weekday as u8 && self.at.0 == other.at.0
    }
}

/// A POSIX `Mm.w.d/time` transition: the given weekday of the month's given week, at a local
/// wall time of that day. A transition at 24:00 or later, or before midnight, which POSIX's
/// `/24` and RFC 9636's extended hours allow, cannot be stated (decision 0125).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Transition {
    pub month: Month,
    pub week: WeekOfMonth,
    pub weekday: Weekday,
    pub at: LocalTime,
}

impl Transition {
    const fn same(self, other: Transition) -> bool {
        self.month as u8 == other.month as u8
            && self.week as u8 == other.week as u8
            && self.weekday as u8 == other.weekday as u8
            && self.at.0 == other.at.0
    }
}

/// Daylight time: `save_secs` added to the standard offset from `start` until `end`. As in
/// POSIX, `start`'s wall time is read in standard time and `end`'s in daylight time, and a
/// rule whose start is later in the year than its end (a southern one) needs nothing more.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct DstRule {
    save_secs: i32,
    start: Transition,
    end: Transition,
}

impl DstRule {
    /// The rule, or `None` when `save_secs` is zero or a day or more either way, or when
    /// `start` is `end`.
    pub const fn new(save_secs: i32, start: Transition, end: Transition) -> Option<DstRule> {
        if save_secs == 0 || save_secs <= -86_400 || save_secs >= 86_400 || start.same(end) {
            return None;
        }
        Some(DstRule {
            save_secs,
            start,
            end,
        })
    }

    /// Seconds added to the standard offset in daylight time (negative where daylight time is
    /// the zone's winter).
    pub const fn save_secs(&self) -> i32 {
        self.save_secs
    }

    /// When daylight time starts, read in standard time.
    pub const fn start(&self) -> Transition {
        self.start
    }

    /// When daylight time ends, read in daylight time.
    pub const fn end(&self) -> Transition {
        self.end
    }
}

/// A zone's offset from UTC as data: seconds east of UTC in standard time and an optional
/// daylight rule. It holds no zone name; a venue crate states its zone's rule and cites it.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct UtcOffsetRule {
    std_offset_secs: i32,
    dst: Option<DstRule>,
}

impl UtcOffsetRule {
    /// A fixed offset, or `None` when it is a day or more either way.
    pub const fn fixed(offset_secs: i32) -> Option<UtcOffsetRule> {
        if offset_secs <= -86_400 || offset_secs >= 86_400 {
            return None;
        }
        Some(UtcOffsetRule {
            std_offset_secs: offset_secs,
            dst: None,
        })
    }

    /// A standard offset with daylight time, or `None` when the standard or the daylight
    /// offset is a day or more either way.
    pub const fn with_dst(std_offset_secs: i32, dst: DstRule) -> Option<UtcOffsetRule> {
        let daylight = std_offset_secs as i64 + dst.save_secs as i64;
        if std_offset_secs <= -86_400
            || std_offset_secs >= 86_400
            || daylight <= -86_400
            || daylight >= 86_400
        {
            return None;
        }
        Some(UtcOffsetRule {
            std_offset_secs,
            dst: Some(dst),
        })
    }

    /// Seconds east of UTC in standard time.
    pub const fn std_offset_secs(&self) -> i32 {
        self.std_offset_secs
    }

    /// The daylight rule, `None` for a fixed offset.
    pub const fn dst(&self) -> Option<DstRule> {
        self.dst
    }

    /// The offset in force at `utc_secs`: standard plus the save when the latest transition at
    /// or before it is a start, standard otherwise.
    fn offset_secs(&self, utc_secs: i64) -> i64 {
        let std = i64::from(self.std_offset_secs);
        match (self.dst, self.latest_transition(utc_secs)) {
            (Some(dst), Some((_, true))) => std + i64::from(dst.save_secs),
            _ => std,
        }
    }

    /// The latest transition at or before `utc_secs`, as its instant and whether it is a start,
    /// from the transitions of the years around it; an end wins a tie with a start.
    fn latest_transition(&self, utc_secs: i64) -> Option<(i64, bool)> {
        let dst = self.dst?;
        let std = i64::from(self.std_offset_secs);
        let save = i64::from(dst.save_secs);
        let (year, _, _) = civil_from_days((utc_secs + std).div_euclid(SECS_PER_DAY));
        // Ordered by instant, then an end (`true`) after a start at the same instant.
        let mut latest: Option<(i64, bool)> = None;
        for y in year - 1..=year + 1 {
            for (transition, is_end) in [(dst.start, false), (dst.end, true)] {
                let wall =
                    transition_day(y, transition) * SECS_PER_DAY + i64::from(transition.at.0);
                let instant = if is_end {
                    wall - std - save
                } else {
                    wall - std
                };
                if instant <= utc_secs {
                    latest = latest.max(Some((instant, is_end)));
                }
            }
        }
        latest.map(|(instant, is_end)| (instant, !is_end))
    }

    /// For a local wall time `wall` (seconds since the local epoch): the first and the last
    /// instant the wall clock reads `wall`. Both are its one reading where it is read once, the
    /// earlier and the later reading where it is repeated, and the jump where it is skipped.
    fn bounds(&self, wall: i64) -> (i64, i64) {
        let std = i64::from(self.std_offset_secs);
        let save = self.dst.map_or(0, |dst| i64::from(dst.save_secs));
        let (standard, daylight) = (wall - std, wall - std - save);
        let (lo, hi) = (standard.min(daylight), standard.max(daylight));
        let reads = |t: i64| t + self.offset_secs(t) == wall;
        match (reads(lo), reads(hi)) {
            (true, true) => (lo, hi),
            (true, false) => (lo, lo),
            (false, true) => (hi, hi),
            // Skipped: the offset changes between the two readings, so the latest transition at
            // or before the later one is the jump.
            (false, false) => {
                let jump = self
                    .latest_transition(hi)
                    .map_or(hi, |(instant, _)| instant);
                (jump, jump)
            }
        }
    }
}

/// A closure every week, from `from` to `to` in a zone's wall time; `to` before `from` wraps
/// past Sunday 24:00. It lasts under a week.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WeeklyClosure {
    zone: UtcOffsetRule,
    from: WeekTime,
    to: WeekTime,
}

impl WeeklyClosure {
    /// The closure, or `None` when `from` is `to`, which could mean nothing or the whole week.
    pub const fn new(zone: UtcOffsetRule, from: WeekTime, to: WeekTime) -> Option<WeeklyClosure> {
        if from.same(to) {
            return None;
        }
        Some(WeeklyClosure { zone, from, to })
    }

    /// The zone whose wall time `from` and `to` are read in.
    pub const fn zone(&self) -> UtcOffsetRule {
        self.zone
    }

    /// When each week's closure starts.
    pub const fn from(&self) -> WeekTime {
        self.from
    }

    /// When each week's closure ends.
    pub const fn to(&self) -> WeekTime {
        self.to
    }

    /// Whether an occurrence closes the instrument at `utc_secs`. The occurrences that can
    /// hold it are those starting in the local week it falls in and the weeks either side.
    fn closes(&self, utc_secs: i64) -> bool {
        let local = utc_secs + self.zone.offset_secs(utc_secs);
        // The local week's Monday 00:00: 1970-01-01 was a Thursday.
        let monday =
            (local + 3 * SECS_PER_DAY).div_euclid(SECS_PER_WEEK) * SECS_PER_WEEK - 3 * SECS_PER_DAY;
        let from = self.from.secs();
        let length = (self.to.secs() - from).rem_euclid(SECS_PER_WEEK);
        (-1..=1).any(|week| {
            let start = monday + week * SECS_PER_WEEK + from;
            let (opens_closed, _) = self.zone.bounds(start);
            let (_, reopens) = self.zone.bounds(start + length);
            opens_closed <= utc_secs && utc_secs < reopens
        })
    }
}

/// A dated closure, closed from `from` until `to`: a published maintenance window.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ClosedWindow {
    from: WallNs,
    to: WallNs,
}

impl ClosedWindow {
    /// The window, or `None` when `to` is not after `from`.
    pub const fn new(from: WallNs, to: WallNs) -> Option<ClosedWindow> {
        if to.0 <= from.0 {
            return None;
        }
        Some(ClosedWindow { from, to })
    }

    /// The first instant it closes.
    pub const fn from(&self) -> WallNs {
        self.from
    }

    /// The instant it reopens.
    pub const fn to(&self) -> WallNs {
        self.to
    }
}

/// What a [`SessionCalendar`] is made of. Parts that close nothing make no calendar
/// ([`SessionCalendar::new`] returns `None`).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct CalendarParts {
    /// The earliest open: closed before it.
    pub first_open: Option<WallNs>,
    /// A close that never reopens: closed at and after it.
    pub final_close: Option<WallNs>,
    /// Dated closures.
    pub windows: Vec<ClosedWindow>,
    /// Closures every week.
    pub weekly: Vec<WeeklyClosure>,
}

/// When a venue's timetable closes an instrument (decision 0125). It only ever closes, and
/// `Some` always closes something: an instrument with no timetable has no calendar.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SessionCalendar {
    parts: CalendarParts,
}

impl SessionCalendar {
    /// The calendar of `parts`, its windows and weekly closures sorted and deduplicated so two
    /// listings of the same parts are the same calendar; `None` when the parts close nothing.
    pub fn new(mut parts: CalendarParts) -> Option<SessionCalendar> {
        // Closed before the start of the clock is closed at no instant.
        if parts.first_open == Some(WallNs(i64::MIN)) {
            parts.first_open = None;
        }
        parts.windows.sort_unstable();
        parts.windows.dedup();
        parts.weekly.sort_unstable();
        parts.weekly.dedup();
        let closes = parts.first_open.is_some()
            || parts.final_close.is_some()
            || !parts.windows.is_empty()
            || !parts.weekly.is_empty();
        closes.then_some(SessionCalendar { parts })
    }

    /// Its parts, in canonical order.
    pub fn parts(&self) -> &CalendarParts {
        &self.parts
    }

    /// Whether no part of the calendar closes the instrument at `at`. Reads no clock.
    pub fn is_open(&self, at: WallNs) -> bool {
        let parts = &self.parts;
        if parts.first_open.is_some_and(|open| at < open) {
            return false;
        }
        if parts.final_close.is_some_and(|close| at >= close) {
            return false;
        }
        if parts.windows.iter().any(|w| w.from <= at && at < w.to) {
            return false;
        }
        // Every weekly bound and offset is a whole second, so the second `at` falls in decides.
        let secs = at.0.div_euclid(NANOS_PER_SEC);
        !parts.weekly.iter().any(|closure| closure.closes(secs))
    }
}

/// The day of `transition` in `year`, as days since 1970-01-01.
fn transition_day(year: i64, transition: Transition) -> i64 {
    let month = transition.month as i64 + 1;
    let weekday = transition.weekday as i64;
    match transition.week {
        WeekOfMonth::Last => {
            let next_first = if month == 12 {
                days_from_civil(year + 1, 1, 1)
            } else {
                days_from_civil(year, month + 1, 1)
            };
            let last = next_first - 1;
            last - (weekday_of(last) - weekday).rem_euclid(7)
        }
        week => {
            let first = days_from_civil(year, month, 1);
            first + (weekday - weekday_of(first)).rem_euclid(7) + 7 * week as i64
        }
    }
}

/// The weekday of a day since 1970-01-01, Monday 0: 1970-01-01 was a Thursday.
fn weekday_of(days: i64) -> i64 {
    (days + 3).rem_euclid(7)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's days_from_civil).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The proleptic Gregorian date of a day since 1970-01-01 (Howard Hinnant's civil_from_days).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS: i64 = 1_000_000_000;
    const DAY: i64 = 86_400;
    const YEAR_NS: i64 = 365 * DAY * NS;

    const fn hm(h: u8, m: u8) -> LocalTime {
        LocalTime::hms(h, m, 0).unwrap()
    }

    /// New York's rule as POSIX states it (EST5EDT,M3.2.0/2,M11.1.0/2), built as a checked
    /// const the way a venue crate states its own.
    const NEW_YORK: UtcOffsetRule = UtcOffsetRule::with_dst(
        -18_000,
        DstRule::new(
            3_600,
            Transition {
                month: Month::March,
                week: WeekOfMonth::Second,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::November,
                week: WeekOfMonth::First,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
        )
        .unwrap(),
    )
    .unwrap();

    /// Berlin's: the last Sunday of March and of October (CET-1CEST,M3.5.0,M10.5.0/3).
    const BERLIN: UtcOffsetRule = UtcOffsetRule::with_dst(
        3_600,
        DstRule::new(
            3_600,
            Transition {
                month: Month::March,
                week: WeekOfMonth::Last,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::October,
                week: WeekOfMonth::Last,
                weekday: Weekday::Sunday,
                at: hm(3, 0),
            },
        )
        .unwrap(),
    )
    .unwrap();

    /// Sydney's: daylight time starts in October and ends in April of the next year
    /// (AEST-10AEDT,M10.1.0,M4.1.0/3).
    const SYDNEY: UtcOffsetRule = UtcOffsetRule::with_dst(
        36_000,
        DstRule::new(
            3_600,
            Transition {
                month: Month::October,
                week: WeekOfMonth::First,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::April,
                week: WeekOfMonth::First,
                weekday: Weekday::Sunday,
                at: hm(3, 0),
            },
        )
        .unwrap(),
    )
    .unwrap();

    /// Dublin's, whose save is negative: standard time is summer's +1 h, and winter is the
    /// "daylight" time at +0 (IST-1GMT0,M10.5.0,M3.5.0/1).
    const DUBLIN: UtcOffsetRule = UtcOffsetRule::with_dst(
        3_600,
        DstRule::new(
            -3_600,
            Transition {
                month: Month::October,
                week: WeekOfMonth::Last,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::March,
                week: WeekOfMonth::Last,
                weekday: Weekday::Sunday,
                at: hm(1, 0),
            },
        )
        .unwrap(),
    )
    .unwrap();

    const UTC: UtcOffsetRule = UtcOffsetRule::fixed(0).unwrap();

    fn at(secs: i64) -> WallNs {
        WallNs(secs * NS)
    }

    fn before(instant: WallNs) -> WallNs {
        WallNs(instant.0 - 1)
    }

    fn week_time(weekday: Weekday, at: LocalTime) -> WeekTime {
        WeekTime { weekday, at }
    }

    fn weekly(zone: UtcOffsetRule, from: WeekTime, to: WeekTime) -> SessionCalendar {
        let closure = WeeklyClosure::new(zone, from, to).unwrap();
        SessionCalendar::new(CalendarParts {
            first_open: None,
            final_close: None,
            windows: vec![],
            weekly: vec![closure],
        })
        .unwrap()
    }

    /// Hibachi's FX closure: Friday 17:00 to Sunday 17:00 in New York.
    fn fx_weekend() -> SessionCalendar {
        weekly(
            NEW_YORK,
            week_time(Weekday::Friday, hm(17, 0)),
            week_time(Weekday::Sunday, hm(17, 0)),
        )
    }

    fn bounds(first_open: Option<WallNs>, final_close: Option<WallNs>) -> SessionCalendar {
        SessionCalendar::new(CalendarParts {
            first_open,
            final_close,
            windows: vec![],
            weekly: vec![],
        })
        .unwrap()
    }

    #[test]
    fn a_weekly_closure_closes_the_new_york_fx_weekend() {
        let calendar = fx_weekend();
        // Each week's close and reopening in UTC seconds, checked against zoneinfo: in
        // winter, across the March change (47 hours), in summer and across the November
        // change (49 hours).
        let weeks = [
            (1_799_445_600, 1_799_618_400),
            (1_804_888_800, 1_805_058_000),
            (1_815_166_800, 1_815_339_600),
            (1_825_448_400, 1_825_624_800),
        ];
        for (close, reopen) in weeks {
            let (close, reopen) = (at(close), at(reopen));
            assert!(calendar.is_open(before(close)), "{close:?}");
            assert!(!calendar.is_open(close), "{close:?}");
            // The Saturday between.
            assert!(!calendar.is_open(WallNs(close.0 + DAY * NS)), "{close:?}");
            assert!(!calendar.is_open(before(reopen)), "{reopen:?}");
            assert!(calendar.is_open(reopen), "{reopen:?}");
        }
        assert_eq!(1_805_058_000 - 1_804_888_800, 47 * 3_600);
        assert_eq!(1_825_624_800 - 1_825_448_400, 49 * 3_600);
    }

    #[test]
    fn a_final_close_never_reopens() {
        let close = at(1_830_000_000);
        let calendar = bounds(None, Some(close));
        assert!(calendar.is_open(before(close)));
        for probe in [close, WallNs(close.0 + 1), WallNs(close.0 + YEAR_NS)] {
            assert!(!calendar.is_open(probe), "{probe:?}");
        }
        assert!(!calendar.is_open(WallNs(i64::MAX)));
    }

    #[test]
    fn an_earliest_open() {
        let open = at(1_800_000_000);
        let calendar = bounds(Some(open), None);
        assert!(!calendar.is_open(WallNs(i64::MIN)));
        assert!(!calendar.is_open(before(open)));
        assert!(calendar.is_open(open));
        assert!(calendar.is_open(WallNs(open.0 + YEAR_NS)));
    }

    #[test]
    fn a_first_open_at_or_after_the_final_close_never_opens() {
        let instant = at(1_800_000_000);
        for open in [instant, WallNs(instant.0 + YEAR_NS)] {
            let calendar = bounds(Some(open), Some(instant));
            let probes = [
                WallNs(i64::MIN),
                before(instant),
                instant,
                WallNs(instant.0 + YEAR_NS),
                WallNs(i64::MAX),
            ];
            for probe in probes {
                assert!(!calendar.is_open(probe), "{open:?} {probe:?}");
            }
        }
    }

    #[test]
    fn the_us_rule_reproduces_published_transitions() {
        // Each instant checked against zoneinfo's America/New_York.
        let springs = [
            1_772_953_200, // 2026-03-08
            1_805_007_600, // 2027-03-14
            1_836_457_200, // 2028-03-12
            1_867_906_800, // 2029-03-11
            1_899_356_400, // 2030-03-10
        ];
        for t in springs {
            assert_eq!(NEW_YORK.offset_secs(t - 1), -18_000, "{t}");
            assert_eq!(NEW_YORK.offset_secs(t), -14_400, "{t}");
        }
        let autumns = [
            1_793_512_800, // 2026-11-01
            1_825_567_200, // 2027-11-07
            1_857_016_800, // 2028-11-05
            1_888_466_400, // 2029-11-04
            1_919_916_000, // 2030-11-03
        ];
        for t in autumns {
            assert_eq!(NEW_YORK.offset_secs(t - 1), -14_400, "{t}");
            assert_eq!(NEW_YORK.offset_secs(t), -18_000, "{t}");
        }
        // A fixed rule has no daylight time: one offset everywhere.
        let tokyo = UtcOffsetRule::fixed(32_400).unwrap();
        for t in [
            i64::MIN / NS,
            -1,
            0,
            1_772_953_200,
            1_793_512_800,
            i64::MAX / NS,
        ] {
            assert_eq!(tokyo.offset_secs(t), 32_400);
        }
        assert_eq!(tokyo.std_offset_secs(), 32_400);
        assert_eq!(tokyo.dst(), None);
        assert_eq!(NEW_YORK.std_offset_secs(), -18_000);
        let dst = NEW_YORK.dst().unwrap();
        assert_eq!(dst.save_secs(), 3_600);
        assert_eq!(dst.start().month, Month::March);
        assert_eq!(dst.end().month, Month::November);
    }

    #[test]
    fn a_last_week_rule_and_a_southern_rule() {
        // Berlin, 2027: to +2 h on the last Sunday of March, back on the last of October.
        assert_eq!(BERLIN.offset_secs(1_806_195_600 - 1), 3_600);
        assert_eq!(BERLIN.offset_secs(1_806_195_600), 7_200);
        assert_eq!(BERLIN.offset_secs(1_824_944_400 - 1), 7_200);
        assert_eq!(BERLIN.offset_secs(1_824_944_400), 3_600);
        // Sydney: daylight time over the new year, its start later in the year than its end.
        assert_eq!(SYDNEY.offset_secs(1_775_318_400 - 1), 39_600);
        assert_eq!(SYDNEY.offset_secs(1_775_318_400), 36_000);
        assert_eq!(SYDNEY.offset_secs(1_791_043_200 - 1), 36_000);
        assert_eq!(SYDNEY.offset_secs(1_791_043_200), 39_600);
        assert_eq!(SYDNEY.offset_secs(1_806_768_000 - 1), 39_600);
        assert_eq!(SYDNEY.offset_secs(1_806_768_000), 36_000);
        assert_eq!(SYDNEY.offset_secs(1_822_492_800 - 1), 36_000);
        assert_eq!(SYDNEY.offset_secs(1_822_492_800), 39_600);
        // Dublin, a negative save: GMT from the last Sunday of October at 01:00Z, IST from
        // the last Sunday of March at 01:00Z (2027-10-31 and 2027-03-28).
        assert_eq!(DUBLIN.offset_secs(1_806_195_600 - 1), 0);
        assert_eq!(DUBLIN.offset_secs(1_806_195_600), 3_600);
        assert_eq!(DUBLIN.offset_secs(1_824_944_400 - 1), 3_600);
        assert_eq!(DUBLIN.offset_secs(1_824_944_400), 0);
    }

    #[test]
    fn a_skipped_edge_takes_effect_at_the_jump() {
        // Sunday 02:30 does not exist in New York on 2027-03-14: the closure starts at the
        // jump, 07:00Z, and ends at 04:00 EDT, 08:00Z.
        let calendar = weekly(
            NEW_YORK,
            week_time(Weekday::Sunday, hm(2, 30)),
            week_time(Weekday::Sunday, hm(4, 0)),
        );
        let jump = 1_805_007_600;
        assert!(calendar.is_open(at(jump - 1)));
        assert!(!calendar.is_open(at(jump)));
        assert!(!calendar.is_open(at(jump + 3_599)));
        assert!(calendar.is_open(at(jump + 3_600)));
    }

    #[test]
    fn a_repeated_edge_never_shortens_the_closure() {
        // Sunday 01:30 to 01:45 happens twice in New York on 2027-11-07: the closure holds
        // from the first 01:30 (05:30Z) through the second 01:45 (06:45Z).
        let calendar = weekly(
            NEW_YORK,
            week_time(Weekday::Sunday, hm(1, 30)),
            week_time(Weekday::Sunday, hm(1, 45)),
        );
        let (first, last) = (1_825_565_400, 1_825_569_900);
        assert!(calendar.is_open(at(first - 1)));
        assert!(!calendar.is_open(at(first)));
        assert!(!calendar.is_open(at(first + 40 * 60)));
        assert!(!calendar.is_open(at(last - 1)));
        assert!(calendar.is_open(at(last)));
    }

    #[test]
    fn an_end_at_the_time_the_clock_falls_back_to_holds_until_its_second_reading() {
        // Saturday 22:00 to Sunday 01:00 in New York, the week of 2027-11-07: the clock reads
        // 01:00 at 05:00Z (EDT) and again at 06:00Z (EST), and the closure ends at the later.
        let calendar = weekly(
            NEW_YORK,
            week_time(Weekday::Saturday, hm(22, 0)),
            week_time(Weekday::Sunday, hm(1, 0)),
        );
        let (close, first, second) = (1_825_552_800, 1_825_563_600, 1_825_567_200);
        assert_eq!(second - first, 3_600);
        assert!(calendar.is_open(at(close - 1)));
        for probe in [close, first - 1, first, first + 30 * 60, second - 1] {
            assert!(!calendar.is_open(at(probe)), "{probe}");
        }
        assert!(calendar.is_open(at(second)));
    }

    #[test]
    fn an_end_wins_a_tie_with_a_start() {
        // Daylight time starting at 02:00 standard and ending at 03:00 daylight on the same
        // day: both at 02:00Z, so the end wins and the zone keeps standard time all year.
        let tie = Transition {
            month: Month::March,
            week: WeekOfMonth::Second,
            weekday: Weekday::Sunday,
            at: hm(2, 0),
        };
        let rule = DstRule::new(
            3_600,
            tie,
            Transition {
                at: hm(3, 0),
                ..tie
            },
        )
        .unwrap();
        let zone = UtcOffsetRule::with_dst(0, rule).unwrap();
        let instant = 1_804_989_600; // 2027-03-14T02:00Z
        let probes = [
            1_800_014_400, // 2027-01-15T12:00Z
            instant - 1,
            instant,
            instant + 1,
            1_814_976_000, // 2027-07-07T16:00Z
            1_828_872_000, // 2027-12-15T12:00Z
        ];
        for probe in probes {
            assert_eq!(zone.offset_secs(probe), 0, "{probe}");
        }
    }

    #[test]
    fn a_transition_of_the_next_year_can_be_the_latest() {
        // Daylight time ending on the first Sunday of January at 00:30 daylight: on
        // 2023-01-01, a Sunday, that is 2022-12-31T23:30Z, still 2022 in standard time.
        let rule = DstRule::new(
            3_600,
            Transition {
                month: Month::July,
                week: WeekOfMonth::First,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::January,
                week: WeekOfMonth::First,
                weekday: Weekday::Sunday,
                at: hm(0, 30),
            },
        )
        .unwrap();
        let zone = UtcOffsetRule::with_dst(0, rule).unwrap();
        let end = 1_672_529_400;
        assert_eq!(zone.offset_secs(end - 15 * 60), 3_600);
        assert_eq!(zone.offset_secs(end), 0);
        assert_eq!(zone.offset_secs(end + 15 * 60), 0);
    }

    #[test]
    fn a_closure_is_found_from_the_local_week_far_from_utc() {
        // Closed all week but Sunday 17:00 to 18:00 at UTC-10. 2027-07-12T01:00Z is a Monday
        // in UTC and Sunday 15:00 there, inside the closure that started the Sunday before.
        let zone = UtcOffsetRule::fixed(-36_000).unwrap();
        let calendar = weekly(
            zone,
            week_time(Weekday::Sunday, hm(18, 0)),
            week_time(Weekday::Sunday, hm(17, 0)),
        );
        assert!(!calendar.is_open(at(1_815_354_000)));
        assert!(!calendar.is_open(at(1_815_361_200 - 1)));
        assert!(calendar.is_open(at(1_815_361_200))); // 17:00 there
        assert!(calendar.is_open(at(1_815_364_800 - 1)));
        assert!(!calendar.is_open(at(1_815_364_800))); // 18:00 there
    }

    #[test]
    fn a_closure_of_the_next_local_week_is_found_in_a_repeated_hour() {
        // Daylight time (+1 h over UTC) ending at Monday 00:30, 2027-11-01: the clock falls
        // back from Monday 00:30 to Sunday 23:30 at 2027-10-31T23:30Z. A closure from Monday
        // 00:00 to 06:00 starts at its first Monday 00:00 (23:00Z), so it holds the Sunday
        // 23:30 to 23:59 the clock reads again after the fall, in the local week before the
        // one the closure starts in.
        let rule = DstRule::new(
            3_600,
            Transition {
                month: Month::March,
                week: WeekOfMonth::Second,
                weekday: Weekday::Sunday,
                at: hm(2, 0),
            },
            Transition {
                month: Month::November,
                week: WeekOfMonth::First,
                weekday: Weekday::Monday,
                at: hm(0, 30),
            },
        )
        .unwrap();
        let zone = UtcOffsetRule::with_dst(0, rule).unwrap();
        let calendar = weekly(
            zone,
            week_time(Weekday::Monday, hm(0, 0)),
            week_time(Weekday::Monday, hm(6, 0)),
        );
        let (close, fall, reopen) = (1_825_023_600, 1_825_025_400, 1_825_048_800);
        assert!(calendar.is_open(at(close - 1)));
        for probe in [close, fall - 1, fall, fall + 15 * 60, reopen - 1] {
            assert!(!calendar.is_open(at(probe)), "{probe}");
        }
        assert!(calendar.is_open(at(reopen)));
    }

    #[test]
    fn a_closure_inside_a_skipped_hour_closes_nothing_that_week() {
        let calendar = weekly(
            NEW_YORK,
            week_time(Weekday::Sunday, hm(2, 10)),
            week_time(Weekday::Sunday, hm(2, 20)),
        );
        // 2027-03-14T05:00Z to 09:00Z, every minute: no 02:10 to 02:20 that Sunday.
        let from = 1_805_000_400;
        for minute in 0..=240 {
            let probe = at(from + minute * 60);
            assert!(calendar.is_open(probe), "{probe:?}");
        }
        // A week later it closes, at 02:10 EDT (2027-03-21T06:10Z).
        assert!(!calendar.is_open(at(1_805_609_400)));
    }

    #[test]
    fn a_skipped_edge_under_a_negative_save() {
        // Dublin skips 01:00 to 02:00 on 2027-03-28 (01:00Z): a closure from 01:30 to 03:00
        // starts at the jump and ends at 03:00 IST, 02:00Z.
        let calendar = weekly(
            DUBLIN,
            week_time(Weekday::Sunday, hm(1, 30)),
            week_time(Weekday::Sunday, hm(3, 0)),
        );
        let jump = 1_806_195_600;
        assert!(calendar.is_open(at(jump - 1)));
        assert!(!calendar.is_open(at(jump)));
        assert!(!calendar.is_open(at(jump + 3_599)));
        assert!(calendar.is_open(at(jump + 3_600)));
    }

    #[test]
    fn a_weekly_closure_may_wrap_the_week() {
        // Saturday 22:00 to Monday 02:00 UTC, the week of 2027-07-05.
        let calendar = weekly(
            UTC,
            week_time(Weekday::Saturday, hm(22, 0)),
            week_time(Weekday::Monday, hm(2, 0)),
        );
        let saturday_close = 1_815_256_800; // 2027-07-10T22:00Z
        let monday_open = 1_815_357_600; // 2027-07-12T02:00Z
        assert!(calendar.is_open(at(saturday_close - 1)));
        assert!(!calendar.is_open(at(saturday_close)));
        assert!(!calendar.is_open(at(saturday_close + DAY))); // Sunday
        assert!(!calendar.is_open(at(monday_open - 1)));
        assert!(calendar.is_open(at(monday_open)));
        // The Monday 01:00 of the week before belongs to the same kind of closure.
        assert!(!calendar.is_open(at(monday_open - 7 * DAY - 3_600)));
        assert!(calendar.is_open(at(monday_open - 3 * DAY))); // Friday
    }

    #[test]
    fn a_pre_epoch_weekend() {
        let calendar = weekly(
            UTC,
            week_time(Weekday::Friday, hm(17, 0)),
            week_time(Weekday::Sunday, hm(17, 0)),
        );
        // 1969-12-26 was a Friday: closed from 17:00Z, and at 22:00Z.
        let friday_close = -457_200;
        assert!(calendar.is_open(WallNs(friday_close * NS - 1)));
        assert!(!calendar.is_open(WallNs(friday_close * NS)));
        assert!(!calendar.is_open(WallNs(-439_200_000_000_000)));
        assert!(!calendar.is_open(WallNs((friday_close + 2 * DAY) * NS - 1)));
        assert!(calendar.is_open(WallNs((friday_close + 2 * DAY) * NS)));
    }

    #[test]
    fn a_dated_window_closes_from_its_start_until_its_end() {
        // A maintenance window, 18:00 to 20:00 UTC.
        let (from, to) = (at(1_811_786_400), at(1_811_793_600));
        let window = ClosedWindow::new(from, to).unwrap();
        assert_eq!((window.from(), window.to()), (from, to));
        let calendar = SessionCalendar::new(CalendarParts {
            first_open: None,
            final_close: None,
            windows: vec![window],
            weekly: vec![],
        })
        .unwrap();
        assert!(calendar.is_open(before(from)));
        assert!(!calendar.is_open(from));
        assert!(!calendar.is_open(before(to)));
        assert!(calendar.is_open(to));
    }

    #[test]
    fn constructors_refuse_what_no_venue_means() {
        assert_eq!(LocalTime::hms(24, 0, 0), None);
        assert_eq!(LocalTime::hms(0, 60, 0), None);
        assert_eq!(LocalTime::hms(0, 0, 60), None);
        assert_eq!(
            LocalTime::hms(23, 59, 59).map(LocalTime::seconds),
            Some(86_399)
        );
        assert_eq!(LocalTime::hms(0, 0, 0).map(LocalTime::seconds), Some(0));

        let start = Transition {
            month: Month::March,
            week: WeekOfMonth::Second,
            weekday: Weekday::Sunday,
            at: hm(2, 0),
        };
        let end = Transition {
            month: Month::November,
            week: WeekOfMonth::First,
            weekday: Weekday::Sunday,
            at: hm(2, 0),
        };
        assert_eq!(DstRule::new(0, start, end), None);
        assert_eq!(DstRule::new(86_400, start, end), None);
        assert_eq!(DstRule::new(-86_400, start, end), None);
        assert_eq!(DstRule::new(3_600, start, start), None);
        assert!(DstRule::new(86_399, start, end).is_some());
        assert!(DstRule::new(-86_399, start, end).is_some());
        // A start differing from the end in any one field is a rule.
        let month = Transition {
            month: Month::April,
            ..start
        };
        let week = Transition {
            week: WeekOfMonth::Third,
            ..start
        };
        let weekday = Transition {
            weekday: Weekday::Saturday,
            ..start
        };
        let time = Transition {
            at: hm(3, 0),
            ..start
        };
        for end in [month, week, weekday, time] {
            assert!(DstRule::new(3_600, start, end).is_some(), "{end:?}");
        }

        assert_eq!(UtcOffsetRule::fixed(86_400), None);
        assert_eq!(UtcOffsetRule::fixed(-86_400), None);
        assert!(UtcOffsetRule::fixed(86_399).is_some());
        assert!(UtcOffsetRule::fixed(-86_399).is_some());
        let hour = DstRule::new(3_600, start, end).unwrap();
        let back = DstRule::new(-3_600, start, end).unwrap();
        assert_eq!(UtcOffsetRule::with_dst(86_400, back), None);
        assert_eq!(UtcOffsetRule::with_dst(-86_400, hour), None);
        assert_eq!(UtcOffsetRule::with_dst(82_800, hour), None);
        assert_eq!(UtcOffsetRule::with_dst(-82_800, back), None);
        assert!(UtcOffsetRule::with_dst(82_799, hour).is_some());
        assert!(UtcOffsetRule::with_dst(-82_799, back).is_some());

        let friday = week_time(Weekday::Friday, hm(17, 0));
        assert_eq!(WeeklyClosure::new(UTC, friday, friday), None);
        let sunday = week_time(Weekday::Sunday, hm(17, 0));
        let later = week_time(Weekday::Friday, hm(17, 1));
        for to in [sunday, later] {
            let closure = WeeklyClosure::new(NEW_YORK, friday, to).unwrap();
            assert_eq!(
                (closure.zone(), closure.from(), closure.to()),
                (NEW_YORK, friday, to)
            );
        }

        let instant = at(1_800_000_000);
        assert_eq!(ClosedWindow::new(instant, instant), None);
        assert_eq!(ClosedWindow::new(instant, before(instant)), None);
        assert!(ClosedWindow::new(before(instant), instant).is_some());
    }

    #[test]
    fn a_calendar_that_closes_nothing_is_none() {
        let nothing = || CalendarParts {
            first_open: None,
            final_close: None,
            windows: vec![],
            weekly: vec![],
        };
        assert_eq!(SessionCalendar::new(nothing()), None);
        // An earliest open at the start of the clock closes no instant either.
        let from_the_start = CalendarParts {
            first_open: Some(WallNs(i64::MIN)),
            ..nothing()
        };
        assert_eq!(SessionCalendar::new(from_the_start), None);
        // Each part alone closes something.
        let instant = at(1_800_000_000);
        let window = ClosedWindow::new(instant, WallNs(instant.0 + 1)).unwrap();
        let friday = week_time(Weekday::Friday, hm(17, 0));
        let sunday = week_time(Weekday::Sunday, hm(17, 0));
        let closure = WeeklyClosure::new(UTC, friday, sunday).unwrap();
        let each = [
            CalendarParts {
                first_open: Some(WallNs(i64::MIN + 1)),
                ..nothing()
            },
            CalendarParts {
                final_close: Some(WallNs(i64::MAX)),
                ..nothing()
            },
            CalendarParts {
                windows: vec![window],
                ..nothing()
            },
            CalendarParts {
                weekly: vec![closure],
                ..nothing()
            },
        ];
        for parts in each {
            let calendar = SessionCalendar::new(parts.clone()).unwrap();
            assert_eq!(calendar.parts(), &parts);
        }
    }

    #[test]
    fn parts_in_any_order_make_the_same_calendar() {
        let instant = at(1_800_000_000);
        let early = ClosedWindow::new(instant, WallNs(instant.0 + NS)).unwrap();
        let late = ClosedWindow::new(
            WallNs(instant.0 + DAY * NS),
            WallNs(instant.0 + 2 * DAY * NS),
        );
        let late = late.unwrap();
        let fx = WeeklyClosure::new(
            NEW_YORK,
            week_time(Weekday::Friday, hm(17, 0)),
            week_time(Weekday::Sunday, hm(17, 0)),
        )
        .unwrap();
        let nightly = WeeklyClosure::new(
            UTC,
            week_time(Weekday::Wednesday, hm(23, 0)),
            week_time(Weekday::Thursday, hm(1, 0)),
        )
        .unwrap();
        let one = SessionCalendar::new(CalendarParts {
            first_open: Some(instant),
            final_close: None,
            windows: vec![late, early, late],
            weekly: vec![fx, nightly],
        })
        .unwrap();
        let other = SessionCalendar::new(CalendarParts {
            first_open: Some(instant),
            final_close: None,
            windows: vec![early, late],
            weekly: vec![nightly, fx, nightly],
        })
        .unwrap();
        assert_eq!(one, other);
        assert_eq!(one.parts().windows, [early, late]);
        assert_eq!(one.parts().weekly.len(), 2);
    }

    #[test]
    fn the_ends_of_the_clock_do_not_panic() {
        let instant = at(1_800_000_000);
        let rules = [NEW_YORK, BERLIN, SYDNEY, DUBLIN, UTC];
        let friday = week_time(Weekday::Friday, hm(17, 0));
        let sunday = week_time(Weekday::Sunday, hm(17, 0));
        let wrapping = week_time(Weekday::Monday, hm(1, 0));
        let mut weekly = Vec::new();
        for rule in rules {
            weekly.push(WeeklyClosure::new(rule, friday, sunday).unwrap());
            weekly.push(WeeklyClosure::new(rule, sunday, wrapping).unwrap());
        }
        let calendar = SessionCalendar::new(CalendarParts {
            first_open: Some(WallNs(i64::MIN + 1)),
            final_close: Some(WallNs(i64::MAX)),
            windows: vec![ClosedWindow::new(instant, WallNs(instant.0 + NS)).unwrap()],
            weekly,
        })
        .unwrap();
        for probe in [i64::MIN, i64::MIN + 1, -1, 0, i64::MAX - 1, i64::MAX] {
            let _ = calendar.is_open(WallNs(probe));
        }
        for rule in rules {
            for probe in [i64::MIN, i64::MAX] {
                let s = probe.div_euclid(NS);
                let offset = rule.offset_secs(s);
                assert!(offset.abs() < DAY, "{rule:?} {offset}");
            }
        }
    }

    #[test]
    fn civil_dates_round_trip_across_the_clock() {
        let first = i64::MIN.div_euclid(DAY * NS) - 1;
        let last = i64::MAX.div_euclid(DAY * NS) + 1;
        for day in first..=last {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), day, "{y}-{m}-{d}");
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        // 1970-01-01 was a Thursday.
        assert_eq!(weekday_of(0), Weekday::Thursday as i64);
        assert_eq!(weekday_of(-4), Weekday::Sunday as i64);
    }

    #[test]
    fn the_nth_and_the_last_weekday_of_a_month() {
        let second_sunday_of_march = Transition {
            month: Month::March,
            week: WeekOfMonth::Second,
            weekday: Weekday::Sunday,
            at: hm(2, 0),
        };
        assert_eq!(
            transition_day(2026, second_sunday_of_march),
            days_from_civil(2026, 3, 8)
        );
        let fourth_monday_of_june = Transition {
            month: Month::June,
            week: WeekOfMonth::Fourth,
            weekday: Weekday::Monday,
            at: hm(0, 0),
        };
        assert_eq!(
            transition_day(2026, fourth_monday_of_june),
            days_from_civil(2026, 6, 22)
        );
        let last_sunday_of_october = Transition {
            month: Month::October,
            week: WeekOfMonth::Last,
            weekday: Weekday::Sunday,
            at: hm(2, 0),
        };
        assert_eq!(
            transition_day(2026, last_sunday_of_october),
            days_from_civil(2026, 10, 25)
        );
        // December's last week reads the next year's first day.
        let last_thursday_of_december = Transition {
            month: Month::December,
            week: WeekOfMonth::Last,
            weekday: Weekday::Thursday,
            at: hm(0, 0),
        };
        assert_eq!(
            transition_day(2026, last_thursday_of_december),
            days_from_civil(2026, 12, 31)
        );
        // February's last week in a leap year.
        let last_thursday_of_february = Transition {
            month: Month::February,
            ..last_thursday_of_december
        };
        assert_eq!(
            transition_day(2028, last_thursday_of_february),
            days_from_civil(2028, 2, 24)
        );
        let third_tuesday = Transition {
            month: Month::January,
            week: WeekOfMonth::Third,
            weekday: Weekday::Tuesday,
            at: hm(0, 0),
        };
        assert_eq!(
            transition_day(2027, third_tuesday),
            days_from_civil(2027, 1, 19)
        );
    }
}
