use fbc_core::SessionCalendar;

fn main() {
    // A session calendar has no Default: a venue with no timetable says `None` (0125).
    let _ = SessionCalendar::default();
}
