// A codec cannot tell whether its marks are recorded: live and replay hand it stamps that look
// the same, so it cannot encode differently in replay (decision 0033, Codex r4180330309).
use fbc_core::PathStamps;

fn codec_side(t: &mut PathStamps<'_>) -> bool {
    t.is_on()
}

fn main() {
    codec_side(&mut PathStamps::off());
}
