// A codec gets PathStamps, a write-only handle: it cannot reach the runtime's recorder through
// it, so it cannot read whatever the recorder kept (decision 0033).
use fbc_core::PathStamps;

fn codec_side(t: &mut PathStamps<'_>) {
    let _recorder = &mut t.recorder;
}

fn main() {
    codec_side(&mut PathStamps::off());
}
