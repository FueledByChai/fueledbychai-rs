// Marking a stage gives nothing back: neither through PathStamps nor through the recorder trait
// does a mark return the instant it was recorded at (decision 0033).
use fbc_core::{MonoNs, PathEdge, PathMark, PathRecorder, PathStage, PathStamps};

fn through_stamps(t: &mut PathStamps<'_>) -> MonoNs {
    t.start(PathStage::Sign)
}

fn through_recorder(r: &mut dyn PathRecorder) -> MonoNs {
    let mark = PathMark {
        stage: PathStage::Sign,
        edge: PathEdge::End,
    };
    r.mark(mark)
}

fn main() {
    let _ = (through_stamps, through_recorder);
}
