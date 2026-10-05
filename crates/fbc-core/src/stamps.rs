//! Per-stage latency stamps on an order's way out, with no clock in the codec (decision 0033,
//! design §4.7, §5.3, §11).
//!
//! The latency budget splits an order's path into stages: the encode, each signer call inside
//! it, and the socket write. The runtime times them, but a codec may read no clock (0002), so a
//! codec marks a stage boundary without learning when it was: [`ExecCodec::encode`] and
//! [`OrderGateway::submit`] take a [`PathStamps`], a write-only handle on a [`PathRecorder`] the
//! runtime implements. At each [`mark`](PathRecorder::mark) the runtime reads its own clock and
//! keeps the instant with the command's record; the call returns nothing, and nothing in this
//! crate reads a clock, so the codec never sees a value and its bytes cannot depend on one.
//! Replay hands the same codec a recorder of its own, or [`PathStamps::off`], and gets the same
//! bytes whatever was recorded.
//!
//! [`ExecCodec::encode`]: crate::ExecCodec::encode
//! [`OrderGateway::submit`]: crate::OrderGateway::submit

use core::fmt;

/// A stage of an order's path that encode and submit span (design §5.3).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum PathStage {
    /// The whole encode of one command, signing included: marked by the gateway around its call
    /// to [`ExecCodec::encode`](crate::ExecCodec::encode). Less its `Sign` stages it is design
    /// §5.3's `pre_send`.
    Encode,
    /// One signer call: marked by the codec around each, so a batch marks one per signed item.
    Sign,
    /// The write of a request's bytes to its socket (`send`): marked by the runtime.
    Write,
}

/// Which end of a stage a mark is.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum PathEdge {
    Start,
    End,
}

/// One boundary of one stage.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PathMark {
    pub stage: PathStage,
    pub edge: PathEdge,
}

/// Records path marks as they happen. The runtime implements it: it reads its own clock at each
/// mark and keeps the instant with the command's record, which it journals (0006). A mark
/// returns nothing, so whoever calls it learns no time.
pub trait PathRecorder {
    /// `mark` happened now.
    fn mark(&mut self, mark: PathMark);
}

/// The handle a gateway and a codec mark an order's stages through: it passes each mark to the
/// runtime's [`PathRecorder`], or drops it when off. It holds no time and gives none back.
pub struct PathStamps<'a> {
    recorder: Option<&'a mut dyn PathRecorder>,
}

impl<'a> PathStamps<'a> {
    /// Marks go to `recorder`.
    pub fn new(recorder: &'a mut dyn PathRecorder) -> PathStamps<'a> {
        PathStamps {
            recorder: Some(recorder),
        }
    }

    /// Marks go nowhere: for a caller that times nothing.
    pub fn off() -> PathStamps<'static> {
        PathStamps { recorder: None }
    }

    /// Whether marks go to a recorder.
    pub fn is_on(&self) -> bool {
        self.recorder.is_some()
    }

    /// Marks the start of `stage`.
    pub fn start(&mut self, stage: PathStage) {
        self.mark(PathMark {
            stage,
            edge: PathEdge::Start,
        });
    }

    /// Marks the end of `stage`.
    pub fn end(&mut self, stage: PathStage) {
        self.mark(PathMark {
            stage,
            edge: PathEdge::End,
        });
    }

    /// Runs `f` as `stage`: marks its start, runs it, marks its end whatever `f` returned, and
    /// gives back what `f` returned.
    pub fn span<T>(&mut self, stage: PathStage, f: impl FnOnce() -> T) -> T {
        self.start(stage);
        let out = f();
        self.end(stage);
        out
    }

    fn mark(&mut self, mark: PathMark) {
        if let Some(recorder) = self.recorder.as_deref_mut() {
            recorder.mark(mark);
        }
    }
}

impl fmt::Debug for PathStamps<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PathStamps")
            .field("on", &self.is_on())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recorder keeping the marks it is handed, in order.
    #[derive(Default)]
    struct Marks(Vec<PathMark>);

    impl PathRecorder for Marks {
        fn mark(&mut self, mark: PathMark) {
            self.0.push(mark);
        }
    }

    const fn at(stage: PathStage, edge: PathEdge) -> PathMark {
        PathMark { stage, edge }
    }

    #[test]
    fn marks_reach_the_recorder_in_the_order_they_are_made() {
        let mut marks = Marks::default();
        let mut t = PathStamps::new(&mut marks);
        assert!(t.is_on());
        t.start(PathStage::Encode);
        let signed = t.span(PathStage::Sign, || 42);
        let failed: Result<u8, &str> = t.span(PathStage::Sign, || Err("refused"));
        t.end(PathStage::Encode);
        t.span(PathStage::Write, || ());
        assert_eq!((signed, failed), (42, Err("refused")));
        assert_eq!(format!("{t:?}"), "PathStamps { on: true }");
        use {PathEdge::*, PathStage::*};
        assert_eq!(
            marks.0,
            [
                at(Encode, Start),
                at(Sign, Start),
                at(Sign, End),
                at(Sign, Start),
                at(Sign, End),
                at(Encode, End),
                at(Write, Start),
                at(Write, End),
            ]
        );
    }

    #[test]
    fn marks_made_while_off_go_nowhere() {
        let mut t = PathStamps::off();
        assert!(!t.is_on());
        t.start(PathStage::Encode);
        assert_eq!(t.span(PathStage::Sign, || 7), 7);
        t.end(PathStage::Encode);
        assert_eq!(format!("{t:?}"), "PathStamps { on: false }");
    }
}
