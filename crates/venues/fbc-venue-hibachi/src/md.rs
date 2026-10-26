//! The market-data codec the factory must name before Hibachi's feeds are decoded. FBC-2kmp
//! writes the `/ws/market` codec; until then `plan_md` plans no endpoint, so the runtime builds
//! no codec, and this one, which a caller can still ask for, refuses every subscription and
//! decodes nothing.

use fbc_core::{
    DecodeError, DecodeScope, Effects, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    Keepalive, MdCodec, MdSink, MonoNs, RawFrame, SpecTable, Subscription, TimerTag, VenueError,
    WallNs,
};

/// A codec that decodes no feed.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct NoFeeds;

impl MdCodec for NoFeeds {
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// Every subscription refused as unsupported, nothing pushed; an empty call is accepted.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        _specs: &SpecTable,
        _fx: &mut Effects,
    ) -> Result<(), VenueError> {
        match add.iter().chain(remove).next() {
            Some(sub) => Err(VenueError::UnsupportedFeed(*sub)),
            None => Ok(()),
        }
    }

    fn on_frame(
        &mut self,
        _f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("no Hibachi feed is decoded yet"))
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "no Hibachi market-data request is made",
        ))
    }

    fn on_timer(
        &mut self,
        _tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) {
    }

    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    /// Public market data holds no credential.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}
