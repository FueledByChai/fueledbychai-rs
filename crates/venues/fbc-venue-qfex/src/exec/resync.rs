//! The probe's resync (FBC-jxau, decision 0132 item 5), under either probe mode (decisions 0143
//! and 0145): one `ResyncBegin` ... `ResyncEnd` bracket, its watermark the resync call's wall
//! time, built from three reads and pushed only once every read has answered.
//!
//! - **Positions.** The resync call itself sends the one signed REST read, `GET /user/positions`
//!   on the REST origin the probe's guard admitted (`account_reads::positions_request`), its
//!   headers from the codec's `src/auth` handle (`rest_headers`) with the resync's two nonces:
//!   the public key, the signature and, for a subaccount, `x-qfex-requested-account-id` each
//!   `redact: true`.
//!   It is charged `OpKind::Rest`, Safety traffic, since a resync may use the reserve (decision
//!   0073), and awaited for `qfex.exec.resync_timeout`. A failed read, a status outside 2xx and an
//!   answer `account_reads::decode_positions` refuses each ask to reconnect, as does a position
//!   off the size step on a held symbol; a row on a symbol the spec table does not hold is skipped
//!   and counted (decision 0147 item 2). Each position that is not flat on a held symbol is one
//!   `ResyncPosition`, with no entry price.
//! - **Open orders.** The resync call also sends the first `get_user_orders` page, at
//!   `qfex.exec.page_size` from offset 0 ([`PAGE_WEIGHT`] under `Query`), and later pages one at a
//!   time from the frames that answer them, each at the next offset, until a page lists fewer
//!   orders and TWAPs together than the page size, an empty one included. Every full page is
//!   counted, with its page size and offset kept for the probe's evidence ([`ResyncEvidence`]); a
//!   full page that names no order or TWAP id an earlier page of the resync named asks to
//!   reconnect. Orders are deduplicated by `order_id`, a later row replacing an earlier one,
//!   counted. TWAPs, and rows whose `type` is one of the
//!   [enums page](https://docs.qfex.com/api-reference/enums)'s two stop kinds, `TAKE_PROFIT`
//!   ("Take-profit stop order.") and `STOP_LOSS` ("Stop-loss stop order."), are left out of the
//!   snapshot, though their ids count toward fullness and new ids. A page that names a
//!   `stop_orders` member is refused, as the probe's own read refuses it
//!   (`account_reads::page_rows`). A row on a symbol the table does not hold is skipped and
//!   counted, whatever it holds but its `order_id` (decision 0147 item 2). Each other row is read
//!   as an `order_response` is, but for its status (`private::snapshot`), and one not as
//!   documented, off the grid or off the step refuses the page.
//! - **Missed fills.** Once the orders' pages end, and only when the codec has a start time,
//!   `get_user_trades` pages run the same way from offset 0: `start_ts`, the earlier of the latest
//!   fill time this codec decoded and the send time of the oldest order it placed and has not
//!   seen end, rounded down to whole seconds; with neither, nothing is read. The
//!   [get_user_trades page](https://docs.qfex.com/websocket/channels/trade/get_user_trades) and
//!   the AsyncAPI give the request's members, `start_ts` an inclusive Unix time in seconds,
//!   `limit` and `offset`, every one optional; neither states a largest `limit` (1000 is the
//!   page's default, and REST's `GET /user/trade` documents 1000 as its largest), so the page size
//!   is `qfex.exec.page_size`, at most 1000. Each page is charged the
//!   [rate page](https://docs.qfex.com/websocket/rate)'s 0.5 under `Query` ([`TRADES_WEIGHT`]).
//!   A row naming an order this codec placed, by our client id or by the venue id an order
//!   update or a page bound to it, on that order's instrument, is a replayed fill
//!   (`FillEvent::replay`) keyed by its `trade_id`, with our side from the order placed, since
//!   the page's row names only the aggressor's: untimed, so fbc-oms's ledger refuses it
//!   (`Admission::Untimed`) until captures show what QFEX's timestamps are. Any other row is not
//!   pushed: it is counted and kept as evidence. A row on a symbol the table does not hold is
//!   read for its `trade_id` and `order_id` alone (decision 0147 item 2); one on a held symbol not
//!   as documented, off the grid or off the step refuses the page. Rows are deduplicated by
//!   `trade_id`.
//! - **One read of a kind.** The resync sends a page only while no other read of it is in
//!   flight, and while its orders' pages run the codec refuses a `get_order` query, whose answer
//!   is also an `all_orders_response`, `NotSent(Backpressure)` with nothing written. An
//!   `all_orders_response` or a trade page that arrives while no page of its kind is asked for
//!   is not read as one.
//! - **Its own deadline.** A timer of `qfex.exec.resync_timeout` asks to reconnect when it fires
//!   before `ResyncEnd`: a page the session's buckets dropped unwritten, an err answering a page
//!   (which settles nothing, decision 0134 item 5) and an answer that never comes all end there.
//! - **What is pushed.** `ResyncBegin`, the open orders, the positions, the replayed fills, then
//!   `ResyncEnd`, all in the call that read the last answer, with no venue time. The resync ends
//!   no order. The probe's caps declare `SnapshotSource::Untrustworthy` and
//!   `SnapshotReads::Separate`, so no resync seeds a position (decision 0055).
//!
//! While a page is in flight, the arm's echo window is closed (decision 0132 item 3: the arm is
//! then not the connection's only request in flight), so an echo of another request then is kept
//! as decision 0134 keeps every echo; the arm's own echo is kept as its own all the same.

use std::collections::HashSet;
use std::num::NonZeroU32;

use fbc_core::{
    CidMatch, ClientOrderId, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, ExecEvent,
    ExecSink, FillEvent, FillId, FillIdent, FillOrigin, HttpFailure, HttpResponse, HttpTag,
    InstrumentId, Liquidity3, OpKind, RateCharge, Side, SignedLots, SpecTable, TimerTag,
    TrafficClass, VenueCommand, VenueMeta, VenueOrderId, VenueOrderSnapshot, VenueOrderState,
    WallNs, WireSlice,
};

use super::json::{self, Member, Node};
use super::{Phase, QfexExec, Sent, TRADE, private};
use crate::account_reads::{self, AnswerError, PAGE_WEIGHT, PageAsk, PositionRow};

/// The resync's deadline's tag is this, plus the number of the opening it belongs to.
pub(super) const RESYNC_TIMER: u64 = 2 << 32;

/// The positions read's tag is this, plus the number of the opening it belongs to.
pub(super) const POSITIONS_READ: u64 = 3 << 32;

/// What one `get_user_trades` page costs, in tenths of QFEX's weight unit: the rate page's 0.5.
pub const TRADES_WEIGHT: NonZeroU32 = NonZeroU32::new(5).expect("five is not zero");

/// The order types the enums page calls stop orders.
const STOP_KINDS: [&str; 2] = ["TAKE_PROFIT", "STOP_LOSS"];

/// An order this codec placed and has not seen end, across its epochs: what a missed fill is
/// matched to, and the start time of the trades read.
pub(super) struct Placed {
    cid: ClientOrderId,
    inst: InstrumentId,
    side: Side,
    sent: WallNs,
    /// The venue's id for it, once an order update or a page named it with our client id.
    vid: Option<VenueOrderId>,
}

/// The read of the resync in flight.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Read {
    Orders(PageAsk),
    /// From `start_ts`.
    Trades(u64, PageAsk),
}

impl Read {
    /// The frame that asks for it, Safety traffic as the resync's reads are.
    fn frame(self) -> Effect {
        let (body, weight) = match self {
            Read::Orders(ask) => (account_reads::user_orders_body(ask), PAGE_WEIGHT),
            Read::Trades(start, ask) => (
                format!(
                    r#"{{"type":"get_user_trades","params":{{"start_ts":{start},"limit":{},"offset":{}}}}}"#,
                    ask.limit(),
                    ask.offset()
                ),
                TRADES_WEIGHT,
            ),
        };
        Effect::Send {
            stream: TRADE,
            frame: WireSlice::plain(body.into_bytes()),
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge {
                op: OpKind::Query,
                inst: None,
                weight,
            },
        }
    }
}

/// What a resync showed the probe, finished or not (decision 0132 item 5).
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct ResyncEvidence {
    /// Each full `get_user_orders` page: its page size and offset.
    pub full_pages: Vec<PageAsk>,
    /// Every row `GET /user/positions` answered, held or not, once it answered.
    pub positions: Vec<PositionRow>,
    /// The trades read's start time, in Unix seconds; `None` when nothing was read.
    pub start_ts: Option<u64>,
    /// The trades a page listed for an order this codec did not place, or does not know it
    /// placed: none was pushed.
    pub unplaced_trades: Vec<FillId>,
    /// Whether it ended in `ResyncEnd`; otherwise its epoch ended first.
    pub finished: bool,
}

/// A resync in flight on the current epoch.
pub(super) struct Resync {
    watermark: WallNs,
    /// The held positions that are not flat, once the positions read answered.
    positions: Option<Vec<(InstrumentId, SignedLots)>>,
    /// The page in flight; `None` once the last page answered.
    reading: Option<Read>,
    start_ts: Option<u64>,
    /// Every order id (`false`) and TWAP id (`true`) a page of it listed.
    seen: HashSet<(bool, String)>,
    orders: Vec<VenueOrderSnapshot>,
    trades: HashSet<FillId>,
    fills: Vec<FillEvent>,
    evidence: ResyncEvidence,
}

/// Why a page ends the epoch: not as documented, a decode error; or paging that cannot end.
enum Stop {
    Malformed,
    Paging(&'static str),
}

impl From<&'static str> for Stop {
    fn from(_: &'static str) -> Stop {
        Stop::Malformed
    }
}

/// The next page after `ask`, or the paging stopped when its offset would pass `u32`.
fn next(ask: PageAsk) -> Result<PageAsk, Stop> {
    ask.offset()
        .checked_add(u32::from(ask.limit()))
        .and_then(|offset| PageAsk::new(ask.limit(), offset).ok())
        .ok_or(Stop::Paging("a QFEX resync page offset past its range"))
}

/// Binds the venue id of the order `placed` holds under our client id `cid` on `inst`, when it
/// names one, and forgets it when `ended`; an order named by a venue id already bound to it is
/// the same order.
fn track(
    placed: &mut Vec<Placed>,
    cid: Option<CidMatch>,
    vid: Option<&VenueOrderId>,
    inst: InstrumentId,
    ended: bool,
) {
    placed.retain_mut(|order| {
        let ours = order.inst == inst
            && (cid == Some(CidMatch::Ours(order.cid))
                || order.vid.is_some() && order.vid.as_ref() == vid);
        if ours {
            order.vid = vid.cloned();
        }
        !(ours && ended)
    });
}

impl QfexExec {
    /// Whether a page of the resync is in flight.
    pub(super) fn resync_reading(&self) -> bool {
        self.resync.as_ref().is_some_and(|r| r.reading.is_some())
    }

    /// Whether the resync's `get_user_orders` pages are still running.
    pub(super) fn orders_paging(&self) -> bool {
        self.resync
            .as_ref()
            .is_some_and(|r| matches!(r.reading, Some(Read::Orders(_))))
    }

    /// Ends the resync in flight, if one is, unfinished: its evidence is kept.
    pub(super) fn drop_resync(&mut self) {
        if let Some(resync) = self.resync.take() {
            self.counts.resyncs_unfinished += 1;
            self.last_resync = Some(resync.evidence);
        }
    }

    /// Records a place the codec sent: an order it placed until it sees it end.
    pub(super) fn record_place(&mut self, cmd: &VenueCommand, ctx: &EncodeCtx) {
        if let VenueCommand::Place(order) = cmd {
            self.placed.push(Placed {
                cid: order.cid,
                inst: order.inst,
                side: order.side,
                sent: ctx.wall,
                vid: None,
            });
        }
    }

    /// Reads an order update off the stream for the orders the codec placed: binds the venue
    /// id it names under our client id, and forgets an order it shows ended.
    pub(super) fn track_update(&mut self, update: &fbc_core::OrderUpdate) {
        let ended = !matches!(
            update.state,
            VenueOrderState::Open | VenueOrderState::Amended { .. }
        );
        track(
            &mut self.placed,
            update.cid,
            update.vid.as_ref(),
            update.inst,
            ended,
        );
    }

    /// The resync call (decision 0132 item 5): the positions read, the first `get_user_orders`
    /// page and the resync's own deadline, in that order. Refused, asking to reconnect, for the
    /// recording-only codec, off an authenticated epoch, while another resync or a query is in
    /// flight, and when the read cannot be signed.
    pub(super) fn start_resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        let Some(probe) = self.probe else {
            return self.end(fx, "QFEX resync is not implemented: recording only");
        };
        let query = self
            .in_flight
            .iter()
            .any(|(_, sent)| matches!(sent, Sent::Query(_)));
        if self.phase != Phase::Authenticated || self.resync.is_some() || query {
            return self.end(fx, "QFEX resync asked out of turn");
        }
        let (Ok(headers), Ok(ask)) = (
            self.auth.rest_headers(ctx),
            PageAsk::new(probe.page_size(), 0),
        ) else {
            return self.end(fx, "QFEX resync's positions read could not be signed");
        };
        let opens = u64::from(self.opens);
        let read = account_reads::positions_request(&self.rest, headers);
        fx.push(Effect::Http {
            tag: HttpTag(POSITIONS_READ | opens),
            req: read.req,
            rpc: None,
            timeout: probe.resync_timeout(),
            class: TrafficClass::Safety,
            charge: read.charge,
        });
        fx.push(Read::Orders(ask).frame());
        fx.push(Effect::Timer {
            tag: TimerTag(RESYNC_TIMER | opens),
            after: probe.resync_timeout(),
        });
        let sent = self
            .placed
            .iter()
            .map(|order| order.sent.0.div_euclid(1_000_000_000))
            .min()
            .and_then(|secs| u64::try_from(secs).ok());
        let start_ts = [self.latest_fill_secs, sent].into_iter().flatten().min();
        self.resync = Some(Resync {
            watermark: ctx.wall,
            positions: None,
            reading: Some(Read::Orders(ask)),
            start_ts,
            seen: HashSet::new(),
            orders: Vec::new(),
            trades: HashSet::new(),
            fills: Vec::new(),
            evidence: ResyncEvidence {
                start_ts,
                ..ResyncEvidence::default()
            },
        });
    }

    /// The resync's deadline, when `tag` is the current epoch's: past it with the resync still
    /// in flight, the epoch ends. Whether `tag` was it.
    pub(super) fn on_resync_timer(&mut self, tag: TimerTag, fx: &mut Effects) -> bool {
        if tag != TimerTag(RESYNC_TIMER | u64::from(self.opens)) {
            return false;
        }
        if self.resync.is_some() {
            self.end(fx, "QFEX resync not ended within qfex.exec.resync_timeout");
        }
        true
    }

    /// A page the resync's read in flight asked for, `top` a frame's top-level members naming
    /// one private envelope: `None` when no page of its kind is in flight.
    pub(super) fn on_resync_page(
        &mut self,
        top: &[Member<'_>],
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Option<Result<(), DecodeError>> {
        let reading = self.resync.as_ref()?.reading?;
        let mut found = top
            .iter()
            .filter(|(name, _)| private::ENVELOPES.contains(&name.as_ref()));
        let (Some((name, body)), None) = (found.next(), found.next()) else {
            return None;
        };
        let resync = self.resync.as_mut()?;
        let (counts, placed) = (&mut self.counts, &mut self.placed);
        let step = match (reading, name.as_ref()) {
            (Read::Orders(ask), "all_orders_response") => {
                resync.orders_page(top, ask, scope, specs, counts, placed)
            }
            (Read::Trades(start, ask), "user_trades" | "user_trades_response") => {
                resync.trades_page((name, body, top), start, ask, scope, specs, counts, placed)
            }
            _ => return None,
        };
        self.counts.private_state += 1;
        Some(match step {
            Ok(next) => {
                resync.reading = next;
                match next {
                    Some(read) => fx.push(read.frame()),
                    None => self.finish(sink),
                }
                Ok(())
            }
            Err(Stop::Paging(why)) => {
                self.end(fx, why);
                Ok(())
            }
            Err(Stop::Malformed) => {
                self.end(fx, "an invalid QFEX resync page");
                Err(DecodeError::Malformed("invalid QFEX resync page"))
            }
        })
    }

    /// The answer to the resync's positions read; any other HTTP answer is refused, as one the
    /// codec never asked for.
    pub(super) fn on_positions(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let asked = tag == HttpTag(POSITIONS_READ | u64::from(self.opens));
        if !asked || !self.resync.as_ref().is_some_and(|r| r.positions.is_none()) {
            return Err(DecodeError::Malformed(
                "unexpected QFEX private HTTP response",
            ));
        }
        let answer = resp
            .map_err(|_| None)
            .and_then(|resp| account_reads::decode_positions(&resp, scope).map_err(Some));
        let rows = match answer {
            Ok(positions) => positions.rows().to_vec(),
            Err(why) => {
                self.end(fx, "QFEX resync's positions read failed");
                return match why {
                    None | Some(AnswerError::Status(_)) => Ok(()),
                    Some(_) => Err(DecodeError::Malformed("invalid QFEX positions answer")),
                };
            }
        };
        let mut held = Vec::new();
        for row in &rows {
            let Some(spec) = specs.by_symbol(row.symbol.as_wire()) else {
                self.counts.unheld_rows += 1;
                continue;
            };
            let Some(qty) = private::steps(spec, row.position) else {
                self.end(fx, "QFEX resync's positions read failed");
                return Err(DecodeError::Malformed("invalid QFEX positions answer"));
            };
            if qty != 0 {
                held.push((spec.id, SignedLots(qty)));
            }
        }
        if let Some(resync) = self.resync.as_mut() {
            resync.positions = Some(held);
            resync.evidence.positions = rows;
        }
        self.finish(sink);
        Ok(())
    }

    /// Pushes the resync's bracket once every read has answered: `ResyncBegin`, the open orders,
    /// the positions, the replayed fills and `ResyncEnd`.
    fn finish(&mut self, sink: &mut dyn ExecSink) {
        let Some(resync) = self
            .resync
            .take_if(|r| r.positions.is_some() && r.reading.is_none())
        else {
            return;
        };
        let push = |sink: &mut dyn ExecSink, event| sink.push(VenueMeta::NONE, event);
        push(
            sink,
            ExecEvent::ResyncBegin {
                watermark: resync.watermark,
            },
        );
        for order in resync.orders {
            push(sink, ExecEvent::ResyncOrder(order));
        }
        for (inst, qty) in resync.positions.into_iter().flatten() {
            push(
                sink,
                ExecEvent::ResyncPosition {
                    inst,
                    qty,
                    avg_entry: None,
                },
            );
        }
        for fill in resync.fills {
            push(sink, ExecEvent::Fill(fill));
        }
        push(sink, ExecEvent::ResyncEnd);
        self.counts.resyncs += 1;
        self.last_resync = Some(ResyncEvidence {
            finished: true,
            ..resync.evidence
        });
    }
}

impl Resync {
    /// One `get_user_orders` page answering `ask`: the next read, `None` once the reads are
    /// done.
    fn orders_page(
        &mut self,
        top: &[Member<'_>],
        ask: PageAsk,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        counts: &mut super::QfexExecCounts,
        placed: &mut Vec<Placed>,
    ) -> Result<Option<Read>, Stop> {
        let (rows, twaps) = account_reads::page_rows(top, ask.limit())?;
        let mut new = false;
        for twap in twaps {
            let id = twap
                .members()
                .and_then(|twap| json::get(twap, "twap_id"))
                .and_then(Node::str);
            if let Some(id) = id {
                new |= self.seen.insert((true, id.to_owned()));
            }
        }
        for row in rows {
            let row = row.members().ok_or("an order row")?;
            let id = json::get(row, "order_id")
                .and_then(Node::str)
                .ok_or("order_id")?;
            new |= self.seen.insert((false, id.to_owned()));
            let kind = json::get(row, "type").and_then(Node::str);
            if kind.is_some_and(|kind| STOP_KINDS.contains(&kind)) {
                continue;
            }
            let Some(order) = private::snapshot(row, scope, specs)? else {
                counts.unheld_rows += 1;
                continue;
            };
            track(placed, order.cid, Some(&order.vid), order.inst, false);
            match self.orders.iter_mut().find(|kept| kept.vid == order.vid) {
                Some(kept) => {
                    counts.duplicate_rows += 1;
                    *kept = order;
                }
                None => self.orders.push(order),
            }
        }
        if rows.len() + twaps.len() < usize::from(ask.limit()) {
            let Some(start) = self.start_ts else {
                return Ok(None);
            };
            let first = PageAsk::new(ask.limit(), 0).map_err(|_| Stop::Malformed)?;
            return Ok(Some(Read::Trades(start, first)));
        }
        counts.full_pages += 1;
        self.evidence.full_pages.push(ask);
        if !new {
            return Err(Stop::Paging(
                "a full QFEX get_user_orders page added no new id",
            ));
        }
        Ok(Some(Read::Orders(next(ask)?)))
    }

    /// One `get_user_trades` page answering `ask`, `page` its envelope's name and body among
    /// the frame's top-level members: the next read, `None` once the reads are done.
    #[expect(
        clippy::too_many_arguments,
        reason = "the page, its read and the codec's state"
    )]
    fn trades_page(
        &mut self,
        page: (&str, &Node<'_>, &[Member<'_>]),
        start: u64,
        ask: PageAsk,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        counts: &mut super::QfexExecCounts,
        placed: &mut [Placed],
    ) -> Result<Option<Read>, Stop> {
        let (name, body, top) = page;
        let rows = private::trade_rows(name, body, top)?;
        for row in rows {
            let trade = private::trade(row, scope, specs)?;
            if !self.trades.insert(trade.fill.clone()) {
                counts.duplicate_rows += 1;
                continue;
            }
            let ours = trade.at.as_ref().and_then(|at| {
                placed.iter().find(|order| {
                    order.inst == at.inst
                        && (trade.cid == Some(CidMatch::Ours(order.cid))
                            || order.vid.as_ref() == Some(&trade.vid))
                })
            });
            let (Some(order), Some(at)) = (ours, trade.at) else {
                counts.unplaced_trades += 1;
                self.evidence.unplaced_trades.push(trade.fill);
                continue;
            };
            self.fills.push(FillEvent {
                ident: FillIdent::Venue {
                    fill: trade.fill,
                    vid: Some(trade.vid),
                    cum_after: None,
                },
                origin: FillOrigin::Order {
                    cid: trade.cid,
                    px: at.px,
                },
                inst: at.inst,
                side: order.side,
                qty: at.qty,
                liquidity: Liquidity3::Unknown,
                fee: at.fee,
                realized_pnl: None,
                realized_funding: None,
                replay: true,
            });
        }
        if rows.len() < usize::from(ask.limit()) {
            return Ok(None);
        }
        Ok(Some(Read::Trades(start, next(ask)?)))
    }
}
