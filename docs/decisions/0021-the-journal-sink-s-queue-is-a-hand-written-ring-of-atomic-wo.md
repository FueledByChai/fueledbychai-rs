# 0021 — The journal sink's queue is a hand-written ring of atomic words with no unsafe code and no dependency

Status: accepted
Date: 2026-10-04

## Context

0006 requires that safety traffic never blocks on the journal: the shard hands each record to
a sink that never waits, and a writer thread of its own puts records on disk. Design §5.1
names `rtrb` for the queue between them, a single-producer single-consumer ring; FBC-870 lets
the queue be hand-written or a pinned dependency, recorded here under FBC-cu0's licence gate
(0017). The queue's budget is in bytes (design §5.4 suggests 64 MB with an 85% soft limit,
numbers that are the consumer's configuration, 0009), and records vary in size, so the queue
has to count bytes, not items. The workspace has no `unsafe` code so far.

## Decision

`fbc-journal`'s `src/sink.rs` holds the queue, hand-written in safe Rust: a ring of
`AtomicU64` words. The producer (`QueueSink`) encodes a record on the caller's thread, checks
it fits its class's limit, stores the entry's words with relaxed stores and publishes its
write position; the writer thread reads up to that position and publishes its read position.
Each position has one writer, so neither end waits for the other; the writer parks only when
the ring is empty and the next record wakes it. An entry is the body's length, the wall time
to file it under, and the body in whole words, so the budget is counted in bytes (rounded to
words) as records really take it. No dependency is added, so the licence gate is unchanged.

## Alternatives

- `rtrb` pinned exactly (design §5.1): a sound, permissively licensed SPSC ring. Not taken
  because a byte budget over variable-size records needs its byte ring and chunk API, which
  brings `unsafe` (in the dependency) and a pin to keep up for about 150 lines that safe atomics
  already express.
- A ring of raw bytes behind `UnsafeCell`: slightly faster copies, but the workspace's first
  `unsafe` code for a journal that is not on the latency budget's critical path.
- `std::sync::mpsc::sync_channel` with a byte counter beside it: allocates a buffer per record
  and bounds items, not bytes.
- A `Mutex<VecDeque>`: the shard could wait on the writer's lock, which 0006 forbids.

## Consequences

- The shard never blocks on the journal and the queue holds no lock. A record costs one encode
  into a reused buffer and one relaxed store per 8 bytes; nothing allocates once the buffers
  have grown.
- The writer reassembles each body from words, a copy `rtrb`'s byte ring would avoid.
- The ring's correctness rests on its own tests (`crates/fbc-journal/tests/sink.rs`, including
  one where both ends run at once while the ring wraps thousands of times), not on a crate's.

## What would show this was wrong

- A profile of the shard showing the sink's copy into words is a material part of a cycle,
  or the writer thread falling behind on copies while the disk keeps up: then a byte ring
  (`rtrb`, pinned, under the licence gate) replaces this one in a new record.
- A lost, duplicated or reordered record from the ring, which its tests would have to have
  missed.
