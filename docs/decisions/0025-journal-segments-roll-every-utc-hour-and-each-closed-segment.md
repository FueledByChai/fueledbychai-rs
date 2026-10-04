# 0025 — Journal segments roll every UTC hour and each closed segment is compressed with zstd 0.13.3 pinned, the bundled libzstd at level 3

Status: accepted
Date: 2026-10-04

## Context

0006 says journal segments are compressed and grouped by day, and design §9 rolls them hourly
with zstd. FBC-aen built the day-grouped writer with one uncompressed segment per day and
writer start; FBC-g67 adds the roll and the compression. A journal records every inbound
frame, so a day of market data is large and repetitive, and the consumer copies and keeps it
(retention and copying are the consumer's, 0006). The writer runs on the sink's writer
thread (0021), never on the shard's. The licence gate (0017) admits BSD-3-Clause, the
licence of libzstd's C sources.

## Decision

The writer files each record under the UTC hour of the caller's time, clamped so it never
goes back: a record of a later hour closes the open segment and starts the next one in that
hour's day directory, so segments roll every hour and at every UTC day boundary, and an hour
with no record has no segment. Each segment the writer closes is compressed, whole, into one
zstd frame with a content checksum (so damage fails decompression rather than reading back
as other records), named `<shard>-<seq>.fbcj.zst`: written under a temporary name, synced,
renamed into place, the directory synced so the rename is durable, then the uncompressed
file removed, so a `.zst` segment is always complete and a segment found in both forms is
read once, compressed. The open segment, and one a stopped writer left open, stay
uncompressed; the reader reads both forms in order. A roll that cannot compress fails that
append without writing the record, and leaves the closed segment uncompressed and readable,
and the writer stays in that segment's hour. The crate is `zstd` `=0.13.3` (MIT), with
zstd-safe 7.3.0 and zstd-sys 2.1.0+zstd.1.5.7 in the lockfile (both BSD-3-Clause; zstd-sys
builds the bundled libzstd, BSD-3-Clause, through `cc`, adding jobserver and pkg-config, MIT
OR Apache-2.0), default features off, pinned exactly in the workspace manifest, at zstd's
default level 3.

## Alternatives

- `ruzstd` (pure Rust, MIT): its decoder is complete, but its encoder offers only the
  fastest levels and is far less proven than libzstd, and a journal that cannot be read back
  is the failure that matters. The C build is one more `cc` build in a tree that already
  builds ring's C and assembly (0020).
- Compressing each record or each block inside the segment (a seekable format): finer
  random access, but worse ratios on small records and a format of our own to keep; replay
  reads segments start to end.
- gzip or lz4: the design names zstd; gzip compresses slower for the same ratio, lz4 trades
  ratio for speed the writer thread does not need.
- A consumer-configured level: no consumer needs another yet; the level changes only the
  size, never what reads back, so it can become configuration later without a format change.
- Compressing the segments a stopped writer left open when a writer starts: that file could
  belong to a writer still running in another process, which the library cannot rule out;
  left to a ticket of its own.

## Consequences

- A closed segment costs a fraction of its raw size on disk and in the consumer's copies;
  the open hour is uncompressed and readable while it is written.
- Each roll reads the closed segment back and writes its compressed form on the writer
  thread, once an hour per shard; the queue (0021) absorbs records meanwhile.
- The reader streams a compressed segment, so its checksum is checked at the segment's end: a
  mismatch is reported after the segment's records have been returned. Damage inside a zstd
  block usually fails decompression earlier, at the damaged block.
- A segment's form is told by its name, not its bytes; the format version (0024's version
  2) is unchanged, since the bytes inside the zstd frame are a segment as before.
- Moving the zstd crates is a ticket that says why.

## What would show this was wrong

- A roll's compression stalling the writer thread long enough for the sink to drop records
  at its soft limit: then compression moves off the writer thread or to a faster level.
- A compressed segment that reads back different from the records written.
- Consumers needing random access inside an hour's segment, which a single zstd frame
  cannot give without decompressing from its start.
