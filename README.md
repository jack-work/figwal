# figwal

A small append-only write-ahead log with a forking primitive. The default on-disk format is one canonical JSON object per line, so a log file can be inspected with `jq`, edited in a text editor, or fed to an LLM without a separate parser.

Designed for agent harnesses and similar systems that want to record conversation steps, tool calls, or other intermediate representations as a durable history that can also branch.

## Status

Pre-1.0. The API and on-disk format may change.

## Install

```sh
go get figwal
go install figwal/cmd/...   # logctl, segctl
```

Go 1.25+.

## Quick start

```go
import (
    "figwal/log"
    "figwal/segment"
)

l, err := log.Open("/var/wal/session-42", log.Options{
    Codec: segment.JSONLCodec{},
})
if err != nil { /* ... */ }
defer l.Close()

l.Write(1, []byte(`{"role":"user","text":"hello"}`))
l.Write(2, []byte(`{"role":"assistant","text":"hi"}`))

// Speculative branch from index 3.
fork, _ := l.Fork(3, "responseB")
defer fork.Close()
fork.Write(3, []byte(`{"role":"assistant","text":"alternate"}`))

// Reads on the fork fall through to the parent for indices < 3.
payload, _ := fork.Read(1) // returns {"role":"user","text":"hello"}
```

## On-disk format

The default JSONL codec emits one line per entry:

```
{"_hash":"<16 hex>","_idx":<N>,...payload fields...}
```

`_idx` is the global log index. `_hash` is a truncated SHA-256 of the canonical JSON form of the payload (sidecars removed, keys sorted). Both keys are reserved; payloads may not contain `_idx` or `_hash` at the top level. Object keys are sorted, so the sidecars always lead the line.

To recover the payload:

```sh
jq 'del(._idx, ._hash)' segment.jsonl
```

A binary codec is also available (`segment.BinaryCodec{}`): length prefix + CRC32 + opaque payload. Faster, not human-readable.

## Forks

`Log.Fork(atIdx, childName)` splits this log at `atIdx`:

- The parent log keeps the prefix `[FirstIndex, atIdx-1]` and becomes read-only.
- Entries `[atIdx, LastIndex]` move into a subdir whose name matches the parent dir's basename. This is the "old future".
- A fresh subdir named `childName` is created, empty and writable from `atIdx`.

An optional third argument overrides the old-future subdir name: `Log.Fork(atIdx, childName, "kept")` parks the moved suffix under `kept/` instead of the default. Pass `""` (or omit) to keep the default.

Mid-segment forks split the boundary segment byte-level; later sealed segments are renamed across subdirs without re-encoding. Global indices are preserved.

Layout after `Fork(6, "branchB")` on a log dir named `session-42/`:

```
session-42/
    00000000000000000001.jsonl   prefix only, indices 1..5
    session-42/                  old future, indices 6..end
        00000000000000000006.jsonl
        ...
        .fork
    branchB/                     fresh fork, appendable from 6
        .fork
```

Any subdirectory in the dir tree marks its parent as a branch point: read-only, still forkable. Forks of forks are fine; deleting a fork cascades to its children.

The `.fork` file in a child dir is one line: `base=<N>`. The parent is always `..`.

## In-memory cache

`log.Log` keeps immutable entry snapshots for lock-free reads:

```go
c, _ := log.Open("/var/wal/session-42", log.Options{Codec: segment.JSONLCodec{}})
defer c.Close()

c.Write(1, []byte(`{"role":"user"}`))   // fsync + cache update under writer mutex
payload, _ := c.Read(1)                  // lock-free
c.Range(1, func(idx uint64, p []byte) error { /* ... */ return nil })
```

Reads are lock-free over an immutable snapshot held in an `atomic.Pointer`. Writes serialize on a writer mutex, fsync to disk, then publish a new snapshot pointer. Many goroutines can read in parallel with zero contention, including while a writer is committing. `Log.Snapshot()` returns a point-in-time view that survives later writes.

`Log.Fork` publishes a truncated immutable prefix shared by sibling forks.

## Recovery

Each segment is scanned forward on open. CRC32 (binary) or `_hash` (JSONL) is verified per row during the scan; a torn tail at end-of-file is truncated. After open, reads do not re-verify, so per-read cost stays near the file I/O floor.

If a fork crashes mid-operation, a `.fork-pending` sentinel file is left in the trunk directory. `Open` refuses to proceed while this file exists; the operator resolves manually.

Other durability points:

- Every `Write` calls `fsync` on the active segment when `SyncMode == SyncAlways` (the default). `SyncManual` defers fsync to `Log.Sync()` for callers driving their own commit cadence.
- Segment creation and fork operations `fsync` the directory so new dentries survive a crash.

## CLI

`logctl` operates on log directories:

```sh
logctl mydir write 1 '{"hello":"world"}'
logctl mydir read 1
logctl mydir range
logctl mydir fork 6 branchB
logctl mydir delete           # lists subtree, prompts y/N
```

`segctl` operates on a single segment file:

```sh
segctl path/to/00001.jsonl append '{"k":1}'
segctl path/to/00001.jsonl readi 0
segctl path/to/00001.jsonl size
```

`--codec` is inferred from on-disk file extensions (`.seg` or `.jsonl`) when not specified.

## Performance

Read benchmarks on cached data, small (~25 byte) and medium (~150 byte) payloads:

```
BenchmarkReadBinarySmall    543 ns/op   32 B/op   1 alloc/op
BenchmarkReadBinaryMedium   615 ns/op  160 B/op   1 alloc/op
BenchmarkReadJSONLSmall     567 ns/op   64 B/op   1 alloc/op
BenchmarkReadJSONLMedium    626 ns/op  192 B/op   1 alloc/op
```

The JSONL read path uses a slice-and-splice extraction over the canonical line format. No JSON parse, no canonical re-marshal, no hash check (hash is verified on the recovery scan instead). For hand-edited files that no longer match the canonical layout, a JSON-parser fallback kicks in.

Writes always fsync (default mode); throughput on a write-heavy workload is bounded by disk latency, not codec choice.

## Layout

- `segment/` per-file framing (Binary, JSONL), recovery scan, torn-tail handling.
- `log/` multi-segment log with rotation, range iteration, prefix truncation, fork primitive.
- `cmd/logctl/` `cmd/segctl/` CLIs.
