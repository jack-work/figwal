# memory-first figwal

The store is an in-memory append-only WAL; disk is a follower with bounded
lag. One writer goroutine per store persists dirty lineages on a timer.
Appends block on disk only when a channel's unflushed lag exceeds
MaxUnflushedBytes; the over-cap append performs an inline bounded flush.

## Surface (the whole thing a client sees)

```go
s, err := xwal.OpenStore(root, xwal.StoreOptions{
    FlushInterval:     time.Second,            // loss window; 0 = default 1s
    MaxUnflushedBytes: 64 << 20,               // lag bound per channel; 0 = default 64MB
    IdleUnload:        5 * time.Minute,        // evict idle heads; 0 = default 5m, <0 = never
    Reducers:          map[string]Reducer{…},  // reducible channels only
})

lt, err := s.Append(trunk, channel, mainLT, payload, meta)
    // in-memory, returns immediately; tail-append always (never forks);
    // mainLT is the fk for related channels, ignored for the main channel;
    // unknown channel => auto-created as a plain log channel

s.Read / s.ReadAt / s.Lookup / s.StateAt / s.Channels / s.List / s.ListLight
s.Fork(trunk, atMainLT) (newTrunk, err)    // topology is always explicit
s.Promote / s.Remove / s.CreateStump       // retained as-is
s.Kick()                                    // schedule an immediate async flush
s.Close()                                   // drain: final flush, release lock
```

No sync modes. No EnsureChannel. No journal types. No per-channel policy.

## Contract

- Append durability: within FlushInterval of the append (sooner after Kick).
- Flush lag is bounded in bytes as well as time: a channel never holds more
  than MaxUnflushedBytes of unflushed entries; the append that would exceed
  the bound flushes inline (brief, bounded block) instead of growing it.
- Cooperative shutdown (Close) loses nothing.
- Hard crash loses at most the flush lag; what survives is a
  lineage-coherent prefix: a related-channel record never survives without
  the main-channel record it references. Reducible channels get exactly
  one turn of slack: a patch keyed one ahead of the main tail (the
  upcoming-turn convention) is durable within the flush bound and
  survives a crash; anything further ahead is trimmed at open.
- Open repairs before serving: crash artifacts are always repaired —
  interrupted fork/channel plans are consumed (rolled back to the pre-fork
  layout, then re-applied, trunk markers included), torn active-segment
  tails are truncated per channel, and each lineage is trimmed to its
  coherent cut. Repair is idempotent and logged. BYTE CORRUPTION of sealed
  segments (bit flips, torn interiors, tampering outside the active tail)
  is out of open-repair scope and deferred to an offline fsck.
- Single writer: OpenStore takes an exclusive flock on root for its
  lifetime; a second writer fails immediately with a clear error.

## Internals (guidance, not contract)

- The flusher captures per-lineage watermark vectors under the append lock
  and persists each lineage's channels as one cut.
- Fork/topology mutations wait (bounded) for a pending flush; never
  ErrTopologyBusy to callers.
- Idle lineages are flushed then unloaded on the flusher's timer; reload is
  transparent on next touch.
- Evaluate deleting the manifest: auto-created channel dirs are their own
  registry; reducer binding comes from StoreOptions. Delete xwal.json if
  nothing else needs it.
