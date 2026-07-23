# memory-first figwal

The store is an in-memory append-only WAL; disk is a follower with bounded
lag. One writer goroutine per store persists dirty lineages on a timer.
Appends never block on disk.

## Surface (the whole thing a client sees)

```go
s, err := xwal.OpenStore(root, xwal.StoreOptions{
    FlushInterval: time.Second,            // loss window; 0 = default 1s
    Reducers:      map[string]Reducer{…},  // reducible channels only
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
- Cooperative shutdown (Close) loses nothing.
- Hard crash loses at most the flush lag; what survives is a
  lineage-coherent prefix: a related-channel record never survives without
  the main-channel record it references, and a reducible watermark never
  runs ahead of its main channel.
- Open repairs before serving: torn frames truncated per channel, then each
  lineage trimmed to its coherent cut. Repair is idempotent and logged.
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
