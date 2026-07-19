# Native XWAL cache prototype handoff

## Status

This is an experimental, reviewable WIP branch. It is not ready to merge,
tag, release, or consume from Figaro.

- Repository: `github.com/jack-work/figwal`
- Branch: `perf/native-xwal-cache`
- Base: `master` at `22223ab`
- Local worktree used during development:
  `C:\Users\jokellih\dev\figaro-worktrees\figwal`
- Figaro continues to depend on Figwal `v0.7.7`.
- No on-disk format migration is intended by this prototype.

The corresponding Figaro performance work is on its separate
`perf/long-aria` branch. Its detailed handoff is:

`docs/long-aria-performance-handoff.md`

## Product intent

The user confirmed these requirements:

1. XWAL should natively own the hot cache for the IR, translation,
   chalkboard, and future UI channel trees.
2. Sibling forks should literally share one physical parent prefix on disk and
   one immutable parent snapshot pointer in memory.
3. Translation catch-up should be proportional to untranslated messages,
   normally one or two, not total aria length.
4. Appending to one lineage must not block unrelated lineages.
5. Forking must serialize with the target lineage's writes without requiring a
   running Figaro actor to stop.
6. Figaro's compensating `cachedLog`, open maps, locks, and linear lookup
   fallbacks should be removed only after XWAL exposes sufficient native
   guarantees.

## Existing committed work

The branch already contains:

1. `d7dbb3b` - accelerate long XWAL histories
2. `582cd42` - keep XWAL heads hot per lineage
3. `2e36f5f` - preserve snapshots across fork appends
4. `f6b044e` - remove obsolete XWAL compatibility code
5. `45dbdc1` - retain exported typed-log API

These commits provide:

- cached long-history operations and benchmarks;
- persistent hot XWAL heads;
- per-lineage serialization rather than one append lock for every trunk;
- snapshot-array ownership across fork/truncate append;
- removal of obsolete internal compatibility paths while retaining public
  typed APIs.

## Prototype committed by this handoff

The WIP commit after the five commits above preserves the following work.

### Shared cached log store

`log/store.go` adds a store that:

- wraps `disk.Store`;
- returns one shared cached `log.Log` per canonical physical directory;
- coalesces concurrent opens;
- constructs one immutable cache snapshot per physical log;
- points sibling snapshots at the same parent `cacheSnapshot`;
- owns the lifetime of shared disk/cache handles;
- rejects caller-provided `Options.Parent`, whose lifetime it cannot own.

`log/store_test.go` covers:

- shared physical and in-memory sibling prefixes;
- concurrent immutable snapshot reads during append;
- rejection of caller-owned parents.

### Shared log mutation safety

`log/log.go` adds:

- `ErrSharedMutation`;
- a shared-handle marker;
- immutable reverse snapshot scans;
- `ForkRehome` cache semantics;
- copy-on-truncate/fork behavior that protects pre-fork snapshots;
- wrappers needed by XWAL topology operations;
- no-op individual `Close` for store-owned handles.

Topology mutations through shared handles are rejected. A caller must retire
the shared generation and reopen a private handle before fork/truncate.

### XWAL cached channels

The prototype changes XWAL channels from direct `*disk.Log` handles to cached
`*log.Log` handles.

- `Open` remains valid for standalone/private XWALs.
- Internal shared opens accept a `*log.Store`.
- `Trunks` hot generations use `log.Store` instead of `disk.Store`.
- Normal channel reads use immutable snapshots.
- Fork recovery retains the existing disk-level recovery path.

### Delta channel reads

`XWAL.RecordsFrom(channel, fromMainLT, limit)` returns ordered channel records
whose main-timeline LT is at least the requested watermark.

It:

- reads one immutable channel snapshot;
- scans backward to locate the boundary;
- returns the requested ascending suffix;
- preserves payload and opaque metadata;
- works across inherited fork prefixes and mid-life dynamic channels;
- accepts zero as unlimited and rejects negative limits.

`xwal/records_test.go` covers:

- forked mid-life translation channels;
- inherited and divergent payloads;
- metadata preservation;
- limits and missing suffixes;
- reopen behavior;
- concurrent append and snapshot reads.

## Current validation

Before the WIP commit:

```sh
CGO_ENABLED=0 go test ./... -count=1
```

passed across:

- `disk`
- `log`
- `log/typed`
- `segment`
- `xwal`
- all Figwal commands

The WIP handoff should also run:

```sh
CGO_ENABLED=0 go vet ./...
```

No race result should be claimed unless a C compiler is available and the
race command actually succeeds.

## Known limitations

1. `RecordsFrom` finds its boundary by reverse-walking the immutable snapshot.
   It is efficient for the normal near-tail untranslated suffix, but it is not
   yet an O(1) main-LT index.
2. XWAL does not yet expose one polished public lineage snapshot containing
   all channel tails, main-LT watermarks, reducible state, and suffix views.
3. Reducible state and duplicate-key patch lookup at a main LT still need a
   native API suitable for Figaro's chalkboard.
4. Dynamic channel addition/fork lifecycle needs more crash-injection and
   generation-retirement testing.
5. Shared generation retirement is subtle. Fork, truncate, clear, close, and
   concurrent borrowed-head lifetimes need race and fault tests.
6. No release/tag exists for this branch.
7. Figaro has not been adapted to these new APIs and must remain on v0.7.7.

## Next-agent checklist

1. Start from `origin/perf/native-xwal-cache`.
2. Read `README.md`, `docs/architecture.md`, and this handoff.
3. Re-run full tests and vet before modifying code.
4. Add race/fault coverage for:
   - concurrent append versus fork;
   - store close during coalesced open;
   - shared generation retirement with outstanding borrowers;
   - crash during multi-channel fork and recovery;
   - dynamic translation channel add/fork/reopen.
5. Decide whether `RecordsFrom` should retain reverse-boundary search or gain a
   native main-LT-to-channel-offset index.
6. Design one immutable XWAL lineage snapshot API exposing:
   - main and channel tails;
   - channel fingerprint/meta at the tail;
   - suffix after main LT;
   - reducible state at main LT;
   - ordered patches, including duplicate main-LT keys.
7. Benchmark 1k, 10k, 50k, and 100k entries:
   - warm append;
   - cold and warm open;
   - suffix read with one/two untranslated entries;
   - sibling fork memory sharing;
   - unrelated-lineage concurrency;
   - fork latency with outstanding readers.
8. Keep disk bytes and fork layout compatible unless a migration is separately
   approved.
9. Only after the API is stable:
   - merge and tag a Figwal release;
   - update Figaro's dependency and Nix vendor hash;
   - replace Figaro `cachedLog`/backend hot caches with native XWAL snapshots;
   - run Figaro's full long-aria and live-fork suites.

## Do not do yet

- Do not merge this branch into `master`.
- Do not tag or publish a release.
- Do not update Figaro to reference this branch or commit.
- Do not delete Figaro's current `cachedLog`.
- Do not claim native O(1) arbitrary suffix lookup until it is benchmarked and
  indexed.
- Do not rewrite or squash the five existing commits; they preserve useful
  experimental history.
