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

The follow-on turn-durability work is workload
`212c263a-64b4-47af-9570-95702e164055` on branch
`workload/212c263a-turn-durability`.

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

### Workload 212c263a: turn durability

The follow-on workload adds:

- runtime-only `ChannelSpec.SyncMode`, resolved by channel name on every
  private, shared/hot, recovery, and newly-added channel open;
- persisted `ChannelSpec.Opaque`, which writes payload bytes as base64 in the
  XWAL envelope so JSONL canonicalization cannot reorder nested provider JSON;
- `(*Trunks).EnsureChannel(ChannelSpec) error`, which idempotently adds or
  backfills a channel, updates runtime policy, retires hot generations, and
  keeps later topology mirrored;
- ordered `(*Trunks).SyncChannel(trunk, channel string) error`;
- immutable-snapshot
  `(*Trunks).LatestChannelRecord(trunk, channel string, minMainLT uint64)
  (Record, bool, error)`;
- empty-root channel forks at channel LT 1, allowing payload-free dynamic
  channels to mirror later stumps, trunks, and forks without a format change.

`xwal.json` remains backward-compatible: sync mode is never persisted, and
opaque encoding is an optional channel flag. Legacy raw `p` frames and new
base64 `p64` frames both decode. Existing channels are never silently changed;
Figaro should create fresh opaque translator namespaces and bump provider
fingerprints. The new turn journal should also be created with `Opaque: true`.

Exact-byte tests cover append, cached and cold reopen, `RecordsFrom`,
`LatestChannelRecord`, tail fork, interior fork, and sibling shared prefixes
with deliberately non-canonical nested JSON key order.

Review follow-ups fixed three upgrade/recovery cases:

- idempotent reducible backfill preserves each existing `.fork` base and
  reconstructs a missing watermark from parent state at `base-1`;
- disk and cached `FirstIndex` fall back to a child's own entries when its
  inherited parent is empty;
- fork preparation fails with `ErrTopologyIncomplete` before mutation when a
  manifest channel lacks its logical branch path;
- only `EnsureChannel` repairs legacy paths, and it rejects a fallback history
  whose latest related record reaches or crosses the missing main fork base;
- channel add/backfill is guarded by a synced `.xwal-channel-pending` plan
  that `Open`/`OpenTrunks` roll forward before serving;
- topology mutation is serialized across all registered `Trunks` for a root
  and rejects peer borrowed heads with `ErrTopologyBusy`;
- same-root peers share per-lineage writer coordination, hand off stale hot
  generations before writing, and refresh derived `nodes`/`heads` from a
  root-scoped topology epoch after another peer mutates the tree;
- marker and watermark replacement validates content, uses synced temporary
  files and directory sync, and protects Windows two-rename fallback with a
  recoverable replacement sentinel and backup;
- fork preflight caches validated channel topology and watermark state by
  topology version/generation, retains a shallow missing-artifact probe, and
  runs deep parent-state repair only after invalidation or detected damage.

Regression tests reopen existing and newly-added reducible channels after
forks, repair an intentionally removed fork watermark, refuse incomplete
fork topology, and repair only safe legacy opaque paths. Fault cases cover
pending plans before manifest publication, partial trees, manifest-first
legacy recovery with a pending plan, empty/partial/wrong watermarks,
missing/malformed markers, replacement temp/backup phases, and peer borrowed
heads. A repair-budget regression test and cached-preflight benchmark guard
against returning to a deep topology walk on every fork.

## Current validation

For workload `212c263a-64b4-47af-9570-95702e164055`, the final Windows gate
was split only to make the long deterministic fuzz independently visible:

```sh
go test ./xwal -skip '^TestForest_FuzzSequential$' -count=1
go test ./xwal -run '^TestForest_FuzzSequential$' -count=1
go test ./disk ./log ./log/typed ./segment ./cmd/... -count=1
go vet ./...
git diff --check
```

All pass. The non-fuzz XWAL suite completed in 30.35 seconds. The deterministic
500-operation fuzz completed in 511.35 seconds with 189 trunks, below the
600-second release gate. Root-peer append, stale-topology, detached-head, and
append-versus-fork regressions passed ten repeated runs. Cached fork preflight
measured 3.86 ms/op over a five-iteration Windows benchmark.

No race result is claimed: native Windows Go has no usable C compiler. Nix/WSL
validation belongs to the consuming Figaro stack and remains pending at this
commit boundary.

## Known limitations

1. `RecordsFrom` finds its boundary by reverse-walking the immutable snapshot.
   It is efficient for the normal near-tail untranslated suffix, but it is not
   yet an O(1) main-LT index.
2. XWAL does not yet expose one polished public lineage snapshot containing
   all channel tails, main-LT watermarks, reducible state, and suffix views.
3. Reducible state and duplicate-key patch lookup at a main LT still need a
   native API suitable for Figaro's chalkboard.
4. Dynamic channel addition/fork lifecycle still needs more crash-injection
   testing beyond the deterministic and concurrent coverage in workload
   `212c263a-64b4-47af-9570-95702e164055`.
5. Shared generation retirement is subtle. Fork, truncate, clear, close, and
   concurrent borrowed-head lifetimes need race and fault tests.
6. No release/tag exists for this branch.
7. Figaro adaptation exists on the stacked
   `workload/212c263a-turn-durability` worktree but is not yet pinned,
   committed, or released.

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

- Do not merge this branch into `master` without review.
- Do not tag or publish a release before the consuming Figaro stack passes.
- Do not update a released Figaro build to reference this commit until the
  stacked workload validation is green.
- Do not delete Figaro's current `cachedLog`.
- Do not claim native O(1) arbitrary suffix lookup until it is benchmarked and
  indexed.
- Do not rewrite or squash the five existing commits; they preserve useful
  experimental history.
