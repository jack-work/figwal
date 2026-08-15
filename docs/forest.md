# forest: the one window shape

See the package comment for the design. This note carries the
CONSOLIDATION SURVEY from the port (2026-08-14), because a library's
dead code cannot be judged from its own mains -- deadcode flagged
SetPayloadCacheBudget, which is figaro's config plumb.

Candidates needing CONSUMER-side verification before any cut
(grep figaro, not figwal):
  disk.Log: ForkRehome, ChildForkBases, TruncateFront, Dir, Hash,
  HashPayload, HeaderAt, RangeOwn; log.SetPayloadCacheBudget and the
  log/-level delegation shims generally (5-36 one-line forwards).
crashtest/bind_trunks.go carries genuinely unreachable helpers
(its own package, no consumer): safe to prune with the next touch.

## Consumer-side verdict on the fork accessors (figaro, 2026-08-15)

The survey above asked for consumer-side proof before cutting. figaro's
forest uptake is that consumer, and the answer for the two fork entries is
SPLIT -- cut neither:

  - `ChildForkBases` has a consumer SHAPE, just not figaro's. It reads every
    child subdir's marker and returns map[child]base: the DOWNWARD direction,
    a parent enumerating children, which is what the joint fork needs. Keep.
  - The thing a consumer actually wants is the UPWARD walk -- for one node,
    walk to root collecting (node, base), to build forest's `[]Ref`. That is
    `disk.Log.forkBase` (log.go, "first index this log owns"), which is
    PRIVATE with no accessor. It is the real gap, and it does not need
    closing: figaro serves the upward walk from its own trunk topology
    (`BranchedLT` plus the parent link is exactly (node, base) per hop), so
    no export is required and the `.fork` marker stays a durable cross-check
    rather than an API.

The conventions were verified to agree on a live store, not on doc comments
-- marker `base=3`, first record present `_idx=3`, figaro `branched_lt=3`.
Base is the FIRST coordinate the child owns; `Cache.split` cuts the ancestor
at `Base-1`. No adapter arithmetic. figaro pins this in
`internal/store/forkbase_convention_test.go` so a drift fails there.

## Installing Evicted: it takes no lock

`Evicted` fires outside every forest lock so a lower layer can clear its
fast pointer. The inversion runs the other way and is the hazard: a consumer
that calls `Put` while holding its own write lock can have eviction pick one
of its own runs, so `Evicted` runs with that lock held. A hook that needs
the lock deadlocks -- and only under budget pressure with concurrent
readers, which is the shape that reaches production first.

The payload cache is safe by accident of design: `Segment` clears an
atomic.Pointer and takes nothing. Consumers should hold that line
deliberately -- **an Evicted hook does a pointer swap and nothing else**. If
it needs more, publish a successor value instead of mutating the held one.

The LARGER consolidation lives in figaro by design: cachedLog (decoded
IR) and TurnCache (composed UI) re-seat on forest.Cache, which deletes
two more bespoke accountants and gives both layers prefix-shared
residency. That is the ui-ir-tree work; forest ships first so they
have something to seat on.
