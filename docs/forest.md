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

The LARGER consolidation lives in figaro by design: cachedLog (decoded
IR) and TurnCache (composed UI) re-seat on forest.Cache, which deletes
two more bespoke accountants and gives both layers prefix-shared
residency. That is the ui-ir-tree work; forest ships first so they
have something to seat on.
