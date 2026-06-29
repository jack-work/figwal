# Trunk primitives & invariants

The trunk-addressed API (`xwal/trunks.go`) is the surface a consumer
(figaro) uses. A **trunk** is a continuation chain across all channels of
the triune; a **node** (`nN` dir + `.fork`) is the per-fork plumbing. All
operations hold `Trunks.mu` and, after any structural change, call
`rebuild()` so the in-memory cache is re-read from disk (the sole source
of truth).

This document gives, for each primitive, its signature, what it does, and
the invariant it preserves. They all rest on a small set of cross-cutting
rules, stated first.

---

## The identity model

> A fork **freezes** the node it splits. The **continuation inherits the
> trunk id** (same trunk, head advances); the **alternative founds a new
> trunk.**

Implemented in `commitFork(headBranch, contDir, altDir)`: it writes the
frozen head's trunk id into the *continuation* node's `.trunk`, mints a
fresh id (`mintTrunk`) for the *alternative*, then rebuilds. Every
content-bearing fork (`Append` interior, `ForkAt`, `ForkTail` on a
non-empty head, `resplitBelow`) routes through `commitFork`.

**Invariant.** Forking is trunk-stable for the continuation: the caller's
trunk id keeps pointing at the live continuation of the same timeline,
and exactly one new trunk id is introduced per fork. The founding node of
a trunk is the shallowest node carrying it (its parent is in a different
trunk, or it is the root).

## Empty-head redirect

> An empty node has no logical time of its own — it **IS its parent's
> tail position.** So forking an empty head N-ary-forks the *parent*; an
> empty node never becomes a parent.

`isEmptyHead(x)` is true when `forkBase > 0` and the main log's
`LastIndex() < forkBase` (content wholly inherited). When `ForkTail` /
`ForkAt` hit an empty head they redirect: they open the *parent* branch
and `Fork(fb, altDir, "")` — an N-ary add-one at the head's fork point,
no old-future — then mint a new alternative trunk. The empty head itself
is left untouched.

**Invariant.** An empty node is never frozen and never gains children; a
fork "at" it becomes another sibling under its parent. An empty *root*
trunk cannot be forked at all.

## Always-materialize (per-trunk write isolation)

Every fork creates the child branch in **every** channel, even when a
channel has nothing of its own to split (the empty-own case in
`disk.forkImpl` / `xwal.buildForkPlan`). A new trunk therefore always has
its own writable branch dir in each channel.

**Invariant.** No two trunks ever share a writable log. Appends to one
trunk can never appear in another; shared history is read-only inherited
prefix, divergent history is each trunk's own segments.

---

## `Append`

```go
func (t *Trunks) Append(trunk string, atMainLT uint64, payload, meta []byte) (string, uint64, error)
```

Adds a main-timeline entry to a trunk. The `atMainLT` argument selects
one of three behaviors:

- **`atMainLT == 0` or `>= tail`** → plain append (no fork).
  `AppendMain` on the head; returns the **same** trunk and the new LT.
- **`ownFirst <= atMainLT < tail`** (interior of the head's own range) →
  **interior fork.** `Fork(atMainLT+1, altDir, contDir)`: a new trunk
  shares `[1..atMainLT]` and diverges at `atMainLT+1`; the existing trunk
  keeps its full history. The payload is written to the new alternative.
  Returns the **new** trunk.
- **`atMainLT < ownFirst`** (the LT lives in a frozen ancestor) →
  **re-split-below** via `resplitBelow`: fork the owning ancestor, mint a
  sibling trunk, append there; the caller's trunk continues unchanged.

`ownFirst` is `ownFirstIdx(x)` — the head's `forkBase`, or `1` at the
root.

**Invariant.** A below-tail `atMainLT` never mutates the caller's trunk:
shared prefix `[1..atMainLT]` is preserved byte-for-byte and the
divergence begins at `atMainLT+1`. An at-or-past-tail append extends the
trunk in place.

## `ForkTail`

```go
func (t *Trunks) ForkTail(trunk string) (string, error)
```

Bisects a trunk's present:

- **head has content** → freeze it; `Fork(tail+1, altDir, contDir)`
  creates both children at `tail+1`, each empty and inheriting the full
  prefix in every channel. The continuation keeps the trunk (new empty
  head); a new alternative trunk is founded (`commitFork`).
- **head is empty** → empty-head redirect (above): N-ary fork the parent,
  add one alternative sibling trunk; the trunk's empty head is untouched.
  An empty *root* trunk is refused.

**Invariant.** After `ForkTail` on a content head, the original trunk
points at a fresh empty continuation and exactly one new sibling trunk
exists; both share the entire prior history read-only.

## `ForkAt`

```go
func (t *Trunks) ForkAt(trunk string, atMainLT uint64) (string, error)
```

The imperative, no-append fork. Shares `[1..atMainLT]` and creates an
**empty** alternative trunk diverging at `atMainLT+1`; the original trunk
keeps its id and suffix. Branch selection mirrors `Append` minus the
write:

- `atMainLT == 0` or `>= tail` → degenerates to `ForkTail`.
- `atMainLT < ownFirst` → `resplitBelow(..., doAppend=false)`.
- otherwise → interior `Fork(atMainLT+1, altDir, contDir)` + `commitFork`.

**Invariant.** Same divergence guarantee as `Append`'s interior case, but
the new trunk's head starts empty (no payload written).

## `SpawnChild`

```go
func (t *Trunks) SpawnChild(parent TrunkID) (TrunkID, error)
```

Adds a child trunk under a **ceremonial** parent *without* a
continuation: `Fork(tail+1, childDir, "")` — an N-ary add-one at the
tail, no old-future. The parent node becomes (or stays) a frozen branch
point that only hosts children. This is loadout / conversation spawning:
a null root, or a loadout many conversations fork from. (Contrast
`ForkTail`, which gives the parent its own continuation.) Children attach
at `anchorOf` — the parent's live head if it has one, else its single
frozen ceremonial node. Returns the new child trunk id.

**Invariant.** The parent gains a child trunk but no continuation of its
own; the parent timeline is not extended. Repeated `SpawnChild` calls
yield N sibling children of the same parent.

## `OwnerTrunk`

```go
func (t *Trunks) OwnerTrunk(trunk string, atMainLT uint64) (string, error)
```

Returns the trunk id of the node that **owns** `atMainLT` along the
trunk's lineage — via `ownerOf`, the deepest ancestor whose own segments
contain it (deepest with `forkBase <= atMainLT`). Consumers layer policy
on this: an LT owned by a ceremonial trunk (null root / loadout) can be
redirected to `SpawnChild` instead of a re-split.

**Invariant.** Pure query — no disk mutation. The returned trunk is the
unique owner of `atMainLT` on the lineage (the one whose own range
contains it).

## `Remove`

```go
func (t *Trunks) Remove(trunk string, recursive bool) error
```

Deletes a trunk by removing its **founding node's entire subtree, in
every channel** (`os.RemoveAll` per channel dir; trunk-addressed, the
node is plumbing). Guards:

- **refuses the root trunk** (`isRoot`).
- **refuses a trunk with live branches** (descendant trunks branched off
  it in the founding node's subtree) unless `recursive` — in which case
  those branches are deleted too.

**Invariant.** A non-recursive `Remove` never silently destroys
descendant trunks; the root is never removable. After removal the cache
is rebuilt so no dangling trunk/node references survive.

---

## Re-split-below + joint child re-home

```go
func (t *Trunks) resplitBelow(branch []string, atMainLT uint64, payload, meta []byte, doAppend bool) (string, uint64, error)
// underlying engine:
func (l *disk.Log) ForkRehome(atIdx uint64, childName, oldFutureName string, rehome []string) (*disk.Log, error)
```

When the fork point `atMainLT` is *below* the head's own range it lives in
a frozen ancestor (a turn shared with ancestors). `resplitBelow` finds
the owning ancestor (`ownerOf`), forks it at `atMainLT+1`, and mints a new
alternative trunk sharing `[1..atMainLT]`. The owner's suffix beyond
`atMainLT` **and all its child forks** (including the caller's own trunk)
re-home into the continuation, which keeps the owner's trunk id (normal
continuation-chain behavior).

The re-home decision is **made once from the main channel and applied to
every channel by name.** `buildForkPlan` reads
`x.chans[main].log.ChildForkBases()` and selects every child whose `.fork`
base is `> atMainLT` into `plan.Rehome`; `applyForkPlan` then calls
`ForkRehome(..., plan.Rehome)` on each channel. This is why the explicit
list exists instead of `disk.Fork`'s per-channel heuristic: a sparse
related channel would otherwise tail-fork and skip the re-home, drifting
out of lockstep with the main tree.

**Invariant.** All channels of the triune re-home the **same** node dirs,
so their node trees stay structurally identical after a re-split-below.
The caller's trunk and the original timeline are unchanged — they ride the
continuation.

---

## `lineage`

```go
func (t *Trunks) lineage(trunk string) (parent string, branchedLT uint64)
```

Walks up from a trunk's head to its founding node (the shallowest node
still carrying the trunk), then reports the **parent trunk id** (the trunk
of the node just above the founding node) and the **`branchedLT`** — the
founding node's main `forkBase`, the LT at which it diverged. The root
trunk reports `("", 0)`. Surfaced through `List` as
`TrunkInfo.Parent` / `TrunkInfo.BranchedLT`.

**Invariant.** `branchedLT` is exactly the inherited/own boundary
(`forkBase`) of the founding node: the trunk shares `[1..branchedLT]` with
its parent and owns everything after.

---

## `Config.MintTrunkID` — pluggable opaque trunk ids (v0.6.5)

```go
MintTrunkID func() string   // in xwal.Config
```

When set, `mintTrunk` calls it to generate trunk ids (retrying on
collision via `trunkExists`) instead of the default sequential `t<N>`.
The id is **not** persisted directly through this field — it lands in the
node's `.trunk` marker, the same as a default id. `List` adapts: with a
custom minter it enumerates `t.heads` sorted by string id; with the
default it walks `0..trunkSeq` reconstructing `t<N>`.

The figwal CLI (`cmd/figwal`) keeps the default `t<N>` ids (e.g. `t0`),
while a consumer can supply opaque ids (figaro's logical-turn ids) by
setting `MintTrunkID`.

**Invariant.** Trunk ids are opaque to figwal: it only requires
non-empty and unique. The id model is interchangeable without touching
the on-disk node tree (which is addressed by `nN` dirs + `.fork`, never
by trunk id).

## `Config.Genesis` — custom genesis payload

```go
Genesis []byte   // in xwal.Config
```

`CreateTrunks` seeds the root trunk's first main entry. If `cfg.Genesis`
is empty it uses the default `genesisMarker` (`{"genesis":true}`);
otherwise it writes the caller's bytes verbatim via `AppendMain`. Every
reducible channel is also seeded with `{}` at the genesis LT. Used only
at creation; ignored on open.

**Invariant.** The root trunk always has a non-empty prefix (one genesis
entry), so it is immediately forkable and no trunk ever has an empty
shared prefix — the precondition the fork engine requires (a fork must
leave a non-empty prefix in the root).
