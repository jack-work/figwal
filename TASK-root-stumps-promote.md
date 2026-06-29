# figwal task: channel root-and-backfill + root/stumps + promote

**Status:** designed + approved. Implement all three in ONE figwal pass, wire the
`figwal` CLI to test each, then hand back to the user with isolated-store tests.
Do NOT yield until it's testable or a real snag is hit.

**Repos**
- figwal library: `/home/gluck/dev/figwal/` (branch `master`, pushed to origin). Layers:
  `segment/` (segments + `ValueHash`) → `disk/` (`log.go`, `fork.go` — the fork engine,
  `.fork` markers, forkBase/own-range vs inherited prefix) → `xwal/xwal.go` (multi-channel
  "triune": Main + reducible/log channels forked as a unit; `Config`; `AddChannel`;
  `fullChannelDir`/`channelDir`) → `xwal/trunks.go` (trunk-addressed view; DISK IS TRUTH;
  node tree = main channel's dir tree of `nN` dirs + `.fork`; the only non-derivable datum
  is a node's trunk id, stored in a `.trunk` file **in the main (`ir`) channel tree only**).
  Docs already exist: `docs/architecture.md`, `docs/primitives.md`.
- figaro consumer: `/home/gluck/dev/figaro-qua/main/` (branch `main`, pushed). Store policy
  lives in `internal/store/xwal_store.go` + `internal/store/xwal_backend.go`. The aria id IS
  the figwal trunk id (stable across forks; the continuation keeps it). `policy.json` holds the
  null/root id + the loadout dedup map (`name@contentversion -> trunk id`).
- **figwal CLI**: there is a `figwal` command (built earlier in the figwal forking work). It
  MUST be extended to drive + inspect the new ops (create root/stumps, spawn trunks, fork,
  promote, add+backfill a channel, dump the tree) so the user can test without figaro. Find it
  in the figwal repo (look for `cmd/` or a `figwal` main).

**⚠️ Caution — corruption history.** Two-daemon races + a `backupLegacyAriaDir` mis-detection
already destroyed a user store this session. Getting **per-channel forkBases** wrong is exactly
how you corrupt an xwal. Test every primitive in an isolated dir before declaring done. Never
test against the user's live store (`~/.local/state/figaro/arias`).

## Current on-disk structure (figaro store)
```
arias/
  xwal.json                 channel manifest (ir, chalkboard, translations/anthropic)
  policy.json               { "null": <root-trunk-id>, "loadouts": { "name@hash": <id> } }
  ir/        .trunk=<null>            root (genesis entry)
    n0/      .trunk=<loadout>         loadout (birth message w/ chalkboard stamp)
      n1/    .trunk=<conversation>    a top-level aria (conversation)
        n2/  .trunk=<branch>          a branch
  chalkboard/  (mirror node tree; reducible jsonmerge)
  translations/anthropic/  (LOG channel — THE BUG below)
  _live/<id>.json           single open message per trunk (discarded on restart)
  _meta/<id>.json           derived stats
  .daemon.lock              angelus flock (lives in <state>, not arias)
```

## The bug that motivates task 1
`translations/anthropic` is a real xwal channel but is **added lazily** by
`XwalBackend.OpenTranslation` (`xwal_backend.go`) the first time ANY conversation is prompted.
`xwal.AddChannel` opens it at `fullChannelDir` = `x.root + name + x.branch` — i.e. the CURRENT
branch (`translations/anthropic/n0/n1`), NOT the root. So the loadout/ancestor nodes never get a
channel node, forks can't propagate it, and only the first-prompted conversation ever gets a
translation node. Reproduced fresh: 3 prompted conversations → `ir` has all of them, but
`translations/anthropic` has exactly one node.

Tried the cheap fix (declare `translations/anthropic` up-front in `storeConfig.Channels`): it
**broke** — the **slash** in the channel name confuses `fullChannelDir`/fork (produced a stray
nested `translations/anthropic/anthropic`, flat `n2/n3/n4` nodes not mirroring the IR nesting,
and a dropped write). Reverted. So the slash + fork/backfill mechanics must be handled in figwal.
The translation channel is a **derivable cache** (re-translate from IR) — perf, not data loss —
so correctness > speed; never ship a half-correct store change.

## LOCKED DESIGN — root & stumps (the cauterization formalization)
- **root** = the channel directory itself (`ir/`, `chalkboard/`, `translations/anthropic/`),
  addressable purely by the xwal's on-disk location. **No marker file.** Holds the genesis entry.
- **stumps** = the root's immediate children, identified by their **directory name**
  (`<loadoutname>@<hashprefix>`, e.g. `config@d880aef2` — legible). **No `.stump`/`.root`/`.trunk`
  marker** — the name + depth-1 position IS the identity. Holds whatever state (e.g. the loadout
  birth message) + the top-level arias as children. Stumps are the cauterization boundary.
- **trunks** = everything below stumps. `nN` dirs with `.trunk` markers (main channel only).
  The `.trunk` closure obeys the invariants below.
- A trunk can be **promoted** but never become a root/stump. Roots/stumps are system-established
  (figaro mints them). figwal need not ship "make-root" yet (but `CreateStump(name)` is needed so
  figaro can name them).
- figaro mapping: root = today's "null" aria; stumps = the `config@hash` loadouts. Replaces the
  figaro-side "cauterized" concept (don't say "cauterized" in figwal).

### Trunk invariants (must hold after every op, esp. promote)
For any trunk id T: its dirs form ONE connected chain — every non-leaf dir with id T has exactly
one child with id T — terminating in a single leaf. The chain's top either reaches a stump (its
parent dir has a different id / is a stump) or is the trunk's divergence point. A trunk is either
rooted at a stump or diverged from a parent trunk (rendered as that parent's immediate child).

## TASK 1 — channel root-and-backfill
Make a lazily-added channel correct: born/rooted at the channel root and **backfilled** so its
node tree mirrors the main (`ir`) channel's tree, with **correct per-channel forkBases** (do NOT
hand-copy the main's `.fork` values — the index spaces differ; use figwal's own fork primitives /
recompute). Backfilled nodes are **empty** (dirs + `.fork`, no segments — content is derivable);
a reducible channel would seed each node with the reducer `Initial`, a log channel writes nothing.
- Fix the **slashed channel name** (`translations/anthropic`) handling so root + per-branch dirs
  nest correctly (no stray `anthropic/anthropic`).
- After this, `OpenTranslation` on any aria appends to the right node, read-through inheritance
  works, and existing stores heal on next open (backfill the missing structure).
- **Test (figwal CLI + isolated dir):** create a tree with a stump + 2 trunks + 1 branch, write to
  the main channel, THEN add a new channel and backfill → assert the new channel's node tree
  exactly mirrors the main's (same dirs, sane forkBases), and a write to each trunk lands in its
  own node with correct prefix read-through.

## TASK 2 — root & stumps
- Root sheds its `.trunk` (it's the dir/location). `CreateTrunks` (or rename) seeds the genesis at
  the root with no trunk id.
- `CreateStump(name)` (new): create a named depth-1 child of the root, no `.trunk`, holding its
  birth content. `SpawnChild(stump)` mints a normal trunk (`nN` + `.trunk`) — the first trunk under
  a stump = a top-level aria.
- Teach the stump boundary to `OwnerTrunk`, `lineage`, `Remove` (a stump/root can't be removed by a
  trunk-level remove; `lineage` stops at the stump; `OwnerTrunk` resolves inherited LTs to the
  owning trunk, never crossing into a stump as a "trunk").
- **Migration:** existing figaro stores have `ir/<.trunk=null>` + `ir/n0/<.trunk=loadout>`. Provide
  a migration (rename `n0` → the loadout's `name@hash`, drop the null/loadout `.trunk`s) OR make
  `OpenTrunks` tolerate + upgrade the old layout. Confirm the approach; don't silently break stores.
- **Test:** create root + 2 stumps, spawn trunks under each, dump the tree; assert stumps are
  named, markerless, and that trunk ops below them work; assert removing a trunk never touches a
  stump/root.

## TASK 3 — Promote
`Promote(trunk, levels)`: rewrite the main channel's `.trunk` markers to climb the trunk up
`levels` stump-bounded levels. Walk up from the promoted trunk's divergence point; the parent dir
carries a DIFFERENT trunk id (its run); rewrite that entire consecutive same-id run to the promoted
id; stop at the next different id. Repeat per level. **Stop at a stump** → return typed `ErrAtStump`.
Excess levels no-op. Worked example (leaf→root): `1234→1234→5678→5678→9012→3456(stump)`;
`promote 1234` → `1234→1234→1234→1234→9012→3456`; again → `…→1234→3456`; again → `ErrAtStump`.
Preserve all trunk invariants. No data moves — only `.trunk` relabeling (other channels follow the
node structure). 
- **figaro side:** `fig promote <trunk-id> [N]` (default 1); map `ErrAtStump` →
  "cannot promote into a loadout; make/edit a loadout instead." 
- **Test (figwal CLI):** build the worked-example tree, `figwal promote <id>`, dump → assert the
  `.trunk` runs match each expected state; assert `ErrAtStump` at the boundary; assert invariants.

## Also (figaro side, after figwal is green)
- `<trunk>:<LT>` addressing: when LT lives in an inherited ancestor, resolve to the owner trunk and
  **announce it** in the UI ("LT N is in trunk P — branching there"); consistent across
  send/fork/attend. (`store.ForkAt` already resolves owner via `OwnerTrunk`; just add the message.)
- After task 1, decide: keep lazy-add (now correct) vs declare known providers; heal the user's
  existing store via backfill-on-open.

## Sequencing
1, then 2, then 3 — each with a figwal-CLI test. The `figwal` CLI is the test harness; ensure it
can create roots/stumps/trunks, fork, add+backfill a channel, promote, and dump the tree.
Build + `go test ./...` green in figwal after each. Hand back with the runnable tests.
