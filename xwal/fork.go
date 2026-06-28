package xwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jack-work/figwal/disk"
)

// forkPendingName, in the xwal root, records a joint fork that began but
// did not finish. Open completes it (roll-forward) before serving any
// branch, so a crash mid-fork never leaves the triune half-diverged.
const forkPendingName = ".xwal-fork-pending"

// forkPlan is the durable description of a joint fork: where to split,
// what to name the branches, and the exact per-channel boundary indexes
// (recorded so recovery never has to recompute them from partial state).
// Channels are listed in apply order — the main channel last.
type forkPlan struct {
	AtMainLT  uint64          `json:"atMainLT"`
	Child     string          `json:"child"`
	OldFuture string          `json:"oldFuture"`
	Rehome    []string        `json:"rehome,omitempty"` // child node dirs to re-home into the old-future (joint, by name)
	Channels  []forkPlanEntry `json:"channels"`
}

type forkPlanEntry struct {
	Name  string `json:"name"`
	Dir   string `json:"dir"` // channel log dir, relative to the xwal root
	AtIdx uint64 `json:"atIdx"`
}

// Fork branches every channel as a unit at the main timeline position
// atMainLT. childName is the new branch; oldFutureName is the original
// continuation (figwal makes the fork point read-only and re-homes the
// suffix under that name). Both names are used identically across every
// channel, so each resulting branch is addressable as a unit. Returns
// the new branch opened for writing.
//
// The main channel forks at atMainLT; each related channel forks at its
// own boundary — the first channel LT whose referenced main LT is >=
// atMainLT (or its tail if it has not yet caught up). Reducible channels
// get a fresh watermark at the boundary so the new branch folds from the
// fork-point state. A channel with no entries is left unforked.
//
// The fork is crash-atomic across channels: a plan sentinel is written
// before any channel diverges and removed only once all have, and Open
// rolls a partial fork forward to completion.
func (x *XWAL) Fork(atMainLT uint64, childName, oldFutureName string) (*XWAL, error) {
	plan, err := x.buildForkPlan(atMainLT, childName, oldFutureName)
	if err != nil {
		return nil, err
	}
	if err := writeForkPlan(x.root, plan); err != nil {
		return nil, err
	}
	// Apply through the already-open channel handles; each Fork makes its
	// log a read-only branch point.
	getLog := func(e forkPlanEntry) (*disk.Log, func(), error) {
		return x.chans[e.Name].log, func() {}, nil
	}
	if err := applyForkPlan(x.root, plan, getLog); err != nil {
		return nil, err
	}
	if err := removeForkPlan(x.root); err != nil {
		return nil, err
	}
	childBranch := append(append([]string(nil), x.branch...), childName)
	return Open(x.root, x.cfg, childBranch...)
}

// buildForkPlan computes the per-channel boundaries for a joint fork at
// atMainLT. Empty channels are skipped (nothing to inherit). The main
// channel is placed last so it is the commit point.
func (x *XWAL) buildForkPlan(atMainLT uint64, childName, oldFutureName string) (forkPlan, error) {
	// childName is required. oldFutureName may be "" — that means an N-ary
	// add-one (no old-future / continuation is materialized); used to add a
	// sibling at a branch point's tail.
	if childName == "" {
		return forkPlan{}, fmt.Errorf("xwal: fork needs a non-empty child name")
	}
	if oldFutureName != "" && oldFutureName == childName {
		return forkPlan{}, fmt.Errorf("xwal: fork child and old-future names must differ")
	}
	plan := forkPlan{AtMainLT: atMainLT, Child: childName, OldFuture: oldFutureName}
	// Joint re-home: decided ONCE from the main channel — every child fork
	// whose divergence (.fork base) is past the split point moves into the
	// continuation, and the SAME set moves in every channel (by name). This
	// keeps the triune's node trees in lockstep on a re-split-below, where a
	// sparse related channel would otherwise tail-fork and skip the re-home.
	bases, err := x.chans[x.main].log.ChildForkBases()
	if err != nil {
		return forkPlan{}, err
	}
	for name, base := range bases {
		if base > atMainLT {
			plan.Rehome = append(plan.Rehome, name)
		}
	}
	var mainEntry *forkPlanEntry
	for _, name := range x.order {
		ch := x.chans[name]
		if ch.log.FirstIndex() == 0 {
			continue // truly-empty channel (never written): nothing to fork
		}
		// NOTE: an empty-OWN log (forkBase>0, all content inherited) is NOT
		// skipped — it forks into empty inheriting children so every trunk
		// gets its own branch in this channel (write isolation). disk.Fork
		// handles the empty-own case.
		var atIdx uint64
		if name == x.main {
			atIdx = atMainLT
			first, last := ch.log.FirstIndex(), ch.log.LastIndex()
			if atIdx <= first || atIdx > last+1 {
				return forkPlan{}, fmt.Errorf("xwal: fork main-LT %d out of range (%d, %d]", atIdx, first, last+1)
			}
		} else {
			b, err := x.boundaryFor(ch, atMainLT)
			if err != nil {
				return forkPlan{}, err
			}
			atIdx = b
		}
		entry := forkPlanEntry{Name: name, Dir: x.relDir(ch.dir), AtIdx: atIdx}
		if name == x.main {
			mainEntry = &entry
			continue
		}
		plan.Channels = append(plan.Channels, entry)
	}
	if mainEntry != nil {
		plan.Channels = append(plan.Channels, *mainEntry)
	}
	return plan, nil
}

// applyForkPlan forks each planned channel that has not been forked yet.
// It is idempotent: a channel whose child branch already exists is
// skipped, so re-running a partially applied plan (recovery) completes it
// without double-forking. getLog yields the log to fork and a cleanup.
func applyForkPlan(root string, plan forkPlan, getLog func(forkPlanEntry) (*disk.Log, func(), error)) error {
	for _, e := range plan.Channels {
		childPath := filepath.Join(root, e.Dir, plan.Child)
		if pathExists(childPath) {
			continue // already forked
		}
		l, done, err := getLog(e)
		if err != nil {
			return fmt.Errorf("xwal: fork %q: open: %w", e.Name, err)
		}
		child, ferr := l.ForkRehome(e.AtIdx, plan.Child, plan.OldFuture, plan.Rehome)
		if ferr != nil {
			done()
			return fmt.Errorf("xwal: fork channel %q at %d: %w", e.Name, e.AtIdx, ferr)
		}
		child.Close()
		done()
	}
	return nil
}

// recoverFork completes a fork that a sentinel says was interrupted. It
// opens each channel fresh (the parent xwal is not open during recovery)
// using the manifest to pick the codec and reducible fold.
func recoverFork(root string, cfg Config, man manifest, plan forkPlan) error {
	codec, err := codecByName(man.Codec)
	if err != nil {
		return err
	}
	kinds := map[string]manifestChannel{}
	for _, mc := range man.Channels {
		kinds[mc.Name] = mc
	}
	getLog := func(e forkPlanEntry) (*disk.Log, func(), error) {
		mc, ok := kinds[e.Name]
		if !ok {
			return nil, nil, fmt.Errorf("channel %q not in manifest", e.Name)
		}
		opts := disk.Options{Codec: codec, SegmentSize: cfg.SegmentSize}
		if mc.Kind == "reducible" {
			r, ok := resolveReducer(cfg, mc.Reducer)
			if !ok || r.Reduce == nil {
				return nil, nil, fmt.Errorf("no reducer %q for channel %q", mc.Reducer, e.Name)
			}
			opts.OnSegmentOpen = reducibleFold(r.Reduce, r.Initial)
		}
		l, err := disk.Open(filepath.Join(root, e.Dir), opts)
		if err != nil {
			return nil, nil, err
		}
		return l, func() { l.Close() }, nil
	}
	return applyForkPlan(root, plan, getLog)
}

func writeForkPlan(root string, plan forkPlan) error {
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	final := filepath.Join(root, forkPendingName)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

func readForkPlan(root string) (forkPlan, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, forkPendingName))
	if errors.Is(err, os.ErrNotExist) {
		return forkPlan{}, false, nil
	}
	if err != nil {
		return forkPlan{}, false, err
	}
	var p forkPlan
	if err := json.Unmarshal(data, &p); err != nil {
		return forkPlan{}, false, fmt.Errorf("xwal: parse fork sentinel: %w", err)
	}
	return p, true, nil
}

func removeForkPlan(root string) error {
	return os.Remove(filepath.Join(root, forkPendingName))
}

// relDir returns a channel log dir relative to the xwal root.
func (x *XWAL) relDir(dir string) string {
	rel, err := filepath.Rel(x.root, dir)
	if err != nil {
		return dir
	}
	return rel
}

// boundaryFor returns the channel LT at which a related channel should
// fork for a main-timeline fork at atMainLT. It works in the channel's
// OWN index space (the one disk.Fork validates against), not the
// parent-delegated global view:
//   - empty-own (no entries since forkBase): fork at the own first index,
//     so the children are empty inheriting branches (write isolation);
//   - else: the first OWN entry whose main LT >= atMainLT, or own-tail+1
//     if the channel hasn't caught up.
func (x *XWAL) boundaryFor(ch *channel, atMainLT uint64) (uint64, error) {
	ownFirst := ch.log.ForkBase()
	if ownFirst == 0 {
		ownFirst = 1
	}
	last := ch.log.LastIndex()
	if last < ownFirst {
		return ownFirst, nil // empty-own: fork at the own first index
	}
	found := uint64(0)
	err := ch.log.RangeOwn(ownFirst, func(idx uint64, payload []byte) error {
		m, _, derr := decodeFrame(payload)
		if derr != nil {
			return derr
		}
		if m >= atMainLT {
			found = idx
			return errStopRange
		}
		return nil
	})
	if err != nil && err != errStopRange {
		return 0, err
	}
	if found == 0 {
		return last + 1, nil
	}
	return found, nil
}
