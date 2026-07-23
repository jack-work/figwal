package xwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/log"
	"github.com/jack-work/figwal/segment"
)

// forkPendingName, in the xwal root, records a joint fork that began but
// did not finish. Open completes it (roll-forward) before serving any
// branch, so a crash mid-fork never leaves the triune half-diverged.
const forkPendingName = ".xwal-fork-pending"

// forkPlan is the durable description of a joint fork: where to split,
// what to name the branches, the exact per-channel boundary indexes
// (recorded so recovery never has to recompute them from partial state),
// and the trunk-marker commit. Channels are listed in apply order — the
// main channel last.
type forkPlan struct {
	AtMainLT  uint64          `json:"atMainLT"`
	Child     string          `json:"child"`
	OldFuture string          `json:"oldFuture"`
	Main      string          `json:"main"`
	Rehome    []string        `json:"rehome,omitempty"` // child node dirs to re-home into the old-future (joint, by name)
	Channels  []forkPlanEntry `json:"channels"`
	Commit    *forkCommit     `json:"commit,omitempty"`
}

// forkCommit is the trunk-marker phase of a joint fork, journaled in the
// plan so a crash between the channel forks and the marker writes can
// never lose the source trunk id (the old-future inherits it) or the
// minted child id.
type forkCommit struct {
	SourceTrunk string `json:"sourceTrunk,omitempty"`
	ChildTrunk  string `json:"childTrunk,omitempty"`
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
// fork-point state.
//
// The fork is crash-atomic across channels: a plan sentinel is written
// before any channel diverges and removed only once all have, and Open
// rolls a partial fork forward to completion.
func (x *XWAL) Fork(atMainLT uint64, childName, oldFutureName string) (*XWAL, error) {
	return x.forkJoint(atMainLT, childName, oldFutureName, nil)
}

func (x *XWAL) forkJoint(atMainLT uint64, childName, oldFutureName string, commit *forkCommit) (*XWAL, error) {
	if err := x.ensurePrivate(); err != nil {
		return nil, err
	}
	if err := x.flushAll(); err != nil {
		return nil, err
	}
	if err := x.validateForkChannels(); err != nil {
		return nil, err
	}
	plan, err := x.buildForkPlan(atMainLT, childName, oldFutureName)
	if err != nil {
		return nil, err
	}
	plan.Commit = commit
	if err := writeForkPlan(x.root, plan); err != nil {
		return nil, err
	}
	// Apply through the already-open channel handles; each Fork makes its
	// log a read-only branch point.
	getLog := func(e forkPlanEntry) (*log.Log, func(), error) {
		return x.chans[e.Name].log, func() {}, nil
	}
	if err := applyCachedForkPlan(x.root, plan, getLog); err != nil {
		return nil, x.abortForkPlan(plan, err)
	}
	if err := applyForkCommit(x.root, plan); err != nil {
		return nil, x.abortForkPlan(plan, err)
	}
	if err := removeForkPlan(x.root); err != nil {
		return nil, err
	}
	childBranch := append(append([]string(nil), x.branch...), childName)
	return Open(x.root, x.cfg, childBranch...)
}

// abortForkPlan unwinds a joint fork that failed live: every channel is
// rolled back to its pre-fork layout and the plan sentinel is removed,
// so the source keeps accepting appends and no phantom fork materializes
// at the next open. If the rollback itself fails the plan stays armed
// for crash-style recovery and both errors surface.
func (x *XWAL) abortForkPlan(plan forkPlan, cause error) error {
	var errs []error
	for _, e := range plan.Channels {
		ch := x.chans[e.Name]
		if ch == nil {
			continue
		}
		dir := filepath.Join(x.root, e.Dir)
		if err := rollbackChannelFork(dir, plan, e.AtIdx, x.codec, ch.kind == ChannelReducible); err != nil {
			errs = append(errs, fmt.Errorf("roll back %q: %w", e.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(append([]error{cause}, errs...)...)
	}
	// The live handle's channels that forked before the failing leg hold
	// read-only inner logs and truncated snapshots; reopen them against
	// the rolled-back layout so the handle stays usable.
	var refreshErrs []error
	for _, e := range plan.Channels {
		ch := x.chans[e.Name]
		if ch == nil {
			continue
		}
		if err := ch.log.Close(); err != nil {
			refreshErrs = append(refreshErrs, fmt.Errorf("refresh %q: %w", e.Name, err))
			continue
		}
		l, err := log.Open(ch.dir, x.channelOpts(ch))
		if err != nil {
			refreshErrs = append(refreshErrs, fmt.Errorf("refresh %q: %w", e.Name, err))
			continue
		}
		ch.mu.Lock()
		ch.log = l
		ch.fk = map[uint64]uint64{}
		ch.fkBuilt, ch.fkScan = false, false
		ch.fkNext, ch.fkFloor = 0, 0
		ch.mu.Unlock()
	}
	if err := removeForkPlan(x.root); err != nil {
		refreshErrs = append(refreshErrs, err)
	}
	if len(refreshErrs) > 0 {
		return errors.Join(append([]error{
			fmt.Errorf("xwal: fork aborted and rolled back; handle stale, reopen required: %w", cause),
		}, refreshErrs...)...)
	}
	return fmt.Errorf("xwal: fork aborted and rolled back: %w", cause)
}

// applyForkCommit writes the trunk markers recorded in the plan:
// the old-future inherits the source trunk id, the child gets the
// minted id. Idempotent; runs both on the live path and in recovery.
func applyForkCommit(root string, plan forkPlan) error {
	if plan.Commit == nil {
		return nil
	}
	mainDir := ""
	for _, e := range plan.Channels {
		if e.Name == plan.Main {
			mainDir = e.Dir
		}
	}
	if mainDir == "" {
		return fmt.Errorf("xwal: fork plan has no main channel entry")
	}
	if plan.OldFuture != "" && plan.Commit.SourceTrunk != "" {
		if err := writeTrunkID(filepath.Join(root, mainDir, plan.OldFuture), plan.Commit.SourceTrunk); err != nil {
			return err
		}
	}
	if plan.Commit.ChildTrunk != "" {
		if err := writeTrunkID(filepath.Join(root, mainDir, plan.Child), plan.Commit.ChildTrunk); err != nil {
			return err
		}
	}
	return nil
}

// ErrTopologyIncomplete reports a manifest channel whose logical branch tree
// is not complete enough for a joint fork.
var ErrTopologyIncomplete = errors.New("xwal: channel topology incomplete")

func (x *XWAL) validateForkChannels() error {
	for _, name := range x.order {
		if name == x.main {
			continue
		}
		ch := x.chans[name]
		logicalDir := filepath.Join(append([]string{x.root, name}, x.branch...)...)
		if filepath.Clean(ch.dir) != filepath.Clean(logicalDir) {
			return fmt.Errorf("%w: channel %q missing logical branch %q",
				ErrTopologyIncomplete, name, strings.Join(x.branch, "/"))
		}
		rootedAt := -1
		probeDir := filepath.Join(x.root, name)
		for i, part := range x.branch {
			probeDir = filepath.Join(probeDir, part)
			if _, err := readForkBaseFile(filepath.Join(probeDir, ".fork")); errors.Is(err, os.ErrNotExist) {
				if first, ok, segmentErr := firstSegmentBase(probeDir, x.codec); segmentErr == nil && ok && first == 1 {
					rootedAt = i
				}
			}
		}
		parentDir := filepath.Join(x.root, name)
		for i, part := range x.branch {
			dir := filepath.Join(parentDir, part)
			if i < rootedAt {
				parentDir = dir
				continue
			}
			if i == rootedAt {
				parentDir = dir
				continue
			}
			_, err := readForkBaseFile(filepath.Join(dir, ".fork"))
			if err != nil {
				return fmt.Errorf("%w: channel %q node %q has no valid fork marker",
					ErrTopologyIncomplete, name, strings.Join(x.branch, "/"))
			}
			parentDir = dir
		}
	}
	return nil
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
	plan := forkPlan{AtMainLT: atMainLT, Child: childName, OldFuture: oldFutureName, Main: x.main}
	// Joint re-home: decided ONCE from the main channel — every child fork
	// whose divergence (.fork base) is past the split point moves into the
	// continuation, and the SAME set moves in every channel (by name). This
	// keeps the triune's node trees in lockstep on a re-split-below, where a
	// sparse related channel would otherwise tail-fork and skip the re-home.
	//
	// An N-ary add-one (oldFutureName == "") has no continuation to re-home
	// into, so it never re-homes. Skipping the computation also avoids
	// materializing an old-future for related channels — which, with no
	// explicit name, would default to filepath.Base(dir) and, for a slashed
	// channel name like "translations/anthropic", create a stray nested
	// "anthropic/anthropic" dir.
	if oldFutureName != "" {
		bases, err := x.chans[x.main].log.ChildForkBases()
		if err != nil {
			return forkPlan{}, err
		}
		for name, base := range bases {
			// Re-home every child whose divergence is at OR after the split
			// point. A fork at index k shares [1..k-1] and moves own content
			// [k..] to the continuation; a child forking at base == k diverges
			// exactly at the split, so it belongs to the future side too. Using
			// a strict `>` here left such a child stranded under the old branch
			// point — and when that child carried the owner's trunk id (the
			// owner's own continuation chain), the continuation re-stamped the
			// id onto a fresh node, producing two live leaves for one trunk.
			if base >= atMainLT {
				plan.Rehome = append(plan.Rehome, name)
			}
		}
	}
	var mainEntry *forkPlanEntry
	for _, name := range x.order {
		ch := x.chans[name]
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
	for _, entry := range plan.Channels {
		for _, descendant := range plan.Rehome {
			dir := filepath.Join(x.root, entry.Dir, descendant)
			if !validForkNode(dir, x.codec) {
				return forkPlan{}, fmt.Errorf(
					"%w: channel %q missing valid rehome descendant %q",
					ErrTopologyIncomplete, entry.Name, descendant,
				)
			}
		}
	}
	return plan, nil
}

func applyCachedForkPlan(root string, plan forkPlan, getLog func(forkPlanEntry) (*log.Log, func(), error)) error {
	for _, e := range plan.Channels {
		childPath := filepath.Join(root, e.Dir, plan.Child)
		if pathExists(childPath) {
			continue
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
		if err := child.Close(); err != nil {
			done()
			return fmt.Errorf("xwal: close forked channel %q: %w", e.Name, err)
		}
		done()
	}
	return nil
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

// recoverFork completes a fork that a sentinel says was interrupted.
// Each planned channel is rolled BACK to its pre-fork layout (every
// mutation up to the marker phase is a rename or a derivable file, so
// the union of the dir and its old-future always holds the full data),
// then re-forked through the normal machinery, then the trunk-marker
// commit is replayed. Idempotent; never refuses a dir-level sentinel —
// the rollback consumes it.
func recoverFork(root string, cfg Config, man manifest, plan forkPlan) error {
	codec, err := codecByName(man.Codec)
	if err != nil {
		return err
	}
	kinds := map[string]manifestChannel{}
	for _, mc := range man.Channels {
		kinds[mc.Name] = mc
	}
	for _, e := range plan.Channels {
		mc, ok := kinds[e.Name]
		if !ok {
			return fmt.Errorf("channel %q not in manifest", e.Name)
		}
		dir := filepath.Join(root, e.Dir)
		complete, err := channelForkComplete(dir, plan)
		if err != nil {
			return err
		}
		if complete {
			continue
		}
		headered := mc.Kind == ChannelReducible.String()
		if err := rollbackChannelFork(dir, plan, e.AtIdx, codec, headered); err != nil {
			return fmt.Errorf("xwal: roll back fork of %q: %w", e.Name, err)
		}
	}
	getLog := func(e forkPlanEntry) (*disk.Log, func(), error) {
		mc, ok := kinds[e.Name]
		if !ok {
			return nil, nil, fmt.Errorf("channel %q not in manifest", e.Name)
		}
		opts := disk.Options{
			Codec:       codec,
			SegmentSize: cfg.SegmentSize,
		}
		if mc.Kind == "reducible" {
			r, ok := resolveReducer(cfg, mc.Reducer, mc.Name)
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
	if err := applyForkPlan(root, plan, getLog); err != nil {
		return err
	}
	return applyForkCommit(root, plan)
}

// channelForkComplete reports whether a channel already finished its
// joint-fork leg: the in-dir sentinel is gone and the child carries its
// .fork marker (the marker is the last mutation before sentinel removal).
func channelForkComplete(dir string, plan forkPlan) (bool, error) {
	if pathExists(filepath.Join(dir, disk.ForkPendingName)) {
		return false, nil
	}
	_, err := readForkBaseFile(filepath.Join(dir, plan.Child, ".fork"))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// rollbackChannelFork restores a channel dir to its pre-fork layout.
// The fork only ever (a) creates the child dir with derivable seeds,
// (b) renames whole segments and re-homed child dirs into the
// old-future, and (c) splits the boundary segment — writing the durable
// suffix into the old-future BEFORE swapping the prefix in place. So:
// the child dir is deleted, old-future subdirs and segments are renamed
// back (a boundary suffix whose entries still live in the unsplit dir
// segment is a copy and is deleted instead), derivable watermark seeds
// are deleted, and the sentinel is consumed.
func rollbackChannelFork(dir string, plan forkPlan, atIdx uint64, codec segment.SegmentCodec, headered bool) (err error) {
	if err := os.RemoveAll(filepath.Join(dir, plan.Child)); err != nil {
		return err
	}
	if plan.OldFuture != "" {
		oldDir := filepath.Join(dir, plan.OldFuture)
		if pathExists(oldDir) {
			if err := rollbackOldFuture(dir, oldDir, atIdx, codec, headered); err != nil {
				return err
			}
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), codec.FileExt()+".tmp") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	if err := os.Remove(filepath.Join(dir, disk.ForkPendingName)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	return disk.SyncDir(dir)
}

func rollbackOldFuture(dir, oldDir string, atIdx uint64, codec segment.SegmentCodec, headered bool) error {
	entries, err := os.ReadDir(oldDir)
	if err != nil {
		return err
	}
	spanning, err := dirSegmentSpans(dir, atIdx, codec, headered)
	if err != nil {
		return err
	}
	for _, e := range entries {
		src := filepath.Join(oldDir, e.Name())
		if e.IsDir() {
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			if err := os.Rename(src, filepath.Join(dir, e.Name())); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(e.Name(), codec.FileExt()) {
			if err := os.Remove(src); err != nil {
				return err
			}
			continue
		}
		base, perr := strconv.ParseUint(strings.TrimSuffix(e.Name(), codec.FileExt()), 10, 64)
		if perr != nil {
			return fmt.Errorf("xwal: unexpected old-future file %q", src)
		}
		count, cerr := segmentEntryCount(src, codec, headered)
		if cerr != nil {
			return cerr
		}
		if base == atIdx && (spanning || count == 0) {
			// A boundary-suffix copy (the data still lives in the unsplit
			// dir segment) or a derivable watermark seed.
			if err := os.Remove(src); err != nil {
				return err
			}
			continue
		}
		if err := os.Rename(src, filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	for _, name := range []string{".fork", ".trunk", disk.ForkPendingName} {
		if err := os.Remove(filepath.Join(oldDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.Remove(oldDir)
}

func segmentEntryCount(path string, codec segment.SegmentCodec, headered bool) (int, error) {
	spans, err := scanSegmentFrames(path, codec)
	if err != nil {
		return 0, err
	}
	n := len(spans)
	if headered && n > 0 {
		n--
	}
	return n, nil
}

// dirSegmentSpans reports whether a segment in dir still covers atIdx
// (base < atIdx and entries reaching at least atIdx) — i.e. the
// boundary split never swapped the prefix in.
func dirSegmentSpans(dir string, atIdx uint64, codec segment.SegmentCodec, headered bool) (bool, error) {
	bases, err := segmentBases(dir, codec)
	if err != nil {
		return false, err
	}
	for _, base := range bases {
		if base >= atIdx {
			continue
		}
		count, err := segmentEntryCount(filepath.Join(dir, segFileName(base, codec)), codec, headered)
		if err != nil {
			return false, err
		}
		if count > 0 && base+uint64(count)-1 >= atIdx {
			return true, nil
		}
	}
	return false, nil
}

func writeForkPlan(root string, plan forkPlan) error {
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	final := filepath.Join(root, forkPendingName)
	tmp := final + ".tmp"
	if err := writeSyncedFile(tmp, body); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return disk.SyncDir(root)
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
	if err := os.Remove(filepath.Join(root, forkPendingName)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	return disk.SyncDir(root)
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
	err := ch.log.Range(ownFirst, func(idx uint64, payload []byte) error {
		m, derr := decodeMainLT(payload)
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
