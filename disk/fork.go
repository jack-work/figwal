package disk

import (
	"errors"
	"github.com/jack-work/figwal/segment"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// forkMarkerName is the name of the metadata file present in every
// forked child directory. It records the global log index this fork
// begins at; the parent is always the immediate parent directory.
const forkMarkerName = ".fork"

// forkPendingName, when present in a log directory, signals that a
// previous fork operation crashed before completing. Open refuses to
// proceed; the operator must resolve manually.
const forkPendingName = ".fork-pending"

var (
	ErrForkPending  = errors.New("fork in progress: dir contains .fork-pending sentinel")
	ErrForkConflict = errors.New("fork name conflicts with existing entry")
	ErrInvalidForkName = errors.New("fork name must be a non-empty filename without separators")
)

// readForkMarker reads .fork from dir if present. Returns (0, nil) if
// the file is absent (dir is a root, not a forked child).
func readForkMarker(dir string) (uint64, error) {
	path := filepath.Join(dir, forkMarkerName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	// Format: one line, key=value pairs separated by lines. Today we
	// only carry base=<N>.
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return 0, fmt.Errorf("malformed .fork line: %q", line)
		}
		switch strings.TrimSpace(k) {
		case "base":
			n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("malformed .fork base: %w", err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf(".fork present but missing base= field")
}

// writeForkMarker writes a .fork file declaring this dir as a fork
// starting at base. The write goes through a temp file + rename for
// atomicity.
func writeForkMarker(dir string, base uint64) error {
	final := filepath.Join(dir, forkMarkerName)
	tmp := final + ".tmp"
	body := fmt.Sprintf("base=%d\n", base)
	if err := os.WriteFile(tmp, []byte(body), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// hasForkPending checks for the in-progress sentinel.
func hasForkPending(dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, forkPendingName))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// hasSubdirs returns true if dir contains at least one subdirectory
// that isn't a dot-prefixed internal directory. Subdirectory presence
// is what marks a log as a branch point and therefore read-only.
func hasSubdirs(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		return true, nil
	}
	return false, nil
}

// pathExists reports whether a filesystem entry exists at p.
func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// childSubdirs returns the names of non-dot subdirectories of dir (the
// child forks of a branch point).
func childSubdirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// validateForkName rejects empty, dot, double-dot, and names containing
// any path separator. The user explicitly opted out of canonicalization
// so we only reject the unambiguously broken inputs.
func validateForkName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalidForkName
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidForkName
	}
	return nil
}

// segAction classifies what happens to each sealed segment under a
// fork at atIdx: keep in the trunk, move whole to the old-future
// subdir, or split into a kept prefix + moved suffix.
type segAction uint8

const (
	actKeep segAction = iota
	actMove
	actSplit
)

// Fork splits this log at atIdx. The log keeps the prefix
// [FirstIndex, atIdx-1] and becomes read-only; entries at and after
// atIdx (if any) move into a same-named "old future" subdir; a fresh
// subdir named childName is created, empty and writable from atIdx.
// The new fork is returned, opened with this log as its parent.
//
// The optional variadic argument overrides the old-future subdir
// name (default: path.Base(l.dir)). Pass at most one override; an
// empty string keeps the default.
//
// Branch points are N-ary and re-splittable:
//   - Forking again at the same point (atIdx == LastIndex+1) just adds
//     another sibling child; no data moves.
//   - Forking BELOW an existing branch point (atIdx < its split index)
//     inserts an intermediate branch point: the suffix and all existing
//     child forks are re-homed into the old-future, since they descend
//     from it. Re-homing is a directory move; `..`-walk parent
//     resolution adapts automatically.
//
// Constraints:
//   - atIdx must be in (FirstIndex, LastIndex+1]; the prefix retains
//     at least one entry.
//   - childName must be a clean filename, must not equal the old-future
//     subdir name, and must not collide with an existing sibling.
//   - oldFutureName (when provided) must also be a clean filename.
//
// Crash safety: a .fork-pending sentinel file is written before any
// destructive change and removed after the fork completes. If a crash
// leaves the sentinel behind, Open refuses to proceed and the
// operator must resolve manually.
func (l *Log) Fork(atIdx uint64, childName string, oldFutureNameOpt ...string) (*Log, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := validateForkName(childName); err != nil {
		return nil, err
	}
	if len(oldFutureNameOpt) > 1 {
		return nil, fmt.Errorf("Fork: at most one oldFutureName override permitted, got %d",
			len(oldFutureNameOpt))
	}
	oldFutureName := filepath.Base(l.dir)
	if len(oldFutureNameOpt) == 1 && oldFutureNameOpt[0] != "" {
		oldFutureName = oldFutureNameOpt[0]
		if err := validateForkName(oldFutureName); err != nil {
			return nil, fmt.Errorf("oldFutureName: %w", err)
		}
	}
	if childName == oldFutureName {
		return nil, fmt.Errorf("%w: childName %q equals old-future subdir name",
			ErrForkConflict, childName)
	}
	if l.isEmptyLocked() {
		return nil, fmt.Errorf("cannot fork empty log %q", l.dir)
	}
	first := l.firstIndexLocked()
	last := l.lastIndexLocked()
	if atIdx <= first || atIdx > last+1 {
		return nil, fmt.Errorf("fork index %d out of range (%d, %d]",
			atIdx, first, last+1)
	}
	// childName must not collide with an existing entry (a segment file
	// or a sibling fork). The old-future name is validated later, only
	// when an old-future child is actually created.
	if p := filepath.Join(l.dir, childName); pathExists(p) {
		return nil, fmt.Errorf("%w: %s", ErrForkConflict, p)
	}
	// Capture pre-existing child forks before we create any new subdir.
	// On a re-split below this branch point they move into the old-future.
	existingChildren, err := childSubdirs(l.dir)
	if err != nil {
		return nil, err
	}

	// Seal the active segment so the fork plan only has to consider
	// sealed segments.
	if l.active != nil {
		if err := l.active.Sync(); err != nil {
			return nil, err
		}
		l.sealed = append(l.sealed, l.active)
		l.active = nil
	}

	// Header (reducible) mode: the new branches need a fresh watermark
	// at the fork boundary (state at atIdx-1), because the prefix is
	// read-only and neither the child nor the old-future should have to
	// fold back into it. Computed once here, while the log is intact,
	// via the same fold the log uses on rotation.
	var forkWatermark []byte
	headerMode := l.opts.OnSegmentOpen != nil
	if headerMode {
		w, werr := l.stateAtLocked(atIdx - 1)
		if werr != nil {
			return nil, fmt.Errorf("fork watermark at %d: %w", atIdx-1, werr)
		}
		forkWatermark = w
	}

	// Classify each sealed segment relative to atIdx.
	actions := make([]segAction, len(l.sealed))
	hasMoves := false
	hasSplit := false
	for i, s := range l.sealed {
		switch {
		case s.LastIndex() < atIdx:
			actions[i] = actKeep
		case s.FirstIndex() >= atIdx:
			actions[i] = actMove
			hasMoves = true
		default:
			actions[i] = actSplit
			hasSplit = true
		}
	}
	oldFutureExists := hasMoves || hasSplit
	// The old-future is only created when there's a suffix to carry; its
	// name conflict is therefore only relevant then.
	if oldFutureExists {
		if p := filepath.Join(l.dir, oldFutureName); pathExists(p) {
			return nil, fmt.Errorf("%w: %s", ErrForkConflict, p)
		}
	}
	oldFutureDir := filepath.Join(l.dir, oldFutureName)
	newForkDir := filepath.Join(l.dir, childName)

	// Write the sentinel describing the plan; remove it on success.
	pendingPath := filepath.Join(l.dir, forkPendingName)
	pendingBody := fmt.Sprintf("at=%d\nchild=%s\nold=%s\n",
		atIdx, childName, oldFutureName)
	if err := os.WriteFile(pendingPath, []byte(pendingBody), 0644); err != nil {
		return nil, err
	}
	rollbackPending := func(err error) error {
		// Best-effort: clear the sentinel even on failure so the
		// caller can attempt cleanup. Any half-staged files remain
		// for the operator to inspect.
		_ = os.Remove(pendingPath)
		return err
	}

	// Make subdirs first; populate them next.
	if oldFutureExists {
		if err := os.MkdirAll(oldFutureDir, 0755); err != nil {
			return nil, rollbackPending(err)
		}
	}
	if err := os.MkdirAll(newForkDir, 0755); err != nil {
		return nil, rollbackPending(err)
	}

	// In header mode, seed the child with a header-only first segment
	// carrying the fork watermark, so its first append builds on the
	// fork-point state rather than an empty one. The whole-moved
	// segments already carry their own watermarks; the split suffix got
	// the watermark above.
	if headerMode {
		childSeg, err := segment.Create(filepath.Join(newForkDir, l.segName(atIdx)), l.codec, atIdx, l.opts.SegmentSize)
		if err != nil {
			return nil, rollbackPending(err)
		}
		if err := childSeg.WriteHeader(forkWatermark); err != nil {
			childSeg.Close()
			return nil, rollbackPending(err)
		}
		if err := childSeg.Sync(); err != nil {
			childSeg.Close()
			return nil, rollbackPending(err)
		}
		childSeg.Close()
	}

	// Split the boundary segment if one exists.
	var prefixReplacement *segment.Segment
	for i, s := range l.sealed {
		if actions[i] != actSplit {
			continue
		}
		prefixPath := s.Path()
		prefixTmp := prefixPath + ".tmp"
		suffixPath := filepath.Join(oldFutureDir, l.segName(atIdx))

		prefix, err := segment.Create(prefixTmp, l.codec, s.BaseIndex(), 0)
		if err != nil {
			return nil, rollbackPending(err)
		}
		// The prefix keeps the boundary segment's original watermark
		// (same starting state); the suffix begins a new branch from the
		// fork watermark (state at atIdx-1).
		if headerMode {
			if err := prefix.WriteHeader(s.Header()); err != nil {
				prefix.Close()
				return nil, rollbackPending(err)
			}
		}
		boundaryLocalCut := atIdx - s.BaseIndex()
		for j := uint64(0); j < boundaryLocalCut; j++ {
			payload, rerr := s.ReadIndex(j)
			if rerr != nil {
				prefix.Close()
				return nil, rollbackPending(rerr)
			}
			if _, aerr := prefix.Append(payload); aerr != nil {
				prefix.Close()
				return nil, rollbackPending(aerr)
			}
		}
		if err := prefix.Sync(); err != nil {
			prefix.Close()
			return nil, rollbackPending(err)
		}
		prefix.Close()

		suffix, err := segment.Create(suffixPath, l.codec, atIdx, 0)
		if err != nil {
			return nil, rollbackPending(err)
		}
		if headerMode {
			if err := suffix.WriteHeader(forkWatermark); err != nil {
				suffix.Close()
				return nil, rollbackPending(err)
			}
		}
		for j := boundaryLocalCut; j < s.Count(); j++ {
			payload, rerr := s.ReadIndex(j)
			if rerr != nil {
				suffix.Close()
				return nil, rollbackPending(rerr)
			}
			if _, aerr := suffix.Append(payload); aerr != nil {
				suffix.Close()
				return nil, rollbackPending(aerr)
			}
		}
		if err := suffix.Sync(); err != nil {
			suffix.Close()
			return nil, rollbackPending(err)
		}
		suffix.Close()

		// Replace the original boundary file with the prefix.
		if err := s.Close(); err != nil {
			return nil, rollbackPending(err)
		}
		if err := os.Rename(prefixTmp, prefixPath); err != nil {
			return nil, rollbackPending(err)
		}
		// Reopen the new prefix as the readOnly segment that will
		// remain in this Log's sealed list.
		var reopen *segment.Segment
		if headerMode {
			reopen, err = segment.OpenReadOnlyHeadered(prefixPath, l.codec, s.BaseIndex())
		} else {
			reopen, err = segment.OpenReadOnly(prefixPath, l.codec, s.BaseIndex())
		}
		if err != nil {
			return nil, rollbackPending(err)
		}
		prefixReplacement = reopen
		break
	}

	// Move whole-move segments into the old-future subdir.
	for i, s := range l.sealed {
		if actions[i] != actMove {
			continue
		}
		src := s.Path()
		dst := filepath.Join(oldFutureDir, filepath.Base(src))
		if err := s.Close(); err != nil {
			return nil, rollbackPending(err)
		}
		if err := os.Rename(src, dst); err != nil {
			return nil, rollbackPending(err)
		}
	}

	// Re-home pre-existing child forks into the old-future. They branched
	// from the suffix [atIdx, ...], which now lives there; their own .fork
	// base is unchanged and their parent resolves via `..` to the
	// old-future. Only happens on a re-split below an existing branch
	// point (oldFutureExists is always true there, since the branch
	// point's own segments supply the suffix).
	if oldFutureExists {
		for _, name := range existingChildren {
			src := filepath.Join(l.dir, name)
			dst := filepath.Join(oldFutureDir, name)
			if err := os.Rename(src, dst); err != nil {
				return nil, rollbackPending(err)
			}
		}
	}

	// Write .fork markers in the children. For the old-future the
	// declared base equals atIdx (its first segment starts there).
	if oldFutureExists {
		if err := writeForkMarker(oldFutureDir, atIdx); err != nil {
			return nil, rollbackPending(err)
		}
	}
	if err := writeForkMarker(newForkDir, atIdx); err != nil {
		return nil, rollbackPending(err)
	}

	// fsync every directory we touched so dentries are durable.
	if err := syncDir(l.dir); err != nil {
		return nil, rollbackPending(err)
	}
	if oldFutureExists {
		if err := syncDir(oldFutureDir); err != nil {
			return nil, rollbackPending(err)
		}
	}
	if err := syncDir(newForkDir); err != nil {
		return nil, rollbackPending(err)
	}

	// Rebuild l.sealed: keep the kept segments and the split prefix
	// (which slots in where the boundary used to be).
	newSealed := make([]*segment.Segment, 0, len(l.sealed))
	for i, s := range l.sealed {
		switch actions[i] {
		case actKeep:
			newSealed = append(newSealed, s)
		case actSplit:
			if prefixReplacement != nil {
				newSealed = append(newSealed, prefixReplacement)
			}
		}
	}
	l.sealed = newSealed
	l.active = nil
	l.readOnly = true

	if err := os.Remove(pendingPath); err != nil {
		return nil, err
	}
	if err := syncDir(l.dir); err != nil {
		return nil, err
	}
	slog.Info("log forked",
		"dir", l.dir,
		"atIdx", atIdx,
		"child", childName,
		"oldFuture", oldFutureExists)

	// Open the new fork as a child of this log.
	childOpts := l.opts
	childOpts.Parent = l
	return Open(newForkDir, childOpts)
}
