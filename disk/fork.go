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
// Constraints:
//   - atIdx must be in (FirstIndex, LastIndex+1]; the prefix retains
//     at least one entry.
//   - childName must be a clean filename and must not equal the
//     old-future subdir name.
//   - oldFutureName (when provided) must also be a clean filename.
//   - This log must not already be a branch point.
//
// Crash safety: a .fork-pending sentinel file is written before any
// destructive change and removed after the fork completes. If a crash
// leaves the sentinel behind, Open refuses to proceed and the
// operator must resolve manually.
func (l *Log) Fork(atIdx uint64, childName string, oldFutureNameOpt ...string) (*Log, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.readOnly {
		return nil, fmt.Errorf("%w: %s", ErrReadOnly, l.dir)
	}
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
	for _, name := range []string{childName, oldFutureName} {
		p := filepath.Join(l.dir, name)
		if _, err := os.Stat(p); err == nil {
			return nil, fmt.Errorf("%w: %s", ErrForkConflict, p)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
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
		reopen, err := segment.OpenReadOnly(prefixPath, l.codec, s.BaseIndex())
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
