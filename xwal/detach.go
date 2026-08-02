package xwal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/segment"
)

// Detach makes a node self-sufficient: it absorbs the history prefix it
// currently reads through an ancestor, and stops pointing at one.
//
// This is what a delete owes a BOUNDARY SURVIVOR — an aria outside the
// delete set whose lineage runs through it. Once every such survivor has
// detached, the doomed directories carry nothing anyone still reads and can
// be unlinked.
//
// Crash-safe and idempotent by ordering, with no journal: the absorbed
// records are written FIRST, as segments below the node's own fork base,
// where every read still delegates to the ancestor and so cannot see them.
// Only the marker flip publishes them, and that is a rename. Crash before
// it and the node still reads through its ancestor, with some ignored files
// that the next Detach overwrites; crash after and it is independent.
//
// Copy, not adopt: it touches nothing the ancestor owns and nothing the
// node's own writer is using, so it needs no quiesce. Adopting the
// ancestor's directory would be cheaper in bytes and would require one.
func (t *Trunks) Detach(node string) error {
	if node == "" {
		return fmt.Errorf("xwal: cannot detach the root")
	}
	endMutation, err := t.beginFlatCreate()
	if err != nil {
		return err
	}
	defer endMutation()

	x, err := t.openHotTopology(node)
	if err != nil {
		return err
	}
	codec := x.codec
	prefixes := map[string][][]byte{}
	bases := map[string]uint64{}
	for _, c := range x.Channels() {
		ch := x.chans[c.Name]
		base := ch.log.ForkBase()
		bases[c.Name] = base
		if base <= 1 {
			continue // already its own root in this channel
		}
		var rows [][]byte
		err := ch.log.Range(1, func(idx uint64, payload []byte) error {
			if idx >= base {
				return errStopRange
			}
			rows = append(rows, append([]byte(nil), payload...))
			return nil
		})
		if err != nil && err != errStopRange {
			x.Close()
			return fmt.Errorf("xwal: detach %q read %q: %w", node, c.Name, err)
		}
		prefixes[c.Name] = rows
	}
	initial := map[string][]byte{}
	for _, c := range x.Channels() {
		if ch := x.chans[c.Name]; ch.kind == ChannelReducible {
			initial[c.Name] = ch.initial
		}
	}
	x.Close()

	// Phase 1: write the absorbed prefix. Invisible until the flip.
	for name, rows := range prefixes {
		dir := filepath.Join(t.root, name, node)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		if err := writePrefixSegments(dir, rows, initial[name], codec, t.cfg.SegmentSize); err != nil {
			return fmt.Errorf("xwal: detach %q write %q: %w", node, name, err)
		}
	}
	// Phase 2: publish. Each marker write is an atomic replacement, so a
	// reader sees the old lineage or the new one, never a torn one.
	for name := range bases {
		dir := filepath.Join(t.root, name, node)
		// A channel added after this node was created has no directory
		// here, so the node has no presence in it and nothing to detach.
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if err := writeSyncedFile(filepath.Join(dir, ".fork"), []byte("base=1\n")); err != nil {
			return err
		}
	}
	kind := "conversation"
	if n := t.node(node); n != nil && n.Kind != "" {
		kind = n.Kind
	}
	if err := writeFlatMarker(t.irDir(node), "", kind); err != nil {
		return err
	}
	return t.rebuild()
}

// writePrefixSegments lays rows down as segments starting at index 1, sized
// by segmentSize. A reducible channel's first segment carries the initial
// watermark, so folding from it reproduces the same state the ancestor gave.
func writePrefixSegments(dir string, rows [][]byte, initial []byte, codec segment.SegmentCodec, segmentSize int64) error {
	if len(rows) == 0 {
		return nil
	}
	if segmentSize <= 0 {
		segmentSize = 1 << 21
	}
	idx := uint64(1)
	for i := 0; i < len(rows); {
		path := filepath.Join(dir, segFileName(idx, codec))
		s, err := segment.Create(path, codec, idx, segmentSize)
		if err != nil {
			return err
		}
		if initial != nil {
			if err := s.WriteHeader(initial); err != nil {
				s.Close()
				return err
			}
		}
		wrote := 0
		for ; i < len(rows); i++ {
			if _, err := s.Append(rows[i]); err != nil {
				if err == segment.ErrFull && wrote > 0 {
					break
				}
				s.Close()
				return err
			}
			wrote++
		}
		if err := s.Sync(); err != nil {
			s.Close()
			return err
		}
		if err := s.Close(); err != nil {
			return err
		}
		idx += uint64(wrote)
	}
	return disk.SyncDir(dir)
}

// DetachAll detaches every node in the set, in lineage order so a survivor
// never absorbs from an ancestor that has already been detached out from
// under it.
func (t *Trunks) DetachAll(nodes []string) error {
	for _, n := range nodes {
		if _, err := os.Stat(t.irDir(n)); err != nil {
			continue
		}
		if err := t.Detach(n); err != nil {
			return err
		}
	}
	return nil
}
