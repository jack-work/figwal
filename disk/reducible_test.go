package disk

import (
	"encoding/binary"
	"testing"

	"github.com/jack-work/figwal/segment"
)

// The reducible model layered on top of figwal headers: each segment's
// block-0 header is a watermark (here, the running sum of all entry
// values in prior segments); each entry is an opaque value (a "patch").
// State at any index = header(segment-of-idx) folded with the entries in
// [segment-base, idx]. These tests exercise the figwal primitive only —
// the fold lives in the OnSegmentOpen callback, as it will in xwal.

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func deU64(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// sumFold is the watermark callback: new header = prev header (sum so
// far) + the sum of the sealed segment's values.
func sumFold(prevHeader []byte, sealed [][]byte) ([]byte, error) {
	sum := deU64(prevHeader)
	for _, e := range sealed {
		sum += deU64(e)
	}
	return u64(sum), nil
}

func openReducible(t *testing.T, dir string, segSize int64) *Log {
	t.Helper()
	l, err := Open(dir, Options{
		Codec:         segment.BinaryCodec{},
		SegmentSize:   segSize,
		OnSegmentOpen: sumFold,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l
}

func TestReducible_HeaderRotationAndState(t *testing.T) {
	dir := t.TempDir()
	// Small segments so a 20-entry write rotates several times.
	l := openReducible(t, dir, 48)

	const n = uint64(20)
	for i := uint64(1); i <= n; i++ {
		if err := l.Write(i, u64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	bases := l.SegmentBaseIndexes()
	if len(bases) < 2 {
		t.Fatalf("expected multiple segments (rotation), got bases=%v", bases)
	}

	// The index is header-free: first/last/values are exactly the data.
	if got := l.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1", got)
	}
	if got := l.LastIndex(); got != n {
		t.Fatalf("LastIndex = %d, want %d", got, n)
	}
	for i := uint64(1); i <= n; i++ {
		b, err := l.Read(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got := deU64(b); got != i {
			t.Fatalf("Read(%d) = %d, want %d (header must not leak into the index)", i, got, i)
		}
	}

	assertWatermarks(t, l, bases, n)

	// Reopen: headers and data must survive recovery unchanged.
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l2 := openReducible(t, dir, 48)
	defer l2.Close()
	if got := l2.LastIndex(); got != n {
		t.Fatalf("after reopen LastIndex = %d, want %d", got, n)
	}
	if !equalU64(l2.SegmentBaseIndexes(), bases) {
		t.Fatalf("after reopen bases = %v, want %v", l2.SegmentBaseIndexes(), bases)
	}
	assertWatermarks(t, l2, bases, n)
}

// assertWatermarks checks that every segment's header equals the sum of
// all values strictly before its base, and that state-at-idx (header +
// in-segment fold) equals the true running sum at idx.
func assertWatermarks(t *testing.T, l *Log, bases []uint64, n uint64) {
	t.Helper()
	for _, base := range bases {
		want := triNum(base - 1) // sum 1..base-1
		h, err := l.HeaderAt(base)
		if err != nil {
			t.Fatalf("HeaderAt(%d): %v", base, err)
		}
		if got := deU64(h); got != want {
			t.Fatalf("watermark at segment base %d = %d, want %d", base, got, want)
		}
	}
	// State-at-idx reconstructed from the local watermark only.
	for idx := uint64(1); idx <= n; idx++ {
		segBase := baseOf(bases, idx)
		h, err := l.HeaderAt(idx)
		if err != nil {
			t.Fatalf("HeaderAt(%d): %v", idx, err)
		}
		state := deU64(h)
		for i := segBase; i <= idx; i++ {
			b, err := l.Read(i)
			if err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
			state += deU64(b)
		}
		if want := triNum(idx); state != want {
			t.Fatalf("state-at(%d) = %d, want %d (folded from watermark at base %d)", idx, state, want, segBase)
		}
	}
}

func triNum(k uint64) uint64 { return k * (k + 1) / 2 }

func baseOf(bases []uint64, idx uint64) uint64 {
	b := bases[0]
	for _, base := range bases {
		if base <= idx {
			b = base
		}
	}
	return b
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A header-mode log must still behave as a plain log for a single
// segment that never rotates (first header = empty state = 0).
func TestReducible_SingleSegmentEmptyWatermark(t *testing.T) {
	dir := t.TempDir()
	l := openReducible(t, dir, 0) // default (large) segment: no rotation
	defer l.Close()

	for i := uint64(1); i <= 5; i++ {
		if err := l.Write(i, u64(i*10)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if bases := l.SegmentBaseIndexes(); len(bases) != 1 {
		t.Fatalf("expected one segment, got %v", bases)
	}
	h, err := l.HeaderAt(1)
	if err != nil {
		t.Fatalf("HeaderAt(1): %v", err)
	}
	if got := deU64(h); got != 0 {
		t.Fatalf("first segment watermark = %d, want 0 (empty state)", got)
	}
	b, err := l.Read(3)
	if err != nil {
		t.Fatalf("read 3: %v", err)
	}
	if got := deU64(b); got != 30 {
		t.Fatalf("Read(3) = %d, want 30", got)
	}
}

// Fork of a header-mode log is rejected at the disk layer (driven by
// xwal instead).
func TestReducible_ForkRejected(t *testing.T) {
	dir := t.TempDir()
	l := openReducible(t, dir, 48)
	defer l.Close()
	for i := uint64(1); i <= 6; i++ {
		if err := l.Write(i, u64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if _, err := l.Fork(4, "child"); err == nil {
		t.Fatal("expected header-mode Fork to be rejected, got nil error")
	}
}
