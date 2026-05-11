package log

import (
	"bytes"
	"errors"
	"figwal/segment"
	"fmt"
	"sync"
	"testing"
)

func TestCachedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := uint64(1); i <= 5; i++ {
		if err := c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if c.FirstIndex() != 1 || c.LastIndex() != 5 {
		t.Fatalf("range %d..%d", c.FirstIndex(), c.LastIndex())
	}
	for i := uint64(1); i <= 5; i++ {
		got, err := c.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf(`{"i":%d}`, i)
		if string(got) != want {
			t.Fatalf("[%d]=%q want %q", i, got, want)
		}
	}
}

func TestCachedReopenLoadsExisting(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	c.Close()

	c2, err := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if c2.LastIndex() != 3 {
		t.Fatalf("LastIndex=%d", c2.LastIndex())
	}
	got, _ := c2.Read(2)
	if string(got) != `{"i":2}` {
		t.Fatalf("got %q", got)
	}
}

func TestCachedRange(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 5; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	var seen []uint64
	err := c.Range(2, func(idx uint64, _ []byte) error {
		seen = append(seen, idx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{2, 3, 4, 5}
	if len(seen) != len(want) {
		t.Fatalf("seen=%v want %v", seen, want)
	}
	for i := range seen {
		if seen[i] != want[i] {
			t.Fatalf("seen[%d]=%d want %d", i, seen[i], want[i])
		}
	}
}

func TestCachedConcurrentReaders(t *testing.T) {
	// Many parallel readers see consistent values across many writes.
	// Readers each iterate a snapshot fully; the snapshot must not
	// shift under them mid-iteration.
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 20; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		i := uint64(21)
		for {
			select {
			case <-stop:
				return
			default:
				c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
				i++
			}
		}
	}()
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := 0; it < 200; it++ {
				snap := c.Snapshot()
				prev := uint64(0)
				err := snap.Range(1, func(idx uint64, payload []byte) error {
					if idx != prev+1 && prev != 0 {
						return fmt.Errorf("gap: %d after %d", idx, prev)
					}
					prev = idx
					want := fmt.Sprintf(`{"i":%d}`, idx)
					if string(payload) != want {
						return fmt.Errorf("[%d]=%q want %q", idx, payload, want)
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-writerDone
}

func TestCachedForkSplitsCache(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{
		Codec:       segment.JSONLCodec{},
		SegmentSize: 200,
	})
	defer c.Close()
	for i := uint64(1); i <= 6; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	child, err := c.Fork(4, "alt")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	// Parent now only sees [1, 3] in its own snapshot.
	if c.LastIndex() != 3 {
		t.Fatalf("parent LastIndex=%d", c.LastIndex())
	}
	for i := uint64(1); i <= 3; i++ {
		got, err := c.Read(i)
		if err != nil {
			t.Fatalf("parent read %d: %v", i, err)
		}
		want := fmt.Sprintf(`{"i":%d}`, i)
		if string(got) != want {
			t.Fatalf("parent[%d]=%q want %q", i, got, want)
		}
	}
	// Child reads of the prefix fall through to parent snapshot.
	for i := uint64(1); i <= 3; i++ {
		got, err := child.Read(i)
		if err != nil {
			t.Fatalf("child read %d: %v", i, err)
		}
		want := fmt.Sprintf(`{"i":%d}`, i)
		if string(got) != want {
			t.Fatalf("child[%d]=%q want %q", i, got, want)
		}
	}
	// Child writes go to its own cache.
	if err := child.Write(4, []byte(`{"alt":4}`)); err != nil {
		t.Fatal(err)
	}
	got, _ := child.Read(4)
	if string(got) != `{"alt":4}` {
		t.Fatalf("child[4]=%q", got)
	}
}

func TestCachedSiblingForksSharePointer(t *testing.T) {
	// Two sibling forks created from the same parent should share the
	// same *cacheSnapshot pointer for the parent chain.
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 4; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	a, err := c.Fork(3, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// After forking once, parent is readOnly so we can't fork again
	// from c. But the parent snapshot a holds should equal c's
	// current snapshot pointer (same truncated trunk).
	if a.snap.Load().parent != c.snap.Load() {
		t.Fatal("child parent snapshot pointer should equal trunk's snapshot pointer")
	}
}

func TestCachedSnapshotIsImmutableView(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 3; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	snap := c.Snapshot()
	// Write more after taking the snapshot.
	for i := uint64(4); i <= 6; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	// Snapshot still shows 3 entries.
	if snap.LastIndex() != 3 {
		t.Fatalf("snapshot LastIndex=%d want 3", snap.LastIndex())
	}
	if _, err := snap.Read(4); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot.Read(4): want ErrNotFound, got %v", err)
	}
}

func TestCachedNotFoundOnEmpty(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	if _, err := c.Read(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCachedTruncateFront(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{
		Codec:       segment.JSONLCodec{},
		SegmentSize: 80,
	})
	defer c.Close()
	for i := uint64(1); i <= 6; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	// Truncate to drop the entire first sealed segment. Use the active
	// segment's FirstIndex as the cut.
	cut := c.log.active.FirstIndex()
	if err := c.TruncateFront(cut); err != nil {
		t.Fatal(err)
	}
	if c.FirstIndex() != cut {
		t.Fatalf("FirstIndex=%d want %d", c.FirstIndex(), cut)
	}
	if _, err := c.Read(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound for truncated idx, got %v", err)
	}
	got, _ := c.Read(cut)
	want := fmt.Sprintf(`{"i":%d}`, cut)
	if string(got) != want {
		t.Fatalf("[%d]=%q want %q", cut, got, want)
	}
}

func TestCachedDisksWriteThrough(t *testing.T) {
	// Cached.Write must hit disk. Reopen as a plain Log and verify
	// the entries are there.
	dir := t.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	for i := uint64(1); i <= 3; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}
	c.Close()
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, _ := l.Read(2)
	if !bytes.Equal(got, []byte(`{"i":2}`)) {
		t.Fatalf("got %q", got)
	}
}

var cachedReadSink byte

func BenchmarkCachedRead(b *testing.B) {
	dir := b.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 1024; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d,"event":"step"}`, i)))
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(1)
		var sink byte
		for pb.Next() {
			payload, err := c.Read(i)
			if err != nil {
				b.Fatal(err)
			}
			sink ^= payload[0]
			i++
			if i > 1024 {
				i = 1
			}
		}
		cachedReadSink = sink
	})
}

func BenchmarkCachedRangeFull(b *testing.B) {
	dir := b.TempDir()
	c, _ := OpenCached(dir, Options{Codec: segment.JSONLCodec{}})
	defer c.Close()
	for i := uint64(1); i <= 1024; i++ {
		c.Write(i, []byte(fmt.Sprintf(`{"i":%d,"event":"step"}`, i)))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var seen uint64
		c.Range(1, func(_ uint64, _ []byte) error {
			seen++
			return nil
		})
		if seen != 1024 {
			b.Fatalf("seen=%d", seen)
		}
	}
}
