package segment

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// The accounting must survive an append racing an eviction: the evictor CASes
// the pointer it read, and an extended block means it lost. Dropping the
// segment from the held set on that path charged bytes to something nothing
// could ever evict, and the budget then shrank for the process's life.
func TestCacheAccountingSurvivesAppendVersusEvict(t *testing.T) {
	old := CacheBudget()
	defer SetCacheBudget(old)
	SetCacheBudget(1 << 20)

	dir := t.TempDir()
	s, err := Create(filepath.Join(dir, "seg"), BinaryCodec{}, 1, 1<<24)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	payload := make([]byte, 256)
	for i := 0; i < 64; i++ {
		if _, err := s.Append(payload); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ReadIndex(0); err != nil { // load the block
		t.Fatal(err)
	}
	// A Segment's own state is serialized by disk.Log's lock; the EVICTOR is
	// the one thing that touches it from outside, which is the interleaving
	// worth testing. `lock` stands in for that lock.
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			lock.Lock()
			_, err := s.Append(payload)
			lock.Unlock()
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			lock.Lock()
			_, err := s.ReadIndex(0)
			lock.Unlock()
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			payloadCache.evictTo(0)
		}
	}()
	wg.Wait()

	// The decision itself, deterministically: an evictor holding a pointer an
	// append has already replaced must keep its hands off the accounting.
	if _, err := s.ReadIndex(0); err != nil {
		t.Fatal(err)
	}
	stale := s.block.Load()
	if _, err := s.Append(payload); err != nil { // extendBlock replaces it
		t.Fatal(err)
	}
	fresh := s.block.Load()
	if stale == fresh {
		t.Fatal("append did not extend the block; the case under test cannot arise")
	}
	charged := CachedBytes()
	payloadCache.mu.Lock()
	lost := payloadCache.dropLocked(s, stale)
	_, stillHeld := payloadCache.held[s]
	payloadCache.mu.Unlock()
	if lost {
		t.Fatal("dropping a stale block reported success")
	}
	if !stillHeld {
		t.Fatal("a lost eviction race forgot the segment, stranding its bytes")
	}
	if got := CachedBytes(); got != charged {
		t.Fatalf("a lost eviction race changed the accounting: %d -> %d", charged, got)
	}

	// Whatever the interleaving, the counter must describe what is held.
	payloadCache.evictTo(0)
	if got := CachedBytes(); got != 0 {
		t.Fatalf("after evicting everything, %d bytes still charged", got)
	}
	if got := CachedSegments(); got != 0 {
		t.Fatalf("after evicting everything, %d segments still held", got)
	}
	_ = os.Remove(filepath.Join(dir, "seg"))
}
