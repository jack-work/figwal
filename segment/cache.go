package segment

import (
	"sort"
	"sync"
	"sync/atomic"
)

// Payload residency is LAZY and BOUNDED.
//
// A segment holds an offset per record from the moment it is opened; the
// payloads themselves are read by pread and are not retained until somebody
// asks for one. The first read of a segment loads its payloads into one
// immutable block published behind an atomic pointer, so every later read of
// that segment is a slice index with no lock and no syscall.
//
// Blocks are charged against a process-wide budget. When a load takes the
// total past it, the least recently used blocks are dropped: the pointer is
// nilled, the segment keeps serving by pread, and a later read reloads it.
// Dropping a block can never lose data — every byte of it is in the file
// already — and a caller still holding a payload from an evicted block keeps
// it alive on its own, because the block is a slice of ordinary garbage
// collected bytes.
//
// The active segment's block is EXTENDED by Append rather than invalidated,
// so a writer's own tail stays resident without a reload per record.

const defaultCacheBudget = 32 << 20

type block struct {
	payloads [][]byte
	bytes    int64
}

// cache is the process-wide registry of loaded blocks. It holds no payloads
// itself: it knows which segments have one, how large it is, and when it was
// last touched, which is everything eviction needs.
type cache struct {
	budget atomic.Int64
	bytes  atomic.Int64
	clock  atomic.Int64

	mu   sync.Mutex
	held map[*Segment]struct{}
}

var payloadCache = func() *cache {
	c := &cache{held: make(map[*Segment]struct{})}
	c.budget.Store(defaultCacheBudget)
	return c
}()

// SetCacheBudget bounds the total bytes of segment payloads held in memory
// across every open log. Zero disables caching entirely (every read becomes a
// pread); a negative value is ignored.
func SetCacheBudget(bytes int64) {
	if bytes < 0 {
		return
	}
	payloadCache.budget.Store(bytes)
	payloadCache.evictTo(bytes)
}

// CacheBudget and CachedBytes report the bound and what is held against it.
func CacheBudget() int64 { return payloadCache.budget.Load() }
func CachedBytes() int64 { return payloadCache.bytes.Load() }

// CachedSegments reports how many segments currently hold a block.
func CachedSegments() int {
	payloadCache.mu.Lock()
	defer payloadCache.mu.Unlock()
	return len(payloadCache.held)
}

func (c *cache) tick() int64 { return c.clock.Add(1) }

func (c *cache) admit(s *Segment, b *block) {
	c.mu.Lock()
	c.held[s] = struct{}{}
	c.mu.Unlock()
	total := c.bytes.Add(b.bytes)
	if budget := c.budget.Load(); total > budget {
		c.evictTo(budget)
	}
}

func (c *cache) charge(delta int64) { c.bytes.Add(delta) }

// forget drops a segment's accounting without touching its pointer. Called
// when the segment itself goes away.
func (c *cache) forget(s *Segment) {
	c.mu.Lock()
	_, held := c.held[s]
	delete(c.held, s)
	c.mu.Unlock()
	if !held {
		return
	}
	if b := s.block.Swap(nil); b != nil {
		c.bytes.Add(-b.bytes)
	}
}

// evictTo drops least recently used blocks until the total fits. Sorting the
// held set is affordable because it only runs when the budget is exceeded,
// and the alternative — an intrusive LRU list — needs a lock on every read.
func (c *cache) evictTo(budget int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bytes.Load() <= budget {
		return
	}
	segs := make([]*Segment, 0, len(c.held))
	for s := range c.held {
		segs = append(segs, s)
	}
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].usedAt.Load() < segs[j].usedAt.Load()
	})
	for _, s := range segs {
		if c.bytes.Load() <= budget {
			return
		}
		c.dropLocked(s, s.block.Load())
	}
}

// dropLocked releases the block the evictor read, and reports whether it was
// the one still installed. A LOST race means an append extended the block
// between the load and the swap: the segment keeps its (larger) block and
// stays in the held set, because forgetting it there would charge bytes to a
// segment nothing could ever evict.
func (c *cache) dropLocked(s *Segment, b *block) bool {
	if b == nil {
		delete(c.held, s)
		return true
	}
	if !s.block.CompareAndSwap(b, nil) {
		return false
	}
	c.bytes.Add(-b.bytes)
	delete(c.held, s)
	return true
}

// cachedPayloads returns this segment's block, loading it if the budget
// allows. A nil result means the caller must read from the file.
func (s *Segment) cachedPayloads() *block {
	if b := s.block.Load(); b != nil {
		s.usedAt.Store(payloadCache.tick())
		return b
	}
	if payloadCache.budget.Load() <= 0 {
		return nil
	}
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	if b := s.block.Load(); b != nil {
		return b
	}
	b, err := s.readAllPayloads()
	if err != nil {
		return nil
	}
	s.usedAt.Store(payloadCache.tick())
	if !s.block.CompareAndSwap(nil, b) {
		return s.block.Load()
	}
	payloadCache.admit(s, b)
	return b
}

// readAllPayloads reads every record of the segment in one pass.
func (s *Segment) readAllPayloads() (*block, error) {
	b := &block{payloads: make([][]byte, 0, s.count)}
	for i := uint64(0); i < s.count; i++ {
		off := s.offsets[i]
		nextOff := s.size
		if int(i)+1 < len(s.offsets) {
			nextOff = s.offsets[i+1]
		}
		payload, _, err := s.codec.ReadFrame(s.f, off, nextOff)
		if err != nil {
			return nil, err
		}
		b.payloads = append(b.payloads, payload)
		b.bytes += int64(len(payload)) + sliceHeaderBytes
	}
	return b, nil
}

// sliceHeaderBytes charges the per-record bookkeeping so a segment of many
// tiny records is not accounted as free.
const sliceHeaderBytes = 24

// extendBlock keeps the writer's own tail resident: when the active segment
// already has a block, an append lands in it instead of invalidating it. The
// CAS is what keeps the accounting honest against a concurrent eviction — if
// the block was dropped between the load and the swap, the append is simply
// not cached.
func (s *Segment) extendBlock(payload []byte) {
	old := s.block.Load()
	if old == nil {
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	nb := &block{
		payloads: append(old.payloads[:len(old.payloads):len(old.payloads)], cp),
		bytes:    old.bytes + int64(len(cp)) + sliceHeaderBytes,
	}
	if s.block.CompareAndSwap(old, nb) {
		payloadCache.charge(nb.bytes - old.bytes)
		s.usedAt.Store(payloadCache.tick())
	}
}

// DropCache releases this segment's payload block. The segment keeps
// serving reads from the file; a later read reloads it.
func (s *Segment) DropCache() { payloadCache.forget(s) }
