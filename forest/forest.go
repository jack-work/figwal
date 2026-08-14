// Package forest is the ONE shape every derived cache in this stack
// shares: a window of materialized units over a durable substrate,
// budgeted in bytes, LRU by epoch, whose INDEX survives eviction and
// whose misses rematerialize exactly the range they name from the layer
// below.
//
// It exists because the same pattern was hand-rolled three times -- the
// segment payload cache here, the decoded-IR window and the composed-UI
// turn cache in figaro -- each with its own vocabulary, its own
// accountant, and its own copy of the bugs. Gluck's ruling (2026-08-14):
// one generic type, type-parameterized units, abstraction over
// convention; and the trunk forest's PREFIX SHARING becomes a property
// of the cache, not just of the addressing: a fork's read of the shared
// prefix hits its ancestor's runs, so N branches of one trunk pay one
// residency for the common history.
//
// Two laws carried in from the field, each paid for in production:
//
//   - RECENCY IS AN EPOCH. A per-read atomic stamp on a shared line made
//     reads SLOWER with more readers (figwal, 2026-08). Touch bumps a
//     coarse epoch counter; eviction compares epochs, not timestamps.
//   - COUNT WHAT YOU PIN. A unit that cannot be rematerialized (no
//     bracket below it) pins ITSELF -- never its cache -- and its bytes
//     stay on the meter. A meter that reads zero at peak retention is
//     the worst possible meter (figaro S1, storm-triage, 2026-08-14).
package forest

import (
	"sync"
	"sync/atomic"
)

// Ref is one step of a lineage: a trunk node and the coordinate at
// which its child diverged (the fork base). A read below the base
// belongs to the ancestor; at or above it, to the child. The LAST Ref
// is the node being read; ancestors precede it, root first.
type Ref struct {
	Node string
	Base uint64 // first coordinate that is the CHILD's own; 0 for the root
}

// Coord names a contiguous unit range within ONE node, (From..To]
// inclusive of To, exclusive of From -- the bracket convention every
// substrate in this stack already speaks.
type Coord struct {
	Node     string
	From, To uint64
}

// Source rematerializes the units of a coord from the layer below.
// Returning fewer units than the coord names is legal (a hole degrades
// to a gap, never a lie); returning an error poisons nothing -- the
// range is simply not resident and the next read retries.
type Source[U any] func(Coord) ([]U, error)

// Sizer estimates one unit's resident bytes. An estimate at insert,
// like every window in this stack; do not reflect-walk (that lied 3x
// low once already) -- count the strings that dominate.
type Sizer[U any] func(U) int

// Keyer names the coordinate of one unit, so a run's index can be
// rebuilt from its units and a Range can slice exactly.
type Keyer[U any] func(U) uint64

// ---- the accountant ----

// Budget is the shared byte bound across every Cache that holds it.
// One per concern (raw / decoded / composed today; one pool tomorrow is
// a config choice, not a rewrite). It never calls an owner while
// holding its lock: eviction collects victims under the lock and
// hollows them after, so lock order cannot invert (the segment cache's
// own trade, kept).
type Budget struct {
	limit atomic.Int64
	bytes atomic.Int64
	epoch atomic.Int64

	mu     sync.Mutex
	owners map[owner]struct{}

	evictions atomic.Int64
}

type owner interface {
	// coldest reports the owner's least-recent evictable run's epoch,
	// and whether one exists.
	coldest() (int64, bool)
	// evictColdest hollows that run and returns the bytes freed.
	evictColdest() int64
}

// NewBudget bounds its caches to limit bytes; 0 is unbounded.
func NewBudget(limit int64) *Budget {
	b := &Budget{owners: map[owner]struct{}{}}
	b.limit.Store(limit)
	return b
}

// SetLimit retunes the bound live (the config reload path).
func (b *Budget) SetLimit(limit int64) { b.limit.Store(limit) }

// Stats reports resident bytes, the limit, and evictions to date --
// every resident structure arrives with its number in doctor mem.
func (b *Budget) Stats() (resident, limit, evictions int64) {
	if b == nil {
		return 0, 0, 0
	}
	return b.bytes.Load(), b.limit.Load(), b.evictions.Load()
}

// charge admits delta bytes and evicts the globally coldest runs until
// the budget fits. Called by caches on insert and re-tally.
func (b *Budget) charge(delta int64) {
	if b == nil {
		return
	}
	b.bytes.Add(delta)
	limit := b.limit.Load()
	if limit <= 0 {
		return
	}
	for b.bytes.Load() > limit {
		b.mu.Lock()
		var victim owner
		var coldestEpoch int64
		for o := range b.owners {
			if e, ok := o.coldest(); ok && (victim == nil || e < coldestEpoch) {
				victim, coldestEpoch = o, e
			}
		}
		b.mu.Unlock()
		if victim == nil {
			return // only pins remain; the meter still tells the truth
		}
		if freed := victim.evictColdest(); freed > 0 {
			b.bytes.Add(-freed)
			b.evictions.Add(1)
		} else {
			return // victim raced away; next charge retries
		}
	}
}

func (b *Budget) adopt(o owner) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.owners[o] = struct{}{}
	b.mu.Unlock()
}

func (b *Budget) disown(o owner) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.owners, o)
	b.mu.Unlock()
}
