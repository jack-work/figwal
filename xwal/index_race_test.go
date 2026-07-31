package xwal

import (
	"fmt"
	"sync"
	"testing"
)

// Node hands callers the live *NodeInfo, so a mutator must REPLACE an entry,
// never edit the one a reader may already be holding. Spawn used to append to
// p.Children in place, which a reader with the pointer reads without the lock:
// a data race that "treat NodeInfo as immutable" documented away rather than
// prevented.
//
// Run with -race; without it this only ever passes.
func TestIndexReadersDoNotRaceSpawn(t *testing.T) {
	x := newIndex(nil)
	if err := x.Spawn("", "p", "t0", false); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if n, ok := x.Node("p"); ok {
					_ = len(n.Children) // the field Spawn used to mutate in place
					_ = n.Frozen()
				}
			}
		}()
	}
	for i := range 200 {
		if err := x.Spawn("p", fmt.Sprintf("p/c%d", i), "", false); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}
