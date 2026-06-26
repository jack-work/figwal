package xwal

import "testing"

// TestXWAL_ForkRecovery simulates a crash partway through a joint fork —
// the sentinel is on disk and one channel has diverged, the rest have
// not — and verifies that Open rolls the fork forward to completion and
// clears the sentinel, leaving every channel branched as a unit.
func TestXWAL_ForkRecovery(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)
	for i := uint64(1); i <= 4; i++ {
		if _, err := x.AppendMain([]byte("msg")); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Append("translations", i, []byte("wire")); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Append("chalkboard", i, []byte(`{"set":{"turn":`+itoa(i)+`}}`)); err != nil {
			t.Fatal(err)
		}
	}

	// Build the plan and write the sentinel, then fork ONLY the first
	// channel — mimicking a crash before the rest (and the main channel)
	// diverged.
	plan, err := x.buildForkPlan(3, "alt", "orig")
	if err != nil {
		t.Fatalf("buildForkPlan: %v", err)
	}
	if err := writeForkPlan(dir, plan); err != nil {
		t.Fatalf("writeForkPlan: %v", err)
	}
	first := plan.Channels[0]
	partial, err := x.chans[first.Name].log.Fork(first.AtIdx, plan.Child, plan.OldFuture)
	if err != nil {
		t.Fatalf("partial fork of %q: %v", first.Name, err)
	}
	partial.Close()
	x.Close()

	// Reopen the trunk: recovery (inside Open) must complete the
	// remaining forks; triune fails the test if Open errors.
	x2, _ := triune(t, dir)
	x2.Close()

	if _, pending, _ := readForkPlan(dir); pending {
		t.Fatal("sentinel still present after recovery")
	}

	// The child branch is now whole: every channel forked and readable.
	child, err := Open(dir, x.cfg, "alt")
	if err != nil {
		t.Fatalf("open child after recovery: %v", err)
	}
	defer child.Close()
	if m, _, err := child.Read("ir", 2); err != nil || m != 2 {
		t.Fatalf("child ir[2] after recovery = (%d,%v), want main-LT 2", m, err)
	}
	st, err := child.StateAt("chalkboard", 2)
	if err != nil {
		t.Fatalf("child chalkboard StateAt(2): %v", err)
	}
	if got := field(t, st, "turn"); got != "2" {
		t.Fatalf("child chalkboard turn = %s, want 2", got)
	}

	// And the original continuation survived on the 'orig' branch.
	orig, err := Open(dir, x.cfg, "orig")
	if err != nil {
		t.Fatalf("open orig after recovery: %v", err)
	}
	defer orig.Close()
	if st, err := orig.StateAt("chalkboard", 4); err != nil {
		t.Fatalf("orig chalkboard StateAt(4): %v", err)
	} else if got := field(t, st, "turn"); got != "4" {
		t.Fatalf("orig chalkboard turn = %s, want 4", got)
	}
}
