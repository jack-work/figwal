package disk

import (
	"testing"
)

// THE HAZARD TEST FOR FOLDING FROM A HEADER, WRITTEN BEFORE THE API THAT
// WOULD MAKE IT PASS.
//
// figaro's delta seam wants to build a snapshot the cheap way: take the
// block-0 header of the segment holding idx, and fold ONLY [segBase..idx]
// onto it. StateAt already does exactly that internally. What figaro cannot
// do is StateAt, because StateAt folds through the reducer -- and figaro's
// reducer JSON round-trips the entire board PER RECORD. It needs the header
// BYTES and then folds the records itself, decoded, once.
//
// TO DO THAT IT NEEDS TWO FACTS: the header, and THE BASE INDEX OF THE
// SEGMENT THAT HEADER BELONGS TO. The base is what says which records to
// fold. And the two facts are not equally available:
//
//	HeaderAt(idx)          WALKS THE PARENT CHAIN for idx < forkBase.
//	SegmentBaseIndexes()   "does NOT walk the parent chain" -- its own doc.
//
// SO ON A BRANCH, FOR ANY INDEX BELOW THE FORK POINT, PAIRING THEM IS WRONG
// AND SILENT: HeaderAt answers from the parent, SegmentBaseIndexes answers
// only about the child, and a caller that pairs them folds the parent's
// header onto a range computed from the child's boundaries. Nobody is told.
// The result is a wrong snapshot, only before a fork point.
//
// This file pins that, and pins the property any exposure must satisfy:
// FOLD(header-of-idx, records[base-of-idx .. idx]) == StateAt(idx), AT EVERY
// INDEX, ON A TRUNK AND ACROSS A FORK. StateAt is the oracle because it is
// the same computation done by the library that owns the segments.

// headerFoldAt is what a consumer would write given ONLY the two functions as
// they exist today: ask for the header, then find the base by searching this
// log's own segment bases for the greatest one <= idx. It is the naive
// pairing, written out so the test can show exactly where it goes wrong
// rather than describing it.
func headerFoldAt(t *testing.T, l *Log, idx uint64) ([]byte, error) {
	t.Helper()
	header, err := l.HeaderAt(idx)
	if err != nil {
		return nil, err
	}
	base := uint64(1)
	for _, b := range l.SegmentBaseIndexes() {
		if b <= idx && b > base {
			base = b
		}
	}
	sealed := make([][]byte, 0, idx-base+1)
	for i := base; i <= idx; i++ {
		p, err := l.Read(i)
		if err != nil {
			return nil, err
		}
		sealed = append(sealed, p)
	}
	return sumFold(header, sealed)
}

// TestHeaderFold_OnATrunkTheNaivePairingIsCorRect: the control. On a log with
// no parent, HeaderAt and SegmentBaseIndexes describe the same lineage, so
// the naive pairing reproduces StateAt at every index. If this fails, the
// test below proves nothing about forks -- it would only prove the harness
// is broken.
func TestHeaderFold_OnATrunkTheNaivePairingIsCorrect(t *testing.T) {
	dir := t.TempDir()
	l := openReducible(t, dir, 256) // small, so it rotates
	defer l.Close()

	const n = 40
	for i := uint64(1); i <= n; i++ {
		if err := l.Write(i, u64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// POSITIVE ASSERTION: a single-segment log would make this test vacuous,
	// because the header would always be the initial one.
	if got := len(l.SegmentBaseIndexes()); got < 3 {
		t.Fatalf("fixture did not rotate enough: %d segments, want >= 3", got)
	}

	for idx := uint64(1); idx <= n; idx++ {
		want, err := l.StateAt(idx)
		if err != nil {
			t.Fatalf("StateAt(%d): %v", idx, err)
		}
		got, err := headerFoldAt(t, l, idx)
		if err != nil {
			t.Fatalf("headerFoldAt(%d): %v", idx, err)
		}
		if deU64(got) != deU64(want) {
			t.Fatalf("idx %d: header-fold %d, StateAt %d", idx, deU64(got), deU64(want))
		}
	}
}

// TestHeaderFold_AcrossAForkTheNaivePairingIsWRONG is the hazard.
//
// It asserts the CURRENT, BROKEN behaviour deliberately, in the shape the
// campaign uses for a hazard that is about to be fixed: when an exposure
// lands that gives a consumer the header and its base TOGETHER, this test
// goes red and whoever lands it must invert it -- which is the point. A
// silent wrong answer that nobody has written down is the failure mode; a
// test that says "this is wrong today" is not.
func TestHeaderFold_AcrossAForkTheNaivePairingIsWrong(t *testing.T) {
	dir := t.TempDir()
	parent := openReducible(t, dir, 256)

	const n = 40
	for i := uint64(1); i <= n; i++ {
		if err := parent.Write(i, u64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if got := len(parent.SegmentBaseIndexes()); got < 3 {
		t.Fatalf("fixture did not rotate enough: %d segments, want >= 3", got)
	}

	// Fork PAST several segment boundaries, so there are indices below the
	// fork base that live in the parent's EARLIER segments -- the ones the
	// child knows nothing about.
	const cut = 30
	child, err := parent.Fork(cut, "branch")
	if err != nil {
		parent.Close()
		t.Fatal(err)
	}
	defer child.Close()
	defer parent.Close()

	if child.ForkBase() != cut {
		t.Fatalf("forkBase = %d, want %d", child.ForkBase(), cut)
	}

	// Write a little on the branch so it has segments of its own.
	for i := uint64(cut); i < cut+8; i++ {
		if err := child.Write(i, u64(i)); err != nil {
			t.Fatalf("child write %d: %v", i, err)
		}
	}

	// THE TWO FACTS DISAGREE ABOUT WHICH LINEAGE THEY DESCRIBE.
	bases := child.SegmentBaseIndexes()
	for _, b := range bases {
		if b < cut {
			t.Fatalf("child reported a segment base %d below its fork base %d, "+
				"which would mean SegmentBaseIndexes walks the parent after all "+
				"and this whole test is about a hazard that does not exist", b, cut)
		}
	}

	// Below the fork base the naive pairing is WRONG, and this asserts it is
	// wrong so that fixing it is visible.
	wrong := 0
	for idx := uint64(1); idx < cut; idx++ {
		want, err := child.StateAt(idx)
		if err != nil {
			t.Fatalf("StateAt(%d): %v", idx, err)
		}
		got, err := headerFoldAt(t, child, idx)
		if err != nil {
			// An error is an honest outcome too -- it is not a silent wrong
			// answer. Count it as "not wrong" and let the assertion below
			// speak for the ones that returned a number.
			continue
		}
		if deU64(got) != deU64(want) {
			wrong++
		}
	}
	if wrong == 0 {
		t.Fatal("EXPECTED THE NAIVE PAIRING TO PRODUCE WRONG STATE BELOW THE FORK " +
			"BASE. If it no longer does, either SegmentBaseIndexes now walks the " +
			"parent chain or an exposure landed that hands the header and its base " +
			"together -- in which case INVERT this test rather than deleting it: it " +
			"is the only place that records why the pairing was unsafe.")
	}
	t.Logf("the naive pairing produced wrong state at %d of %d indices below the fork base",
		wrong, cut-1)

	// AT OR ABOVE the fork base the child's own bases apply, so the pairing
	// is correct there -- which is what makes the bug subtle rather than
	// obvious: it is right everywhere a single-lineage test would look.
	for idx := uint64(cut); idx < cut+8; idx++ {
		want, err := child.StateAt(idx)
		if err != nil {
			t.Fatalf("StateAt(%d): %v", idx, err)
		}
		got, err := headerFoldAt(t, child, idx)
		if err != nil {
			t.Fatalf("headerFoldAt(%d): %v", idx, err)
		}
		if deU64(got) != deU64(want) {
			t.Fatalf("idx %d at/above the fork base should agree: header-fold %d, StateAt %d",
				idx, deU64(got), deU64(want))
		}
	}
}
