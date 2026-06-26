package xwal

import "fmt"

// Fork branches every channel as a unit at the main timeline position
// atMainLT. childName is the new branch; oldFutureName is the original
// continuation (figwal makes the fork point read-only and re-homes the
// suffix under that name). Both names are used identically across every
// channel, so each resulting branch is addressable as a unit. Returns
// the new branch opened for writing.
//
// The main channel forks at atMainLT; each related channel forks at its
// own boundary — the first channel LT whose referenced main LT is >=
// atMainLT (or its tail if it has not yet caught up). Reducible channels
// get a fresh watermark at the boundary so the new branch folds from the
// fork-point state. A channel with no entries is left unforked.
func (x *XWAL) Fork(atMainLT uint64, childName, oldFutureName string) (*XWAL, error) {
	type plan struct {
		ch    *channel
		atIdx uint64
		fork  bool
	}
	plans := make([]plan, 0, len(x.order))
	for _, name := range x.order {
		ch := x.chans[name]
		first := ch.log.FirstIndex()
		if first == 0 {
			plans = append(plans, plan{ch: ch, fork: false})
			continue
		}
		var atIdx uint64
		if name == x.main {
			atIdx = atMainLT
			last := ch.log.LastIndex()
			if atIdx <= first || atIdx > last+1 {
				return nil, fmt.Errorf("xwal: fork main-LT %d out of range (%d, %d]", atIdx, first, last+1)
			}
		} else {
			b, err := x.boundaryFor(ch, atMainLT)
			if err != nil {
				return nil, err
			}
			atIdx = b
		}
		plans = append(plans, plan{ch: ch, atIdx: atIdx, fork: true})
	}

	// Fork the main channel last: it is the commit point. If a related
	// channel fork fails we have not yet diverged the main timeline.
	ordered := make([]plan, 0, len(plans))
	var mainPlan *plan
	for i := range plans {
		if plans[i].ch.name == x.main {
			mainPlan = &plans[i]
			continue
		}
		ordered = append(ordered, plans[i])
	}
	if mainPlan != nil {
		ordered = append(ordered, *mainPlan)
	}

	for _, p := range ordered {
		if !p.fork {
			continue
		}
		child, err := p.ch.log.Fork(p.atIdx, childName, oldFutureName)
		if err != nil {
			return nil, fmt.Errorf("xwal: fork channel %q at %d: %w", p.ch.name, p.atIdx, err)
		}
		// We reopen the whole branch below; close this transient handle.
		child.Close()
	}

	childBranch := append(append([]string(nil), x.branch...), childName)
	return Open(x.root, x.cfg, childBranch...)
}

// boundaryFor returns the channel LT at which a related channel should
// fork for a main-timeline fork at atMainLT: the first entry whose main
// LT is >= atMainLT, or LastIndex+1 if the channel has not caught up.
func (x *XWAL) boundaryFor(ch *channel, atMainLT uint64) (uint64, error) {
	first := ch.log.FirstIndex()
	last := ch.log.LastIndex()
	if first == 0 || last < first {
		return last + 1, nil
	}
	found := uint64(0)
	err := ch.log.Range(first, func(idx uint64, payload []byte) error {
		m, _, derr := decodeFrame(payload)
		if derr != nil {
			return derr
		}
		if m >= atMainLT {
			found = idx
			return errStopRange
		}
		return nil
	})
	if err != nil && err != errStopRange {
		return 0, err
	}
	if found == 0 {
		return last + 1, nil
	}
	return found, nil
}
