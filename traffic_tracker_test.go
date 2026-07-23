package main

import "testing"

func TestTrafficTrackerRetainsHistoryAcrossSeconds(t *testing.T) {
	tr := NewTrafficTracker()

	// Simulate adds in distinct unix seconds without sleeping: inject via map.
	tr.mu.Lock()
	base := int64(1_700_000_000)
	tr.bySec[base] = 100
	tr.bySec[base+1] = 50
	tr.bySec[base+2] = 25
	tr.total = 175
	tr.mu.Unlock()

	tr.mu.Lock()
	now := base + 2
	tr.pruneLocked(now)
	out := make([]int, trafficWindowSecs)
	for i := 0; i < trafficWindowSecs; i++ {
		sec := now - int64(trafficWindowSecs-1-i)
		out[i] = tr.bySec[sec]
	}
	tr.mu.Unlock()

	if out[trafficWindowSecs-1] != 25 {
		t.Fatalf("newest bucket want 25 got %d (%v)", out[trafficWindowSecs-1], out)
	}
	if out[trafficWindowSecs-2] != 50 {
		t.Fatalf("prev bucket want 50 got %d", out[trafficWindowSecs-2])
	}
	if out[trafficWindowSecs-3] != 100 {
		t.Fatalf("older bucket want 100 got %d", out[trafficWindowSecs-3])
	}
}
