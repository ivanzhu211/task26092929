package hotdeploy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitForActive(t *testing.T, m *Manager[int, int], wantVersion string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := m.Status(); st.Active != nil && st.Active.Version == wantVersion {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active revision never became %q: %+v", wantVersion, m.Status())
}

func recvStart(t *testing.T, l *fakeLauncher, wantVersion string) startCall {
	t.Helper()
	select {
	case c := <-l.starts:
		if c.version != wantVersion {
			t.Fatalf("start order: got %q want %q", c.version, wantVersion)
		}
		return c
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for Start(%q)", wantVersion)
	}
	return startCall{}
}

// Scenario 1: several candidates are pending; startup results arrive in
// reverse order and the newest candidate fails. The active revision stays in
// place and invalidated candidates must never serve.
func TestReverseStartupNewestFails(t *testing.T) {
	l := newFakeLauncher()
	m := New[int, int](l, Config{DrainTimeout: time.Second})
	defer m.Close()

	m.Publish("v1")
	startV1 := recvStart(t, l, "v1")
	instV1 := newFakeInstance("v1")
	startV1.done <- startResult{inst: instV1}
	waitForActive(t, m, "v1")

	// Two more publishes while v1 is active: v2 loses to v3 immediately.
	seq2 := m.Publish("v2")
	startV2 := recvStart(t, l, "v2")
	seq3 := m.Publish("v3")
	startV3 := recvStart(t, l, "v3")
	if seq2 != 2 || seq3 != 3 {
		t.Fatalf("seqs = %d,%d, want 2,3", seq2, seq3)
	}

	// Results arrive in reverse order: newest fails first.
	startV3.done <- startResult{err: errors.New("boom")}

	// v1 must keep serving while startup results are still outstanding.
	go func() {
		rs, err := m.Serve(context.Background(), 7)
		if err != nil {
			t.Errorf("Serve on v1: %v", err)
			return
		}
		if rs != 70 {
			t.Errorf("Serve rs = %d, want 70", rs)
		}
	}()
	h1 := <-instV1.handles
	h1.finish <- serveResult[int]{rs: 70}

	// Late success of the invalidated v2: release only, never activated.
	instV2 := newFakeInstance("v2")
	startV2.done <- startResult{inst: instV2}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if instV2.releases.Load() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := instV2.releases.Load(); got != 1 {
		t.Fatalf("invalidated v2 releases = %d, want exactly 1", got)
	}
	if got := instV1.releases.Load(); got != 0 {
		t.Fatalf("active v1 released %d times, want 0", got)
	}

	st := m.Status()
	if st.Active == nil || st.Active.Version != "v1" {
		t.Fatalf("active = %+v, want v1", st.Active)
	}
	if st.Candidate != nil {
		t.Fatalf("candidate = %+v, want nil", st.Candidate)
	}

	stateBySeq := map[int]RevisionState{}
	for _, h := range st.History {
		stateBySeq[h.Seq] = h.State
	}
	if stateBySeq[2] != StateSucceeded {
		t.Errorf("v2 state = %v, want succeeded (late-start released)", stateBySeq[2])
	}
	if stateBySeq[3] != StateFailed {
		t.Errorf("v3 state = %v, want failed", stateBySeq[3])
	}
}
