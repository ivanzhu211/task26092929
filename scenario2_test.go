package hotdeploy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func mustSucceedStart(t *testing.T, m *Manager[int, int], l *fakeLauncher, version string) *fakeInstance {
	t.Helper()
	c := recvStart(t, l, version)
	inst := newFakeInstance(version)
	c.done <- startResult{inst: inst}
	waitForActive(t, m, version)
	return inst
}

type serveOutcome struct {
	rs  int
	err error
}

func serveAsync(m *Manager[int, int], req int) <-chan serveOutcome {
	out := make(chan serveOutcome, 1)
	go func() {
		rs, err := m.Serve(context.Background(), req)
		out <- serveOutcome{rs: rs, err: err}
	}()
	return out
}

// Scenario 2: several old revisions drain at once. One finishes its in-flight
// request normally and is released immediately; another exceeds its drain
// window, the request receives exactly one terminal ErrDrainTimeout, and the
// instance is released once regardless of the late handler result. The current
// active revision is unaffected.
func TestMultipleDrainsCompletionAndTimeout(t *testing.T) {
	l := newFakeLauncher()
	m := New[int, int](l, Config{DrainTimeout: 400 * time.Millisecond})
	defer m.Close()

	m.Publish("v1")
	instV1 := mustSucceedStart(t, m, l, "v1")

	r1 := serveAsync(m, 1)
	h1 := <-instV1.handles

	m.Publish("v2")
	instV2 := mustSucceedStart(t, m, l, "v2")

	r2 := serveAsync(m, 2)
	h2 := <-instV2.handles

	m.Publish("v3")
	instV3 := mustSucceedStart(t, m, l, "v3")

	// v1 and v2 now drain simultaneously with independent deadlines.
	st := m.Status()
	if st.Active == nil || st.Active.Version != "v3" {
		t.Fatalf("active = %+v, want v3", st.Active)
	}
	if len(st.Draining) != 2 {
		t.Fatalf("draining = %d, want 2: %+v", len(st.Draining), st.Draining)
	}

	// Old request completes normally: single success outcome, immediate release.
	h1.finish <- serveResult[int]{rs: 11}
	select {
	case o := <-r1:
		if o.err != nil || o.rs != 11 {
			t.Fatalf("r1 outcome = %+v, want 11/nil", o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("r1 never returned")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && instV1.releases.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := instV1.releases.Load(); got != 1 {
		t.Fatalf("v1 releases = %d, want 1", got)
	}

	// v2 outlives its own deadline: one timeout outcome, one release.
	select {
	case o := <-r2:
		if !errors.Is(o.err, ErrDrainTimeout) {
			t.Fatalf("r2 outcome = %+v, want ErrDrainTimeout", o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("r2 never timed out")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && instV2.releases.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := instV2.releases.Load(); got != 1 {
		t.Fatalf("v2 releases = %d, want 1", got)
	}

	// Late handler result must not be delivered again or change anything.
	select {
	case o := <-r2:
		t.Fatalf("r2 received a second outcome: %+v", o)
	default:
	}
	h2.finish <- serveResult[int]{rs: 999}
	time.Sleep(50 * time.Millisecond)
	if got := instV2.releases.Load(); got != 1 {
		t.Fatalf("v2 releases after late result = %d, still want 1", got)
	}

	// Active revision unaffected and still serves.
	r3 := serveAsync(m, 3)
	h3 := <-instV3.handles
	if h3.version != "v3" {
		t.Fatalf("request routed to %q, want v3", h3.version)
	}
	h3.finish <- serveResult[int]{rs: 33}
	select {
	case o := <-r3:
		if o.err != nil || o.rs != 33 {
			t.Fatalf("r3 outcome = %+v, want 33/nil", o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("r3 never returned")
	}

	st = m.Status()
	if st.Active == nil || st.Active.Version != "v3" {
		t.Fatalf("active = %+v, want v3", st.Active)
	}
	if len(st.Draining) != 0 {
		t.Fatalf("draining = %+v, want empty", st.Draining)
	}
	if got := instV1.releases.Load(); got != 1 {
		t.Fatalf("v1 final releases = %d, want 1", got)
	}
	if got := instV2.releases.Load(); got != 1 {
		t.Fatalf("v2 final releases = %d, want 1", got)
	}
	if got := instV3.releases.Load(); got != 0 {
		t.Fatalf("v3 releases before Close = %d, want 0", got)
	}
}
