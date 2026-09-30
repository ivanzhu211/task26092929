package hotredeploy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type controllableApp struct {
	start func(ctx context.Context) error
	serve func(ctx context.Context, req Request) (Response, error)

	releaseCount atomic.Int64
}

func (c *controllableApp) hooks() AppHooks {
	start := c.start
	if start == nil {
		start = func(context.Context) error { return nil }
	}
	serve := c.serve
	if serve == nil {
		serve = func(_ context.Context, req Request) (Response, error) { return req, nil }
	}
	return AppHooks{
		Start:   start,
		Serve:   serve,
		Release: func() { c.releaseCount.Add(1) },
	}
}

func waitActive(t *testing.T, m *Manager, app string, want Version, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		st, ok := m.State(app)
		if ok && st.Active != nil && st.Active.Version == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ := m.State(app)
	t.Fatalf("active never became %q: %+v", want, st)
}

func waitReleases(t *testing.T, n *atomic.Int64, want int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if n.Load() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("releases = %d, want %d", n.Load(), want)
}

// Scenario 1: v1 is serving. v2 and v3 are then accepted back-to-back, so v2
// loses qualification the instant v3 is accepted. Startup results arrive in
// reverse order: the newest candidate (v3) fails and v2 later starts
// successfully. v1 must keep serving, no invalidated candidate becomes
// active, and v2's late successful startup only leads to release.
func TestReverseStartupOrderNewestFails(t *testing.T) {
	m := NewManager(WithDrainTimeout(time.Second))
	defer m.Close()

	v1Ready := make(chan struct{})
	v2Done := make(chan struct{})
	v3Done := make(chan struct{})

	v1 := &controllableApp{start: func(ctx context.Context) error {
		<-v1Ready
		return nil
	}}
	v2 := &controllableApp{start: func(ctx context.Context) error {
		<-v2Done
		return nil
	}}
	v3 := &controllableApp{start: func(ctx context.Context) error {
		<-v3Done
		return errors.New("boom")
	}}

	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Serve(context.Background(), "app", "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
	close(v1Ready)
	waitActive(t, m, "app", "v1", 2*time.Second)

	// Two new releases accepted before either settles.
	r2, err := m.Publish("app", "v2", v2.hooks())
	if err != nil || r2.Seq != 2 {
		t.Fatalf("publish v2: %+v %v", r2, err)
	}
	r3, err := m.Publish("app", "v3", v3.hooks())
	if err != nil || r3.Seq != 3 {
		t.Fatalf("publish v3: %+v %v", r3, err)
	}

	// Active keeps serving through candidate startup.
	if _, err := m.Serve(context.Background(), "app", "live"); err != nil {
		t.Fatalf("active should keep serving: %v", err)
	}

	// Reverse arrival: newest fails first, invalidated v2 succeeds after.
	close(v3Done)
	close(v2Done)

	// Give startup callbacks time to settle; active must remain v1.
	time.Sleep(100 * time.Millisecond)
	st, _ := m.State("app")
	if st.Active == nil || st.Active.Version != "v1" {
		t.Fatalf("active changed: %+v", st.Active)
	}
	for _, cand := range st.Candidates {
		if cand.State == StateCandidate {
			t.Fatalf("invalidated candidate must not remain qualified: %+v", cand)
		}
	}

	// v2 started successfully after losing qualification: release once.
	waitReleases(t, &v2.releaseCount, 1, 2*time.Second)
	// v3 failed startup: nothing to release.
	if got := v3.releaseCount.Load(); got != 0 {
		t.Fatalf("failed instance released %d times, want 0", got)
	}
	// v1 is still active: not released.
	if got := v1.releaseCount.Load(); got != 0 {
		t.Fatalf("active v1 released %d times", got)
	}

	resp, err := m.Serve(context.Background(), "app", "still-live")
	if err != nil || resp != "still-live" {
		t.Fatalf("serve after settlement: %v %v", resp, err)
	}
}
