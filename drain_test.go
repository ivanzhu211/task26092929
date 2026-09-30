package hotredeploy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingApp struct {
	name    Version
	block   chan struct{}
	entered chan struct{}
	enter   sync.Once

	releaseCount atomic.Int64
	finishes     atomic.Int64
}

func newBlockingApp(name Version, block chan struct{}) *blockingApp {
	return &blockingApp{name: name, block: block, entered: make(chan struct{})}
}

func (b *blockingApp) hooks() AppHooks {
	name := b.name
	block := b.block
	return AppHooks{
		Start: func(context.Context) error { return nil },
		Serve: func(ctx context.Context, req Request) (Response, error) {
			b.enter.Do(func() { close(b.entered) })
			select {
			case <-block:
			case <-ctx.Done():
				// Ignore the signal: the handler keeps running past the drain
				// deadline to prove late results are never delivered.
				<-block
			}
			b.finishes.Add(1)
			return name, nil
		},
		Release: func() { b.releaseCount.Add(1) },
	}
}

type serveResult struct {
	resp Response
	err  error
}

func dispatchAsync(m *Manager, req Request) <-chan serveResult {
	ch := make(chan serveResult, 1)
	go func() {
		resp, err := m.Serve(context.Background(), "app", req)
		ch <- serveResult{resp: resp, err: err}
	}()
	return ch
}

func waitChan(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// Scenario 2: v1 and v2 both drain simultaneously after v3 takes over, each
// carrying one long-running request with independent deadlines. The requests
// hit the deadline while handlers ignore cancellation and finish late: every
// caller gets exactly one terminal outcome (ErrDrainTimeout), each replaced
// instance is released exactly once, and the current active v3 is unaffected.
func TestMultipleDrainsTimeoutEachOneOutcome(t *testing.T) {
	const drainTimeout = 120 * time.Millisecond
	m := NewManager(WithDrainTimeout(drainTimeout))
	defer m.Close()

	block := make(chan struct{})
	v1 := newBlockingApp("v1", block)
	v2 := newBlockingApp("v2", block)
	v3 := &controllableApp{serve: func(context.Context, Request) (Response, error) {
		return Version("v3"), nil
	}} // active v3: requests return immediately

	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v1", 2*time.Second)

	r1 := dispatchAsync(m, "req1")
	waitChan(t, v1.entered, "req1 to enter v1")

	if _, err := m.Publish("app", "v2", v2.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v2", 2*time.Second)

	r2 := dispatchAsync(m, "req2")
	waitChan(t, v2.entered, "req2 to enter v2")

	if _, err := m.Publish("app", "v3", v3.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v3", 2*time.Second)

	// Two old versions drain at once with independent deadlines.
	st, _ := m.State("app")
	versions := map[Version]bool{}
	var deadlines []time.Time
	for _, d := range st.Draining {
		versions[d.Version] = true
		deadlines = append(deadlines, d.DrainDeadline)
		if d.InFlight != 1 {
			t.Fatalf("%s in-flight = %d, want 1", d.Version, d.InFlight)
		}
	}
	if !versions["v1"] || !versions["v2"] {
		t.Fatalf("expected v1 and v2 draining, got %+v", st.Draining)
	}
	if len(deadlines) != 2 || deadlines[0].IsZero() || deadlines[1].IsZero() {
		t.Fatalf("each draining version needs an independent deadline: %v", deadlines)
	}

	// New traffic goes only to v3.
	resp, err := m.Serve(context.Background(), "app", "new")
	if err != nil || resp != Version("v3") {
		t.Fatalf("new request = %v %v, want v3", resp, err)
	}

	// Both old requests time out with exactly one terminal outcome.
	select {
	case o := <-r1:
		if !errors.Is(o.err, ErrDrainTimeout) {
			t.Fatalf("req1 outcome = %+v, want drain timeout", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("req1 never got a terminal outcome")
	}
	select {
	case o := <-r2:
		if !errors.Is(o.err, ErrDrainTimeout) {
			t.Fatalf("req2 outcome = %+v, want drain timeout", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("req2 never got a terminal outcome")
	}

	waitReleases(t, &v1.releaseCount, 1, 2*time.Second)
	waitReleases(t, &v2.releaseCount, 1, 2*time.Second)
	if got := v3.releaseCount.Load(); got != 0 {
		t.Fatalf("active v3 released %d times", got)
	}

	// Active remains v3 after all drain events.
	st, _ = m.State("app")
	if st.Active == nil || st.Active.Version != "v3" {
		t.Fatalf("active changed after drain: %+v", st.Active)
	}
	if len(st.Draining) != 0 {
		t.Fatalf("draining list not cleaned: %+v", st.Draining)
	}

	// Let the late handlers finish: their results must not be delivered and
	// releases must remain exactly once.
	close(block)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (v1.finishes.Load() < 1 || v2.finishes.Load() < 1) {
		time.Sleep(2 * time.Millisecond)
	}
	if v1.finishes.Load() < 1 || v2.finishes.Load() < 1 {
		t.Fatalf("handlers did not finish: v1=%d v2=%d", v1.finishes.Load(), v2.finishes.Load())
	}

	select {
	case extra := <-r1:
		t.Fatalf("late result delivered to req1: %+v", extra)
	default:
	}
	select {
	case extra := <-r2:
		t.Fatalf("late result delivered to req2: %+v", extra)
	default:
	}
	time.Sleep(50 * time.Millisecond)
	if got := v1.releaseCount.Load(); got != 1 {
		t.Fatalf("v1 released %d times, want 1", got)
	}
	if got := v2.releaseCount.Load(); got != 1 {
		t.Fatalf("v2 released %d times, want 1", got)
	}
}
