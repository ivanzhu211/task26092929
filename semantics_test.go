package hotredeploy

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Sequence numbers increase per application, and different applications are
// independent.
func TestSequencePerApp(t *testing.T) {
	m := NewManager(WithDrainTimeout(50 * time.Millisecond))
	defer m.Close()

	okHooks := func() AppHooks {
		return AppHooks{
			Start: func(context.Context) error { return nil },
			Serve: func(_ context.Context, r Request) (Response, error) { return r, nil },
		}
	}

	r, err := m.Publish("a", "1", okHooks())
	if err != nil || r.Seq != 1 {
		t.Fatalf("a1: %+v %v", r, err)
	}
	r, err = m.Publish("a", "2", okHooks())
	if err != nil || r.Seq != 2 {
		t.Fatalf("a2: %+v %v", r, err)
	}
	r, err = m.Publish("b", "1", okHooks())
	if err != nil || r.Seq != 1 {
		t.Fatalf("b1: %+v %v", r, err)
	}

	if _, ok := m.State("never"); ok {
		t.Fatal("unknown app should report ok=false")
	}
	if _, err := m.Serve(context.Background(), "never", "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unknown app serve: %v", err)
	}
}

// A replaced active instance with no in-flight requests is released
// immediately without waiting for the drain deadline.
func TestDrainEmptyReleasedImmediately(t *testing.T) {
	m := NewManager(WithDrainTimeout(30 * time.Second))
	defer m.Close()

	v1 := &controllableApp{}
	v2 := &controllableApp{}

	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v1", 2*time.Second)
	if _, err := m.Publish("app", "v2", v2.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v2", 2*time.Second)

	waitReleases(t, &v1.releaseCount, 1, 2*time.Second)
	st, _ := m.State("app")
	if len(st.Draining) != 0 {
		t.Fatalf("empty instance should never appear in draining: %+v", st.Draining)
	}
}

// If the only candidate fails startup, the active version is kept. A later
// successful release then takes over normally.
func TestCandidateFailureKeepsActive(t *testing.T) {
	m := NewManager(WithDrainTimeout(time.Second))
	defer m.Close()

	v1 := &controllableApp{}
	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v1", 2*time.Second)

	failReady := make(chan struct{})
	bad := &controllableApp{start: func(context.Context) error {
		<-failReady
		return errors.New("nope")
	}}
	if _, err := m.Publish("app", "bad", bad.hooks()); err != nil {
		t.Fatal(err)
	}
	close(failReady)

	time.Sleep(100 * time.Millisecond)
	st, _ := m.State("app")
	if st.Active == nil || st.Active.Version != "v1" {
		t.Fatalf("active after failed candidate: %+v", st.Active)
	}
	if len(st.Candidates) != 0 {
		t.Fatalf("failed candidate should be removed: %+v", st.Candidates)
	}
	if got := bad.releaseCount.Load(); got != 0 {
		t.Fatalf("failed candidate released %d times", got)
	}

	v2 := &controllableApp{serve: func(context.Context, Request) (Response, error) {
		return "v2", nil
	}}
	if _, err := m.Publish("app", "v2", v2.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v2", 2*time.Second)
	waitReleases(t, &v1.releaseCount, 1, 2*time.Second)
}

// Close releases the active instance and any draining instances, and reports
// drain timeout to in-flight callers.
func TestCloseReleases(t *testing.T) {
	m := NewManager(WithDrainTimeout(time.Hour))

	entered := make(chan struct{})
	enterOnce := make(chan struct{})
	block := make(chan struct{})
	v1 := &controllableApp{serve: func(context.Context, Request) (Response, error) {
		select {
		case enterOnce <- struct{}{}:
		default:
		}
		<-entered
		<-block
		return "v1", nil
	}}
	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v1", 2*time.Second)
	out := dispatchAsync(m, "x")
	select {
	case <-enterOnce:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never entered")
	}
	close(entered)

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-out:
		if !errors.Is(o.err, ErrDrainTimeout) {
			t.Fatalf("outcome = %+v, want drain timeout", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not terminate in-flight request")
	}
	waitReleases(t, &v1.releaseCount, 1, 2*time.Second)
	close(block)

	if _, err := m.Publish("app", "x", v1.hooks()); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}
