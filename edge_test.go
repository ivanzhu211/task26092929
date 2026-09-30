package hotdeploy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUnavailableBeforeFirstReady(t *testing.T) {
	l := newFakeLauncher()
	m := New[int, int](l, Config{})
	defer m.Close()

	m.Publish("v1")

	// Startup still pending: requests fail fast.
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := m.Serve(context.Background(), 1)
			done <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-done; !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Serve err = %v, want ErrUnavailable", err)
		}
	}
}

func TestImmediateDrainWithNoInflight(t *testing.T) {
	l := newFakeLauncher()
	m := New[int, int](l, Config{DrainTimeout: 5 * time.Second})
	defer m.Close()

	m.Publish("v1")
	instV1 := mustSucceedStart(t, m, l, "v1")
	m.Publish("v2")
	instV2 := mustSucceedStart(t, m, l, "v2")

	// v1 had no in-flight requests: it must be released well before timeout.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && instV1.releases.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := instV1.releases.Load(); got != 1 {
		t.Fatalf("v1 releases = %d, want 1 (immediate, not timeout)", got)
	}
	if got := instV2.releases.Load(); got != 0 {
		t.Fatalf("v2 releases = %d, want 0 while active", got)
	}
	if st := m.Status(); st.Active == nil || st.Active.Version != "v2" {
		t.Fatalf("active = %+v, want v2", st.Active)
	}
}

func TestCloseReleasesActiveOnce(t *testing.T) {
	l := newFakeLauncher()
	m := New[int, int](l, Config{DrainTimeout: 5 * time.Second})

	m.Publish("v1")
	instV1 := mustSucceedStart(t, m, l, "v1")

	m.Close()
	m.Close() // idempotent

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && instV1.releases.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := instV1.releases.Load(); got != 1 {
		t.Fatalf("v1 releases = %d, want exactly 1", got)
	}
	if _, err := m.Serve(context.Background(), 1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Serve after Close err = %v, want ErrUnavailable", err)
	}
	if seq := m.Publish("late"); seq != -1 {
		t.Fatalf("Publish after Close = %d, want -1", seq)
	}
}

func TestDefaultDrainTimeout(t *testing.T) {
	m := New[int, int](newFakeLauncher(), Config{})
	if m.drainTimeout != defaultDrainTimeout {
		t.Fatalf("drainTimeout = %v, want %v", m.drainTimeout, defaultDrainTimeout)
	}
}
