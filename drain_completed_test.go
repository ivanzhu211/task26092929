package hotredeploy

import (
	"context"
	"testing"
	"time"
)

// A draining instance whose in-flight requests all finish before the deadline
// is released immediately (not at the deadline) and the request receives its
// normal result. The new active instance keeps serving.
func TestDrainReleasedWhenRequestsComplete(t *testing.T) {
	m := NewManager(WithDrainTimeout(5 * time.Second))
	defer m.Close()

	started := make(chan struct{})
	releaseReq := make(chan struct{})
	enterOnce := make(chan struct{})
	var closeOnce chan struct{}
	_ = closeOnce

	v1 := &controllableApp{
		start: func(ctx context.Context) error { close(started); return nil },
		serve: func(ctx context.Context, req Request) (Response, error) {
			select {
			case enterOnce <- struct{}{}:
			default:
			}
			<-releaseReq
			return "v1-result", nil
		},
	}
	v2 := &controllableApp{serve: func(context.Context, Request) (Response, error) {
		return "v2-result", nil
	}}

	if _, err := m.Publish("app", "v1", v1.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v1", 2*time.Second)

	out := dispatchAsync(m, "old")
	select {
	case <-enterOnce:
	case <-time.After(2 * time.Second):
		t.Fatal("request never entered v1 handler")
	}

	if _, err := m.Publish("app", "v2", v2.hooks()); err != nil {
		t.Fatal(err)
	}
	waitActive(t, m, "app", "v2", 2*time.Second)

	st, _ := m.State("app")
	if len(st.Draining) != 1 || st.Draining[0].Version != "v1" || st.Draining[0].InFlight != 1 {
		t.Fatalf("unexpected drain state: %+v", st.Draining)
	}

	// Handler finishes well before the 5s deadline.
	close(releaseReq)
	select {
	case o := <-out:
		if o.err != nil || o.resp != "v1-result" {
			t.Fatalf("old request outcome = %+v", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old request never returned")
	}

	waitReleases(t, &v1.releaseCount, 1, 2*time.Second)
	if got := v2.releaseCount.Load(); got != 0 {
		t.Fatalf("active v2 released %d times", got)
	}

	st, _ = m.State("app")
	if st.Active == nil || st.Active.Version != "v2" {
		t.Fatalf("active = %+v", st.Active)
	}
	if len(st.Draining) != 0 {
		t.Fatalf("draining not cleaned: %+v", st.Draining)
	}

	resp, err := m.Serve(context.Background(), "app", "new")
	if err != nil || resp != "v2-result" {
		t.Fatalf("new request = %v %v", resp, err)
	}
}
