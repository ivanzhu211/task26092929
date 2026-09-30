package hotredeploy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeApp struct {
	mu        sync.Mutex
	releases  []string
	serveBody func(ctx context.Context, req Request) (Response, error)
}

func (f *fakeApp) start(ctx context.Context) error { return nil }
func (f *fakeApp) serve(ctx context.Context, req Request) (Response, error) {
	f.mu.Lock()
	body := f.serveBody
	f.mu.Unlock()
	if body != nil {
		return body(ctx, req)
	}
	return req, nil
}
func (f *fakeApp) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
}

func TestSmokeTakeover(t *testing.T) {
	m := NewManager(WithDrainTimeout(200 * time.Millisecond))
	defer m.Close()

	v1 := &fakeApp{}
	h1 := AppHooks{Start: v1.start, Serve: v1.serve, Release: v1.release}
	r1, err := m.Publish("app", "v1", h1)
	if err != nil || r1.Seq != 1 {
		t.Fatalf("publish v1: %+v %v", r1, err)
	}

	if _, err := m.Serve(context.Background(), "app", "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("before ready: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := m.Serve(context.Background(), "app", "hello")
		if err == nil {
			if resp != "hello" {
				t.Fatalf("resp %v", resp)
			}
			break
		}
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}

	st, _ := m.State("app")
	if st.Active == nil || st.Active.Version != "v1" {
		t.Fatalf("state: %+v", st)
	}
}
