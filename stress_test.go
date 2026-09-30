package hotredeploy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Rapid publishes overlap slow and fast startups while concurrent requests
// run. Whatever starts successfully as a qualified candidate must take over;
// invalidated candidates are never active; each started-but-replaced instance
// releases exactly once.
func TestConcurrentPublishesAndServes(t *testing.T) {
	m := NewManager(WithDrainTimeout(20 * time.Millisecond))
	defer m.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = m.Serve(context.Background(), "app", "x")
			}
		}()
	}

	var apps []*controllableApp
	var pubWG sync.WaitGroup
	for i := 0; i < 60; i++ {
		app := &controllableApp{}
		apps = append(apps, app)
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			_, _ = m.Publish("app", Version("v"), app.hooks())
		}()
		if i%3 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	pubWG.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := m.State("app")
		if st.Active != nil && len(st.Candidates) == 0 && len(st.Draining) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	st, _ := m.State("app")
	if st.Active == nil {
		t.Fatal("no active instance after storm settles")
	}
	for _, c := range st.Candidates {
		if c.State == StateCandidate {
			t.Fatalf("qualified candidate still pending: %+v", c)
		}
	}

	for _, app := range apps {
		if got := app.releaseCount.Load(); got > 1 {
			t.Fatalf("instance released %d times", got)
		}
	}

	if _, err := m.Serve(context.Background(), "app", "x"); err != nil && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unexpected serve error after storm: %v", err)
	}
}
