package hotdeploy_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	hotdeploy "hotdeploy"
)

// appInstance is a tiny stand-in for a real process-bound application version.
type appInstance struct {
	version string
}

func (a *appInstance) Handle(_ context.Context, req string) (string, error) {
	return a.version + ":" + req, nil
}

func (a *appInstance) Release() {}

// This example shows accepting two releases, waiting for the first to become
// active, and observing the new version take over new requests.
func ExampleManager() {
	var mu sync.Mutex
	versions := map[string]*appInstance{}

	launcher := hotdeploy.LauncherFunc[string, string](
		func(_ context.Context, version string) (hotdeploy.Instance[string, string], error) {
			time.Sleep(5 * time.Millisecond) // simulate slow startup
			mu.Lock()
			inst := &appInstance{version: version}
			versions[version] = inst
			mu.Unlock()
			return inst, nil
		},
	)

	m := hotdeploy.New[string, string](launcher, hotdeploy.Config{
		DrainTimeout: 20 * time.Millisecond,
	})
	defer m.Close()

	seq1 := m.Publish("v1")
	seq2 := m.Publish("v2")
	fmt.Println("accepted:", seq1, seq2)

	// Publish returns before startup; poll for takeover in real code you would
	// observe readiness via Status() or traffic shape.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if st := m.Status(); st.Active != nil && st.Active.Version == "v2" {
			break
		}
		time.Sleep(time.Millisecond)
	}

	rs, err := m.Serve(context.Background(), "ping")
	fmt.Println("served:", rs, err)

	st := m.Status()
	fmt.Println("active:", st.Active.Version, "candidate:", st.Candidate)
	// Output:
	// accepted: 1 2
	// served: v2:ping <nil>
	// active: v2 candidate: <nil>
}
