package hotredeploy_test

import (
	"context"
	"fmt"
	"time"

	"hotredeploy"
)

type echoInstance struct{ version string }

func (e *echoInstance) start(context.Context) error { return nil }

func (e *echoInstance) serve(_ context.Context, req hotredeploy.Request) (hotredeploy.Response, error) {
	return fmt.Sprintf("%s:%v", e.version, req), nil
}

func (e *echoInstance) release() {}

func Example() {
	m := hotredeploy.NewManager(hotredeploy.WithDrainTimeout(100 * time.Millisecond))
	defer m.Close()

	for _, version := range []hotredeploy.Version{"v1", "v2"} {
		inst := &echoInstance{version: string(version)}
		r, err := m.Publish("checkout", version, hotredeploy.AppHooks{
			Start:   inst.start,
			Serve:   inst.serve,
			Release: inst.release,
		})
		if err != nil {
			panic(err)
		}
		fmt.Println("accepted", r.Seq, r.Version)
	}

	// Wait briefly for the asynchronous startups to settle in this example;
	// real services simply keep calling Serve.
	time.Sleep(20 * time.Millisecond)
	st, _ := m.State("checkout")
	fmt.Println("active", st.Active.Version)

	resp, err := m.Serve(context.Background(), "checkout", "order-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
	// Output:
	// accepted 1 v1
	// accepted 2 v2
	// active v2
	// v2:order-42
}
