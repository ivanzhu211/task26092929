package hotdeploy

import (
	"context"
	"sync/atomic"
)

type startResult struct {
	inst *fakeInstance
	err  error
}

type startCall struct {
	version string
	done    chan startResult
}

// fakeLauncher blocks each Start until the test feeds a result through the
// returned call, so startup completion order is fully controllable.
type fakeLauncher struct {
	starts chan startCall
}

func newFakeLauncher() *fakeLauncher {
	return &fakeLauncher{starts: make(chan startCall, 64)}
}

func (l *fakeLauncher) Start(_ context.Context, version string) (Instance[int, int], error) {
	done := make(chan startResult, 1)
	l.starts <- startCall{version: version, done: done}
	r := <-done
	if r.err != nil {
		return nil, r.err
	}
	return r.inst, nil
}

// inflight is one Handle invocation under test control.
type inflight struct {
	version string
	finish  chan serveResult[int]
}

// fakeInstance blocks each Handle until the test finishes it.
type fakeInstance struct {
	version  string
	releases atomic.Int32
	handles  chan *inflight
}

func newFakeInstance(version string) *fakeInstance {
	return &fakeInstance{
		version: version,
		handles: make(chan *inflight, 64),
	}
}

func (i *fakeInstance) Handle(_ context.Context, req int) (int, error) {
	in := &inflight{version: i.version, finish: make(chan serveResult[int], 1)}
	i.handles <- in
	r := <-in.finish
	return r.rs, r.err
}

func (i *fakeInstance) Release() {
	i.releases.Add(1)
}
