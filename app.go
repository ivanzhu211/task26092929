package hotredeploy

import (
	"context"
	"sync"
	"time"
)

// appRegistry holds all in-memory state for one application.
//
// At most one instance is active. Candidates that lose qualification before
// starting are moved to superseded and removed when their startup settles.
// Replaced active instances drain concurrently with independent deadlines.
type appRegistry struct {
	name         string
	drainTimeout time.Duration

	mu         sync.Mutex
	seq        uint64
	candidate  *instance
	active     *instance
	draining   []*instance
	superseded []*instance
	closed     bool
}

func newApp(name string, drainTimeout time.Duration) *appRegistry {
	return &appRegistry{name: name, drainTimeout: drainTimeout}
}

func (a *appRegistry) publish(version Version, hooks AppHooks) (ReleaseResult, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ReleaseResult{}, ErrClosed
	}
	a.seq++
	seq := a.seq

	if a.candidate != nil {
		old := a.candidate
		a.candidate = nil
		a.superseded = append(a.superseded, old)
		old.invalidate()
	}

	inst := newInstance(a.name, seq, version, hooks)
	inst.onRelease = a.forget
	a.candidate = inst
	a.mu.Unlock()

	go a.runStartup(inst)

	return ReleaseResult{Seq: seq, Version: version}, nil
}

// runStartup drives StartFunc outside the registry lock.
func (a *appRegistry) runStartup(inst *instance) {
	err := inst.hooks.Start(inst.startCtx)
	if err != nil {
		a.startupFailed(inst)
		return
	}
	a.startupSucceeded(inst)
}

func (a *appRegistry) startupFailed(inst *instance) {
	a.mu.Lock()
	a.candidate = a.removeIfCandidate(inst)
	a.superseded = removeInstance(a.superseded, inst)
	a.mu.Unlock()
}

func (a *appRegistry) startupSucceeded(inst *instance) {
	a.mu.Lock()
	if a.closed {
		a.candidate = a.removeIfCandidate(inst)
		a.superseded = removeInstance(a.superseded, inst)
		a.mu.Unlock()
		inst.releaseSuperseded()
		return
	}

	if a.candidate == inst {
		a.candidate = nil
		prev := a.active
		a.active = inst
		inst.activate()
		a.mu.Unlock()

		if prev != nil {
			deadline := time.Now().Add(a.drainTimeout)
			if !prev.beginDrain(deadline) {
				a.mu.Lock()
				a.draining = append(a.draining, prev)
				a.mu.Unlock()
			}
		}
		return
	}

	a.superseded = removeInstance(a.superseded, inst)
	a.mu.Unlock()
	inst.releaseSuperseded()
}

func (a *appRegistry) serve(ctx context.Context, req Request) (Response, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ErrClosed
	}
	active := a.active
	a.mu.Unlock()
	if active == nil {
		return nil, ErrUnavailable
	}

	tok := active.beginServe(req)
	if tok == nil {
		return nil, ErrUnavailable
	}

	select {
	case o := <-tok.done:
		return o.resp, o.err
	case <-ctx.Done():
		if o, ok := active.detach(tok); ok {
			return o.resp, o.err
		}
		return nil, ctx.Err()
	}
}

// close stops accepting work and releases every successfully started
// instance. Candidates still starting are cancelled; they release nothing if
// startup fails and are released if a late startup success arrives.
func (a *appRegistry) close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true

	pendingCandidate := a.candidate
	a.candidate = nil

	var toRelease []*instance
	if a.active != nil {
		toRelease = append(toRelease, a.active)
		a.active = nil
	}
	for _, inst := range a.draining {
		toRelease = append(toRelease, inst)
	}
	a.draining = nil
	a.mu.Unlock()

	if pendingCandidate != nil {
		pendingCandidate.invalidate()
	}
	for _, inst := range toRelease {
		inst.forceRelease()
	}
}

// forget removes a fully released instance from whichever list holds it.
func (a *appRegistry) forget(inst *instance) {
	a.mu.Lock()
	a.draining = removeInstance(a.draining, inst)
	a.superseded = removeInstance(a.superseded, inst)
	if a.candidate == inst {
		a.candidate = nil
	}
	if a.active == inst {
		a.active = nil
	}
	a.mu.Unlock()
}

func (a *appRegistry) removeIfCandidate(inst *instance) *instance {
	if a.candidate == inst {
		return nil
	}
	return a.candidate
}

func (a *appRegistry) state() AppState {
	a.mu.Lock()
	defer a.mu.Unlock()

	st := AppState{App: a.name}
	if a.active != nil {
		s := a.active.snapshot()
		st.Active = &s
	}
	if a.candidate != nil {
		st.Candidates = append(st.Candidates, a.candidate.snapshot())
	}
	for _, inst := range a.superseded {
		st.Candidates = append(st.Candidates, inst.snapshot())
	}
	for _, inst := range a.draining {
		st.Draining = append(st.Draining, inst.snapshot())
	}
	return st
}

func removeInstance(list []*instance, target *instance) []*instance {
	for i, inst := range list {
		if inst == target {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
