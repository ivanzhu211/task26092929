package hotredeploy

import (
	"context"
	"sync"
	"time"
)

type serveOutcome struct {
	resp Response
	err  error
}

// serveToken guarantees a request receives exactly one terminal outcome.
type serveToken struct {
	done chan serveOutcome
}

// instance tracks one accepted release of one application.
//
// All field access is guarded by mu. Lifecycle transitions driven by the
// owning appRegistry (candidate -> active -> draining) happen while that
// registry holds its own lock; instance methods never call back into the
// registry while holding mu (see finishDrain) to keep lock ordering one way:
// registry lock -> instance lock.
type instance struct {
	app     string
	version Version
	seq     uint64
	hooks   AppHooks

	// startCtx is cancelled when the candidate is superseded by a newer
	// release. A well-behaved StartFunc can abort startup promptly.
	startCtx    context.Context
	startCancel context.CancelFunc

	mu          sync.Mutex
	state       InstanceState
	inFlight    int
	pending     map[*serveToken]struct{}
	drainUntil  time.Time
	drainTimer  *time.Timer
	released    bool
	releaseOnce sync.Once

	// onRelease lets the instance ask its registry to forget it. Invoked
	// without holding mu.
	onRelease func(*instance)
}

func newInstance(app string, seq uint64, version Version, hooks AppHooks) *instance {
	ctx, cancel := context.WithCancel(context.Background())
	return &instance{
		app:         app,
		version:     version,
		seq:         seq,
		hooks:       hooks,
		startCtx:    ctx,
		startCancel: cancel,
		state:       StateCandidate,
		pending:     make(map[*serveToken]struct{}),
	}
}

func (in *instance) setStateLocked(s InstanceState) {
	in.state = s
}

// invalidate marks a not-yet-qualified candidate as permanently disqualified
// and signals startup to abort.
func (in *instance) invalidate() {
	in.mu.Lock()
	if in.state == StateCandidate {
		in.state = StateSuperseded
	}
	in.mu.Unlock()
	in.startCancel()
}

// activate promotes the qualified candidate to active.
func (in *instance) activate() {
	in.mu.Lock()
	in.state = StateActive
	in.mu.Unlock()
}

// beginDrain moves the instance into draining with an independent deadline.
// It reports whether the instance was already empty and thus released
// immediately.
func (in *instance) beginDrain(deadline time.Time) (empty bool) {
	in.mu.Lock()
	in.state = StateDraining
	in.drainUntil = deadline
	if in.inFlight == 0 {
		in.released = true
		in.mu.Unlock()
		go in.afterDrain()
		return true
	}
	d := time.Until(deadline)
	in.drainTimer = time.AfterFunc(d, in.drainTimedOut)
	in.mu.Unlock()
	return false
}

// beginServe attaches one request to the instance and launches its handler.
// Exactly one terminal outcome is delivered on the returned token.
func (in *instance) beginServe(req Request) *serveToken {
	tok := &serveToken{done: make(chan serveOutcome, 1)}

	in.mu.Lock()
	if in.released {
		in.mu.Unlock()
		return nil
	}
	in.inFlight++
	in.pending[tok] = struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	in.mu.Unlock()

	go func() {
		resp, err := in.hooks.Serve(ctx, req)
		cancel()

		var finishNow bool
		in.mu.Lock()
		in.inFlight--
		_, waiting := in.pending[tok]
		delete(in.pending, tok)
		if waiting {
			tok.done <- serveOutcome{resp: resp, err: err}
		}
		if in.state == StateDraining && in.inFlight == 0 && !in.released {
			in.released = true
			if in.drainTimer != nil {
				in.drainTimer.Stop()
			}
			finishNow = true
		}
		in.mu.Unlock()

		if finishNow {
			in.afterDrain()
		}
	}()

	return tok
}

// detach is called when the caller stops waiting (context cancellation) so a
// late handler result is never delivered. An already-delivered drain-timeout
// outcome wins and is returned to the caller.
func (in *instance) detach(tok *serveToken) (serveOutcome, bool) {
	in.mu.Lock()
	delete(in.pending, tok)
	select {
	case o := <-tok.done:
		in.mu.Unlock()
		return o, true
	default:
	}
	in.mu.Unlock()
	return serveOutcome{}, false
}

// drainTimedOut forces the drain deadline: every still-waiting caller gets
// ErrDrainTimeout, the instance is released without waiting for handlers.
func (in *instance) drainTimedOut() {
	in.mu.Lock()
	if in.released {
		in.mu.Unlock()
		return
	}
	in.released = true
	pending := in.pending
	in.pending = make(map[*serveToken]struct{})
	for tok := range pending {
		tok.done <- serveOutcome{err: ErrDrainTimeout}
	}
	in.mu.Unlock()

	in.afterDrain()
}

// afterDrain releases instance resources exactly once and asks the registry
// to forget the instance. Always called without holding in.mu.
func (in *instance) afterDrain() {
	in.releaseOnce.Do(func() {
		if in.hooks.Release != nil {
			in.hooks.Release()
		}
	})
	if in.onRelease != nil {
		in.onRelease(in)
	}
}

// releaseSuperseded releases a candidate that lost qualification but finished
// startup successfully. Startup failures release nothing.
func (in *instance) releaseSuperseded() {
	in.mu.Lock()
	if in.released {
		in.mu.Unlock()
		return
	}
	in.released = true
	in.mu.Unlock()
	in.startCancel()
	in.afterDrain()
}

// forceRelease immediately shuts down a successfully started instance without
// waiting for in-flight requests. Waiting callers receive ErrDrainTimeout and
// resources are released exactly once. It behaves like an immediately reached
// drain deadline.
func (in *instance) forceRelease() {
	in.mu.Lock()
	if in.state != StateDraining {
		in.state = StateDraining
	}
	if in.drainUntil.IsZero() {
		in.drainUntil = time.Now()
	}
	if in.drainTimer != nil {
		in.drainTimer.Stop()
	}
	in.mu.Unlock()
	in.drainTimedOut()
}

// snapshot copies observable state.
func (in *instance) snapshot() ReleaseState {
	in.mu.Lock()
	defer in.mu.Unlock()
	return ReleaseState{
		Seq:           in.seq,
		Version:       in.version,
		State:         in.state,
		InFlight:      in.inFlight,
		DrainDeadline: in.drainUntil,
	}
}
