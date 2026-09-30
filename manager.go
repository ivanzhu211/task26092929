package hotdeploy

import (
	"context"
	"sort"
	"sync"
	"time"
)

const defaultDrainTimeout = 30 * time.Second

type serveResult[RS any] struct {
	rs  RS
	err error
}

// Manager drives hot redeployment for a single application. All state is held
// in memory. At most one revision is active at any time.
type Manager[RQ any, RS any] struct {
	launcher     Launcher[RQ, RS]
	drainTimeout time.Duration

	mu        sync.Mutex
	closed    bool
	nextSeq   int
	active    *revision[RQ, RS]
	candidate *revision[RQ, RS]
	draining  map[*revision[RQ, RS]]struct{}
	revisions map[int]*revision[RQ, RS]
}

type revision[RQ any, RS any] struct {
	seq     int
	version string

	state RevisionState

	instance   Instance[RQ, RS]
	release    sync.Once
	forceClose sync.Once

	// inFlight is the number of requests handed to this revision whose Handle
	// call has not returned yet.
	inFlight int
	// zeroInFlight is closed once inFlight reaches zero while draining.
	zeroInFlight chan struct{}
	// forceDone is closed when the drain window expires (or the manager
	// closes), unblocking in-flight Serve calls.
	forceDone chan struct{}
	// deadline is the drain deadline; zero for non-draining revisions.
	deadline time.Time
}

// New creates a Manager. A zero Config uses the 30s default drain timeout.
func New[RQ any, RS any](launcher Launcher[RQ, RS], cfg Config) *Manager[RQ, RS] {
	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}
	return &Manager[RQ, RS]{
		launcher:     launcher,
		drainTimeout: drainTimeout,
		draining:     make(map[*revision[RQ, RS]]struct{}),
		revisions:    make(map[int]*revision[RQ, RS]),
	}
}

// Publish accepts a new release and returns its application-scoped, monotonic
// sequence number immediately. It never waits for startup. Publishing
// permanently disqualifies any candidate that had not taken over yet.
//
// It returns -1 if the manager has been closed.
func (m *Manager[RQ, RS]) Publish(version string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return -1
	}

	m.nextSeq++
	rev := &revision[RQ, RS]{
		seq:          m.nextSeq,
		version:      version,
		state:        StateCandidate,
		zeroInFlight: make(chan struct{}),
		forceDone:    make(chan struct{}),
	}
	m.revisions[rev.seq] = rev

	if m.candidate != nil {
		m.candidate.state = StateInvalidated
	}
	m.candidate = rev

	go m.launchAndReport(rev)
	return rev.seq
}

func (m *Manager[RQ, RS]) launchAndReport(rev *revision[RQ, RS]) {
	inst, err := m.launcher.Start(context.Background(), rev.version)
	m.onStarted(rev, inst, err)
}

// onStarted applies a startup result according to the revision's current
// eligibility. It is the single place the active revision can change due to a
// startup completion.
func (m *Manager[RQ, RS]) onStarted(rev *revision[RQ, RS], inst Instance[RQ, RS], err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		// The manager is gone; any instance that still came up must be freed.
		m.releaseInstance(inst)
		return
	}

	switch rev.state {
	case StateCandidate:
		if m.candidate != rev {
			m.releaseInstance(inst)
			return
		}
		if err != nil {
			rev.state = StateFailed
			m.candidate = nil
			return
		}
		rev.instance = inst
		rev.state = StateActive
		m.candidate = nil
		previous := m.active
		m.active = rev
		if previous != nil {
			m.beginDrainLocked(previous)
		}

	case StateInvalidated:
		// It lost eligibility before becoming ready; a late success can never
		// take over, so the instance is released straight away.
		if err != nil {
			return
		}
		rev.instance = inst
		rev.state = StateSucceeded
		m.doReleaseLocked(rev)
	}
}

// doReleaseLocked releases the revision's instance at most once, off the lock.
func (m *Manager[RQ, RS]) doReleaseLocked(rev *revision[RQ, RS]) {
	inst := rev.instance
	rev.release.Do(func() {
		if inst != nil {
			go inst.Release()
		}
	})
}

func (m *Manager[RQ, RS]) releaseInstance(inst Instance[RQ, RS]) {
	if inst != nil {
		go inst.Release()
	}
}

// Serve dispatches one request to exactly one revision: the active one at the
// time of dispatch. It returns ErrUnavailable when no revision is ready.
//
// A request is never transferred or retried. If its revision is superseded and
// the drain window expires first, Serve returns ErrDrainTimeout; a handler
// result arriving afterwards is silently dropped.
func (m *Manager[RQ, RS]) Serve(ctx context.Context, req RQ) (RS, error) {
	var zero RS

	m.mu.Lock()
	if m.closed || m.active == nil {
		m.mu.Unlock()
		return zero, ErrUnavailable
	}
	rev := m.active
	rev.inFlight++
	m.mu.Unlock()

	done := make(chan serveResult[RS], 1)
	go func() {
		rs, err := rev.instance.Handle(ctx, req)
		m.finishRequest(rev)
		done <- serveResult[RS]{rs: rs, err: err}
	}()

	select {
	case r := <-done:
		return r.rs, r.err
	case <-rev.forceDone:
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return zero, ErrUnavailable
		}
		return zero, ErrDrainTimeout
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// finishRequest accounts for a Handle call that actually returned. A late
// completion after force-release merely updates the counter.
func (m *Manager[RQ, RS]) finishRequest(rev *revision[RQ, RS]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rev.inFlight--
	if rev.state == StateDraining && rev.inFlight == 0 {
		close(rev.zeroInFlight)
	}
}

// beginDrainLocked moves an active revision into draining and arms its
// independent deadline.
func (m *Manager[RQ, RS]) beginDrainLocked(rev *revision[RQ, RS]) {
	rev.state = StateDraining
	rev.deadline = time.Now().Add(m.drainTimeout)
	m.draining[rev] = struct{}{}
	if rev.inFlight == 0 {
		close(rev.zeroInFlight)
	}
	go m.watchDrain(rev, rev.deadline)
}

// watchDrain finalizes a draining revision exactly once: as soon as all
// in-flight requests finish, or when the independent drain deadline expires.
func (m *Manager[RQ, RS]) watchDrain(rev *revision[RQ, RS], deadline time.Time) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-rev.zeroInFlight:
		m.completeDrain(rev, false)
	case <-timer.C:
		m.completeDrain(rev, true)
	}
}

// completeDrain releases a draining revision. On timeout it first unblocks
// waiting Serve calls; the instance is released whether or not handlers have
// returned.
func (m *Manager[RQ, RS]) completeDrain(rev *revision[RQ, RS], timeout bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || rev.state != StateDraining {
		return
	}
	if timeout {
		m.signalForceClosedLocked(rev)
	}
	rev.state = StateSucceeded
	delete(m.draining, rev)
	m.doReleaseLocked(rev)
}

// signalForceClosedLocked closes the forceDone channel at most once.
func (m *Manager[RQ, RS]) signalForceClosedLocked(rev *revision[RQ, RS]) {
	rev.forceClose.Do(func() { close(rev.forceDone) })
}

func info[RQ any, RS any](rev *revision[RQ, RS]) RevisionInfo {
	return RevisionInfo{Seq: rev.seq, Version: rev.version, State: rev.state}
}

// Status returns a point-in-time snapshot distinguishing active, candidate and
// draining revisions. History lists every accepted revision in publish order.
func (m *Manager[RQ, RS]) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()

	st := Status{}
	if m.active != nil {
		active := info(m.active)
		st.Active = &active
	}
	if m.candidate != nil {
		candidate := info(m.candidate)
		st.Candidate = &candidate
	}
	for rev := range m.draining {
		st.Draining = append(st.Draining, info(rev))
	}
	sort.Slice(st.Draining, func(i, j int) bool { return st.Draining[i].Seq < st.Draining[j].Seq })

	for seq := 1; seq <= m.nextSeq; seq++ {
		if rev, ok := m.revisions[seq]; ok {
			st.History = append(st.History, info(rev))
		}
	}
	return st
}

// Close marks the manager closed, unblocks in-flight Serve calls and releases
// every existing instance exactly once. Instances whose startup has not
// returned yet are released by the startup path when it completes.
func (m *Manager[RQ, RS]) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for _, rev := range m.revisions {
		m.signalForceClosedLocked(rev)
		if rev.instance != nil {
			m.doReleaseLocked(rev)
		}
	}
	m.active = nil
	m.candidate = nil
	m.draining = nil
	m.mu.Unlock()
}
