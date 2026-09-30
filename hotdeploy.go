// Package hotredeploy implements in-process hot application redeployment.
//
// Each application keeps at most one active instance. Publishing a release
// accepts it immediately and returns a per-application monotonic sequence
// number; startup happens asynchronously. A successfully started qualified
// candidate takes over new traffic while the previous active instance drains
// in-flight requests and is released exactly once.
package hotredeploy

import (
	"context"
	"sync"
	"time"
)

// DefaultDrainTimeout is used when WithDrainTimeout is not provided.
const DefaultDrainTimeout = 30 * time.Second

// Version is an upper-layer validated application version identifier.
type Version string

// Request is an opaque application request.
type Request any

// Response is an opaque application response.
type Response any

// StartFunc starts an application instance. It may block for a long time and
// may fail. A cancelled ctx means the instance has lost its qualification and
// should stop starting.
type StartFunc func(ctx context.Context) error

// ServeFunc handles one request on a started instance.
type ServeFunc func(ctx context.Context, req Request) (Response, error)

// ReleaseFunc releases all resources held by an instance.
type ReleaseFunc func()

// AppHooks bundles the lifecycle callbacks supplied with one release.
type AppHooks struct {
	Start   StartFunc
	Serve   ServeFunc
	Release ReleaseFunc
}

// ReleaseResult identifies an accepted release.
type ReleaseResult struct {
	Seq     uint64
	Version Version
}

// InstanceState labels the lifecycle state of one released version.
type InstanceState string

const (
	// StateCandidate is accepted and may still take over.
	StateCandidate InstanceState = "candidate"
	// StateSuperseded is an accepted candidate invalidated by a newer release.
	StateSuperseded InstanceState = "superseded"
	// StateActive is the current serving instance.
	StateActive InstanceState = "active"
	// StateDraining has been replaced; in-flight requests are finishing.
	StateDraining InstanceState = "draining"
)

// ReleaseState is a point-in-time view of one released version.
type ReleaseState struct {
	Seq           uint64
	Version       Version
	State         InstanceState
	InFlight      int
	DrainDeadline time.Time
}

// AppState is a point-in-time view of one application.
type AppState struct {
	App        string
	Active     *ReleaseState
	Candidates []ReleaseState
	Draining   []ReleaseState
}

// Option configures a Manager.
type Option func(*config)

type config struct {
	drainTimeout time.Duration
}

// WithDrainTimeout sets the drain timeout. Non-positive values keep the default.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.drainTimeout = d
		}
	}
}

// Manager drives hot redeployments across named applications.
//
// All state is in-memory: there is no persistence.
type Manager struct {
	cfg config

	mu     sync.Mutex
	apps   map[string]*appRegistry
	closed bool
}

// NewManager creates a Manager.
func NewManager(opts ...Option) *Manager {
	cfg := config{drainTimeout: DefaultDrainTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Manager{cfg: cfg, apps: make(map[string]*appRegistry)}
}

// app returns the registry for app, creating it under lock.
func (m *Manager) appLocked(name string) *appRegistry {
	a, ok := m.apps[name]
	if !ok {
		a = newApp(name, m.cfg.drainTimeout)
		m.apps[name] = a
	}
	return a
}

// Publish accepts a new release for app and immediately returns its sequence.
// Startup is driven asynchronously; the returned sequence is granted before
// startup completes.
func (m *Manager) Publish(app string, version Version, hooks AppHooks) (ReleaseResult, error) {
	m.mu.Lock()
	closed := m.closed
	var a *appRegistry
	if !closed {
		a = m.appLocked(app)
	}
	m.mu.Unlock()
	if closed {
		return ReleaseResult{}, ErrClosed
	}
	return a.publish(version, hooks)
}

// Serve dispatches one request to the current active instance of app. Before
// the first release is ready it returns ErrUnavailable. A request is handed to
// exactly one instance and is never retried or re-dispatched.
func (m *Manager) Serve(ctx context.Context, app string, req Request) (Response, error) {
	m.mu.Lock()
	a, ok := m.apps[app]
	m.mu.Unlock()
	if !ok {
		return nil, ErrUnavailable
	}
	return a.serve(ctx, req)
}

// State returns the current state of app. The second result is false when the
// application has never had a release.
func (m *Manager) State(app string) (AppState, bool) {
	m.mu.Lock()
	a, ok := m.apps[app]
	m.mu.Unlock()
	if !ok {
		return AppState{}, false
	}
	return a.state(), true
}

// Close stops accepting new releases and requests and releases every
// successfully started instance. In-flight requests are not awaited; their
// callers receive ErrDrainTimeout. Close is idempotent.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	apps := m.apps
	m.mu.Unlock()

	for _, a := range apps {
		a.close()
	}
	return nil
}
