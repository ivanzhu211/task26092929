package hotdeploy

import (
	"context"
	"errors"
	"time"
)

// Launcher creates a running instance for a given validated version.
//
// Start may be slow or fail. It must return only after the instance is ready
// to serve requests, or with a non-nil error.
type Launcher[RQ any, RS any] interface {
	Start(ctx context.Context, version string) (Instance[RQ, RS], error)
}

// Instance is one running version of an application.
//
// Handle may block for a long time. Release frees the instance's resources and
// must be called exactly once for every instance that started successfully.
type Instance[RQ any, RS any] interface {
	Handle(ctx context.Context, req RQ) (RS, error)
	Release()
}

// LauncherFunc adapts a plain function into a Launcher.
type LauncherFunc[RQ any, RS any] func(ctx context.Context, version string) (Instance[RQ, RS], error)

func (f LauncherFunc[RQ, RS]) Start(ctx context.Context, version string) (Instance[RQ, RS], error) {
	return f(ctx, version)
}

// RevisionState identifies the lifecycle state of a published revision.
type RevisionState int

const (
	// StateCandidate: accepted and possibly still starting, not yet in control.
	StateCandidate RevisionState = iota
	// StateActive: currently serving all new requests.
	StateActive
	// StateDraining: superseded; in-flight requests are finishing.
	StateDraining
	// StateSucceeded: drained or never activated but released normally.
	StateSucceeded
	// StateFailed: candidate startup failed; no instance was created.
	StateFailed
	// StateInvalidated: a newer publish superseded it before it took over;
	// it can never become active.
	StateInvalidated
)

func (s RevisionState) String() string {
	switch s {
	case StateCandidate:
		return "candidate"
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	case StateSucceeded:
		return "succeeded"
	case StateFailed:
		return "failed"
	case StateInvalidated:
		return "invalidated"
	default:
		return "unknown"
	}
}

// RevisionInfo is a snapshot of one published revision.
type RevisionInfo struct {
	Seq     int
	Version string
	State   RevisionState
}

// Status is a point-in-time snapshot of an application.
type Status struct {
	Active    *RevisionInfo
	Candidate *RevisionInfo
	Draining  []RevisionInfo
	History   []RevisionInfo
}

// Config tunes a Manager.
type Config struct {
	// DrainTimeout is the upper bound a superseded revision keeps serving
	// in-flight requests before it is force-released. Defaults to 30s.
	DrainTimeout time.Duration
}

// Sentinel errors returned by Serve.
var (
	// ErrUnavailable means no active revision exists yet (or the manager is
	// closed).
	ErrUnavailable = errors.New("hotdeploy: service unavailable")
	// ErrDrainTimeout means an in-flight request outlived the drain window of
	// its revision.
	ErrDrainTimeout = errors.New("hotdeploy: drain timeout")
)
