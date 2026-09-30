package hotredeploy

import "errors"

// ErrUnavailable is returned when no active instance can serve a request yet.
var ErrUnavailable = errors.New("hotredeploy: no active instance")

// ErrDrainTimeout is returned when an in-flight request is still running when
// its instance reaches the drain deadline.
var ErrDrainTimeout = errors.New("hotredeploy: drain timeout")

// ErrClosed is returned after the Manager has been closed.
var ErrClosed = errors.New("hotredeploy: manager closed")
