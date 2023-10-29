package mr

import (
	"time"
)

type options struct {
	timeout time.Duration
}

type timeoutOption time.Duration

func (t timeoutOption) apply(o *options) {
	o.timeout = time.Duration(t)
}

// WithTimeout returns an Options object that sets the timeout duration for some operation.
//
// timeout: the duration for the timeout.
// Returns: an Options object that can be used to configure some operation.
func WithTimeout(timeout time.Duration) Options {
	return timeoutOption(timeout)
}

type Options interface {
	apply(o *options)
}
