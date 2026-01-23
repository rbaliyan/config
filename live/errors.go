package live

import "errors"

var (
	// ErrInvalidTarget is returned when the target is not a pointer to a struct.
	ErrInvalidTarget = errors.New("live: target must be a pointer to a struct")

	// ErrBindingStopped is returned when attempting to reload a stopped binding.
	ErrBindingStopped = errors.New("live: binding has been stopped")
)
