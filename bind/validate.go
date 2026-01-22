package bind

import (
	"errors"
	"fmt"
)

// Validator validates configuration values.
type Validator interface {
	// Validate checks if the value is valid.
	// Returns nil if valid, or an error describing the validation failure.
	Validate(v any) error
}

// ValidatorFunc is a function adapter for Validator.
type ValidatorFunc func(v any) error

// Validate implements the Validator interface.
func (f ValidatorFunc) Validate(v any) error {
	return f(v)
}

// Sentinel errors for validation
var (
	ErrValidationFailed = errors.New("config: validation failed")
	ErrBindingFailed    = errors.New("config: binding failed")
	ErrInvalidSchema    = errors.New("config: invalid schema")
)

// ValidationError represents a validation failure.
type ValidationError struct {
	Key    string // Config key
	Field  string // Struct field (if applicable)
	Value  any    // The value that failed validation
	Reason string // Human-readable reason
	Err    error  // Underlying error
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation failed for %q field %q: %s", e.Key, e.Field, e.reason())
	}
	if e.Key != "" {
		return fmt.Sprintf("validation failed for %q: %s", e.Key, e.reason())
	}
	return fmt.Sprintf("validation failed: %s", e.reason())
}

func (e *ValidationError) reason() string {
	if e.Reason != "" {
		return e.Reason
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "unknown error"
}

func (e *ValidationError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrValidationFailed
}

// BindError represents a binding/unmarshaling failure.
type BindError struct {
	Key string // Config key
	Op  string // Operation (marshal, unmarshal)
	Err error  // Underlying error
}

func (e *BindError) Error() string {
	return fmt.Sprintf("binding failed for %q during %s: %v", e.Key, e.Op, e.Err)
}

func (e *BindError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrBindingFailed
}

// IsValidationError returns true if err is a ValidationError.
func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// IsBindError returns true if err is a BindError.
func IsBindError(err error) bool {
	var be *BindError
	return errors.As(err, &be)
}

// ChainValidators creates a validator that runs multiple validators in sequence.
// Validation stops at the first error.
func ChainValidators(validators ...Validator) Validator {
	return ValidatorFunc(func(v any) error {
		for _, validator := range validators {
			if err := validator.Validate(v); err != nil {
				return err
			}
		}
		return nil
	})
}
