//go:build unix

package config

import (
	"fmt"
	"syscall"
)

// Lock attempts to lock the secret's memory pages so they are not swapped to
// disk. Requires sufficient OS privileges (typically CAP_IPC_LOCK on Linux).
// Returns nil if the secret is zero.
func (s *Secret) Lock() error {
	if s == nil || len(s.v) == 0 {
		return nil
	}
	if err := syscall.Mlock(s.v); err != nil {
		return fmt.Errorf("config: secret mlock: %w", err)
	}
	return nil
}

// Unlock releases the mlock on the secret's memory pages.
// Call Unlock before Wipe if Lock was previously called.
// Returns nil if the secret is zero.
func (s *Secret) Unlock() error {
	if s == nil || len(s.v) == 0 {
		return nil
	}
	if err := syscall.Munlock(s.v); err != nil {
		return fmt.Errorf("config: secret munlock: %w", err)
	}
	return nil
}
