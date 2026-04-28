//go:build unix

package config

import "syscall"

// Lock attempts to lock the secret's memory pages so they are not swapped to
// disk. Requires sufficient OS privileges (typically CAP_IPC_LOCK on Linux).
// Returns nil if the secret is zero.
func (s *Secret) Lock() error {
	if len(s.v) == 0 {
		return nil
	}
	return syscall.Mlock(s.v)
}

// Unlock releases the mlock on the secret's memory pages.
// Call Unlock before Wipe if Lock was previously called.
// Returns nil if the secret is zero.
func (s *Secret) Unlock() error {
	if len(s.v) == 0 {
		return nil
	}
	return syscall.Munlock(s.v)
}
