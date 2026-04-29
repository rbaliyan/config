//go:build windows

package config

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Lock attempts to lock the secret's memory pages using VirtualLock so they
// are not swapped to disk. Requires the SE_LOCK_MEMORY_NAME privilege.
// Returns nil if the secret is zero.
func (s *Secret) Lock() error {
	if s == nil || len(s.v) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(s.v)))
	if err := windows.VirtualLock(addr, uintptr(len(s.v))); err != nil {
		return fmt.Errorf("config: secret VirtualLock: %w", err)
	}
	return nil
}

// Unlock releases the VirtualLock on the secret's memory pages.
// Call Unlock before Wipe if Lock was previously called.
// Returns nil if the secret is zero.
func (s *Secret) Unlock() error {
	if s == nil || len(s.v) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(s.v)))
	if err := windows.VirtualUnlock(addr, uintptr(len(s.v))); err != nil {
		return fmt.Errorf("config: secret VirtualUnlock: %w", err)
	}
	return nil
}
