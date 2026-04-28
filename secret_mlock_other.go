//go:build windows

package config

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// Lock attempts to lock the secret's memory pages using VirtualLock so they
// are not swapped to disk. Requires the SE_LOCK_MEMORY_NAME privilege.
// Returns nil if the secret is zero.
func (s *Secret) Lock() error {
	if len(s.v) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(s.v)))
	return windows.VirtualLock(addr, uintptr(len(s.v)))
}

// Unlock releases the VirtualLock on the secret's memory pages.
// Call Unlock before Wipe if Lock was previously called.
// Returns nil if the secret is zero.
func (s *Secret) Unlock() error {
	if len(s.v) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(s.v)))
	return windows.VirtualUnlock(addr, uintptr(len(s.v)))
}
