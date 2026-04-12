package config

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// AliasManager provides runtime management of key aliases.
//
// Aliases allow transparent key migration by mapping old key names to new
// canonical keys. All operations (Get, Set, Delete) automatically resolve
// aliases to their target keys.
//
// When the underlying store implements [AliasStore], aliases are persisted and
// propagated to all connected Manager instances via Watch. When the store does
// not implement AliasStore, aliases are held in memory only (useful for testing).
//
// Aliases are global (not per-namespace) and single-hop: if A is aliased to B,
// accessing A resolves to B. Chain aliases (A→B→C) are not supported; update
// A's target directly if the canonical key changes.
//
// Use type assertion on Manager:
//
//	if am, ok := mgr.(config.AliasManager); ok {
//	    am.SetAlias(ctx, "old/key", "new/key")
//	}
type AliasManager interface {
	// SetAlias creates a new alias mapping from alias to target.
	// When the store implements [AliasStore], the alias is persisted and
	// propagated to other instances via Watch.
	//
	// Returns [ErrAliasExists] if the alias key is already registered as an
	// alias or exists as a configuration entry.
	// Returns [ErrAliasSelf] if alias equals target.
	// Returns [ErrAliasChain] if the mapping would create a chain.
	SetAlias(ctx context.Context, alias, target string) error

	// RemoveAlias removes an alias.
	// Returns [ErrNotFound] if the alias does not exist.
	RemoveAlias(ctx context.Context, alias string) error

	// ResolveAlias returns the canonical key for the given key.
	// If the key is not an alias, it is returned unchanged.
	ResolveAlias(key string) string

	// Aliases returns a snapshot of all registered aliases (alias → target).
	Aliases() map[string]string
}

// aliasResolver manages key aliases with thread-safe in-memory access.
// It is the fast-path for alias resolution during Get/Set/Delete operations.
// Persistence is handled by the Manager via [AliasStore].
type aliasResolver struct {
	mu      sync.RWMutex
	aliases map[string]string // alias key → target key
}

func newAliasResolver() *aliasResolver {
	return &aliasResolver{
		aliases: make(map[string]string),
	}
}

// resolve returns the canonical key for the given key.
func (r *aliasResolver) resolve(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if target, ok := r.aliases[key]; ok {
		return target
	}
	return key
}

// set adds or updates an alias mapping with chain validation.
func (r *aliasResolver) set(alias, target string) error {
	if alias == target {
		return fmt.Errorf("%w: %q", ErrAliasSelf, alias)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Target must not be an existing alias (prevents chains).
	if _, ok := r.aliases[target]; ok {
		return fmt.Errorf("%w: target %q is itself an alias", ErrAliasChain, target)
	}

	// Alias must not be the target of an existing alias (prevents chains).
	for existing, existingTarget := range r.aliases {
		if existingTarget == alias && existing != alias {
			return fmt.Errorf("%w: %q is already a target of alias %q", ErrAliasChain, alias, existing)
		}
	}

	r.aliases[alias] = target
	return nil
}

// setUnchecked adds an alias without chain validation.
// Used when applying watch events where the store is the source of truth.
func (r *aliasResolver) setUnchecked(alias, target string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.aliases[alias] = target
}

// remove removes an alias.
func (r *aliasResolver) remove(alias string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.aliases, alias)
}

// has reports whether the alias exists.
func (r *aliasResolver) has(alias string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.aliases[alias]
	return ok
}

// all returns a snapshot of all aliases.
func (r *aliasResolver) all() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]string, len(r.aliases))
	maps.Copy(result, r.aliases)
	return result
}
