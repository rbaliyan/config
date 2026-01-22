package config

import (
	"context"
)

type configContextKey int

const (
	configContextManagerKey configContextKey = iota
	configContextNamespaceKey
)

// ContextWithManager adds a Manager to the context.
// This allows handlers to access configuration without explicit dependency injection.
func ContextWithManager(ctx context.Context, mgr Manager) context.Context {
	return context.WithValue(ctx, configContextManagerKey, mgr)
}

// ContextManager retrieves the Manager from context.
// Returns nil if no manager is set.
func ContextManager(ctx context.Context) Manager {
	mgr, ok := ctx.Value(configContextManagerKey).(Manager)
	if !ok {
		return nil
	}
	return mgr
}

// ContextWithNamespace adds a namespace to the context.
// Operations will use this namespace.
func ContextWithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, configContextNamespaceKey, namespace)
}

// ContextNamespace retrieves the namespace from context.
// Returns empty string if no namespace is set.
func ContextNamespace(ctx context.Context) string {
	ns, ok := ctx.Value(configContextNamespaceKey).(string)
	if !ok {
		return ""
	}
	return ns
}

// Get is a convenience function that retrieves a value using the Manager from context.
// Returns ErrManagerClosed if no manager is in context.
// Uses the namespace from context, or "" (default) if not set.
func Get(ctx context.Context, key string) (Value, error) {
	mgr := ContextManager(ctx)
	if mgr == nil {
		return nil, ErrManagerClosed
	}

	ns := ContextNamespace(ctx)
	return mgr.Namespace(ns).Get(ctx, key)
}

// Set is a convenience function that sets a value using the Manager from context.
// Returns ErrManagerClosed if no manager is in context.
// Uses the namespace from context, or "" (default) if not set.
func Set(ctx context.Context, key string, value any, opts ...SetOption) error {
	mgr := ContextManager(ctx)
	if mgr == nil {
		return ErrManagerClosed
	}

	ns := ContextNamespace(ctx)
	return mgr.Namespace(ns).Set(ctx, key, value, opts...)
}
