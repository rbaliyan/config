package config

import "strings"

// SystemNamespacePrefix is the reserved prefix for internal system namespaces.
// Namespaces starting with this prefix are used by infrastructure components
// (e.g., key rotation, service discovery) and should not be written by clients.
const SystemNamespacePrefix = "internal:"

// IsSystemNamespace returns true if the namespace is reserved for internal use.
// A system namespace must start with the "internal:" prefix.
func IsSystemNamespace(namespace string) bool {
	return strings.HasPrefix(namespace, SystemNamespacePrefix)
}
