package config

import "testing"

func TestIsSystemNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		want      bool
	}{
		{"internal:config:crypto", true},
		{"internal:", true},
		{"internal:anything", true},
		{"production", false},
		{"", false},
		{"internal", false},
		{"INTERNAL:", false},
		{"Internal:", false},
		{"xinternal:", false},
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			got := IsSystemNamespace(tt.namespace)
			if got != tt.want {
				t.Errorf("IsSystemNamespace(%q) = %v, want %v", tt.namespace, got, tt.want)
			}
		})
	}
}

func TestSystemNamespacePassesValidation(t *testing.T) {
	// System namespaces like "internal:config:crypto" must be valid namespaces.
	namespaces := []string{
		"internal:config:crypto",
		"internal:keys",
		"internal:discovery",
	}

	for _, ns := range namespaces {
		t.Run(ns, func(t *testing.T) {
			if err := ValidateNamespace(ns); err != nil {
				t.Errorf("ValidateNamespace(%q) = %v, want nil", ns, err)
			}
			if !IsSystemNamespace(ns) {
				t.Errorf("IsSystemNamespace(%q) = false, want true", ns)
			}
		})
	}
}
