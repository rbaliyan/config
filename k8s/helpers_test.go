package k8s

import "testing"

func TestConfigKeyToK8sKey_RoundTrip(t *testing.T) {
	cases := []struct {
		config string
		k8s    string
	}{
		{"simple", "simple"},
		{"a/b", "a.b"},
		{"deep/nested/key", "deep.nested.key"},
		{"", ""},
	}
	for _, c := range cases {
		if got := configKeyToK8sKey(c.config); got != c.k8s {
			t.Errorf("configKeyToK8sKey(%q) = %q, want %q", c.config, got, c.k8s)
		}
		if got := k8sKeyToConfigKey(c.k8s); got != c.config {
			t.Errorf("k8sKeyToConfigKey(%q) = %q, want %q", c.k8s, got, c.config)
		}
	}
}

func TestIsSecretKey(t *testing.T) {
	cases := []struct {
		key, prefix string
		want        bool
	}{
		{"secret/db-password", "secret/", true},
		{"config/host", "secret/", false},
		{"", "secret/", false},
		{"secret/", "secret/", true},
		{"any", "", false}, // empty prefix disables secret routing
	}
	for _, c := range cases {
		if got := isSecretKey(c.key, c.prefix); got != c.want {
			t.Errorf("isSecretKey(%q, %q) = %v, want %v", c.key, c.prefix, got, c.want)
		}
	}
}

func TestResourceNames(t *testing.T) {
	if got := configMapResourceName("ns1"); got != "config-ns1" {
		t.Errorf("configMapResourceName: %q", got)
	}
	if got := secretResourceName("ns1"); got != "config-secrets-ns1" {
		t.Errorf("secretResourceName: %q", got)
	}
}

func TestResolveCodec(t *testing.T) {
	anns := map[string]string{
		codecAnnotationKey("mykey"): "yaml",
	}
	if got := resolveCodec(anns, "mykey"); got != "yaml" {
		t.Errorf("expected yaml, got %q", got)
	}
	if got := resolveCodec(anns, "other"); got != "json" {
		t.Errorf("expected json fallback, got %q", got)
	}
	if got := resolveCodec(nil, "x"); got != "json" {
		t.Errorf("expected json fallback on nil anns, got %q", got)
	}
}

func TestParseResourceVersion(t *testing.T) {
	if got := parseResourceVersion(""); got != 0 {
		t.Errorf("empty should return 0, got %d", got)
	}
	if got := parseResourceVersion("123"); got != 123 {
		t.Errorf("123 expected, got %d", got)
	}
	if got := parseResourceVersion("abc"); got != 0 {
		t.Errorf("non-numeric should return 0, got %d", got)
	}
}

func TestStore_ConfigNamespace(t *testing.T) {
	s := &Store{}
	if got := s.configNamespace("any", "config-prod"); got != "prod" {
		t.Errorf("got %q", got)
	}
	if got := s.configNamespace("any", "config-secrets-prod"); got != "prod" {
		t.Errorf("got %q", got)
	}
	if got := s.configNamespace("any", "unrelated-cm"); got != "" {
		t.Errorf("unmanaged name should return empty, got %q", got)
	}
}

func TestStore_K8sNamespace(t *testing.T) {
	// When k8sNamespace is empty, config namespace is used directly.
	s := &Store{opts: storeOptions{}}
	if got := s.k8sNamespace("prod"); got != "prod" {
		t.Errorf("got %q", got)
	}
	if got := s.k8sNamespace(""); got != "default" {
		t.Errorf("empty config ns should map to default, got %q", got)
	}

	// When k8sNamespace is set, it overrides.
	s = &Store{opts: storeOptions{k8sNamespace: "kube-system"}}
	if got := s.k8sNamespace("prod"); got != "kube-system" {
		t.Errorf("got %q", got)
	}
}

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	if o.secretPrefix != defaultSecretPrefix {
		t.Errorf("secretPrefix: %q", o.secretPrefix)
	}
	if o.watchBufSize != defaultWatchBufSize {
		t.Errorf("watchBufSize: %d", o.watchBufSize)
	}
	if o.resyncPeriod != defaultResyncPeriod {
		t.Errorf("resyncPeriod: %v", o.resyncPeriod)
	}
}

func TestOptions(t *testing.T) {
	o := defaultOptions()
	WithK8sNamespace("myns")(&o)
	if o.k8sNamespace != "myns" {
		t.Error("k8sNamespace not set")
	}
	WithSecretKeyPrefix("sec/")(&o)
	if o.secretPrefix != "sec/" {
		t.Error("secretPrefix not set")
	}
	WithResyncPeriod(0)(&o)
	if o.resyncPeriod != defaultResyncPeriod {
		t.Error("zero resync should be ignored")
	}
	WithWatchBufferSize(-1)(&o)
	if o.watchBufSize != defaultWatchBufSize {
		t.Error("negative bufSize should be ignored")
	}
	WithWatchBufferSize(250)(&o)
	if o.watchBufSize != 250 {
		t.Error("bufSize not updated")
	}
}
