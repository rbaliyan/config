package file

import (
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-viper/mapstructure/v2"
)

func mustRegister(t *testing.T, l *Loader, name string, cfg any) {
	t.Helper()
	if err := l.Register(name, cfg); err != nil {
		t.Fatalf("Register(%q) error: %v", name, err)
	}
}

// Test structs mimicking real-world compass config patterns.

type dbConfig struct {
	Host           string            `mapstructure:"host"`
	Port           int               `mapstructure:"port"`
	MaxPoolSize    int               `mapstructure:"max_pool_size"`
	ConnectTimeout time.Duration     `mapstructure:"connect_timeout"`
	Stores         map[string]string `mapstructure:"stores"`
}

type httpConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	Cors         []string      `mapstructure:"cors"`
}

type authConfig struct {
	Enabled   bool          `mapstructure:"enabled"`
	Provider  string        `mapstructure:"provider"`
	JWTExpiry time.Duration `mapstructure:"jwt_expiry"`
	Auth0     auth0Config   `mapstructure:"auth0"`
}

type auth0Config struct {
	Domain       string             `mapstructure:"domain"`
	ClientID     string             `mapstructure:"client_id"`
	EventStorage eventStorageConfig `mapstructure:"event_storage"`
}

type eventStorageConfig struct {
	Enabled        bool     `mapstructure:"enabled"`
	SkipEventTypes []string `mapstructure:"skip_event_types"`
}

type temporalConfig struct {
	Host              string        `mapstructure:"host"`
	Port              int           `mapstructure:"port"`
	Namespace         string        `mapstructure:"namespace"`
	TaskQueue         string        `mapstructure:"task_queue"`
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	Retry             retryConfig   `mapstructure:"retry"`
}

type retryConfig struct {
	InitialInterval    time.Duration `mapstructure:"initial_interval"`
	BackoffCoefficient float64       `mapstructure:"backoff_coefficient"`
	MaximumInterval    time.Duration `mapstructure:"maximum_interval"`
	MaximumAttempts    int           `mapstructure:"maximum_attempts"`
}

func TestLoader_YAML(t *testing.T) {
	var db dbConfig
	var http httpConfig
	var auth authConfig
	var temporal temporalConfig

	loader := New("testdata/config.yaml")
	mustRegister(t, loader, "db", &db)
	mustRegister(t, loader, "http", &http)
	mustRegister(t, loader, "auth", &auth)
	mustRegister(t, loader, "temporal", &temporal)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// DB config
	if db.Host != "localhost" {
		t.Errorf("db.Host = %q, want %q", db.Host, "localhost")
	}
	if db.Port != 5432 {
		t.Errorf("db.Port = %d, want %d", db.Port, 5432)
	}
	if db.MaxPoolSize != 50 {
		t.Errorf("db.MaxPoolSize = %d, want %d", db.MaxPoolSize, 50)
	}
	if db.ConnectTimeout != 10*time.Second {
		t.Errorf("db.ConnectTimeout = %v, want %v", db.ConnectTimeout, 10*time.Second)
	}
	if len(db.Stores) != 2 {
		t.Errorf("db.Stores len = %d, want 2", len(db.Stores))
	}
	if db.Stores["global"] != "mongodb://global:27017" {
		t.Errorf("db.Stores[global] = %q", db.Stores["global"])
	}

	// HTTP config
	if http.Host != "0.0.0.0" {
		t.Errorf("http.Host = %q, want %q", http.Host, "0.0.0.0")
	}
	if http.Port != 8080 {
		t.Errorf("http.Port = %d, want %d", http.Port, 8080)
	}
	if http.ReadTimeout != 600*time.Second {
		t.Errorf("http.ReadTimeout = %v, want %v", http.ReadTimeout, 600*time.Second)
	}
	if len(http.Cors) != 2 {
		t.Errorf("http.Cors len = %d, want 2", len(http.Cors))
	}

	// Auth config with nested structs
	if !auth.Enabled {
		t.Error("auth.Enabled = false, want true")
	}
	if auth.Provider != "auth0" {
		t.Errorf("auth.Provider = %q, want %q", auth.Provider, "auth0")
	}
	if auth.JWTExpiry != 24*time.Hour {
		t.Errorf("auth.JWTExpiry = %v, want %v", auth.JWTExpiry, 24*time.Hour)
	}
	if auth.Auth0.Domain != "dev.auth0.com" {
		t.Errorf("auth.Auth0.Domain = %q", auth.Auth0.Domain)
	}
	if auth.Auth0.ClientID != "test-client-id" {
		t.Errorf("auth.Auth0.ClientID = %q", auth.Auth0.ClientID)
	}
	if !auth.Auth0.EventStorage.Enabled {
		t.Error("auth.Auth0.EventStorage.Enabled = false")
	}
	if len(auth.Auth0.EventStorage.SkipEventTypes) != 2 {
		t.Errorf("SkipEventTypes len = %d, want 2", len(auth.Auth0.EventStorage.SkipEventTypes))
	}

	// Temporal config with nested retry
	if temporal.Host != "localhost" {
		t.Errorf("temporal.Host = %q", temporal.Host)
	}
	if temporal.Port != 7233 {
		t.Errorf("temporal.Port = %d, want 7233", temporal.Port)
	}
	if temporal.ConnectionTimeout != 30*time.Second {
		t.Errorf("temporal.ConnectionTimeout = %v", temporal.ConnectionTimeout)
	}
	if temporal.Retry.InitialInterval != time.Second {
		t.Errorf("temporal.Retry.InitialInterval = %v", temporal.Retry.InitialInterval)
	}
	if temporal.Retry.BackoffCoefficient != 2.0 {
		t.Errorf("temporal.Retry.BackoffCoefficient = %f", temporal.Retry.BackoffCoefficient)
	}
	if temporal.Retry.MaximumAttempts != 3 {
		t.Errorf("temporal.Retry.MaximumAttempts = %d", temporal.Retry.MaximumAttempts)
	}
}

func TestLoader_TOML(t *testing.T) {
	var db dbConfig
	var http httpConfig
	var auth authConfig
	var temporal temporalConfig

	loader := New("testdata/config.toml")
	mustRegister(t, loader, "db", &db)
	mustRegister(t, loader, "http", &http)
	mustRegister(t, loader, "auth", &auth)
	mustRegister(t, loader, "temporal", &temporal)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if db.Host != "localhost" {
		t.Errorf("db.Host = %q, want %q", db.Host, "localhost")
	}
	if db.Port != 5432 {
		t.Errorf("db.Port = %d, want %d", db.Port, 5432)
	}
	if db.ConnectTimeout != 10*time.Second {
		t.Errorf("db.ConnectTimeout = %v", db.ConnectTimeout)
	}
	if len(db.Stores) != 2 {
		t.Errorf("db.Stores len = %d, want 2", len(db.Stores))
	}

	if http.Port != 8080 {
		t.Errorf("http.Port = %d, want %d", http.Port, 8080)
	}
	if len(http.Cors) != 2 {
		t.Errorf("http.Cors len = %d, want 2", len(http.Cors))
	}

	if !auth.Enabled {
		t.Error("auth.Enabled = false, want true")
	}
	if auth.Auth0.Domain != "dev.auth0.com" {
		t.Errorf("auth.Auth0.Domain = %q", auth.Auth0.Domain)
	}
	if !auth.Auth0.EventStorage.Enabled {
		t.Error("auth.Auth0.EventStorage.Enabled = false")
	}
	if len(auth.Auth0.EventStorage.SkipEventTypes) != 2 {
		t.Errorf("auth SkipEventTypes len = %d, want 2", len(auth.Auth0.EventStorage.SkipEventTypes))
	}

	if temporal.Port != 7233 {
		t.Errorf("temporal.Port = %d, want 7233", temporal.Port)
	}
	if temporal.Retry.BackoffCoefficient != 2.0 {
		t.Errorf("temporal.Retry.BackoffCoefficient = %f", temporal.Retry.BackoffCoefficient)
	}
}

func TestLoader_JSON(t *testing.T) {
	var db dbConfig
	var http httpConfig
	var auth authConfig
	var temporal temporalConfig

	loader := New("testdata/config.json")
	mustRegister(t, loader, "db", &db)
	mustRegister(t, loader, "http", &http)
	mustRegister(t, loader, "auth", &auth)
	mustRegister(t, loader, "temporal", &temporal)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if db.Host != "localhost" {
		t.Errorf("db.Host = %q, want %q", db.Host, "localhost")
	}
	if db.Port != 5432 {
		t.Errorf("db.Port = %d, want %d", db.Port, 5432)
	}
	if len(db.Stores) != 2 {
		t.Errorf("db.Stores len = %d, want 2", len(db.Stores))
	}

	if http.Port != 8080 {
		t.Errorf("http.Port = %d, want %d", http.Port, 8080)
	}
	if len(http.Cors) != 2 {
		t.Errorf("http.Cors len = %d, want 2", len(http.Cors))
	}

	if !auth.Enabled {
		t.Error("auth.Enabled = false, want true")
	}
	if auth.Auth0.Domain != "dev.auth0.com" {
		t.Errorf("auth.Auth0.Domain = %q", auth.Auth0.Domain)
	}
	if !auth.Auth0.EventStorage.Enabled {
		t.Error("auth.Auth0.EventStorage.Enabled = false")
	}

	if temporal.Port != 7233 {
		t.Errorf("temporal.Port = %d, want 7233", temporal.Port)
	}
	if temporal.Retry.MaximumAttempts != 3 {
		t.Errorf("temporal.Retry.MaximumAttempts = %d", temporal.Retry.MaximumAttempts)
	}
}

func TestLoader_LoadReader(t *testing.T) {
	yamlData := `
db:
  host: "reader-host"
  port: 3306
`
	var db dbConfig

	loader := New("", WithFormat("yaml"))
	mustRegister(t, loader, "db", &db)

	if err := loader.LoadReader(strings.NewReader(yamlData)); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	if db.Host != "reader-host" {
		t.Errorf("db.Host = %q, want %q", db.Host, "reader-host")
	}
	if db.Port != 3306 {
		t.Errorf("db.Port = %d, want %d", db.Port, 3306)
	}
}

func TestLoader_Get(t *testing.T) {
	var db dbConfig

	loader := New("testdata/config.yaml")
	mustRegister(t, loader, "db", &db)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	got := loader.Get("db")
	if got == nil {
		t.Fatal("Get(db) returned nil")
	}
	if got != &db {
		t.Error("Get(db) did not return the same pointer")
	}

	if loader.Get("nonexistent") != nil {
		t.Error("Get(nonexistent) should return nil")
	}
}

func TestLoader_Raw(t *testing.T) {
	loader := New("testdata/config.yaml")

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	raw := loader.Raw("db")
	if raw == nil {
		t.Fatal("Raw(db) returned nil")
	}
	if raw["host"] != "localhost" {
		t.Errorf("Raw(db)[host] = %v", raw["host"])
	}

	if loader.Raw("nonexistent") != nil {
		t.Error("Raw(nonexistent) should return nil")
	}
}

func TestLoader_AllSettings(t *testing.T) {
	loader := New("testdata/config.yaml")

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	all := loader.AllSettings()
	if all == nil {
		t.Fatal("AllSettings() returned nil")
	}
	if _, ok := all["db"]; !ok {
		t.Error("AllSettings() missing 'db' key")
	}
	if _, ok := all["http"]; !ok {
		t.Error("AllSettings() missing 'http' key")
	}
}

func TestLoader_UnregisteredSection(t *testing.T) {
	var db dbConfig

	loader := New("testdata/config.yaml")
	mustRegister(t, loader, "db", &db)
	// Note: not registering "http" — should not error

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if db.Host != "localhost" {
		t.Errorf("db.Host = %q", db.Host)
	}
}

func TestLoader_MissingSectionPreservesDefaults(t *testing.T) {
	db := dbConfig{
		Host:        "default-host",
		Port:        9999,
		MaxPoolSize: 10,
	}

	yamlData := `
http:
  port: 8080
`
	loader := New("", WithFormat("yaml"))
	mustRegister(t, loader, "db", &db)

	if err := loader.LoadReader(strings.NewReader(yamlData)); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	// db section is not in file — defaults should be preserved
	if db.Host != "default-host" {
		t.Errorf("db.Host = %q, want %q (default preserved)", db.Host, "default-host")
	}
	if db.Port != 9999 {
		t.Errorf("db.Port = %d, want %d (default preserved)", db.Port, 9999)
	}
}

func TestLoader_WithFormat(t *testing.T) {
	yamlData := `
db:
  host: "format-test"
`
	var db dbConfig

	// No file extension, explicit format
	loader := New("noext", WithFormat("yaml"))
	mustRegister(t, loader, "db", &db)

	if err := loader.LoadReader(strings.NewReader(yamlData)); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	if db.Host != "format-test" {
		t.Errorf("db.Host = %q", db.Host)
	}
}

func TestLoader_FileNotFound(t *testing.T) {
	loader := New("testdata/nonexistent.yaml")
	if err := loader.Load(); err == nil {
		t.Error("Load() should error for missing file")
	}
}

func TestLoader_InvalidYAML(t *testing.T) {
	loader := New("", WithFormat("yaml"))
	if err := loader.LoadReader(strings.NewReader("{{{")); err == nil {
		t.Error("LoadReader() should error for invalid YAML")
	}
}

func TestLoader_NoFormatDetectable(t *testing.T) {
	loader := New("noext")
	if err := loader.LoadReader(strings.NewReader("{}")); err == nil {
		t.Error("LoadReader() should error when format cannot be detected")
	}
}

func TestLoader_WithTagName(t *testing.T) {
	type yamlTagConfig struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	}

	var cfg yamlTagConfig
	yamlData := `
server:
  host: "tagged"
  port: 9090
`
	loader := New("", WithFormat("yaml"), WithTagName("yaml"))
	mustRegister(t, loader, "server", &cfg)

	if err := loader.LoadReader(strings.NewReader(yamlData)); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	if cfg.Host != "tagged" {
		t.Errorf("cfg.Host = %q, want %q", cfg.Host, "tagged")
	}
	if cfg.Port != 9090 {
		t.Errorf("cfg.Port = %d, want %d", cfg.Port, 9090)
	}
}

func TestLoader_WithStrictMode(t *testing.T) {
	type strictConfig struct {
		Host string `mapstructure:"host"`
	}

	var cfg strictConfig
	// YAML has an extra "port" field not in struct
	yamlData := `
server:
  host: "strict"
  port: 8080
`
	loader := New("", WithFormat("yaml"), WithStrictMode())
	mustRegister(t, loader, "server", &cfg)

	err := loader.LoadReader(strings.NewReader(yamlData))
	if err == nil {
		t.Error("LoadReader() should error in strict mode with unknown fields")
	}
}

func TestLoader_Register_Validation(t *testing.T) {
	loader := New("", WithFormat("yaml"))

	// nil
	if err := loader.Register("x", nil); err == nil {
		t.Error("Register(nil) should error")
	}

	// non-pointer
	if err := loader.Register("x", dbConfig{}); err == nil {
		t.Error("Register(value) should error")
	}

	// pointer to non-struct
	s := "hello"
	if err := loader.Register("x", &s); err == nil {
		t.Error("Register(*string) should error")
	}

	// valid
	var db dbConfig
	if err := loader.Register("db", &db); err != nil {
		t.Errorf("Register(*struct) should succeed: %v", err)
	}
}

func TestLoader_Loaded(t *testing.T) {
	loader := New("", WithFormat("yaml"))

	if loader.Loaded() {
		t.Error("Loaded() should be false before Load")
	}

	if err := loader.LoadReader(strings.NewReader("db:\n  host: test\n")); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	if !loader.Loaded() {
		t.Error("Loaded() should be true after Load")
	}
}

func TestLoader_AllSettings_ReturnsCopy(t *testing.T) {
	loader := New("", WithFormat("yaml"))
	if err := loader.LoadReader(strings.NewReader("db:\n  host: test\n")); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	all := loader.AllSettings()
	// Mutate the returned copy
	all["injected"] = "bad"

	// Original should be unaffected
	all2 := loader.AllSettings()
	if _, ok := all2["injected"]; ok {
		t.Error("AllSettings() mutation leaked into internal state")
	}
}

func TestLoader_EmptyFile(t *testing.T) {
	loader := New("", WithFormat("yaml"))
	var db dbConfig
	mustRegister(t, loader, "db", &db)

	// Empty YAML should not error, just leave defaults
	if err := loader.LoadReader(strings.NewReader("")); err != nil {
		t.Fatalf("LoadReader() error for empty: %v", err)
	}

	if db.Host != "" {
		t.Errorf("db.Host = %q, want empty", db.Host)
	}
}

func TestLoader_ConcurrentAccess(t *testing.T) {
	loader := New("testdata/config.yaml")
	var db dbConfig
	mustRegister(t, loader, "db", &db)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = loader.Get("db")
			_ = loader.Raw("db")
			_ = loader.AllSettings()
			_ = loader.Loaded()
		}()
	}
	wg.Wait()
}

func TestLoader_WithDecodeHook(t *testing.T) {
	type serverConfig struct {
		Addr net.IP `mapstructure:"addr"`
	}

	// Custom hook: string → net.IP
	ipHook := func(from, to reflect.Type, data any) (any, error) {
		if to != reflect.TypeOf(net.IP{}) {
			return data, nil
		}
		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		ip := net.ParseIP(s)
		if ip == nil {
			return data, fmt.Errorf("invalid IP: %s", s)
		}
		return ip, nil
	}

	var cfg serverConfig
	yamlData := `
server:
  addr: "192.168.1.1"
`
	loader := New("", WithFormat("yaml"), WithDecodeHook(mapstructure.DecodeHookFuncType(ipHook)))
	mustRegister(t, loader, "server", &cfg)

	if err := loader.LoadReader(strings.NewReader(yamlData)); err != nil {
		t.Fatalf("LoadReader() error: %v", err)
	}

	expected := net.ParseIP("192.168.1.1")
	if !cfg.Addr.Equal(expected) {
		t.Errorf("cfg.Addr = %v, want %v", cfg.Addr, expected)
	}
}

func TestLoader_WithLogger(t *testing.T) {
	// Just verify the logger option is accepted and doesn't panic.
	logger := slog.Default()
	loader := New("testdata/config.yaml", WithLogger(logger))
	var db dbConfig
	mustRegister(t, loader, "db", &db)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if db.Host != "localhost" {
		t.Errorf("db.Host = %q, want %q", db.Host, "localhost")
	}
}

func TestLoader_Reload(t *testing.T) {
	var db dbConfig
	loader := New("testdata/config.yaml")
	mustRegister(t, loader, "db", &db)

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if db.Host != "localhost" {
		t.Errorf("db.Host = %q after first load", db.Host)
	}

	// Reload should re-read and re-populate
	if err := loader.Reload(); err != nil {
		t.Fatalf("Reload() error: %v", err)
	}
	if db.Host != "localhost" {
		t.Errorf("db.Host = %q after reload", db.Host)
	}
}

func TestLoader_NameOf(t *testing.T) {
	var db dbConfig
	var http httpConfig

	loader := New("testdata/config.yaml")
	mustRegister(t, loader, "db", &db)
	mustRegister(t, loader, "http", &http)

	// Registered pointers should resolve
	if name := loader.NameOf(&db); name != "db" {
		t.Errorf("NameOf(&db) = %q, want %q", name, "db")
	}
	if name := loader.NameOf(&http); name != "http" {
		t.Errorf("NameOf(&http) = %q, want %q", name, "http")
	}

	// Unregistered pointer
	var other dbConfig
	if name := loader.NameOf(&other); name != "" {
		t.Errorf("NameOf(&other) = %q, want empty", name)
	}

	// nil and non-pointer
	if name := loader.NameOf(nil); name != "" {
		t.Errorf("NameOf(nil) = %q, want empty", name)
	}
	if name := loader.NameOf(db); name != "" {
		t.Errorf("NameOf(value) = %q, want empty", name)
	}
}

func TestLoader_Names(t *testing.T) {
	loader := New("testdata/config.yaml")
	var db dbConfig
	var http httpConfig
	mustRegister(t, loader, "db", &db)
	mustRegister(t, loader, "http", &http)

	names := loader.Names()
	if len(names) != 2 {
		t.Fatalf("Names() len = %d, want 2", len(names))
	}

	nameSet := map[string]bool{}
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet["db"] {
		t.Error("Names() missing 'db'")
	}
	if !nameSet["http"] {
		t.Error("Names() missing 'http'")
	}
}

func TestLoader_AllSettings_DeepCopy(t *testing.T) {
	loader := New("testdata/config.yaml")
	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	all := loader.AllSettings()

	// Mutate a nested map in the returned copy
	dbRaw, ok := all["db"].(map[string]any)
	if !ok {
		t.Fatal("all[db] is not a map")
	}
	dbRaw["host"] = "MUTATED"

	// Original should be unaffected
	all2 := loader.AllSettings()
	dbRaw2, _ := all2["db"].(map[string]any)
	if dbRaw2["host"] == "MUTATED" {
		t.Error("deep nested mutation leaked into internal state")
	}
}
