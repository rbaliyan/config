package file

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"
)

// Loader reads a configuration file and unmarshals named sections into
// registered struct pointers. It replaces go-conf and viper for the common
// use case of loading YAML/TOML/JSON config files.
//
// Usage:
//
//	loader := file.New("config.yaml")
//	loader.Register("db", &dbConfig)
//	loader.Register("http", &httpConfig)
//	if err := loader.Load(); err != nil { ... }
//	// dbConfig and httpConfig are now populated from the file
type Loader struct {
	path    string
	opts    loaderOptions
	mu      sync.RWMutex
	configs map[string]any    // name -> pointer to struct
	raw     map[string]any    // parsed file contents
	loaded  bool
}

// New creates a Loader for the given config file path.
// The file format is auto-detected from the extension (.yaml, .yml, .toml, .json).
// Use WithFormat to override auto-detection.
func New(path string, opts ...LoaderOption) *Loader {
	o := loaderOptions{
		tagName: "mapstructure",
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &Loader{
		path:    path,
		opts:    o,
		configs: make(map[string]any),
	}
}

// Register registers a config struct pointer by name.
// The name must match a top-level key in the config file.
//
// The config argument must be a non-nil pointer to a struct. After Load is called,
// the struct will be populated with values from the corresponding section.
//
// Existing mapstructure, yaml, json, and toml struct tags are supported.
func (l *Loader) Register(name string, config any) error {
	if config == nil {
		return fmt.Errorf("file: Register(%q): config must be a non-nil pointer to a struct", name)
	}
	rv := reflect.ValueOf(config)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("file: Register(%q): config must be a non-nil pointer to a struct, got %T", name, config)
	}
	if rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("file: Register(%q): config must point to a struct, got pointer to %s", name, rv.Elem().Kind())
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.configs[name] = config
	return nil
}

// MustRegister is like Register but panics on error.
// Use this during program initialization where invalid registration is a programmer error.
func (l *Loader) MustRegister(name string, config any) {
	if err := l.Register(name, config); err != nil {
		panic(err)
	}
}

// Load reads the config file and unmarshals sections into registered structs.
// Returns an error if the file cannot be read or parsed, or if any registered
// struct cannot be unmarshaled.
func (l *Loader) Load() error {
	f, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("file: open %s: %w", l.path, err)
	}
	defer f.Close()

	return l.LoadReader(f)
}

// LoadReader reads configuration from an io.Reader and unmarshals sections
// into registered structs. The format must be set via WithFormat or the
// Loader must have been created with a file path that has a recognized extension.
func (l *Loader) LoadReader(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("file: read: %w", err)
	}

	format := l.detectFormat()
	if format == "" {
		return fmt.Errorf("file: cannot detect format; use WithFormat option")
	}

	raw, err := l.parse(data, format)
	if err != nil {
		return fmt.Errorf("file: parse %s: %w", format, err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.raw = raw
	l.loaded = true

	return l.unmarshalAllLocked()
}

// Get returns a registered config by name.
// Returns nil if the name is not registered.
func (l *Loader) Get(name string) any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.configs[name]
}

// NameOf returns the registration name for the given config pointer,
// or "" if the pointer was not registered. The argument must be the
// exact pointer previously passed to Register or MustRegister.
func (l *Loader) NameOf(config any) string {
	if config == nil {
		return ""
	}
	rv := reflect.ValueOf(config)
	if rv.Kind() != reflect.Ptr {
		return ""
	}
	ptr := rv.Pointer()

	l.mu.RLock()
	defer l.mu.RUnlock()
	for name, registered := range l.configs {
		if reflect.ValueOf(registered).Pointer() == ptr {
			return name
		}
	}
	return ""
}

// Names returns all registered config names.
func (l *Loader) Names() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	names := make([]string, 0, len(l.configs))
	for name := range l.configs {
		names = append(names, name)
	}
	return names
}

// Raw returns the raw parsed map for a given top-level key.
// Returns nil if the key does not exist or Load has not been called.
func (l *Loader) Raw(name string) map[string]any {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.raw == nil {
		return nil
	}
	v, ok := l.raw[name]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m
}

// AllSettings returns a deep copy of the full raw parsed map.
// Returns nil if Load has not been called.
func (l *Loader) AllSettings() map[string]any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.raw == nil {
		return nil
	}
	return deepCopyMap(l.raw)
}

// deepCopyMap returns a deep copy of a map[string]any, recursively copying
// nested maps and slices so the caller cannot mutate internal state.
func deepCopyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = deepCopyValue(v)
	}
	return cp
}

func deepCopyValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return deepCopyMap(val)
	case []any:
		cp := make([]any, len(val))
		for i, item := range val {
			cp[i] = deepCopyValue(item)
		}
		return cp
	default:
		return v // scalars are immutable
	}
}

// Reload re-reads the config file and re-populates all registered structs.
// This is equivalent to calling Load again but is safe to call on an already-loaded Loader.
// Returns an error if the file cannot be read, parsed, or unmarshaled.
func (l *Loader) Reload() error {
	return l.Load()
}

// Loaded reports whether Load or LoadReader has been called successfully.
func (l *Loader) Loaded() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.loaded
}

// detectFormat returns the file format from the explicit option or file extension.
func (l *Loader) detectFormat() string {
	if l.opts.format != "" {
		return normalizeFormat(l.opts.format)
	}
	ext := strings.TrimPrefix(filepath.Ext(l.path), ".")
	return normalizeFormat(ext)
}

// normalizeFormat normalizes format names.
func normalizeFormat(format string) string {
	switch strings.ToLower(format) {
	case "yaml", "yml":
		return "yaml"
	case "toml":
		return "toml"
	case "json":
		return "json"
	default:
		return ""
	}
}

// parse decodes raw bytes into a map based on the format.
func (l *Loader) parse(data []byte, format string) (map[string]any, error) {
	var raw map[string]any

	switch format {
	case "yaml":
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
	case "toml":
		if _, err := toml.Decode(string(data), &raw); err != nil {
			return nil, err
		}
	case "json":
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}

	if raw == nil {
		raw = make(map[string]any)
	}

	return raw, nil
}

// unmarshalAllLocked unmarshals raw data into all registered config structs.
// Must be called with l.mu held (read or write).
func (l *Loader) unmarshalAllLocked() error {
	for name, target := range l.configs {
		section, ok := l.raw[name]
		if !ok {
			// Section not in file — leave struct at its default values
			continue
		}

		if err := l.decode(section, target); err != nil {
			return fmt.Errorf("file: unmarshal %q: %w", name, err)
		}
	}

	return nil
}

// decode uses mapstructure to decode a raw value into a target struct.
func (l *Loader) decode(input, output any) error {
	hooks := []mapstructure.DecodeHookFunc{
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToTimeHookFunc(time.RFC3339),
		mapstructure.TextUnmarshallerHookFunc(),
	}
	hooks = append(hooks, l.opts.decoderFns...)

	config := &mapstructure.DecoderConfig{
		Result:           output,
		TagName:          l.opts.tagName,
		WeaklyTypedInput: true,
		DecodeHook:       mapstructure.ComposeDecodeHookFunc(hooks...),
		ErrorUnused:      l.opts.strict,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}
