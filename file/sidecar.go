package file

import (
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

type sidecar struct {
	path string
	mu   sync.RWMutex
	data map[string]map[string]any // namespace -> key -> raw value (nil = tombstone/deleted)
}

func newSidecar(path string) *sidecar {
	return &sidecar{
		path: path,
		data: make(map[string]map[string]any),
	}
}

func (sc *sidecar) load() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	data, err := os.ReadFile(sc.path)
	if err != nil {
		if os.IsNotExist(err) {
			sc.data = make(map[string]map[string]any)
			return nil
		}
		return err
	}

	var raw map[string]map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw == nil {
		raw = make(map[string]map[string]any)
	}
	sc.data = raw
	return nil
}

func (sc *sidecar) set(namespace, key string, val any) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.data[namespace] == nil {
		sc.data[namespace] = make(map[string]any)
	}
	sc.data[namespace][key] = val
	return sc.flushLocked()
}

func (sc *sidecar) delete(namespace, key string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.data[namespace] == nil {
		sc.data[namespace] = make(map[string]any)
	}
	sc.data[namespace][key] = nil
	return sc.flushLocked()
}

func (sc *sidecar) allSettings() map[string]map[string]any {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	cp := make(map[string]map[string]any, len(sc.data))
	for ns, keys := range sc.data {
		kcp := make(map[string]any, len(keys))
		for k, v := range keys {
			kcp[k] = v
		}
		cp[ns] = kcp
	}
	return cp
}

func (sc *sidecar) flushLocked() error {
	data, err := yaml.Marshal(sc.data)
	if err != nil {
		return err
	}

	tmp := sc.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, sc.path)
}
