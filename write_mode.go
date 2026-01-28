package config

// WriteMode specifies the conditional behavior for Set operations.
type WriteMode int

const (
	// WriteModeUpsert creates or updates the key (default behavior).
	WriteModeUpsert WriteMode = iota

	// WriteModeCreate only creates the key if it doesn't exist.
	// Returns ErrKeyExists if the key already exists.
	WriteModeCreate

	// WriteModeUpdate only updates the key if it already exists.
	// Returns ErrNotFound if the key doesn't exist.
	WriteModeUpdate
)

// String returns the string representation of the write mode.
func (m WriteMode) String() string {
	switch m {
	case WriteModeUpsert:
		return "upsert"
	case WriteModeCreate:
		return "create"
	case WriteModeUpdate:
		return "update"
	default:
		return "unknown"
	}
}
