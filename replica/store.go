// Package replica provides a store that replicates a primary to one or more secondary stores.
//
// All writes are routed through the primary. Changes propagate to secondaries either
// synchronously (ModeSync, on each write) or asynchronously (ModeAsync, via a background
// Watch goroutine). Any store combination is supported: MongoDB→PostgreSQL,
// PostgreSQL→memory, MongoDB→MongoDB, etc.
//
// # Replication Modes
//
//   - ModeAsync: writes complete when the primary accepts them; secondaries are updated
//     by a background goroutine watching the primary's change stream. Requires the primary
//     to support Watch.
//   - ModeSync: writes propagate to all reachable secondaries before returning.
//     Works with any store type (no Watch required); secondary failures are metered but
//     do not fail the caller.
//
// # Read Preferences
//
//   - ReadPrimary (default): all reads go to the primary (strong consistency).
//   - ReadPrimaryPreferred: reads primary; falls back to a secondary on primary failure.
//     Applies to Get, GetMany, and Find.
//   - ReadSecondary: reads from the first secondary (eventual consistency, lower latency).
//
// # Alias Support
//
// When the primary implements [config.AliasStore], the replica store delegates all alias
// operations to the primary. In ModeSync, SetAlias/DeleteAlias are also propagated to
// secondaries that implement AliasStore. In ModeAsync, alias events from the Watch stream
// are applied to supporting secondaries automatically.
//
// # Observability
//
// Tracing and metrics are opt-in (disabled by default):
//
//	store := replica.NewStore(primary, secondaries,
//	    replica.WithTracesEnabled(true),
//	    replica.WithMetricsEnabled(true),
//	    replica.WithBackendName("mongo->postgres"),
//	)
//
// Replica-specific metrics (beyond standard operation counters):
//   - config.replica.replication.events.total — async events processed (including aliases)
//   - config.replica.replication.lag — histogram of event-to-apply latency
//   - config.replica.replication.errors.total — errors applying events or sync propagation
//   - config.replica.sync.duration — initial sync duration
package replica

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/rbaliyan/config"
)

// errWatchChannelClosed is returned by replicateLoop when the primary closes the Watch channel.
var errWatchChannelClosed = errors.New("replica: primary watch channel closed")

// errAliasingNotSupported is returned when an alias operation is requested but the primary
// does not implement config.AliasStore.
var errAliasingNotSupported = errors.New("replica: primary store does not support aliasing")

// ReplicationMode controls how writes propagate to secondaries.
type ReplicationMode int

const (
	// ModeAsync propagates changes via a background Watch goroutine.
	// Writes complete as soon as the primary accepts them. Requires Watch support on primary.
	ModeAsync ReplicationMode = iota

	// ModeSync propagates changes inline on every write.
	// Write latency includes secondary propagation; secondary failures are metered but
	// do not fail the caller.
	ModeSync
)

// String returns a human-readable name for the replication mode.
func (m ReplicationMode) String() string {
	if m == ModeSync {
		return "sync"
	}
	return "async"
}

// ReadPreference controls which store serves read requests.
type ReadPreference int

const (
	// ReadPrimary routes all reads to the primary (strong consistency).
	ReadPrimary ReadPreference = iota

	// ReadPrimaryPreferred reads from primary; falls back to a secondary on failure.
	// Applies to Get, GetMany, and Find.
	ReadPrimaryPreferred

	// ReadSecondary reads from the first secondary (may return stale data).
	ReadSecondary
)

// String returns a human-readable name for the read preference.
func (r ReadPreference) String() string {
	switch r {
	case ReadPrimaryPreferred:
		return "primary_preferred"
	case ReadSecondary:
		return "secondary"
	default:
		return "primary"
	}
}

type options struct {
	mode            ReplicationMode
	readPref        ReadPreference
	syncNamespaces  []string
	watchBackoff    time.Duration
	replicateFilter config.WatchFilter // explicit filter for the async goroutine

	// OTel — disabled by default, opt-in via With* options.
	enableTraces  bool
	enableMetrics bool
	tracerName    string
	meterName     string
	backendName   string
	tracer        trace.Tracer
	meter         metric.Meter
}

// Option configures the replica store.
type Option func(*options)

// WithReplicationMode sets the replication mode (default: ModeAsync).
//
// In ModeAsync the primary must support Watch. Bulk write operations (SetMany,
// DeleteMany) rely on the primary emitting per-key change events; if the primary
// coalesces or drops events under load, secondary stores may silently diverge.
// Use ModeSync when the primary does not support Watch or when strong secondary
// consistency is required.
func WithReplicationMode(m ReplicationMode) Option {
	return func(o *options) { o.mode = m }
}

// WithReadPreference sets the read preference (default: ReadPrimary).
func WithReadPreference(rp ReadPreference) Option {
	return func(o *options) { o.readPref = rp }
}

// WithInitialSync copies all entries in the given namespaces from primary to secondaries
// during Connect. Pass config.DefaultNamespace ("") to include the default namespace.
// When set and no explicit WithReplicateFilter is provided, ModeAsync also watches only
// these namespaces.
func WithInitialSync(namespaces ...string) Option {
	return func(o *options) { o.syncNamespaces = append(o.syncNamespaces, namespaces...) }
}

// WithWatchBackoff sets the reconnect backoff for the async replication goroutine (default: 5s).
func WithWatchBackoff(d time.Duration) Option {
	return func(o *options) { o.watchBackoff = d }
}

// WithReplicateFilter sets an explicit WatchFilter for the async replication goroutine.
// When not set, the filter is derived from WithInitialSync namespaces, or watches everything.
func WithReplicateFilter(f config.WatchFilter) Option {
	return func(o *options) { o.replicateFilter = f }
}

// Store replicates a primary store to one or more secondary stores.
// The stores slice is immutable after construction; each underlying store provides
// its own concurrency protection.
type Store struct {
	primary     config.Store
	secondaries []config.Store
	opts        options

	// OTel (nil when the respective feature is disabled)
	tracer  trace.Tracer
	metrics *replicaMetrics

	cancel         context.CancelFunc
	wg             sync.WaitGroup
	watchReady     chan struct{} // closed once the first Watch is registered (ModeAsync)
	watchReadyOnce sync.Once
}

// NewStore creates a replica store with the given primary and secondaries.
// Panics if primary is nil (initialization-time check, consistent with package convention).
//
// Example — MongoDB primary replicated to PostgreSQL secondary with tracing:
//
//	store := replica.NewStore(primary, []config.Store{secondary},
//	    replica.WithReplicationMode(replica.ModeAsync),
//	    replica.WithReadPreference(replica.ReadPrimary),
//	    replica.WithInitialSync(config.DefaultNamespace, "prod"),
//	    replica.WithTracesEnabled(true),
//	    replica.WithMetricsEnabled(true),
//	    replica.WithBackendName("mongo->postgres"),
//	)
func NewStore(primary config.Store, secondaries []config.Store, opts ...Option) *Store {
	if primary == nil {
		panic("replica: primary store must not be nil")
	}
	s := &Store{
		primary:     primary,
		secondaries: secondaries,
		watchReady:  make(chan struct{}),
		opts: options{
			mode:         ModeAsync,
			readPref:     ReadPrimary,
			watchBackoff: 5 * time.Second,
			tracerName:   "github.com/rbaliyan/config/replica",
			meterName:    "github.com/rbaliyan/config/replica",
		},
	}
	for _, opt := range opts {
		opt(&s.opts)
	}
	return s
}

// Compile-time interface checks.
var (
	_ config.Store          = (*Store)(nil)
	_ config.HealthChecker  = (*Store)(nil)
	_ config.StatsProvider  = (*Store)(nil)
	_ config.BulkStore      = (*Store)(nil)
	_ config.VersionedStore = (*Store)(nil)
	_ config.CodecValidator = (*Store)(nil)
	_ config.AliasStore     = (*Store)(nil)
)

// Connect connects the primary and all secondaries. It also:
//   - initializes OTel tracer and metrics when enabled (metric errors are returned here),
//   - copies the configured namespaces from primary to secondaries (WithInitialSync),
//   - starts the background replication goroutine (ModeAsync).
//
// Connect succeeds as long as the primary connects; individual secondary failures are
// tolerated unless all secondaries fail.
func (s *Store) Connect(ctx context.Context) error {
	// Initialize OTel tracer.
	if s.opts.enableTraces {
		if s.opts.tracer != nil {
			s.tracer = s.opts.tracer
		} else {
			s.tracer = otel.Tracer(s.opts.tracerName)
		}
	}

	// Initialize OTel metrics.
	if s.opts.enableMetrics {
		var m metric.Meter
		if s.opts.meter != nil {
			m = s.opts.meter
		} else {
			m = otel.Meter(s.opts.meterName)
		}
		metrics, err := initReplicaMetrics(m)
		if err != nil {
			return err
		}
		s.metrics = metrics
	}

	// Instrument Connect itself.
	if s.opts.enableTraces {
		var span trace.Span
		ctx, span = s.tracer.Start(ctx, "config.replica.Connect",
			trace.WithAttributes(s.commonAttrs()...))
		defer span.End()
	}

	start := time.Now()

	if err := s.primary.Connect(ctx); err != nil {
		s.recordOp(ctx, "connect", "", start, err)
		return err
	}

	var secErrs []error
	for _, sec := range s.secondaries {
		if err := sec.Connect(ctx); err != nil {
			secErrs = append(secErrs, err)
		}
	}
	if len(s.secondaries) > 0 && len(secErrs) == len(s.secondaries) {
		err := errors.Join(secErrs...)
		s.recordOp(ctx, "connect", "", start, err)
		return err
	}

	if len(s.opts.syncNamespaces) > 0 {
		if err := s.initialSync(ctx); err != nil {
			s.recordOp(ctx, "connect", "", start, err)
			return err
		}
	}

	s.recordOp(ctx, "connect", "", start, nil)

	if s.opts.mode == ModeAsync && len(s.secondaries) > 0 {
		// Derive replication filter from syncNamespaces when no explicit filter is set.
		filter := s.opts.replicateFilter
		if len(filter.Namespaces) == 0 && len(s.opts.syncNamespaces) > 0 {
			filter.Namespaces = s.opts.syncNamespaces
		}

		repCtx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		s.wg.Add(1)
		go s.replicate(repCtx, filter)

		// Wait until the replication goroutine has registered its Watch,
		// so callers can rely on async replication being active after Connect returns.
		select {
		case <-s.watchReady:
		case <-ctx.Done():
		}
	}

	return nil
}

// Close stops the replication goroutine and closes all stores.
func (s *Store) Close(ctx context.Context) error {
	if s.opts.enableTraces && s.tracer != nil {
		var span trace.Span
		ctx, span = s.tracer.Start(ctx, "config.replica.Close",
			trace.WithAttributes(s.commonAttrs()...))
		defer span.End()
	}

	start := time.Now()

	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
	}

	var errs []error
	if err := s.primary.Close(ctx); err != nil {
		errs = append(errs, err)
	}
	for _, sec := range s.secondaries {
		if err := sec.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	err := errors.Join(errs...)
	s.recordOp(ctx, "close", "", start, err)
	return err
}

// Get retrieves a value according to the configured ReadPreference.
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		val, err := s.get(ctx, namespace, key)
		s.recordOp(ctx, "get", namespace, start, err)
		return val, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.Get",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.String("config.key", key),
			attribute.String("config.replica.read_preference", s.opts.readPref.String()),
		)...))
	defer span.End()

	start := time.Now()
	val, err := s.get(ctx, namespace, key)
	s.recordOp(ctx, "get", namespace, start, err)
	setSpanResult(span, err)
	if err == nil && val != nil {
		span.SetAttributes(
			attribute.Int64("config.version", val.Metadata().Version()),
			attribute.String("config.type", val.Type().String()),
		)
	}
	return val, err
}

func (s *Store) get(ctx context.Context, namespace, key string) (config.Value, error) {
	switch s.opts.readPref {
	case ReadSecondary:
		return s.getFromSecondaries(ctx, namespace, key)
	case ReadPrimaryPreferred:
		val, err := s.primary.Get(ctx, namespace, key)
		if err == nil {
			return val, nil
		}
		if isRetriable(err) && len(s.secondaries) > 0 {
			return s.getFromSecondaries(ctx, namespace, key)
		}
		return nil, err
	default: // ReadPrimary
		return s.primary.Get(ctx, namespace, key)
	}
}

func (s *Store) getFromSecondaries(ctx context.Context, namespace, key string) (config.Value, error) {
	var lastErr error
	for _, sec := range s.secondaries {
		val, err := sec.Get(ctx, namespace, key)
		if err == nil {
			return val, nil
		}
		lastErr = err
		if !isRetriable(err) {
			return nil, err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &config.KeyNotFoundError{Namespace: namespace, Key: key}
}

// Set writes to the primary. In ModeSync, changes are propagated to secondaries before returning.
// In ModeAsync, secondaries receive the change via the background replication goroutine.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		result, err := s.set(ctx, namespace, key, value)
		s.recordOp(ctx, "set", namespace, start, err)
		return result, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.Set",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.String("config.key", key),
			attribute.String("config.type", valueTypeName(value)),
		)...))
	defer span.End()

	start := time.Now()
	result, err := s.set(ctx, namespace, key, value)
	s.recordOp(ctx, "set", namespace, start, err)
	setSpanResult(span, err)
	if err == nil && result != nil {
		span.SetAttributes(attribute.Int64("config.version", result.Metadata().Version()))
	}
	return result, err
}

func (s *Store) set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	result, err := s.primary.Set(ctx, namespace, key, value)
	if err != nil {
		return nil, err
	}
	if s.opts.mode == ModeSync {
		s.propagateSet(ctx, namespace, key, result)
	}
	return result, nil
}

// Delete removes a key from the primary. In ModeSync, removal is propagated to secondaries.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.delete(ctx, namespace, key)
		s.recordOp(ctx, "delete", namespace, start, err)
		return err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.Delete",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.String("config.key", key),
		)...))
	defer span.End()

	start := time.Now()
	err := s.delete(ctx, namespace, key)
	s.recordOp(ctx, "delete", namespace, start, err)
	setSpanResult(span, err)
	return err
}

func (s *Store) delete(ctx context.Context, namespace, key string) error {
	if err := s.primary.Delete(ctx, namespace, key); err != nil {
		return err
	}
	if s.opts.mode == ModeSync {
		s.propagateDelete(ctx, namespace, key)
	}
	return nil
}

// Find queries stores according to the configured ReadPreference.
// ReadPrimaryPreferred falls back to the first secondary when the primary is unavailable.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if filter == nil {
		filter = config.NewFilter().Build()
	}

	if !s.opts.enableTraces {
		start := time.Now()
		page, err := s.find(ctx, namespace, filter)
		s.recordOp(ctx, "find", namespace, start, err)
		return page, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.Find",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.String("config.prefix", filter.Prefix()),
			attribute.Int("config.limit", filter.Limit()),
			attribute.String("config.replica.read_preference", s.opts.readPref.String()),
		)...))
	defer span.End()

	start := time.Now()
	page, err := s.find(ctx, namespace, filter)
	s.recordOp(ctx, "find", namespace, start, err)
	setSpanResult(span, err)
	if err == nil {
		span.SetAttributes(attribute.Int("config.result_count", len(page.Results())))
	}
	return page, err
}

func (s *Store) find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	switch s.opts.readPref {
	case ReadSecondary:
		if len(s.secondaries) > 0 {
			return s.secondaries[0].Find(ctx, namespace, filter)
		}
		return s.primary.Find(ctx, namespace, filter)
	case ReadPrimaryPreferred:
		page, err := s.primary.Find(ctx, namespace, filter)
		if err != nil && isRetriable(err) && len(s.secondaries) > 0 {
			return s.secondaries[0].Find(ctx, namespace, filter)
		}
		return page, err
	default: // ReadPrimary
		return s.primary.Find(ctx, namespace, filter)
	}
}

// Watch delegates to the primary (source of truth).
// Callers should be aware that closing the replica store cancels the returned channel's
// underlying context via the primary implementation's cleanup path.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		ch, err := s.primary.Watch(ctx, filter)
		s.recordOp(ctx, "watch", "", start, err)
		return ch, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.Watch",
		trace.WithAttributes(s.commonAttrs()...))
	defer span.End()

	start := time.Now()
	ch, err := s.primary.Watch(ctx, filter)
	s.recordOp(ctx, "watch", "", start, err)
	setSpanResult(span, err)
	return ch, err
}

// Unwrap returns the primary store, satisfying the decorator-chain convention.
func (s *Store) Unwrap() config.Store {
	return s.primary
}

// Secondaries returns a copy of the secondary stores slice.
func (s *Store) Secondaries() []config.Store {
	out := make([]config.Store, len(s.secondaries))
	copy(out, s.secondaries)
	return out
}

// Health checks the primary's health. Returns nil if the primary implements HealthChecker
// and is healthy, or if it does not implement HealthChecker.
func (s *Store) Health(ctx context.Context) error {
	if hc, ok := s.primary.(config.HealthChecker); ok {
		return hc.Health(ctx)
	}
	return nil
}

// Stats returns statistics from the primary.
func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if sp, ok := s.primary.(config.StatsProvider); ok {
		return sp.Stats(ctx)
	}
	return nil, nil
}

// GetMany retrieves multiple values. Follows ReadPreference for routing.
func (s *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		results, err := s.getMany(ctx, namespace, keys)
		s.recordOp(ctx, "get_many", namespace, start, err)
		return results, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.GetMany",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.Int("config.key_count", len(keys)),
			attribute.String("config.replica.read_preference", s.opts.readPref.String()),
		)...))
	defer span.End()

	start := time.Now()
	results, err := s.getMany(ctx, namespace, keys)
	s.recordOp(ctx, "get_many", namespace, start, err)
	setSpanResult(span, err)
	if err == nil {
		span.SetAttributes(attribute.Int("config.result_count", len(results)))
	}
	return results, err
}

func (s *Store) getMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	switch s.opts.readPref {
	case ReadSecondary:
		if len(s.secondaries) > 0 {
			return bulkGet(ctx, s.secondaries[0], namespace, keys)
		}
		return bulkGet(ctx, s.primary, namespace, keys)
	case ReadPrimaryPreferred:
		results, err := bulkGet(ctx, s.primary, namespace, keys)
		if err != nil && isRetriable(err) && len(s.secondaries) > 0 {
			return bulkGet(ctx, s.secondaries[0], namespace, keys)
		}
		return results, err
	default: // ReadPrimary
		return bulkGet(ctx, s.primary, namespace, keys)
	}
}

// SetMany writes multiple values to the primary. In ModeSync, propagates to secondaries.
func (s *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.setMany(ctx, namespace, values)
		s.recordOp(ctx, "set_many", namespace, start, err)
		return err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.SetMany",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.Int("config.entry_count", len(values)),
		)...))
	defer span.End()

	start := time.Now()
	err := s.setMany(ctx, namespace, values)
	s.recordOp(ctx, "set_many", namespace, start, err)
	setSpanResult(span, err)
	return err
}

func (s *Store) setMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if err := bulkSet(ctx, s.primary, namespace, values); err != nil {
		return err
	}
	if s.opts.mode == ModeSync {
		for _, sec := range s.secondaries {
			if err := bulkSet(ctx, sec, namespace, values); err != nil {
				s.countReplicationError(ctx, "set_many")
			}
		}
	}
	return nil
}

// DeleteMany removes multiple values from the primary. In ModeSync, propagates to secondaries.
func (s *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		deleted, err := s.deleteMany(ctx, namespace, keys)
		s.recordOp(ctx, "delete_many", namespace, start, err)
		return deleted, err
	}

	ctx, span := s.tracer.Start(ctx, "config.replica.DeleteMany",
		trace.WithAttributes(append(s.commonAttrs(),
			attribute.String("config.namespace", namespace),
			attribute.Int("config.key_count", len(keys)),
		)...))
	defer span.End()

	start := time.Now()
	deleted, err := s.deleteMany(ctx, namespace, keys)
	s.recordOp(ctx, "delete_many", namespace, start, err)
	setSpanResult(span, err)
	if err == nil {
		span.SetAttributes(attribute.Int64("config.deleted_count", deleted))
	}
	return deleted, err
}

func (s *Store) deleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	deleted, err := bulkDelete(ctx, s.primary, namespace, keys)
	if err != nil {
		return 0, err
	}
	if s.opts.mode == ModeSync {
		for _, sec := range s.secondaries {
			if _, err := bulkDelete(ctx, sec, namespace, keys); err != nil {
				s.countReplicationError(ctx, "delete_many")
			}
		}
	}
	return deleted, nil
}

// GetVersions retrieves version history by delegating to the primary.
func (s *Store) GetVersions(ctx context.Context, namespace, key string, filter config.VersionFilter) (config.VersionPage, error) {
	if vs, ok := s.primary.(config.VersionedStore); ok {
		return vs.GetVersions(ctx, namespace, key, filter)
	}
	return nil, config.ErrVersioningNotSupported
}

// SupportsCodec delegates to the primary (writes always go through primary).
func (s *Store) SupportsCodec(codecName string) bool {
	if cv, ok := s.primary.(config.CodecValidator); ok {
		return cv.SupportsCodec(codecName)
	}
	return true
}

// SetAlias creates an alias on the primary and propagates to secondaries in ModeSync.
// Returns errAliasingNotSupported if the primary does not implement config.AliasStore.
func (s *Store) SetAlias(ctx context.Context, alias, target string) (config.Value, error) {
	as, ok := s.primary.(config.AliasStore)
	if !ok {
		return nil, errAliasingNotSupported
	}
	result, err := as.SetAlias(ctx, alias, target)
	if err != nil {
		return nil, err
	}
	if s.opts.mode == ModeSync {
		for _, sec := range s.secondaries {
			if secAs, ok := sec.(config.AliasStore); ok {
				if _, err := secAs.SetAlias(ctx, alias, target); err != nil {
					s.countReplicationError(ctx, "set_alias")
				}
			}
		}
	}
	return result, nil
}

// DeleteAlias removes an alias from the primary and propagates to secondaries in ModeSync.
// Returns errAliasingNotSupported if the primary does not implement config.AliasStore.
func (s *Store) DeleteAlias(ctx context.Context, alias string) error {
	as, ok := s.primary.(config.AliasStore)
	if !ok {
		return errAliasingNotSupported
	}
	if err := as.DeleteAlias(ctx, alias); err != nil {
		return err
	}
	if s.opts.mode == ModeSync {
		for _, sec := range s.secondaries {
			if secAs, ok := sec.(config.AliasStore); ok {
				if err := secAs.DeleteAlias(ctx, alias); err != nil && !config.IsNotFound(err) {
					s.countReplicationError(ctx, "delete_alias")
				}
			}
		}
	}
	return nil
}

// GetAlias retrieves the target for a specific alias from the primary.
// Returns errAliasingNotSupported if the primary does not implement config.AliasStore.
func (s *Store) GetAlias(ctx context.Context, alias string) (config.Value, error) {
	as, ok := s.primary.(config.AliasStore)
	if !ok {
		return nil, errAliasingNotSupported
	}
	return as.GetAlias(ctx, alias)
}

// ListAliases returns all registered aliases from the primary.
// Returns errAliasingNotSupported if the primary does not implement config.AliasStore.
func (s *Store) ListAliases(ctx context.Context) (map[string]config.Value, error) {
	as, ok := s.primary.(config.AliasStore)
	if !ok {
		return nil, errAliasingNotSupported
	}
	return as.ListAliases(ctx)
}

// initialSync copies all entries in the configured namespaces from primary to secondaries.
func (s *Store) initialSync(ctx context.Context) error {
	var span trace.Span
	if s.opts.enableTraces && s.tracer != nil {
		ctx, span = s.tracer.Start(ctx, "config.replica.InitialSync",
			trace.WithAttributes(append(s.commonAttrs(),
				attribute.Int("config.replica.namespace_count", len(s.opts.syncNamespaces)),
			)...))
		defer span.End()
	}

	start := time.Now()
	err := s.doInitialSync(ctx)

	if s.metrics != nil {
		s.metrics.syncDuration.Record(ctx, time.Since(start).Seconds())
	}
	if span != nil {
		setSpanResult(span, err)
	}
	return err
}

func (s *Store) doInitialSync(ctx context.Context) error {
	const pageSize = 100
	for _, ns := range s.opts.syncNamespaces {
		cursor := ""
		for {
			f := config.NewFilter().WithPrefix("").WithLimit(pageSize).WithCursor(cursor).Build()
			page, err := s.primary.Find(ctx, ns, f)
			if err != nil {
				return err
			}
			results := page.Results()
			if len(results) > 0 {
				for _, sec := range s.secondaries {
					if err := bulkSet(ctx, sec, ns, results); err != nil {
						s.countReplicationError(ctx, "initial_sync")
					}
				}
			}
			if len(results) < page.Limit() || page.NextCursor() == "" {
				break
			}
			cursor = page.NextCursor()
		}
	}
	return nil
}

// replicate watches the primary and applies change events to secondaries.
// Retries with backoff on watch failure; stops when ctx is cancelled.
func (s *Store) replicate(ctx context.Context, filter config.WatchFilter) {
	defer s.wg.Done()
	for {
		err := s.replicateLoop(ctx, filter)
		if err == nil || ctx.Err() != nil || errors.Is(err, config.ErrWatchNotSupported) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.opts.watchBackoff):
		}
	}
}

// replicateLoop runs one Watch session. It uses a derived context so the primary cleans
// up its watcher registration promptly when the loop exits (retry, close, or error).
func (s *Store) replicateLoop(ctx context.Context, filter config.WatchFilter) error {
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch, err := s.primary.Watch(loopCtx, filter)
	if err != nil {
		s.watchReadyOnce.Do(func() { close(s.watchReady) })
		return err
	}
	// Signal that the Watch is registered; Connect can unblock.
	s.watchReadyOnce.Do(func() { close(s.watchReady) })
	for {
		select {
		case <-loopCtx.Done():
			return loopCtx.Err()
		case event, ok := <-ch:
			if !ok {
				return errWatchChannelClosed
			}
			s.applyEvent(loopCtx, event)
		}
	}
}

// applyEvent routes a change event to all secondaries, recording metrics.
// Handles both regular and alias change events.
func (s *Store) applyEvent(ctx context.Context, event config.ChangeEvent) {
	lag := time.Since(event.Timestamp).Seconds()
	if s.metrics != nil {
		evAttrs := []attribute.KeyValue{attribute.String("event_type", event.Type.String())}
		s.metrics.replicationEvents.Add(ctx, 1, metric.WithAttributes(evAttrs...))
		s.metrics.replicationLag.Record(ctx, lag, metric.WithAttributes(evAttrs...))
	}

	for _, sec := range s.secondaries {
		var applyErr error
		switch event.Type {
		case config.ChangeTypeSet:
			if event.Value == nil {
				continue
			}
			_, applyErr = sec.Set(ctx, event.Namespace, event.Key, event.Value)
		case config.ChangeTypeDelete:
			applyErr = sec.Delete(ctx, event.Namespace, event.Key)
			if config.IsNotFound(applyErr) {
				applyErr = nil
			}
		case config.ChangeTypeAliasSet:
			if secAs, ok := sec.(config.AliasStore); ok && event.Value != nil {
				target, err := event.Value.String()
				if err != nil {
					s.countReplicationError(ctx, "alias_set")
					continue
				}
				_, applyErr = secAs.SetAlias(ctx, event.Key, target)
				if config.IsAliasExists(applyErr) {
					applyErr = nil // already replicated
				}
			}
		case config.ChangeTypeAliasDelete:
			if secAs, ok := sec.(config.AliasStore); ok {
				applyErr = secAs.DeleteAlias(ctx, event.Key)
				if config.IsNotFound(applyErr) {
					applyErr = nil
				}
			}
		}
		if applyErr != nil {
			s.countReplicationError(ctx, event.Type.String())
		}
	}
}

// propagateSet writes to all secondaries, metering failures.
func (s *Store) propagateSet(ctx context.Context, namespace, key string, value config.Value) {
	for _, sec := range s.secondaries {
		if _, err := sec.Set(ctx, namespace, key, value); err != nil {
			s.countReplicationError(ctx, "set")
		}
	}
}

// propagateDelete deletes from all secondaries, ignoring NotFound but metering other failures.
func (s *Store) propagateDelete(ctx context.Context, namespace, key string) {
	for _, sec := range s.secondaries {
		if err := sec.Delete(ctx, namespace, key); err != nil && !config.IsNotFound(err) {
			s.countReplicationError(ctx, "delete")
		}
	}
}

// --- OTel helpers ---

// commonAttrs returns the common OTel attributes for all spans and metrics.
// The slice has len==cap so callers can safely append without aliasing.
func (s *Store) commonAttrs() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs,
		attribute.String("config.backend", "replica"),
		attribute.String("config.replica.mode", s.opts.mode.String()),
	)
	if s.opts.backendName != "" {
		attrs = append(attrs, attribute.String("config.replica.backend", s.opts.backendName))
	}
	return attrs[:len(attrs):len(attrs)]
}

// recordOp records operation count, latency, and (on error) error count.
func (s *Store) recordOp(ctx context.Context, op, namespace string, start time.Time, err error) {
	if s.metrics == nil {
		return
	}
	latency := time.Since(start).Seconds()
	attrs := []attribute.KeyValue{attribute.String("operation", op)}
	if namespace != "" {
		attrs = append(attrs, attribute.String("namespace", namespace))
	}
	s.metrics.operationCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	s.metrics.operationLatency.Record(ctx, latency, metric.WithAttributes(attrs...))
	if err != nil {
		errAttrs := append(attrs, attribute.String("error_type", classifyError(err)))
		s.metrics.errorCount.Add(ctx, 1, metric.WithAttributes(errAttrs...))
	}
}

// countReplicationError bumps the replication error counter for the given operation.
func (s *Store) countReplicationError(ctx context.Context, op string) {
	if s.metrics == nil {
		return
	}
	s.metrics.replicationErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", op),
	))
}

// setSpanResult records the error (or ok) status on the span.
func setSpanResult(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
}

// valueTypeName returns value.Type().String() safely, guarding against nil.
func valueTypeName(v config.Value) string {
	if v == nil {
		return "unknown"
	}
	return v.Type().String()
}

// classifyError returns a short label for the error type used as a metric attribute.
func classifyError(err error) string {
	switch {
	case config.IsNotFound(err):
		return "not_found"
	case config.IsTypeMismatch(err):
		return "type_mismatch"
	case errors.Is(err, config.ErrStoreClosed), errors.Is(err, config.ErrStoreNotConnected):
		return "unavailable"
	default:
		return "internal"
	}
}

// --- isRetriable and bulk helpers ---

// isRetriable reports whether an error should cause a fallback to the next store.
func isRetriable(err error) bool {
	return config.IsNotFound(err) ||
		errors.Is(err, config.ErrStoreClosed) ||
		errors.Is(err, config.ErrStoreNotConnected)
}

// bulkGet retrieves keys from a store, using BulkStore if available.
func bulkGet(ctx context.Context, s config.Store, namespace string, keys []string) (map[string]config.Value, error) {
	if bulk, ok := s.(config.BulkStore); ok {
		return bulk.GetMany(ctx, namespace, keys)
	}
	results := make(map[string]config.Value, len(keys))
	for _, key := range keys {
		val, err := s.Get(ctx, namespace, key)
		if err == nil {
			results[key] = val
		} else if !config.IsNotFound(err) {
			return nil, err
		}
	}
	return results, nil
}

// bulkSet writes values to a store, using BulkStore if available.
func bulkSet(ctx context.Context, s config.Store, namespace string, values map[string]config.Value) error {
	if bulk, ok := s.(config.BulkStore); ok {
		return bulk.SetMany(ctx, namespace, values)
	}
	for key, val := range values {
		if _, err := s.Set(ctx, namespace, key, val); err != nil {
			return err
		}
	}
	return nil
}

// bulkDelete removes keys from a store, using BulkStore if available.
func bulkDelete(ctx context.Context, s config.Store, namespace string, keys []string) (int64, error) {
	if bulk, ok := s.(config.BulkStore); ok {
		return bulk.DeleteMany(ctx, namespace, keys)
	}
	var deleted int64
	for _, key := range keys {
		if err := s.Delete(ctx, namespace, key); err == nil {
			deleted++
		} else if !config.IsNotFound(err) {
			return deleted, err
		}
	}
	return deleted, nil
}
