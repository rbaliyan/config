package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func TestAliasResolveGet(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/timeout", "app/timeout"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Write using canonical key
	if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Read using alias
	val, err := cfg.Get(ctx, "old/timeout")
	if err != nil {
		t.Fatalf("Get via alias: %v", err)
	}
	got, _ := val.Int64()
	if got != 30 {
		t.Errorf("Get via alias = %d, want 30", got)
	}

	// Read using canonical key still works
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get via canonical: %v", err)
	}
	got, _ = val.Int64()
	if got != 30 {
		t.Errorf("Get via canonical = %d, want 30", got)
	}
}

func TestAliasResolveSet(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/timeout", "app/timeout"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Write using alias
	if err := cfg.Set(ctx, "old/timeout", 42); err != nil {
		t.Fatalf("Set via alias: %v", err)
	}

	// Read using canonical key
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get via canonical: %v", err)
	}
	got, _ := val.Int64()
	if got != 42 {
		t.Errorf("Get via canonical = %d, want 42", got)
	}
}

func TestAliasResolveDelete(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	if err := cfg.Set(ctx, "new/key", "value"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Delete using alias
	if err := cfg.Delete(ctx, "old/key"); err != nil {
		t.Fatalf("Delete via alias: %v", err)
	}

	// Canonical key should be gone
	_, err = cfg.Get(ctx, "new/key")
	if !config.IsNotFound(err) {
		t.Errorf("expected NotFound after alias delete, got: %v", err)
	}
}

func TestAliasKeyMigration(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAliases("database/host", "db.host"),
		config.WithAliases("database/port", "db.port"),
		config.WithAliases("database/timeout", "db.timeout"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("prod")

	// New code writes using canonical keys
	if err := cfg.Set(ctx, "database/host", "localhost"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := cfg.Set(ctx, "database/port", 5432); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Old code reads using legacy aliases
	val, err := cfg.Get(ctx, "db.host")
	if err != nil {
		t.Fatalf("Get via alias: %v", err)
	}
	s, _ := val.String()
	if s != "localhost" {
		t.Errorf("db.host = %q, want %q", s, "localhost")
	}

	val, err = cfg.Get(ctx, "db.port")
	if err != nil {
		t.Fatalf("Get via alias: %v", err)
	}
	got, _ := val.Int64()
	if got != 5432 {
		t.Errorf("db.port = %d, want 5432", got)
	}

	// Old code writes using alias — should write to canonical key
	if err := cfg.Set(ctx, "db.timeout", 30); err != nil {
		t.Fatalf("Set via alias: %v", err)
	}
	val, err = cfg.Get(ctx, "database/timeout")
	if err != nil {
		t.Fatalf("Get canonical: %v", err)
	}
	got, _ = val.Int64()
	if got != 30 {
		t.Errorf("database/timeout = %d, want 30", got)
	}
}

func TestAliasMultipleAliasesToSameTarget(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAliases("canonical/key", "old/key", "legacy/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	if err := cfg.Set(ctx, "canonical/key", "hello"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	for _, key := range []string{"old/key", "legacy/key", "canonical/key"} {
		val, err := cfg.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		s, _ := val.String()
		if s != "hello" {
			t.Errorf("Get(%q) = %q, want %q", key, s, "hello")
		}
	}
}

func TestAliasNamespaceIsolation(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	prod := mgr.Namespace("prod")
	dev := mgr.Namespace("dev")

	if err := prod.Set(ctx, "new/key", "prod-value"); err != nil {
		t.Fatalf("Set prod: %v", err)
	}
	if err := dev.Set(ctx, "new/key", "dev-value"); err != nil {
		t.Fatalf("Set dev: %v", err)
	}

	val, _ := prod.Get(ctx, "old/key")
	s, _ := val.String()
	if s != "prod-value" {
		t.Errorf("prod alias = %q, want %q", s, "prod-value")
	}

	val, _ = dev.Get(ctx, "old/key")
	s, _ = val.String()
	if s != "dev-value" {
		t.Errorf("dev alias = %q, want %q", s, "dev-value")
	}
}

func TestAliasSelfReferenceRejected(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)
	err = am.SetAlias(ctx, "key", "key")
	if err == nil {
		t.Fatal("expected error for self-referencing alias")
	}
}

func TestAliasChainRejected(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)

	if err := am.SetAlias(ctx, "a", "b"); err != nil {
		t.Fatalf("SetAlias a→b: %v", err)
	}

	// b→c should be rejected: b is a target of a
	err = am.SetAlias(ctx, "b", "c")
	if err == nil {
		t.Fatal("expected error for alias chain")
	}
}

func TestAliasDuplicateRejected(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)

	if err := am.SetAlias(ctx, "old/key", "new/key"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	// Same alias again should be rejected by the store (ErrAliasExists)
	err = am.SetAlias(ctx, "old/key", "other/key")
	if err == nil {
		t.Fatal("expected error for duplicate alias")
	}
	if !config.IsAliasExists(err) {
		t.Errorf("expected ErrAliasExists, got: %v", err)
	}
}

func TestAliasConflictsWithConfigKey(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Create a config entry first
	if err := cfg.Set(ctx, "db.host", "localhost"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Now try to create alias with same name — should fail
	am := mgr.(config.AliasManager)
	err = am.SetAlias(ctx, "db.host", "database/host")
	if err == nil {
		t.Fatal("expected error when alias conflicts with config key")
	}
	if !config.IsAliasExists(err) {
		t.Errorf("expected ErrAliasExists, got: %v", err)
	}
}

func TestAliasManagerInterface(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am, ok := mgr.(config.AliasManager)
	if !ok {
		t.Fatal("manager does not implement AliasManager")
	}

	// Check initial aliases (seed was persisted on Connect)
	aliases := am.Aliases()
	if len(aliases) != 1 {
		t.Errorf("Aliases() len = %d, want 1", len(aliases))
	}
	if aliases["old/key"] != "new/key" {
		t.Errorf("alias[old/key] = %q, want %q", aliases["old/key"], "new/key")
	}

	// ResolveAlias
	if got := am.ResolveAlias("old/key"); got != "new/key" {
		t.Errorf("ResolveAlias(old/key) = %q, want %q", got, "new/key")
	}
	if got := am.ResolveAlias("unknown"); got != "unknown" {
		t.Errorf("ResolveAlias(unknown) = %q, want %q", got, "unknown")
	}

	// SetAlias at runtime
	if err := am.SetAlias(ctx, "another/old", "another/new"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}
	aliases = am.Aliases()
	if len(aliases) != 2 {
		t.Errorf("Aliases() len = %d, want 2", len(aliases))
	}

	// RemoveAlias
	if err := am.RemoveAlias(ctx, "old/key"); err != nil {
		t.Fatalf("RemoveAlias: %v", err)
	}

	// Remove non-existent returns NotFound
	err = am.RemoveAlias(ctx, "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("expected NotFound for nonexistent alias, got: %v", err)
	}

	aliases = am.Aliases()
	if len(aliases) != 1 {
		t.Errorf("Aliases() len = %d, want 1", len(aliases))
	}
}

func TestAliasRuntimeSetAndUse(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")
	am := mgr.(config.AliasManager)

	if err := cfg.Set(ctx, "new/key", "hello"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// No alias yet — old key doesn't work
	_, err = cfg.Get(ctx, "old/key")
	if !config.IsNotFound(err) {
		t.Fatalf("expected NotFound before alias, got: %v", err)
	}

	// Add alias at runtime
	if err := am.SetAlias(ctx, "old/key", "new/key"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	// Now old key resolves
	val, err := cfg.Get(ctx, "old/key")
	if err != nil {
		t.Fatalf("Get via runtime alias: %v", err)
	}
	s, _ := val.String()
	if s != "hello" {
		t.Errorf("Get via runtime alias = %q, want %q", s, "hello")
	}

	// Remove alias
	if err := am.RemoveAlias(ctx, "old/key"); err != nil {
		t.Fatalf("RemoveAlias: %v", err)
	}
	_, err = cfg.Get(ctx, "old/key")
	if !config.IsNotFound(err) {
		t.Fatalf("expected NotFound after alias removal, got: %v", err)
	}
}

func TestAliasPersistence(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()

	// First manager creates an alias.
	mgr1, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New mgr1: %v", err)
	}
	if err := mgr1.Connect(ctx); err != nil {
		t.Fatalf("Connect mgr1: %v", err)
	}

	am1 := mgr1.(config.AliasManager)
	if err := am1.SetAlias(ctx, "old/key", "new/key"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	// Verify alias was persisted in the store.
	val, err := store.GetAlias(ctx, "old/key")
	if err != nil {
		t.Fatalf("GetAlias from store: %v", err)
	}
	target, _ := val.String()
	if target != "new/key" {
		t.Errorf("persisted target = %q, want %q", target, "new/key")
	}

	// Second manager on the same store should load the persisted alias on Connect.
	mgr2, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New mgr2: %v", err)
	}
	if err := mgr2.Connect(ctx); err != nil {
		t.Fatalf("Connect mgr2: %v", err)
	}

	am2 := mgr2.(config.AliasManager)
	aliases := am2.Aliases()
	if len(aliases) != 1 {
		t.Fatalf("mgr2 Aliases() len = %d, want 1", len(aliases))
	}
	if aliases["old/key"] != "new/key" {
		t.Errorf("mgr2 alias[old/key] = %q, want %q", aliases["old/key"], "new/key")
	}

	mgr2.Close(ctx)
	mgr1.Close(ctx)
}

func TestAliasSeedPersistence(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()

	// Manager with seed aliases — seeds should be persisted on Connect.
	mgr1, err := config.New(
		config.WithStore(store),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr1.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Seeds should be persisted in the store.
	val, err := store.GetAlias(ctx, "old/key")
	if err != nil {
		t.Fatalf("GetAlias: %v", err)
	}
	target, _ := val.String()
	if target != "new/key" {
		t.Errorf("persisted target = %q, want %q", target, "new/key")
	}

	// Second manager (no seeds) on same store should load the alias.
	mgr2, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New mgr2: %v", err)
	}
	if err := mgr2.Connect(ctx); err != nil {
		t.Fatalf("Connect mgr2: %v", err)
	}

	am2 := mgr2.(config.AliasManager)
	if got := am2.ResolveAlias("old/key"); got != "new/key" {
		t.Errorf("mgr2 ResolveAlias = %q, want %q", got, "new/key")
	}

	mgr2.Close(ctx)
	mgr1.Close(ctx)
}

func TestAliasWatchPropagation(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()

	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)

	// Let the watch goroutine start and register its watcher.
	time.Sleep(50 * time.Millisecond)

	// Create alias via the store directly (simulates another instance writing).
	if _, err := store.SetAlias(ctx, "external/alias", "external/target"); err != nil {
		t.Fatalf("store.SetAlias: %v", err)
	}

	// Wait for the watch event to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := am.ResolveAlias("external/alias"); got == "external/target" {
			return // Success
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("ResolveAlias(external/alias) never resolved to %q", "external/target")
}

func TestAliasGetVersions(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	for i := range 3 {
		if err := cfg.Set(ctx, "new/key", i); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	vr := cfg.(config.VersionedReader)
	page, err := vr.GetVersions(ctx, "old/key", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions via alias: %v", err)
	}
	if len(page.Versions()) != 3 {
		t.Errorf("GetVersions returned %d versions, want 3", len(page.Versions()))
	}
}

func TestAliasRefresh(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")
	if err := cfg.Set(ctx, "new/key", "value"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := mgr.Refresh(ctx, "test", "old/key"); err != nil {
		t.Fatalf("Refresh via alias: %v", err)
	}
}

func TestAliasWithConditionalWrites(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Create via alias with IfNotExists
	if err := cfg.Set(ctx, "old/key", "first", config.WithIfNotExists()); err != nil {
		t.Fatalf("Set via alias with IfNotExists: %v", err)
	}

	// Second create via canonical key should fail
	err = cfg.Set(ctx, "new/key", "second", config.WithIfNotExists())
	if !config.IsKeyExists(err) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}

	// Update via alias with IfExists
	if err := cfg.Set(ctx, "old/key", "updated", config.WithIfExists()); err != nil {
		t.Fatalf("Set via alias with IfExists: %v", err)
	}

	val, err := cfg.Get(ctx, "new/key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	s, _ := val.String()
	if s != "updated" {
		t.Errorf("value = %q, want %q", s, "updated")
	}
}

func TestAliasesSnapshotIsolation(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("a", "b"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)
	snap := am.Aliases()
	snap["x"] = "y"

	aliases := am.Aliases()
	if _, ok := aliases["x"]; ok {
		t.Error("mutating Aliases() snapshot affected internal state")
	}
}

func TestAliasContextHelpers(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("old/key", "new/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	ctx = config.ContextWithManager(ctx, mgr)
	ctx = config.ContextWithNamespace(ctx, "test")

	if err := config.Set(ctx, "old/key", "ctx-value"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := config.Get(ctx, "old/key")
	if err != nil {
		t.Fatalf("Get alias: %v", err)
	}
	s, _ := val.String()
	if s != "ctx-value" {
		t.Errorf("context Get = %q, want %q", s, "ctx-value")
	}

	val, err = config.Get(ctx, "new/key")
	if err != nil {
		t.Fatalf("Get canonical: %v", err)
	}
	s, _ = val.String()
	if s != "ctx-value" {
		t.Errorf("canonical Get = %q, want %q", s, "ctx-value")
	}
}

func TestAliasChangeTypes(t *testing.T) {
	if !config.ChangeTypeAliasSet.IsAliasChange() {
		t.Error("ChangeTypeAliasSet.IsAliasChange() = false")
	}
	if !config.ChangeTypeAliasDelete.IsAliasChange() {
		t.Error("ChangeTypeAliasDelete.IsAliasChange() = false")
	}
	if config.ChangeTypeSet.IsAliasChange() {
		t.Error("ChangeTypeSet.IsAliasChange() = true")
	}
	if config.ChangeTypeDelete.IsAliasChange() {
		t.Error("ChangeTypeDelete.IsAliasChange() = true")
	}
}

func TestAliasMatchesWatchFilter(t *testing.T) {
	// Alias events should always match, even with restrictive filters.
	event := config.ChangeEvent{
		Type: config.ChangeTypeAliasSet,
		Key:  "some/alias",
	}
	filter := config.WatchFilter{
		Namespaces: []string{"specific-ns"},
		Prefixes:   []string{"specific/"},
	}
	if !config.MatchesWatchFilter(event, filter) {
		t.Error("alias event should always match watch filter")
	}
}

func TestWithAliasesOption(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAliases("new/a", "old/a"),
		config.WithAliases("new/b", "old/b"),
		config.WithAliases("new/c", "old/c"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	am := mgr.(config.AliasManager)
	got := am.Aliases()
	if len(got) != 3 {
		t.Errorf("Aliases() len = %d, want 3", len(got))
	}

	expect := map[string]string{
		"old/a": "new/a",
		"old/b": "new/b",
		"old/c": "new/c",
	}
	for k, v := range expect {
		if got[k] != v {
			t.Errorf("alias[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestAliasNonAliasKeyUnchanged(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithAlias("aliased/key", "target/key"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	if err := cfg.Set(ctx, "regular/key", "value"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := cfg.Get(ctx, "regular/key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	s, _ := val.String()
	if s != "value" {
		t.Errorf("regular key = %q, want %q", s, "value")
	}
}
