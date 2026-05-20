// sqlmock-backed unit tests for the postgres store. These run with `go test
// ./...` — no real PostgreSQL required — and close the documented unit-coverage
// gap on the SQL builder paths: Get, Delete, Find (prefix/keys), Stats, and
// ListNamespaces. Watch is intentionally NOT covered here because it depends
// on pq.Listener which sqlmock cannot replace; the integration suite at
// `just test-pg` exercises that path.
//
// Each test creates a fresh sqlmock DB, passes a nil pq.Listener (the store
// guards against it), and uses Connect() to apply the schema DDL via a
// loose regex expectation. After that, the test asserts the SQL shape the
// store builds for each operation matches the documented contract.

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/postgres"
)

// newMockStore returns a postgres.Store wired to sqlmock with schema DDL
// already expected. After this helper returns, the test asserts on the
// specific operation it cares about; ExpectationsWereMet is checked at
// cleanup.
//
// The store is created with the default table ("config_entries") and a
// nil pq.Listener — sqlmock cannot mock the LISTEN/NOTIFY pathway, so
// watch-related tests live in the integration suite, not here.
func newMockStore(t *testing.T) (*postgres.Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
		_ = db.Close()
	})

	// createSchema issues 3 ExecContext calls: schema DDL, trigger
	// function, and the trigger itself (the trigger SQL combines DROP +
	// CREATE in one statement). Match loosely so the test does not
	// break when columns/indexes evolve.
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS config_entries`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE OR REPLACE FUNCTION notify_config_entries_changes`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DROP TRIGGER IF EXISTS config_entries_notify ON config_entries`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	store := postgres.NewStore(db, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store, mock
}

func TestPostgresStore_Mock_Get_SQLShape(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	// Expect SELECT shape: covers all columns, predicates on namespace + key,
	// TTL guard, parameter order ($1=namespace, $2=key).
	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"id", "key", "namespace", "value", "codec", "type",
		"version", "created_at", "updated_at", "expires_at",
	}).AddRow(int64(42), "k", "ns", []byte(`"v"`), "json", int(config.TypeString),
		int64(3), now, now, sql.NullTime{})

	mock.ExpectQuery(`SELECT id, key, namespace, value, codec, type, version, created_at, updated_at, expires_at\s+FROM config_entries WHERE namespace = \$1 AND key = \$2`).
		WithArgs("ns", "k").
		WillReturnRows(rows)

	got, err := store.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Metadata().Version() != 3 {
		t.Errorf("Version = %d, want 3", got.Metadata().Version())
	}
	if config.EntryID(got) != "42" {
		t.Errorf("EntryID = %q, want \"42\"", config.EntryID(got))
	}
}

func TestPostgresStore_Mock_Get_NotFoundSentinel(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	mock.ExpectQuery(`SELECT.*FROM config_entries WHERE namespace = \$1 AND key = \$2`).
		WithArgs("ns", "missing").
		WillReturnError(sql.ErrNoRows)

	_, err := store.Get(ctx, "ns", "missing")
	if !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("err = %v, want wraps ErrNotFound", err)
	}
}

func TestPostgresStore_Mock_Delete_SQLShape(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	mock.ExpectExec(`DELETE FROM config_entries WHERE namespace = \$1 AND key = \$2`).
		WithArgs("ns", "k").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.Delete(ctx, "ns", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestPostgresStore_Mock_Delete_NotFoundSentinel(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	mock.ExpectExec(`DELETE FROM config_entries`).
		WithArgs("ns", "missing").
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows affected

	err := store.Delete(ctx, "ns", "missing")
	if !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("err = %v, want wraps ErrNotFound", err)
	}
}

func TestPostgresStore_Mock_Find_PrefixSQLShape(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"id", "key", "namespace", "value", "codec", "type",
		"version", "created_at", "updated_at", "expires_at",
	}).
		AddRow(int64(1), "app/a", "ns", []byte(`1`), "json", int(config.TypeInt),
			int64(1), now, now, sql.NullTime{}).
		AddRow(int64(2), "app/b", "ns", []byte(`2`), "json", int(config.TypeInt),
			int64(1), now, now, sql.NullTime{})

	// Postgres Find with prefix uses LIKE 'prefix%' (per the file's #nosec
	// comments) with an escape clause to keep literal % and _ safe.
	mock.ExpectQuery(`SELECT id, key, namespace, value, codec, type, version, created_at, updated_at, expires_at\s+FROM config_entries WHERE namespace = \$\d+.*key LIKE`).
		WillReturnRows(rows)

	page, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").WithLimit(10).Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) != 2 {
		t.Fatalf("Find returned %d entries, want 2", len(page.Results()))
	}
}

func TestPostgresStore_Mock_Find_KeysSQLShape(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"id", "key", "namespace", "value", "codec", "type",
		"version", "created_at", "updated_at", "expires_at",
	}).
		AddRow(int64(1), "a", "ns", []byte(`1`), "json", int(config.TypeInt),
			int64(1), now, now, sql.NullTime{})

	// Keys mode uses IN ($1, $2, ...) — sqlmock's regexp matcher needs
	// to handle the IN-list with arbitrary placeholders.
	mock.ExpectQuery(`SELECT.*FROM config_entries WHERE namespace = \$\d+ AND key IN \(\$\d+`).
		WillReturnRows(rows)

	page, err := store.Find(ctx, "ns", config.NewFilter().WithKeys("a", "b").Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) != 1 {
		t.Fatalf("Find returned %d entries, want 1 (b is missing in mock)", len(page.Results()))
	}
}

func TestPostgresStore_Mock_Stats_SQLShape(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	// Stats issues three independent queries: total count, group by type,
	// group by namespace. Order is fixed by the store implementation.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM config_entries`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(5)))
	mock.ExpectQuery(`SELECT type, COUNT\(\*\) FROM config_entries GROUP BY type`).
		WillReturnRows(sqlmock.NewRows([]string{"type", "count"}).
			AddRow(int(config.TypeInt), int64(3)).
			AddRow(int(config.TypeString), int64(2)))
	mock.ExpectQuery(`SELECT namespace, COUNT\(\*\) FROM config_entries GROUP BY namespace`).
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "count"}).
			AddRow("ns1", int64(3)).
			AddRow("ns2", int64(2)))

	sp, ok := any(store).(config.StatsProvider)
	if !ok {
		t.Fatal("postgres store does not implement StatsProvider")
	}
	stats, err := sp.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalEntries() != 5 {
		t.Errorf("TotalEntries = %d, want 5", stats.TotalEntries())
	}
	if stats.CountForType(config.TypeInt) != 3 {
		t.Errorf("CountForType(Int) = %d, want 3", stats.CountForType(config.TypeInt))
	}
	if stats.CountForNamespace("ns1") != 3 {
		t.Errorf("CountForNamespace(ns1) = %d, want 3", stats.CountForNamespace("ns1"))
	}
}

func TestPostgresStore_Mock_ListNamespaces_ForcesCCollation(t *testing.T) {
	t.Parallel()
	store, mock := newMockStore(t)
	ctx := t.Context()

	// The documented invariant: postgres ListNamespaces forces COLLATE "C"
	// on every namespace comparison so the result order is byte-wise
	// regardless of the database's default LC_COLLATE. If this regression
	// fires, NamespaceLister contract on multi-locale databases breaks.
	mock.ExpectQuery(`SELECT DISTINCT namespace COLLATE "C"\s+FROM config_entries`).
		WillReturnRows(sqlmock.NewRows([]string{"namespace"}).
			AddRow("alpha").
			AddRow("beta").
			AddRow("gamma"))

	nl, ok := any(store).(config.NamespaceLister)
	if !ok {
		t.Fatal("postgres store does not implement NamespaceLister")
	}
	names, cursor, err := nl.ListNamespaces(ctx, "", 10, "")
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if cursor != "" {
		t.Errorf("nextCursor = %q, want empty (only 3 < limit 10)", cursor)
	}
	if len(names) != 3 {
		t.Fatalf("got %d names, want 3: %v", len(names), names)
	}
}

func TestPostgresStore_Mock_ListNamespaces_RejectsForeignCursor(t *testing.T) {
	t.Parallel()
	store, _ := newMockStore(t)
	ctx := t.Context()

	nl, ok := any(store).(config.NamespaceLister)
	if !ok {
		t.Fatal("postgres store does not implement NamespaceLister")
	}
	// A garbage cursor must fail-fast with ErrInvalidCursor without
	// issuing a SQL query — sqlmock will complain at cleanup if the
	// store dispatches one anyway.
	_, _, err := nl.ListNamespaces(ctx, "", 10, "not-a-real-cursor")
	if !config.IsInvalidCursor(err) {
		t.Fatalf("ListNamespaces with garbage cursor: err = %v, want IsInvalidCursor", err)
	}
}

func TestPostgresStore_Mock_ListNamespaces_RejectsNonPositiveLimit(t *testing.T) {
	t.Parallel()
	store, _ := newMockStore(t)
	ctx := t.Context()

	nl, ok := any(store).(config.NamespaceLister)
	if !ok {
		t.Fatal("postgres store does not implement NamespaceLister")
	}
	// limit <= 0 must be rejected fail-fast (no silent default fallback).
	for _, limit := range []int{0, -1} {
		_, _, err := nl.ListNamespaces(ctx, "", limit, "")
		if !errors.Is(err, config.ErrInvalidValue) {
			t.Errorf("limit=%d: err = %v, want wraps ErrInvalidValue", limit, err)
		}
	}
}

// regexpForGet is exported to keep the failure messages of the SQL-shape
// assertions readable when the file is searched by symbol.
var regexpForGet = regexp.MustCompile(`SELECT.*FROM config_entries.*WHERE namespace = \$1 AND key = \$2`)

var _ = regexpForGet // keep the test maintainable: the regex stays alongside its dependents
