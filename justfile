# Container runtime: podman (default) or docker. Override with DOCKER=docker.
DOCKER := env("DOCKER", "podman")

# MongoDB container settings
MONGO_CONTAINER := "config-mongo-test"
MONGO_PORT := "27019"
MONGO_IMAGE := "mongo:6.0"

# PostgreSQL container settings
PG_CONTAINER := "config-postgres-test"
PG_PORT := "5433"
PG_IMAGE := "postgres:16"
PG_USER := "config_test"
PG_PASS := "config_test"
PG_DB := "config_test"

# Redis container settings
REDIS_CONTAINER := "config-redis-test"
REDIS_PORT := "6379"
REDIS_IMAGE := "redis:7"

# etcd container settings
ETCD_CONTAINER := "config-etcd-test"
ETCD_PORT := "2379"
ETCD_IMAGE := "quay.io/coreos/etcd:v3.5.13"

# Default recipe
default:
    @just --list

# Build all Go packages
build:
    go build ./...

# Run unit tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Run the smoke suite: hermetic in-process tests + godoc Examples.
# Used as the fast pre-merge gate before slower integration runs.
smoke:
    go test -run '^TestSmoke|^Example' -timeout 30s ./...

# Per-package unit-coverage gate. Fails if any unit-runnable package drops
# below COVER_MIN (default 75%).
#
# Why per-package instead of a single repo-wide total:
#   The backend packages postgres/, mongodb/, etcd/, and redis/ only execute
#   their real logic when a database/broker is reachable (Docker, or a *_DSN /
#   *_URI env var). Without those services their tests skip and report
#   near-0% coverage. Folding them into one aggregate total silently masks
#   regressions in the always-runnable packages. This gate therefore measures
#   each unit-runnable package independently and EXCLUDES:
#     - the externally-gated backends (postgres/mongodb/etcd/redis), which are
#       instead covered by `just test-integration` in the CI integration job;
#     - test-support packages (internal/testutil has no tests of its own;
#       internal/storetest is a conformance suite exercised indirectly by the
#       backends that import it).
#
# Threshold: 75% floor. The always-runnable packages currently sit at 80%+.
# Override with `COVER_MIN=80 just test-cover-gate`; raise the default as
# coverage improves.
test-cover-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${COVER_MIN:=75.0}"
    excluded='/postgres|/mongodb|/etcd|/redis|/internal/testutil|/internal/storetest'
    pkgs=$(go list ./... | grep -Ev "${excluded}")
    fail=0
    # Parse `go test -cover`'s own per-package "coverage: NN.N% of statements"
    # line, which is the exact statement-weighted figure go reports.
    while read -r line; do
        case "${line}" in
            *"coverage:"*)
                pkg=$(echo "${line}" | awk '{print $2}')
                cov=$(echo "${line}" | sed -E 's/.*coverage: ([0-9.]+)%.*/\1/')
                echo "  ${pkg}: ${cov}%"
                awk -v p="${cov}" -v m="${COVER_MIN}" -v pk="${pkg}" \
                    'BEGIN { if (p+0 < m+0) { printf("  -> %s below %.1f%% gate\n", pk, m); exit 1 } }' \
                    || fail=1
                ;;
            *"[no test files]"*)
                : ;; # packages with no tests are not gated
        esac
    done < <(go test -cover ${pkgs})
    if [ "${fail}" -ne 0 ]; then
        echo "coverage gate FAILED (COVER_MIN=${COVER_MIN}%)"
        exit 1
    fi
    echo "coverage gate PASSED (all unit packages >= ${COVER_MIN}%)"

# Run all integration tests (MongoDB + PostgreSQL + Redis + etcd + SQLite)
test-integration: mongo-start pg-start redis-start etcd-start
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Running integration tests..."
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" \
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    REDIS_ADDR="localhost:{{REDIS_PORT}}" \
    ETCD_ENDPOINTS="localhost:{{ETCD_PORT}}" \
    go test -v -count=1 ./mongodb/... ./postgres/... ./sqlite/... ./redis/... ./etcd/...
    just mongo-stop
    just pg-stop
    just redis-stop
    just etcd-stop

# Run Redis integration tests only
test-redis: redis-start
    #!/usr/bin/env bash
    set -euo pipefail
    REDIS_ADDR="localhost:{{REDIS_PORT}}" go test -v -count=1 ./redis/...
    just redis-stop

# Run etcd integration tests only
test-etcd: etcd-start
    #!/usr/bin/env bash
    set -euo pipefail
    ETCD_ENDPOINTS="localhost:{{ETCD_PORT}}" go test -v -count=1 ./etcd/...
    just etcd-stop

# Run MongoDB integration tests only
test-mongo: mongo-start
    #!/usr/bin/env bash
    set -euo pipefail
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" go test -v -count=1 ./mongodb/...
    just mongo-stop

# Run PostgreSQL integration tests only
test-pg: pg-start
    #!/usr/bin/env bash
    set -euo pipefail
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" go test -v -count=1 ./postgres/...
    just pg-stop

# Run SQLite tests (no external services needed)
test-sqlite:
    go test -v -count=1 ./sqlite/...

# Run benchmarks (variance-aware, ready for benchstat).
# count=10 samples per benchmark; output saved to bench.txt.
# Compare against baseline.txt with:
#   go run golang.org/x/perf/cmd/benchstat@latest baseline.txt bench.txt
bench:
    go test -run '^$' -bench=. -benchmem -count=10 ./... | tee bench.txt

# Start MongoDB replica set for testing
mongo-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        echo "Removing existing container {{MONGO_CONTAINER}}..."
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
    fi
    echo "Starting MongoDB replica set on port {{MONGO_PORT}}..."
    {{DOCKER}} run -d --name {{MONGO_CONTAINER}} -p {{MONGO_PORT}}:27017 {{MONGO_IMAGE}} --replSet rs0
    echo "Waiting for MongoDB to start..."
    sleep 3
    {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --eval "rs.initiate()" > /dev/null
    echo "MongoDB ready on port {{MONGO_PORT}}"

# Stop MongoDB container
mongo-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
        echo "MongoDB container stopped"
    fi

# Start PostgreSQL for testing
pg-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        echo "Removing existing container {{PG_CONTAINER}}..."
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
    fi
    echo "Starting PostgreSQL on port {{PG_PORT}}..."
    {{DOCKER}} run -d --name {{PG_CONTAINER}} \
        -p {{PG_PORT}}:5432 \
        -e POSTGRES_USER={{PG_USER}} \
        -e POSTGRES_PASSWORD={{PG_PASS}} \
        -e POSTGRES_DB={{PG_DB}} \
        {{PG_IMAGE}}
    echo "Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{PG_CONTAINER}} pg_isready -U {{PG_USER}} > /dev/null 2>&1; then
            echo "PostgreSQL ready on port {{PG_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "PostgreSQL failed to start within 30 seconds"
    exit 1

# Stop PostgreSQL container
pg-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
        echo "PostgreSQL container stopped"
    fi

# Start Redis for testing
redis-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        echo "Removing existing container {{REDIS_CONTAINER}}..."
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null
    fi
    echo "Starting Redis on port {{REDIS_PORT}}..."
    {{DOCKER}} run -d --name {{REDIS_CONTAINER}} -p {{REDIS_PORT}}:6379 {{REDIS_IMAGE}}
    echo "Waiting for Redis to be ready..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{REDIS_CONTAINER}} redis-cli ping > /dev/null 2>&1; then
            echo "Redis ready on port {{REDIS_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "Redis failed to start within 30 seconds"
    exit 1

# Stop Redis container
redis-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null
        echo "Redis container stopped"
    fi

# Start single-node etcd for testing
etcd-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{ETCD_CONTAINER}}$"; then
        echo "Removing existing container {{ETCD_CONTAINER}}..."
        {{DOCKER}} rm -f {{ETCD_CONTAINER}} > /dev/null
    fi
    echo "Starting etcd on port {{ETCD_PORT}}..."
    {{DOCKER}} run -d --name {{ETCD_CONTAINER}} -p {{ETCD_PORT}}:2379 \
        -e ETCD_NAME=node1 \
        -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
        -e ETCD_ADVERTISE_CLIENT_URLS=http://127.0.0.1:2379 \
        -e ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380 \
        -e ETCD_INITIAL_ADVERTISE_PEER_URLS=http://127.0.0.1:2380 \
        -e ETCD_INITIAL_CLUSTER=node1=http://127.0.0.1:2380 \
        -e ETCD_INITIAL_CLUSTER_STATE=new \
        -e ETCD_INITIAL_CLUSTER_TOKEN=test-token \
        {{ETCD_IMAGE}}
    echo "Waiting for etcd to be ready..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{ETCD_CONTAINER}} etcdctl endpoint health > /dev/null 2>&1; then
            echo "etcd ready on port {{ETCD_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "etcd failed to start within 30 seconds"
    exit 1

# Stop etcd container
etcd-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{ETCD_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{ETCD_CONTAINER}} > /dev/null
        echo "etcd container stopped"
    fi

# Format Go code
fmt:
    go fmt ./...

# Run linter
lint:
    golangci-lint run

# Run go mod tidy
tidy:
    go mod tidy

# Clean up containers and test cache
clean: mongo-stop pg-stop redis-stop etcd-stop
    go clean -testcache

# Install mise tools
tools:
    mise install

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Run gosec SAST scan with the same flags CI uses (.github/workflows/security.yml).
# Filters to medium-severity / medium-confidence findings to match the CI gate so
# anything green locally is also green in CI. Use `just gosec-all` to inspect the
# longer tail of low-confidence findings.
gosec:
    gosec -severity medium -confidence medium -exclude-dir k8s/example ./...

# Run gosec with default severity/confidence to surface low-priority findings
# the CI gate ignores. Useful when reviewing legacy code, not for CI parity.
gosec-all:
    gosec -exclude-dir k8s/example ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
