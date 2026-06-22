# Benchmarks

This repo ships a suite of Go benchmarks covering the hot paths: the memory
store (Get/Set/Find/bulk/parallel), `Value` create/marshal/unmarshal, the cache
hit path, and the json/yaml/toml codecs (Encode and Decode, parameterized by
payload size).

## Running

```bash
# All benchmarks, 10 samples each, written to bench.txt (variance-aware):
just bench

# Or directly:
go test -run '^$' -bench=. -benchmem -count=10 ./... | tee bench.txt

# A single benchmark:
go test -run '^$' -bench=BenchmarkStoreFind -benchmem ./...

# A single package:
go test -run '^$' -bench=. -benchmem ./codec/json/...
```

`-run '^$'` disables unit tests so only benchmarks run. `-benchmem` reports
allocations (B/op, allocs/op). All benchmarks are hermetic — no external
services are required, so they are deterministic and safe to run in CI.

## Comparing against a baseline

`baseline.txt` is a committed reference run (single `-count`). To check whether
a change regressed performance, capture a fresh run and diff with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
just bench   # produces bench.txt
go run golang.org/x/perf/cmd/benchstat@latest baseline.txt bench.txt
```

For a statistically meaningful comparison run both sides with `-count=10` (or
more) so benchstat can estimate variance.

## Sub-benchmarks

Several benchmarks are parameterized so cost scaling is visible:

- `BenchmarkStoreFind/{100,1000,10000}` — Find is O(n) over the namespace.
- `BenchmarkStoreGetMany|SetMany|DeleteMany/{10,100,1000}` — bulk ops; each
  reports a `keys/op` metric so per-element cost is comparable across sizes.
- `BenchmarkNewValueByType/{int,string,bool,float,map,slice}` — type detection.
- `codec/*` `BenchmarkEncode|Decode/{small,medium,large}` — per-format,
  per-size encode and decode.

Benchmarks that repeatedly write the same keys cap version history via
`memory.WithMaxHistory` so they measure steady-state write cost rather than
O(n²) history accumulation.

## CI

`.github/workflows/bench.yml` runs the suite on pull requests and nightly. On a
PR it benchmarks both the PR head and the base commit and compares them with
`benchstat`. The job deliberately does **not** run under `-race`, since race
instrumentation distorts timings.
