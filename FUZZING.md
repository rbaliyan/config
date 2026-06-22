# Fuzzing

This repository is continuously fuzzed by
[ClusterFuzzLite](https://google.github.io/clusterfuzzlite/) (see
`.clusterfuzzlite/`) and ships native Go fuzz targets you can run locally.

## Targets

| Target | Package | What it checks |
|--------|---------|----------------|
| `FuzzJSONCodecDecode` | `codec/json` | JSON decode never panics; decode→encode→decode reaches a stable fixed point (data-corruption detector) |
| `FuzzYAMLCodecDecode` | `codec/yaml` | YAML decode never panics; round-trip fixed point on JSON-shaped values |
| `FuzzTOMLCodecDecode` | `codec/toml` | TOML decode never panics; round-trip fixed point on JSON-shaped values |
| `FuzzValidateKey` | `.` (root) | `ValidateKey` never panics on arbitrary keys |
| `FuzzValidateNamespace` | `.` (root) | `ValidateNamespace` never panics on arbitrary namespaces |
| `FuzzNewValueFromBytes` | `.` (root) | `NewValueFromBytes` + typed accessors (`Int64`/`Float64`/`String`/`Bool`) + `Marshal` never panic; Marshal output round-trips |
| `FuzzCursorUnmarshal` | `internal/cursor` | Marshal/Unmarshal round-trip identity; raw bytes never panic and errors wrap `config.ErrInvalidCursor`; foreign backend tags are rejected |
| `FuzzBindFlatMap` | `bind` | `FlatMapToStruct` + tag validation never panic; validation failures return errors wrapping `ErrValidationFailed` |

Each target carries a committed seed corpus under
`<pkg>/testdata/fuzz/<FuzzName>/`. The codec corpora are seeded from the real
configs in `file/testdata/` plus malformed and edge-case variants (deep
nesting, huge integers, duplicate keys, BOM, unicode).

## Running a target locally

Run a single target for a fixed duration:

```bash
go test -run '^$' -fuzz=FuzzCursorUnmarshal -fuzztime=20s ./internal/cursor
go test -run '^$' -fuzz=FuzzJSONCodecDecode -fuzztime=20s ./codec/json
go test -run '^$' -fuzz=FuzzNewValueFromBytes -fuzztime=20s .
go test -run '^$' -fuzz=FuzzBindFlatMap -fuzztime=20s ./bind
```

`-run '^$'` disables the normal unit tests so only the fuzz engine runs.
Drop `-fuzz` to execute just the committed seed corpus as a regular test
(this is what CI does on every push):

```bash
go test -run 'Fuzz' ./...
```

## Adding a new target

1. Write `FuzzXxx(f *testing.F)` in a `_test.go` file in the package under
   test. Seed it with `f.Add(...)` calls covering both valid and malformed
   inputs.
2. Prefer a **strong oracle** over decode-and-discard. Good oracles:
   - *Round-trip identity*: `decode(encode(x)) == x`, or a fixed-point variant
     when the codec normalises on the first pass.
   - *Typed-error invariant*: any returned error must satisfy `errors.Is`
     against the package's sentinel (e.g. `config.ErrInvalidCursor`,
     `bind.ErrValidationFailed`).
   - *No panic* on arbitrary bytes is the minimum bar, not the goal.
3. Keep targets **deterministic** — no time, randomness, network, or
   goroutines that outlive the call.
4. (Optional) Commit a seed corpus under
   `<pkg>/testdata/fuzz/<FuzzName>/` using the `go test fuzz v1` file format.
   When the exact serialization is awkward, richer `f.Add` seeds inside the
   harness are equally effective.
5. Register the target in `.clusterfuzzlite/build.sh` so CFLite picks it up:

   ```bash
   compile_native_go_fuzzer github.com/rbaliyan/config/<pkg> FuzzXxx fuzz_xxx
   ```

## Crash triage and committing a reproducer

When a fuzz run finds a crash, Go writes the failing input to
`<pkg>/testdata/fuzz/<FuzzName>/<hash>` and prints a re-run command:

```
go test -run=FuzzXxx/<hash> ./<pkg>
```

Triage flow:

1. **Reproduce** with the printed `-run` command. The crashing input is now a
   regression test — running the package tests replays it automatically.
2. **Classify**:
   - *Bug in production code* → fix the code. The committed crasher under
     `testdata/fuzz/` becomes the regression test guarding the fix; keep it.
   - *Bug in the harness / oracle too strict* → fix the harness to assert the
     intended invariant, and remove the spurious crasher file (it does not
     represent a real defect). Document any deliberately-excluded value space
     in the harness (see the `yamlUnsafe` / `containsTime` guards in the codec
     targets for upstream-library normalisation quirks that are excluded from
     the round-trip oracle).
3. **Re-run** the target to confirm the crash is gone and no new ones appear.

Always commit the `testdata/fuzz/...` reproducer for a genuine production bug —
it permanently pins the regression.
