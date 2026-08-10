# Contributing

## The proto is the contract

`proto/statelet.proto` is a copy of the same file in the
[engine repository](https://github.com/stateletlab/statelet). The server defines the wire
contract; this repository consumes it. **Change the proto there first**, then copy the
updated file here and run:

```bash
./scripts/sync-proto.sh    # fan out to java/, rust/, nodejs/, cpp/
cd go     && make proto    # regenerate committed Go stubs
cd python && make proto    # regenerate committed Python stubs
```

CI runs `./scripts/sync-proto.sh --check` and fails on drift. Never hand-edit a vendored
copy or a generated stub.

### Field naming

A field with presence named `X` generates a `has_X()` accessor in C++ and Go's opaque
API. If a sibling field is also called `has_X`, the C++ codegen emits a redefinition and
does not compile — the other five languages build fine, so this only surfaces in the C++
job. Don't introduce a `has_`-prefixed field name.

## Per-SDK checks

```bash
cd python  && pip install -e ".[dev]" && pytest tests/
cd nodejs  && npm install && npm test
cd go      && go vet ./... && go test ./...
cd rust    && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
cd java    && mvn --batch-mode test
cd cpp     && cmake -B build && cmake --build build && ctest --test-dir build
```

## Keeping a package publishable

A published artifact contains only its own directory. Anything a build or runtime needs
must live inside it and be listed in the package manifest:

- `rust/Cargo.toml` → `include` must cover `proto/**` (`cargo package` verifies this)
- `nodejs/package.json` → `files` must list `proto` (CI checks `npm pack`)
- `python/pyproject.toml` → `[tool.setuptools] packages` is explicit, so the sibling
  `langchain-statelet/` is not swept into the `statelet` wheel

## Releases

Push a prefixed tag matching the version declared in the package manifest; the release
workflow refuses a mismatch.

| Package | Tag | Workflow |
|---|---|---|
| `statelet` (PyPI) | `python-v0.1.1` | `release-python.yml` |
| `langchain-statelet` (PyPI) | `langchain-v0.1.0` | `release-langchain.yml` |
| Go module | `go/v0.1.0` | none needed — the proxy serves tags directly |

PyPI uses trusted publishing; there are no tokens in this repository. npm, crates.io, and
Maven Central releases are still manual.

## License

Contributions are Apache-2.0. New source files carry the license header used throughout
the engine repository.
