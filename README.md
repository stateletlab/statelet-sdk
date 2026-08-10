# Statelet SDKs

Official client libraries for [Statelet](https://github.com/stateletlab/statelet) — an
AI-native distributed database that unifies key-value storage, vector search, and a
temporal causal graph in one engine.

All six SDKs wrap the same gRPC contract: [`proto/statelet.proto`](proto/statelet.proto).

| Language | Directory | Package | Transport |
|---|---|---|---|
| Python | [`python/`](python/) | `statelet-sdk` (PyPI) | grpcio |
| Node.js / TypeScript | [`nodejs/`](nodejs/) | `statelet-sdk` (npm) | `@grpc/grpc-js` |
| Go | [`go/`](go/) | `github.com/stateletlab/statelet-sdk/go` | grpc-go |
| Rust | [`rust/`](rust/) | `statelet-sdk` | tonic |
| Java | [`java/`](java/) | `ai.statelet:statelet-sdk` | grpc-java + Netty |
| C++ | [`cpp/`](cpp/) | `statelet_sdk` (CMake) | gRPC C++ |

Plus [`python/langchain-statelet/`](python/langchain-statelet/) — a LangChain integration
(`VectorStore`, `ChatHistory`, `GraphStore`) published separately as `langchain-statelet`.

## Quick start

```bash
pip install statelet-sdk                           # Python
npm install statelet-sdk                           # Node.js
go get github.com/stateletlab/statelet-sdk/go@latest  # Go
```

```python
from statelet import Client

db = Client("localhost:9379")
db.set("user:1", "alice")
print(db.get("user:1"))
```

You need a running Statelet cluster to talk to. The server lives in the
[engine repository](https://github.com/stateletlab/statelet) — this repository contains
clients only, no server binaries.

## Repository layout

```
proto/statelet.proto     ← single source of truth for the wire contract
scripts/sync-proto.sh    ← fans it out to the per-language vendored copies
python/  nodejs/  go/  rust/  java/  cpp/
```

### The proto, and why there are copies of it

`proto/statelet.proto` at the repository root is the **only** file to edit. Several
languages additionally keep a vendored copy inside their own directory:

| Copy | Needed because |
|---|---|
| `java/src/main/proto/` | `protobuf-maven-plugin` reads from `protoSourceRoot` |
| `rust/proto/` | `build.rs` runs inside the published crate, which contains only its own directory |
| `nodejs/proto/` | loaded at runtime by `@grpc/proto-loader`, so it must ship in the npm tarball |
| `cpp/proto/` | keeps the directory buildable standalone via `FetchContent`/`add_subdirectory` |

Regenerate them after touching the root proto:

```bash
./scripts/sync-proto.sh           # rewrite the copies
./scripts/sync-proto.sh --check   # CI gate: fails if any copy drifted
```

Go and Python instead commit **generated** stubs (`go/statelet/proto/*.pb.go`,
`python/statelet/statelet_pb2*.py`), so their consumers never need `protoc`. Regenerate
those with `make proto` in the respective directory. Never hand-edit generated code.

## Development

```bash
cd python  && pip install -e ".[dev]" && pytest tests/
cd nodejs  && npm install && npm test
cd go      && go test ./...
cd rust    && cargo test
cd java    && mvn test
cd cpp     && cmake -B build && cmake --build build && ctest --test-dir build
```

## Versioning and releases

Each SDK versions independently, and each is released by pushing a prefixed tag:

| Package | Tag |
|---|---|
| `statelet` (PyPI) | `python-v0.1.1` |
| `langchain-statelet` (PyPI) | `langchain-v0.1.0` |
| Go module | `go/v0.1.0` |

PyPI releases use [trusted publishing](https://docs.pypi.org/trusted-publishers/) — no
API tokens are stored in this repository.

## History

These SDKs previously lived under `sdk/` in the
[engine repository](https://github.com/stateletlab/statelet) and were split out so client
releases stop churning engine history. See [CHANGELOG.md](CHANGELOG.md) for what changed
in the move — the Go import path and the Python package contents both changed.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
