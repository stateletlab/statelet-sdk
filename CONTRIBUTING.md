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

All six SDKs release from one workflow, `release.yml`. Versions are per-SDK, not
lockstep: push the prefixed tag for the one you are releasing, matching the version its
own manifest declares. `scripts/sdk-version.sh` is what compares the two, and the
workflow refuses a mismatch.

| SDK | Tag | Registry | Version comes from |
|---|---|---|---|
| `statelet` | `python-v0.1.1` | PyPI | `python/pyproject.toml` |
| `statelet-client` (npm) | `nodejs-v0.1.0` | npm | `nodejs/package.json` |
| `statelet-client` (crate) | `rust-v0.1.0` | crates.io | `rust/Cargo.toml` |
| `ai.statelet:statelet-client` | `java-v0.1.0` | Maven Central | `java/pom.xml` |
| Go module | `go/v0.1.0` | none — the proxy serves tags | the tag itself |
| C++ | `cpp-v0.1.0` | none — tarball on the GitHub Release | `cpp/CMakeLists.txt` |

```bash
git tag python-v0.1.1 && git push origin python-v0.1.1
git tag go/v0.1.0     && git push origin go/v0.1.0    # the go/ prefix is mandatory
```

Two SDKs have no registry to publish to. Go is released by its tag alone — the workflow
only verifies the tagged tree and warms `proxy.golang.org`. C++ gets a self-contained
source tarball attached to the GitHub Release for its tag.

`langchain-statelet` is an integration package, not an SDK, and keeps its own
`release-langchain.yml` (tag `langchain-v0.1.0`).

### Rehearsing a release

`release.yml` also takes a manual `workflow_dispatch` with an SDK list and a `dry_run`
flag that defaults to on. A dry run builds, tests and packages everything a real release
would, and stops short of publishing. Turning `dry_run` off on a dispatch publishes
whatever version is currently committed — there is no tag to check it against — so
prefer the tag path for anything real.

### Publishing credentials

PyPI uses trusted publishing: no token is stored here. The pending publisher at
<https://pypi.org/manage/account/publishing/> must name **`release.yml`** as the
workflow — it is keyed on the filename, and this replaced the old `release-python.yml`.

The rest are repository secrets:

| Secret | Used by |
|---|---|
| `NPM_TOKEN` | npm (an automation token; provenance is signed via OIDC separately) |
| `CARGO_REGISTRY_TOKEN` | crates.io |
| `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` | Maven Central — Portal tokens, not the account password |
| `MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE` | Maven Central artifact signing; the key must be published to a keyserver |

Maven Central additionally needs the `ai.statelet` namespace verified in the Central
Portal before the first publish. The C++ job needs no secret — it uses the run's own
`GITHUB_TOKEN` to attach the tarball.

## License

Contributions are Apache-2.0. New source files carry the license header used throughout
the engine repository.
