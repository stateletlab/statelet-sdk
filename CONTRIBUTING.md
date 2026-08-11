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
| `statelet-sdk` (PyPI) | `python-v0.1.1` | PyPI | `python/pyproject.toml` |
| `statelet-sdk` (npm) | `nodejs-v0.1.0` | npm | `nodejs/package.json` |
| `statelet-sdk` (crate) | `rust-v0.1.0` | crates.io | `rust/Cargo.toml` |
| `ai.statelet:statelet-sdk` | `java-v0.1.0` | Maven Central | `java/pom.xml` |
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

### Releasing from the Actions tab instead

**Actions → Release SDKs → Run workflow** is the other way in, and it inverts who owns
the version: instead of editing manifests and then tagging to match, you give it `sdks`
and `version` and it does both.

| field | |
|---|---|
| `sdks` | `all`, or a comma-separated subset |
| `version` | e.g. `0.2.0`. Written into every selected manifest, committed, and only then built. Empty releases whatever the manifests already say. |
| `dry_run` | on by default |

One field sets all six, so a lockstep release needs no hand-editing. Versions are still
per-SDK — `sdks` decides who moves — so bumping two and leaving the rest behind is the
normal case rather than a workaround.

`scripts/set-sdk-version.sh` does the writing (`sdk-version.sh` reads it back to confirm),
and it moves lockfiles with their manifests: `npm ci` and `cargo publish` both compare the
two and fail on a mismatch, so a bumped `package.json` with a stale `package-lock.json`
breaks the *next* release rather than this one.

**The version is committed, not just written.** An artifact built from an uncommitted edit
corresponds to no revision anyone can check out, so `git checkout <tag>` would rebuild a
different version than the one on the registry. Every job after `plan` is pinned to the
commit `plan` produced, and the tags are cut from it too.

That commit is pushed with `GITHUB_TOKEN`, which raises no events — which is what stops
the workflow retriggering itself, and also means CI does not run on it. The release jobs
build and test that commit directly, so nothing ships unverified, but expect no check
marks against it on the branch.

Tags are created after the publish jobs succeed, never before, so a tag cannot end up
claiming a release that failed halfway. An existing tag is left alone with a warning.

**A dry run does not write the version to the branch.** It rehearses the rewrite in the
runner and throws it away, so the versions the jobs report are the committed ones, not the
one you typed. That is also why the per-SDK version check is relaxed on a dry run: the
rehearsal is what proves the version is writable, and asserting it against a tree that was
deliberately reverted would fail every dry run that names a version.

### Re-running a release that failed halfway

Just run it again with the same inputs. Each publish step asks its registry whether that
exact version is already there (`scripts/is-published.sh`, via `scripts/publish-gate.sh`)
and skips itself if so, so the re-run publishes only what is missing and then reaches the
tag job.

This matters more than it sounds. The tag job needs all six SDKs green in one run, and
before the check existed a single failed SDK made that unreachable forever: the repair run
died on the five that had already published, because no registry lets a version be
published twice. `0.1.2` went out to PyPI, npm, crates.io and Maven Central that way and
was tagged on none of them.

A registry that cannot be reached is a failure, not a skip — otherwise an outage would
look like a successful release that shipped nothing.

**Go borrows its version from its neighbours.** Every other SDK falls back to its own
manifest when `version` is empty; a Go module records its version nowhere — the
`go/vX.Y.Z` tag *is* the version — so there is nothing to fall back to. With the field
empty, `scripts/go-release-version.sh` takes the version the other SDKs in the same
release unanimously declare, since they are being shipped from the same commit in the same
run. Versions are per-SDK, so a disagreement is legitimate and means there is no single
version to lend: the run then stops in `plan`, before anything is published, and asks for
the field.

That fallback exists because its absence cost a release its tags. `0.1.3` was dispatched
the recommended way — manifests already bumped, `version` left empty — and Go was the one
job that could not survive it. Five SDKs published; the tag job, which needs all six,
never ran.

### Publishing credentials

PyPI and npm both use trusted publishing, so neither stores a token here. Both are keyed
on the workflow **filename**, `release.yml`, so renaming this file breaks them until the
registry-side entry is updated to match.

- PyPI: <https://pypi.org/manage/account/publishing/> (a *pending* publisher until the
  first release creates the project, then it converts on its own)
- npm: <https://www.npmjs.com/package/statelet-sdk/access> — owner `stateletlab`,
  repository `statelet-sdk`, workflow `release.yml`, environment `npm`

npm has no equivalent of PyPI's pending publishers: a trusted publisher can only be
attached to a package that already exists, so `statelet-sdk@0.1.0` was published by hand
to get past the chicken-and-egg. Nothing after it needs a token.

The rest are repository secrets:

| Secret | Used by |
|---|---|
| `CARGO_REGISTRY_TOKEN` | crates.io |
| `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` | Maven Central — Portal tokens, not the account password |
| `MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE` | Maven Central artifact signing; the key must be published to a keyserver |

Maven Central additionally needs the `ai.statelet` namespace verified in the Central
Portal before the first publish. The C++ job needs no secret — it uses the run's own
`GITHUB_TOKEN` to attach the tarball.

## License

Contributions are Apache-2.0. New source files carry the license header used throughout
the engine repository.
