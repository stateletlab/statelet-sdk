# Changelog

## Unreleased — split out of the engine repository

The six SDKs were moved here from `sdk/` in
[`stateletlab/statelet`](https://github.com/stateletlab/statelet). The client code itself
is unchanged; what moved, and what that breaks, is listed below.

### Breaking

- **Go import path.** `github.com/stateletlab/statelet/sdk/go/...` →
  `github.com/stateletlab/statelet-sdk/go/...`. The repository name is part of a Go
  module path, so a move cannot preserve it. The package name (`statelet`), every type,
  and every method are unchanged — updating the import lines is the whole migration.
  Releases are tagged `go/vX.Y.Z`, the nested-module convention the Go proxy expects.

- **The `statelet` Python package no longer ships server binaries.** It previously
  declared `statelet-gateway`, `statelet-metadata`, and `statelet-datanode` console
  scripts, which exec'd binaries cross-compiled from the Rust engine and dropped into
  `statelet/bin/` by the engine repository's release workflow. That engine source is not
  in this repository, so those entry points could never work here; `statelet/_cli.py` and
  `statelet/bin/` were removed along with them. `pip install statelet` now installs a
  pure client. Server distribution stays with the engine repository.

  No published release is affected — `statelet` had never been uploaded to PyPI.

### Fixed

- **Package metadata pointed at a domain that is not ours.** Five manifests and one README
  linked to `statelet.com`, which resolves to an Afternic parking page — its nameservers
  are the marketplace's and the page does nothing but redirect to `/lander`. The project
  site is `statelet.ai`. A registry page's Homepage link is the first thing a prospective
  user clicks, so every reference now points there.

  Timing matters, because crates.io and PyPI both freeze a version's metadata at publish.
  The Rust crate has not shipped yet, so it takes the corrected link from its first
  release. PyPI is half past that point: `statelet` 0.1.1 went out before the client
  distribution was renamed to `statelet-sdk`, and that artifact carries the dead link
  permanently — but `statelet-sdk` itself starts clean.

- **The npm package shipped no proto.** `statelet-client` listed only `dist` and
  `README.md` in `files`, while `client.ts` resolved the proto three directories above
  the package root — a path that exists in a repository checkout and in no installed
  tarball. The proto is now vendored at `nodejs/proto/statelet.proto`, included in
  `files`, and resolved relative to the package root.

- **The Rust crate could not have been published.** `build.rs` compiled
  `../../proto/statelet.proto`, outside the crate directory, so a packaged crate would
  fail at build time. The proto is now vendored at `rust/proto/` and listed in `include`.

- **The Java SDK did not compile against the current proto.** `java/` had been pinned to
  a stale vendored proto (3137 lines, against the engine's 4736); syncing it to the real
  contract surfaced a name collision that only the Java codegen hits:

  ```protobuf
  WriteConditionKind condition       = 2;   // enum  → int         getConditionValue()
  bytes              condition_value = 3;   // bytes → ByteString  getConditionValue()
  ```

  protoc-gen-java emits `get<Field>Value()` for an enum field, so `condition` and
  `condition_value` produced two methods with the same signature and javac rejected the
  generated file. It affected `ConditionalBatchWriteRequest` and
  `AgentStateConditionalWriteRequest`; the other five languages are unaffected — this is
  the Java analogue of the `has_edge` collision that previously broke the C++ codegen.

  Fixed here rather than in the engine: **protoc 30 and newer detect the collision and
  disambiguate by field number**, so the bytes accessor becomes `getConditionValue3()`
  and the enum keeps `getConditionValue()`. Raising `protobuf.version` from `3.25.3` to
  `4.31.1` is therefore the whole fix — no proto edit, no field rename, no change to the
  other five SDKs, and the wire format is untouched. `protobuf.version` now carries a
  comment recording that 4.30 is a floor, not a preference.

  A field rename in the engine remains the tidier long-term answer, since
  `getConditionValue3()` is an unlovely name to hand a caller, but it is no longer
  blocking. Nothing in `ai.statelet.client` calls these accessors — only code driving the
  generated stubs directly is affected.

- **The Java round-trip test imported a generated class that no longer exists.**
  `VectorGroupingTest` imported `statelet.Statelet.*`, from before the proto declared
  `package statelet.v1` and `java_outer_classname = "StateletProto"`. The compile error
  above fired first, so this one had never been reached. Imports now point at
  `statelet.v1.StateletProto`, and `mvn test` is green.

### Changed

- `proto/statelet.proto` at the repository root is the single source of truth;
  `scripts/sync-proto.sh` fans it out to the per-language vendored copies and
  `--check` gates drift in CI.
- Build inputs that referenced `../../proto` now reference `../proto`, following the
  flattened layout (`sdk/python/` → `python/`, and so on).
- Added repository, homepage, and issue-tracker metadata to the Python, Node.js, Rust,
  and Java package manifests; added license and SCM blocks to `java/pom.xml`.

- **All six SDKs now release from one workflow.** `release-python.yml` is replaced by
  `release.yml`, which covers Python, Node.js, Rust, Java, Go and C++. Versions stay
  per-SDK — each is released by its own prefixed tag (`python-v*`, `nodejs-v*`,
  `rust-v*`, `java-v*`, `cpp-v*`, `go/v*`) against the version its own manifest declares.
  `scripts/sdk-version.sh` parses all six manifest formats and is what enforces the
  match; the old inline grep only ran on tag pushes, so a manual dispatch could publish
  an unchecked version.

  Two of the six have no registry, and the workflow says so rather than pretending
  otherwise. Go is released by its tag alone: the job verifies the tagged tree and warms
  `proxy.golang.org`, and a manual dispatch logs that it published nothing. C++ ships a
  self-contained source tarball, plus a `.sha256`, attached to the GitHub Release.

  **Action required before the next PyPI release:** a trusted publisher is keyed on the
  workflow filename, so the pending publisher on PyPI has to be re-pointed from
  `release-python.yml` to `release.yml`.

- `java/pom.xml` gained a `release` profile carrying what Maven Central requires —
  sources and javadoc jars, GPG signing, `<developers>`, and the Central Portal
  publishing plugin. It is off by default so `mvn test` needs no signing key.
