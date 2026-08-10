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

- **The npm package shipped no proto.** `statelet-client` listed only `dist` and
  `README.md` in `files`, while `client.ts` resolved the proto three directories above
  the package root — a path that exists in a repository checkout and in no installed
  tarball. The proto is now vendored at `nodejs/proto/statelet.proto`, included in
  `files`, and resolved relative to the package root.

- **The Rust crate could not have been published.** `build.rs` compiled
  `../../proto/statelet.proto`, outside the crate directory, so a packaged crate would
  fail at build time. The proto is now vendored at `rust/proto/` and listed in `include`.

### Known issues

- **The Java SDK does not compile against the current proto.** `java/` had been pinned to
  a stale vendored proto (3137 lines, against the engine's 4736); syncing it to the real
  contract surfaced a name collision that only the Java codegen hits:

  ```protobuf
  WriteConditionKind condition       = 2;   // enum  → int         getConditionValue()
  bytes              condition_value = 3;   // bytes → ByteString  getConditionValue()
  ```

  protoc-gen-java emits `get<Field>Value()` for an enum field, so `condition` and
  `condition_value` produce two methods with the same signature and the generated file is
  rejected by javac. It affects `ConditionalBatchWriteRequest` and
  `AgentStateConditionalWriteRequest`. The other five languages are unaffected — this is
  the Java analogue of the `has_edge` collision that previously broke the C++ codegen.

  The fix is a field rename in the **engine** repository, which owns the contract, and is
  wire-compatible as long as the field numbers stay put. Until then the `java` CI job
  fails, deliberately: pinning the SDK back to a stale proto would only re-hide it.

### Changed

- `proto/statelet.proto` at the repository root is the single source of truth;
  `scripts/sync-proto.sh` fans it out to the per-language vendored copies and
  `--check` gates drift in CI.
- Build inputs that referenced `../../proto` now reference `../proto`, following the
  flattened layout (`sdk/python/` → `python/`, and so on).
- Added repository, homepage, and issue-tracker metadata to the Python, Node.js, Rust,
  and Java package manifests; added license and SCM blocks to `java/pom.xml`.
