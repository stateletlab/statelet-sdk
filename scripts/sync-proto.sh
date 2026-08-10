#!/usr/bin/env bash
# Copyright 2025 Statelet Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Fan `proto/statelet.proto` out to the per-language vendored copies.
#
# The root file is the single source of truth. Every copy exists because that
# package has to be self-contained once published: a crates.io crate, an npm
# tarball, and a Maven jar are all built from their own directory only, so a
# build script reaching up to `../proto` works from a git checkout and then
# breaks for anyone who installs the released artifact.
#
#   ./scripts/sync-proto.sh           # rewrite the copies
#   ./scripts/sync-proto.sh --check   # exit 1 if any copy is stale (CI)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="$REPO_ROOT/proto/statelet.proto"

# Destinations, relative to the repo root.
TARGETS=(
  "java/src/main/proto/statelet.proto"   # protobuf-maven-plugin protoSourceRoot
  "rust/proto/statelet.proto"            # tonic-build, must ship inside the crate
  "nodejs/proto/statelet.proto"          # loaded at runtime by @grpc/proto-loader
  "cpp/proto/statelet.proto"             # protoc invocation in CMakeLists.txt
)

if [[ ! -f "$SOURCE" ]]; then
  echo "error: source proto not found at $SOURCE" >&2
  exit 1
fi

CHECK_ONLY=0
if [[ "${1:-}" == "--check" ]]; then
  CHECK_ONLY=1
fi

stale=0
for target in "${TARGETS[@]}"; do
  dest="$REPO_ROOT/$target"
  if [[ "$CHECK_ONLY" == "1" ]]; then
    if [[ ! -f "$dest" ]]; then
      echo "MISSING  $target"
      stale=1
    elif ! cmp -s "$SOURCE" "$dest"; then
      echo "STALE    $target"
      stale=1
    else
      echo "ok       $target"
    fi
  else
    mkdir -p "$(dirname "$dest")"
    cp "$SOURCE" "$dest"
    echo "synced   $target"
  fi
done

if [[ "$CHECK_ONLY" == "1" && "$stale" == "1" ]]; then
  echo >&2
  echo "error: vendored proto copies are out of sync with proto/statelet.proto." >&2
  echo "       run ./scripts/sync-proto.sh and commit the result." >&2
  exit 1
fi
