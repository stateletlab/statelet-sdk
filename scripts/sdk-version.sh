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
# Print the version an SDK's own manifest declares, and optionally assert that a
# release tag agrees with it.
#
#   ./scripts/sdk-version.sh rust           # -> 0.1.0
#   ./scripts/sdk-version.sh rust 0.1.0     # -> 0.1.0, exit 0
#   ./scripts/sdk-version.sh rust 0.2.0     # -> error, exit 1
#
# Six manifests in five formats is why this exists as a script instead of six
# near-identical greps inlined in the release workflow. The second argument is
# the version parsed off the release tag; passing it empty (a manual
# workflow_dispatch, where there is no tag) skips the comparison, which is the
# only sanctioned way to publish a version the tag does not name.
#
# Go is the odd one out: a Go module has no manifest field for its own version —
# the `go/vX.Y.Z` tag IS the version — so there is nothing to cross-check and the
# expected version is echoed straight back.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    echo "usage: $0 <python|nodejs|rust|go|java|cpp> [expected-version]" >&2
    exit 2
}

[ $# -ge 1 ] || usage
SDK="$1"
EXPECTED="${2:-}"

die() { echo "::error::$*" >&2; exit 1; }

# Pull the first capture group of a regex out of a file, failing loudly rather
# than returning the empty string — a silent empty version would compare unequal
# to every tag and produce a baffling error one step later.
extract() {
    local file="$1" regex="$2" label="$3" value
    [ -f "$file" ] || die "$label: $file does not exist"
    value="$(sed -nE "s/$regex/\1/p" "$file" | head -1)"
    [ -n "$value" ] || die "$label: no version matched in $file"
    printf '%s' "$value"
}

case "$SDK" in
    python)
        VERSION="$(extract "$REPO_ROOT/python/pyproject.toml" \
            '^version[[:space:]]*=[[:space:]]*"([^"]+)".*' python)"
        ;;
    nodejs)
        VERSION="$(extract "$REPO_ROOT/nodejs/package.json" \
            '^[[:space:]]*"version"[[:space:]]*:[[:space:]]*"([^"]+)".*' nodejs)"
        ;;
    rust)
        # Cargo.toml has exactly one [package] table and `version` is declared in
        # it before any dependency table, so the first match is the crate's own.
        VERSION="$(extract "$REPO_ROOT/rust/Cargo.toml" \
            '^version[[:space:]]*=[[:space:]]*"([^"]+)".*' rust)"
        ;;
    cpp)
        VERSION="$(extract "$REPO_ROOT/cpp/CMakeLists.txt" \
            '^project\(.*VERSION[[:space:]]+([0-9]+\.[0-9]+\.[0-9]+).*' cpp)"
        ;;
    java)
        # The project version is the first <version> AFTER the project's own
        # <artifactId>; grabbing the first <version> in the file would pick up
        # <modelVersion>, and grabbing the last would pick up a dependency's.
        POM="$REPO_ROOT/java/pom.xml"
        [ -f "$POM" ] || die "java: $POM does not exist"
        VERSION="$(sed -n '/<artifactId>statelet-sdk<\/artifactId>/,$p' "$POM" \
            | sed -nE 's|^[[:space:]]*<version>([^<]+)</version>.*|\1|p' | head -1)"
        [ -n "$VERSION" ] || die "java: no project version matched in $POM"
        ;;
    go)
        [ -n "$EXPECTED" ] || die "go: a Go module declares no version of its own — pass the version from the go/vX.Y.Z tag"
        VERSION="$EXPECTED"
        ;;
    *)
        usage
        ;;
esac

if [ -n "$EXPECTED" ] && [ "$EXPECTED" != "$VERSION" ]; then
    die "$SDK: release tag says $EXPECTED but the manifest declares $VERSION"
fi

echo "$VERSION"
