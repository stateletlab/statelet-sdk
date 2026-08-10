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
# Write a version into an SDK's manifest. The inverse of sdk-version.sh, which
# reads one back, and the release workflow runs the two in sequence: set, then
# read to confirm the file really says what was asked for.
#
#   ./scripts/set-sdk-version.sh rust 0.2.0
#   ./scripts/set-sdk-version.sh all  0.2.0
#
# Lockfiles move with their manifest. `npm ci` and `cargo publish` both compare
# the two and fail on a mismatch, so bumping package.json without
# package-lock.json breaks the very next release rather than this one.
#
# Go accepts a version and does nothing with it: a Go module has no field for
# its own version anywhere in its tree — the go/vX.Y.Z tag IS the version. It is
# accepted rather than rejected so that `all` means all six without the caller
# special-casing it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALL_SDKS=(python nodejs rust go java cpp)

usage() {
    echo "usage: $0 <python|nodejs|rust|go|java|cpp|all> <version>" >&2
    exit 2
}

[ $# -eq 2 ] || usage
SDK="$1"
VERSION="$2"

die() { echo "::error::$*" >&2; exit 1; }

# Refuse anything that is not a plain dotted release. A stray `v` prefix or a
# trailing newline sails through sed and then fails much later, at upload time,
# with an error that says nothing about where it came from.
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]] \
    || die "'$VERSION' is not a version: expected MAJOR.MINOR.PATCH, with no leading 'v'"

# Replace the FIRST line matching a regex, and fail if there was none. Silent
# no-ops are the failure mode that matters here: the release would carry on and
# publish the old version under the new tag.
sub_first() {
    local file="$1" match="$2" replacement="$3" label="$4"
    [ -f "$file" ] || die "$label: $file does not exist"
    grep -qE "$match" "$file" || die "$label: nothing in $file matched /$match/"
    # -0 slurps the file so s/// hits the first match in it rather than one per
    # line; /m is what keeps ^ anchored to line starts under that slurp.
    perl -0pi -e "s{$match}{$replacement}m" "$file"
}

set_python() {
    sub_first "$REPO_ROOT/python/pyproject.toml" \
        '^version = "[^"]+"' "version = \"$VERSION\"" python
}

set_nodejs() {
    # JSON by hand is how lockfiles get corrupted; let Python round-trip it.
    # package-lock.json carries the version twice — once at the top level and
    # once in the "" entry of `packages` — and npm ci checks both.
    python3 - "$REPO_ROOT" "$VERSION" <<'PY'
import json, pathlib, sys
root, version = pathlib.Path(sys.argv[1]), sys.argv[2]

pkg_path = root / "nodejs" / "package.json"
pkg = json.loads(pkg_path.read_text())
pkg["version"] = version
pkg_path.write_text(json.dumps(pkg, indent=2) + "\n")

lock_path = root / "nodejs" / "package-lock.json"
lock = json.loads(lock_path.read_text())
lock["version"] = version
lock["packages"][""]["version"] = version
lock_path.write_text(json.dumps(lock, indent=2) + "\n")
PY
}

set_rust() {
    sub_first "$REPO_ROOT/rust/Cargo.toml" \
        '^version = "[^"]+"' "version = \"$VERSION\"" rust
    # In Cargo.lock the crate is one [[package]] block among its dependencies,
    # so the version to rewrite is the one directly under its own name line.
    python3 - "$REPO_ROOT" "$VERSION" <<'PY'
import pathlib, re, sys
root, version = pathlib.Path(sys.argv[1]), sys.argv[2]
path = root / "rust" / "Cargo.lock"
text = path.read_text()
new, n = re.subn(
    r'(name = "statelet-sdk"\nversion = ")[^"]+(")',
    rf'\g<1>{version}\g<2>',
    text,
)
if n != 1:
    raise SystemExit(f"::error::rust: expected one statelet-sdk block in Cargo.lock, found {n}")
path.write_text(new)
PY
}

set_java() {
    # The project version is the <version> directly after the project's own
    # <artifactId>; the first <version> in the file is <modelVersion> and the
    # ones further down belong to dependencies.
    python3 - "$REPO_ROOT" "$VERSION" <<'PY'
import pathlib, re, sys
root, version = pathlib.Path(sys.argv[1]), sys.argv[2]
path = root / "java" / "pom.xml"
text = path.read_text()
new, n = re.subn(
    r'(<artifactId>statelet-sdk</artifactId>\s*\n\s*<version>)[^<]+(</version>)',
    rf'\g<1>{version}\g<2>',
    text,
    count=1,
)
if n != 1:
    raise SystemExit("::error::java: could not find the project <version> after <artifactId>statelet-sdk</artifactId>")
path.write_text(new)
PY
}

set_cpp() {
    sub_first "$REPO_ROOT/cpp/CMakeLists.txt" \
        'project\((\S+) VERSION [0-9]+\.[0-9]+\.[0-9]+' "project(\$1 VERSION $VERSION" cpp
}

set_go() {
    echo "go: nothing to write — a Go module's version is its go/v$VERSION tag"
}

set_one() {
    case "$1" in
        python) set_python ;;
        nodejs) set_nodejs ;;
        rust)   set_rust   ;;
        java)   set_java   ;;
        cpp)    set_cpp    ;;
        go)     set_go; return ;;
        *)      usage ;;
    esac
    # Read it back through the same parser the release workflow trusts. If these
    # two ever disagree, the release is the wrong place to find out.
    local got
    got="$("$REPO_ROOT/scripts/sdk-version.sh" "$1")"
    [ "$got" = "$VERSION" ] || die "$1: wrote $VERSION but the manifest reads back as $got"
    echo "$1: $VERSION"
}

if [ "$SDK" = all ]; then
    for s in "${ALL_SDKS[@]}"; do set_one "$s"; done
else
    set_one "$SDK"
fi
