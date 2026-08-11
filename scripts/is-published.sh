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
# Ask a registry whether it already has a version of an SDK.
#
#   ./scripts/is-published.sh rust 0.1.2    # exit 0  — already on crates.io
#   ./scripts/is-published.sh rust 0.9.9    # exit 10 — not there yet
#
# Why this exists: six SDKs publish to six unrelated registries, and one of them
# failing leaves the other five already published. The re-run then dies on them
# — every registry refuses to overwrite a version — so the release never reaches
# the tag job, which needs all six green. 0.1.2 went out that way: published
# everywhere, tagged nowhere. Skipping what is already up makes the re-run mean
# "finish the release" instead of "publish everything again".
#
# THREE outcomes, not two. 0 is published and 10 is not, and anything else is
# "the registry did not answer clearly" — a distinct exit on purpose. Folding an
# unreachable registry into "already published" would silently skip a real
# publish and report a green release that shipped nothing; folding it into "not
# published" is harmless but pointless, since the publish that follows would hit
# the same unreachable registry. So it fails, and says which registry.
#
# The package NAMES come from the manifests rather than being written here.
# `statelet` was renamed to `statelet-sdk` once already, and a rename that this
# script did not follow would report "not published" forever and turn every
# release into a duplicate-publish failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PUBLISHED=0
NOT_PUBLISHED=10

usage() {
    echo "usage: $0 <python|nodejs|rust|java> <version>" >&2
    exit 2
}

[ $# -eq 2 ] || usage
SDK="$1"
VERSION="$2"

die() { echo "::error::$*" >&2; exit 1; }

# A HEAD against a URL that exists for a published version and 404s for one that
# does not. Every status other than those two is a refusal to guess: a 5xx, a
# captive-portal 200 on the wrong body, a DNS failure exiting non-zero.
probe() {
    local registry="$1" url="$2" status
    status="$(curl -sS -o /dev/null -w '%{http_code}' \
                   -H 'User-Agent: statelet-sdk-release (github.com/stateletlab/statelet-sdk)' \
                   --max-time 30 --retry 3 --retry-connrefused \
                   -L "$url")" \
        || die "$registry: could not be reached at $url"
    case "$status" in
        200) return "$PUBLISHED" ;;
        404|410) return "$NOT_PUBLISHED" ;;
        *) die "$registry: answered HTTP $status for $url, which is neither 'here' nor 'not here'" ;;
    esac
}

# The first capture group of a regex in a file, failing loudly on no match — a
# silently empty package name would build a URL that 404s, i.e. would claim
# nothing has ever been published.
extract() {
    local file="$1" regex="$2" label="$3" value
    [ -f "$file" ] || die "$label: $file does not exist"
    value="$(sed -nE "s/$regex/\1/p" "$file" | head -1)"
    [ -n "$value" ] || die "$label: no name matched in $file"
    printf '%s' "$value"
}

case "$SDK" in
    python)
        name="$(extract "$REPO_ROOT/python/pyproject.toml" \
            '^name[[:space:]]*=[[:space:]]*"([^"]+)".*' python)"
        probe PyPI "https://pypi.org/pypi/$name/$VERSION/json"
        ;;
    nodejs)
        name="$(extract "$REPO_ROOT/nodejs/package.json" \
            '^[[:space:]]*"name"[[:space:]]*:[[:space:]]*"([^"]+)".*' nodejs)"
        # A scoped name (@scope/pkg) has to stay percent-unencoded in the path
        # here; the registry serves /@scope/pkg/version directly.
        probe npm "https://registry.npmjs.org/$name/$VERSION"
        ;;
    rust)
        name="$(extract "$REPO_ROOT/rust/Cargo.toml" \
            '^name[[:space:]]*=[[:space:]]*"([^"]+)".*' rust)"
        probe crates.io "https://crates.io/api/v1/crates/$name/$VERSION"
        ;;
    java)
        # Central's search API lags behind the artifacts by minutes to hours;
        # repo1 is the thing Maven itself resolves against, so ask that. The .pom
        # is the one file every published version has.
        pom="$REPO_ROOT/java/pom.xml"
        [ -f "$pom" ] || die "java: $pom does not exist"
        group="$(sed -nE 's|^[[:space:]]*<groupId>([^<]+)</groupId>.*|\1|p' "$pom" | head -1)"
        artifact="$(sed -nE 's|^[[:space:]]*<artifactId>([^<]+)</artifactId>.*|\1|p' "$pom" | head -1)"
        [ -n "$group" ] && [ -n "$artifact" ] || die "java: no groupId/artifactId in $pom"
        # tr, not ${group//./\/}: that substitution leaves the backslash in, and
        # the resulting ai\/statelet path 404s — which this script would have
        # read as "never published".
        group_path="$(printf '%s' "$group" | tr . /)"
        probe "Maven Central" \
            "https://repo1.maven.org/maven2/$group_path/$artifact/$VERSION/$artifact-$VERSION.pom"
        ;;
    go|cpp)
        # Neither has a registry to ask. Go's release IS its go/vX.Y.Z tag and
        # C++'s is a GitHub Release asset; both are created by the tag job, which
        # already skips a tag that exists and uploads assets with --clobber.
        die "$SDK: no registry — its release is a tag, and the tag job is already idempotent"
        ;;
    *)
        usage
        ;;
esac
