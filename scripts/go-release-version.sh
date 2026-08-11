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
# Work out which version Go's go/vX.Y.Z tag should carry.
#
#   ./scripts/go-release-version.sh all 0.2.0      # -> 0.2.0, the input wins
#   ./scripts/go-release-version.sh all            # -> whatever the other five say
#   ./scripts/go-release-version.sh go             # -> all five, since none was selected
#
# A Go module records its version nowhere in its tree — the tag IS the version —
# so Go alone has no manifest to fall back on when the workflow's `version` input
# is empty. That used to be a hard error, and it made the natural way to ship a
# release impossible: with the manifests already bumped, the correct dispatch is
# `version` empty, and Go was the one job that could not survive it. Five SDKs
# published 0.1.3 that way and Go failed, which cost the release its tags.
#
# So when the input is empty, ask the SDKs that DO have manifests. They are being
# released from the same commit, in the same run, and if they unanimously say
# 0.1.3 then 0.1.3 is what this release is — there is nothing else Go's tag could
# reasonably be. Unanimity is the whole safeguard: versions are per-SDK here, and
# releasing python 0.2.0 beside rust 0.1.5 is legitimate, so a disagreement means
# the run has no single version to lend and Go must be told explicitly.
#
# Only the SDKs selected for this release get a vote. One left behind at an old
# version is not evidence about the version being released now.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Every SDK whose version is recorded in a file. Deliberately not go.
MANIFEST_SDKS=(python nodejs rust java cpp)

usage() {
    echo "usage: $0 <selected-csv> [explicit-version]" >&2
    exit 2
}

[ $# -ge 1 ] || usage
SELECTED=",${1// /},"
EXPLICIT="${2:-}"

die() { echo "::error::$*" >&2; exit 1; }

# The input always wins. It is the one path where the caller has said outright
# what the release is, including the tag-push path, where the version was parsed
# off the ref before any manifest was consulted.
if [ -n "$EXPLICIT" ]; then
    printf '%s\n' "$EXPLICIT"
    exit 0
fi

voters=()
for sdk in "${MANIFEST_SDKS[@]}"; do
    case "$SELECTED" in
        *",$sdk,"*) voters+=("$sdk") ;;
    esac
done

# Go released on its own. Nothing else is moving, so the manifests as they stand
# describe the commit being tagged, and all five get a vote.
[ ${#voters[@]} -gt 0 ] || voters=("${MANIFEST_SDKS[@]}")

version=""
disagreement=""
for sdk in "${voters[@]}"; do
    v="$("$REPO_ROOT/scripts/sdk-version.sh" "$sdk")"
    disagreement="$disagreement  $sdk $v"$'\n'
    if [ -z "$version" ]; then
        version="$v"
    elif [ "$v" != "$version" ]; then
        version="DISAGREE"
    fi
done

if [ "$version" = DISAGREE ]; then
    die "go: the SDKs released alongside it do not agree on a version, so there is none to borrow for the go/vX.Y.Z tag. Pass the version input explicitly.
$disagreement"
fi

printf '%s\n' "$version"
