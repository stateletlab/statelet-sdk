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
# Decide whether an SDK's publish step should run, and say why in the job log.
#
#   ./scripts/publish-gate.sh rust     # -> skip=true|false on $GITHUB_OUTPUT
#
# The version comes from the manifest, not from an input: the checkout is pinned
# to the commit plan produced, so this is the version that was actually built.
# It is the same rule the tag job uses, deliberately — a gate reading one number
# and a tag reading another is how you get a tag pointing at a different release
# than the registry has.
#
# Lives in a script rather than inline YAML because four jobs need it verbatim,
# and because the three-way exit of is-published.sh is exactly the thing that
# gets flattened into `|| true` when it is retyped per job.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ $# -eq 1 ] || { echo "usage: $0 <python|nodejs|rust|java>" >&2; exit 2; }
SDK="$1"

VERSION="$("$REPO_ROOT/scripts/sdk-version.sh" "$SDK")"

rc=0
"$REPO_ROOT/scripts/is-published.sh" "$SDK" "$VERSION" || rc=$?

case "$rc" in
    0)  skip=true ;;
    10) skip=false ;;
    # is-published.sh already printed which registry and why. Propagating the
    # code rather than defaulting to "publish anyway" keeps an unreachable
    # registry a failure, not a coin flip on a release.
    *)  exit "$rc" ;;
esac

echo "skip=$skip" >> "${GITHUB_OUTPUT:-/dev/stdout}"

if [ "$skip" = true ]; then
    echo "::notice::$SDK $VERSION is already published; skipping the publish step. This is a re-run finishing a release, not a no-op."
    echo "- \`$SDK\` **$VERSION** — already published, publish skipped" \
        >> "${GITHUB_STEP_SUMMARY:-/dev/stdout}"
else
    echo "$SDK $VERSION is not published yet; publishing."
fi
