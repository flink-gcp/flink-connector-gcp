#!/usr/bin/env bash
#
# Copyright 2026 The flink-gcp authors
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
# Fails if Flink has released a minor newer than the given supported ceiling,
# or, when the 1.x LTS is given as well, a patch of the LTS minor newer than it.
#
#     scripts/check-flink-release.sh 2.3.0 1.20.5
#
# This project supports the current and the previous Flink minor, which is a
# range defined relative to what Flink has released — so a Flink release makes
# the claim in this repository wrong without anything here changing. The thing
# that used to announce a release was the monthly dependabot minor PR, and
# .github/dependabot.yml now suppresses it on purpose. This is its replacement,
# run weekly from .github/workflows/weekly.yaml.
#
# The 1.x LTS is a single minor (1.20; Flink plans no 1.21), so only its
# patches can move, and nothing else announces one: FLINK_LTS is workflow env,
# which dependabot does not see. Flink 1.20.5 went unnoticed for nearly five
# months that way (#1973). Every X.Y.Z-1.20 release is compiled against
# FLINK_LTS and the weekly lts row tests at it (Flink itself is provided, never
# shipped), so a stale pin publishes connectors never built against the current
# patch.
#
# On failure it prints the ordered edit list, because a runbook filed somewhere
# is only read by someone who already knows to go looking for it. Both checks
# run and report before the exit, so one stale pin cannot hide the other.

set -euo pipefail

# The synthetic tests in scripts/tests point this at a file:// fixture.
readonly METADATA_URL=${FLINK_METADATA_URL:-https://repo1.maven.org/maven2/org/apache/flink/flink-core/maven-metadata.xml}
# The one 1.x minor the -1.20 release line is built for (scripts/stage-release-guard.py).
readonly LTS_LINE=1.20

usage() {
    echo "usage: $0 <supported-ceiling> [<lts>]   e.g. $0 2.3.0 1.20.5" >&2
    exit 2
}

[ $# -ge 1 ] && [ $# -le 2 ] || usage
ceiling=$1
lts=${2:-}
[[ $ceiling =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || usage
# A second argument that is present but empty is a pin that went missing, not
# a request to skip the LTS: only an omitted one does that.
if [ $# -eq 2 ] && ! [[ $lts =~ ^${LTS_LINE//./\\.}\.[0-9]+$ ]]; then
    echo "::error::the LTS must be a ${LTS_LINE}.x patch, not '$lts'" >&2
    exit 2
fi

metadata=$(curl -sSf --max-time 30 "$METADATA_URL")

# Every fully-numbered release whose version starts with the given escaped
# prefix, oldest first: 2.0-preview1 and SNAPSHOTs never match.
releases() {
    grep -oE "<version>$1\.[0-9]+</version>" <<<"$metadata" |
        sed 's/<[^>]*>//g' | sort -V || true
}

status=0

major=${ceiling%%.*}
ceiling_minor=$(echo "$ceiling" | cut -d. -f2)

# Only releases of the ceiling's major: this must not trip over a 1.x LTS patch.
latest=$(releases "${major}\.[0-9]+" | tail -1)

if [ -z "$latest" ]; then
    echo "::error::Could not read any ${major}.x release from $METADATA_URL"
    status=1
else
    latest_minor=$(echo "$latest" | cut -d. -f2)
    echo "ceiling $ceiling, newest release on Maven Central $latest"
    if [ "$latest_minor" -le "$ceiling_minor" ]; then
        echo "Supported range is current."
    else
        status=1
        cat <<MSG
Flink $latest is released, so the supported range — the current and the
previous minor — is now ${major}.${ceiling_minor} and ${major}.${latest_minor},
and this repository still claims otherwise.

Moving the range means editing, in this order:
  1. pom.xml                         flink.version -> $ceiling
                                     flink.version.floor -> $ceiling (kept equal;
                                     the central-release enforcer compares them)
                                     the old ceiling becomes the new floor, and
                                     this one edit moves ci.yaml, the floor row
                                     of the weekly matrix and binary_compat's
                                     base along with it
  2. .github/workflows/weekly.yaml   FLINK_CEILING -> $latest
                                     FLINK_NEXT_SNAPSHOT -> the next unreleased
                                     minor
  3. docs/content/_index.md          the Supported versions table
  4. README.md                       the supported-range claim under Supported versions
  5. AGENTS.md                       the Version policy section

Then re-run the binary-compatibility measurement against the new ceiling before
claiming the range — the old measurement says nothing about the new pair:

  just binary-compat $latest
MSG
    fi
fi

if [ $# -eq 2 ]; then
    lts_releases=$(releases "${LTS_LINE//./\\.}")
    latest_lts=$(tail -1 <<<"$lts_releases")
    echo "LTS $lts, newest $LTS_LINE release on Maven Central ${latest_lts:-none}"
    if [ -z "$latest_lts" ]; then
        echo "::error::Could not read any $LTS_LINE release from $METADATA_URL"
        status=1
    elif ! grep -qxF "$lts" <<<"$lts_releases"; then
        echo "::error::FLINK_LTS $lts is not a Flink release on Maven Central"
        status=1
    elif [ "$latest_lts" = "$lts" ]; then
        echo "LTS patch is current."
    else
        status=1
        cat <<MSG
Flink $latest_lts is released, and the 1.x LTS line still builds against $lts.
Every X.Y.Z-1.20 release, the weekly lts row and the API tier audit build at
FLINK_LTS, so the next release would publish connectors compiled, and tested
weekly, only against the old patch.

Moving the LTS patch means editing:
  1. .github/workflows/weekly.yaml   FLINK_LTS -> $latest_lts
                                     (release.yaml, rehearse-release.py and
                                     check-flink-api-tiers.py read it there)
  2. the documented build command    just verify-flink $lts -> $latest_lts in
                                     AGENTS.md, README.md,
                                     .agents/references/repository-guide.md,
                                     .agents/skills/maintain-doc-sql-snippets/SKILL.md,
                                     docs/content/_index.md and
                                     docs/content/docs/development/_index.md
  3. statements of current coverage  git grep -n '$lts' — change a sentence that
                                     says what the build covers now, and keep one
                                     that records what was measured on $lts

Then verify the new patch the way the weekly lts row does, because per-PR CI
builds only the 2.x floor. Each Maven run cleans first: after a 2.x build,
target/ still holds the flink2 compat classes, and an incremental 1.20 build
reuses them.

  just check-flink-api-tiers
  just verify-flink $latest_lts clean
  just verify clean -Ptier3-cloudtasks -pl kubernetes/apps/cloudtasks -am -Dflink.version=$latest_lts -Dflink.compat=flink1
  just check-doc-snippets clean -Dflink.version=$latest_lts -Dflink.compat=flink1
MSG
    fi
fi

exit "$status"
