#!/usr/bin/env bash
#
# Copyright 2026 laughingman7743
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
# Fails if Flink has released a minor newer than the given supported ceiling.
#
#     scripts/check-flink-release.sh 2.3.0
#
# This project supports the current and the previous Flink minor, which is a
# range defined relative to what Flink has released — so a Flink release makes
# the claim in this repository wrong without anything here changing. The thing
# that used to announce a release was the monthly dependabot minor PR, and
# .github/dependabot.yml now suppresses it on purpose. This is its replacement,
# run weekly from .github/workflows/weekly.yaml.
#
# On failure it prints the ordered edit list, because a runbook filed somewhere
# is only read by someone who already knows to go looking for it.

set -euo pipefail

readonly METADATA_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-core/maven-metadata.xml

ceiling=${1:-}
if ! [[ $ceiling =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "usage: $0 <supported-ceiling>   e.g. $0 2.3.0" >&2
    exit 2
fi

major=${ceiling%%.*}
ceiling_minor=$(echo "$ceiling" | cut -d. -f2)

# Only fully-numbered releases of the same major: this must not trip over
# 2.0-preview1, a 1.x LTS patch, or a SNAPSHOT.
latest=$(curl -sSf --max-time 30 "$METADATA_URL" |
    grep -oE "<version>${major}\.[0-9]+\.[0-9]+</version>" |
    sed 's/<[^>]*>//g' | sort -V | tail -1)

if [ -z "$latest" ]; then
    echo "::error::Could not read any ${major}.x release from $METADATA_URL"
    exit 1
fi

latest_minor=$(echo "$latest" | cut -d. -f2)
echo "ceiling $ceiling, newest release on Maven Central $latest"

if [ "$latest_minor" -le "$ceiling_minor" ]; then
    echo "Supported range is current."
    exit 0
fi

cat <<MSG
Flink $latest is released, so the supported range — the current and the
previous minor — is now ${major}.${ceiling_minor} and ${major}.${latest_minor},
and this repository still claims otherwise.

Moving the range means editing, in this order:
  1. pom.xml                         flink.version -> $ceiling
                                     the old ceiling becomes the new floor, and
                                     this one edit moves ci.yaml, the floor row
                                     of the weekly matrix and binary_compat's
                                     base along with it
  2. .github/workflows/weekly.yaml   FLINK_CEILING -> $latest
                                     FLINK_NEXT_SNAPSHOT -> the next unreleased
                                     minor
  3. docs/content/_index.md          the Supported versions table
  4. README.md                       the supported-range sentence under Build
  5. CLAUDE.md                       the Version policy section

Then re-run the binary-compatibility measurement against the new ceiling before
claiming the range — the old measurement says nothing about the new pair:

  just binary-compat $latest
MSG
exit 1
