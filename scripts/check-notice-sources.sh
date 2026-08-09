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
# Proof that the pinned licence-text sources still hold (issue #343):
# regenerate every shaded module's META-INF/NOTICE and META-INF/licenses/ from
# scripts/licence-sources.toml — fetching each url source — and require the
# working tree unchanged. The offline NOTICE check compares checked-in bytes
# against sha256 pins in the same repository — nothing on the check path ever
# consults the recorded sources, so in CI this script is what does. A tag
# deleted upstream fails the fetch, an upstream text edit fails the sha256 pin
# inside check-notice.py, and a stale generation fails the diff below.
#
# Run from two places, via `just check-notice-sources`: verify.yaml's build
# job when the change touches a licence-source input (a pom, the pin file, a
# NOTICE — scripts/ci-maven-args.py derives that), and weekly.yaml's
# notice_sources job, which owns the fetch that needs no change to trigger.
# Safe to run by hand from anywhere in the repository (it fails loudly on
# local NOTICE drift, which is what it is for).
set -euo pipefail
cd "$(dirname "$0")/.."

# A shaded module is one carrying a NOTICE.template — the same convention
# check-notice.py's dead-entry check discovers modules by. nullglob so that
# zero matches is the explicit error below, not a literal '*/NOTICE.template'.
shopt -s nullglob
modules=()
for template in */NOTICE.template; do
    modules+=("${template%/NOTICE.template}")
done
if [[ ${#modules[@]} -eq 0 ]]; then
    echo "error: no */NOTICE.template found — nothing to verify, which cannot be right" >&2
    exit 1
fi

# One reactor build for every shaded module: --update reads the THIRD-PARTY
# report and the runtime classpath this generates.
printf -v selection '%s,' "${modules[@]}"
./mvnw -ntp -pl "${selection%,}" -am generate-test-resources

for module in "${modules[@]}"; do
    scripts/check-notice.py --update "$module"
done

# git status rather than git diff alone: a regeneration that *adds* a licence
# file leaves it untracked, which git diff does not report.
paths=("${modules[@]/%//src/main/resources/META-INF}")
changed="$(git status --porcelain -- "${paths[@]}")"
if [[ -n "$changed" ]]; then
    echo "$changed" >&2
    git diff -- "${paths[@]}" >&2
    echo "error: regenerating from the pinned sources changed the files above:" >&2
    echo "the checked-in NOTICE/licences no longer match what the sources serve." >&2
    exit 1
fi
echo "check-notice-sources: ${#modules[@]} modules regenerated with no drift."
