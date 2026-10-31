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
# Regenerate every shaded module's META-INF/NOTICE and META-INF/licenses/ in
# one reactor build: `just update-notice <module>` for all of them, without
# building the shared modules once per shaded module. Fetches each url source
# in scripts/config/licence-sources.toml, so it needs the network.
#
# Run from two places: check-notice-sources.sh, which then requires the
# working tree unchanged, and renovate.yaml's bom_notices job, whose result
# bom_notices_push commits to the BOM update branch (ADR-0191). Safe to run by hand after a
# dependency change that moves more than one shaded module's bundle.
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
    echo "error: no */NOTICE.template found — nothing to regenerate, which cannot be right" >&2
    exit 1
fi

# One reactor build for every shaded module: --update reads the THIRD-PARTY
# report and the runtime classpath this generates. A phase rather than a bare
# goal, for the reason the justfile's update-notice recipe records.
printf -v selection '%s,' "${modules[@]}"
./mvnw -ntp -pl "${selection%,}" -am generate-test-resources

for module in "${modules[@]}"; do
    scripts/check-notice.py --update "$module"
done
echo "update-notices: ${#modules[@]} modules regenerated."
