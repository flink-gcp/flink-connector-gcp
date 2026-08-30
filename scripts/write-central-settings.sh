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
# Writes the Maven settings.xml the Central Portal upload authenticates with
# (issue #724, ADR-0147). The file carries no secret: Maven interpolates the
# ${env.*} references when the `central` server entry is used, so the token
# pair only ever exists in the environment of the process running the deploy.
# Release CI and a maintainer staging by hand run the same script, so the one
# place the server entry is spelled out is runnable from a shell.
#
# Usage: write-central-settings.sh [--overwrite] [dest]
#        (default dest: ~/.m2/settings.xml)
#
# An existing destination is refused without --overwrite: a maintainer's
# settings.xml may carry mirrors, proxies or other server entries, and
# truncating it would silently break every later Maven run. Release CI passes
# --overwrite because the only thing on the runner is the throwaway file
# setup-java wrote.

set -euo pipefail

overwrite=false
if [ "${1:-}" = "--overwrite" ]; then
    overwrite=true
    shift
fi
dest="${1:-$HOME/.m2/settings.xml}"
if [ -e "$dest" ] && [ "$overwrite" != true ]; then
    echo "write-central-settings: $dest exists; merge the <server> entry by hand" \
        "or re-run with --overwrite (and lose the current contents)" >&2
    exit 1
fi
mkdir -p "$(dirname "$dest")"
cat > "$dest" <<'SETTINGS'
<settings>
  <servers>
    <server>
      <!-- Must equal publishingServerId in the root pom's central-release
           profile; the credentials come from the environment at deploy time
           (CENTRAL_TOKEN_USERNAME / CENTRAL_TOKEN_PASSWORD, a Central Portal
           user token pair). -->
      <id>central</id>
      <username>${env.CENTRAL_TOKEN_USERNAME}</username>
      <password>${env.CENTRAL_TOKEN_PASSWORD}</password>
    </server>
  </servers>
</settings>
SETTINGS
echo "wrote $dest"
