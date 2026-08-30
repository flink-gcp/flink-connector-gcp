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
# Collects one version line's five shaded SQL uber-jars and their .asc
# signatures for the draft GitHub Release (issue #724, ADR-0147). The glob
# shape is load-bearing: the module-directory anchor keeps the unshaded
# original-*.jar out, the exact -<version>.jar suffix keeps -sources/-javadoc
# and the other version line out, and the signatures sit beside the jars in
# target/ (measured on maven-gpg-plugin 3.2.8's bc signer, 2026-08-31 — not in
# its ascDirectory default target/gpg/). The count assertion is what turns a
# renamed module or a moved signature into a red step instead of a draft
# Release quietly missing artifacts. The destination must lie OUTSIDE the
# working tree (the release workflow uses $RUNNER_TEMP): the second staging
# build runs apache-rat over the whole tree, and a collected .asc is exactly
# the unlicensed text file it fails on.
#
# Usage: collect-release-jars.sh <version> <dest>

set -euo pipefail

version="$1"
dest="$2"
if python3 -c '
import os, sys
dest = os.path.realpath(sys.argv[1])
root = os.path.realpath(".")
sys.exit(0 if os.path.commonpath([dest, root]) == root else 1)
' "$dest"; then
    echo "collect-release-jars: destination $dest is inside the working tree;" \
        "the next staging build runs apache-rat over the tree and fails on the" \
        "collected .asc files — use a directory outside it (CI uses \$RUNNER_TEMP)" >&2
    exit 1
fi
mkdir -p "$dest"

shopt -s nullglob
jars=(flink-sql-connector-gcp-*/target/flink-sql-connector-gcp-*-"$version".jar)
ascs=(flink-sql-connector-gcp-*/target/flink-sql-connector-gcp-*-"$version".jar.asc)
if [ "${#jars[@]}" -ne 5 ] || [ "${#ascs[@]}" -ne 5 ]; then
    echo "collect-release-jars: expected 5 uber-jars and 5 signatures for $version," \
        "found ${#jars[@]} and ${#ascs[@]} — run from the repository root after" \
        "\`just stage-release $version\`" >&2
    exit 1
fi
cp "${jars[@]}" "${ascs[@]}" "$dest"/
echo "collected ${#jars[@]} jars and ${#ascs[@]} signatures for $version into $dest"
