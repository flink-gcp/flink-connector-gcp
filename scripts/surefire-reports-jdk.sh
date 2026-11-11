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
# Usage: surefire-reports-jdk.sh <java home>
#
# Fails unless every surefire report under the working directory records the
# java.specification.version of the JDK at <java home>. The binary-compat
# recipe runs it after a rerun on another JDK (issue #2132): surefire
# overwrites reports but never clears the directory, so a report the rerun did
# not rewrite still names the build JDK, and the test fingerprint alone, which
# compares paths and counts, would pass it.

set -euo pipefail

java_home=${1:?usage: surefire-reports-jdk.sh <java home>}
want=$("$java_home/bin/java" -XshowSettings:properties -version 2>&1 |
    sed -n 's/^ *java\.specification\.version = //p')
if [ -z "$want" ]; then
    echo "::error::Could not read java.specification.version from $java_home." >&2
    exit 1
fi

total=0
stale=()
while IFS= read -r report; do
    total=$((total + 1))
    if ! grep -q "name=\"java.specification.version\" value=\"$want\"" "$report"; then
        stale+=("${report#./}")
    fi
done < <(find . -path '*/target/surefire-reports/TEST-*.xml' -print | sort)

if [ "$total" -eq 0 ]; then
    echo "::error::No surefire reports to check." >&2
    exit 1
fi
if [ "${#stale[@]}" -gt 0 ]; then
    echo "::error::${#stale[@]} of $total surefire reports were not written on Java $want, first ${stale[0]}." >&2
    exit 1
fi
echo "All $total surefire reports were written on Java $want."
