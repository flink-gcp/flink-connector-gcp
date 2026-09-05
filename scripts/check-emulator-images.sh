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
# Do the registries still serve the pinned emulator images — and for how long?
# (issue #1200, after the #1196 rotation)
#
#     scripts/check-emulator-images.sh
#     MARGIN_DAYS=400 scripts/check-emulator-images.sh   # makes the margin fire today
#
# Testcontainers pulls the emulator images pinned as string literals in the
# IMAGE constant of each *EmulatorContainers.java under test-utils. Nothing
# else reads those literals — no dependabot ecosystem can — so a registry
# dropping a tag was discovered by every emulator lane on every open pull
# request going red at once (#1196: gcr.io keeps google-cloud-cli tags for
# about a year, then rotates them out release by release). Two questions per
# pin, weekly from .github/workflows/weekly.yaml:
#
#   1. Is the manifest still served?  `docker manifest inspect` talks to the
#      registry directly (no daemon involved) through the same token dance a
#      pull runs, so a "no" here is the ContainerFetchException a build would
#      see. Every pin is asked before the script fails, so one run names all
#      the rot.
#   2. On gcr.io, for how long?  Its tags/list carries an upload time per
#      manifest (a GCR extension the standard API lacks), and the retention
#      window is whatever it still keeps: the pin's remaining life is its
#      upload time minus the oldest surviving tag's. Below MARGIN_DAYS the run
#      fails while the image still pulls, so the bump is a planned pull request
#      instead of an outage. The listing has to be one page carrying real
#      upload times, or the step fails rather than guessing. ghcr.io's registry
#      API exposes no upload times and ghcr.io has no retention window; those
#      pins get question 1 only.
#
# The pins are discovered from the source files by name, never listed here: a
# second copy would rot in the other direction. Each file must yield exactly
# one image literal outside comments — zero or several means the extractor no
# longer reads the file, and a partial inventory that passes is the failure
# this guard exists to prevent, so it fails loudly instead.
#
# Network by design, and it needs the docker CLI (`just verify` needs it too),
# curl and python3; run by hand as `just check-emulator-images`.
set -euo pipefail
cd "$(dirname "$0")/.."

# Weekly cadence makes this at least four warnings; a bump is a one-line edit
# plus, for Bigtable, the deviation suites' verdict (ADR-0044) — a pull
# request, not an afternoon. Validated because `[[ 5 -lt 30d ]]` is an error
# that reads as "not below", which would pass exactly when it should not.
readonly MARGIN_DAYS="${MARGIN_DAYS:-30}"
if ! [[ $MARGIN_DAYS =~ ^[1-9][0-9]*$ ]]; then
    echo "error: MARGIN_DAYS must be a positive whole number of days, not '${MARGIN_DAYS}'" >&2
    exit 1
fi

# A whole quoted registry reference — dotted host, path, tag — closing its
# argument with `)` or `;`. Tight enough that an endpoint ("localhost:9010") or
# a resource path does not match, and a concatenation ("…:583.0.0" +
# "-emulators") yields nothing rather than the served prefix.
readonly IMAGE_LITERAL='"[a-z0-9.-]+\.[a-z]+/[A-Za-z0-9._/-]+:[A-Za-z0-9._-]+"[[:blank:]]*[);]'

for tool in docker curl python3; do
    if ! command -v "$tool" >/dev/null; then
        echo "error: $tool is not on PATH, and this check cannot run without it" >&2
        exit 1
    fi
done

shopt -s nullglob
files=(flink-connector-gcp-test-utils/src/main/java/io/github/flink/gcp/connector/testutils/*/*EmulatorContainers.java)
if [[ ${#files[@]} -eq 0 ]]; then
    echo "error: no *EmulatorContainers.java found — nothing to check, which cannot be right" >&2
    exit 1
fi

# failed: anything went wrong. rotten: a registry answered about a pin, which
# is the one failure whose repair the epilogue below can name.
failed=0
rotten=0
for file in "${files[@]}"; do
    # Comments are stripped first — block and line, blind to string contents —
    # so a commented-out initializer cannot stand in for the live one. A
    # comment marker inside a string literal is not understood: no string these
    # classes plausibly hold splices or hides a literal, and a bare `/*` in one
    # either matches nothing or swallows up to the next `*/`, which fails
    # loudly below when the IMAGE literal is inside.
    literals=$(python3 -c 'import re, sys; sys.stdout.write(re.sub(r"/\*.*?\*/|//[^\n]*", "", sys.stdin.read(), flags=re.S))' < "$file" | grep -oE "$IMAGE_LITERAL" | tr -d '");[:blank:]' || true)
    count=$(printf '%s\n' "$literals" | grep -c . || true)
    if [[ $count -ne 1 ]]; then
        joined=${literals//$'\n'/, }
        echo "::error file=${file}::expected exactly one whole quoted registry/path:tag argument outside comments, found ${count}: ${joined:-(none)}" >&2
        failed=1
        continue
    fi
    ref=$literals
    if ! message=$(docker manifest inspect "$ref" 2>&1 >/dev/null); then
        echo "::error file=${file}::${ref} cannot be fetched — ${message}" >&2
        failed=1
        rotten=1
        continue
    fi
    host=${ref%%/*}
    case $host in
        gcr.io | *.gcr.io) ;;
        *)
            echo "ok  ${ref}  (served; ${host} retention not measured)"
            continue
            ;;
    esac
    path=${ref#*/}
    repo=${path%:*}
    tag=${path##*:}
    # Headers ride along (-D -) so a paginated listing is refused rather than
    # measured over its first page; an HTTPS proxy's own "Connection
    # established" block is suppressed so the split below finds the registry's.
    if ! margin=$({ curl -sSf --max-time 60 --suppress-connect-headers -D - "https://${host}/v2/${repo}/tags/list" | python3 -c '
import json, sys
tag = sys.argv[1]
raw = sys.stdin.read()
head, separator, body = raw.partition("\r\n\r\n")
if not separator:
    head, body = "", raw
if any(line.lower().startswith("link:") for line in head.splitlines()):
    sys.exit("tags/list is paginated, and this check reads one page")
try:
    listing = json.loads(body)
except ValueError as error:
    sys.exit(f"tags/list is not JSON: {error}")
manifests = listing.get("manifest") if isinstance(listing, dict) else None
if not manifests:
    sys.exit("tags/list carries no upload times; the GCR extension this check reads is gone")
try:
    uploaded = {t: int(m["timeUploadedMs"]) for m in manifests.values() for t in m.get("tag", [])}
except (AttributeError, KeyError, TypeError, ValueError) as error:
    sys.exit(f"tags/list manifests no longer carry an integer timeUploadedMs: {error!r}")
if tag not in uploaded:
    sys.exit(f"{tag} is served but absent from tags/list")
oldest = min(uploaded.values())
if oldest <= 0:
    sys.exit("tags/list carries a zero upload time, which would inflate the margin")
print((uploaded[tag] - oldest) // 86_400_000)
' "$tag"; } 2>&1); then
        echo "::error file=${file}::${ref}: could not measure the retention margin — ${margin}" >&2
        failed=1
        continue
    fi
    if [[ $margin -lt $MARGIN_DAYS ]]; then
        echo "::error file=${file}::${ref} is only ${margin} days younger than the oldest tag ${host} still keeps (threshold ${MARGIN_DAYS}); bump the pin before it rotates out" >&2
        failed=1
        rotten=1
        continue
    fi
    echo "ok  ${ref}  (served; ${margin} days younger than the oldest tag ${host} still keeps)"
done

if [[ $rotten -ne 0 ]]; then
    cat >&2 <<'MSG'

Where a registry above answered that a tag is gone, or about to rotate out,
the pin has to move: edit the IMAGE constant in the named file. A
google-cloud-cli bump also owes the Bigtable deviation suites' verdict on what
the new emulator changed (ADR-0044; "Emulators are conveniences, not
authorities" in .agents/references/repository-guide.md). A transport failure
on the same line is not that: re-run the job.
MSG
fi
if [[ $failed -ne 0 ]]; then
    exit 1
fi
echo "check-emulator-images: ${#files[@]} pins served."
