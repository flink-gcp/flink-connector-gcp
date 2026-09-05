<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0151: Emulator image pins are probed weekly at their registries, read from the classes that pin them

- Status: Accepted
- Date: 2026-09-05 ([#1200])
- Issues: [#1200], [#1196]
- Modules: test-utils (the pins), scripts, CI (`weekly.yaml`)
- Current behavior: `scripts/check-emulator-images.sh` (its header carries the failure modes),
  `.agents/references/repository-guide.md` § Build (`just check-emulator-images`), the docs
  Checks page

## Context

Five Docker emulator images are pinned as string literals in the `IMAGE` constant of each
`*EmulatorContainers` class in `flink-connector-gcp-test-utils`: Pub/Sub and Bigtable share
`gcr.io/google.com/cloudsdktool/google-cloud-cli:<version>-emulators` (duplicated on purpose so
a bump declares its scope, [#27]), the Spanner emulator is on gcr.io, and the BigQuery and Cloud
Tasks emulators are on ghcr.io. No dependabot ecosystem reads a Java literal, no checker asserted
on them and no workflow named them. gcr.io keeps roughly a year of google-cloud-cli tags and
rotated `441.0.0-emulators` out; on 2026-09-03 every Bigtable and Pub/Sub lane on all five open
pull requests was red before a test ran ([#1196]), 26 `ContainerFetchException`s whose one cause
had to be diagnosed across pull requests that had changed nothing relevant. [#1200] asked for a
guard and priced three shapes in order — a manifest check in `weekly.yaml`, a Renovate custom
manager, a checker script with synthetic tests — with the rule that the first must be disproved
before the others are considered. It was not disproved.

## Decision

**A shell script, `scripts/check-emulator-images.sh`, reads the pins out of the classes that
carry them and asks each registry two questions, weekly.** It has the shape of
`scripts/check-flink-release.sh` (ADR-0053): a network probe of an upstream that needs no
repository change to trigger, run from its own `weekly.yaml` job (`emulator_images`) that is
independent of the Flink-range jobs, so a rotten pin cannot stop the range from being verified
nor the other way round.

- **The inventory is discovered, never listed.** The script globs
  `testutils/*/*EmulatorContainers.java` and takes the one whole quoted `registry/path:tag`
  argument in each file, comments stripped, so a commented-out initializer cannot be read as the
  live one; a concatenation or a digest reads as none. Zero files, or a file yielding zero or
  several literals, fails naming the file: a partial inventory that passes is the failure the
  guard exists to prevent.
- **Existence is `docker manifest inspect`.** The docker CLI talks to the registry directly — no
  daemon is involved — through the same token dance a pull runs, so a "no" here is the
  `ContainerFetchException` a build would see, and anything non-zero fails: ghcr.io answers
  *denied* rather than 404 for a repository that does not exist. Every pin is probed before the
  run fails, so one run names all the rot.
- **The gcr.io pins also get a margin.** gcr.io's `tags/list` carries an upload time per manifest
  (a GCR extension the standard API lacks), and the retention window is whatever the registry
  still keeps: the pin's remaining life is its upload time minus the oldest surviving tag's.
  Below `MARGIN_DAYS` the run fails while the image still pulls, so the bump is a planned pull
  request instead of an outage. The threshold is an environment override, which is also the
  firing control, validated as a positive whole number because `[[ 5 -lt 30d ]]` is an error
  that reads as "not below". ghcr.io's registry API exposes no upload times and ghcr.io has no
  retention window, so those pins get the existence question only. The listing has to be one
  page carrying non-zero integer upload times and the served tag, or the step fails naming what
  it found — it never degrades to a silent pass.
- **The failure names the pin, the file and the repair once**, and says when the failure is a
  transport error rather than the pin's.
- **Weekly only, not per pull request**: a dropped tag already fails every affected lane the
  same day, and the margin is what closes the mid-week gap.
- **No `curate-*` skill**, on the argument `just check-gated-tags` makes: no allowlist to judge,
  and the failure message carries the whole repair.

## Evidence

Measured 2026-09-05 unless stated.

- `docker manifest inspect` on the four distinct pins: exit 0 for all. On
  `google-cloud-cli:441.0.0-emulators`, `cloud-tasks-emulator:9.9.9` and a non-existent ghcr.io
  repository: exit 1 with "no such manifest", "manifest unknown" and "denied" respectively.
- `docker manifest inspect` needs no daemon: with `DOCKER_HOST` pointed at an unreachable
  address it still answers exit 0 for a present tag and "manifest unknown" for a missing one, so
  the runner needs only the CLI.
- The registry API by hand: gcr.io answers an anonymous manifest `HEAD` with 200/404 and no
  token; ghcr.io answers 401 with a realm, serves an anonymous token from it, then 200 for a
  present tag, 404 for a missing tag and **403 for a missing repository** — the case that makes
  "anything but success fails" the rule rather than "404 fails".
- gcr.io `tags/list` for `google.com/cloudsdktool/google-cloud-cli`: 556 KB, 0.8 s, 2034 tags on
  1601 manifests; `timeCreatedMs` is 0 on the oldest entries, `timeUploadedMs` is populated
  throughout. The oldest surviving upload across all tags is 2025-09-09 — the same date as the
  oldest surviving `-emulators` tag, so the window is per repository, not per tag family — and
  the pin was uploaded 2026-09-01: **357 days of margin**. Two days earlier ([#1196]'s
  measurement) the oldest was `537.0.0-emulators` from 2025-09-03, so the edge advances one
  release day (42–47 tags) at a time. `441.0.0-emulators` is a 2023 release ([#1196]) that was
  still served until 2026-09, so the year-long window is a recent policy rather than a
  long-standing one — which is why the margin is measured from the listing each week rather than
  assumed. `cloud-spanner-emulator/emulator` keeps 515 tags back to 2020-03-16: no retention
  window, a margin of 2331 days, and no special case needed.
- The whole check takes about 48 s on a macOS workstation: roughly 5 s per `docker manifest
  inspect`, of which about a second is the credential helper (3.7 s with an empty
  `DOCKER_CONFIG`, 4.9 s with the default), the rest the CLI's registry round trips. It reports
  357 days for the two google-cloud-cli pins and 2331 for Spanner.
- Firing controls, each against a copy of the tree, every one exit 1: the Pub/Sub pin moved back
  to `441.0.0-emulators` → names that file and pin while the other four still report;
  `MARGIN_DAYS=400` → both google-cloud-cli pins, Spanner passes; `MARGIN_DAYS=30d` and
  `MARGIN_DAYS=0` → rejected before any probe; a concatenated literal (`"…:583.0.0" +
  "-emulators"`) → "found 0" rather than the served prefix; that concatenation beside a
  commented-out copy of the old initializer in each comment shape (a `//` line, a Javadoc
  block, a bare `/* … */` block, a trailing `// was "…";` on the live line) → still "found 0"
  rather than the old pin; a second quoted image literal in one file → "found 2"; a constant
  that is no longer a registry reference → "found 0"; no `*EmulatorContainers.java` at all →
  "nothing to check"; a `tags/list` without the `manifest` map, without the served tag, with a
  zero upload time, paginated (a `Link` header), or with an entry lacking `timeUploadedMs` → the
  margin step names which, in one line; a `docker` on `PATH` that exits non-zero → every pin
  still reported, never a pass. Two positive controls beside them: the plain stubbed run, which
  a greedy block strip would fail because every class carries Javadoc after its `IMAGE` field;
  and a block-comment marker inside an unrelated string literal (`"src/*path*/"`), which leaves
  all five pins served. The harness asserts the exit code and every pattern the claims above
  name over the output of each case.
- `--suppress-connect-headers` (curl 7.54+; 8.7.1 here), measured with a loopback CONNECT stub
  (a socket that reads the request, answers `200 Connection established`, and closes):
  `curl -x <stub> -D - https://…` prints that block, and with the flag prints nothing — the half
  a stub can show; that the split then finds the registry's block follows by construction,
  nothing else preceding it. Without the flag the failure would have been loud ("tags/list is
  not JSON"), not a wrong margin.

## Alternatives declined

- **Renovate `customManagers` regex over the five literals.** Adopts a second bot beside
  dependabot, a regex manager to keep in step with the Java, and it *bumps* where this repository
  needs to *verify*: a google-cloud-cli bump is a deliberate act that runs the deviation suites
  and declares whether Pub/Sub moves with Bigtable ([#1196] moved three measured rows), so an
  auto-opened bump is the wrong shape — the reason ADR-0053 suppresses dependabot's Flink minor
  bumps.
- **A Python checker with synthetic tests under `scripts/tests/`.** Priced at roughly 330–430
  lines against ~170 for the shell script; the sibling `check-flink-release.sh` has no test
  either, the extractor's contract is self-checked at runtime, and ADR-0127's withdrawn checker
  (3,798 lines, every review finding in the instrument) is the calibration for a tool larger
  than the five-item inventory it polices. The cheapest committed form — a pytest around the
  unchanged shell script with `docker` and `curl` stubs on `PATH`, the `test_sweep_e2e.py`
  shape — prices at roughly 160–180 lines offline; declined for now because no firing control
  found a silent pass that only a committed test closes, with the weakness stated: the harness
  holding those cases is run by hand.
- **A hand-written `curl` HEAD with the 401 → realm → token flow.** Works (measured above) and
  runs without docker, at ~25 more lines of authentication parsing that the docker CLI already
  owns and keeps current; the recipe's docker requirement is one `just verify` already has.
- **A per-pull-request job in `verify.yaml`.** No earlier detection, a network call on every
  pull request, and an enrolment in the CI gate for a fact that changes about once a year.
- **Existence only, as [#1200] literally named.** Fires only after the tag is gone; on a weekly
  cadence a mid-week rotation is still discovered by red lanes, so the check would replace "red
  lanes on Tuesday" with "red weekly on Sunday". The margin is the ~25 lines that make the job
  worth having; chosen with the owner on 2026-09-05.
- **A pin list in the workflow, or a hard-coded count of five.** A second home for the pins, or
  for their number, rots when a sixth emulator arrives; the glob and the exactly-one rule
  discover the inventory instead, and the module reference records that a new emulator follows
  the `*EmulatorContainers` name or goes unwatched.
- **Digest pins.** Would close the mutable-tag hole and not the retention one, and the retention
  is what bit.

## Consequences

- A red `emulator_images` job names the pin and the file. The repair is the one-line edit plus,
  for a google-cloud-cli bump, the deviation suites' verdict; the bump does not go in quietly
  (ADR-0044).
- The margin depends on a GCR extension. If gcr.io stops serving upload times the job goes red
  saying so, and the margin step is removed or re-sourced by hand — never skipped in silence.
- No retry on a transient registry failure, as with `check-flink-release`: eight requests a week
  (five manifests, three listings) are far below any anonymous rate limit. A transient red is re-run with GitHub's "Re-run failed
  jobs", which re-runs this job alone; a `workflow_dispatch` of `weekly.yaml` runs the whole
  Flink matrix, which is also why the job's first live run is the next scheduled Sunday rather
  than a dispatch on merge.

[#27]: https://github.com/flink-gcp/flink-connector-gcp/issues/27
[#1196]: https://github.com/flink-gcp/flink-connector-gcp/issues/1196
[#1200]: https://github.com/flink-gcp/flink-connector-gcp/issues/1200
