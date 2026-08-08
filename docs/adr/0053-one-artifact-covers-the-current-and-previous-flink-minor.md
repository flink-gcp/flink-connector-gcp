<!--
Copyright 2026 laughingman7743

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

# ADR-0053: One artifact covers the current and previous Flink minor, and the weekly `binary_compat` run is the evidence

- Status: Accepted
- Date: 2026-07-25 ([#102], settled by measurement on PR
  [#108](https://github.com/laughingman7743/flink-connector-gcp/pull/108))
- Issues: [#102], [#29], [#39] (publishing deferred there)
- Modules: all (build/CI)
- Current behavior: `docs/content/_index.md` supported-versions table, `README.md` § Build

## Context

`main` has to say which Flink versions it supports, and whether one jar or several cover them.
Only about half the Flink API surface these connectors touch is `@Public` — and `@Public`
guarantees *source*, not binary, compatibility across minors — so a one-artifact claim cannot
rest on annotations alone. Before [#102] the working assumption was a per-minor artifact suffix
(`-2.1`); that assumption is dropped here.

## Decision

- **`main` supports the current and previous Flink minor**, mirroring Flink's own support
  policy. `flink.version` is pinned to the **floor** of the range, because compiling against
  the oldest and running on newer is the direction that works.
- **A new Flink minor moves both ends deliberately** — an edit to `flink.version` plus
  `.github/workflows/weekly.yaml`, never a dependabot minor bump, which a `dependabot.yml`
  `ignore` rule now suppresses (patch bumps still arrive). Closed PRs
  [#42](https://github.com/laughingman7743/flink-connector-gcp/pull/42) and
  [#97](https://github.com/laughingman7743/flink-connector-gcp/pull/97) are the precedent for
  rejecting minor bumps. The full edit list for a range move is printed by
  `scripts/check-flink-release.sh` in its failure output — deliberately not hand-maintained
  anywhere else, because the printed copy is the one that gets read. After moving the range,
  **re-run the binary-compatibility measurement against the new ceiling**
  (`just binary-compat <new ceiling>`) before claiming it: the old measurement says nothing
  about the new pair.
- **One artifact, no per-minor suffix.** The claim rests on the `binary_compat` job in
  `weekly.yaml`: build against the floor, then re-run the whole suite with the newest supported
  Flink swapped onto the classpath and nothing recompiled.
- **The version matrix lives in `weekly.yaml`, not `verify.yaml`**: per-PR CI stays
  single-version for latency, matching Flink's own `push_pr.yml` / `weekly.yml` split.
  `verify.yaml` names no Flink version and no ceiling, so bumping the pom moves it with no
  edit. Inside the matrix:
  - Every job checks out `github.sha` rather than a branch — a merge landing mid-run once made
    one version look like it had silently skipped 60 tests.
  - Rows carry a **role** (`floor` / `ceiling` / `next` / `lts`), not a version, because
    GitHub does not expose the `env` context to `strategy` and a version repeated across rows
    is how one of them gets missed; the version is resolved in a step from `FLINK_CEILING` /
    `FLINK_NEXT_SNAPSHOT` / `FLINK_LTS` at the top of the file. Those envs are hand-edited —
    dependabot does not see workflow env, an accepted staleness.
  - The `floor` row passes no `-Dflink.version` at all, so the pom stays the single source of
    truth for it, and it runs on JDK 21 because floor-on-17 is already covered by `verify.yaml`
    and by `binary_compat`.
  - The `next` row is upstream early-warning and is deliberately **not** `continue-on-error`.
- **`scripts/check-flink-release.sh` (the `new_minor_check` job) announces new upstream
  minors**, because suppressing the dependabot minor PR removed the only thing that did. It
  compares the ceiling against Maven Central weekly and fails until the range is moved. It is
  deliberately **not** a dependency of the other jobs: a new upstream release must not stop the
  current range from being verified.

## Evidence

The `just binary-compat` recipe is the measurement, and its order is load-bearing:
floor-build, install, fingerprint, ceiling-rerun with nothing recompiled, diff. The install
step (root pom + each connector a SQL uber-jar bundles + the base module every connector
compiles against + the test-utils module every module's tests depend on) exists because the
goal-only rerun cannot resolve inter-module dependencies from the reactor — the same mechanism
that bit the licence goal through the SQL uber-jar on PR
[#181](https://github.com/laughingman7743/flink-connector-gcp/pull/181). Run by hand it primes
`~/.m2` with `io.github.flink-gcp` SNAPSHOTs; the recipe comment carries the cleanup line.
Reproducing a red weekly `binary_compat` locally with this recipe is the first move when the
row goes red.

## Alternatives declined

- **Per-minor artifacts** as `apache/flink-connector-kafka` publishes them (`5.0.0-2.1` /
  `5.0.0-2.2` from one branch; Paimon and Iceberg do the same). Not rejected forever: this is
  the recorded **fallback if `binary_compat` ever goes red**. Until then one artifact is
  cheaper to build, test and (eventually, [#29]/[#39]) publish.

[#29]: https://github.com/laughingman7743/flink-connector-gcp/issues/29
[#39]: https://github.com/laughingman7743/flink-connector-gcp/issues/39
[#102]: https://github.com/laughingman7743/flink-connector-gcp/issues/102
