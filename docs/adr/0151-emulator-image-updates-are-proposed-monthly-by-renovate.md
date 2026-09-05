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

# ADR-0151: Emulator image updates are proposed monthly by Renovate

- Status: Accepted
- Date: 2026-09-05
- Issues: [#1200](https://github.com/flink-gcp/flink-connector-gcp/issues/1200), [#1232](https://github.com/flink-gcp/flink-connector-gcp/issues/1232)
- Modules: CI, test-utils
- Current behavior: [Emulator image updates](../content/docs/development/testing.md#emulator-image-updates)

## Context

An image disappearing from a registry can fail every connector lane that consumes it.
That happened when `google-cloud-cli:441.0.0-emulators` stopped resolving in September 2026 ([#1196](https://github.com/flink-gcp/flink-connector-gcp/issues/1196)).
Dependabot handles Maven, GitHub Actions and Go modules here, but none of those managers reads the Java constants that pin the test emulators.

[PR #1223](https://github.com/flink-gcp/flink-connector-gcp/pull/1223) implemented a weekly manifest and retention probe, then was closed unmerged in favor of this change.
Its review record measured gcr.io's `tags/list` extension on 2026-09-05: the oldest surviving `timeUploadedMs` was 2025-09-09 across all 2,034 tags in the Cloud SDK repository.
The 2023 `441.0.0-emulators` release had still been served until September 2026, so that observed window was recent rather than evidence of a long-standing guarantee.
The same investigation found Spanner tags back to 2020, no upload times in ghcr.io's registry API, and no equivalent ghcr.io retention window to measure.
It also established that `docker manifest inspect` needs no Docker daemon.
These are the earlier investigation's dated observations, not a retention contract or measurements repeated by Renovate.

## Decision

Run self-hosted Renovate on the existing organization-owned App from [ADR-0121](0121-ci-push-back-runs-as-an-org-owned-github-app-with-per-use-downscoped-tokens.md), with a token scoped to this repository and contents/pull-requests write.
The workflow runs on the first day of each month at 03:23 UTC and can be dispatched manually on `main` for initial verification or recovery.
The cron is the only scheduling restriction, so a manual recovery does not wait for another monthly window.
Concurrent runs are serialized and each run has a twenty-minute timeout.
Missing or invalid App credentials fail the job.
The repository guard uses its immutable ID so forks stay excluded after a repository rename or transfer.

One regex manager reads the complete image literals from the `IMAGE` declarations in `*EmulatorContainers.java`, including both String and DockerImageName initializers.
Docker versioning preserves the `-emulators` compatibility suffix.
The five declarations describe four images: Bigtable and Pub/Sub share the Cloud SDK image and update together.
All updates share one branch and pull request, including updates Renovate classifies as major; a monthly run need not create a new pull request if there are no updates or one is already open.
The other managers, dependency dashboard and onboarding stay disabled.
The workflow supplies global log remaps for Docker package lookup failures, `Host error`, `Git error - aborting`, and missing, disabled or invalid repository configuration, so these paths produce a non-zero exit.
Loading the remaps before repository initialization also covers a clone failure that prevents Renovate from reading the repository configuration.

The action is pinned to a commit, while the Renovate container follows that action's default major release line.
Dependabot continues to propose action updates.
Validation records the concrete Renovate version used, since the monthly runtime can receive newer releases within that line.
The configuration uses JSON5 to keep its license header and comments in the file.

## Alternatives declined

The weekly probe still leaves its repair to a hand-written pull request.
Monthly grouped updates propose that repair directly, at a cadence that leaves time to review the emulator's changed behavior.
The weekly probe and its checker are therefore not introduced.

Renovate at the upstream release cadence would produce frequent emulator update pull requests, each requiring the same review.
A monthly run and one group address that cost, including Spanner's independent releases.
A hosted Renovate App would add an installation when the existing App already grants the permissions this job uses.

## Consequences

Updates arrive as draft pull requests in the repository's WHAT/WHY format.
Their bodies ask for the Bigtable deviation suites' verdict (ADR-0044) and, when Spanner changes, the pinned-emulator measurements recorded in ADR-0075 and ADR-0077.
They also ask reviewers to update the current `docker run` instructions in the Spanner and Cloud Tasks example pages when those images move; the manager updates only the Java declarations.
The existing CI selection follows changes in test-utils through its consumers; a human reviews the results and merges, and automerge stays disabled.

Keeping the pins current depends on those pull requests being reviewed and merged.
A successful run does not prove that an unmerged pin will remain available, and a schedule that never starts has no failed run to inspect.
The Actions run history and outstanding update pull request are the operational record; there is no independent retention monitor.
