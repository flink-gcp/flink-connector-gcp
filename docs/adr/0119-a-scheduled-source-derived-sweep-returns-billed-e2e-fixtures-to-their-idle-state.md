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

# ADR-0119: A scheduled source-derived sweep returns billed E2E fixtures to their idle state

- Status: Accepted
- Date: 2026-08-02; revised by [#224] (2026-08-10), [#630] (2026-08-14) and the interval change
  of 2026-08-18
- Issues: [#224], [#246], [#630]
- Modules: bigtable, spanner, cloudtasks (tests), scripts, CI
- Current behavior: [`sweep-e2e.sh`](../../scripts/sweep-e2e.sh),
  [sweep workflow](../../.github/workflows/sweep-e2e.yaml),
  [`just sweep-e2e`](../../justfile)

## Context

The Bigtable real-GCP harness deletes its ephemeral instance in teardown and sweeps stale instances at the start of the next gated class.
Neither path runs after a killed runner, a cancelled job, a process crash or an interrupted local run, and the only scheduled gated run was weekly.

Issue [#246] priced that gap at the then-published $0.65 per node-hour.
One leaked node standing for seven days cost about $109, while two gated classes could leave two instances from one run.
Bigtable has no server-side instance expiration analogous to the test bucket and dataset expiry used elsewhere in the project.

The same lifecycle shape later reached Spanner through [#224].
Cloud Tasks then added a persistent App Engine Standard version through [#630]; its idle state is `STOPPED` with zero instances rather than deletion.
Each fixture has a normal cleanup path, but all need a process-independent fallback.

## Decision

The Bigtable, Spanner and App Engine fixtures in the shared sweep have two cleanup layers.
The harness or lifecycle wrapper restores the idle state after an ordinary run, and a scheduled GitHub Actions workflow invokes `just sweep-e2e` after hard cancellation prevents that cleanup.
The interval is a priced parameter rather than a fixed property of the design: it moved from weekly to daily when the sweep was introduced, and to three hours once the repository became public and its Actions minutes free.
The staleness threshold, not the timetable, is what keeps a sweep from deleting a running instance, so shortening the interval is the safe direction and lowering the threshold is not.
That safety rests on every run finishing inside the threshold, which `e2e.yaml`'s 60-minute ceiling guarantees and a local gated run does not: one left running past two hours can have its instance swept from under it, and a shorter interval makes that pre-existing hazard likelier to be hit.
It is not a service-side expiry and cannot bound a delayed or disabled workflow run.

One shell script owns the shared sweep.
It attempts Bigtable deletion, Spanner deletion and App Engine stop independently, then returns the worst status so one service's failure cannot prevent another service from being cleaned.
A separate justfile line per service was rejected because a recipe stops after its first failing line and would skip every later guardrail.

The Bigtable and Spanner instance prefixes and staleness thresholds are parsed from the Java test classes that own them.
The App Engine transition delegates to the lifecycle wrapper that reads the checked-in fixture identifiers.
The script does not repeat those values because a stale copy could turn a broken sweep into the same empty result as a project with nothing to delete.
Missing sources, unparsable constants and failed resource listings are therefore hard errors.

Only instance identifiers with the owned prefix and a parseable creation epoch older than the source threshold are deleted.
An identifier the script cannot date is left alone, matching the Java harness's conservative deletion boundary.
`--dry-run` reports eligible changes without applying them.

The workflow authenticates through the existing E2E Workload Identity Federation boundary and uses the E2E service account's project-scoped service roles.
The sweep does not enable the Billing Budgets API, grant a billing-account-level role or put a billing account identifier in source or state.
A billing budget detects costs that this resource-specific sweep cannot foresee, but that billing-account boundary remains outside this repository.

## Evidence

[PR #259](https://github.com/laughingman7743/flink-connector-gcp/pull/259) implemented the Bigtable sweep and priced one daily interval at about $15 using the same historical rate, compared with about $109 for the previous seven-day interval.
The existing WIF condition already admitted `schedule` and `workflow_dispatch` on `main`, so the workflow required no new OpenTofu grant.

The 2026-08-18 change to three hours priced the whole range the same way.
A leaked instance is billed for the two-hour staleness threshold plus the wait for the next sweep, at the same historical $0.65 a node-hour this ADR has used throughout, for the single node an ephemeral instance runs:

| Interval | Worst-case billed lifetime | Cost |
| --- | --- | --- |
| Weekly | 170 h | ~$110 |
| Daily | 26 h | ~$17 |
| Every three hours | 5 h | ~$3 |
| Hourly | 3 h | ~$2 |

The knee is the threshold: below roughly two hours it dominates the total, so an hourly sweep saves about a dollar over a three-hourly one while running three times as often — 24 runs a day against 8.
What moved the trade-off is not the arithmetic but the repository becoming public, which makes the added runs free; each sweep run took 20 to 30 seconds across the eight most recent runs before the change, seven scheduled and one manual.

A leak observed on 2026-08-17 cost about $9 and is what prompted the repricing.
An interrupted gated run created an instance at 13:04 UTC; the killed run's own teardown never ran, and the next run's sweep correctly skipped it at 1.4 hours old.
The then-daily workflow would have reclaimed it 17 hours later, and it was deleted by hand instead.
All three cleanup layers behaved as specified, which is why this is an interval change rather than a defect fix.

The first script piped the resource listing into a loop.
A failed process substitution did not trip `set -e`, so an authentication or API failure produced an empty list and a false successful result.
Capturing and checking the listing before iteration made that failure loud.
Six mutation probes covered prefix, age, date parsing, listing failure, dry-run behavior and unknown-argument rejection; the prefix and date probes each exposed an initially non-discriminating negative test.

When [#224] added Spanner, the script became service-parameterized and preserved independent attempts.
That refactor found a second shell boundary: calling a function under `|| outcome=$?` suppresses errexit inside the function, so the resource listing remains explicitly checked rather than relying on shell mode.

[PR #643](https://github.com/laughingman7743/flink-connector-gcp/pull/643) added the fixed App Engine fixture and reused the same lifecycle wrapper after OpenTofu apply, around gated acceptance and in the shared sweep.
The verified idle state is `STOPPED` with zero instances, and runtime instance-count changes are the only part excluded from OpenTofu ownership.

## Alternatives declined

- **Rely on test teardown and next-run startup cleanup**: neither executes after every hard-cancellation path, and the next scheduled class was one week away.
- **Drive the scheduled cleanup through Maven and the Java harness**: it would install a JDK and build the reactor to perform resource administration that `gcloud` can finish directly.
- **Copy prefixes and thresholds into workflow configuration**: either copy could drift and make a broken sweep report an honest-looking empty result.
- **Put each service in a separate justfile line**: the first failure would prevent later cleanup from running.
- **Manage a billing budget in the OpenTofu root**: budgets live on the billing account and would require the first grant outside the project's IAM boundary plus an account identifier in configuration and state.
- **Use a Bigtable node quota as the leak control**: a count quota can stop runaway creation but cannot shorten the billed life of one allowed leaked instance.

## Consequences

Adding a billed E2E fixture extends both its normal lifecycle cleanup and the shared scheduled sweep.
Its safe idle state, source of identifiers and failure behavior must be testable without creating a real resource.

The scheduled job shortens the intended fallback interval for known fixture leaks but does not claim a hard time or spending bound, or general billing detection.
Any proposal to manage those concerns must separately address billing-account permissions and identifier handling.

[#224]: https://github.com/laughingman7743/flink-connector-gcp/issues/224
[#246]: https://github.com/laughingman7743/flink-connector-gcp/issues/246
[#630]: https://github.com/laughingman7743/flink-connector-gcp/issues/630
