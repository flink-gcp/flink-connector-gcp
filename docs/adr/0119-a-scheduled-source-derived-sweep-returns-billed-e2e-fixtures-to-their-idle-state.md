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
- Date: 2026-08-02; revised by [#224] (2026-08-10), [#630] (2026-08-14), the interval change
  of 2026-08-18, and [#1161] (2026-08-30)
- Issues: [#224], [#246], [#630], [#1161]
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
The staleness threshold, not the timetable, is what keeps an age-gated sweep from deleting a running instance, so shortening the interval is the safe direction and lowering the threshold is not.
For the `integration-tests` path, that safety rests on every fork that creates and uses an owned instance finishing inside the threshold.
Issue [#959] supplied that backstop: surefire kills each fork after `it.fork.timeout.seconds`, 90 minutes by default.
**That ceiling is coupled to this threshold rather than merely generous** — while it stays below two hours, a fork cannot keep one of its own Bigtable or Spanner instances in use until that instance crosses the age gate.
Raising the fork ceiling past the threshold removes that age-gated guarantee, which is why its POM definition says what depends on it.
`e2e.yaml`'s separate job ceiling bounds the whole CI window and how long the fixed App Engine fixture can stay serving before the post-E2E sweep, but the fork ceiling already bounds Bigtable and Spanner in-use instance age if that job value later rises.
A gated class selected through surefire's `default-test` execution with `-Dtest` and `-Dtest.excluded.groups=`, or run from an IDE, does not inherit the integration-test fork ceiling.
If that runner survives the class's `@Timeout` and remains live for two hours, the next class's startup sweep or a scheduled sweep can delete its instance from under it.
The 90-minute value deliberately reserves most of the headroom below two hours.
Surefire fork pools are module-local, so the main gated Maven invocation applies the ceiling independently to each connector rather than across its combined class list.
At selection time, the only measurement was Bigtable on 2026-08-18: seven classes took 20.6 minutes of total work across two forks.
The other module-local workloads were unmeasured, so 45 minutes was not justified.
Individual `@Timeout` values are not a substitute for this ceiling, which bounds the reused fork when it will not die.
The two manual BigQuery schema-propagation probes do not use a fixture managed by this sweep, and their documented commands scope the raised property to the BigQuery module and one probe class; their three-hour timeouts therefore require a higher command-line fork ceiling without weakening this guarantee.
It is not a service-side expiry and cannot bound a delayed or disabled workflow run, which is not a hypothetical: GitHub schedules are best effort and may be dropped under load, and this repository's slots have started 38 to 91 minutes late.
The sweep therefore also triggers on the E2E workflow's completion in every terminal state (#964), so the run that creates the fixtures is always followed by one that reclaims them, whatever the schedule does.
That trigger is `workflow_run` rather than a finalising step inside the E2E job because a step dies with its job and hard cancellation is the case the sweep exists for; the cost is one more event name on the E2E service account's Workload Identity binding, which widens which event may assume it and not the repository, ref or roles.

One shell script owns the shared sweep.
It attempts Bigtable deletion, Spanner deletion and App Engine stop independently, then returns the worst status so one service's failure cannot prevent another service from being cleaned.
A separate justfile line per service was rejected because a recipe stops after its first failing line and would skip every later guardrail.

The Bigtable and Spanner instance prefixes and staleness thresholds are parsed from the Java test classes that own them.
The App Engine transition delegates to the lifecycle wrapper that reads the checked-in fixture identifiers.
The script does not repeat those values because a stale copy could turn a broken sweep into the same empty result as a project with nothing to delete.
Missing sources, unparsable constants and failed resource listings are therefore hard errors.

Only instance identifiers with the owned prefix and a parseable creation epoch older than the source threshold are deleted.
The threshold has two exceptions, and both are the caller's to state: the sweep that runs when the E2E workflow has finished passes `--all` and drops the age comparison, because that workflow is over and anything still carrying the prefix is a leak (#966), and a manual dispatch may ask for the same through a typed boolean input.
The manual switch exists because the operator reaching for it has usually just seen a leak, and the threshold would skip exactly the fresh instance they are trying to reclaim — the case that sent one cleanup through `gcloud` by hand.
It defaults to off, so an absent-minded dispatch behaves like the schedule.
Without it the post-E2E trigger reclaims nothing — the instances it exists to collect are minutes old, which is precisely what the threshold skips.
`--all` widens which of the sweep's own instances go, never which instances are its own: the prefix guard is untouched, so a neighbouring production instance is as safe as before.
The accepted cost is a local gated run in flight when the weekly E2E finishes, which loses its instance and fails loudly.
An identifier the script cannot date is left alone, matching the Java harness's conservative deletion boundary.
`--dry-run` reports eligible changes without applying them.

The workflow authenticates through the existing E2E Workload Identity Federation boundary and uses the E2E service account's project-scoped service roles.
The sweep does not enable the Billing Budgets API, grant a billing-account-level role or put a billing account identifier in source or state.
A billing budget detects costs that this resource-specific sweep cannot foresee, but that billing-account boundary remains outside this repository.

## Evidence

[PR #259](https://github.com/flink-gcp/flink-connector-gcp/pull/259) implemented the Bigtable sweep and priced one daily interval at about $15 using the same historical rate, compared with about $109 for the previous seven-day interval.
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

Issue #959's two-layer ceiling was measured rather than reasoned.
Against an interruptible wait both `@Timeout` modes end at the deadline, so the default mode is not useless; against a retry loop that swallows the interrupt — the shape of `CollectResultIterator.hasNext()`, where #951 hung — the default mode never ends, while `SEPARATE_THREAD` ended a 3-second deadline in 3.0 seconds and abandoned the body still running.
Abandoning is all JUnit can do to a thread, which is why the surefire fork ceiling exists beneath it: #951's fork outlived its reported timeout by more than 40 minutes, so something in the abandoned work kept that JVM alive.

The first script piped the resource listing into a loop.
A failed process substitution did not trip `set -e`, so an authentication or API failure produced an empty list and a false successful result.
Capturing and checking the listing before iteration made that failure loud.
Six mutation probes covered prefix, age, date parsing, listing failure, dry-run behavior and unknown-argument rejection; the prefix and date probes each exposed an initially non-discriminating negative test.

When [#224] added Spanner, the script became service-parameterized and preserved independent attempts.
That refactor found a second shell boundary: calling a function under `|| outcome=$?` suppresses errexit inside the function, so the resource listing remains explicitly checked rather than relying on shell mode.

[PR #643](https://github.com/flink-gcp/flink-connector-gcp/pull/643) added the fixed App Engine fixture and reused the same lifecycle wrapper after OpenTofu apply, around gated acceptance and in the shared sweep.
The verified idle state is `STOPPED` with zero instances, and runtime instance-count changes are the only part excluded from OpenTofu ownership.

## Alternatives declined

- **Rely on test teardown and next-run startup cleanup**: neither executes after every hard-cancellation path, and the next scheduled class was one week away.
- **Drive the scheduled cleanup through Maven and the Java harness**: it would install a JDK and build the reactor to perform resource administration that `gcloud` can finish directly.
- **Copy prefixes and thresholds into workflow configuration**: either copy could drift and make a broken sweep report an honest-looking empty result.
- **Put each service in a separate justfile line**: the first failure would prevent later cleanup from running.
- **Leave the fork side unbounded and rely on the threshold alone**: the threshold cannot tell a leaked instance from one a still-running local fork is using, so without a ceiling on that fork the two are only distinguishable by luck.
- **Manage a billing budget in the OpenTofu root**: budgets live on the billing account and would require the first grant outside the project's IAM boundary plus an account identifier in configuration and state.
- **Use a Bigtable node quota as the leak control**: a count quota can stop runaway creation but cannot shorten the billed life of one allowed leaked instance.

## Consequences

Adding a billed E2E fixture extends both its normal lifecycle cleanup and the shared scheduled sweep.
Its safe idle state, source of identifiers and failure behavior must be testable without creating a real resource.

The scheduled job shortens the intended fallback interval for known fixture leaks but does not claim a hard time or spending bound, or general billing detection.
Any proposal to manage those concerns must separately address billing-account permissions and identifier handling.

[#224]: https://github.com/flink-gcp/flink-connector-gcp/issues/224
[#246]: https://github.com/flink-gcp/flink-connector-gcp/issues/246
[#630]: https://github.com/flink-gcp/flink-connector-gcp/issues/630
[#959]: https://github.com/flink-gcp/flink-connector-gcp/issues/959
[#1161]: https://github.com/flink-gcp/flink-connector-gcp/issues/1161
