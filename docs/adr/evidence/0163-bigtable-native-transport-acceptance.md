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

# Bigtable native-transport acceptance of checkpoint-owned writes

This record covers the half of the [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319) correctness acceptance that the [production recovery lease](0163-bigtable-production-recovery-service-plan.md) excludes.
That lease connects the production sinks to a loopback proxy through `emulatorEndpoint(...)` so it can discard one successful response deterministically; the proxy's own client authenticates to Bigtable, and the connector's native TLS and application-default-credentials branch never runs.
The gated class `BigtableStagedSinkRealGcpITCase` runs the same production `BigtableSink.builder()` and Table factory sinks without an emulator endpoint, key file or forwarding client, so the connector dials the data and both admin endpoints itself.
It makes no performance claim and cannot inject an ambiguous response; response-loss recovery remains the lease's oracle.
The [attempts](#attempts) section records each run; the class passed all six invocations on Flink 2.2.1 (attempt 3) and on Flink 1.20.4 (attempt 1) on 2026-09-14.

## Fixtures and scenario

Each class run provisions one ephemeral instance under the gated suite's contract (ADR-0044): `flink-it-<epoch>-<runId>`, one SSD node in `us-central1-b`, deleted after the class and swept after two hours.
Inside it the class creates three application profiles, `single-cluster` with transactional writes, `no-tx` without them and `multi-cluster`, and six tables.
`staged-datastream`, `staged-table` and `staged-no-tx` carry the documented shape: a data family, an INT64 SUM family `agg` and the raw marker family `flink_commit` without a GC rule.
`marker-missing`, `marker-gc` (one retained version) and `marker-typed` (INT64 SUM) are the rejected marker shapes.

The accepted scenario stages 24 distinct inputs at parallelism 2 from a held source with an hour-long checkpoint interval and an equal minimum pause, which pins Flink's randomly drawn first periodic trigger at one hour, so the stop-with-savepoint is the only checkpoint that can complete and nothing reaches the table before it.
The hot-row distribution places 21 inputs on one row and one on each of three others.
Restoring that same savepoint at parallelism 1 and stopping again, then at parallelism 3 and finishing, re-commits the same envelopes, because the snapshot precedes the commits it authorized; the markers absorb them.
The delegating committer around the production committer records every request it receives and the signal the production committer answers with, so each restore must replay exactly the 24 original identities on their original rows and have each one reported as already committed by the service.
The readback then compares the retained markers with that identity-to-row inventory.
A local control on the loopback fake shows the replay assertion fails both for a restore whose committer sends nothing and for a restore that produces no commit request, which the readback alone could not tell from an absorbed replay.
After each phase the readback must show one SUM cell per row equal to that row's distinct contributions, one 32-character marker per envelope on the row it protected with timestamp zero and value `1`, no other family, and the writer's serialization count unchanged at 24.
The run repeats for the DataStream builder and the Table factory (`sink.delivery-guarantee = 'exactly-once'`, `sink.write-mode = 'aggregate'`).

## Paths exercised and not exercised

The credential path has no runtime observable of its own.
The class asserts that the configuration the production sink was built from carries neither an emulator endpoint nor a service-account key file, and that the scenario succeeded against the service; together those select the application-default-credentials branch of the client builders.
Locally that means the owner's user credentials; in the weekly suite it means the workload-identity token of the E2E service account.

The class also measures the direct remote error paths that need no IAM change:

- `GetAppProfile` and `GetTable` over the native admin transport, with the `no-tx` and `multi-cluster` profiles rejected before any write by the transactional-routing message, and the three marker shapes rejected by the raw-marker-family message.
- A nonexistent table and a nonexistent profile, surfaced as `NOT_FOUND` wrapped in the metadata-validation failure.
- A `CheckAndMutateRow` against a nonexistent table through the production single-row client, classified fatal with the missing-table explanation in the job failure.
- The committer's own ordering: a DataStream job under `no-tx` fails at its first stop-with-savepoint before a target write, and a Table job against `marker-gc` fails when its writer opens, leaving both tables empty.

The staged committer does not route its data-path failures through the single-row request classification; a commit failure fails the checkpoint attempt so that a restore retries the unchanged envelope.
The classification assertion therefore drives the production client directly.

The following remain unexercised, and deliberately so: the service-account key-file branch (the test project's identities are keyless; unit tests pin its injection), `PERMISSION_DENIED` and a missing `bigtable.appProfiles.get` or `bigtable.tables.get` grant (an IAM change), `UNAUTHENTICATED`, `RESOURCE_EXHAUSTED`, and the ambiguous statuses `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `ABORTED`, which cannot be induced deterministically against the service and whose only coverage is the proxy lease's injected `UNAVAILABLE`.

## Limits and cost

One class run holds one node for its lifetime, expected under 15 minutes.
Node billing is per clock hour, so a run bounds at one node-hour, or two if it crosses an hour boundary, at USD 0.65 per hour.
The acceptance needs one run per supported Flink line, and the authorization allows four class runs in total, each of which may cross an hour boundary, so at most eight clock node-hours, USD 5.20 (USD 6.80 at the Enterprise Plus rate), inside the USD 20 aggregate ceiling reconciled in the recovery plan.
The owner first framed those four runs as one retry per line and on 2026-09-14, after two Flink 2.2.1 runs had failed on harness defects, allowed the remaining two to be spent as one more run per line.
No scheduling constraint is relied on to stay under that bound; a run that starts late in a clock hour simply spends its second hour.
The class joins the weekly gated suite, which the owner accepted on 2026-09-14 as a recurring cost of one node-hour per week, two when the run crosses an hour boundary.

The Flink 1.20 line is run by hand with `-Dflink.version=1.20.4 -Dflink.compat=flink1` after cleaning the reactor and the project's own artifacts from the local Maven repository; the `e2e` recipe and the weekly workflow run the pom's default line only.

## Attempts

### Attempt 1 on Flink 2.2.1, 2026-09-14

Instance `flink-it-1789350401-cf737afa` was created at 01:46:41 UTC from source `51279d2317285989481195ce780864a78db51d61` and deleted by the class teardown before 01:50:19 UTC; the instance list was empty afterwards.
Three of the six test invocations passed: the direct metadata rejections, the Table writer's rejection of the GC-managed marker family, and the native data client's `NOT_FOUND` classification.
Both parameterized scenarios completed their native initial phase: 24 inputs stayed invisible until the stop-with-savepoint, the production committer applied all 24 over the connector's own transport, and the readback matched the inventory for the DataStream and the Table sink.
Both then failed at the parallelism-1 restore with `UnavailableDispatcherOperationException` because the RUNNING wait accepted the empty execution graph the dispatcher returns while a JobManager is still initializing; sources after `51279d2317285989481195ce780864a78db51d61` require the job status to be RUNNING and at least one vertex before the wait ends.
The `no-tx` scenario failed on its assertion, not on the service: the committer rejected the profile, but a stop-with-savepoint that fails during stopping reports `StopWithSavepointStoppingException` on both the operation and the job result and, among what the archived execution graph exposes, keeps the task's cause only on the failed execution's failure info; sources after `51279d2317285989481195ce780864a78db51d61` read every execution's failure info into the assertion.
The run consumed one clock node-hour, USD 0.65.

### Attempt 2 on Flink 2.2.1, 2026-09-14

Instance `flink-it-1789353213-75fddade` was created at 02:33:33 UTC from source `a767a069093220be4798a3a48868fa59c24e9609` and deleted by the class teardown before 02:40:46 UTC; the instance list was empty afterwards.
Four of the six test invocations passed: the metadata-validation and GC-managed-marker rejections and the native `NOT_FOUND` case, as in attempt 1, plus the `no-tx` committer rejection, whose message the repaired assertion read from the failed execution's failure info.
Both scenarios failed earlier than in attempt 1: their initial stop-with-savepoint, which attempt 1 had completed about 62 seconds after task start on the DataStream entry and about 14 seconds on the Table entry, did not complete within the 90-second control bound on either entry point, and the phase was cancelled with its executions in CANCELED state and no failure recorded.
The JobManager's stopping warning names the created savepoint for both scenarios, so the time went after the savepoint was written; the harness's 30-second waits after cancellation also expired, on the job result for the Table entry and on the MiniCluster shutdown for the DataStream entry.
The native `NOT_FOUND` case passed in 88 seconds; attempt 1 retained no per-test time for it, and that run's class total leaves about 80 seconds for it, so the duration is not new to this run.
The log does not show where in the post-savepoint stage the time went, because the MiniCluster logs only warnings and the class logged no phase timings.
The run consumed one clock node-hour, USD 0.65, and used the second of the four authorized runs.
For the third run the class logs each phase with a timestamp, publishes each commit observation before the production committer runs and dumps them with the vertex states when a stop does not complete, and bounds its control futures at 180 seconds instead of the lease's 90, so a slow commit stage is measured rather than cancelled; the correctness oracles are unchanged.

### Attempt 3 on Flink 2.2.1, 2026-09-14

Instance `flink-it-1789356168-c4cfdb4c` was created at 03:22:48 UTC from source `520843c0102fb7b399d3582c54e25ca81fbbf0bf` and deleted by the class teardown before 03:26:47 UTC; the instance list was empty afterwards.
All six invocations passed in 240.8 seconds.
The phase log places the DataStream initial stop-with-savepoint at 15 seconds after the request and the Table one at 52 seconds, both inside the 180-second bound; the restore to parallelism 1 reached RUNNING 17 and 19 seconds after submission and the restore to parallelism 3 after 44 and 18 seconds.
The native data client took 12 seconds to create and answered the `NOT_FOUND` request in under a second.
Both scenarios passed the oracles: an inventory of 24 applied envelopes after the initial stop, exactly 24 deduplicated replays on the original rows after each restore, and an unchanged readback each time; the log records only the final acceptance PASS line per scenario.
The run consumed one clock node-hour, USD 0.65, and used the third of the four authorized runs.

### Attempt 1 on Flink 1.20.4, 2026-09-14

Instance `flink-it-1789356504-b9a42669` was created at 03:28:24 UTC from the same source and deleted by the class teardown before 03:33:07 UTC; the instance list was empty afterwards.
All six invocations passed in 284.3 seconds.
The DataStream initial stop-with-savepoint completed 13 seconds after the request and the Table one 45 seconds after it; the DataStream restores reached RUNNING about 48 seconds after submission, and the native data client took about 8 seconds to create.
The run consumed one clock node-hour, USD 0.65, and used the fourth authorized run.

## Result

The connector's native TLS and application-default-credentials branch carried the checkpoint-owned mode through both production entry points on both supported Flink lines, with the metadata rejections, the `NOT_FOUND` classification and the replay oracle passing as recorded above.
The four runs consumed four clock node-hours, USD 2.60 at the Server Node rate, inside the USD 5.20 reservation.
In the two passing runs the initial stop-with-savepoint of the Table entry point took 45 to 52 seconds after the request and the DataStream one 13 to 15 seconds, while attempt 1 on Flink 2.2.1 had measured the reverse ordering from task start; the variation is unexplained by this record and belongs to the performance assessment in [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327), not to correctness.
