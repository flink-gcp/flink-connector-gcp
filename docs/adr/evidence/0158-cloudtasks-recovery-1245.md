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

# Cloud Tasks recovery acceptance protocol

This is the preregistration for [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245), within [ADR-0158](../0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md) and [ADR-0154](../0154-support-follows-published-google-cloud-specifications.md).
Three separately approved runs executed on 2026-09-10.
The [first result](#result-on-2026-09-10) records 38 successful cases before operation reservations were exhausted; the [second result](#second-result-on-2026-09-10) records all 48 fault cases before a direct-path checkpoint observation timed out.
The [third result](#third-result-on-2026-09-10) records all 56 recovery cases passing on each Flink line and all 24 positive tombstone observations, but none of the twelve negative controls recreated within the registered observation period.
All three runs verified resource cleanup.
The [owner's acceptance clarification](#acceptance-clarification-on-2026-09-10) adopts the completed recovery and positive-retention observations; actual post-tombstone recreation remains unobserved and is an optional additional experiment.
Local calibration validates the harness against an assumed model; it supplies no service acceptance.
The protocol and failed-run dispositions below preserve the original preregistration; the clarification changes the acceptance requirement, not those execution results.
Performance and release disposition remain [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).

## Questions and coverage

The recovery matrix uses Flink 1.20.4 and 2.2.1, each with DataStream and Table, HTTP and App Engine tasks, and persisted random names and stable-key names.
Each of the eight API/target/identity combinations executes these cases:

| Case | Observation required |
| --- | --- |
| Lost response | No request while a checkpoint acknowledgement is deliberately withheld; capture the service success before suppressing the response; restore identical named Task bytes and observe the original live creation. |
| Partial commit and lost durable acknowledgement | Withhold the second creation's successful response; cancel with a readable retained checkpoint; separately restore committer parallelism 2 to 1 and 2 to 3, preserving the original requests. |
| Terminal expiry | Tighten current recovery options without replacing the saved origin; bounded restarts fail before any new request and preserve readable checkpoint metadata; an explicit DROP recovery finalizes four dropped envelopes and completes another checkpoint without a request. |
| Finished stop-with-savepoint | Observe FINISHED, then resume that savepoint within the original window and re-send the original requests. |
| Old incarnation | Hold a request before forwarding, cancel its downstream client, then let it complete under its original absolute deadline; record and observe creation before starting the replacement, which deduplicates that same request. |
| Direct production path | Use the public builder or actual Table factory without an emulator endpoint; default v2beta3 retention preflight and credential resolution succeed, and four tasks appear only after checkpoint completion. |

There are 48 fault-injection parameterizations plus eight direct-path parameterizations per Flink version.
The same 48 fault-injection cases run locally in `CloudTasksRecoveryHarnessITCase` against an in-memory service model.
`CloudTasksRecoveryModelTest` uses an explicit generation counter and a controlled clock to check authorization immediately before, at, and after the deadline, including a deliberately unauthorized post-tombstone re-creation.
The existing staged writer, committer, lifecycle, recovery and Table tests retain coverage of envelope origin/deadline serialization, the final bounded batch, mode transitions, expiry overrides and failed-stop runbooks.
Run those affected suites on both supported lines; also run the recovery calibration on the supported 2.x ceiling, 2.3.0.

The forwarding proxy uses the existing emulator-endpoint option only to route the connector's requests to a loopback server.
That server forwards the same v2 request to GCP, retaining the incoming absolute deadline and recording the real response before fault injection.
Its upstream call is detached from downstream cancellation so an old incarnation can finish without informing the replacement.
This injection path bypasses the connector's retention preflight, so the separate direct-path cases cover that preflight.
The controlled old-incarnation case does not establish a bound on arbitrary server-side late effects.

## Creation oracle and controls

Raw JSONL records contain request and response protobuf bytes in base64, wall-clock observation times, remaining RPC deadline, client incarnation, case identity, queue readback and checkpoint-related outcomes.
Each response record is flushed to disk before it can reach the connector or be suppressed.
Keep the exact manifest, source checksums, ordered runtime classpaths, Java version, test reports and all partial logs with these records.
Test payloads contain only generated identifiers; exclude credentials, private filesystem paths and account identities from published evidence.

`TaskCreationLedger` distinguishes a received request, a successful response, a live creation generation, and removal.
Repeated successful responses carrying one `create_time` do not establish two creations.
Before removal, require both GetTask and ListTasks to observe the captured generation.
An unexpected changed creation time fails the oracle.
An unrecorded old-incarnation creation cannot silently become a successful observation.
Handler counts and task-name headers are never the creation oracle.

The slow phase uses twelve controls: HTTP/App Engine, explicit deletion/successful execution/retry exhaustion, and ordinary/old-client response isolation.
All original tasks are created on paused queues and observed live before removal.
The execution-removal phase uses [unlimited attempts and retry duration](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/RetryConfig) (`maxAttempts=-1`, `maxRetryDuration=0`).
Within its five-minute limit, the published task lifetime and exclusive administrative history exclude TTL expiry, retry exhaustion and external deletion, so disappearance is evidence of successful execution under those prerequisites.
After those tasks disappear, pause the queues and update only `retry_config` to bounded retries, record and read back that change, then create the retry-exhaustion controls.
Those cases must additionally expose a failed dispatch attempt before disappearance.
Use only the existing fixture's `/accepted` and `/unavailable` handlers.
The fixture revision and its HTTP status responses are checked before dispatch; the test never deploys another handler.
Resume the queues only for each removal phase, require removal within five minutes per phase, then pause both queues again.

Require `ALREADY_EXISTS` immediately after removal and at a second observation scheduled fifty minutes after the earliest observed removal.
After at least one hour and five seconds from the latest observed removal, intentionally submit the same test IDs again.
Poll only the still-protected IDs every thirty seconds for at most one further hour, within the overall lifetime.
Require a new successful response, a strictly later second-truncated `create_time`, and both live readbacks of that new generation.
The old-client arm suppresses the successful response from a separate downstream client too, proving that the external observer detects a creation the current client cannot acknowledge.
These negative controls deliberately issue fresh RPCs after authorization has expired; they do not extend a production RPC deadline or claim to reproduce unbounded server-side delay.
A control that never recreates is inconclusive, not a pass.

## Service conditions

The experiment creates fresh API-managed queues with `tombstoneTtl` explicitly set to one hour.
It records their initial configuration and permits no purge, queue recreation, retention reduction or other administrator during any test envelope's lifetime.
Readback checks configuration; exclusive ownership and the recorded operation history supply the experiment's administrative-history prerequisite.
Current readback alone does not prove historical policy stability.

| Condition | Evidence adopted for this experiment |
| --- | --- |
| API-created queues, HTTP/App Engine, explicit deletion and execution | Real creation/readback/removal/replay controls, when executed. |
| Retry exhaustion | Controlled failing handler, observed failed attempt, disappearance and subsequent replay controls. |
| `taskTtl` automatic deletion | Published deletion/tombstone semantics and deterministic model; explicitly not measured. |
| Longer configured retention and queue.yaml/xml histories | Published scope and deployment prerequisites; no long-lived real-time experiment or extrapolated measured duration. |
| Purge, queue recreation, retention reduction, arbitrary late effects, concurrent forks and historical rollback | ADR-0158 exclusions; no positive support claim or mandatory exhaustive experiment. |

The owner chose representative service cases and excluded a ten-day wait on 2026-09-10.
No ten-day resource or reminder is created.
The [Queue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues) defines the one-hour minimum tombstone retention and ten-day minimum task lifetime.
Those distinct durations are not interchangeable.

## Resource authorization and execution

Implementation and local calibration do not authorize GCP execution.
Before any billable work, present the exact manifest and its SHA-256, resource names, fixture endpoint, operation/dispatch allowances, lifetime and cost estimate for separate approval.
The approved manifest is immutable; its hash is checked before opening service clients.
The execution token prevents accidental changes and is not itself evidence of user approval.

| Resource or allowance | Bound across all phases |
| --- | --- |
| Project/location | Existing test project, `us-central1` |
| Queue IDs | `flink-ct-r1245-<run-id>-{120,221,slow}-{http,ae}`; six queues total, two per serial phase |
| Task data | Four small generated records per recovery case; twelve original and twelve intentionally re-created slow-control tasks |
| CreateTask allowance | At most 5,000 calls, including direct-path records; relay phases reserve at most 1,500 each |
| Dispatch allowance | 1,000 attempts; recovery queues stay paused with tasks scheduled twelve hours ahead, slow queues use concurrency/rate 1, two removal phases of at most five minutes, and bounded retries for exhaustion |
| Billable operation allowance | 100,000 32-KB units; at most 30,000 reserved units per phase, with a separate remainder for direct-path creation, preflight, cleanup and fixture checks |
| Fixture | Existing `default/flink-e2e` App Engine B1, one running instance |
| Lifetime | Four hours from the recorded run start, including thirty minutes reserved for cleanup |
| Cost ceiling | USD 3 without free-tier credit |

At the published [Cloud Tasks rate](https://cloud.google.com/tasks/pricing), 100,000 units cost USD 0.04.
At the [App Engine B1 rate](https://cloud.google.com/appengine/pricing), four hours plus the fifteen-minute manual-scaling shutdown tail cost USD 0.2125 before network and other fixture charges.
The remaining budget covers those charges; no performance workload runs alongside this experiment.
ListTasks requests and reserves a whole ten-task page before each call because list billing counts returned tasks.
The first run used 100-task pages; the result below records why that reservation was reduced without increasing the operation allowance.

`CLOUDTASKS_RECOVERY_ACCEPTANCE=approved` enables only the manually selected acceptance classes.
It is a dedicated environment gate, intentionally outside `just e2e`'s known project gates.
Both real-service classes carry `gated`; the tombstone class also carries `slow`.
Neither ordinary builds, the weekly slow lane, nor normal E2E discover and run this real-service experiment.
The 48-case local model remains in the ordinary integration-test lane.
Keep the supervisor and prepared manifests outside the repository.
Build and freeze each version's reactor test classpath before activating the fixture, then invoke `CloudTasksAcceptanceLauncher` directly with that classpath; the paid run performs no Maven build.
Prepare clean, separate classpaths for each Flink major before starting the billed lifetime.
The supervisor must require exactly 56 successful recovery cases for each version and one successful slow test with twelve control-pass records, with no skips or missing cases.

Start the existing fixture through `scripts/appengine-e2e-fixture.sh run -- ...` and use its verified service/version/instance.
Before admitting tasks, verify `/revision` against the tracked handler source and `/accepted` and `/unavailable` against their expected responses.
An unexpected RPC outcome, changed queue state, oracle failure, exhausted budget, missing case, interrupted run or missed expiry observation stops admission and preserves partial evidence.
Do not add repetitions or extend the run to obtain a pass.

Delete only queues this run attempted to create after an absence check.
A CreateQueue `ALREADY_EXISTS` response revokes ownership and suppresses deletion, including a race after that check.
After terminating every job, downstream client and forwarding server, delete each owned queue and independently require GetQueue `NOT_FOUND`, with at most three cleanup attempts.
The fixture wrapper must then verify `STOPPED` and zero running instances.
The external supervisor must also retain exact-target cleanup instructions for termination that bypasses JVM teardown; a killed JVM is never proof of cleanup.
External cleanup requires a durable successful queue-creation record; an interrupted creation without that record remains unresolved and requires an ownership check before deletion.

Counterexamples inside the adopted contract require a design repair or an explicit scope decision through the parent.
Missing controls or unverified cleanup leave acceptance incomplete.

## Result on 2026-09-10

Before this run, the Flink 2.3.0 ceiling calibration at source commit `0c8793a4fa68b23cdbf9b37100ddc8ac4b304f30` passed all 53 selected tests: 48 recovery-harness cases, three generation-ledger tests, one authorization-boundary test and one recording-proxy test.
This local result predates the later harness repairs and supplies no GCP retention evidence.

Run `20260910a` received separate approval and executed source commit `0c8793a4fa68b23cdbf9b37100ddc8ac4b304f30` with Java 17.0.20 and the frozen Flink 1.20.4 classpath.
Its manifest SHA-256 is `930337b2f433720e717cabe35b76470d6e09cf4797832f5509e6ba13392d4675`.
The supervisor started at 02:30:51 UTC, verified the existing fixture's revision and responses, and opened the recovery phase at 02:33:49 UTC.
The first failure stopped admission; no Flink 2.2.1 or slow phase started.

| Flink 1.20.4 case family | Successful | Planned | Remaining disposition |
| --- | ---: | ---: | --- |
| Finished savepoint | 8 | 8 | Complete for this version |
| Terminal expiry and retained state | 8 | 8 | Complete for this version |
| Lost response | 8 | 8 | Complete for this version |
| Partial commit and rescale | 14 | 16 | One budget failure, one refused admission |
| Old incarnation | 0 | 8 | Refused admission after the failure |
| Direct production path | 0 | 8 | Refused admission after the failure |

JUnit discovered all 56 cases and reported 38 successes, 18 failures, zero skips and zero aborts.
The first failure was `Acceptance budget exhausted` in the ListTasks reservation for `partial-commit-rescale table-ae-stable 2-to-1`; the other 17 failures were the admission guard refusing further cases.
This is an incomplete experiment caused by the harness's page reservation, not 18 independent connector failures.

The raw journal contains 205 forwarded CreateTask calls: 132 successful service responses and 73 `ALREADY_EXISTS` responses.
Those successes identify 132 task names, each with one captured creation generation; no name has two different captured creation times.
It also records 53 pending-checkpoint absence observations and matching checkpoint-completion records, and eight suppressed responses.
These observations cover only the executed cases and establish no tombstone-expiry or old-incarnation result.

ListTasks made 292 calls and returned 336 tasks in total, with at most four tasks in any page.
The harness reserved 100 units before each list call, accounting for 29,200 of the final 29,998 reserved units, and another full page could not fit within the 30,000-unit phase allowance.
Reservations are upper bounds, not measured billing charges.
The repair uses one constant for both the ten-task request page size and its reservation, preserving pagination and the existing allowance.
Repricing the same completed operation sequence with ten-task pages gives 3,718 reserved units; all observed pages fit within that size.
That calculation validates the cause and the narrower reservation, but is not a successful rerun of the remaining cases.

Both created queues, `flink-ct-r1245-20260910a-120-http` and `flink-ct-r1245-20260910a-120-ae` in `flink-gcp/us-central1`, were deleted and independently observed absent by 02:40:10 UTC.
The external supervisor recorded no unresolved queues, and the fixture wrapper and final supervisor check both confirmed `default/flink-e2e` STOPPED with zero instances.
The other four queues were never created.
The partial JSONL journal, JUnit summary, fixture checks, manifest, lifecycle logs and per-case result remain retained outside the repository.
Their filename/checksum inventory SHA-256 is `0778e10925e5d31d3876692e7bfb0d116c2371d4e8a2fab4dfccfc05685164ef`.

No repetition was added to this run.
A further attempt requires a new immutable manifest, fresh queue names and separate approval; it must retain this failed attempt and cannot substitute its partial observations for missing controls.
Acceptance and release eligibility remain pending.

## Second result on 2026-09-10

Run `20260910b` received separate approval and executed source commit `46494a2d2e58837ea0fd5bfd9fa859c4dc2f988c` under manifest SHA-256 `d3e93a8ffa02fa6f5dfc795e8b327ec6216046bc05787665b302d7ad0cbc2834`.
The supervisor started at 09:27:20 UTC and verified the fixture before running the frozen Flink 1.20.4 classpath.
All 48 fault cases passed: eight finished-savepoint, eight expiry/retained-state, eight lost-response, sixteen partial-commit/rescale and eight old-incarnation cases.

The first direct-production-path case, DataStream HTTP with random names, timed out waiting thirty seconds for the checkpoint observation gate.
The recorded assertion did not retain the checkpoint future's outcome, so this run does not establish why that gate was never entered.
The other seven direct cases were refused admission; JUnit reported 48 successes, eight failures, zero skips and zero aborts out of 56 discovered cases.
Neither Flink 2.2.1 nor the slow controls started.

The relay journal records 256 CreateTask calls: 168 successful responses and 88 `ALREADY_EXISTS` responses.
Each of the 168 captured names has one creation generation, including the eight old-incarnation cases; no changed captured generation was observed.
There are 64 matching pending-checkpoint/completion observations and eight suppressed responses.
These counts exclude unobserved direct-client calls; the failed direct case produced no adopted creation/readback evidence.
ListTasks made 356 calls and returned 398 tasks in total, at most four per page.
The ten-task reservation kept the phase at 4,577 reserved units, below its unchanged 30,000-unit allowance.

A local regression deliberately holds a checkpoint participant in initialization while the job reports RUNNING.
Flink rejects an early checkpoint because not all required tasks are running; the former job-status-only helper returns prematurely and fails the regression.
The repaired helper waits until every execution vertex is RUNNING or FINISHED, and the same regression then completes a checkpoint after releasing initialization.
The checkpoint observation also waits on the checkpoint future alongside the gate, preserving an early rejection instead of hiding it behind the gate timeout.
This reproduces and repairs a harness readiness defect; it does not retroactively identify the unrecorded service-run checkpoint outcome or the cause of any initialization delay.
Future runs select the eight direct-path cases before the fault matrix to expose connection, retention-readback or initialization failures earlier, without removing any case.

Both owned queues, `flink-ct-r1245-20260910b-120-http` and `flink-ct-r1245-20260910b-120-ae`, were deleted and independently observed absent by 09:40:15 UTC.
The supervisor recorded no unresolved queues, and the fixture was verified STOPPED with zero instances.
The four queues for later phases were never created.
The retained raw-evidence filename/checksum inventory SHA-256 is `1e3f8c85104a2e5f1ccc439fe07cfeaa1701307ca5dc0644de7beffc55648f7a`.
No repetition was added; any later attempt requires fresh names, a new immutable manifest and separate approval.
The direct production paths, second Flink version and tombstone controls remain unaccepted, so this result does not complete service acceptance or authorize release.

## Third result on 2026-09-10

Run `20260910c` received separate approval and executed source commit `f158dc6cf593093e32758c3efab3dfd8d6f713dd` under manifest SHA-256 `dde0b4acee56fdb7342116db48a693a7b5dc742d503e5c84d0c9e384f199387e`.
The supervisor started at 11:32:33 UTC, verified the fixture, and ran the frozen Java 17.0.20 classpaths serially.
Both recovery phases passed every registered case, with no failures, skips or aborts.
The direct production-path cases ran first on each version.

| Case family | Flink 1.20.4 passed | Flink 2.2.1 passed |
| --- | ---: | ---: |
| Direct production path | 8 | 8 |
| Finished savepoint | 8 | 8 |
| Terminal expiry and retained state | 8 | 8 |
| Lost response | 8 | 8 |
| Partial commit and rescale | 16 | 16 |
| Old incarnation | 8 | 8 |
| Total | 56 | 56 |

The Flink 1.20.4 relay recorded 251 CreateTask calls: 168 successful responses and 83 `ALREADY_EXISTS` responses.
The Flink 2.2.1 relay recorded 253 calls: 168 successful responses and 85 `ALREADY_EXISTS` responses.
Each phase captured 168 distinct names with one creation generation per name, 72 matching pending-checkpoint/completion observations and eight suppressed responses.
Each also recorded 32 distinct direct-path task names through readback; direct-client calls are excluded from the relay counts.
ListTasks made 401 calls per phase, returning 427 and 426 tasks respectively, with at most four tasks per page.
The phases reserved 5,152 and 5,158 operation units respectively, below their unchanged 30,000-unit allowances.
These are adopted recovery observations for the executed matrix; they do not substitute for the remaining tombstone controls.

The slow phase created and observed all twelve original task generations, then observed their removal across HTTP/App Engine, deletion/successful execution/retry exhaustion, and current/old-client arms.
An independent decode of the retained protobufs confirmed each original generation through both GetTask and ListTasks before removal.
The six old-client originals also have recorded unavailable-response observations.
All twelve names returned `ALREADY_EXISTS` at the first post-removal observation, after both removal phases had completed, and again between 12:47:52 and 12:48:02 UTC at the registered second positive observation.
The saved queue readbacks reported `tombstoneTtl = 3600s`.

The negative phase recorded 1,260 further `ALREADY_EXISTS` outcomes from 12:59:16 through 13:58:57 UTC: 105 attempts for each of the twelve names.
No second creation response or generation was observed, and no control-pass record was emitted.
At the observation limit, the slow test failed with `Actual tombstone expiry not observed within the registered budget`; JUnit reported one discovered test, one failure, zero successes, zero skips and zero aborts.
The slow phase made 1,296 outgoing CreateTask calls in total and reserved 1,632 operation units.
It was not extended or repeated.
This result establishes neither when name reuse becomes possible nor why retention continued beyond the configured hour.
It is an inconclusive negative control, not evidence of a second physical creation or a violation of the adopted recovery contract.

All six owned queues, `flink-ct-r1245-20260910c-{120,221,slow}-{http,ae}` in `flink-gcp/us-central1`, were deleted and independently observed absent.
The recovery phases finished cleanup at 11:47:11 and 11:57:28 UTC; the slow phase finished at 13:59:32 UTC.
The supervisor recorded no unresolved queues, and both fixture checks confirmed `default/flink-e2e` stopped with zero instances.
The eighteen retained raw-evidence files have filename/checksum inventory SHA-256 `e62e1aa290012c966085e024e27e0920c41efa0d3cbc7ef260d4c828cc39c94d`.
The inventory was verified after teardown; the two earlier partial runs remain retained unchanged.

The recovery matrix and positive retention observations now have real-service evidence on the registered configurations.
Under the original preregistration, the missing post-tombstone recreation controls left issue [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) incomplete.
A further billable observation requires a new plan, immutable manifest, fresh queue names and separate approval; this run supplies no authorization for it.
The ten-day task-lifetime observation remains excluded, and performance acceptance remains [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).

## Acceptance clarification on 2026-09-10

After reviewing the result, the owner clarified that observing eventual ID reuse is not a prerequisite for accepting exactly-once recovery of the same logical task.
This amends the acceptance experiment in [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) and the corresponding control requirement in its parent [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238).
It does not change ADR-0158's production protocol, supported scope, recovery deadlines or service prerequisites.

The required property is that authorized replay preserves the original identity and does not cause a second physical creation, with expired envelopes stopped before a new send.
For that same logical task, longer name protection does not violate this property.
Protection disappearing before the assumed safe window ends would threaten it and would still require a repair or an explicit scope decision.
Neither a configured duration nor these finite observations prove that every service execution follows the published specification.
The accepted guarantee remains conditional on ADR-0158's service assumptions and exclusions, including its treatment of unbounded late effects.

The one-hour setting was not a sound upper bound for planning when reuse would become observable.
The [v2 creation reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create) and [system limits](https://docs.cloud.google.com/tasks/docs/quotas) describe name reuse as taking up to 24 hours, while the [v2beta3 Queue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues) describes the configured tombstone duration and a one-hour default.
The saved readbacks and observed collisions do not resolve that discrepancy or identify the eventual reuse time.
No measured longer retention is used to extend the connector's authorization window.

Actual service-side recreation is an optional additional calibration of the generation observer.
`TaskCreationLedgerTest` checks that a changed creation time is detected, both generations require live readbacks, and an unrecorded old-client generation is rejected.
`CloudTasksRecoveryModelTest` checks the production authorization boundary with a controlled clock, and deliberately bypasses it to demonstrate a second generation in the assumed model.
Those tests validate detection and boundary logic; they do not establish actual GCP expiry behavior.
The real runs separately validate original creation capture and GetTask/ListTasks readbacks, including response loss and old-client isolation.
The unexercised real-service second-generation path remains an explicit evidence limit, not a passing negative control.

With that additional calibration optional, the two complete 56-case recovery matrices, twelve removal controls, 24 positive observations, deterministic boundary/oracle tests and verified cleanup satisfy this issue's adopted correctness acceptance.
The slow JUnit result remains failed under its original registered criterion; no case is relabelled, skipped or rerun to obtain a pass.
The original manifests, logs, checksums, test assertions and launcher remain unchanged.
Release still requires [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246)'s performance assessment.

A next-day observation could be planned separately if it later has a concrete purpose; a day is not a measured reuse deadline from this run.
It would require fresh resources and its own operation, lifetime, cost and approval plan.
No next-day probe or reminder is scheduled, and no further GCP resources are authorized by this clarification.
