# Pub/Sub Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the DataStream relay of [issue #1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
It relays two pre-created subscriptions through the production Pub/Sub source and publisher sink into a pre-created output topic.
Its Table entry point, which no trial ever ran, was removed with the reviewed trial files ([#2240](https://github.com/flink-gcp/flink-connector-gcp/issues/2240)).
It is an unbounded streaming job over a bounded logical input domain: receiving every expected ID does not stop the job, Pods or Operator.
An independently supervised lifecycle must bound execution and perform cleanup before deployment is admitted.
Its `sequence` kind, the only one dispatched, injects every fault in one run under ADR-0204 ([#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151)); the single-fault trials before it ran under ADR-0165.

## Build and local validation

```sh
mise x -- just tier3-pubsub-verify
```

The opt-in `tier3-pubsub` Maven profile keeps the application outside published connector artifacts.
The build produces `target/pubsub-recovery.jar` and copies runtime dependencies with their original licenses/notices into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `pubsub-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base.
The application CI lane builds the Dockerfile without publishing; its context admits only the Dockerfile, application JAR and packaged runtime dependency JARs.
The [first publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116) built main commit `cba9043faee0ceb067cba23b56fe3e0abe7f0542`; a GAR read confirmed its application digest.
The reusable deployment definition below consumes a supplied digest; it does not select or admit an active run.

Unit tests cover argument and input bounds, serialization, missing/foreign restored state, contradictory evidence, missing IDs and duplicate classification.
They also pin the relay's operator UIDs.
MiniCluster tests run the relay with production source/sink RPCs against the Pub/Sub emulator, replace TaskManagers after a completed checkpoint and restore savepoints at parallelism 1→2 and 2→1.
They also check that the sink never creates a missing output topic.
They publish new input to both subscriptions after the disruption and require output from restored attempts.
These are local wiring and state-continuity checks; they do not measure real-service redelivery, ACK deadlines, lease expiry, ordering, GCS access, GKE scheduling or Operator reconciliation.

## Run identity and service resources

The fixed project is `flink-gcp`.
Input index `i` is 0 or 1; its topic and subscription both have ID `t3-<run-id>-in-<i>`.
The output topic and independent observer subscription both have ID `t3-<run-id>-out`.
All three subscriptions must exist before any corresponding publication, including the output observer before starting the relay.
The job uses ADC, provides no key-file option, never creates a subscription and sets the sink to `CREATE_NEVER`.
The emulator endpoint is a package-private test seam, absent from the deployed CLI.

## Payload and restoration

Each UTF-8 input payload is `v1|<run-id>|<input-index>|<sequence>` with canonical decimal numbers and sequence in `[0, records-per-subscription)`.
The relay applies one input check, which rejects foreign runs, wrong subscriptions, invalid UTF-8, payloads over 128 bytes and absent service message IDs.
It does not discard or deduplicate replayed input.
The logical count is not a cap on service deliveries, retries, bytes billed, output publications or elapsed time.
Those require separately approved execution ceilings and an independent stop path.

The source, observer and sink have fixed UIDs `pubsub-input-v1`, `pubsub-observer-v1` and `pubsub-output-v1`.
The `main` method fixes maximum parallelism at 128 and enables checkpoints every 120 seconds with at most one in flight; the CUE configuration sets the same interval and tolerates two consecutive failed checkpoints before the job restarts.
The interval dates from the single-fault exercise of ADR-0165, which had to publish, process and observe a cohort between two checkpoints while the supervisor and the runner each acted once per 15-second poll.

The observer keeps version/project/run ID/input-domain identity in union operator state, accepting rescaling but refusing missing or incompatible restored identity.
Only phase, parallelism and the restoration requirement may change between phases of the same run.
The source's enumerator owns subscription reassignment; its readers checkpoint neither payloads nor split ownership, so unacknowledged delivery is recovered through the service.
The publisher flushes on checkpoints and remains at-least-once.

Each output payload contains thirteen pipe-delimited fields:
`v1|<run-id>|<input-index>|<sequence>|<base64url-input-message-id>|<attempt-uuid>|<observation-uuid>|<phase>|<restored>|<origin>|<epoch>|<subtask>|<attempt-number>`.
`origin` is the checkpoint the attempt restored, or 0 when it restored nothing, and `epoch` is the latest checkpoint whose barrier the attempt had passed when it processed the input, or `origin` before the first.
Checkpoint barriers are aligned, so an input whose epoch is below a checkpoint's id reached the observer ahead of that checkpoint's barrier, which means the source emitted it before the barrier and the checkpoint covers it.
`subtask` and `attempt-number` are the subtask's index and Flink's attempt number for it, which grows across the restarts one JobManager makes; ordered by origin and then attempt number, the same subtask's next attempt is the restart that ended an attempt's run, and an attempt a new JobManager numbers from zero again still sorts after the old ones, because its origin is higher.
An earlier processing counts as covered for a later restored attempt when its attempt's origin is below the later attempt's and its epoch is below the end of its own run, the next attempt's origin: an input processed after the barrier of a checkpoint that a later run restored was covered by no checkpoint the later attempt descends from, and two restores of one checkpoint are two runs.
A run also ends at the next origin above its own, because a whole-job restart, such as a JobManager failover's, restarts every subtask; that bounds a subtask a scale-in removed and a later restore brought back, whose next attempt comes several restores later.
Neither bound sees a restart that restores the same checkpoint again and removes a subtask: the removed subtask's run then reads as lasting until the next restore that brings it back, so an input it was processing can be counted as covered although no checkpoint covered it.
Nothing a task can read names the job-wide run, so the count is an upper bound when the job restored one checkpoint twice.
A region failover that leaves some subtasks running reads their runs as ending at it, so the count can miss coverage there, and an attempt that wrote no output is unseen, so the run it ended reads as longer.
Base64url encoding is UTF-8 without padding.
The attempt UUID is generated during each observer initialization; the observation UUID is new for every processing call, including repeated input.
Neither is a deterministic deduplication key for the logical input.
The app logs `pubsub-attempt`, `pubsub-snapshot` and `pubsub-checkpoint-complete` with run, phase and attempt identity; checkpoint events also name the checkpoint ID.
It also logs `pubsub-progress` with the attempt's restored checkpoint and how many inputs it has processed, at its first input and then at least every ten seconds while it processes, which the [fault sequence](#fault-sequence) reads as the job's progress.
A snapshot is not completion, and the observer's completion callback does not prove that the Pub/Sub service has accepted an ACK.
Abrupt failure can lose logs, so the deployed lifecycle must export observations outside the task JVM and correlate them with Flink checkpoint history, faults and service ACK/lease observations.

## Arguments

| Argument | Contract |
| --- | --- |
| `--run-id` | Required Tier-3 label of at most 40 lowercase letters/digits/hyphens, beginning and ending with a letter or digit |
| `--records-per-subscription` | Logical sequence domain per input, 1–10,000; defaults to 1,000 |
| `--parallelism` | 1 or 2; defaults to 1 |
| `--phase` | `initial` by default, or `upgrade` |
| `--require-restored` | `false` by default; `upgrade` requires `true` |
| `--restore-deduplication` | `false` by default, or `true`; sets the source's restore deduplication |
| `--record-delay-millis` | 0 to 1,000; defaults to 0. The observer waits this long before each input, which paces what the subscriptions already hold: the `sequence` kind sets 100, ten inputs a second per subtask |

Arguments use `--name=value`; unknown or repeated arguments fail.

## Deployment definition

[`pkg/pubsub.#Application`](../../pkg/pubsub/application.cue) defines the relay on Flink 2.2.1 / Java 17.
A delivery below `runs/` supplies `run.id`, `run.expiresAt`, `run.namespace: "tier3-pubsub"` and the application's `id`, `image`, `phase`, `recordsPerSubscription` and `parallelism` inputs.
The lifecycle delivery also supplies `restoreDeduplication` and `recordDelayMillis`; the source runs at the connector's default flow control.
Unless a delivery names them, the package takes the fault sequence's input and relay settings, 4,800 records per subscription with restore deduplication on and 100 milliseconds before each record, and every delivery names the parallelism.
The [synthetic fixture](../../tests/fixtures/pubsub.cue) demonstrates that binding; tests supply disposable values and render every initial/upgrade and parallelism 1/2 combination, and the lifecycle tests render the `sequence` kind.
The lifecycle CLI renders an [offline trial proposal](#offline-trial-proposal), and [dispatch](#approval-dispatch) runs the kind it names through the runner's [admission](#admission-and-effective-access) and the supervisor's [fault sequence](#fault-sequence).

The first verified image reference is:

```text
us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:d56245c72fb69d3b1a68b05319cd4ecbdf74a3080995433ed9d5fe12d509e938
```

This records publication, not current availability.
GAR versions become deletion-eligible seven days after creation; admission must verify existence and retention through the run and cleanup window.
The package requires this GAR package with a lowercase SHA-256 digest and rejects tags and other application packages.

The deployment uses the `tier3-pubsub/pubsub` KSA and native workload ADC.
It retains checkpoints on cancellation and separates checkpoints, savepoints and Kubernetes HA data beneath `gs://flink-gcp-tier3-pubsub/runs/<run-id>/`.
Objects become deletion-eligible after one day, with soft delete and versioning disabled.
Complete recovery trials within that storage lifetime; the state bucket is not durable evidence storage.
One JobManager and one or two single-slot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage on AMD64 nodes; the TaskManagers run on Spot and the JobManager on normal capacity.
At parallelism two, the application alone therefore uses three Pods; later admission must additionally budget the Operator and independent supervisor.
Namespace quotas remain idle until a separately approved lifecycle admits the complete workload.

Savepoint upgrades preserve state and refuse unclaimed state.
The definition disables last-state fallback, so an unavailable savepoint cannot be replaced by last-state recovery, and sets `kubernetes.operator.snapshot.resource.enabled=false`, so the Operator reports the upgrade savepoint in the application's status rather than in a separate FlinkStateSnapshot, as the BigQuery application does.
The rendered job arguments use the application's `--name=value` syntax, match Flink parallelism and require restored identity in the `upgrade` phase.
That phase does not locate a savepoint: the Operator restores the one its upgrade takes, and the supervisor's [fault sequence](#fault-sequence) waits until the job runs again after it.
The restart strategy is unbounded, so a run waits out any number of restarts (ADR-0204).
The logical input domain does not bound elapsed execution or billable service operations.

Before deployment, integrate the owned-resource operations described below with scoped service grants, independent stop/cleanup supervision, concrete execution limits and external fault/evidence collection.
The deployed recovery trials of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) ran one of each kind and lost no message; their [findings](../../../docs/adr/evidence/0165-pubsub-trial-findings-1361.md) record the verdicts, every attempt and what they leave unobserved; the replacement reruns of [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720) observed the redelivery those trials did not, as their [findings](../../../docs/adr/evidence/0165-pubsub-redelivery-findings-1720.md) record.
The removed [restore-deduplication trials](#restore-deduplication-trials) of [#2127](https://github.com/flink-gcp/flink-connector-gcp/issues/2127) each restored a checkpoint whose completion notification had not run, and the source suppressed as many redeliveries as its ledger held message ids, as their [findings](../../../docs/adr/evidence/0198-pubsub-restore-deduplication-trial-findings-2127.md) record.
The offline manifest check is `mise x -- just tier3-check`; it does not create resources or establish service settings.

## Recovery trial

The `pubsub-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs the relay once as a FlinkDeployment in `tier3-pubsub` between the three topics and three subscriptions it creates for the run, injects every fault kind in one run while the relay's input and output flow, and judges the run by its output.
The runner and the supervisor prepare, admit and clean up the run's resources, and the verdict is recomputed offline from the evidence.

### Fault sequence

[`PubSubExercise`](../../../tools/tier3/src/flink_tier3/pubsub/exercise.py) runs the `sequence` kind under ADR-0204: the shared fault sequence of [`flink_tier3/exercise.py`](../../../tools/tier3/src/flink_tier3/exercise.py), with the relay's input, progress and completion.

Admission publishes `before_checkpoint`, and on each poll the exercise asks the runner for the next [cohort](#cohort-requests) once the one before it is out, so the subscriptions hold the whole input within a few polls of the job starting.
The relay reads it at 100 ms per input per subtask, ten inputs a second per subscription at parallelism two and five at one.
The input is 4,800 records per subscription, 480 seconds of input at parallelism two, so it outlasts the faults.
The earlier sequence runs began their input 477 to 677 seconds into the window, the faults delayed it by 245 to 292 seconds, and it had advanced by 100 to 134 seconds when the last fault came, which puts the verdict at about 20.5 to 25.6 minutes.
The first `sequence` run, `ps2151-seq-a1`, began its input 605 seconds into the window, finished it 406 seconds later than its length, and reached `usable` 24.9 minutes into its window, as its [findings](../../../docs/adr/evidence/0204-pubsub-fault-sequence-findings-2151.md) record.

On each poll the exercise first pulls the output subscription, at most 20 batches, through the [output collector](#output-collector).
The relay logs `event=pubsub-progress` with its attempt, whether it restored, the checkpoint it restored and how many inputs it processed, at its first input and then at least every ten seconds while it processes.
These lines are the job's progress: the job runs again after a fault once an attempt the fault's restore started logs one, with the other conditions ADR-0204 names.
A TaskManager's loss restores only the failover region on that Pod, so a line from the attempt on the other TaskManager, which never stopped, does not count.
Once the job runs with input flowing and a checkpoint has completed after its first progress, the exercise injects the five faults in turn: the savepoint upgrade, the JobManager's deletion, the deletion of the TaskManager whose attempt logged the latest progress, and a rescale to one and back to two.
Each savepoint fault patches the approved upgrade manifest, changing only `--parallelism=`, `job.parallelism` and the TaskManager replicas.
Everything else is waited out and recorded.

The source does not end, so the job never finishes, and ADR-0204 reads its completion another way.
After the last fault the job runs again, and then either every record of both subscriptions is on the output subscription, or 600 seconds, Pub/Sub's longest acknowledgement deadline, have passed since the later of the job's latest restore and its latest progress line, and a checkpoint triggered after them has completed.
By then the paced input is through, and every lease the stopped attempts held has lapsed, since the client library caps each extension at 600 seconds; a redelivery the relay processes logs progress and moves the wait on, so a record still missing is lost.
The record names which completion it reached as `completion`, `all-observed` or `redelivery-horizon`.
The output ends the wait early only when nothing is missing, so it never turns a loss into an unfinished run; a loss is expected to reach its verdict 31 to 38 minutes into the window, under the one-hour limit.
The run stops unconfirmed, and can run again as it is, when the job could not be seen running again after a fault because the input was through: every record is on the output before the last fault has been injected and waited out, or the job runs again after a fault in every other respect but logs no progress for 600 seconds after its restore, having nothing left to process; only a poll whose checkpoint statistics answered declares it idle, since a failed read may hide a later restore.

A pull that the service refuses with 408 or 429 leaves no request in flight, as the session counts it, and saved nothing but its intent, so the handoff clears its marker and the next poll pulls again; a `pubsub-pull-retried` event names its batch, and the record lists it under `retried_pulls`.
What the service had leased to it comes back as a duplicate, and a refusal in the round before the verdict defers the verdict to the next poll.
A 5xx answer may still complete at the service, which the session records as unsettled for good, so it stops the run, and so does a pull without an answer, an evidence write that failed, or an acknowledgement that failed after its pull was saved.

#### Restore-deduplication trials

The three AdaptiveScheduler kinds of [#2054](https://github.com/flink-gcp/flink-connector-gcp/issues/2054), `reactive-scale-out`, `reactive-tm-kill` and `adaptive-scale-in`, produced on purpose the case that restore deduplication exists for: a restore of a checkpoint whose completion notification never ran.
Their findings, of [#2056](../../../docs/adr/evidence/0198-pubsub-reactive-restore-deduplication-findings-2056.md) and [#2127](../../../docs/adr/evidence/0198-pubsub-restore-deduplication-trial-findings-2127.md), record the source suppressing what that case redelivers.
[#2240](https://github.com/flink-gcp/flink-connector-gcp/issues/2240) removed them, with their reactive and adaptive manifests and the relay's checkpoint hold.
They could not join the fault sequence, which runs in native mode and rescales through savepoint upgrades, and proving that their case happened is the per-fault proof ADR-0204 declined.
`PubSubSourceRestoreDeduplicationITCase` keeps the case on the emulator, and the `sequence` kind runs with restore deduplication on, so every sequence run checks that the ledger suppresses nothing that the restored checkpoint did not cover.
Their evidence keeps the interpretation of the revision that produced it.

### Offline trial proposal

Render one unapproved trial of a kind with the existing application and lifecycle delivery packages:

```bash
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario pubsub-recovery \
  --run-id proposal-1361 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-09-21T00:00:00Z --expires-at 2026-09-21T01:30:00Z \
  --active-seconds 5220 \
  --revision bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --pubsub-trial sequence > /tmp/pubsub-proposal.json
```

Run from the repository root, or set the CLI's global `--repository` argument before `render`.
The example uses synthetic revision/image identities and dates; rendering performs no credential lookup, image availability check or cloud operation.
The output contains `proposal`, `application`, `recovery_application` and `delivery` (ConfigMap and supervisor Job).
The proposal records the supplied source revision, exact application/supervisor hashes, the application and supervisor images, service settings and grants.
It does not prove that the installed package, checkout or application image came from the supplied revision.
`approved` is always false and the ConfigMap's `approval.json` remains empty; neither output is an execution approval.

The trial's kind, `sequence`, starts both manifests at parallelism two and runs every fault of ADR-0204 in turn, as the [fault sequence](#fault-sequence) describes.
Its recovery manifest, `recovery_application`, which the delivery also stores under the shared key `upgrade-application.json`, is its savepoint upgrade at two and requires restored state; the exercise patches its rescales from it.
The kind fixes the rest of the run: `pubsub_plan.trial` derives its 4,800 records per subscription, and the plan renders restore deduplication on and 100 ms per input into both manifests.
Each plan covers two input subscriptions.
Each subscription's domain splits into three disjoint, nonempty cohorts at `floor(records / 3)` and `floor(2 * records / 3)`, published in this order:

| Cohort | Published |
| --- | --- |
| `before_checkpoint` | At admission, before the application exists |
| `after_checkpoint` | When the supervisor asks, as soon as `before_checkpoint` is out |
| `after_recovery` | When the supervisor asks, as soon as `after_checkpoint` is out |

Each range is published in ascending batches of at most 1,000, the publish API's per-request maximum, without crossing the range boundary: the first by admission, the others through the [cohort requests](#cohort-requests) below.
The output records exact logical messages, payload bytes and required publication calls.
The cohorts' names come from the single-fault exercise, which published `after_checkpoint` after a retained checkpoint and `after_recovery` after a recovery; the sequence only spreads the input over three publications.

The trial, which the approval keeps as `pubsub_trial`, has exactly three fields: `version` 8, `trial` and `records_per_subscription`.
Version 8 removed `restore_deduplication`, which version 6 had added: every run sets the source's `restoreDeduplication` ([#2049](https://github.com/flink-gcp/flink-connector-gcp/issues/2049)) in both manifests, and the [offline recomputation](#offline-recomputation) reads a version-7 approval, which names it, as the same `sequence` run.
Version 7 removed `entry_point`, with the Table entry point.
Version 5 removed version 4's `traffic_limits`: no runtime ceiling meters the trial's messages, calls, requests or evidence bytes.
Those requests are nearly free, and the trial's cost is bounded by its input domain, its window and its Pod quota, as [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records.

The proposal fixes a 90-minute window with the last 15 minutes reserved for cleanup and a 5,220-second supervisor Job deadline.
Admission's temporary quotas hold seven Pods: three steady Flink Pods plus one Flink replacement allowance in `tier3-pubsub`, and the Operator, supervisor and one Operator replacement allowance in `tier3-system`; PVCs are zero.
Terminating Pods can overlap their replacements and remain [charged to namespace quota](https://kubernetes.io/docs/concepts/policy/resource-quotas/#quota-on-object-count) until their phase is terminal, which is what the allowances are for.
Each Flink Pod uses the existing one-vCPU, 2-GiB shape and the shared AMD64 constraint; only the TaskManagers select Spot.
A runnable approval also needs live image/provenance checks, stop enforcement, effective-access checks and independent cleanup supervision.
A passed deadline, evidence failure, lost ownership, uncertain actor quiescence and expiry must stop a later trial rather than produce a success verdict.
The [fault sequence](#fault-sequence) injects the faults, orchestrates the savepoints and decides the trial's [verdict](#verdict-and-analysis), which the [offline analysis](#offline-recomputation) recomputes from the exported evidence.

### Approval dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) accepts `pubsub-recovery`, builds its approval and runs the trial.
The scenario takes two inputs beyond the common ones.

| Input | Contract |
| --- | --- |
| `pubsub_trial` | The trial kind, `sequence`; `none`, the default, is refused |
| `application_digest` | The published `pubsub-recovery` GAR digest, verified live at dispatch and never pinned |

The CLI takes the kind as `--pubsub-trial`, checks it against the plan's kinds, which the workflow's choices mirror and a test holds to them, and builds the [trial](#offline-trial-proposal) the kind fixes; the offline renderer takes the same flag.
A `rig_sha` that predates [#2240](https://github.com/flink-gcp/flink-connector-gcp/issues/2240) reads the flag as the name of a reviewed trial file: it refuses `sequence` while it resolves the trial, before the lock or any resource.
Dispatching is the approval, and no estimate is approved first (ADR-0204).

The window starts when dispatch admits the run, on the whole second, and lasts exactly 90 minutes, so queueing before the job starts costs the run none of its startup budget; the last 15 minutes are cleanup's, which leaves a `sequence` run its full hour from admission to its verdict (ADR-0204), as in the other scenarios.

The checks that need neither the cluster nor the lock run first: the trial kind and digest, the dispatch from `main`, the rig commit (the dispatched `main` commit unless `rig_sha` names another), the run ID and an existing run's evidence.
Dispatch then snapshots the idle foundation for `tier3-pubsub`, renders and verifies the proposal, takes live image receipts, assembles and validates the approval and binds the rendered delivery with the approval embedded.
The proposal names its second manifest the recovery application; the approval pins that manifest as `upgrade_application_sha256`, the name every service scenario shares, and takes the supervisor image from the proposal's `images`, as a BigQuery approval does.

Dispatch also confirms that it may create Pods in `tier3-pubsub`, which admission's access probe needs.
It then acquires the environment lock, rechecks the idle foundation, stores the approval, both manifests and the image receipts as the run's documents, writes the control record and builds the runner through `pubsub_actors.runner()`, as a BigQuery dispatch does.
The runner starts the run with the bound ConfigMap, supervisor Job, application and access probe, and settles it; a runner that cannot be built leaves the lock to a plain runner's settlement.
The workflow job's limit is 120 minutes, as for the other recovery scenarios, because the window is 90 minutes.
Enabling the scenario does not approve a run.

#### Internal approval contract

[Dispatch](#approval-dispatch) builds a `pubsub-recovery` approval and validates it once, before it takes the lock.
Its `pubsub_trial` is the trial its kind names, in the [offline proposal schema](#offline-trial-proposal), version 8, from which the input plan derives the trial's input domain.
The approval pins the run/nonce, the rig commit, initial and recovery manifest hashes, exactly the Operator, lifecycle-tools and Pub/Sub application image roles, and observed idle namespace/quota UIDs for `tier3-pubsub` and `tier3-system`.
It requires an integral 90-minute window with fifteen minutes reserved for cleanup.
Nothing validates it again: admission checks only its window, and recovery reads it as written, so a later policy change cannot strand a held run's recovery.
These checks do not verify live provenance or establish a recovery verdict.

Admission's temporary quotas admit four equal Flink shapes in `tier3-pubsub` and three control shapes in `tier3-system`, and no PVCs.
The fourth application slot covers a replacement while its predecessor terminates; the third control slot covers an Operator replacement.
The rig counts neither Pods nor state bytes beside those quotas; cleanup lists at most 10,000 state objects and restores the recorded idle quotas.
Message evidence has no separate cap; its size follows from the input domain and the exercise's pulls.

`Approval.pubsub_plan` and `PubSubLifecycle` derive service names and grants from the approval's identity.
Final settlement preserves `pubsub_trial` and the recovery observations even before service intent exists, and derives success only from the exercise's `usable` [verdict](#verdict-and-analysis).
A finalization retry keeps the original receipt.

The runner admits the trial and the supervisor joins it, as [admission](#admission-and-effective-access) describes, the supervisor runs the [fault sequence](#fault-sequence), and [dispatch](#approval-dispatch) starts it.

### Resource lifecycle

The runner creates the six service resources and installs their grants during admission, before it creates the application, and only the supervisor deletes them, once both actors have released and the namespace barrier holds.
The subsections below give the resource operations, the grants, the durable record that ties them to the run, the two actors and their request guard, admission, and the handoff that gates cleanup.

#### Owned resource operations

[`flink_tier3.pubsub.resources`](../../../tools/tier3/src/flink_tier3/pubsub/resources.py) supplies internal `ResourcePlan` and `Resources` helpers for the internal durable lifecycle controller.
These helpers are not a CLI scenario and do not admit a workload.
A plan requires a fresh run ID and a caller-generated ownership nonce of 32 lowercase hexadecimal digits.
The caller supplies the shared authorized HTTP session, GCS `Storage` adapter and a mandatory `before_operation(phase, method, name)` guard; construction performs no I/O.
Use the shared evidence bucket for the control record, as with the other lifecycle records.

`provision()` refuses an existing control record or any of the six existing service names, including resources with matching labels.
It then writes the version 2 plan, including the fixed data-role bindings described below, to `_control/pubsub/<run-id>.json` with GCS generation-match zero before creating the three topics followed by the three subscriptions.
Every service resource carries `tier3-run` and `tier3-nonce` labels.
The helper re-reads the matching manifest before every service write and validates all six service resources after creation; `inspect()` repeats that readback validation.
An ambiguous create stops without retrying or adopting the resource; the retained manifest permits cleanup of matching partial creation.
Re-running `provision()` against that manifest is refused.
The retained intent lives outside `_control/runs/`, whose contents block new shared environment locks.
The durable controller below maintains separate active run control before provisioning; the retained Pub/Sub manifest is not that active-run record.

| Service setting | Required value |
| --- | --- |
| Topic persistence region | `us-central1`, without in-transit region enforcement |
| Topic retention, schema, KMS key, ingestion or message transforms | Unset |
| Subscription delivery | Pull, with no filter, dead-letter policy, retry policy or transforms |
| Acknowledgement deadline | 30 seconds |
| Unacknowledged message retention | 1 day |
| Subscription inactivity expiration | 1 day |
| Retain acknowledged messages, ordering, exactly-once delivery | Disabled |

The service may omit false scalar fields and return integral durations with fractional zeros; readback accepts those representations.
A subscription reads back `state: ACTIVE`, which readback requires; a topic without an ingestion source reads back no `state` at all, as measured on 2026-10-10, so a topic's state is checked only when present.
The [subscription API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions) defines these settings; an unset retry policy uses immediate redelivery, and inactivity expiration is not a run deadline.
Subscriptions can expire after one day without subscriber activity; provision within the approved admission window and revalidate presence and settings immediately before admitting the workload.
The [topic API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics) defines the persistence policy and optional topic features.
The relay makes an at-least-once contract; the connector rejects service-side exactly-once-delivery subscriptions.

`cleanup()` requires the matching manifest, verifies resource labels and subscription topic bindings, and deletes subscriptions before topics.
For cleanup only, it also accepts the service's `_deleted-topic_` marker on an owned subscription, so a topic deleted earlier does not prevent removing that subscription; a different live topic binding still refuses deletion.
Inspection requires the expected live binding and identifies absent resources by name.
It confirms absence after each deletion and retains the control record for repeated cleanup and reconciliation.
Settings drift does not erase ownership, but missing or inconsistent recorded identity refuses deletion.
Failures stop that call, including stopping before topic deletion when subscription cleanup fails.
No listing, prefix deletion, same-name adoption or Pub/Sub request retry is performed.

Pub/Sub resource names can be reused after deletion, and the [subscription](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/delete) and [topic](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/delete) delete APIs accept no generation precondition.
Names and ownership labels therefore do not make deletion atomic with replacement.
The caller's shared environment lock and exclusive control of run resources must cover provisioning through cleanup, including each read/delete gap; unexpected replacement is an incident to reconcile.
The guard runs before each Pub/Sub request or logical storage adapter call; the production guard holds each Pub/Sub request to its deadline, and the caller keeps exclusive ownership.
For a control-record `GET`, the shared adapter reads metadata and then generation-matched data, restarting after a jittered wait on a generation race, in at most twenty attempts: up to forty GCS data requests for that single callback.
A control-record `PUT` callback denotes the logical create upload, whose GCS HTTP method is `POST`.
Callback counts are not HTTP-request counts.
The helper delegates those checks to the caller; it does not implement the lock, deadlines or independent supervision.
Pub/Sub HTTP requests use the shared 20-second timeout with redirects disabled and no automatic retry; this is a per-request transport limit, not a total elapsed-time or cost ceiling.
Responses are read streamed and refused once their body passes 1 MiB, the same local cap the message helper applies; a body that fails while arriving is a transport failure.
The actor sessions treat no response as a redirect, because requests otherwise reads a redirect's whole body before any cap, even when it does not follow it; a 3xx to a resource, message or identity request therefore returns unread and is refused as a non-success status.
Credential refresh goes through google-auth's own request, which does not stream, so a token endpoint's response body is still read whole.

#### Permissions and remaining integration

The [persistent IAM foundation](../../../opentofu/README.md#pubsub-lifecycle-authority) defines project-wide resource control for the runner and metadata/deletion authority for the supervisor.
The runner can change policies on any topic or subscription in the project, including granting itself data access; the runtime's ownership checks are not an IAM restriction on that identity.
The workload GSA receives no project-level Pub/Sub grant.
The custom `projects/flink-gcp/roles/tier3PubSubConsumer` role contains only `pubsub.subscriptions.get` and `pubsub.subscriptions.consume` and is defined without a project binding.

The version 2 ownership manifest freezes these explicit resource policies before creation:

| Owned resource | Member at `flink-gcp.iam.gserviceaccount.com` | Role |
| --- | --- | --- |
| Both input topics | `tier3-runner` | `roles/pubsub.publisher` |
| Both input subscriptions | `tier3-pubsub` | `tier3PubSubConsumer` |
| Output topic | `tier3-pubsub` | `roles/pubsub.publisher` |
| Output subscription | `tier3-supervisor` | `tier3PubSubConsumer` |

`install_grants()` validates all resources and requires all six explicit policies to be empty before the first write.
It requests policy version 3 to expose conditional bindings, then revalidates each resource and empty policy before setting exactly its recorded binding.
Every write preserves the returned nonempty etag; a missing etag refuses installation, even on an otherwise empty policy.
The [Pub/Sub policy contract](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/Policy) describes that read/modify/write concurrency precondition.
The [GET](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/getIamPolicy) and [POST](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/setIamPolicy) operations use the same guarded transport and storage budget as the resource helpers.
IAM requests use `grant` or `inspect-grants` phases and the resource name with its IAM method suffix (including the version query on reads).
Both methods also call `inspect()`, whose guard callbacks use the `inspect` phase; installation ends by calling `inspect_grants()`.
Unknown policy fields, extra or conditional bindings, ownership drift, non-success responses and ambiguous writes stop installation without retry or policy merging.
Partial installation can be inspected through service readback and cleaned by deleting the owned resources; installation is not resumable and refuses even matching pre-existing grants.
`inspect_grants()` validates the six resource settings and exact explicit bindings again and returns their named policies for external evidence.
It fails when any policy is incomplete; it is not a partial-installation recovery command.

A successful call makes the following operations, measured with the shared helper and synthetic transport:

| Method | Guard callbacks by phase | Pub/Sub requests | Logical GCS reads | Maximum GCS data requests |
| --- | --- | --- | --- | --- |
| `install_grants()` | `inspect`: 14; `grant`: 42; `inspect-grants`: 12 | 42 | 26 | 260 |
| `inspect_grants()` | `inspect`: 7; `inspect-grants`: 12 | 12 | 7 | 70 |
| `provision()` | `provision`: 20; `inspect`: 7 | 18 | 8, plus one create | 80, plus one upload |
| `cleanup_or_confirm_absent()`, manifest present | `cleanup`: 26 | 18 | 8 | 80 |
| `cleanup_or_confirm_absent()`, manifest absent | `cleanup`: 7 | 6 | 1 | 10 |

Nothing reserves these counts: they size a call, and the production guard holds each request to its deadline instead.
These totals exclude credential refresh; total elapsed time needs its own deadline.
Policy reads deliberately re-read ownership, so they use more storage reads than the resource inspection that checks its manifest once.

Policy readback does not prove effective permissions, propagation or the absence of inherited grants.
During separately approved provisioning, retain fresh-resource version-3 policy responses and verify that empty policies return nonempty etags before installation.
If that service behavior is absent, stop and clean the owned resources rather than attempting an unconditional policy write.
[Admission](#admission-and-effective-access) verifies access with each participating identity, retains the observations and waits for IAM propagation within its deadline.
The output subscription belongs to the supervisor's independent observation path; runner and workload are not granted consume access there by this plan.
The [Pub/Sub access-control reference](https://docs.cloud.google.com/pubsub/docs/access-control#required_permissions) lists the permissions for each API operation.
The workload uses input metadata/consume and output publish access, with no resource creation, deletion or policy mutation.
An unsupported manifest version is reported explicitly and refused rather than upgraded or adopted; no version 1 live resource execution was performed in the preparation stage.

Synthetic tests exercise resource and policy request grammar, collisions, manifest replacement, etag conflicts, ambiguous and partial writes, settings drift and repeated cleanup without credentials or service calls.
A composition test uses the production GCS adapter with a fake client to exercise a read that settles after four lost generation races, and that the guard sees one callback per adapter call however many data requests it makes.
These resource-helper tests do not establish live API behavior, effective IAM access or deployed cross-actor lifecycle safety.
A deployed trial starts the relay only after the access probes have passed and the owner has approved the dispatch, as [approval dispatch](#approval-dispatch) describes.

`Resources.cleanup_or_confirm_absent()` selects strict owned cleanup or absence confirmation through one additional guarded manifest read.
A present manifest delegates to `cleanup()` and its ownership checks; an absent manifest permits only six guarded name reads, refuses any existing resource, and never authorizes deletion or adoption.
Its caller must prove quiescence first.
The durable controller below uses this entry point; the original `cleanup()` still refuses a missing manifest.

#### Durable preparation and cleanup

[`PubSubLifecycle`](../../../tools/tier3/src/flink_tier3/pubsub/lifecycle.py) connects the resource helper to the existing generation-checked active run record.
It is an internal caller contract for `pubsub-recovery`; [dispatch](#approval-dispatch) builds and validates the approval, whose trial binds the inputs, but does not admit execution through it.
The caller supplies the lifecycle actor's session, the shared ownership lock, exclusive resource control and deadline guard.
The [IAM apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35504842276) succeeded; subsequent role/binding readback matched the three custom roles and two project bindings, and a local refreshed GCP plan was empty.
These foundation observations do not establish per-resource data access or a recovery result.

`initialize()` saves immutable resource/application intent in `_control/runs/<run-id>.json` before preparation.
Only the submitting runner may initialize or prepare.
`prepare()` claims one attempt through a conditional transition from `initialized` to `preparing`; a competing or restarted runner cannot repeat it, even after a lost response.
Before the helper writes its resource manifest, the controller records `creation_intent: true` in the active run.
Successful resource-settings readback and the final explicit-policy readback are retained in that record, outside the workload JVM, before the stage becomes `prepared`.
The Pub/Sub portion of control is limited to 256 KiB; a failed observation write leaves its partial service work attributable for cleanup, without permitting preparation to resume.
This is bounded control evidence, not the later output-message evidence stream or proof of effective permissions.
`verify_prepared()`, which an actor reaches through its handoff's `verify()`, repeats `inspect_grants()` for either actor while the run is still `APPROVED` or `READY` and the stage is `prepared`, so settings, ownership or policy drift since preparation, including an expired subscription, refuses before admission without writing anything; it checks the run and the stage once, at its start.

Every resource request passes the caller's guard; the controller's own control reads and writes do not.
Only this rig's own code writes the record, the run's two actors and the lifecycle's [recovery](../../lifecycle/README.md#stranded-environment-lock), so the controller reads it as written rather than validating its shape again.
Preparation checks for a shared stop, an evidence failure or a run phase outside approved/ready at its transitions: the claim, the creation intent, the step from creating resources to installing their grants, and the `prepared` mark.
It does not re-read the record before each resource request, so a stop during a step lets that step's remaining requests run and then refuses the next step, which leaves the partial work cleanable.
Verification checks the same conditions once, at its start, and an access probe before each round of tests and each pull, because its wait for grants can last most of the admission window.
There is no numeric execution policy in this controller.

`cleanup(quiesce)` is available to the runner or independent supervisor.
It first persists the shared stop and `cleaning` stage, then requires the caller's barrier to return exactly `True` after proving all creators, writers, replacements and their in-flight service calls quiescent.
The stop flag alone cannot prove that an earlier API request has stopped at the server.
After the barrier, the controller re-reads the record, requires every bound actor's release and delegates deletion to the strict resource helper, whose requests it holds to no deadline, because cleanup must finish whenever it runs.
If creation intent was never recorded, cleanup leaves any colliding service names untouched because this attempt authorized no resource writes.
If creation intent was recorded but the resource manifest is absent, cleanup checks all six planned service names with guarded reads and succeeds only when every name is absent.
This covers a lost creation-intent write response or a crash before the manifest write without reconstructing ownership or deleting anything.
An existing resource, an uncertain absence read or a replaced manifest blocks cleanup.
A barrier failure, uncertain ownership, lost deletion response or failed control write retains active run control; cleanup can be retried, and successful retries recheck owned service absence.
A retry from `cleaned` first returns to `cleaning`, so an uncertain fresh check closes settlement again; a previous absence observation does not override a failed recheck.
Missing-manifest cases with remaining resources require separate investigation; preparation cannot repair or adopt them.

The controller marks only service cleanup complete; it does not set run success, remove Flink state, shut down the Operator, delete active control or release the environment lock.
Shared Operator shutdown and final receipt/lock release refuse a present Pub/Sub record unless its stage is `cleaned` and the shared stop is set.
Final settlement copies the cleaned Pub/Sub control portion, including its intent and retained observations, into `runs/<run-id>/result.json` before deleting active control.
Finalization deletes active control at the generation it derived the receipt from, so a concurrent cleanup or evidence update retains the record and lock for retry, and a retry keeps the receipt it finds.
When control deletion succeeded and lock release did not, the [recovery workflow](../../../.github/workflows/tier3-recover.yaml) writes a minimal cleaned record and finalizes again, keeping the receipt that still carries the cleaned Pub/Sub portion, as the [lifecycle runbook](../../lifecycle/README.md#stranded-environment-lock) describes.
The caller must keep exclusive control and writer quiescence through settlement; a stored cleanup marker does not detect a resource recreated afterward by another administrator.
Synthetic tests compose production record and resource adapters with fake transports for concurrent claims, restart, stop drift, partial mutations, the control-record size limit and shared settlement gates.
[Admission](#admission-and-effective-access) composes this controller with the actors and the access probes, and the supervisor's [fault sequence](#fault-sequence) follows it.

#### Actor construction and the request guard

[`pubsub_actors`](../../../tools/tier3/src/flink_tier3/pubsub/actors.py) is the only production path that binds a Pub/Sub handoff to a runner or supervisor.
Neither checks the manifests again: dispatch rendered them through `pubsub_plan.prepare()`, which requires through `require_trial_jobs()` that each `spec.job` runs exactly what the trial derives, and the approval pins their digests, which the supervisor entrypoint checks against its mount.
A digest pins a manifest, and only that rendering check says the manifest runs the approved trial.
Each actor sends its requests through a [`PubSubSession`](../../../tools/tier3/src/flink_tier3/pubsub/session.py) over the BigQuery actors' [session](../bigquery/README.md#internal-actors), which returns a redirect unread, and its own workload's credentials, and a supervisor takes a fresh process token each time it is constructed.
Constructing an actor sends no Pub/Sub request and does not check which principal the credentials name, although discovering the default credentials may ask the metadata server: each actor takes the credentials of the environment it runs in, the runner's workload identity federation in CI and the supervisor's Kubernetes service account, so a wrong identity needs a misconfigured environment, and IAM then refuses the first call that identity has no permission for ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
The supervisor's cleanup barrier, [`pubsub_quiesce.barrier`](../../../tools/tier3/src/flink_tier3/pubsub/quiesce.py), is built from the run rather than supplied, and waits until no object of any writer kind remains in `tier3-pubsub`, whoever owns it: the namespace's `pubsub` service account can create Pods and Deployments, and any Pod running as it holds the workload's data grants.
It proves only that no such object remains after graceful deletion, not that a request a terminated Pod sent has finished at the service; the actors' unsettled-call markers cover only their own requests.
Outside a run the namespace is expected to hold no such object; dispatch does not refuse one, and one left there keeps the barrier from passing, so cleanup keeps the lock until it is removed.
`Runner.start` and `Supervisor.supervise` refuse `pubsub-recovery` without these actors, and the supervisor entrypoint builds its own through this factory.

Each actor's [`PubSubGuard`](../../../tools/tier3/src/flink_tier3/pubsub/guard.py) is its `before_operation`, and it checks only Pub/Sub service requests.
It refuses to start one less than 20 seconds, the request's timeout, before its deadline.
It runs as the last step before the request is sent, after any control write of the call, such as its begin marker.
That timeout bounds connecting and each read rather than the whole request, a token refresh before the request takes the same timeout, and reading a streamed body is bounded only by the per-read timeout and the helpers' 1 MiB response cap, so a request can still end after the deadline.
The deadline is the admission deadline, the earlier of 900 seconds after the window's start and `cleanup_at`, unless a caller holds another through the controller's `until()`: `publish` and `collect` hold `cleanup_at`, because later cohorts and collection belong to the exercise, a caller publishing during admission passes the admission deadline, and cleanup holds none.
Control and storage operations are not held to it, so a stop, a cleared marker or the evidence of a request already sent can still be written; the handoff enforces stop and evidence failure where it admits new work.
No per-method operation bound and no aggregate request ceiling exists, as the [offline trial proposal](#offline-trial-proposal) explains.

#### Admission and effective access

`Runner.start(config, job, application, probe)` admits a `pubsub-recovery` run only with a handoff that carries its `PubSubGuard` and settled-write check, as `pubsub_actors.runner()` builds.
The manifests and the probe Pod it submits are the ones dispatch rendered and bound to the approval; that rendering checked both jobs against the trial and the probe Pod against the plan it tests and the program it runs, so admission does not check them again.
After the supervisor Job and the Operator are ready, and while the run is `READY`, [`pubsub_admission`](../../../tools/tier3/src/flink_tier3/pubsub/admission.py) runs these steps in order:

1. Bind the runner and the resource intent in one control update.
2. Create the six resources, then install their grants: installing a policy reads the resource it belongs to, so creation comes first.
3. Probe the runner's own access.
4. Open the application quota, run the workload's probe Pod, read what it saw, delete it, and wait until it is gone, so that no other Pod of the run stands beside the application.
5. Wait for the supervisor to join and to record its own access.
6. Re-read every resource and policy.
7. Publish the first cohort, so that the application finds its input waiting.

It then creates the application and sets `RUNNING`.
Every step must finish within 900 seconds of the window's start: `admission_open()` refuses after that, and the guard refuses to start a Pub/Sub request too close to it.
A grant is not taken as proof of access.
Each identity is tested on all six resources for the one permission the run's bindings decide there, publish on a topic and consume on a subscription, so every resource is a positive or a negative control for each identity.
A permission test answers only for the identity that sends it, so the runner and the supervisor each probe themselves.
The workload's service account is reachable only through the `pubsub` Kubernetes service account, so the runner creates a Pod named `<run>-access-probe` that runs as it, through a runner-only Role in `tier3-pubsub`.
The Pod runs the lifecycle tools image with [`pubsub/probe.py`](../../../tools/tier3/src/flink_tier3/pubsub/probe.py) passed inline, because a Pod cannot mount the control ConfigMap from another namespace; CUE renders it at dispatch.
The runner keeps the Pod's log, at most 64 KiB, as `pubsub-workload-probe` evidence and requires the Pod to succeed: the program checks its own identity and every expectation, and exits non-zero on any refusal, so its exit status is the result.
The program refreshes its own token before each request and then starts the request only while its 20-second timeout still fits before the deadline; it uses a plain HTTP session, so nothing replays a request answered 401 after that check.
When a request no longer fits, the program logs a refusal and fails.
As on the runner's side, a slowly arriving response body is bounded only by the per-read timeout and the 1 MiB cap.
The runner records its intent before it creates the Pod, so cleanup deletes a probe Pod the runner left behind, including one whose creation the runner did not survive to record.
A create whose response was lost can land after cleanup began, even after the workload is gone, so cleanup looks for that Pod on every pass of its deletion loop and again at each poll of its namespace-barrier wait.
It adopts the Pod only when it matches the intent, as the runner's own adoption requires, and deletes it even when it cannot record it.
A Pod that lands between a poll's barrier check and the handoff's own second check ends that cleanup pass with the lock kept, as any other writer appearing there would.
A grant that is missing may still be propagating, which IAM documents as typically two minutes and potentially seven or longer, so the six tests are repeated every 15 seconds while a whole further round, one test at a time, still fits before the deadline; a local stop, or one recorded in the run control, ends the wait.
A grant present where it must not be is refused at once: the resource policies were empty before installation, so the run's own writes cannot explain it.
Each consumer then pulls its subscriptions once, without acknowledging, while nothing is published yet; a 403 is retried in the same way, and a received message is refused.
A pull that gets no definite answer marks the session's writes unsettled, but the access probe runs outside the handoff's call markers, so it keeps no marker of its own; nothing is published yet, so it cannot lease a message.
The probe program silences library logging, so the log kept as evidence holds only its own JSON lines.
The actors' observations are recorded under the run control's `pubsub.access`, one entry each for the runner and the supervisor, and reach the final receipt with the rest of the Pub/Sub state; the workload's are the probe Pod's log.

The supervisor's entrypoint builds its actor through `pubsub_actors.supervisor()`.
Once preparation completes it joins, probes its own access, including an empty pull of the output subscription, and waits for admission to finish.
It then runs the [fault sequence](#fault-sequence), and cleanup deletes the run's resources after it.
Only a supervisor deletes Pub/Sub resources, and the Job replaces no supervisor Pod.
A supervisor stopped once the run has Pub/Sub state waits for the runner to settle and release, then cleans up, which must fit its grace period.
A supervisor whose cleanup outlasts its grace period, or whose Pod is gone before cleanup, leaves the resources and the lock for an operator.

When admission fails after resources exist, for example through settings or policy drift found by a read, a probe refused or still missing access at the deadline, or a supervisor that never joins, `start` raises with no call in flight, the runner's settlement releases it, and the supervisor, joined or not, deletes the six resources.
Ownership drift, a replaced manifest or a resource whose labels or live topic binding no longer match, stops the run without completing deletion and keeps the lock, because cleanup cannot prove the resources are this run's.
A write without a definite answer in a handoff call keeps its marker and the lock, as the [handoff](#actor-ownership-and-cleanup-handoff) describes.

#### Actor ownership and cleanup handoff

[`PubSubHandoff`](../../../tools/tier3/src/flink_tier3/pubsub/handoff.py) wraps the resource controller and the message helpers in a durable actor protocol.
The original runner constructs it from its `PubSubLifecycle` and a fresh 32-character lowercase hexadecimal process token, then calls `initialize()` before resource preparation.
Initialize through `PubSubHandoff.initialize()` without first calling the underlying resource controller's initializer.
This handoff entry point writes resource intent and runner authority together in one generation-checked control update; a crash cannot commit resource intent without its actor binding.
If the write did not commit, common cleanup sees no Pub/Sub state; if it committed but the acknowledgement was lost, the original process may repeat initialization with its same token while admission remains open.
If that process died, its binding stays unreleased, and common cleanup keeps the resources and the lock for an operator.
Competing initializations and shared stop are rechecked after a generation conflict; neither a retry nor cleanup adopts a different runner token.
`prepare()` records one runner invocation before creating resources and clears it once the stage is `prepared`.
After preparation and transition to `READY`, the supervisor constructs its own instance with a distinct fresh token and calls `join()` before collection; joining during `RUNNING` is also permitted while admission is open.
The submitting process may repeat its binding call idempotently while that admission window remains open, but another process must not reconstruct or transfer its recorded token.
Tokens identify process ownership; exclusive resource control remains the caller's responsibility.
Use the wrapper from the first initialization call onward: direct resource initialization bypasses atomic actor binding, and direct preparation or message calls bypass its invocation records.

The wrapper's `publish()` and `collect()` use the existing message helpers, with the records-per-subscription domain from the approved trial.
Before each preparation, publication or collection, a conditional update records a fresh invocation ID under the actor's token.
For a publication or a collection, the same update checks that message traffic is still admitted: prepared resources, no shared or local stop or evidence failure, a time before `cleanup_at`, and `RUNNING`, or for the runner `READY` too, so admission can publish the first cohort before the application exists and the run becomes `RUNNING` with its input waiting.
That begin marker and the call's completion are its only control updates on the normal path, two per publication or pull, which cuts the contention on the shared record against Cloud Storage's rate of about one mutation a second on one object; a test holds that count.
Neither actor spaces its writes, so a burst can still exceed that rate, and the conditional update's retries remain.
A call already under way finishes after a stop: it keeps its response and observations and acknowledges what it has durably observed, and only a new call is refused.
An evidence upload that fails, including a committed write whose response was lost, latches a local evidence failure and records the shared one, which refuses new calls by either actor, and the call acknowledges nothing it had not durably observed.
If that failure cannot be recorded, the wrapper raises an explicit restart-barrier error; the caller must stop both actors independently before retrying or restarting, because another process cannot observe an unpersisted failure.
A retry of an existing interval or batch ID collides with its create-only intent, which is such an evidence failure.
One actor cannot start another call while its marker is present; runner and supervisor calls may overlap.
A successful call clears only its own marker, making up to three attempts to persist the conditional completion acknowledgement without repeating the service operation.
If the acknowledgement committed but its response was lost, the retry cannot clear a newer invocation's marker.
A failed call requests shared stop, while a failed control write may prevent that stop from reaching the other actor.
It keeps its marker unless the handoff was given a settled-write check that answers true: the production actors pass their session's, which is true while every Pub/Sub request other than a GET or a permission test that the actor's session ever sent received a status below 500 rather than a transport failure.
The record is the session's, not the call's, so one unsettled write keeps every later marker too.
Storage uploads go through another client and are not tracked; a lost upload response can at most leave its object to land after settlement, never a service resource.
Such a call then clears its own marker, so a refusal before any request, or drift found by a read, leaves the run cleanable through ordinary release; a clearing failure keeps the marker.
A begin write can also land with its response lost; the call then fails before sending anything, so it reads the record and clears that marker too, rather than leaving the lock to an operator's investigation the call does not need.
A write that left without a status, or was answered 5xx, may still complete at the service, and its marker and the lock stay for an operator, as below.
The caller must independently stop actors when shared failure/stop persistence is unavailable.
If requesting shared stop also fails, that control error propagates with the original operation error as its Python exception context; callers that record only the outer message lose that original diagnostic.
The marker does not expire, and a timeout does not prove the service operation has ended.
An original process may retry a lost successful release acknowledgement with its existing token, but an unresolved call keeps the lock for an operator.
Even one transient write failure can therefore stop both actors and keep the lock for an operator; the wrapper does not retry the failed service call.

`stop()` closes admission without declaring quiescence.
Each bound actor calls `release()` to stop admission and permanently surrender its authority once it has no unresolved call.
Normal supervisor `cleanup(quiesce)` requires all bound actors released, then requires the external barrier to prove creators, workload writers and their in-flight requests quiescent.
A supervisor that never joined may call `release()` to stop admission without recording a fictitious actor release; it has no data authority, and stop prevents it joining later.
`released()` observes whether every bound actor has released, without proving external quiescence.
When a handoff is present, the underlying resource controller also refuses service deletion before actor release, and shared Operator shutdown/final settlement enforce the same gate.
Older resource records without a handoff keep their existing external-barrier contract.

An abandoned or unresolved actor has no in-protocol reclamation: the supervisor Job replaces no Pod, so no other process exists to reclaim it.
The record is read as this rig's own code wrote it, and there is no in-protocol repair or reset for corrupt control.
Retain its evidence for investigation and a separately reviewed recovery procedure.
The complete handoff and retained invocation markers survive into the final result receipt.

Synthetic tests exercise concurrent claims, stop/release races, lost acknowledgements, partial preparation, conservative failure handling and receipt preservation.
The deployed trials of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) ran through these actors and this cleanup; their [findings](../../../docs/adr/evidence/0165-pubsub-trial-findings-1361.md) record the outcome.

#### Shared settlement integration

The internal caller may attach its original runner handoff with `Runner(env, pubsub=handoff)` and its supervisor handoff with `Supervisor(env, pubsub=handoff, quiesce=barrier)`; in production, [`pubsub_actors`](#actor-construction-and-the-request-guard) makes both attachments and builds the barrier from the run.
Each handoff must belong to that exact actor environment; a supervisor attachment requires the external barrier, and a lifecycle cannot attach both BigQuery and Pub/Sub handoffs.
Admission initializes, prepares and publishes the first cohort, and the supervisor joins, through `PubSubHandoff`; the runner publishes the later cohorts from `Runner.settle()`, as [cohort requests](#cohort-requests) describes.

A runner that settles with `request_stop=True`, or that is already stopping, releases before it waits, as before.
Otherwise it keeps its authority while it waits, serving cohort requests on each poll, and releases once a stop or evidence failure is recorded, the supervisor's Job ends or is lost, the run leaves `RUNNING`, `cleanup_at` passes or a publication fails; the last is recorded as `pubsub-publication-failed`.
A stop that lands while a requested cohort is still being published refuses its next batch, which is such a failure and withholds success; a stop that lands before the runner has recorded a requested cohort's start, including one between its poll and that record, only closes serving, so the cohort stays unpublished without a failure, and the run's verdict is the supervisor's.
Release closes shared admission, so the supervisor collects no further output after it.
There is no circular wait with the supervisor's cleanup: that cleanup records the shared stop before it waits for the runner's release, and the runner releases on the next poll that sees the stop.
A supervisor whose Job ends before it cleaned also releases the runner, whose settlement then finds the Pub/Sub resources undeleted and keeps the lock, because only a supervisor deletes them.
It retries an unsuccessful release during subsequent settlement polls and once after the wait.
`pubsub-release-blocked` records each changed failure cause; a release failure latches local stop and prevents that runner's settlement from recording success even if a later acknowledgement succeeds.
No retry repeats publication or reclaims an unresolved invocation.
Failure to persist stop or evidence still requires the independent stop path described above.

Common cleanup first stops admission and removes the owned Flink workload while the Operator is running.
It then releases the supervisor, waits for all bound releases and an exactly-`True` external barrier, and rechecks the barrier inside resource cleanup before deletion.
Only recorded service cleanup permits checkpoint/state deletion, Operator shutdown and shared completion.
While service cleanup is unfinished, an unresolved call, missing supervisor attachment or failed service deletion keeps common cleanup from deleting temporary state or stopping the Operator.
Common cleanup never takes over another process's actor.

Direct `Records.set_phase(CLEANED)`, `Records.settled()` and `verify_idle()` now enforce the same Pub/Sub clean-state gate as final receipt creation.
Records without Pub/Sub state retain their prior behavior.
Cleanup routes Pub/Sub resources to `tier3-pubsub` and temporary state to `flink-gcp-tier3-pubsub`; existing smoke, Cloud Tasks and BigQuery inventory scopes remain unchanged.
The production attachments are built by [`pubsub_actors`](#actor-construction-and-the-request-guard), and [admission](#admission-and-effective-access) uses them.
A successful synthetic cleanup is not a recovery verdict; the [verdict](#verdict-and-analysis) is.

### Exercise and oracle

The supervisor runs the [fault sequence](#fault-sequence) on the relay's `pubsub-progress` lines.
The runner publishes the input cohorts the supervisor asks for, the [output collector](#output-collector) pulls the output subscription on each poll, and when the run completes the [oracle](#offline-output-reconciliation) reconciles every collected line and the supervisor checks each output's input message ID against the runner's publication receipts.

#### Input publication and output collection

[`Messages`](../../../tools/tier3/src/flink_tier3/pubsub/messages.py) supplies internal data helpers; [admission](#admission-and-effective-access) and the runner's [cohort requests](#cohort-requests) publish through them, and the supervisor's [output collector](#output-collector) pulls through them.
Construct it with the existing `ResourcePlan`, the frozen `records_per_subscription` domain, the actor's role, the shared authorized HTTP session/GCS adapter and a mandatory `before_operation(phase, method, name)` guard.
The role parameter checks routing only; the caller must bind the plan and input domain to current approval.
The guard runs before every operation; the caller verifies prepared resources and effective access, the active run, exclusive actor authority and stop conditions before each call.
A storage `PUT` callback denotes a create-only logical upload (GCS HTTP `POST`).
The guard receives phase `publish` for input intent, publication and response storage, `collect` for output intent, pull and response/observation storage, and `acknowledge` for the ACK request and receipt.
Method `PUT` always denotes storage, while `POST` always denotes a Pub/Sub data request in this helper.
The [actor handoff](#actor-ownership-and-cleanup-handoff) composes these helpers with prepared run control; the helpers alone neither hand off actors nor implement the cleanup quiescence barrier.

The runner calls `publish(input_index, start, count)` for one interval of at most 1,000 logical inputs on one of the two planned input topics.
It stores the exact request and run/nonce/domain binding in `runs/<run-id>/pubsub/messages/<nonce>/input/<index>/<start>-<count>/intent.json` with generation-match zero before publishing.
An existing intent refuses that interval, including after a restart, a lost intent-write response or an ambiguous publication response.
A successful service response is saved as `response.json` before returning its IDs; the [publish API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish) binds returned IDs to request order.
The helper rejects missing, empty or repeated IDs within the batch.
A missing receipt means the outcome is unknown, not that the service accepted no messages.
The caller must freeze disjoint phase cohorts, because overlapping intervals would publish duplicate logical inputs; the [cohort ranges](#offline-trial-proposal) are disjoint by construction.

The supervisor calls `collect(batch_id, max_messages=100)` for one pull from the planned output subscription.
The pull sets `returnImmediately`, so an idle subscription answers at once: the service may otherwise hold the request until messages arrive, past the 20-second transport timeout, and a pull without a definite answer would keep the call's marker and the lock.
The service may then answer empty even while messages wait, which costs a poll's delay and nothing else.
It creates an intent before the pull and saves the decoded response before interpreting its received-message envelopes.
It then writes `observations.json`, containing every output message ID and full payload as the offline oracle's unpadded base64url TSV, before sending any acknowledgement.
Append each batch's `tsv` value once when assembling the oracle input; preserve repeated lines and repeated deliveries across different batches.
It decodes both standard and URL-safe Base64, with or without padding, following the [ProtoJSON bytes format](https://protobuf.dev/programming-guides/json/#representation-of-each-type), and emits canonical unpadded base64url in the TSV.
The collector preserves foreign, empty and binary payloads for the oracle to reject; it does not validate relay semantics, filter logical identities or deduplicate messages.
Malformed envelopes, invalid encoding or exceeded local limits retain the response and stop without acknowledgement.
Malformed JSON, transport failure or a response over the transport cap leaves the intent but may have no response receipt.

An evidence-write failure, including a committed write with a lost response, prevents the following acknowledgement.
An ambiguous ACK retains the observations; another pull requires a new batch ID and may record the same output again.
A successful ACK response is followed by a create-only `acknowledged.json` receipt; its absence leaves ACK outcome unknown.
The [acknowledge API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/acknowledge) permits expired IDs and repeated acknowledgements, so this receipt records a successful API response without promising no redelivery.
An [empty pull](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull) is saved without sending ACKs and is not evidence of a drained subscription.
No API operation or evidence upload is retried by the helper, and no existing evidence is overwritten or removed.
The caller must preserve intents and evidence through assessment and must not reuse a run's batch paths after deleting them.

A publication admits at most 1,000 messages and a collection at most 100, each with a streamed response of 1 MiB, an output payload of 4096 decoded bytes, a message ID of 1024 UTF-8 bytes and an ACK ID of 4096 UTF-8 bytes.
These are local collection limits, not Pub/Sub service limits; decoded response JSON and serialized GCS evidence can be larger than the wire representation.
Each publication makes two logical GCS writes and one Pub/Sub request, and each nonempty collection four logical GCS writes and two Pub/Sub requests (an empty collection three writes and one request).
The shared 20-second HTTP timeout applies to transport waits, not total trial elapsed time; the response byte cap does not bound service billing or an integrated trial's storage and request counts.
Before cleanup, the caller must stop and fence both actors and prove their in-flight requests quiescent, even if a helper has returned a timeout.
Synthetic tests establish request/evidence ordering and refusal behavior; they do not establish effective IAM access, service ACK behavior or deployed recovery acceptance.

#### Cohort requests

Only the runner holds the publisher grant on the input topics, so the [cohorts](#offline-trial-proposal) after the first are published by the runner at the supervisor's request, through [`PubSubHandoff`](../../../tools/tier3/src/flink_tier3/pubsub/handoff.py).
The run control's Pub/Sub portion keeps one entry per cohort under `cohorts`, with its deadline and the times it was requested, started and published.

- **`request_cohort(name, deadline=…)`**: the supervisor asks for `after_checkpoint` or `after_recovery`, only once the cohort before it is published, while the run is `RUNNING` and its message traffic is admitted, which lasts until `cleanup_at` rather than the 900-second admission window, and with a deadline after now and no later than `cleanup_at`. Repeating a recorded request with the same deadline changes nothing, even after that deadline or a stop, so a retry after a lost response confirms it; another deadline is refused.
- **`serve()`**: the runner publishes the first requested cohort that has not started, and returns its name, or `None` when nothing waits. `Runner.settle()` calls it on each poll.
- **`publish_cohort(name, deadline=…)`**: the runner publishes one cohort to both input topics in batches of at most 1,000. Admission calls it for `before_checkpoint` with the admission deadline; `serve()` calls it for a requested cohort, which takes no deadline argument and uses the recorded one.

A cohort's start is recorded before its first publication request, so `started_at` precedes every message of the cohort, and a cohort that started is never started again.
A batch that fails stops the run through the handoff's usual failure path, leaving the cohort started but unpublished.
The start is refused, and nothing recorded, once less than the guard's 20-second request budget is left before the deadline, because the guard would refuse every batch after it; each batch's own request keeps that budget too.
The supervisor sets a requested cohort's deadline 90 seconds after its request (`PUBSUB_EXERCISE["cohort_seconds"]`), or at `cleanup_at` when that is sooner, which leaves the runner, serving requests once a poll, several polls to start it.
A cohort that cannot start, or a batch that cannot be sent, before the deadline fails its publication, which the runner records as `pubsub-publication-failed` before it releases its authority.
The request and the two marks are one control update each, and each batch is a `publish` call with its own markers.
The start and end marks are read from the runner's clock and the request time from the supervisor's, so an ordering across the two holds only up to the skew between those hosts.

#### Output collector

[`OutputCollector`](../../../tools/tier3/src/flink_tier3/pubsub/output.py) pulls the output subscription through the supervisor's handoff, so every pull is a marked call and leaves the evidence `collect` writes.
`pull()` takes one batch of up to 100 under the next batch ID: `out-`, eight random hexadecimal digits fixed for the collector, and a counter, so that two collectors name the same create-only evidence only if those digits collide.
`drain(max_pulls)` repeats it until a batch comes back empty or the pulls are spent; an empty batch ends the round without proving the subscription empty.
A short batch does not end it, because an immediate pull returns what the service has ready at once rather than the backlog; the exercise drains at most 20 pulls a poll, the bound [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) set under [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720) so that one poll could collect a cohort's output before the next checkpoint.
Each collected line whose payload is this run's relay output, with the thirteen fields of [the payload](#payload-and-restoration) in canonical form, becomes an observation: input index and sequence, input message ID, attempt, observation ID, phase, whether the attempt restored state, its origin and epoch, and its subtask and attempt number, with the output message ID, batch and collection time.
Any other line, including another run's, is kept apart by batch and stays in the evidence, and the [verdict](#verdict-and-analysis)'s oracle refuses it.
Every collected line is also kept in collection order, repeats included, as the oracle's input for the [verdict](#verdict-and-analysis).
The observations live in the supervisor's process for the exercise.

#### Offline output reconciliation

An independent output subscriber must record every observed output service message ID and its full payload before acknowledgement.
The offline tool reads one record per UTF-8 line: base64url output message ID, a tab, then base64url payload, both without padding.
Use the built application and copied dependencies from its directory:

```sh
java -cp 'target/pubsub-recovery.jar:target/image-lib/*' \
  io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryReport \
  /tmp/pubsub-output.tsv --run-id=example --records-per-subscription=1000
```

The tool rejects files over 64 MiB, more than 200,000 observations, oversized records, conflicting identity reuse, foreign runs and missing logical input IDs.
Its counts describe the supplied evidence, not a complete service inventory or future deliveries after collection stops.

| Counter | Counted population |
| --- | --- |
| `logical_inputs` | Distinct input-index/sequence pairs; must equal twice the configured per-subscription count |
| `input_publication_duplicates` | Extra input service message IDs for the same logical input, scoped by input topic |
| `repeated_input_processing` | Extra processing observation UUIDs for the same input service message ID |
| `output_publication_duplicates` | Extra output service message IDs carrying the same processing observation UUID |
| `repeated_output_delivery` | Repeated observations of the same output service message ID |
| `covered_redeliveries` | Input service message IDs a restored attempt processed although a checkpoint in its restore lineage had covered them |

The collector must preserve output message IDs: discarding them would conflate its own redelivery with duplicate sink publication.
Completeness alone does not establish recovery, exactly-once processing/output, strict replay ordering or a checkpoint-confirmed fault boundary.
The exercise's [verdict](#verdict-and-analysis) applies these rules through a Python port, because the supervisor's image carries no JVM; the emulator integration test keeps using `PubSubRecoveryReport`.
The port returns the same six counters, and both test suites decide the cases in [`oracle-cases.txt`](src/test/resources/oracle-cases.txt), so a rule changed on one side only fails the other's run of them.
Those cases pin the report's own rejection messages; where the JDK writes the message instead, as for an unparseable number, UUID or Base64 field, the port words it differently, and the cases require only that both refuse.
The Java tool's 64 MiB file bound and each side's line-reader checks are outside the shared cases.
Instead of refusing a run with missing inputs, the port reports how many are missing, so that the verdict can tell a refused line from an incomplete set.
The [offline recomputation](#offline-recomputation) uses the same port over the exported batches.

### Verdict and analysis

The shared [`sequence_verdict`](../../../tools/tier3/src/flink_tier3/verdict.py) decides a `sequence` run from the record its completion writes, with the reasons [`pubsub.data_reasons`](../../../tools/tier3/src/flink_tier3/pubsub/verdict.py) gives.
The run is `usable` when it completed, the [oracle](#offline-output-reconciliation) accepted every collected line and found every logical input of both subscriptions, and every output names its input under an ID the runner's publication receipt for that input holds.

| Condition | Reason when it fails |
| --- | --- |
| The stage is `complete` | `recovery-incomplete` |
| The oracle accepted every collected line | `oracle-missing`, `oracle-rejected` |
| The oracle found every logical input of both subscriptions | `oracle-incomplete` |
| The record carries the receipt check | `inputs-missing` |
| Every output's input message ID is one that its input's receipt holds | `input-ids-unpublished` |
| Every output's input has a receipt | `input-receipts-missing` |

The relay copies each input's message ID into its output, and the publish API binds the IDs it returns to request order, so each receipt names its inputs' IDs and an output under another ID carries a wrong value.
The exercise reads every publication's `response.json` from the evidence bucket when the run completes.
A line of another run, or a malformed one, is the oracle's refusal, so a record outside the expected set fails the run as well.
The oracle's duplicate counters are reported beside the verdict; restores, interruptions, timings and metrics decide nothing, and no window is sampled.
At-least-once delivery owes them, and neither unique Pub/Sub message IDs nor a deduplicated count can establish exactly-once output, so their absence would prove nothing either.
A `usable` verdict says nothing about ordering across the replay: the oracle is a completeness check over a set of identities.

#### Offline recomputation

`flink-tier3 analyze --evidence <directory>` reads a downloaded run directory, as `gcloud storage cp --recursive` leaves `runs/<run-id>/`, and recomputes a `pubsub-recovery` run's verdict beside the BigQuery section; it contacts no service.
[`pubsub.analyze`](../../../tools/tier3/src/flink_tier3/pubsub/analyze.py) rebuilds from the evidence alone:

- the output oracle, from every batch in batch order, each appended once, with its lines derived again from the saved pull response by the function the collector used, so the batch's `observations.json` is not read;
- the input identities, from the runner's publication receipts, which bind the returned IDs to request order within the interval their path names, so each output's input message ID is checked against what was published for its input and sequence.

The verdict is recomputed from the rebuilt oracle and from the receipts' check of every exported output, through the same `sequence_verdict`.
A batch that a `pubsub-pull-retried` event names holds an intent and no observations, so it is not counted as lost, even in a run that stopped before its record listed it.

The report lists the faults the run injected and places the recomputed verdict beside the verdict the run stored, through steps every section [shares](../../../tools/tier3/src/flink_tier3/recovery_analysis.py).
A single-fault run is reported without a verdict, beside the verdict it stored and the problem `single-fault-trial`; it is analysed at the revision its approval's `sha` names.
Its approval's trial names a kind a single-fault trial named: `jm-replacement`, `tm-replacement`, `rescale-out` or `rescale-in`, or one of the reactive and adaptive scheduler kinds, `reactive-scale-out`, `reactive-scale-in`, `reactive-tm-eviction`, `reactive-tm-kill` or `adaptive-scale-in`.
An approval whose trial names any other kind, or none, is reported as `approval-without-pubsub-trial`.

| Problem | Meaning |
| --- | --- |
| `verdict-differs-from-the-record` | The stored verdict or its reasons are not the recomputed ones |
| `single-fault-trial` | The approval's trial is a single-fault kind of ADR-0165 |
| `approval-without-pubsub-trial`, `missing-result.json`, `malformed-evidence`, `malformed-message-evidence`, `missing-recovery-complete` | The evidence the verdict is decided from cannot be read; the run has no verdict |
| `missing-output-batches`, `missing-output-lines`, `missing-input-receipts` | A partial download; the run has no verdict |

A partial download is found by a gap in a collector's batch counter, by fewer exported lines than the completed record counted, or by an output of a completed run whose input has no publication receipt.
An oracle rebuilt from it would lower the verdict for what the export lost, so the run gets none; fetch the evidence again.
The analysis does not classify a disagreement as an edited record or as an inconsistent export, as for BigQuery ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
An unreadable `supervisor/` or `result.json` document stops the analyzer for the whole directory, as for the other sections.
The section reports the oracle's duplicate counters, and the runner's receipts beyond one per input as extra publications, apart from the verdict, and states that a usable verdict establishes neither ordering across the replay nor exactly-once output.
Tests export the evidence a simulated `sequence` run writes through the real supervisor, collector and message helpers and recompute its verdict from it; edited copies show which edits the recomputation reads and which it does not, and one whose approval names a single-fault kind is reported as `single-fault-trial`.
