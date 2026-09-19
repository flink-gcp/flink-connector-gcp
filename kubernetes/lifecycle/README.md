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

# Bounded Tier-3 lifecycle

The `Tier-3 run` workflow admits one generic smoke application and returns the environment to its tracked idle state.
Its `scenario` input selects ordinary completion (`smoke`, the default) or the fixed `generic-recovery` exercise below.
The [flink-tier3 workspace member](../../tools/tier3/pyproject.toml) owns the CLI, bootstrap/schema commands, runner and supervisor.
Its [source package](../../tools/tier3/src/flink_tier3/) uses ordinary imports, with `Supervisor`, `Runner` and `Cleanup` composed through `Environment`.
The installed `flink-tier3` CLI is the entry point; [workspace instructions](../../tools/README.md) cover building a wheel and selecting a repository checkout.
[delivery.cue](delivery.cue) receives the installed package's Python modules and `policy.toml` as JSON on standard input and projects them through one immutable ConfigMap.
The Pod runs `python3 -m flink_tier3 supervisor` from that projected directory.
The approval's `runtime_sha256` covers the relative paths and SHA-256 hashes of the complete package source bundle, including policy; the supervisor verifies it before cloud access.
Adding a module automatically includes it in both the source bundle and its hash; the rendered-payload test checks completeness.
Third-party dependencies remain preinstalled in the pinned image; package source changes need no image rebuild or startup installation.

| File | Responsibility |
| --- | --- |
| `flink_tier3/policy.toml` | Reviewed environment names, conservative cost rates, fixed ceilings and Pod resources |
| `flink_tier3/google.py` | Google credentials, standard authorized session and generation-checked GCS operations |
| `flink_tier3/kubernetes.py` | Kubernetes SDK calls, API collection mapping, UID/version conditions and log limits |
| `flink_tier3/model.py` | Approval, schedule and typed control-record transitions |
| `flink_tier3/records.py` | GCS control CAS, evidence and the shared environment lock |
| `flink_tier3/environment.py` | Actor dependencies, identity/admission checks and bounded waits |
| `flink_tier3/cleanup.py` | Ownership-aware cleanup and idle verification |
| `flink_tier3/supervisor.py` | Progress/checkpoint observation and supervision |
| `flink_tier3/exercise.py` | One savepoint upgrade, one JM Pod deletion and phase-specific recovery evidence |
| `flink_tier3/cloudtasks.py` | Cloud Tasks queue ownership, campaign ledger, session files and cell manifest checks |
| `flink_tier3/protocol.py`, `protocol_1246.toml` | The pinned #1246 measurement protocol and the quantities sessions derive from it |
| `flink_tier3/observe.py` | Per-poll Flink REST, TaskManager, queue and Pod observations of a running cell, and the interrupt control |
| `flink_tier3/evidence.py` | Row and receipt reconciliation, verified export to durable evidence and benchmark prefix release |
| `flink_tier3/analyze.py` | Offline analysis of downloaded evidence: windows, throughput, p95, verdicts and calibration checks |

The policy file is part of the reviewed revision, with no runtime override path.
The fixed approval phrase and existing ceiling values remain unchanged.
API collection paths and identity-validation rules stay in Python because they describe implementation contracts.
The workload remains the [source-backed generic smoke application](../apps/smoke/README.md).
Live execution and recovery behavior still require the separately approved [#1311 exercise](https://github.com/flink-gcp/flink-connector-gcp/issues/1311).
Merging this implementation does not authorize a dispatch.

## Execution approval

A maintainer separately approves the current reviewed `main` SHA, scenario, a unique run ID, the absolute UTC expiry, and the ceilings below.
Only a dispatch on `main` with that exact SHA can assume the runner identity and admit work.
The workflow input must contain `APPROVE ONE SMOKE RUN: 4 PODS, 60 MINUTES, USD 1` verbatim.
Expiry must be 55–60 minutes ahead when admission starts; queue delay can make an otherwise valid dispatch fail before changing quotas.
The last 15 minutes are reserved for cleanup, leaving at most 45 minutes for startup and the 30-minute smoke job.
A Cloud Tasks session's evidence is downloaded and analyzed after the run; see [Local verification and evidence](#local-verification-and-evidence).

After obtaining that separate execution approval, use the Actions UI or this command with the approved values:

```sh
gh workflow run tier3-run.yaml --repo flink-gcp/flink-connector-gcp --ref main \
  -f run_id=APPROVED_RUN_ID \
  -f reviewed_sha=APPROVED_MAIN_SHA \
  -f expires_at=APPROVED_UTC_EXPIRY \
  -f 'approval=APPROVE ONE SMOKE RUN: 4 PODS, 60 MINUTES, USD 1'
```

For the recovery exercise, also pass `-f scenario=generic-recovery`.
This authorizes one savepoint upgrade and one deletion of the owned JobManager Pod within that run; another trial requires a new approval and run ID.
Ordinary smoke approvals retain version 1, while recovery approvals use version 2 and pin the recovery policy and both application manifests.
Recovery tooling can settle either version; it never resumes the exercise or repeats an injected disruption.

A Cloud Tasks measurement session is the third scenario; its dispatch, ceilings and queue lifecycle are in [Cloud Tasks session](#cloud-tasks-session) below.

Run IDs contain lowercase ASCII letters, digits and internal hyphens, with at most 40 characters.
A retained approval without a final idle receipt blocks another run, including reuse of that ID.
Before admitting anything, the runner verifies the dedicated kubeconfig, explicit `gke_flink-gcp_us-central1_flink-tier3` context, GKE-discovered DNS endpoint, both namespace UIDs, original quota UIDs and zero admission, idle workload inventory, and the installed Operator image/resources.
It checks each pinned GAR digest exists and remains at least 24 hours away from seven-day deletion eligibility at the approved expiry.
These checks run before temporary quota changes or Operator scale-up.

## Resource and cost approval

Requests equal limits for every container.
The supervisor excludes Spot through required node affinity and runs in `tier3-system`, independently of the two Spot Flink Pods.
The Operator uses its tracked normal-capacity template.

| Workload | Namespace | Maximum Pods | Capacity | CPU per Pod | Memory per Pod | Ephemeral storage per Pod |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| JobManager | tier3-smoke | 1 | Spot | 1 | 2 GiB | 1 GiB |
| TaskManager | tier3-smoke | 1 | Spot | 1 | 2 GiB | 1 GiB |
| Operator | tier3-system | 1 | Normal | 1 | 2 GiB | 1 GiB |
| Supervisor | tier3-system | 1 | Normal | 0.25 | 0.5 GiB | 0.125 GiB |

| Budget | Ceiling or stop condition |
| --- | --- |
| Duration | Absolute expiry within 60 minutes; begin cleanup 15 minutes before it |
| Pods | Four total; two in each namespace |
| PVCs | Zero throughout admission and cleanup |
| State | Stop on observed usage above 1 GiB or 10,000 objects in the run prefix |
| Logs | Stop at 100 MiB collected; stop if the initial 1 MiB or a later 64 KiB read would truncate |
| Durable evidence | 100 MiB per run; supervisor 88 MiB, runner 10 MiB, 2 MiB reserved for approval/manifests/final receipts |
| Incremental cost approval | USD 1 per run |

ResourceQuotas enforce aggregate Pod, CPU, memory, ephemeral storage, PVC, Job, CronJob and StatefulSet admission limits.
Every observation also checks actual container counts, image digests, effective requests/limits and ownership.
State and telemetry are polling stop thresholds, not service-side write quotas; writes between observations can exceed a threshold before cleanup begins.
Evidence export uses conditional creates and a separate budget for each writer; it stops admission when an export fails or its budget is exhausted.

The conservative cost model charges all 3.25 CPUs at USD 0.10 per CPU-hour, all 6.5 GiB memory at USD 0.02 per GiB-hour, and all 3.125 GiB ephemeral storage at USD 0.001 per GiB-hour, even for Spot Pods.
Including USD 0.25 for the bounded storage, requests and telemetry gives USD 0.708125 for a full hour.
Those rates exceed the [published Iowa Autopilot prices](https://cloud.google.com/kubernetes-engine/pricing), checked on 2026-09-14; admission refuses a pricing review older than 30 days.
This is an execution budget model, not a real-time billing meter or a cloud billing cutoff.
The standing cluster fee and existing persistent infrastructure are outside incremental cost.
An API outage, stuck finalizer or unavailable node can delay verified termination beyond expiry; the workflow reports failure, retains the lock and requires incident recovery rather than claiming a cost guarantee it cannot enforce.

## Admission and supervision

The runner writes a conditional-generation environment lock, immutable approval/application/image receipts, and a mutable run record before it admits the supervisor.
The ConfigMap creation intent retains a hash of its data alongside the submitted metadata, so each heartbeat CAS does not copy the entire source bundle.
Lost-response adoption verifies that hash before recording the ConfigMap UID.
Once a root UID is recorded, cleanup uses that identity without re-adopting the resource against its original submitted fields; server-side template changes therefore cannot block shutdown.
The approval pins the source hash, application hash, run nonce, workflow execution, namespace/Operator/quota UIDs, baseline object UIDs and original quotas.
The runner opens enough system quota for one supervisor, then creates its Job with no retry and an active deadline calculated from three minutes before expiry.
The supervisor waits for its recorded Job UID, verifies its Pod belongs to that Job, and reports its heartbeat while waiting for the runner to complete admission.
The runner requires that heartbeat and a running, UID-owned supervisor Pod before opening full quotas, scaling the Operator to one, waiting for Operator readiness and creating the fixed FlinkDeployment.
It persists the application UID and changes the control phase to `running` only after admission completes.
While the phase is `approved` or `ready`, the supervisor reports its heartbeat without auditing an application whose creation or UID publication may still be in flight.
If supervision fails in that phase, it requests stop and exits; the runner settles after its last admission call returns.
A lost runner before that handoff requires completed-execution recovery, so admission-stage shutdown depends on that recovery path.
Once `running` is published, admission makes no further Kubernetes writes and the supervisor can clean independently of the runner.
Each quota/scale admission retry rechecks stop and phase as well as identity and lock ownership.
The Operator's Scale API can omit `spec.replicas` when it is zero; lifecycle reads that absence as zero and uses JSON Patch `add` to set either an absent or existing replica count.
The scale patch retains UID and resource-version conditions, and cleanup skips the write when the Operator is already at zero.

For ordinary smoke, the supervisor observes the application and writes Kubernetes only toward idle.
The recovery scenario also permits the two recorded operations described below.
Both supervisor and recovery use `Cleanup`; concurrent cleanup repeats the same UID-checked deletes and original quota/replica values.
Generation conflicts merge roots and observations without replacing recorded identities, and phase transitions never return from cleanup to admission.
The supervisor uses the projected Kubernetes service-account token against the in-cluster API and Google metadata credentials for GCS.
The external runner uses ADC/WIF against the verified DNS endpoint; Kubernetes service-account tokens are not sent to that endpoint.
The Google SDK requests both `cloud-platform` and `userinfo.email` OAuth scopes, matching the [GKE authentication plugin](https://github.com/kubernetes/cloud-provider-gcp/blob/master/cmd/gke-gcloud-auth-plugin/default_credentials_token_provider.go).
The email scope lets GKE identify the impersonated service account by the email bound in Kubernetes RBAC; without it, the numeric account ID can produce a `403` even when kubectl preflight succeeds.
The supervisor records a heartbeat, inventory, Pod logs and Flink checkpoint observations.
The first log read retains up to 1 MiB of startup history; subsequent reads retain up to 64 KiB since the previous observation.
Reaching either read ceiling fails the run instead of discarding possible smoke lineage evidence.
Ordinary smoke success requires the job to finish, smoke progress to establish its lineage, and at least one completed checkpoint to have been observed through the UID-owned REST service.
Progress from another run/phase or a changed lineage fails the run, including an unexpected fresh start following disruption.
Failure, cancellation, resource/evidence limits or the test deadline enters cleanup.
In ordinary smoke, a transient checkpoint-proxy error also starts cleanup, including a JobManager restart while its Service has no endpoints.

## Generic recovery exercise

The reviewed policy fixes 12,000 deterministic sequence records at ten records per second, one savepoint upgrade and one JM Pod deletion, with no automatic repetition.
These inputs require about 20 minutes of uninterrupted processing.
The resource, storage, telemetry and USD 1 ceilings above are unchanged; this is a correctness exercise, not a performance sample.

| Stage | Required evidence and deadline |
| --- | --- |
| Initial execution | Within ten minutes of admission start, observe nonzero initial progress and a completed checkpoint triggered after that progress |
| Savepoint upgrade | Change only `--phase` and `--require-restored` on the same FlinkDeployment; within five minutes, observe its reconciled generation, a new upgrade savepoint and restoration from that path |
| JobManager failover | After upgrade recovery and a later completed checkpoint, delete the owned JM Pod with its UID as a precondition; within five minutes, observe a different JM UID restoring the same job from that checkpoint or a newer checkpoint |
| Completion | After both recoveries, observe all 12,000 records, the final sequence value 11,999 and `FINISHED`, before the cleanup deadline |

Every recovery must retain the initial lineage, emit `restored=true`, advance beyond the pre-disruption processed count and complete a checkpoint triggered after the resumed progress.
The oracle uses Kubernetes log timestamps, current JM identity, job ID and Flink's restored checkpoint ID, path and timestamp.
Savepoint upgrades can start a new job ID; JM failover must retain the upgraded job ID.
Checkpoint counts alone, a snapshot barrier alone, or replaying logs from a previous phase cannot satisfy these predicates.
The savepoint and checkpoint paths must stay within this run's state prefix.

The recovery manifests keep `upgradeMode=savepoint` and `allowNonRestoredState=false`, and disable last-state fallback.
They also set `kubernetes.operator.snapshot.resource.enabled=false`: Operator 1.15.0 otherwise creates a separate FlinkStateSnapshot by default.
This bounded exercise uses that version's supported status-based savepoint reporting and keeps the existing application cleanup graph.
Both manifests are rendered through CUE, hashed into approval and delivered in the immutable supervisor ConfigMap.
The runtime validates that their only difference is the two job arguments before accessing the cluster.

The supervisor records each operation's intent and immutable evidence before issuing its API call.
It rechecks cancellation, evidence status, the shared lock and the phase deadline before the write.
The upgrade patch carries application UID and resourceVersion tests and cannot recreate a deleted application; JM deletion targets only the observed Pod UID and uses ordinary termination.
A lost response is accepted only after a read verifies the requested outcome; an uncertain outcome stops the trial without repeating the operation.
An upgrade precondition rejection also stops the trial without retry; its receipt distinguishes that rejection from an uncertain response.
Cleanup can race these calls: a deleted or changed application rejects the old patch, and a replacement Pod cannot receive an old UID's delete.

During either five-minute recovery window, checkpoint-proxy transport failures and HTTP 404/502/503/504 are recorded and retried by the observation loop.
Authentication failures and ownership changes still fail immediately.
An old Pod disappearing during log collection is recorded; the run must still obtain the required recovery evidence from surviving/replacement Pods.
During the savepoint upgrade, temporary cancellation/suspension is expected.
Operator 1.15.0 also reports the old job as `FINISHED` after stopping it with a savepoint; accept that state only for the old job ID while reconciliation remains `UPGRADING`.
It does not satisfy final completion, and terminal failure or Operator rollback still stops the trial.
No wait extends the absolute cleanup deadline.

JM/TM Pod placement remains Spot and the supervisor remains on normal capacity.
The supervisor retains run-owned Kubernetes Events, Pod conditions and scheduling observations.
Observed eviction/preemption, container restart or replacement outside a planned recovery window makes the trial inconclusive and starts cleanup.
Exclude already-terminating Pods when recording the stable post-failover Pod set.
After the complete input has been observed, allow that set to shrink as idle Pods terminate before the Operator reports `FINISHED`; a new Pod UID still counts as an unplanned replacement.
An interrupted or insufficiently observed trial is never reported as steady-state performance evidence.

Before approving dispatch, verify the three applied infrastructure roots and their empty refreshed plans, the idle Helm release, workload KSA/GSA binding and state-bucket grant, and live image retention through expiry plus 24 hours.
Do not merge infrastructure changes while the run holds the shared lock.
This implementation prepares [issue #1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311); it supplies no GKE execution result by itself.

## Cloud Tasks session

A `cloudtasks` dispatch admits one measurement session for [issue #1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246): an ordered list of cells from a reviewed session file, executed one FlinkDeployment at a time in `tier3-cloudtasks`.
The workflow input must contain `APPROVE ONE CLOUD TASKS SESSION: 4 PODS, 300 MINUTES, USD 10, 0 DISPATCHES` verbatim, and the dispatch names `session` (a file under [sessions/](sessions/) without its `.toml` suffix), `flink_version` (`2.2.1` or `1.20.4`) and `application_digest` (the published `sha256:` digest of that line's measurement application package).
Expiry must cover the session plan plus 15 minutes of cleanup and lie within ten minutes of that sum, at most 300 minutes ahead; the plan is the sum over cells of ten minutes of startup, the nominal input window and three minutes of teardown.
After obtaining that separate execution approval and a published digest, dispatch with the approved values:

```sh
gh workflow run tier3-run.yaml --repo flink-gcp/flink-connector-gcp --ref main \
  -f scenario=cloudtasks -f session=APPROVED_SESSION_NAME \
  -f flink_version=2.2.1 -f application_digest=sha256:PUBLISHED_DIGEST \
  -f run_id=APPROVED_RUN_ID -f reviewed_sha=APPROVED_MAIN_SHA \
  -f expires_at=APPROVED_UTC_EXPIRY \
  -f 'approval=APPROVE ONE CLOUD TASKS SESSION: 4 PODS, 300 MINUTES, USD 10, 0 DISPATCHES'
```

The application digest is verified live against Artifact Registry with the same 24-hour retention margin as the other images; it is never pinned in `images/pins.cue`, and a digest for the wrong line's package is refused.
Merging this implementation authorizes no dispatch, and no application image has been published for it yet.

| Budget | Ceiling or stop condition |
| --- | --- |
| Duration | Absolute expiry within 300 minutes; begin cleanup 15 minutes before it |
| Cells | At most 20 per session, each with a unique ID that is not `running` or `completed` in the campaign ledger |
| Pods | Four total: JobManager and TaskManager in `tier3-cloudtasks`, Operator and supervisor in `tier3-system` |
| Task creations | Planning bound over the session: each cell's per-creator attempt limit times its subtasks times the four incarnations one JobMaster's fixed-delay strategy allows, at most 12,000,000; the expected count is the cells' record totals, and an attempt limit must at least cover the busiest creator's records (all of them at parallelism 1, nine tenths under skew, an even share otherwise) |
| Queue administration | At most six queue writes per actor (create, pause and delete are the only ones issued, none retried); zero dispatches, checked on every poll |
| Reads | At most 60,000 metered Cloud Tasks, Flink REST and evidence-storage requests per actor, counted one per listing, receipt, part and rewrite; the chunk reads behind a part and the metadata read after a rewrite are not counted separately. A poll costs about ten, a session about 12,000 |
| State | Stop on observed checkpoint state above 1 GiB or 20,000 objects under the current cell |
| Logs | Stop collecting at 100 MiB; a truncated read is recorded, not fatal, because evidence rows travel through storage |
| Durable evidence | 4 GiB per session for the exported rows and receipts; supervisor receipts 256 MiB, runner receipts 32 MiB |
| Incremental cost approval | USD 10 per session, covering compute at the policy rates, the task-creation planning bound at USD 0.40 per million and a USD 0.25 reserve |

The TaskManager shape follows the cell's parallelism class: 1 CPU/4 GiB for parallelism 1, 2 CPU/8 GiB for 4 and 4 CPU/16 GiB for 16, each with one slot per parallel subtask; the JobManager is 1 CPU/2 GiB.
Flink Pods select Spot; the session quota admits two Pods sized for the largest approved class.
The approval embeds the session ceilings and shapes byte for byte from `policy.toml`, the queue name, the target URL, the Flink line and every cell with the SHA-256 of its rendered manifest.

The runner creates `projects/flink-gcp/locations/us-central1/queues/ct1246-RUN_ID` only after a read proves it absent, with its rate limits, retry configuration and one-hour tombstone in the create request, then pauses it and reads back `PAUSED`, the submitted configuration and zero dispatch statistics.
Project IAM does not restrict the queue prefix, so the client is bound to that exact name and refuses any other.
A name the service still tombstones fails either the create or the readback that follows, and with it the run; the name is never changed or retried, and unique run IDs keep the runtime from tombstoning its own names.
The supervisor re-reads the queue on every poll and stops the session on any state change or nonzero dispatch count.
Cleanup pauses and deletes the queue and proves it absent; a remaining queue blocks the idle receipt and retains the environment lock.
Cleanup touches the name only after this run persisted its intent to create it; a run refused because the name already existed leaves that queue alone and records `queue-retained-unowned` when one is present.
A JobManager failover resets the restart budget the creation bound assumes, so the bound is a planning figure: the cell deadline and the paused queue, not the bound, cap what a misbehaving cell can spend.

Each cell is a rendered FlinkDeployment named after the cell, with the approval nonce, scenario and cell annotations, the measurement application's arguments in a fixed order and checkpoint state under `gs://flink-gcp-cloudtasks-benchmark/runs/RUN_ID/cells/CELL_ID/state/`.
The CLI validates the session file's cell vocabulary before rendering, and the CUE delivery refuses duplicate IDs and unknown keys on its output path because the pinned CUE evaluator does not report closedness errors away from that path.
The supervisor verifies every delivered manifest against the approval before touching the cluster, then for each cell in order: rechecks the queue, claims the cell in the campaign ledger, persists the creation intent, creates the deployment, observes it through its owner-verified REST Service until `FINISHED`, a terminal state or the cell deadline, deletes it, waits for its Pods and HA metadata to disappear, and deletes the cell's checkpoint state.
The cell deadline allows ten minutes of startup, four nominal input windows and three minutes of teardown, so an arm that sustains a quarter of the offered rate still finishes its finite input.
A cell whose job fails or exceeds its deadline is recorded as `failed` and the session continues; a cell whose budget no longer fits before cleanup is recorded as `skipped` and never started.
A queue deviation, evidence failure, stop request or lost workload ends the session immediately, and cleanup marks the running cell `interrupted`.
Cleanup checks the persisted cell intent against the inventory on every pass and adopts a deployment that landed after the stop when its nonce and approved manifest match, so a create in flight at the moment of the stop is still removed; the runner's cleanup also waits for such an intent while the supervisor Job is still active, up to the force window.
When the approved manifest cannot be read, the object is neither adopted nor deleted; it is reported as `cell-adoption-refused` and the idle check then retains the lock for an operator.
The first cell is credited with the time since the approval's start, up to its startup allowance, so a session approved with the minimum window still runs it.
The session succeeds only when every approved cell completed.

The campaign ledger `_control/campaigns/CAMPAIGN.json` records each cell's status, run, nonce, attempt count and reason across sessions.
`completed` records that the cell's job finished, its workload was removed and its evidence was exported; the collector's verdict on whether those rows and receipts are complete travels with the export and never reopens the ledger entry.
Admission refuses a session whose cells are already `completed` or `running`; a `failed` or `interrupted` cell can be claimed again by a later reviewed session, and the attempt count records that repetition.
A cell cut short by the session's cleanup deadline rather than its own is recorded as `failed` with the reason `session window`.
A queue read that fails transiently is recorded as `queue-read-unavailable`; the third consecutive failure stops the session, and any readback that shows a resumed or dispatching queue stops it at once.
Mutable control records are outside automatic expiry; the ledger is the durable record that a cell already ran.

Evidence under `runs/RUN_ID/` additionally holds `session.json`, `application.json` as the array of cell manifests, `queue-admitted`, `queue-deviation`, `queue-deleted`, `cell-start`, `cell-status`, `cell-finished` and `cell-skipped` receipts, one `observation` receipt per poll, and a final receipt with the campaign, queue readback, per-cell outcomes, each actor's operation counters, the exported cells and their evidence bytes.

While a cell runs, the supervisor records one `observation` per poll: the job state and timestamps with the JobManager clock offset, checkpoint counts and the latest completed and restored checkpoints with at most two new checkpoint details per poll, the sink and source vertex metrics (including the connector's staged-task, staged-byte and replay-budget gauges when the arm registers them), each TaskManager's heap and garbage-collection figures, the last queue readback, and the Pods' phases, restarts and effective resources.
Metric identifiers are discovered on the first poll and recorded; a staged arm whose gauges are missing is marked `metrics-unavailable`.
The JobManager's clock is compared with the supervisor's only through the offset each observation records, and monotonic values are never compared across processes; the TaskManager wall clock that stamps receipts and rows carries no recorded offset, so the analyzer compares it with the supervisor domain as it is and says so in its report.
A cell whose ID ends in `-k11` is the interrupt control: 60 seconds after its warm-up the supervisor persists an `interrupt-intent` receipt and deletes the TaskManager Pod without a grace period, once, to produce an incarnation without a terminal receipt.

After each cell and before the campaign ledger settles it, the collector reconciles the cell's rows and receipts in the benchmark bucket before anything is copied: every creator registration needs a terminal receipt, part indices are contiguous per incarnation, rows decode and belong to the cell, sequences lie within the source's mapped range, duplicate `(sequence, attempt)` rows within an incarnation and unexplained `ALREADY_EXISTS` outcomes invalidate the cell, and row counts must equal the terminal counts.
The verdict is `complete`, `incomplete`, `restarted` or `invalid` with its reasons; every verdict is exported.
Objects are copied with server-side rewrites into `runs/RUN_ID/cells/CELL_ID/` under the evidence bucket, each verified by size and by a checksum the destination must report, against the bytes streamed during reconciliation, within the session evidence ceiling checked before the first copy; the run record then gains the export and its bytes, and `exported.json` records the verdict, the object hashes and a manifest hash in both buckets.
Only after that marker exists is the cell's benchmark prefix (state, rows, receipts and finally the marker) released, and the release deletes only what the marker authorizes: each evidence object at the generation that was copied and verified, the job's own checkpoint state, and the benchmark copy of the marker.
An object the reconciliation refused, and one replaced after the export read it, are kept and named in `benchmark-evidence-retained`, because evidence that no longer exists cannot reproduce the reason recorded about it.
A cell whose export failed keeps its objects for the bucket's one-day expiry and is listed there too.
An export failure stops the session and leaves the cell running in the ledger, so cleanup marks it interrupted and a later reviewed session may claim it again; an exported cell whose prefix still holds objects blocks the idle receipt.

## SDK transport

The shared Python runtime uses the official Kubernetes client, google-cloud-storage and google-auth.
The lifecycle-tools image, Actions CLI and local tests resolve the `flink-tier3` workspace member dependencies through the shared root `uv.lock`.
Image builds consume a hashed export of that member's third-party dependencies; the CLI runs with `uv run --locked --package flink-tier3 --no-dev flink-tier3`, and the root test group includes the member.
Publish the SDK-equipped image through the existing image workflow and adopt its GAR digest before running this lifecycle.
The supervisor does not install packages at startup.

SDK API retries and automatic credential refresh retries after a rejected request are disabled; failed operations return to the lifecycle's existing recovery and generation-conflict handling.
A generation replaced between a GCS metadata read and its download restarts the complete read, at most five times; only an initial metadata 404 means the object is absent.
Other download failures return immediately.
The GCS SDK's optional background bucket-metadata reads are disabled to preserve the object-only permission boundary.
Kubernetes and GCS use the SDK transports with explicit timeouts and disabled proxy/redirect forwarding.
GCS receives a standard `AuthorizedSession` through the SDK's `_http` constructor hook because its public Client options do not expose those session settings.
There are no custom pool wrappers or response-buffer rewrites.
Generic API responses are no longer capped at 8 MiB; a large response from managed GKE or GCS can therefore consume more memory.
Kubernetes inventories still request 500 items per page and stop above 1,000 items per resource kind and namespace; GCS inventories retain their operation-specific count ceilings.
Pod log requests retain their separate byte limits.
Uploads disable SDK checksum-triggered automatic deletion; explicit lifecycle deletes always carry an observed generation.
The lifecycle retains namespace, UID, resourceVersion, deadline, evidence and cleanup policy.

## Cleanup and recovery

Cleanup requests FlinkDeployment deletion while the Operator is still running.
It records the transitive ownerReference UID graph before deleting parents and persists discovered descendants so a later recovery can recognize orphaned children.
After up to ten minutes of normal cleanup, or when only five minutes remain, it closes smoke quota and may delete only verified UID-owned controllers, Pods, Services and ConfigMaps, then remove the verified FlinkDeployment finalizer.
Every delete carries a UID precondition; patches also test resourceVersion.
It never uses a label or name alone as deletion authority and does not force-delete Pods.

Flink 2.2.1's HA ConfigMaps intentionally lack ownerReferences ([leader election](https://github.com/apache/flink/blob/release-2.2.1/flink-kubernetes/src/main/java/org/apache/flink/kubernetes/kubeclient/resources/KubernetesLeaderElector.java)).
The supervisor observes the run's expected HA metadata but does not adopt it into its deletion graph.
The Operator's normal finalizer is responsible for removing it.
Leftover unowned metadata blocks the final idle check and lock release, requiring an administrator to investigate its provenance before any separately approved repair.

Once owned workload objects are absent, cleanup deletes only generation-checked state objects under the exact run prefix, scales the Operator to zero, verifies its Pods have stopped, and restores the exact original quotas.
Evidence or state export failures mark the run failed while allowing paid workload shutdown to continue.
The supervisor exits with its Job/Pod retained so the external runner can wait for Job completion, attempt the final log export, and delete the temporary Job and source ConfigMap.
The final log attempt is recorded after Job completion and is not a prerequisite for shutdown; missing logs or a failed export keep the evidence outcome failed across retries.
The finalizer requires no workload Pods, PVCs or Flink objects, no non-baseline resources except zero-replica Operator ReplicaSets, and quotas observed back at their original values.
After temporary objects disappear, recovery waits up to three minutes for Operator and quota status to reflect their restored specifications; identity changes, unexpected objects and API failures still fail immediately.
It then changes to the read-only plan identity, checks all three refreshed infrastructure plans under the same retained lock, and changes back to the runner identity to write the immutable result and release control records.
Any nonempty run-finalization plan retains the lock and fails the workflow.

Cancellation of the Actions runner does not cancel the supervisor Job.
A `workflow_run` recovery starts when `Tier-3 run` or `CI` completes, including failure or cancellation.
Completed CI recovery releases a retained plan lock after idle and fresh-plan checks, so a routine superseding PR push does not require a manual recovery dispatch.
Every CI completion starts the recovery checkout, tool setup and runner authentication before checking lock ownership, including executions that immediately return without work.
This adds Actions startup work and an authentication exchange per completion; the catch-all trigger favors recovery coverage over filtering successful executions.
An automatic completion event for an execution that does not own the lock is a no-op.
Recovery concurrency is grouped by source execution, so unrelated CI completions cannot replace a pending lock-holder recovery.
Recovery verifies the repository, original workflow path/event, main SHA, execution attempt and completed status against the GitHub API, then compares the saved lock and approval.
Recovery requests cooperative stop and waits for Job completion when possible, then reconciles cleanup against actual state even if the record already says `cleaned`.
It can clean concurrently with a supervisor; the recovery exercise's two additional operations are guarded by the stop state and object UID/version as described above.
It needs no Pod claim, container termination proof or quarantine delay.
Before quota or scale writes, cleanup reads the resource version and then rechecks the exact environment-lock owner.
A concurrent update retries from a fresh identity/version snapshot, so an old actor cannot use a newer run's foundation version under its old lock.
The runner attempts the final supervisor log after Job completion; a disappeared Pod or missing final log marks evidence incomplete instead of blocking shutdown.
Recovery never admits new work.
For a retained lock, dispatch recovery with the original GitHub Actions execution ID:

```sh
gh workflow run tier3-recover.yaml --repo flink-gcp/flink-connector-gcp --ref main \
  -f source_id=COMPLETED_LOCK_HOLDER_EXECUTION_ID
```

This dispatch authorizes cleanup of that completed execution, not another smoke run.
It also handles a completed infrastructure workflow that retained a lock after cancellation.
Infrastructure recovery verifies idle and records fresh plans; a nonempty infrastructure plan is reported as failure after releasing that infrastructure lock, allowing the existing follow-up PR process to repair drift.
Idle infrastructure recovery permits an installed Operator image to differ from the current main pin after an interrupted upgrade; the refreshed plan reports that drift.
It never applies that plan or retries a stale saved apply artifact.
`Schedule` derives the admission, Job, cleanup and settlement deadlines from the immutable approval.
The runner waits until three minutes before expiry before taking over unfinished cleanup, with a one-minute minimum wait for a late recovery.
All six final-verification `tofu init` and `tofu plan` commands share one nine-minute deadline.
Each command receives the remaining time, allowing an individual refresh to exceed 90 seconds without extending the overall allowance.
The configured serial allowance is 84 minutes: 57 for the initial wait, three for late cleanup, two for state deletion, one each for Operator and final-log grace, three each for temporary objects and idle status, nine for six OpenTofu commands, and five for setup and final receipts.
A regression compares that allowance with the workflow's 85-minute timeout.
Network/controller delays can still exhaust the workflow; timeout retains the lock for completed-execution recovery.
Exhausting the shared plan budget retains the lock and requires investigation before retrying; recovery uses the same total budget.
A timeout preserves available output in the local root/operation log and emits up to 64 KiB from its tail to the workflow log.
The failure names the timed-out operation and retains the lock for recovery.

## Shared infrastructure ownership

All three CI plan/apply roots acquire `_control/environment.json` using `ifGenerationMatch=0` before Kubernetes preflight or OpenTofu init and retain it through their operations and post-apply checks.
Contention waits up to 20 minutes, then fails visibly; the lock has no expiry or age-based takeover.
Each matrix job has its own owner nonce and releases only its own observed generation.
Queued matrix jobs can exhaust that wait while another execution recovers; wait for lock-holder recovery to finish before rerunning a cancelled CI execution.
Cancellation retains a lock until the completed-execution recovery checks it; cancelled applies also support explicit recovery.
The existing per-root OpenTofu state locks still protect state transactions.

Temporary scale/quota patches are outside OpenTofu state and exist only while a lifecycle execution owns the shared lock.
They restore the tracked zero-replica/zero-admission values before infrastructure resumes.
Defer infrastructure PR merges while a smoke run holds the environment lock.
An infrastructure change merged during that window cannot apply under the run lock and makes recovery's current-main plans nonempty; automatic recovery then retains the lock and requires coordinated incident repair.
Raw administrative Kubernetes/OpenTofu commands can bypass this workflow protocol; coordinate them as maintenance and do not run them concurrently with a retained lifecycle lock.
The plan and runtime identities are trusted control writers and can delete the environment lock; its absence alone is not an admission proof.
The runner additionally checks unfinished durable approvals and live inventory.

## Local verification and evidence

`just test-scripts` includes the tests in `tools/tier3/tests/` and runs synthetic fault injection with fake GCS generations, Kubernetes UIDs and time.
`just tier3-check` discovers the lifecycle delivery alongside ordinary runs, renders its bundle, verifies source embedding and normal/Spot separation, and rejects malformed inputs against the shared run policy and pinned schemas.
Neither command creates a cloud workload.
The wheel test installs the built artifact outside the source tree and checks CLI startup and policy/source resources using the test environment's installed SDKs.

Evidence is under `gs://flink-gcp-tier3-evidence/runs/RUN_ID/`:
`approval.json`, `application.json`, `images.json`, per-observation supervisor/runner receipts, and `result.json` after verified idle and empty plans.
Recovery runs additionally retain `upgrade-application.json`, operation intents, phase proofs and scheduling Events.
Their final receipt requires both recovery proofs and full input completion as well as successful cleanup.
Run evidence becomes deletion-eligible after 30 days; mutable `_control/` records have no automatic expiry.
An unsuccessful run can still have an idle result: check both `success` and `idle`, together with the three plan receipts.
After the run, download the complete prefix to a private local evidence directory before its 30-day expiry, for example with `gcloud storage cp --recursive gs://flink-gcp-tier3-evidence/runs/RUN_ID LOCAL_EVIDENCE_DIRECTORY`.
Retain the approval, manifests, digests, phase observations, final receipt and plan output together, record file hashes locally, and publish only a reviewed summary of the measured result.
A Cloud Tasks session's download also holds `cells/CELL_ID/{rows,receipts,exported.json}`; `just tier3-analyze LOCAL_EVIDENCE_DIRECTORY` verifies every exported object against the hash its own marker records, re-runs the reconciliation, selects each cell's observation window, computes unique-record throughput and per-repetition p95, applies the preregistered verdict rules and writes `report.json` and `report.md` into an `analysis` directory beside the run, without contacting any service.
The marker is not signed, so the analysis proves the mirror matches what the session recorded, not that the mirror is the session's.
A cell carries a steady-state result only when the supervisor's outcome for it was `completed`, its evidence reconciles complete and matches its marker, it did not restart, its observed window holds the required checkpoints and, when its ID names a protocol entry, its executed conditions match that entry; anything else is reported with its reason and contributes to no group verdict, capacity decision, or pacing, delay or steady-state acceptance item.
The acceptance items about metric discovery and task-name shape ask only that the cell's evidence was exported and matches its marker, because neither reads a rate or a window.
Calibration acceptance is reported once per campaign and Flink line, because the 1.20.4 session repeats `k01` to `k04` under the same identifiers.
The analyzer's rules are part of the supervisor source bundle, so the approval's `runtime_sha256` covers the protocol pin and the analysis code that will judge the run.
Synthetic validation establishes the control logic; actual WIF/KSA permissions, Autopilot mutation, Spot survival and recovery timing remain measurements for the approved #1311 exercise.

To render a synthetic lifecycle bundle without cloud access, run from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --run-id local-probe --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --expires-at 2026-09-20T00:00:00Z --active-seconds 3300
```

This prints JSON resources for inspection; its synthetic inputs do not authorize admission.
Add `--scenario generic-recovery` to render the recovery payload and both manifests.
For a Cloud Tasks session, add `--scenario cloudtasks --cells-file kubernetes/lifecycle/sessions/example-wiring.toml --application-image IMAGE` with a digest-form image reference; `--expression cellManifests` prints the per-cell manifests instead of the supervisor bundle.
A synthetic digest renders locally but never passes the live registry check that admission performs.
The lifecycle delivery requires package sources from this command or the runner; raw CUE rendering without those inputs is incomplete.
