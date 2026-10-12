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

```text
lifecycle/
  delivery.cue       # Shared inputs, ConfigMap and supervisor Job
  smoke.cue          # Smoke application
  bigquery.cue       # BigQuery trial inputs and applications
  pubsub.cue         # Pub/Sub trial inputs, applications and access probe
```

The scenario files share `package tier3` with `delivery.cue` and load together as one delivery.
The runner selects the scenario through its existing input.

The `Tier-3 run` workflow admits one application and returns the environment to its tracked idle state.
Its `scenario` input selects the generic smoke application's ordinary completion (`smoke`, the default), a BigQuery recovery trial or a Pub/Sub recovery trial.
The [flink-tier3 workspace member](../../tools/tier3/pyproject.toml) owns the CLI, bootstrap/schema commands, runner and supervisor.
Its [source package](../../tools/tier3/src/flink_tier3/) uses ordinary imports, with `Supervisor`, `Runner` and `Cleanup` composed through `Environment`.
BigQuery and Pub/Sub each have a service subpackage, and the single-host Cloud Tasks rig of [ADR-0162](../../docs/adr/0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) has its own; shared lifecycle and infrastructure modules remain at the package root.
The delivered source inventory uses relative paths, including subdirectories.
ConfigMap keys replace `/` with `.`, and volume projection restores the directories before the supervisor imports them.
The installed `flink-tier3` CLI is the entry point; [workspace instructions](../../tools/README.md) cover building a wheel and selecting a repository checkout.
[delivery.cue](delivery.cue) receives Python modules and `policy.toml` as JSON on standard input and projects them through one immutable ConfigMap.
The Pod runs `python3 -m flink_tier3 supervisor` from that projected directory.
The delivery carries the modules that entrypoint can reach, closed under their own imports, plus the package data they name — not the whole installed package, because otherwise every module any scenario adds is carried by all of them and they share the data ceiling below.
Because the set is closed under imports, a delivery missing a module it needs fails to render instead of running.
Adding a module the entrypoint reaches includes it in the delivery automatically; a module nothing delivered imports is not delivered, and enters the delivery as soon as a delivered module imports it.
The rendered-payload test runs the projected directory, which catches a module missing at import time; a module reached only by a deferred import inside a function is delivered by the walk but not exercised there.
The Pod's command is fixed by this manifest, and `bundle.POD_COMMANDS` names it, because the CLI resolves a command to its module through a computed import that no walk can follow.
Every command but `supervisor` and the offline analyses reads the repository from `repository.ROOT`, which the CLI sets from `--repository`; the CLI does not import the modules those commands use, and the cluster client and root plans live in `lifecycle`, their only caller, so nothing delivered reaches `bootstrap`, `lifecycle` or `schemas`.
Every lifecycle scenario's rendered ConfigMap keeps UTF-8 data below 768 KiB, reserving 256 KiB below the [Kubernetes data ceiling](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/apis/core/validation/validation.go); it also keeps the serialized JSON below 1 MiB.
The data budget counts values as Kubernetes does, excluding the extra quoting and escaping in JSON.
Data headroom and JSON overhead are guarded separately, because JSON quoting and escaping can push the wire form over a 768 KiB guard the data stays under: with the Pub/Sub handoff module a 32-cell Cloud Tasks delivery measured 737,522 data bytes and 789,286 JSON bytes, and after that scenario's removal the smoke delivery measured 526,309 and 553,425.
Third-party dependencies remain preinstalled in the pinned image; package source changes need no image rebuild or startup installation.

| File | Responsibility |
| --- | --- |
| `flink_tier3/policy.toml` | Reviewed environment names, conservative cost rates, each scenario's window and Pod resources |
| `flink_tier3/google.py` | Google credentials, standard authorized session and generation-checked GCS operations |
| `flink_tier3/kubernetes.py` | Kubernetes SDK calls, API collection mapping, UID/version conditions and log limits |
| `flink_tier3/model.py` | Approval, schedule and typed control-record transitions |
| `flink_tier3/records.py` | GCS control CAS, evidence and the shared environment lock |
| `flink_tier3/environment.py` | Actor dependencies, identity/admission checks and bounded waits |
| `flink_tier3/cleanup.py` | Ownership-aware cleanup and idle verification |
| `flink_tier3/supervisor.py` | Progress/checkpoint observation and supervision |
| `flink_tier3/exercise.py` | The recovery exercise base the BigQuery and Pub/Sub exercises share: recorded operation intents, the owned savepoint upgrade and Pod deletion, and scheduling evidence |
| `flink_tier3/runs.py`, `flink_tier3/analyze.py` | Downloaded run directories and the offline analysis of deployed BigQuery and Pub/Sub runs |
| `flink_tier3/cloudtasks/` | The single-host #1246 rig: the pinned protocol, campaign journal and controller, row and receipt reconciliation, and the offline verdict analysis |
| `flink_tier3/bigquery/oracle.py` | Offline BigQuery recovery SQL generation and aggregate completeness/routing/duplicate checks |
| `flink_tier3/bigquery/plan.py` | Offline, unapproved BigQuery trial proposal and initial/upgrade/supervisor bundle |
| `flink_tier3/bigquery/lifecycle.py` | Durable BigQuery intents, query slots, evidence and cleanup after the quiescence barrier |
| `flink_tier3/bigquery/handoff.py` | Query requests and runner release, attached explicitly to common settlement/supervisor cleanup; shared completion guards retain pending BigQuery control |
| `flink_tier3/bigquery/exercise.py` | Warmup, recovery and final query sequencing with explicit actor handoffs, built by the production supervisor entrypoint |
| `flink_tier3/lifecycle.py` | The Actions frontend: dispatch, which binds a rendered delivery's approval, Job deadline and ConfigMap size limit, then recovery, plans and finalization |
| `flink_tier3/bigquery/resources.py` | Internal BigQuery table creation and deletion, query slots and budget, and paginated result operations |
| `flink_tier3/bigquery/actors.py` | The BigQuery runner and supervisor the production entrypoints run, over the shared authorized session |
| `flink_tier3/bigquery/quiesce.py` | The supervisor's quiescence barrier: re-lists writer-capable objects in the application namespace and refuses while any of this run's remains |
| `flink_tier3/bigquery/observe.py`, `bigquery/verdict.py` | Sampling the deployed sink through the job's REST service, and whether a run is a usable measurement |
| `flink_tier3/bigquery/analyze.py` | Offline recomputation of a deployed BigQuery run's verdict from its exported evidence |
| `flink_tier3/pubsub/resources.py` | Internal Pub/Sub topic/subscription ownership, fixed-settings and explicit IAM readback, scoped data-grant installation and partial-work cleanup; not wired into admission |
| `flink_tier3/pubsub/plan.py` | Offline, unapproved Pub/Sub trial, reviewed trial files, finite input cohorts, proposed limits and initial/recovery/supervisor delivery |
| `flink_tier3/pubsub/lifecycle.py` | Internal durable Pub/Sub preparation claim, service/policy observations and cleanup after external quiescence; shared settlement gates, without runnable scenario admission |
| `flink_tier3/pubsub/messages.py` | Internal single-attempt input publication and independent output collection with durable evidence before ACK; caller-owned admission |
| `flink_tier3/pubsub/handoff.py` | Process-owned preparation/message calls, each admitted by its begin marker, and releases attached to common settlement/supervisor cleanup |
| `flink_tier3/pubsub/actors.py`, `pubsub/session.py`, `pubsub/guard.py`, `pubsub/quiesce.py` | The Pub/Sub runner and supervisor, their session's record of unsettled writes, the guard that holds each Pub/Sub request to its deadline, and the namespace-wide cleanup barrier |
| `flink_tier3/pubsub/admission.py`, `pubsub/access.py`, `pubsub/probe.py` | Pub/Sub admission's ordered preparation, each identity's effective-access probe, and the workload probe Pod's program |

The policy file is part of the reviewed revision, with no runtime override path.
API collection paths and identity-validation rules stay in Python because they describe implementation contracts.
The smoke workload remains the [source-backed generic smoke application](../apps/smoke/README.md).
The `generic-recovery` scenario that took it through one savepoint upgrade and one JobManager failover was removed once the [#1311 exercise](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) had run it, on 2026-09-17 in [run 35169364218](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35169364218); the BigQuery and Pub/Sub exercises build on the recovery exercise it introduced.
Merging this implementation does not authorize a dispatch.

## Execution approval

The owner approves the scenario, a unique run ID and the run's cost estimate before dispatching; dispatching the run is that approval, and nothing at run time compares spend against the estimate.
The estimate is `estimated_cost` over the window for smoke, `bigquery_plan.estimate` for a BigQuery trial and `pubsub_plan.estimate` for a Pub/Sub trial, which their rendered proposals carry.
Only a dispatch on `main` can assume the runner identity and admit work, and the run is pinned to the commit it checks out.
The run workflow can check out another rig commit through `rig_sha`, which must head a branch of this repository; the workflow verifies that from `main` before checking the rig out, so a fork commit reachable through a pull request ref cannot be selected. The workflow and recovery still run from `main`, and the lock owner records the rig commit beside the workflow's.
The window starts at admission rather than at dispatch, so queueing costs the run none of its startup budget: a smoke run and a Pub/Sub trial end 60 minutes after admission, and a BigQuery trial 90 minutes after it.
The last 15 minutes of each window are reserved for cleanup.

Dispatch does not check the node count. On a one-node cluster, kube-dns may preempt the supervisor within seconds as the cluster scales up, as it did to the BigQuery pilot `bq1312-alo-10-a2`; in every observed case that happened about 30 seconds after the supervisor started, before application admission, and an immediate redispatch found the cluster scaled up.
The Job replaces no Pod: a preempted supervisor ends the Job, the runner settles the run, and the owner redispatches it under a new run ID.

After approving the estimate, use the Actions UI or this command:

```sh
gh workflow run tier3-run.yaml --repo flink-gcp/flink-connector-gcp --ref main \
  -f run_id=APPROVED_RUN_ID
```

The cluster rig has no Cloud Tasks scenario.
Its measurement session was removed once [ADR-0162](../../docs/adr/0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) moved the assessment to a single VM; the `tier3-cloudtasks` namespace and its workload identity remain for a later recovery scenario ([#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362)), and every run's inventory still lists that namespace.

Run IDs contain lowercase ASCII letters, digits and internal hyphens, with at most 40 characters.
A retained approval without a final idle receipt blocks another run, including reuse of that ID.
Before admitting anything, the runner verifies the dedicated kubeconfig, explicit `gke_flink-gcp_us-central1_flink-tier3` context, GKE-discovered DNS endpoint, both namespace UIDs, original quota UIDs and zero admission, idle workload inventory, and the installed Operator image/resources.
It checks each pinned GAR digest exists and remains at least 24 hours away from seven-day deletion eligibility at the run's expiry.
These checks run before temporary quota changes or Operator scale-up.

## Resource and cost approval

Requests equal limits for every container.
The supervisor excludes Spot through required node affinity and runs in `tier3-system`, independently of the Flink Pods.
Autopilot implements the supervisor's `safe-to-evict` annotation as an extended run time by adding a second node-selector term, `cloud.google.com/extended-duration-pods In [...]`; extended run time is refused to Spot Pods, so the runtime audit accepts that term, and only that one, beside the Spot exclusion.
The Operator uses its tracked normal-capacity template.

| Workload | Namespace | Pods running | Capacity | CPU per Pod | Memory per Pod | Ephemeral storage per Pod |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| JobManager | tier3-smoke | 1 | Normal | 1 | 2 GiB | 1 GiB |
| TaskManager | tier3-smoke | 1 | Spot | 1 | 2 GiB | 1 GiB |
| Operator | tier3-system | 1 (+1 replacement slot) | Normal | 1 | 2 GiB | 1 GiB |
| Supervisor | tier3-system | 1 | Normal | 1 | 2 GiB | 0.125 GiB |

`tier3-system` is sized for a third Pod of the Operator's shape, and nothing runs in it.
A preempted Pod holds its quota until it finishes terminating, so without that slot the Deployment's own replacement is refused.
The Spot namespaces get no such slot on purpose, for a reason of cost rather than of detection: a slot for a replacement TaskManager would raise every application quota by a Pod the run rarely needs.
What a replacement costs the measurement is scenario-specific; for a smoke run an owned replacement that restores the same lineage can finish successfully.

| Budget | Bound |
| --- | --- |
| Duration | 60 minutes from admission; cleanup begins 15 minutes before the end |
| Pods | The temporary ResourceQuotas: two in each namespace, and one slot in `tier3-system` for the Operator's replacement |
| PVCs | Zero, by the same quotas |
| State | Cleanup lists at most 10,000 objects under the run prefix and refuses a larger one |
| Logs | Stop if any 1 MiB read would truncate |
| Cost estimate | USD 0.81 for a full hour, approved before dispatch |

ResourceQuotas enforce aggregate Pod, CPU, memory, ephemeral storage, PVC, Job, CronJob and StatefulSet admission limits, so the rig does not count Pods or PVCs a second time.
Every observation checks each Pod's containers, image digests, effective requests/limits and ownership.
Nothing stops a run on the size of its state or of the evidence it exports: the window bounds how long either grows, the state bucket's one-day expiry bounds what a failed cleanup leaves, and the evidence bucket's 30-day expiry bounds what a run retains.
Evidence export uses conditional creates; a failed export stops admission and records its operation and cause, as [Cleanup and recovery](#cleanup-and-recovery) describes.

The conservative cost model charges all 4 CPUs at USD 0.10 per CPU-hour, all 8 GiB memory at USD 0.02 per GiB-hour, and all 3.125 GiB ephemeral storage at USD 0.001 per GiB-hour, even for Spot Pods.
Including USD 0.25 for the bounded storage, requests and telemetry gives USD 0.813125 for a full hour.
The replacement slot is not charged, because it is quota rather than a running Pod; a Pod that occupied it for a whole hour would add USD 0.141 and reach USD 0.954125.
Those rates exceed the [published Iowa Autopilot prices](https://cloud.google.com/kubernetes-engine/pricing), checked on 2026-09-14; that date is recorded as the estimate's basis, and a run is not refused for its age.
This is an execution budget model, not a real-time billing meter or a cloud billing cutoff.
The standing cluster fee and existing persistent infrastructure are outside incremental cost.
An API outage, stuck finalizer or unavailable node can delay verified termination beyond expiry; the workflow reports failure, retains the lock and requires incident recovery rather than claiming a cost guarantee it cannot enforce.

## Admission and supervision

The runner writes a conditional-generation environment lock, immutable approval/application/image receipts, and a mutable run record before it admits the supervisor.
The ConfigMap creation intent retains a hash of its data alongside the submitted metadata, so each heartbeat CAS does not copy the entire source bundle.
Lost-response adoption verifies that hash before recording the ConfigMap UID.
Once a root UID is recorded, cleanup uses that identity without re-adopting the resource against its original submitted fields; server-side template changes therefore cannot block shutdown.
The approval pins the rig commit, the application hash, run nonce, workflow execution, namespace/Operator/quota UIDs, baseline object UIDs and original quotas.
The runner opens enough system quota for one supervisor, then creates its Job once, without retrying the create, with an active deadline calculated from three minutes before expiry.
The supervisor waits for its recorded Job UID, verifies its Pod belongs to that Job, and reports its heartbeat while waiting for the runner to complete admission.
The runner requires that heartbeat and a running, UID-owned supervisor Pod before opening full quotas, scaling the Operator to one, waiting for Operator readiness and creating the fixed FlinkDeployment.
It persists the application UID and changes the control phase to `running` only after admission completes.
While the phase is `approved` or `ready`, the supervisor reports its heartbeat without auditing an application whose creation or UID publication may still be in flight.
If supervision fails in that phase, including on a signal, it requests stop and exits; the runner settles after its last admission call returns.
The Job's `backoffLimit` is zero, so a supervisor Pod that fails or is taken away ends the Job, and the runner stops admitting once the Job has ended.
A lost runner before that handoff requires completed-execution recovery, so admission-stage shutdown depends on that recovery path.
Once `running` is published, admission makes no further Kubernetes writes and the supervisor can clean independently of the runner.
Each quota/scale admission retry rechecks stop and phase as well as identity and lock ownership.
The Operator's Scale API can omit `spec.replicas` when it is zero; lifecycle reads that absence as zero and uses JSON Patch `add` to set either an absent or existing replica count.
The scale patch retains UID and resource-version conditions, and cleanup skips the write when the Operator is already at zero.

For ordinary smoke, the supervisor observes the application and writes Kubernetes only toward idle.
The BigQuery and Pub/Sub recovery exercises also permit the recorded operations their runbooks describe.
Both supervisor and recovery use `Cleanup`; concurrent cleanup repeats the same UID-checked deletes and original quota/replica values.
Generation conflicts merge roots and observations without replacing recorded identities, and phase transitions never return from cleanup to admission.
The supervisor uses the projected Kubernetes service-account token against the in-cluster API and Google metadata credentials for GCS.
The external runner uses ADC/WIF against the verified DNS endpoint; Kubernetes service-account tokens are not sent to that endpoint.
The Google SDK requests both `cloud-platform` and `userinfo.email` OAuth scopes, matching the [GKE authentication plugin](https://github.com/kubernetes/cloud-provider-gcp/blob/master/cmd/gke-gcloud-auth-plugin/default_credentials_token_provider.go).
The email scope lets GKE identify the impersonated service account by the email bound in Kubernetes RBAC; without it, the numeric account ID can produce a `403` even when kubectl preflight succeeds.
The supervisor records a heartbeat, inventory, Pod logs and Flink checkpoint observations.
Every log read retains up to 1 MiB: the first the startup history, later ones what arrived since the previous observation, which can still be a whole startup when the first read came before the container printed it.
A read that reaches that ceiling fails the run instead of discarding possible lineage evidence.
Every measurement Pod carries `cluster-autoscaler.kubernetes.io/safe-to-evict: false`.
Autopilot's autoscaler repacks Pods off an under-used node, and the end of one Cloud Tasks measurement cell left exactly that; measured on 2026-09-20, two JobManagers were replaced two and five minutes after the previous cell's Pods went away, restarting jobs that were otherwise healthy and invalidating both cells.
The annotation is the supported way to say a Pod must not be moved for consolidation; it does not stop a `system-cluster-critical` preemption or a Spot reclamation, which no workload setting can.
The JobManager also runs on normal capacity while the TaskManagers stay on Spot, because losing it restarts the job rather than costing slots.

A Pod that went away between the inventory listing and its log read has that read retired and recorded as `retired-pod-log-unavailable`; a 404 for a Pod that is still there, under the same UID and without a deletion timestamp, still fails the run.
Deciding that needs a second read of the Pod, so a transient status on that read is recorded as `pod-presence-unavailable` and tolerated for two consecutive polls rather than being read as proof the Pod is alive; the third fails the run, as does a refusal that answers about the caller rather than about the Pod.
That tolerance is what lets a preempted Pod be replaced mid-run, and it is also why an application Pod deleted from outside the run is not detected by the log read alone.
Ordinary smoke success requires the job to finish, smoke progress to establish its lineage, and at least one completed checkpoint to have been observed through the UID-owned REST service.
Progress from another run/phase or a changed lineage fails the run, including an unexpected fresh start following disruption.
Failure, cancellation, an evidence failure, a log read reaching its ceiling or the test deadline enters cleanup.
In ordinary smoke, a transient checkpoint-proxy error also starts cleanup, including a JobManager restart while its Service has no endpoints.

## SDK transport

The shared Python runtime uses the official Kubernetes client, google-cloud-storage and google-auth.
The lifecycle-tools image, Actions CLI and local tests resolve the `flink-tier3` workspace member dependencies through the shared root `uv.lock`.
Image builds consume a hashed export of that member's third-party dependencies; the CLI runs with `uv run --locked --package flink-tier3 --no-dev flink-tier3`, and the root test group includes the member.
Publish the SDK-equipped image through the existing image workflow and adopt its GAR digest before running this lifecycle.
The supervisor does not install packages at startup.

SDK API retries and automatic credential refresh retries after a rejected request are disabled; failed operations return to the lifecycle's existing recovery and generation-conflict handling.
The one exception is a Kubernetes read answered 500, 502, 503 or 504, which the adapter repeats up to three times after 1, 2 and 4 seconds: a single 503 from a control-plane blip ended pilot `bq1312-alo-10-a6` after both recoveries had passed. A mutation, a read that timed out, and a Flink REST read through the API server's proxy are not repeated; the last is the job's answer, which the recovery windows classify themselves.
A generation replaced between a GCS metadata read and its download restarts the complete read after a random wait, as a lost conditional-update race does, and the twentieth conflicting attempt gives up; only an initial metadata 404 means the object is absent.
Other download failures return immediately.
The GCS SDK's optional background bucket-metadata reads are disabled to preserve the object-only permission boundary.
Kubernetes and GCS use the SDK transports with explicit timeouts and disabled proxy/redirect forwarding.
GCS receives a standard `AuthorizedSession` through the SDK's `_http` constructor hook because its public Client options do not expose those session settings.
There are no custom pool wrappers or response-buffer rewrites.
Generic API responses are not capped at 8 MiB; a large response from managed GKE or GCS can therefore consume more than 8 MiB of memory.
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
The supervisor prints nothing on its normal path, so an empty `supervisor-final-log` is expected; its standard output carries only a failed run's traceback and one `evidence-failed` line per evidence failure.

Whatever sets the evidence failure also records why: the control record's `evidence_failure` keeps the first failure's operation, actor, time and cause, and the final receipt copies it beside a `success` of `false`.
The operation names the write that failed: `evidence:<event>` for an exported observation, `observe`, `remember-probe`, `request-stop`, `begin-cleanup` or `record-cleaned` for a cleanup write, `pubsub-message-evidence` for a Pub/Sub message upload, and `supervisor-final-log` for the runner's export.
Each actor's first failure is also exported as an `evidence-failed` event, unless that failure was itself an exported observation's; the event is best effort because the evidence write may be what failed, and every failure is printed on the actor's standard output.
A cleaned-record write that fails leaves the cause only in that event and on the supervisor's standard output.
Read the receipt's `evidence_failure` first, then the `evidence-failed` events under the actor's receipts.
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
Recovery concurrency is grouped by source execution, so unrelated CI completions cannot replace a pending lock-holder recovery, and a second recovery of the same source waits for the first.
Recovery requires that the source holds the lock and that the GitHub API reports its execution completed, then compares the saved lock and approval.
It reads the approval as written, without checking it against the current policy, so a policy change merged while a run holds the lock does not strand that run's recovery.
Recovery requests cooperative stop and waits for Job completion when possible, then reconciles cleanup against actual state even if the record already says `cleaned`.
It can clean concurrently with a supervisor; a recovery exercise's operations are guarded by the stop state and object UID/version.
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

### Stranded environment lock

Finalization writes the receipt `runs/<run-id>/result.json`, deletes the run control record at the generation it derived the receipt from, and only then releases the environment lock.
A failure between the deletion and the release leaves the lock held with no control record, and the run workflow fails.
The recovery that the run's completion starts writes a minimal cleaned record before settling and finalizing again, and the retry keeps the receipt it finds; the dispatch above repeats it for the same execution ID.
A missing record without a receipt was never created, since only finalization deletes one and writes the receipt first, and no admission precedes that creation.
The write creates a new object, so a record that reappeared since recovery read it is never replaced.

A retry checks only that the receipt names this run's nonce and an idle environment; it does not rederive the receipt from the restored record, which no longer holds what the receipt was derived from.
A record changed after its receipt was derived fails the conditional deletion and keeps the lock, and the retry then keeps the receipt.
A recovery's own settlement failure after the receipt was written no longer fails that recovery: it is recorded and exported as the recovery's evidence, and the receipt keeps the verdict it was written with.
Deleting the environment lock by hand strands any run record recovery already wrote, because recovery then stops at "Environment has no retained lock" while that record refuses every later acquisition.

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
BigQuery and Pub/Sub trials additionally retain `upgrade-application.json`, operation intents, phase proofs and scheduling Events.
Their final receipt requires the usable verdict their exercise decided as well as successful cleanup.
Run evidence becomes deletion-eligible after 30 days; mutable `_control/` records have no automatic expiry.
An unsuccessful run can still have an idle result: check both `success` and `idle`, together with the three plan receipts.
After the run, download the complete prefix to a private local evidence directory before its 30-day expiry, for example with `gcloud storage cp --recursive gs://flink-gcp-tier3-evidence/runs/RUN_ID LOCAL_EVIDENCE_DIRECTORY`.
Retain the approval, manifests, digests, phase observations, final receipt and plan output together, record file hashes locally, and publish only a reviewed summary of the measured result.
`just tier3-analyze LOCAL_EVIDENCE_DIRECTORY` writes `report.json` and `report.md` into an `analysis` directory beside the run, without contacting any service.
It reports a deployed BigQuery or Pub/Sub run's verdict, recomputed from its exported evidence rather than read from its receipt, with the reasons behind it and the verdict the run stored beside it; a stored verdict other than the recomputed one is named as a problem, an evidence set without what the verdict is decided from has no verdict, and a run that simply did not complete is inconclusive.
The analyzer's rules are part of the commit the approval's `sha` names, so the approval still pins the analysis code that will judge the run; the analyzer runs offline from a checkout rather than in the Pod, so it is not part of the delivered set.
Synthetic validation establishes the control logic; WIF/KSA permissions, Autopilot mutation, Spot survival and recovery timing are properties only deployed runs such as the [#1311 exercise](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) can show.

To render a synthetic lifecycle bundle without cloud access, run from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --run-id local-probe --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --expires-at 2026-09-20T00:00:00Z --active-seconds 3300
```

This prints JSON resources for inspection; its synthetic inputs do not authorize admission.
The lifecycle delivery requires package sources from this command or the runner; raw CUE rendering without those inputs is incomplete.

The offline `bigquery-recovery` proposal is described in the [BigQuery proposal runbook](../apps/bigquery/README.md#offline-execution-proposal); it produces an explicitly unapproved bundle.
The offline `pubsub-recovery` proposal is described in the [Pub/Sub proposal runbook](../apps/pubsub/README.md#offline-trial-proposal).
The BigQuery approval and its dedicated state bucket are described in [Approval and shared resource policy](../apps/bigquery/README.md#approval-and-shared-resource-policy).
A BigQuery trial is dispatched through the same run workflow, which takes the trial (`alo-10`, `eo-10`, `alo-50`, `eo-50` or `fl-10`) and the published application digest, as [Production dispatch](../apps/bigquery/README.md#production-dispatch) describes.
A Pub/Sub trial is dispatched the same way from a [reviewed trial file](#reviewed-pubsub-trials), as [Approval dispatch](../apps/pubsub/README.md#approval-dispatch) describes.

## Reviewed Pub/Sub trials

A `pubsub-recovery` dispatch names one file under `kubernetes/lifecycle/pubsub-trials/`, without `.toml`, through the run workflow's `pubsub_trial` input.
Each file holds the [offline trial schema](../apps/pubsub/README.md#offline-trial-proposal) as TOML beside the Apache licence header that apache-rat requires.
The dispatched commit must contain the file: the workflow checks that commit out fresh, so no untracked file is there to name.

The directory holds the four trials [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) runs once each: `jm-replacement`, `tm-replacement`, `rescale-out` and `rescale-in`, on the DataStream entry point with 1,000 records per subscription.
They use the version 5 schema, which carries no traffic limits; each run is dispatched only after the owner approves its [estimate](../apps/pubsub/README.md#offline-trial-proposal).
A file under that path is a runnable trial, so the example the tests use lives in `tools/tier3/tests/fixtures/pubsub-trials/` instead, where no dispatch can name it.
