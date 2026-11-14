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
  faults.cue         # Recovery fault kinds and each phase's sizes
  smoke.cue          # Smoke application
  bigquery.cue       # BigQuery trial inputs and applications
  pubsub.cue         # Pub/Sub trial inputs, applications and access probe
  cloudtasks.cue     # Cloud Tasks trial inputs and applications
  bigtable.cue       # Bigtable trial inputs and applications
  spanner.cue        # Spanner trial inputs and applications
  firestore.cue      # Firestore trial inputs and applications
  pubsub-trials/     # Reviewed Pub/Sub trial files
```

The scenario files share `package tier3` with `delivery.cue` and load together as one delivery.
The recovery scenarios take their fault kinds and each phase's sizes from [faults.cue](faults.cue), the copy of the shared exercise's `FAULTS` table that a test holds to it ([#1789](https://github.com/flink-gcp/flink-connector-gcp/issues/1789)).
The runner selects the scenario through its existing input.

The `Tier-3 run` workflow admits one application and returns the environment to its tracked idle state.
Its `scenario` input selects the generic smoke application's ordinary completion (`smoke`, the default) or one connector's recovery trial, as the [scenario table](../README.md#scenarios) lists.
The [flink-tier3 workspace member](../../tools/tier3/pyproject.toml) owns the CLI, bootstrap/schema commands, runner and supervisor.
Its [source package](../../tools/tier3/src/flink_tier3/) uses ordinary imports, with `Supervisor`, `Runner` and `Cleanup` composed through `Environment`.
Each connector's scenario has a subpackage of its own, as the [layout](../README.md#layout-per-connector) describes, and the shared lifecycle and infrastructure modules are at the package root.
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

| Module in `flink_tier3` | Responsibility |
| --- | --- |
| `__init__.py`, `cli.py`, `__main__.py`, `runtime.py` | The package's shared exports, the CLI, its entry point in the projected package, and the supervisor's entrypoint |
| `lifecycle.py` | The Actions frontend: dispatch, which binds a rendered delivery's approval, Job deadline and ConfigMap size limit, then recovery, plans and finalization |
| `workflow.py` | GitHub, CUE and OpenTofu integration for that frontend |
| `campaign.py` | The next attempt of a campaign that one dispatch carries |
| `render.py`, `bundle.py` | The rendered lifecycle delivery, and the package source it projects to the supervisor |
| `proposal.py` | Rendering and checking an offline trial proposal's manifests |
| `policy.py`, `policy.toml` | Reviewed environment names, conservative cost rates, each scenario's window and Pod resources |
| `model.py` | Approval, schedule and typed control-record transitions |
| `records.py` | GCS control CAS, evidence and the shared environment lock |
| `google.py` | Google credentials, standard authorized session and generation-checked GCS operations |
| `kubernetes.py` | Kubernetes SDK calls, API collection mapping, UID/version conditions and log limits |
| `environment.py` | Actor dependencies, identity/admission checks and bounded waits |
| `runner.py` | External admission, settlement and finalization |
| `supervisor.py` | Progress/checkpoint observation and supervision |
| `cleanup.py` | Ownership-aware cleanup and idle verification |
| `exercise.py` | The recovery exercise every scenario builds on: recorded operation intents, the owned savepoint upgrade and rescales, Pod deletion and scheduling evidence, with the measurement windows of the scenarios still under ADR-0165 and the fault sequence of [ADR-0204](../../docs/adr/0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) |
| `metrics.py` | Flink metric primitives and the observation coverage built from them |
| `resource.py`, `actors.py`, `rest.py` | What the scenarios whose runner creates one service resource share: its lifecycle from intent to deletion, the actors that own it, and deadline-bounded REST calls |
| `quiesce.py` | The writer listings and retrying poll that the quiescence barriers share |
| `analyze.py`, `runs.py`, `recovery_analysis.py` | Downloaded run directories and the offline analysis of deployed recovery runs |
| `bootstrap.py`, `chart.py`, `schemas.py` | The idle bootstrap and Operator roots, the checksum-pinned Operator chart, and the generated CRD packages |
| `common.py`, `repository.py` | Shared helpers, and the repository checkout the checkout commands read |
| `<connector>/` | One connector's scenario: its trial, plan, the lifecycle of the resources it creates, exercise, observation, oracles, verdict and analysis |

The policy file is part of the reviewed revision, with no runtime override path.
API collection paths and identity-validation rules stay in Python because they describe implementation contracts.
The smoke workload remains the [source-backed generic smoke application](../apps/smoke/README.md).
Merging this implementation does not authorize a dispatch.

## Execution approval

For a scenario still under ADR-0165, the owner approves the scenario, a unique run ID and the run's cost estimate before dispatching; dispatching the run is that approval, and nothing at run time compares spend against the estimate.
The estimate is `estimated_cost` over the window for smoke, and the scenario's plan estimate for a recovery trial, which its rendered proposal carries.
[ADR-0204](../../docs/adr/0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) drops the estimate and its approval: a scenario moved to it is bounded by its one-hour time limit instead, although a Firestore proposal still carries the scenario's estimate until the last scenario moves.
Only a dispatch on `main` can assume the runner identity and admit work, and the run is pinned to the commit it checks out.
The run workflow can check out another rig commit through `rig_sha`, which must head a branch of this repository; the workflow verifies that from `main` before checking the rig out, so a fork commit reachable through a pull request ref cannot be selected. The workflow and recovery still run from `main`, and the lock owner records the rig commit beside the workflow's.
The window starts at admission rather than at dispatch, so queueing costs the run none of its startup budget: a smoke run and a Pub/Sub trial end 60 minutes after admission, and a BigQuery, Cloud Tasks, Bigtable, Spanner or Firestore trial 90 minutes after it.
The last 15 minutes of each window are reserved for cleanup.
A run under ADR-0204 also stops as abnormal one hour after admission if it has no verdict by then.

Dispatch does not check the node count. On a one-node cluster, kube-dns may preempt the supervisor within seconds as the cluster scales up, as it did to the BigQuery pilot `bq1312-alo-10-a2`; in every observed case that happened about 30 seconds after the supervisor started, before application admission, and an immediate redispatch found the cluster scaled up.
The Job replaces no Pod: a preempted supervisor ends the Job, the runner settles the run, and the owner redispatches it under a new run ID.

Dispatch through the Actions UI or with this command:

```sh
gh workflow run tier3-run.yaml --repo flink-gcp/flink-connector-gcp --ref main \
  -f run_id=APPROVED_RUN_ID
```

Each connector's scenario renders its applications from its own file here in its own namespace.
The Cloud Tasks, Bigtable, Spanner and Firestore runbooks list the tags their file takes in their dispatch sections; the BigQuery and Pub/Sub dispatches derive theirs from the trial they are given, as those runbooks describe.
Every run's inventory lists every application namespace.

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
A create whose response was lost is first settled by the Kubernetes adapter's read-back below, which recognises the object by its labels and annotations with the run's approval nonce; lost-response adoption, which verifies that hash before recording the ConfigMap UID, runs when that read-back could not settle it.
Once a root UID is recorded, cleanup uses that identity without re-adopting the resource against its original submitted fields; server-side template changes therefore cannot block shutdown.
The approval pins the rig commit, the application hash, run nonce, workflow execution, namespace/Operator/quota UIDs, baseline object UIDs and original quotas.
The runner opens enough system quota for one supervisor, then creates its Job, repeating the create only as the Kubernetes mutation rule below allows, with an active deadline calculated from three minutes before expiry.
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
Each recovery exercise also permits the recorded operations its runbook describes.
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
The first exception is a Kubernetes read answered 429, 500, 502, 503 or 504, or left without an answer by a connection reset or aborted before it or a TLS handshake the peer closed, which the adapter repeats up to three times after 1, 2 and 4 seconds: a single 503 from a control-plane blip ended pilot `bq1312-alo-10-a6` after both recoveries had passed, a single 429 ended trial `ct1778-rescale-out-a6` during its JobManager failover, and a single reset connection on the settle's namespace read cut off the campaign after the usable `sp1878-ro-a3`.
A `Retry-After` longer than the scheduled delay replaces it, up to 4 seconds, so the three waits total at most 12 seconds.
A read that timed out is not repeated, since its timeout is already spent against the caller's deadline.
A dropped connection usually fails within moments, but a peer can drop a connection the request has waited on almost as late as its timeouts would end it, 20 seconds to connect and then 20 seconds without data on any read, so a repeated read can take four such attempts and their delays, and a peer that trickles bytes can keep one attempt alive for longer still.
A Kubernetes mutation takes the same schedule and settles an uncertain outcome instead of failing on it ([#1937](https://github.com/flink-gcp/flink-connector-gcp/issues/1937)).
A 429 sends it again, since API Priority and Fairness refused it before the API server processed it.
Any transport failure, or a 500, 502, 503 or 504, may have followed an applied change, so the adapter waits the next delay and reads the object back.
A create whose name reads absent did not land; the object it sent, recognised by its labels and annotations with the run's approval nonce among them, is taken as created; and any other object holding the name is refused.
A patch, whose first operations test the UID and `resourceVersion`, did not land, or changed nothing, while the `resourceVersion` reads unchanged and is taken as applied when a changed one shows every change; otherwise it fails with the 422 the API server answers a failed `test` operation with, so the generation-conflict handling applies unchanged, or with the 404 a patch of a missing object gets when its target is gone.
A delete did not land while an object with its UID reads without a deletion mark; an object that is gone, marked for deletion or replaced under its name is taken as deleted.
A mutation that did not land is sent again after a dropped connection or a 500, 502, 503 or 504, and its failure is raised otherwise: after a timeout, including one while the request body was uploading, because the attempt has already spent its 20-second timeout, and after any other transport failure, such as a refused connection, because a repeat would meet the same failure.
Each read-back is itself a read on the schedule above, so a mutation can wait up to 12 seconds on its own schedule and up to 12 more in each of its read-backs; a read-back that fails is raised chained to the failure it settled.
A repeat answered 409 to a create, 409 or 422 to a patch, or 404 or 409 to a delete may have met the earlier attempt's doing, so it is settled by the same read-back.
Before each repeat, never before a read-back, the adapter reruns the check its caller made before the first attempt: the runner's admission, with its startup deadlines, for the application and root creates, admission for an admission quota or scale patch, lock ownership for a cleanup one, with the clean-services check before the Operator is scaled to zero, and the recovery exercise's stop and deadline check for its upgrade patch.
A failed check is raised chained to the latest failure after which the mutation may have landed, or else to the one the repeat would have followed; a check that failed with a cause of its own, such as a lock read's transport error, keeps it, since the rig's transient-failure classifiers follow causes, and names the Kubernetes failure in a note.
A delete reruns no check: each removes a run-owned object that cleanup removes after a stop as well, whether in cleanup itself, the runner's teardown, the Pub/Sub access probe or the exercise's planned Pod deletion, so a late repeat does nothing cleanup would not.
When the schedule is spent, or a later answer is neither repeated nor settled, the adapter raises the latest failure that left the outcome unknown, chained to that answer, so the lifecycle's own checks of an unanswered Pod delete or upgrade patch still apply.
A dry-run create persists nothing to read back, so it is repeated only on 429, and the supervisor's identity review is repeated as a read.
A Flink REST call through the API server's proxy is not repeated; it is the job's answer, which the recovery windows classify themselves.
The second exception is a GCS read, whether a metadata read, a download or a listing, answered 408, 429, 500, 502, 503 or 504, or left without an answer by a refused, reset or timed-out connection or a token refresh that failed in transport, which the adapter repeats up to three times after the same delays: a single timed-out control-record read ended trial `ct1799-rescale-out-a1` during its JobManager failover. Once a request has spent a full 20-second timeout only its first failure is retried, so a read that timed out is repeated at most once.
A generation replaced between a GCS metadata read and its download restarts the complete read after a random wait, as a lost conditional-update race does, and the twentieth conflicting attempt gives up; only an initial metadata 404 means the object is absent.
Other download failures return immediately.
A GCS write whose request got no answer may have landed, so the adapter does not repeat it; the two writers that make up nearly all of a run's traffic resolve it instead.
A control-record update compares its next read with what it sent and returns on a match, otherwise editing again as after a lost race, and an evidence event, a create of a unique name, is sent again, a 412 then meaning the first attempt landed, at most three times each ([#1869](https://github.com/flink-gcp/flink-connector-gcp/issues/1869)).
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
The finalizer requires no workload Pods, PVCs or Flink objects and no non-baseline resources except zero-replica Operator ReplicaSets in the namespaces the approval recorded, its application namespace and `tier3-system`, and quotas observed back at their original values.
After temporary objects disappear, recovery waits up to three minutes for Operator and quota status to reflect their restored specifications; identity changes, unexpected objects and API failures still fail immediately.
It then writes the immutable result and releases the control records and the lock.
A run proves that it restored what it changed; it does not plan the infrastructure roots, so a change merged while it ran neither fails it nor keeps its lock ([#2115](https://github.com/flink-gcp/flink-connector-gcp/issues/2115)).

Cancellation of the Actions runner does not cancel the supervisor Job.
A `workflow_run` recovery starts when `Tier-3 run` or `CI` completes, including failure or cancellation.
Completed CI recovery releases a retained plan lock after idle and fresh-plan checks, so a routine superseding PR push does not require a manual recovery dispatch.
Every CI completion starts the recovery checkout, tool setup and runner authentication before checking lock ownership, including executions that immediately return without work.
This adds Actions startup work and an authentication exchange per completion; the catch-all trigger favors recovery coverage over filtering successful executions.
An automatic completion event for an execution that does not own the lock is a no-op.
Recovery concurrency is grouped by source execution, so unrelated CI completions cannot replace a pending lock-holder recovery, and a second recovery of the same source waits for the first.
Recovery requires that the source holds the lock and that the GitHub API reports its execution completed, then compares the saved lock and approval.
It reads the approval as written, without checking it against the current policy, so a policy change merged while a run holds the lock does not strand that run's recovery.
Cleanup reads only the namespaces the approval recorded, so a namespace added to the policy since the run started does not count against its baseline.
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

Only runs acquire `_control/environment.json`, using `ifGenerationMatch=0`; the lock has no expiry or age-based takeover.
CI plans and applies take no lock ([#2115](https://github.com/flink-gcp/flink-connector-gcp/issues/2115)); an apply's post-apply idle and drift check is skipped while a run holds the lock, and one a run started during the check fails and is rerun.
A lock an earlier plan or apply left is recovered as before, with refreshed plans.
A run whose `rig_sha` predates that change still needs plans to finalize, so the run workflow refuses it before admission.
The existing per-root OpenTofu state locks still protect state transactions.

Temporary scale/quota patches are outside OpenTofu state and exist only while a lifecycle execution owns the shared lock.
The run restores the tracked zero-replica/zero-admission values before it releases the lock.
The environment lock only keeps two runs apart: a pull request's plan and a merge's apply take no lock and do not require an idle cluster, so infrastructure changes merge at any time.
A `tier3-bootstrap` or `tier3-operator` apply during a trial can reset the quotas the trial raised or, through Helm, the Operator's scale; such a trial fails and is run again.
A plan made during a trial can show the trial's temporary quotas and Operator scale.
Raw administrative Kubernetes/OpenTofu commands can bypass this workflow protocol; coordinate them as maintenance and do not run them concurrently with a retained lifecycle lock.
The runtime identities are trusted control writers and can delete the environment lock; its absence alone is not an admission proof.
The runner additionally checks unfinished durable approvals and live inventory.

## Local verification and evidence

`just test-scripts` includes the tests in `tools/tier3/tests/` and runs synthetic fault injection with fake GCS generations, Kubernetes UIDs and time.
`just tier3-check` renders the lifecycle bundle, verifies source embedding and normal/Spot separation, and rejects malformed inputs against the shared run policy and pinned schemas.
Neither command creates a cloud workload.
The wheel test installs the built artifact outside the source tree and checks CLI startup and policy/source resources using the test environment's installed SDKs.

Evidence is under `gs://flink-gcp-tier3-evidence/runs/RUN_ID/`:
`approval.json`, `application.json`, `images.json`, per-observation supervisor/runner receipts, and `result.json` after verified idle.
A recovery trial additionally retains `upgrade-application.json`, operation intents, its exercise's records and scheduling Events, and what its oracles read, such as a Cloud Tasks trial's task listing pages.
Their final receipt requires the usable verdict their exercise decided as well as successful cleanup.
Run evidence becomes deletion-eligible after 30 days; mutable `_control/` records have no automatic expiry.
An unsuccessful run can still have an idle result: check both `success` and `idle`, together with the three plan receipts.
After the run, download the complete prefix to a private local evidence directory before its 30-day expiry, for example with `gcloud storage cp --recursive gs://flink-gcp-tier3-evidence/runs/RUN_ID LOCAL_EVIDENCE_DIRECTORY`.
Retain the approval, manifests, digests, phase observations, final receipt and plan output together, record file hashes locally, and publish only a reviewed summary of the measured result.
`just tier3-analyze LOCAL_EVIDENCE_DIRECTORY` writes `report.json` and `report.md` into an `analysis` directory beside the run, without contacting any service.
It reports a deployed recovery run's verdict, recomputed from its exported evidence rather than read from its receipt, with the reasons behind it and the verdict the run stored beside it; a stored verdict other than the recomputed one is named as a problem, an evidence set without what the verdict is decided from has no verdict, and a run that simply did not complete is inconclusive.
The analyzer's rules are part of the commit the approval's `sha` names, so the approval still pins the analysis code that will judge the run; the analyzer runs offline from a checkout rather than in the Pod, so it is not part of the delivered set.
Synthetic validation establishes the control logic; WIF/KSA permissions, Autopilot mutation, Spot survival and recovery timing are properties only deployed runs can show.

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
The offline `cloudtasks-recovery` proposal is described in the [Cloud Tasks trial runbook](../apps/cloudtasks/README.md#trial-and-estimate).
The offline `bigtable-recovery` proposal is described in the [Bigtable trial runbook](../apps/bigtable/README.md#trial-and-estimate).
The offline `spanner-recovery` proposal is described in the [Spanner trial runbook](../apps/spanner/README.md#trial-and-estimate).
The offline `firestore-recovery` proposal is described in the [Firestore trial runbook](../apps/firestore/README.md#trial-and-estimate).
The BigQuery approval and its dedicated state bucket are described in [Approval and shared resource policy](../apps/bigquery/README.md#approval-and-shared-resource-policy).
A BigQuery trial is dispatched through the same run workflow, which takes the trial (`alo-10`, `eo-10`, `alo-50`, `eo-50`, `fl-10` or `fl-10-uploader`, or an `eo-10`, `fl-10` or `fl-10-uploader` fault trial) and the published application digest, as [Production dispatch](../apps/bigquery/README.md#production-dispatch) describes.
A Pub/Sub trial is dispatched the same way from a [reviewed trial file](#reviewed-pubsub-trials), as [Approval dispatch](../apps/pubsub/README.md#approval-dispatch) describes.
A Cloud Tasks trial takes only the published application digest, as [Dispatch](../apps/cloudtasks/README.md#dispatch) describes.
A Bigtable trial takes its kind and the published application digest, as [Dispatch](../apps/bigtable/README.md#dispatch) describes.
A Spanner trial takes its kind and the published application digest, as [Dispatch](../apps/spanner/README.md#dispatch) describes, and so does a Firestore trial, as [its Dispatch section](../apps/firestore/README.md#dispatch) describes.
A Cloud Tasks, Bigtable, Spanner or Firestore dispatch can also carry a campaign, the kinds to run after its own and a repeat budget, which dispatches each next attempt once the last is idle, as the [Bigtable dispatch section](../apps/bigtable/README.md#dispatch) describes, which applies to all four.

## Reviewed Pub/Sub trials

A `pubsub-recovery` dispatch names one file under `kubernetes/lifecycle/pubsub-trials/`, without `.toml`, through the run workflow's `pubsub_trial` input.
Each file holds the [offline trial schema](../apps/pubsub/README.md#offline-trial-proposal) as TOML beside the Apache licence header that apache-rat requires.
The dispatched commit must contain the file: the workflow checks that commit out fresh, so no untracked file is there to name.

The directory holds the four trials [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) runs once each: `jm-replacement`, `tm-replacement`, `rescale-out` and `rescale-in`, on the DataStream entry point with 1,000 records per subscription.
It also holds the three [restore-deduplication trials](../apps/pubsub/README.md#restore-deduplication-trials) of [#2054](https://github.com/flink-gcp/flink-connector-gcp/issues/2054), `reactive-scale-out`, `reactive-tm-kill` and `adaptive-scale-in`, each with restore deduplication on, on the DataStream entry point with 10,000 records per subscription.
They use the version 6 schema, which carries no traffic limits; each run is dispatched only after the owner approves its [estimate](../apps/pubsub/README.md#offline-trial-proposal).
A file under that path is a runnable trial, so the example the tests use lives in `tools/tier3/tests/fixtures/pubsub-trials/` instead, where no dispatch can name it.
