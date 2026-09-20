# Generic Tier-3 smoke application

This internal application prepares the stateful workload for [issue #1309](https://github.com/flink-gcp/flink-connector-gcp/issues/1309).
It uses Flink 2.2.1 on Java 17 and is built only through the `tier3-smoke` Maven profile.
It is not a published connector artifact.

## Build and inspect

From the repository root, run:

```sh
mise x -- just tier3-smoke-verify
```

The recipe builds `target/smoke.jar` and copies the unchanged Apache Datagen JAR into `target/image-lib/` under this directory.
It runs operator-state tests and local MiniCluster checkpoint/savepoint recovery tests without GCP credentials or Kubernetes access.
The application JAR has a fixed archive timestamp so the same sources and JDK can reproduce its bytes.
The separate Datagen JAR retains its upstream license and notice; Flink runtime classes are provided by the base image.

The [image publication workflow](../../../.github/workflows/tier3-images.yaml) runs the same recipe, takes its Flink base from the reviewed CUE image pin, and builds this directory's Dockerfile for AMD64.
Both JARs are copied into `/opt/flink/usrlib/`; the application entry point is `io.github.flink.gcp.connector.tier3.smoke.SmokeJob` and its URI is `local:///opt/flink/usrlib/smoke.jar`.
`ENABLE_BUILT_IN_PLUGINS` enables the bundled GCS plugin on both managers.
Startup needs no Maven downloads or public image registry access.

## Input and recovery evidence

| Argument | Meaning |
| --- | --- |
| `--run-id` | Required Tier-3 run label; must match the run whose state is restored |
| `--phase` | `initial` or `upgrade`; defaults to `initial` and identifies the deployment phase in evidence |
| `--records` | Total deterministic input count, from 1 to 18,000; defaults to 18,000 |
| `--records-per-second` | Positive finite generation rate, at most 10; defaults to 10 |
| `--require-restored` | `true` rejects a fresh start without checkpoint/savepoint state; defaults to `false` |

One source subtask generates the sequence from zero to the configured count minus one.
Four keys retain their next expected sequence value in Flink keyed state.
A duplicate, missing or reordered value within a key fails the job with the key, expected value and actual value.
The source and verifier use fixed UIDs, and the verifier's operator state retains the run ID, lineage UUID and processed count.
Restoring another run's state, a missing lineage or multiple verifier state entries fails initialization.
The smoke job fixes its source and verifier parallelism and maximum parallelism at one; it does not test rescaling.

The output emits `event=smoke-progress` on the first verified record of each attempt, every 100 processed records and the last input record.
Each line contains `run_id`, `phase`, `lineage`, `restored`, `processed` and `sequence` fields.
The verifier also logs `event=smoke-snapshot` with the checkpoint ID and processed count at the barrier, and `event=smoke-checkpoint-complete` when Flink notifies it of completion.
An observed snapshot is not evidence that a checkpoint completed.
Progress logs can repeat after recovery and are not a transactional output sink.

A recovery oracle compares lineage before and after the disruption, requires `restored=true`, checks that the recovered checkpoint covered nonzero progress, and observes continued sequence validation and a later completed checkpoint.
The lifecycle runner must also inspect Flink job/checkpoint status and reject a terminal failure or an unexpected fresh lineage.
`--require-restored=true` protects the upgrade delivery from accidentally starting it as a new job; an initial delivery can recover after a JM failure and will report that restoration.
Local tests cover state continuity and the missing-state rejection, not GKE scheduling, IAM or Operator reconciliation.

## Deployment and storage

`pkg/smoke.#Application` in the [CUE module](../../README.md) accepts `run.id`, `run.image` and `run.phase` and exposes the FlinkDeployment as `resource`.
Only a `smoke@sha256:...` reference in the existing GAR repository is accepted.
The committed [`initial`](../../runs/generic-smoke/initial/delivery.cue) and [`upgrade`](../../runs/generic-smoke/upgrade/delivery.cue) deliveries select the published `images.smoke` digest and share their [run inputs](../../runs/generic-smoke/common.cue).
They are sequential updates of one deployment, not two applications to run together.
The upgrade changes the phase and requires restored state while retaining the savepoint upgrade mode and `allowNonRestoredState=false`.

The planned run ID is `smoke-1309-20260916`, with expiry `2026-09-16T16:30:00Z` (September 17, 01:30 JST), for execution within two to three days of September 14.
Render each phase independently from the repository root:

```sh
mise x -- just tier3-render runs/generic-smoke/initial > /tmp/smoke-initial.yaml
mise x -- just tier3-render runs/generic-smoke/upgrade > /tmp/smoke-upgrade.yaml
```

Rendering supplies no execution authorization and does not contact the cluster.
The expiry is an admission and supervision input, not a scheduled start or an automatic stop implemented by these manifests.
If execution is postponed beyond this window, review a fresh run ID and deadline and recheck live image availability and retention before admission.
Do not reuse this run ID for a separate experiment: it also identifies the recovery state in GCS.

The definition requests one JM and one TM, each with 1 CPU and 2 GiB, one TM slot and parallelism one.
The common run policy selects AMD64 Spot nodes and adds the run label and expiry annotation.
The main containers request and limit 1 GiB of ephemeral storage; no PVC is used.
Checkpoints run every 30 seconds, with at most one in flight and two retained checkpoints; Kubernetes HA stores its metadata in GCS.
The application restart strategy permits three retries with a ten-second delay.
The separately selected [generic recovery lifecycle](../../lifecycle/README.md#generic-recovery-exercise) renders its own initial/upgrade inputs with 12,000 records at ten per second, leaving time for both planned disruptions inside the same execution window.
The committed ordinary deliveries above retain their 18,000-record input.

The GCP root defines `flink-gcp-tier3-smoke` in `us-central1` as a STANDARD bucket with uniform access and public access prevention.
The workload GSA has bucket-scoped `roles/storage.objectUser`, without bucket administration, object IAM changes or connector data permissions.
Bootstrap annotates `tier3-smoke/smoke` for this GSA; the GCP root owns its impersonation trust.
The installer and publisher identities are not runtime identities.

Storage paths are `gs://flink-gcp-tier3-smoke/runs/<run-id>/checkpoints`, `/savepoints` and `/ha`.
All paths are below the existing bucket; the application never creates a bucket.
Object lifecycle deletion becomes eligible after one day; soft delete and versioning are disabled.
These paths preserve state across Pod replacement within that retention window, not permanent evidence.
The lifecycle stage must export evidence before cleanup and keep the run within the storage lifetime.

## Publication and execution boundary

Merge the reviewed application/identity change and verify the GCP and bootstrap CI applies and empty refreshed plans before publishing.
Review the exact new bucket/GSA and the publication scope before approving those operations.
Publication uses the existing serialized, main-only workflow with its 30-minute timeout and seven-day GAR cleanup policy.
After a successful publication, verify the recorded smoke digest in GAR and adopt it with concrete run IDs and expiries in a separate reviewed delivery change.
Do not insert a stand-in digest or create `ci.cue` to make a delivery render before publication.

The [successful smoke publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34768916308) built main commit `053e23835782059830f871ca53f90fba43efa324` after successful GCP/bootstrap applies and empty refreshed plans.
A GAR read confirmed the digest that [pins.cue](../../images/pins.cue) selected until the Cloud Tasks measurement publication republished the smoke image from a later commit; both committed phases consume whichever digest that pin names, without command-line tags.

This preparation leaves the Operator at zero replicas and both namespaces' Pod/PVC quotas at zero.
[Issue #1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310) supplies admission, expiry/failure supervision and cleanup; [issue #1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) supplies separately approved GKE execution.
Neither consuming all input nor a bucket lifecycle rule terminates the Kubernetes resources.
