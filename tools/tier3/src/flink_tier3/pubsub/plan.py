# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Offline Pub/Sub trial proposals; never authorize execution."""

import copy
import json
import re
from decimal import Decimal

from ..bundle import package_sources
from ..common import Failure, digest, json_bytes, quantity, timestamp, utc, verify_pod
from ..exercise import FAULTS, SEQUENCE_KIND
from ..model import Schedule, _hourly
from ..policy import CLEANUP_SECONDS, GAR, POD_RESOURCES, SHA, WINDOWS
from ..workflow import render
from .access import probe_spec
from .messages import MAX_BATCH, PUBLISH_BATCH, cohort_ranges, publications
from .resources import ResourcePlan
from .verdict import application_pods, sizes

TRIALS = (*FAULTS, SEQUENCE_KIND)
WINDOW_SECONDS = WINDOWS["pubsub-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# When the rates behind the estimate were read from the official pricing
# pages: the basis of the number the owner approves, not an admission deadline.
REVIEWED_AT = "2026-10-10T00:00:00Z"
RESERVE_USD = Decimal("1.00")
# Version 7 removed `entry_point` with the relay's Table entry point (#2240).
VERSION = 7
FIELDS = {
    "version",
    "trial",
    "records_per_subscription",
    "restore_deduplication",
}
# What a single-fault trial runs: 1,000 records per subscription, with
# restore deduplication off, as #1361 ran them.
SINGLE_FAULT_RECORDS = 1000
# The fault sequence's input (ADR-0204): 4,800 records per subscription, which
# the relay reads at ten a second per subtask, 480 seconds of input at
# parallelism two. The earlier sequence runs' inputs began 477 to 677 seconds
# into the window, the faults delayed them by 245 to 292 seconds, and 100 to
# 134 seconds of input had been read when the last fault came, so the input
# outlasts the faults and the run reaches its verdict in about 20.5 to 25.6
# minutes. It runs with restore deduplication on, so that every run also shows
# the source's ledger suppressing nothing it should deliver.
SEQUENCE_RECORDS = 4800
RECORD_DELAY_MILLIS = 100


def trial(kind):
    """The trial a dispatch of ``kind`` runs; nothing else of it is chosen."""
    if kind not in TRIALS:
        raise Failure("Unknown Pub/Sub trial")
    sequence = kind == SEQUENCE_KIND
    return {
        "version": VERSION,
        "trial": kind,
        "records_per_subscription": SEQUENCE_RECORDS
        if sequence
        else SINGLE_FAULT_RECORDS,
        "restore_deduplication": sequence,
    }


def record_delay_millis(kind):
    """How long the relay waits before each record of a ``kind`` trial."""
    return RECORD_DELAY_MILLIS if kind == SEQUENCE_KIND else 0


def validate_trial(value):
    """Validate a trial's kind and input domain."""
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise Failure(f"Pub/Sub trial fields must match the version {VERSION} schema")
    if type(value["version"]) is not int or value["version"] != VERSION:
        raise Failure("Invalid Pub/Sub trial field: version")
    records = value["records_per_subscription"]
    if type(records) is not int or not 3 <= records <= 10000:
        raise Failure("Invalid Pub/Sub trial field: records_per_subscription")
    if value["trial"] not in TRIALS:
        raise Failure("Unknown Pub/Sub trial")
    if type(value["restore_deduplication"]) is not bool:
        raise Failure("Invalid Pub/Sub trial field: restore_deduplication")


def estimate(kind):
    """Planning estimate for one trial; not a bound on bills or SDK retries.

    It charges the Pod slots the admission quotas hold for the whole window:
    the application's at the Flink shape, four for every kind, which runs at
    most two TaskManagers, and the three control slots at the Operator's,
    which is at least the supervisor's, with no Spot or commitment discount. The
    reserve covers what the input domain and the window bound: Pub/Sub
    throughput, Cloud Storage operations and storage, Cloud Logging ingestion
    and the logs and evidence leaving Google Cloud, each a fraction of it at
    the largest input domain and the exercise's pulls over the whole window.
    """
    shapes = [POD_RESOURCES["smoke"]] * application_pods(kind) + [
        POD_RESOURCES["operator"]
    ] * 3
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    return compute + RESERVE_USD


def _pod(pod, role):
    containers = pod.get("containers", [])
    if (
        len(containers) != 1
        or pod.get("initContainers")
        or pod.get("ephemeralContainers")
    ):
        raise Failure("Pub/Sub proposal requires one container per Pod")
    for category in ("requests", "limits"):
        resources = containers[0].get("resources", {}).get(category, {})
        expected = POD_RESOURCES[role]
        if set(resources) != set(expected) or any(
            quantity(resources[k]) != quantity(v) for k, v in expected.items()
        ):
            raise Failure("Rendered Pod differs from the Pub/Sub resource proposal")


def probe_command():
    """The probe Pod's command: the packaged program, passed inline."""
    return ["python3", "-I", "-c", package_sources()["pubsub/probe.py"]]


def probe_argument(plan, schedule):
    """The probe Pod's one argument: what it tests, until when."""
    return json_bytes(probe_spec(plan, schedule)).decode()


def require_probe(probe, plan, schedule, image):
    """The workload access probe runs the packaged program on this run's plan.

    It is a control Pod in the application namespace, shaped and placed as
    the supervisor is, and running as the workload's service account.
    """
    metadata = probe.get("metadata", {}) if isinstance(probe, dict) else {}
    spec = probe.get("spec", {}) if isinstance(probe, dict) else {}
    containers = spec.get("containers") or [{}]
    if (
        metadata.get("name") != plan.run_id + "-access-probe"
        or metadata.get("namespace") != "tier3-pubsub"
        or spec.get("serviceAccountName") != "pubsub"
        or spec.get("restartPolicy") != "Never"
        or containers[0].get("command") != probe_command()
        or containers[0].get("args") != [probe_argument(plan, schedule)]
    ):
        raise Failure("Rendered access probe differs from the Pub/Sub plan")
    verify_pod(probe, "supervisor", image, POD_RESOURCES["supervisor"], spot=False)


def input_plan(run_id, trial):
    """Validate the trial and derive its exact logical input domain."""
    validate_trial(trial)
    records = trial["records_per_subscription"]
    cohorts = cohort_ranges(records)
    # The helper's payload has no attributes; this excludes service framing.
    input_bytes = sum(
        len(f"v1|{run_id}|{i}|{n}".encode()) for i in range(2) for n in range(records)
    )
    publish_calls = len(list(publications(records)))
    return {
        "subscriptions": 2,
        "records_per_subscription": records,
        "cohorts_per_subscription": cohorts,
        "publish_batch_size": PUBLISH_BATCH,
        "pull_batch_size": MAX_BATCH,
        "publish_calls": publish_calls,
        "messages": 2 * records,
        "payload_bytes": input_bytes,
    }


def manifest_jobs(run_id, trial):
    """The (parallelism, job arguments) of the initial and recovery manifests.

    Replacement trials keep both manifests at parallelism two; a rescale trial
    changes one to two or two to one and requires restored state in the
    upgrade phase. The fault sequence's recovery manifest is its savepoint
    upgrade at two, from which the exercise patches its rescales.
    """
    validate_trial(trial)
    kind = trial["trial"]
    initial, recovery = sizes(kind)
    upgraded = kind == SEQUENCE_KIND or FAULTS[kind].rescale is not None
    jobs = []
    for parallelism, phase in (
        (initial, "initial"),
        (recovery, "upgrade" if upgraded else "initial"),
    ):
        jobs.append(
            (
                parallelism,
                [
                    f"--run-id={run_id}",
                    f"--phase={phase}",
                    f"--records-per-subscription={trial['records_per_subscription']}",
                    f"--parallelism={parallelism}",
                    f"--require-restored={str(phase == 'upgrade').lower()}",
                    "--restore-deduplication="
                    + str(trial["restore_deduplication"]).lower(),
                    f"--record-delay-millis={record_delay_millis(kind)}",
                ],
            )
        )
    return tuple(jobs)


def require_trial_jobs(run_id, trial, application, recovery):
    """Refuse pinned manifests whose jobs are not exactly the trial's.

    An approval pins both manifests by digest and carries the trial, but the
    digests alone do not say the manifests run that trial.
    """
    for manifest, (parallelism, args) in zip(
        (application, recovery), manifest_jobs(run_id, trial)
    ):
        spec = manifest.get("spec") if isinstance(manifest, dict) else None
        job = spec.get("job") if isinstance(spec, dict) else None
        if (
            not isinstance(job, dict)
            # bool is an int subclass, and True == 1.
            or type(job.get("parallelism")) is not int
            or job["parallelism"] != parallelism
            or job.get("args") != args
        ):
            raise Failure("Pinned Pub/Sub manifests differ from the approved trial")


def prepare(
    *,
    run_id,
    nonce,
    started_at,
    expires_at,
    active_seconds,
    revision,
    application_image,
    trial,
):
    """Freeze one offline trial and its exact manifests for later review."""
    trial = copy.deepcopy(trial)
    validate_trial(trial)
    resources = ResourcePlan(run_id, nonce)
    if not isinstance(revision, str) or not SHA.fullmatch(revision):
        raise Failure("Pub/Sub proposal requires a full source revision")
    if not isinstance(application_image, str) or not re.fullmatch(
        re.escape(GAR) + r"pubsub-recovery@sha256:[0-9a-f]{64}", application_image
    ):
        raise Failure("Pub/Sub proposal requires its application image by digest")
    start, end = timestamp(started_at), timestamp(expires_at)
    if (
        start != int(start)
        or end - start != WINDOW_SECONDS
        or type(active_seconds) is not int
        or active_seconds != ACTIVE_SECONDS
    ):
        raise Failure(
            "Pub/Sub proposal requires a 90-minute window and 5220 supervisor seconds"
        )
    schedule = Schedule.for_window(start, end)
    planned_input = input_plan(run_id, trial)
    records = trial["records_per_subscription"]
    initial, recovery, delivery = render(
        run_id,
        nonce,
        utc(end),
        active_seconds,
        scenario="pubsub-recovery",
        expression="[application, upgradeApplication, delivery.resources]",
        application_image=application_image,
        pubsub_trial=trial["trial"],
        pubsub_records=records,
        pubsub_restore_deduplication=str(trial["restore_deduplication"]).lower(),
        pubsub_record_delay_millis=record_delay_millis(trial["trial"]),
        pubsub_probe=probe_argument(resources, schedule),
        pubsub_probe_source=probe_command()[3],
    )
    require_trial_jobs(run_id, trial, initial, recovery)
    for application, replicas in zip((initial, recovery), sizes(trial["trial"])):
        metadata = application["metadata"]
        annotations = metadata.get("annotations", {})
        if (
            metadata.get("name") != run_id
            or metadata.get("namespace") != "tier3-pubsub"
            or annotations.get("flink-gcp.io/approval") != nonce
            or annotations.get("flink-gcp.io/expires-at") != utc(end)
        ):
            raise Failure("Rendered application differs from the Pub/Sub identity")
        spec = application["spec"]
        if spec["image"] != application_image:
            raise Failure("Rendered application differs from the Pub/Sub trial")
        _pod(spec["podTemplate"]["spec"], "smoke")
        for manager, count in (("jobManager", 1), ("taskManager", replicas)):
            if spec[manager]["replicas"] != count or spec[manager]["resource"] != {
                "cpu": 1,
                "memory": "2Gi",
            }:
                raise Failure(
                    "Rendered Flink managers differ from the Pub/Sub Pod budget"
                )
            _pod(spec[manager]["podTemplate"]["spec"], "smoke")
    supervisor = delivery["supervisor"]["spec"]
    if (
        supervisor["parallelism"] != 1
        or supervisor["completions"] != 1
        or supervisor["activeDeadlineSeconds"] != active_seconds
    ):
        raise Failure(
            "Rendered supervisor differs from the Pub/Sub schedule or Pod budget"
        )
    _pod(supervisor["template"]["spec"], "supervisor")
    supervisor_image = supervisor["template"]["spec"]["containers"][0]["image"]
    require_probe(delivery["probe"], resources, schedule, supervisor_image)
    data = delivery["config"]["data"]
    if (
        json.loads(data["approval.json"]) != {}
        or json.loads(data["application.json"]) != initial
        or json.loads(data["upgrade-application.json"]) != recovery
    ):
        raise Failure("Pub/Sub delivery must contain the unapproved trial manifests")
    proposal = {
        "kind": "pubsub-trial-proposal",
        "version": 1,
        "approved": False,
        "run_id": run_id,
        "nonce": nonce,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(schedule.cleanup_at),
        "expires_at": utc(end),
        "trial": trial,
        "resources": resources.manifest(),
        "application_sha256": digest(initial),
        "recovery_application_sha256": digest(recovery),
        "supervisor_sha256": digest(delivery["supervisor"]),
        "images": {
            "application": application_image,
            "supervisor": delivery["supervisor"]["spec"]["template"]["spec"][
                "containers"
            ][0]["image"],
        },
        "input": planned_input,
        "limits": {
            "seconds": WINDOW_SECONDS,
            "cleanup_seconds": CLEANUP_SECONDS,
        },
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate(trial["trial"])),
            "reviewed_at": REVIEWED_AT,
            "other_reserve_usd": str(RESERVE_USD),
        },
    }
    data["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "recovery_application": recovery,
        "delivery": delivery,
    }
