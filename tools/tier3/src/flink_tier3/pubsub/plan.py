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
import tomllib
from dataclasses import asdict
from decimal import Decimal

from ..bundle import delivery_digest, package_sources, source_digest
from ..common import Failure, digest, json_bytes, quantity, timestamp, utc, verify_pod
from ..model import Schedule, _hourly
from ..policy import GAR, POD_RESOURCES, POLL, PUBSUB_CEILINGS, SHA
from ..workflow import render
from .access import probe_spec
from .messages import MAX_BATCH, PUBLISH_BATCH, cohort_ranges
from .resources import ResourcePlan
from .traffic import COUNTER_CEILINGS, TrafficLimits

TRIALS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")
ENTRY_POINTS = ("datastream", "table")
WINDOW_SECONDS = PUBSUB_CEILINGS["seconds"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The supervisor's exercise pulls the output subscription at least once per
# poll, empty or not, from admission until cleanup: at most this many polls.
EXERCISE_PULLS = (WINDOW_SECONDS - PUBSUB_CEILINGS["cleanup_seconds"]) // POLL
# When the rates behind the estimate were read from the official pricing
# pages: the basis of the number the owner approves, not an admission deadline.
REVIEWED_AT = "2026-10-10T00:00:00Z"
RESERVE_USD = Decimal("1.00")
FIELDS = {
    "version",
    "trial",
    "entry_point",
    "records_per_subscription",
    "traffic_limits",
}


def validate_trial(value):
    """Validate proposed caps, without asserting feasibility or bill enforcement."""
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise Failure("Pub/Sub trial fields must match the version 4 schema")
    for key, low, high in (
        ("version", 4, 4),
        ("records_per_subscription", 3, 10000),
    ):
        if type(value[key]) is not int or not low <= value[key] <= high:
            raise Failure("Invalid Pub/Sub trial field: " + key)
    if value["trial"] not in TRIALS:
        raise Failure("Unknown Pub/Sub trial")
    if value["entry_point"] not in ENTRY_POINTS:
        raise Failure("Unknown Pub/Sub entry point")
    limits = value["traffic_limits"]
    if not isinstance(limits, dict) or set(limits) != set(COUNTER_CEILINGS):
        raise Failure("Pub/Sub trial requires every traffic counter")
    TrafficLimits(**limits, admit_until=1)


def estimate():
    """Planning estimate for one trial; not a bound on bills or SDK retries.

    It is the same for every trial. It charges all seven Pods of the policy for
    the whole window, the four application slots at the Flink shape and the
    three control slots at the Operator's, which is at least the supervisor's,
    with no Spot or commitment discount. The reserve covers what the trial's
    message and byte ceilings bound: Pub/Sub throughput, Cloud Storage
    operations and storage, Cloud Logging ingestion and the logs and evidence
    leaving Google Cloud, each a fraction of it at those ceilings.
    """
    shapes = [POD_RESOURCES["smoke"]] * 4 + [POD_RESOURCES["operator"]] * 3
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    return compute + RESERVE_USD


def load_trial(path):
    """Read at most 8 KiB, refusing unknown fields and duplicate object keys."""

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate trial field: " + key)
            result[key] = value
        return result

    try:
        with path.open("rb") as stream:
            data = stream.read(8193)
        if len(data) > 8192:
            raise ValueError("Trial input exceeds 8 KiB")
        result = json.loads(data, object_pairs_hook=pairs)
        validate_trial(result)
        return result
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise Failure("Invalid Pub/Sub trial proposal: " + str(error)) from error


def load_reviewed_trial(path):
    """Read a reviewed trial file: the same schema, as TOML beside its licence."""
    try:
        with path.open("rb") as stream:
            trial = tomllib.load(stream)
    # TOML is UTF-8 by definition; other bytes fail to decode before parsing.
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise Failure("Unreadable Pub/Sub trial file") from error
    validate_trial(trial)
    return trial


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
    """Validate one feasible pass and derive its exact logical input domain."""
    validate_trial(trial)
    records = trial["records_per_subscription"]
    cohorts = cohort_ranges(records)
    # The helper's payload has no attributes; this excludes service framing.
    input_bytes = sum(
        len(f"v1|{run_id}|{i}|{n}".encode()) for i in range(2) for n in range(records)
    )
    publish_calls = 2 * sum(
        (c["count"] + PUBLISH_BATCH - 1) // PUBLISH_BATCH for c in cohorts.values()
    )
    minimum_pulls = sum(
        (2 * c["count"] + MAX_BATCH - 1) // MAX_BATCH for c in cohorts.values()
    )
    limits = TrafficLimits(**trial["traffic_limits"], admit_until=1)
    pulls = minimum_pulls + EXERCISE_PULLS
    if (
        limits.input_messages < 2 * records
        or limits.input_bytes < input_bytes
        or limits.publish_calls < publish_calls
        # Every pull reserves its whole batch, even when fewer arrive.
        or limits.output_messages < pulls * MAX_BATCH
        or limits.pull_calls < pulls
        # A nonempty pull also acknowledges; an empty one does not.
        or limits.pubsub_requests < publish_calls + 2 * minimum_pulls + EXERCISE_PULLS
    ):
        raise Failure(
            "Traffic proposal cannot cover even one complete input/output pass"
        )
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
    upgrade phase.
    """
    validate_trial(trial)
    kind = trial["trial"]
    jobs = []
    for parallelism, phase in (
        (1 if kind == "rescale-out" else 2, "initial"),
        (
            1 if kind == "rescale-in" else 2,
            "upgrade" if kind.startswith("rescale-") else "initial",
        ),
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
                    f"--entry-point={trial['entry_point']}",
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
            "Pub/Sub proposal requires a one-hour window and 3420 supervisor seconds"
        )
    schedule = Schedule.for_window(start, end)
    planned_input = input_plan(run_id, trial)
    records = trial["records_per_subscription"]
    limits = TrafficLimits(**trial["traffic_limits"], admit_until=schedule.cleanup_at)
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
        pubsub_entry_point=trial["entry_point"],
        pubsub_probe=probe_argument(resources, schedule),
        pubsub_probe_source=probe_command()[3],
    )
    require_trial_jobs(run_id, trial, initial, recovery)
    for application, (parallelism, _) in zip(
        (initial, recovery), manifest_jobs(run_id, trial)
    ):
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
        for manager, replicas in (("jobManager", 1), ("taskManager", parallelism)):
            if spec[manager]["replicas"] != replicas or spec[manager]["resource"] != {
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
        "runtime_sha256": source_digest(),
        "delivery_sha256": delivery_digest(),
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
        "traffic_limits": asdict(limits),
        "limits": {
            "seconds": WINDOW_SECONDS,
            "cleanup_seconds": PUBSUB_CEILINGS["cleanup_seconds"],
            "pods": PUBSUB_CEILINGS["pods"],
            "pvcs": 0,
        },
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
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
