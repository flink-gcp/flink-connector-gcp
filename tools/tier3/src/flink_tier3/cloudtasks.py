#
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
"""Cloud Tasks measurement sessions: queue ownership, campaign ledger and cells."""

from __future__ import annotations

import json
import re
import tomllib

import google.auth.exceptions
import requests

from .common import (
    ApiError,
    Failure,
    TransportError,
    contains,
    digest,
    quantity,
    utc,
)
from .model import queue_name, taskmanager_class, validate_cells
from .policy import (
    BENCHMARK,
    CLOUDTASKS,
    CLOUDTASKS_CEILINGS,
    CLOUDTASKS_POLICY,
    FLINK_LINES,
    HTTP_TIMEOUT,
    LABEL,
    NONCE,
    PROJECT,
    REGION,
    RUN_ID,
)
from .records import conditional_update

BASE = "https://cloudtasks.googleapis.com/v2beta3/"
PARENT = f"projects/{PROJECT}/locations/{REGION}"
SCENARIO = "flink-gcp.io/scenario"
CELL = "flink-gcp.io/cell"
SERVICE_ACCOUNT = "cloudtasks-benchmark"
JAR = "local:///opt/flink/usrlib/cloudtasks-measurement.jar"
ENTRY_CLASS = "io.github.flink.gcp.connector.tier3.cloudtasks.CloudTasksMeasurementJob"
# Follows the paused-queue protocol of the accepted #1245 harness: a one-hour
# tombstone matches the staged arms' name retention; dispatch limits are
# irrelevant while paused but keep any unexpected resume slow.
QUEUE_CONFIGURATION = {
    "rateLimits": {"maxDispatchesPerSecond": 1, "maxConcurrentDispatches": 1},
    "retryConfig": {"maxAttempts": 1},
    "tombstoneTtl": "3600s",
}
QUEUE_READ_MASK = "name,state,rateLimits,retryConfig,tombstoneTtl,stats"
QUEUE_POLL_MASK = "name,state,stats"


class Meter:
    """In-memory operation counters checked against the session ceilings."""

    def __init__(self, ceilings=CLOUDTASKS_CEILINGS):
        self.ceilings = ceilings
        self.counts = {"read_ops": 0, "admin_write_ops": 0}

    def tick(self, kind, count=1):
        self.counts[kind] += count
        if self.counts[kind] > self.ceilings[kind]:
            raise Failure(f"Session {kind} ceiling exceeded")

    def snapshot(self):
        return dict(self.counts)


class Queues:
    """REST v2beta3 client bound to exactly one approved queue name."""

    def __init__(self, http, name, meter=None):
        expected = re.compile(
            re.escape(queue_name("")) + RUN_ID.pattern.removesuffix("\\Z") + "\\Z"
        )
        if not expected.fullmatch(name):
            raise Failure("Queue is outside this run's ownership")
        self.http, self.name = http, name
        self.meter = meter or Meter()

    def _call(self, method, path, body=None, read=True):
        self.meter.tick("read_ops" if read else "admin_write_ops")
        try:
            response = self.http.request(
                method,
                BASE + path,
                json=body,
                timeout=HTTP_TIMEOUT,
                allow_redirects=False,
            )
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                f"{method} Cloud Tasks request failed: {type(error).__name__}"
            ) from error
        if response.status_code == 404 and method in ("GET", "DELETE"):
            return None
        if not 200 <= response.status_code < 300:
            raise ApiError(response.status_code, method, path.split("?")[0])
        return response.json() if response.content else {}

    def get(self, read_mask=QUEUE_READ_MASK):
        return self._call("GET", f"{self.name}?readMask={read_mask}")

    def create(self):
        return self._call(
            "POST",
            PARENT + "/queues",
            {"name": self.name, **QUEUE_CONFIGURATION},
            read=False,
        )

    def pause(self):
        return self._call("POST", self.name + ":pause", {}, read=False)

    def delete(self):
        return self._call("DELETE", self.name, read=False) is not None


TRANSIENT_STATUSES = (408, 429, 500, 502, 503, 504)
# A queue is not readable the instant it is created. The service answers a
# transient status or nothing at all while it initializes, and reports the
# state it held before the pause for a moment after it, so admission waits
# for the queue to settle rather than spending a whole session on one read.
QUEUE_SETTLE_SECONDS = 120
QUEUE_SETTLE_POLL = 5
# The only state a queue reports on the way to the pause it was just given.
QUEUE_SETTLING_STATE = "RUNNING"


def transient(error):
    """Whether a failed queue read says nothing about the queue itself."""
    return isinstance(error, TransportError) or (
        isinstance(error, ApiError) and error.status in TRANSIENT_STATUSES
    )


def _count(stats, key):
    value = (stats or {}).get(key)
    return 0 if value in (None, "") else int(value)


def dispatched(readback):
    """Dispatch activity beyond the approved ceiling, which is zero."""
    stats = readback.get("stats") or {}
    observed = max(
        _count(stats, "executedLastMinuteCount"),
        _count(stats, "concurrentDispatchesCount"),
    )
    return observed > CLOUDTASKS_CEILINGS["dispatches"]


def admission_deadline(env):
    """The end of the session's admission allowance, one cell's startup."""
    return env.schedule.started + CLOUDTASKS_POLICY["cell_startup_seconds"]


def admission_budget_open(env):
    """Refuse an admission step taken after the session's own allowance.

    ``Environment.admission_open`` proves the approval, the phase and this
    run's ownership, and knows nothing of this scenario's startup budget.
    The settling windows are bounded by that budget through
    ``settle_deadline``; this is the check the irreversible step carries, so
    a window that ends exactly at the deadline cannot still create a queue.
    """
    if env.clock() >= admission_deadline(env):
        raise Failure("Session admission deadline expired")


def settle_deadline(env, seconds):
    """The end of a settling window, never past what admission may spend.

    Admission is allowed one cell's startup allowance for the whole session,
    so a service that keeps answering transiently must not push the queue's
    creation past that budget on top of it.
    """
    return min(env.clock() + seconds, admission_deadline(env))


def absent_before_create(env, seconds=QUEUE_SETTLE_SECONDS, poll=QUEUE_SETTLE_POLL):
    """Whether the run's queue name is free, retrying a read that says nothing.

    A transient failure here reports on the service, not on the name, and a
    single one would cost the whole session. The read is repeated until it
    answers or the window closes; an answer naming an existing queue still
    refuses the run, because this run would not own it.
    """
    deadline = settle_deadline(env, seconds)
    while True:
        env.admission_open()
        try:
            return env.queues.get(QUEUE_POLL_MASK) is None
        except Failure as error:
            if not transient(error) or env.clock() >= deadline:
                raise
        env.sleep(min(poll, max(0, deadline - env.clock())))


def settled_readback(env, seconds=QUEUE_SETTLE_SECONDS, poll=QUEUE_SETTLE_POLL):
    """The paused readback that admits the queue, once the service settles.

    An absent, unreadable or still-running answer inside the window is the
    queue initializing, not a deviation. A readback that names another queue,
    carries a configuration outside the approved one, or shows any dispatch
    is a deviation whenever it arrives, and never waits.
    """
    deadline = settle_deadline(env, seconds)
    readback, waits = None, 0
    while True:
        env.admission_open()
        try:
            readback = env.queues.get()
        except Failure as error:
            if not transient(error):
                raise
            readback = None
        if readback is not None:
            if (
                readback.get("name") != env.queues.name
                or not contains(readback, QUEUE_CONFIGURATION)
                or dispatched(readback)
                # Only the pre-pause state is a queue still settling. Any
                # other answer, including none at all, is a deviation now.
                or readback.get("state") not in (QUEUE_SETTLING_STATE, "PAUSED")
            ):
                break
            if readback.get("state") == "PAUSED":
                if waits:
                    env.emit("queue-initializing", {"reads": waits})
                return readback
        waits += 1
        if env.clock() >= deadline:
            break
        env.sleep(min(poll, max(0, deadline - env.clock())))
    env.emit("queue-deviation", {"phase": "admission", "readback": readback})
    raise Failure("Queue readback is not the paused approved configuration")


def admit_queue(env):
    """Create the run's queue, pause it, and prove the paused readback."""
    queues = env.queues
    if not absent_before_create(env):
        raise Failure("Queue already exists; this run does not own it")
    # Persist that this run is about to create its queue: cleanup may delete
    # the name only after this intent exists, never a queue it merely found.
    env.records.intend("queue")
    # The settling window may have spent the rest of the admission budget.
    env.admission_open()
    admission_budget_open(env)
    created = queues.create()
    if created.get("name") != queues.name:
        raise Failure("Created queue name differs from the approved name")
    queues.pause()
    readback = settled_readback(env)
    env.emit("queue-admitted", {"readback": readback})
    env.records.set_queue(readback)
    return readback


def verify_queue(env):
    """Re-read the queue and stop on any deviation from paused, zero dispatch."""
    readback = env.queues.get(QUEUE_POLL_MASK)
    if (
        readback is None
        or readback.get("name") != env.queues.name
        or readback.get("state") != "PAUSED"
        or dispatched(readback)
    ):
        env.emit("queue-deviation", {"phase": "session", "readback": readback})
        raise Failure("Paused queue deviated; stopping the session")
    return readback


def release_queue(env):
    """Pause, delete and prove absence of the queue this run created."""
    queues = env.queues
    if not env.records.cache.queue_intent:
        # Admission refused an existing queue or never reached the create;
        # whatever carries the name belongs to someone else and stays untouched.
        if queues.get(QUEUE_POLL_MASK) is not None:
            env.emit("queue-retained-unowned", {"name": queues.name})
        return
    if queues.get(QUEUE_POLL_MASK) is None:
        env.emit("queue-deleted", {"name": queues.name, "observed": "absent"})
        return
    try:
        queues.pause()
    except ApiError as error:
        if error.status not in (400, 404, 409, 412):
            raise
    queues.delete()
    if queues.get(QUEUE_POLL_MASK) is not None:
        raise Failure("Queue remains after deletion")
    env.emit("queue-deleted", {"name": queues.name, "observed": "deleted"})


class Ledger:
    """Campaign-wide cell outcomes in mutable control storage."""

    def __init__(self, store, campaign):
        if not RUN_ID.fullmatch(campaign):
            raise Failure("Invalid campaign identity")
        self.store, self.campaign = store, campaign
        self.path = f"_control/campaigns/{campaign}.json"

    def read(self):
        value, generation = self.store.read(self.path)
        if value is None:
            value = {"campaign": self.campaign, "cells": {}}
        elif value.get("campaign") != self.campaign or "cells" not in value:
            raise Failure("Campaign ledger is not the expected record")
        return value, generation

    def _change(self, edit):
        return conditional_update(self.store, self.path, self.read, edit)

    def admit(self, cell_ids):
        value, _ = self.read()
        for cell_id in cell_ids:
            entry = value["cells"].get(cell_id)
            if entry and entry.get("status") in ("completed", "running"):
                raise Failure(
                    f"Cell {cell_id} is already {entry['status']} in the campaign ledger"
                )

    def claim(self, cell_id, run_id, nonce, at):
        def edit(value):
            entry = value["cells"].get(cell_id) or {}
            if entry.get("status") in ("completed", "running"):
                raise Failure(f"Cell {cell_id} cannot be claimed twice")
            value["cells"][cell_id] = {
                "status": "running",
                "run_id": run_id,
                "nonce": nonce,
                "at": utc(at),
                "attempts": int(entry.get("attempts", 0)) + 1,
            }

        return self._change(edit)

    def settle(self, cell_id, run_id, nonce, status, reason, at):
        if status not in ("completed", "failed", "interrupted"):
            raise Failure("Unknown ledger outcome")

        def edit(value):
            entry = value["cells"].get(cell_id)
            if (
                not entry
                or entry.get("status") != "running"
                or entry.get("run_id") != run_id
                or entry.get("nonce") != nonce
            ):
                raise Failure(f"Cell {cell_id} is not this run's running cell")
            entry.update(status=status, reason=reason, at=utc(at))

        return self._change(edit)

    def interrupt(self, run_id, at, reason="session stopped"):
        def edit(value):
            for entry in value["cells"].values():
                if entry.get("status") == "running" and entry.get("run_id") == run_id:
                    entry.update(status="interrupted", reason=reason, at=utc(at))

        return self._change(edit)


def load_session(path):
    """Read a reviewed session file: a campaign and its ordered cells."""
    try:
        with path.open("rb") as stream:
            session = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise Failure("Unreadable session file") from error
    if set(session) != {"campaign", "cells"} or not isinstance(
        session["campaign"], str
    ):
        raise Failure("Session file must define exactly campaign and cells")
    if not RUN_ID.fullmatch(session["campaign"]):
        raise Failure("Invalid campaign identity")
    validate_cells(session["cells"], manifest=False)
    return session


def cell_arguments(cell, run_id, queue, target):
    """The measurement application's argument list, in the rendered order."""
    return [
        "--run-id",
        run_id,
        "--cell-id",
        cell["id"],
        "--queue",
        queue,
        "--target",
        target,
        "--arm",
        cell["arm"],
        "--body-bytes",
        str(cell["body_bytes"]),
        "--parallelism",
        str(cell["parallelism"]),
        "--concurrency",
        str(cell["concurrency"]),
        "--checkpoint-seconds",
        str(cell["checkpoint_seconds"]),
        "--channel-pool-size",
        str(cell["channel_pool_size"]),
        "--distribution",
        cell["distribution"],
        "--offered-rate",
        str(cell["offered_rate"]),
        "--warmup-seconds",
        str(cell["warmup_seconds"]),
        "--observation-seconds",
        str(cell["observation_seconds"]),
        "--record-limit",
        str(cell["record_limit"]),
        "--attempt-limit",
        str(cell["attempt_limit"]),
        "--control-delay-millis",
        str(cell["control_delay_millis"]),
        "--emit-attempts",
        "true" if cell["emit_attempts"] else "false",
    ]


def state_prefix(run_id, cell_id):
    return f"gs://{BENCHMARK}/runs/{run_id}/cells/{cell_id}/state"


def _same_shape(actual, expected):
    return set(actual or {}) == set(expected) and all(
        quantity(actual[key]) == quantity(value) for key, value in expected.items()
    )


def _verify_component(component, shape, replicas=1):
    if component.get("replicas") != replicas:
        raise Failure("Cell manifest must run exactly one JobManager and TaskManager")
    resource = component.get("resource", {})
    if quantity(resource.get("cpu", "0")) != quantity(shape["cpu"]) or quantity(
        resource.get("memory", "0")
    ) != quantity(shape["memory"]):
        raise Failure("Cell manifest resource differs from the approved shape")
    containers = component.get("podTemplate", {}).get("spec", {}).get("containers")
    if not containers or len(containers) != 1:
        raise Failure("Cell manifest must template exactly one Flink container")
    resources = containers[0].get("resources", {})
    if not _same_shape(resources.get("requests"), shape) or not _same_shape(
        resources.get("limits"), shape
    ):
        raise Failure("Cell manifest container resources differ from the shape")


def validate_cell_manifest(manifest, cell, approval):
    """Prove a rendered FlinkDeployment carries exactly the approved cell."""
    meta = manifest.get("metadata", {})
    spec = manifest.get("spec", {})
    annotations = meta.get("annotations", {})
    if (
        manifest.get("kind") != "FlinkDeployment"
        or meta.get("namespace") != CLOUDTASKS
        or meta.get("name") != cell["id"]
        or meta.get("labels", {}).get(LABEL) != approval.run_id
        or annotations.get(NONCE) != approval.nonce
        or annotations.get(SCENARIO) != "cloudtasks"
        or annotations.get(CELL) != cell["id"]
    ):
        raise Failure("Cell manifest identity differs from the approval")
    package, flink_version = FLINK_LINES[approval.flink_version]
    if (
        spec.get("image") != approval.images["application"]
        or spec.get("flinkVersion") != flink_version
        or spec.get("serviceAccount") != SERVICE_ACCOUNT
    ):
        raise Failure("Cell manifest runtime differs from the approval")
    configuration = spec.get("flinkConfiguration", {})
    prefix = state_prefix(approval.run_id, cell["id"])
    if (
        configuration.get("taskmanager.numberOfTaskSlots") != str(cell["parallelism"])
        or configuration.get("restart-strategy.type") != "fixed-delay"
        or configuration.get("restart-strategy.fixed-delay.attempts")
        != str(CLOUDTASKS_POLICY["restart_attempts"])
        or configuration.get("execution.checkpointing.interval")
        != f"{cell['checkpoint_seconds']} s"
        or configuration.get("execution.checkpointing.dir") != prefix + "/checkpoints"
        or configuration.get("execution.checkpointing.savepoint-dir")
        != prefix + "/savepoints"
        or configuration.get("high-availability.storageDir") != prefix + "/ha"
    ):
        raise Failure("Cell manifest Flink configuration differs from the cell")
    job = spec.get("job", {})
    if (
        job.get("args")
        != cell_arguments(cell, approval.run_id, approval.queue, approval.target)
        or job.get("parallelism") != cell["parallelism"]
        or job.get("upgradeMode") != "stateless"
        or job.get("allowNonRestoredState") is not False
        or job.get("entryClass") != ENTRY_CLASS
        or job.get("jarURI") != JAR
    ):
        raise Failure("Cell manifest job differs from the approved cell")
    shapes = approval.cloudtasks_pod_resources
    _verify_component(spec.get("jobManager", {}), shapes["jobmanager"])
    _verify_component(
        spec.get("taskManager", {}),
        shapes["taskmanager"][taskmanager_class(cell["parallelism"])],
    )
    return package


def load_cells(directory, approval):
    """Load and verify the delivered cell manifests against the approval."""
    manifests = json.loads((directory / "application.json").read_text())
    if not isinstance(manifests, list) or digest(manifests) != (
        approval.application_sha256
    ):
        raise Failure("Supervisor cell manifests differ from approval")
    if len(manifests) != len(approval.cells):
        raise Failure("Cell manifest count differs from the approved cells")
    for manifest, cell in zip(manifests, approval.cells, strict=True):
        if digest(manifest) != cell["manifest_sha256"]:
            raise Failure("Cell manifest differs from its approved pin")
        validate_cell_manifest(manifest, cell, approval)
    return manifests
