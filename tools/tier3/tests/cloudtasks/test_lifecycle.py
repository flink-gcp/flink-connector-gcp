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
"""Queue intent, paused admission, release and cleanup over a fake queue."""

import copy
from types import SimpleNamespace

import pytest
from flink_tier3.cloudtasks.lifecycle import CloudTasksLifecycle
from flink_tier3.cloudtasks.queue import QUEUE_CONFIGURATION
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import ApiError, Failure, TransportError, digest
from flink_tier3.model import Phase, RunRecord
from flink_tier3.records import Records
from flink_tier3.resource import require_resources_clean
from flink_tier3.rest import Unsent

from ..test_lifecycle import Store

NOW = 1700000000
TRIAL = Trial("ct-lifecycle", 100, 10)
DEADLINE = NOW + 600


def application(trial=TRIAL):
    return {
        "metadata": {"name": trial.run_id, "namespace": "tier3-cloudtasks"},
        "spec": {"job": {"args": trial.arguments("initial")}},
    }


class Environment:
    """The controller's caller contract, over generation-checked storage."""

    def __init__(self, trial=TRIAL):
        self.approval = SimpleNamespace(
            scenario="cloudtasks-recovery",
            run_id=trial.run_id,
            nonce="a" * 32,
            cloudtasks_plan=trial,
            application_sha256=digest(application(trial)),
        )
        self.actor = "runner"
        self.now = NOW
        self.owner = True
        self.store = Store()
        self.records = Records(self.store, self.approval, lambda: self.now)
        self.store.write(self.records.path, RunRecord("a" * 32).to_dict())
        self.events = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def emit(self, event, payload):
        self.events.append((event, payload))

    def assert_owner(self):
        if not self.owner:
            raise Failure("Environment ownership lost")

    def refresh(self):
        return self.records.read()[0]

    def admission_open(self):
        self.assert_owner()
        record = self.refresh()
        if (
            record.phase not in (Phase.APPROVED, Phase.READY)
            or record.stop_requested
            or record.evidence_failed
        ):
            raise Failure("Admission closed")


class FakeQueue:
    """The service's answers for one queue name, as Cloud Tasks gives them.

    A create on an existing name answers 409, a pause or a delete of a missing
    queue 404 and nothing respectively, and a deadline narrower than the
    caller's is kept so a test can see which one an operation used. `reads`
    scripts the answers of the next gets, before the queue's own state.
    """

    def __init__(self, trial=TRIAL):
        self.trial = trial
        self.name = trial.queue_path
        self.queue = None
        self.calls = []
        self.reads = []
        self.create_error = None
        self.deadline = None

    def with_deadline(self, deadline):
        narrowed = copy.copy(self)
        narrowed.deadline = deadline
        narrowed.parent = self
        return narrowed

    def _root(self):
        return getattr(self, "parent", self)

    def get(self):
        root = self._root()
        root.calls.append(("get", self.deadline))
        if root.reads:
            value = root.reads.pop(0)
            if isinstance(value, Exception):
                raise value
            return copy.deepcopy(value)
        return copy.deepcopy(root.queue)

    def create(self):
        root = self._root()
        root.calls.append(("create", self.deadline))
        if root.create_error:
            raise root.create_error
        if root.queue is not None:
            raise ApiError(409, "POST", "queues")
        root.queue = {
            "name": self.trial.queue_path,
            "state": "RUNNING",
            **QUEUE_CONFIGURATION,
        }
        return copy.deepcopy(root.queue)

    def pause(self):
        root = self._root()
        root.calls.append(("pause", self.deadline))
        if root.queue is None:
            raise ApiError(404, "POST", "pause")
        root.queue["state"] = "PAUSED"

    def delete(self):
        root = self._root()
        root.calls.append(("delete", self.deadline))
        root.queue = None

    def tasks(self, token):
        self._root().calls.append(("tasks", self.deadline))
        return [], None


@pytest.fixture
def env():
    return Environment()


@pytest.fixture
def api():
    return FakeQueue()


def controller(env, api, role="runner"):
    env.actor = role
    return CloudTasksLifecycle(env, api, application())


def admitted(env, api):
    runner = controller(env, api)
    runner.initialize()
    runner.provision(DEADLINE)
    return runner


def operations(api):
    return [name for name, _ in api.calls]


def test_the_controller_binds_to_the_approved_trial_and_application(env, api):
    controller(env, api)
    with pytest.raises(Failure, match="approved application"):
        CloudTasksLifecycle(env, api, {**application(), "metadata": {"name": "x"}})
    other = Trial("ct-other", 100, 10)
    with pytest.raises(Failure, match="approved application"):
        CloudTasksLifecycle(env, FakeQueue(other), application())


def test_admission_records_the_intent_then_creates_pauses_and_reads_back(env, api):
    runner = controller(env, api)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    assert operations(api) == ["get", "create", "pause", "get"]
    # Every admission call is held to the admission deadline.
    assert {deadline for _, deadline in api.calls} == {DEADLINE}
    assert receipt["state"] == "PAUSED"
    assert receipt["tombstoneTtl"] == "3600s"
    state = env.refresh().cloudtasks
    assert state["creation_intent"] is True
    assert state["inflight"] is None
    assert state["queue"] == receipt
    assert env.events[-1][0] == "cloudtasks-queue-admitted"


def test_admission_cannot_be_resumed(env, api):
    runner = admitted(env, api)
    api.calls.clear()
    api.queue = None
    with pytest.raises(Failure, match="cannot be resumed"):
        runner.provision(DEADLINE)
    assert "create" not in operations(api)


def test_initialize_never_replaces_an_intent(env, api):
    runner = admitted(env, api)
    runner.initialize()
    assert env.refresh().cloudtasks["creation_intent"] is True
    # The record write refuses a stop that lands after the admission check.
    other = Environment()
    other.records._change(lambda r: r.set_phase(Phase.CLEANING))
    other.admission_open = lambda: None
    with pytest.raises(Failure, match="admission has stopped"):
        controller(other, FakeQueue()).initialize()
    assert other.refresh().cloudtasks is None


def test_an_existing_queue_is_not_this_runs_to_admit(env, api):
    api.queue = {"name": TRIAL.queue_path, "state": "PAUSED"}
    runner = controller(env, api)
    runner.initialize()
    with pytest.raises(Failure, match="already exists"):
        runner.provision(DEADLINE)
    assert env.refresh().cloudtasks["creation_intent"] is False
    assert "create" not in operations(api)


def test_a_409_after_a_free_read_is_another_partys_queue(env, api):
    runner = controller(env, api)
    runner.initialize()
    # The name read free; the create then finds a queue someone else made.
    api.reads = [None]
    api.queue = {"name": TRIAL.queue_path, "state": "RUNNING", **QUEUE_CONFIGURATION}
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    state = env.refresh().cloudtasks
    assert state["creation_intent"] is False and state["inflight"] is None
    runner.release()
    assert controller(env, api, "supervisor").cleanup(lambda: True, deadline=DEADLINE)
    assert api.queue is not None


@pytest.mark.parametrize("status", [400, 403])
def test_a_refused_create_clears_its_marker_and_intent(env, api, status):
    runner = controller(env, api)
    runner.initialize()
    api.create_error = ApiError(status, "POST", "queues")
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    state = env.refresh().cloudtasks
    assert state["inflight"] is None and state["creation_intent"] is False
    runner.release()


@pytest.mark.parametrize(
    "error",
    [TransportError("timeout"), ApiError(503, "POST", "q"), ApiError(429, "POST", "q")],
)
def test_a_create_that_may_have_landed_keeps_its_marker(env, api, error):
    runner = controller(env, api)
    runner.initialize()
    api.create_error = error
    with pytest.raises(type(error)):
        runner.provision(DEADLINE)
    state = env.refresh().cloudtasks
    assert state["creation_intent"] is True
    assert state["inflight"]["operation"] == "provision"
    with pytest.raises(Failure, match="in flight"):
        runner.release()
    supervisor = controller(env, api, "supervisor")
    with pytest.raises(Failure, match="has not released"):
        supervisor.cleanup(lambda: True, deadline=DEADLINE)


def test_a_settling_readback_waits(env, api):
    runner = controller(env, api)
    runner.initialize()
    running = {"name": TRIAL.queue_path, "state": "RUNNING", **QUEUE_CONFIGURATION}
    # Absent before create, then unreadable, absent and still running.
    api.reads = [None, TransportError("reset"), None, running]
    assert runner.provision(DEADLINE)["state"] == "PAUSED"
    assert ("cloudtasks-queue-settling", {"reads": 3}) in env.events


@pytest.mark.parametrize(
    "readback",
    [
        {"tombstoneTtl": "600s"},
        {"name": TRIAL.queue_path + "-other"},
        {"state": "DISABLED"},
    ],
)
def test_a_deviating_readback_refuses_and_clears_the_marker(env, api, readback):
    runner = controller(env, api)
    runner.initialize()
    paused = {"name": TRIAL.queue_path, "state": "PAUSED", **QUEUE_CONFIGURATION}
    api.reads = [None, {**paused, **readback}]
    with pytest.raises(Failure, match="not the paused configuration"):
        runner.provision(DEADLINE)
    assert env.events[-1][0] == "cloudtasks-queue-deviation"
    # The create returned, so its marker is cleared for cleanup.
    state = env.refresh().cloudtasks
    assert state["creation_intent"] is True and state["inflight"] is None


def test_a_queue_still_running_at_the_deadline_refuses(env, api):
    runner = controller(env, api)
    runner.initialize()
    running = {"name": TRIAL.queue_path, "state": "RUNNING", **QUEUE_CONFIGURATION}
    api.reads = [None] + [running] * 1000
    with pytest.raises(Failure, match="not the paused configuration"):
        runner.provision(NOW + 60)
    assert env.now >= NOW + 60


def test_release_closes_admission_without_stopping_the_run(env, api):
    runner = admitted(env, api)
    runner.release()
    record = env.refresh()
    assert record.cloudtasks["released"] is True
    assert record.cloudtasks["stopped"] is True
    assert record.stop_requested is False
    supervisor = controller(env, api, "supervisor")
    assert supervisor.released() is True
    with pytest.raises(Failure, match="requires the runner"):
        supervisor.release()


def test_no_queue_is_created_after_the_release(env, api):
    runner = controller(env, api)
    runner.initialize()
    runner.release()
    with pytest.raises(Failure, match="admission has stopped"):
        runner.provision(DEADLINE)
    assert "create" not in operations(api)


def test_cleanup_needs_the_release_and_the_barrier_then_deletes(env, api):
    admitted(env, api)
    supervisor = controller(env, api, "supervisor")
    with pytest.raises(Failure, match="has not released"):
        supervisor.cleanup(lambda: True, deadline=DEADLINE)
    controller(env, api).release()
    supervisor = controller(env, api, "supervisor")
    with pytest.raises(Failure, match="not quiescent"):
        supervisor.cleanup(lambda: False, deadline=DEADLINE)
    assert api.queue is not None
    api.calls.clear()
    assert supervisor.cleanup(lambda: True, deadline=NOW + 900) is True
    assert api.calls == [("delete", NOW + 900), ("get", NOW + 900)]
    record = env.refresh()
    assert record.cloudtasks["cleaned"] is True
    assert record.stop_requested is True
    require_resources_clean(record)


def test_cleanup_retries_until_the_deletion_is_visible(env, api):
    admitted(env, api)
    controller(env, api).release()
    supervisor = controller(env, api, "supervisor")
    api.reads = [{"name": TRIAL.queue_path}]
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    assert env.refresh().cloudtasks["cleaned"] is False
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is True


def test_without_a_creation_intent_cleanup_touches_no_queue(env, api):
    runner = controller(env, api)
    runner.initialize()
    runner.release()
    api.queue = {"name": TRIAL.queue_path}
    supervisor = controller(env, api, "supervisor")
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is True
    assert "delete" not in operations(api)
    assert api.queue is not None


def test_listing_tasks_is_the_supervisors(env, api):
    admitted(env, api)
    with pytest.raises(Failure, match="requires the supervisor"):
        controller(env, api).listing(NOW + 60)
    assert controller(env, api, "supervisor").listing(NOW + 60).tasks(None) == (
        [],
        None,
    )
    assert api.calls[-1] == ("tasks", NOW + 60)


def test_a_create_refused_before_sending_leaves_no_marker(env, api):
    """The admission deadline can pass while the marker is being written."""
    runner = controller(env, api)
    runner.initialize()
    api.create_error = Unsent("Cloud Tasks operation deadline exceeded")
    with pytest.raises(Unsent):
        runner.provision(DEADLINE)
    state = env.refresh().cloudtasks
    assert state["inflight"] is None and state["creation_intent"] is False
    runner.release()


def test_a_settle_write_whose_response_was_lost_is_found_applied(env, api):
    runner = controller(env, api)
    runner.initialize()
    change = runner._change
    writes = []

    def lost_once(edit, **kwargs):
        result = change(edit, **kwargs)
        writes.append(edit)
        # The write landed; the caller only hears that it failed.
        if len(writes) == 2:
            raise Failure("conditional update response lost")
        return result

    runner._change = lost_once
    receipt = runner.provision(DEADLINE)
    assert len(writes) == 3
    state = env.refresh().cloudtasks
    assert state["inflight"] is None and state["queue"] == receipt
