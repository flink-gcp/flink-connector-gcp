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
"""The shared lifecycle with a Cloud Tasks approval, its queue faked."""

import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
from flink_tier3 import actors
from flink_tier3.cloudtasks import plan
from flink_tier3.cloudtasks.exercise import LISTING_STAGE, CloudTasksExercise
from flink_tier3.cloudtasks.verdict import PAGE_EVENT
from flink_tier3.common import Failure, TransportError
from flink_tier3.exercise import TIME_LIMIT_SECONDS
from flink_tier3.model import Approval, validate_approval
from flink_tier3.policy import (
    CLOUDTASKS,
    CLOUDTASKS_OBSERVATIONS,
    CLOUDTASKS_STATE,
    POD_RESOURCES,
    POLL,
)
from flink_tier3.resource import require_resources_clean
from flink_tier3.rest import Unsent
from flink_tier3.runner import Runner
from flink_tier3.supervisor import RESOURCE_EXERCISES
from flink_tier3.verdict import COMPLETE_EVENT

from ..dispatched import inputs as inputs  # noqa: PLC0414
from ..dispatched import prepared as prepared  # noqa: PLC0414
from ..dispatched import renderer as renderer  # noqa: PLC0414
from ..test_lifecycle import app, lifecycle, prepared_supervisor, rt
from ..test_lifecycle import env as env  # noqa: PLC0414 - shared fixture


class Controller:
    """The queue controller's actor interface; its protocol has its own tests.

    It keeps the controller's refusals: a release with an unresolved call
    marker, and a cleanup before the release or the barrier.
    """

    field = "cloudtasks"
    subject = "Cloud Tasks queue"
    service = "Cloud Tasks"

    def __init__(self, env, trial=None):
        self.env = env
        self.trial = trial
        self.calls = []
        self.release_errors = []
        self.cleanups = 0
        self.readbacks = []
        self.pages = []
        self.page_seconds = 0
        self.task_errors = []
        # How a call the deadline cuts off fails: a timeout before its
        # response, or the deadline met while the response arrives.
        self.overrun = TransportError("timeout")
        # (time, error): every listing call from that time on fails so.
        self.failing = None

    def initialize(self):
        self.calls.append("initialize")

    def provision(self, deadline):
        self.calls.append(("provision", deadline))

    def stop(self):
        self.calls.append("stop")
        self.env.records.request_stop()
        self.env.records._change(lambda r: r.cloudtasks.update(stopped=True))

    def release(self):
        self.calls.append("release")
        if self.release_errors:
            raise self.release_errors.pop(0)
        if self.env.refresh().cloudtasks["inflight"] is not None:
            raise Failure("Cloud Tasks runner call is in flight or unresolved")
        self.env.records._change(
            lambda r: r.cloudtasks.update(released=True, stopped=True)
        )

    def released(self):
        state = self.env.refresh().cloudtasks
        return state["released"] is True and state["inflight"] is None

    def cleanup(self, quiesce, *, deadline):
        assert deadline > self.env.clock()
        self.calls.append("cleanup")
        if not self.released():
            raise Failure("Cloud Tasks runner has not released its authority")
        if quiesce() is not True:
            raise Failure("Cloud Tasks workload is not quiescent")
        self.cleanups += 1
        if self.cleanups < 2:
            return False
        self.env.records._change(lambda r: r.cloudtasks.update(cleaned=True))
        return True

    def listing(self, deadline):
        return SimpleNamespace(
            get=lambda: self.readback(deadline),
            tasks=lambda token=None: self.tasks(token, deadline),
        )

    def readback(self, deadline):
        assert deadline <= self.env.schedule.cleanup_at
        return self.readbacks.pop(0) if self.readbacks else {"state": "PAUSED"}

    def tasks(self, token, deadline):
        """One page of the next scripted listing; a None token starts one."""
        assert deadline <= self.env.schedule.cleanup_at
        if self.env.clock() >= deadline:
            raise Unsent("Cloud Tasks operation deadline exceeded")
        if self.task_errors:
            raise self.task_errors.pop(0)
        if self.failing is not None and self.env.clock() >= self.failing[0]:
            raise self.failing[1]
        # A request is bounded by the deadline, as the transport's timeout is.
        if self.env.clock() + self.page_seconds > deadline:
            self.env.clock.now = deadline
            raise self.overrun
        self.env.clock.now += self.page_seconds
        if token is None:
            self.scripted = self.pages.pop(0)
        index = 0 if token is None else int(token)
        more = index + 1 < len(self.scripted)
        return self.scripted[index], (str(index + 1) if more else None)


def pending(environment, **changes):
    environment.records._change(
        lambda r: setattr(
            r,
            "cloudtasks",
            {
                "stopped": False,
                "creation_intent": True,
                "inflight": None,
                "released": False,
                "queue": {"state": "PAUSED"},
                "cleaned": False,
                **changes,
            },
        )
    )


def running(env):
    supervisor, _ = prepared_supervisor(env)
    environment = supervisor.env
    environment.remember("application", env[0].put(app(env)))
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    pending(environment)
    return environment


def cleaned(environment):
    def edit(record):
        record.stop_requested = True
        record.cloudtasks.update(stopped=True, released=True, cleaned=True)

    environment.records._change(edit)


def finish_job(environment):
    job = environment.root("supervisor")
    job["status"]["succeeded"] = 1
    environment.kube.put(job)


def runner_events(env, name):
    return [
        value[0]["payload"]
        for (_, key), value in env[1].data.items()
        if "/runner/" in key and value[0].get("event") == name
    ]


def test_a_cloudtasks_approval_validates_and_derives_its_trial(prepared):
    environment, _, _, _ = prepared
    value = environment.approval.to_dict()
    validate_approval(value)
    assert "bigquery_trial" not in value and "pubsub_trial" not in value
    trial = Approval.from_dict(value).cloudtasks_plan
    assert (trial.records, trial.records_per_second) == (3600, 10)
    assert trial.run_id == value["run_id"]
    assert environment.approval.application_namespace == CLOUDTASKS
    smoke = Approval.from_dict({**value, "scenario": "smoke"})
    with pytest.raises(Failure, match="not describe a Cloud Tasks trial"):
        assert smoke.cloudtasks_plan is None


def test_admission_quota_is_three_pods_and_state_includes_staging(prepared):
    environment, _, _, _ = prepared
    runner = Runner(environment, resource=Controller(environment))
    runner.cleanup.quota(CLOUDTASKS, "run")
    hard = environment.kube.get("ResourceQuota", CLOUDTASKS, "tier3-idle")["spec"][
        "hard"
    ]
    assert hard["pods"] == "3"
    assert hard["requests.cpu"] == str(3 * int(POD_RESOURCES["smoke"]["cpu"]))
    run_id = environment.approval.run_id
    assert runner.cleanup.state_prefixes() == [
        (f"runs/{run_id}/", CLOUDTASKS_STATE),
        (f".inprogress/{CLOUDTASKS_STATE}/runs/{run_id}/", CLOUDTASKS_STATE),
    ]


def test_the_state_bucket_is_the_one_the_application_writes_to():
    # The exercise reads checkpoints and cleanup deletes state in the policy's
    # bucket; the CUE package names the bucket the job writes them to.
    application = (
        Path(__file__).resolve().parents[4]
        / "kubernetes/pkg/cloudtasks/application.cue"
    ).read_text()
    assert f'"gs://{CLOUDTASKS_STATE}/runs/' in application


def test_the_runner_needs_its_queue_controller_in_its_own_environment(prepared, env):
    environment, application, _, _ = prepared
    with pytest.raises(Failure, match="another runner environment"):
        Runner(environment, resource=Controller(lifecycle(env, Runner).env))
    runner = Runner(environment)
    with pytest.raises(Failure, match="requires its queue controller"):
        runner.start({}, {}, application)


def test_admission_provisions_the_queue_before_the_quota_and_application(
    prepared, monkeypatch
):
    environment, application, _, _ = prepared
    controller = Controller(environment)
    runner = Runner(environment, resource=controller)
    order = []
    monkeypatch.setattr(runner, "create_root", lambda key, _: order.append(key))
    monkeypatch.setattr(runner, "supervisor_open", lambda: None)
    monkeypatch.setattr(
        runner.cleanup, "quota", lambda ns, admission: order.append((ns, admission))
    )
    monkeypatch.setattr(
        runner.cleanup, "scale_operator", lambda n: order.append(("operator", n))
    )
    monkeypatch.setattr(environment, "wait", lambda ready, until: None)
    monkeypatch.setattr(
        runner, "create_application", lambda value: order.append(("app", value))
    )
    monkeypatch.setattr(
        controller, "provision", lambda deadline: order.append(("queue", deadline))
    )
    runner.start({}, {}, application)
    started = environment.schedule.started
    assert order == [
        "config",
        (rt.SYSTEM, "supervisor"),
        "supervisor",
        (rt.SYSTEM, "run"),
        ("operator", 1),
        ("queue", started + CLOUDTASKS_OBSERVATIONS["startup_seconds"]),
        (CLOUDTASKS, "run"),
        ("app", application),
    ]
    assert controller.calls == ["initialize"]


def test_admission_closes_at_the_startup_deadline(prepared):
    environment, _, _, _ = prepared
    runner = Runner(environment, resource=Controller(environment))
    runner.admission_open()
    environment.clock.now = (
        environment.schedule.started + CLOUDTASKS_OBSERVATIONS["startup_seconds"]
    )
    with pytest.raises(Failure, match="Cloud Tasks startup deadline"):
        runner.admission_open()


def test_settlement_releases_the_queue_at_once_without_stopping_the_run(env):
    environment = running(env)
    controller = Controller(environment)
    runner = Runner(environment, resource=controller)
    sleeps = 0

    def advance(seconds):
        nonlocal sleeps
        env[3].sleep(seconds)
        sleeps += 1
        if sleeps == 1:
            assert controller.calls == ["release"]
            assert environment.refresh().cloudtasks["released"] is True
            assert environment.refresh().stop_requested is False
            environment.records.request_stop()
        else:
            cleaned(environment)
            finish_job(environment)

    environment.sleep = advance
    runner.settle()
    assert controller.calls == ["release"]
    assert environment.refresh().idle


def test_a_blocked_release_is_retried_and_reported_once(env):
    environment = running(env)
    environment.records._change(lambda r: setattr(r, "success", True))
    controller = Controller(environment)
    controller.release_errors = [Failure("conflict"), Failure("conflict")]
    runner = Runner(environment, resource=controller)

    def advance(seconds):
        env[3].sleep(seconds)
        if environment.refresh().cloudtasks["released"]:
            cleaned(environment)
            finish_job(environment)

    environment.sleep = advance
    runner.settle()
    assert controller.calls.count("release") == 3
    assert runner_events(env, "cloudtasks-release-blocked") == [{"cause": "conflict"}]
    assert environment.refresh().success


def test_a_release_that_never_lands_keeps_the_lock(env):
    environment = running(env)
    environment.records._change(lambda r: r.cloudtasks.update(inflight={"id": "x"}))
    finish_job(environment)
    runner = Runner(environment, resource=Controller(environment))
    with pytest.raises(Failure, match="Cloud Tasks queue cleanup"):
        runner.settle(request_stop=True)
    assert environment.root("application") is None
    assert not environment.refresh().idle
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


def test_supervisor_cleanup_waits_for_the_release_and_the_barrier(env):
    environment = running(env)
    controller = Controller(environment)
    barriers = []

    def quiesce():
        assert environment.root("application") is None
        barriers.append(env[3]())
        # The first call finds the namespace not yet empty.
        return len(barriers) > 1

    supervisor = rt.Supervisor(environment, resource=controller, quiesce=quiesce)

    def advance(seconds):
        env[3].sleep(seconds)
        if not controller.released():
            controller.release()

    environment.sleep = advance
    supervisor.cleanup.run("test", True)
    assert controller.cleanups == 2
    assert environment.refresh().phase == rt.Phase.CLEANED
    assert environment.refresh().success
    require_resources_clean(environment.refresh())


def test_an_unfinished_admission_waits_for_the_release_before_teardown(prepared):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    environment.records.set_phase(rt.Phase.READY)
    pending(environment)
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    supervisor = rt.Supervisor(
        environment, upgrade, resource=controller, quiesce=lambda: True
    )
    released_at = []

    def advance(seconds):
        environment.clock.sleep(seconds)
        if not released_at:
            assert controller.calls == []
            released_at.append(environment.clock())
            environment.records._change(
                lambda r: r.cloudtasks.update(released=True, stopped=True)
            )

    environment.sleep = advance
    supervisor.conclude("admission failed", False)
    assert released_at
    assert controller.cleanups == 2
    assert environment.refresh().phase == rt.Phase.CLEANED


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: setattr(r, "cloudtasks", False),
        lambda r: r.cloudtasks.update(cleaned=False),
        lambda r: r.cloudtasks.update(stopped=False),
        lambda r: setattr(r, "stop_requested", False),
        lambda r: r.cloudtasks.update(released=False),
        lambda r: r.cloudtasks.update(inflight={"id": "unresolved"}),
    ],
)
def test_an_uncleaned_queue_blocks_every_shared_completion_path(env, mutation):
    runner = lifecycle(env, Runner)
    pending(runner.env)
    cleaned(runner.env)
    runner.env.records._change(mutation)
    {
        "nonce": env[2]["nonce"],
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
    }
    for call in (
        lambda: runner.cleanup.scale_operator(0),
        lambda: runner.env.records.set_phase(rt.Phase.CLEANED),
        lambda: runner.env.records.settled(None),
        lambda: rt.verify_idle(runner.env),
        lambda: runner.finalize(),
    ):
        with pytest.raises(Failure, match="Cloud Tasks queue cleanup"):
            call()


def test_supervision_requires_its_exercise(prepared):
    environment, _, _, _ = prepared
    environment.actor = "supervisor"
    with pytest.raises(Failure, match="queue controller"):
        rt.Supervisor(environment).supervise("pod-uid")


def listing(trial, sequences, per_page=2):
    names = [
        {
            "name": trial.queue_path + "/tasks/" + trial.task_id(n),
            "httpRequest": {
                "body": base64.b64encode(f"{trial.run_id}/{n}".encode()).decode()
            },
        }
        for n in sequences
    ]
    return [names[i : i + per_page] for i in range(0, len(names), per_page)] or [[]]


@pytest.fixture
def exercise(prepared, monkeypatch):
    value, controller, small = built(prepared, monkeypatch)
    # The finished application's transition, which waits for the listing.
    value.persist("complete")
    assert value.stage == LISTING_STAGE
    return value, controller, small


def events(environment, name):
    prefix = f"runs/{environment.approval.run_id}/supervisor/"
    return [
        value["payload"]
        for item in environment.store.objects(prefix)
        if (value := environment.store.read(item["name"])[0])["event"] == name
    ]


def test_a_passing_listing_completes_the_run_as_usable(exercise):
    value, controller, small = exercise
    controller.pages = [listing(small, range(4))]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["stage"] == "complete"
    assert record["verdict"] == "usable"
    assert record["outcomes"]["listing"]["report"]["verdict"] == "pass"
    assert record["outcomes"]["listing"]["pages"] == 2
    pages = events(value.env, PAGE_EVENT)
    assert sorted(item["page"] for item in pages) == [0, 1]
    assert {item["attempt"] for item in pages} == {1}
    assert events(value.env, COMPLETE_EVENT)


def test_a_listing_is_retried_until_it_passes(exercise):
    value, controller, small = exercise
    controller.pages = [listing(small, range(3)), listing(small, range(4))]
    started = value.env.clock()
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["outcomes"]["listing"]["attempt"] == 2
    assert record["verdict"] == "usable"
    assert value.env.clock() - started >= POLL


def test_a_listing_that_keeps_failing_is_the_trials_finding(exercise):
    value, controller, small = exercise
    controller.pages = [listing(small, [0, 1, 2, 2])] * 100
    started = value.env.clock()
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["stage"] == "complete"
    assert record["verdict"] == "inconclusive"
    assert record["reasons"] == ["oracle-not-passed"]
    assert record["outcomes"]["listing"]["report"]["duplicates"] == 1
    # Every 15 seconds within the 600-second window, the last one ending
    # inside it.
    assert record["outcomes"]["listing"]["attempt"] == 40
    assert value.env.clock() - started < CLOUDTASKS_OBSERVATIONS["visibility_seconds"]


def test_a_long_listing_is_not_started_again_past_the_window(exercise):
    """At the trial's size one listing takes minutes; the next must fit."""
    value, controller, small = exercise
    controller.page_seconds = 100
    controller.pages = [listing(small, [0, 1, 2])] * 10
    value.verify_tasks()
    record = value.env.refresh().recovery
    # 200 seconds an attempt: the third would end past the window.
    assert record["outcomes"]["listing"]["attempt"] == 2
    assert record["reasons"] == ["oracle-not-passed"]


def test_a_retry_that_runs_out_of_the_window_keeps_the_last_whole_listing(
    exercise,
):
    value, controller, small = exercise
    controller.page_seconds = 100
    # Two pages, 200 seconds; the retry's ten pages cannot finish in time.
    controller.pages = [
        listing(small, [0, 1, 2]),
        listing(small, range(4), per_page=1) * 3,
    ]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["stage"] == "complete"
    assert record["outcomes"]["listing"]["attempt"] == 1
    assert record["outcomes"]["listing"]["report"]["missing"] == 1
    assert record["reasons"] == ["oracle-not-passed"]


def test_a_listing_near_the_time_limit_still_writes_its_last_attempt(exercise):
    value, controller, small = exercise
    value.env.clock.now = value.deadline - 300
    controller.page_seconds = 100
    controller.pages = [listing(small, [0, 1], per_page=2), listing(small, range(4), 1)]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["stage"] == "complete"
    assert record["outcomes"]["listing"]["attempt"] == 1
    assert value.env.clock() < value.deadline


def test_a_deadline_met_while_a_response_arrives_keeps_the_last_attempt(exercise):
    value, controller, small = exercise
    controller.page_seconds = 100
    controller.overrun = Failure("Cloud Tasks operation deadline exceeded")
    controller.pages = [listing(small, [0, 1, 2]), listing(small, range(4), 1) * 3]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["outcomes"]["listing"]["attempt"] == 1
    assert record["reasons"] == ["oracle-not-passed"]


def test_a_failing_state_check_during_a_retry_is_not_the_window_closing(
    exercise, monkeypatch
):
    value, controller, small = exercise
    controller.pages = [listing(small, [0, 1, 2]), listing(small, range(4))]
    checks = []
    check_open = value.check_open

    def failing_second(*args):
        checks.append(value.env.clock())
        if len(checks) == 2:
            raise TransportError("namespace read reset")
        return check_open(*args)

    monkeypatch.setattr(value, "check_open", failing_second)
    with pytest.raises(TransportError):
        value.verify_tasks()
    assert value.env.refresh().recovery["stage"] == LISTING_STAGE


def test_a_window_that_closes_on_the_first_attempt_records_no_listing(exercise):
    value, controller, small = exercise
    controller.page_seconds = 100
    controller.pages = [listing(small, range(4), 1) * 10]
    with pytest.raises(Failure):
        value.verify_tasks()
    assert value.env.refresh().recovery["stage"] == LISTING_STAGE


def test_a_state_read_the_window_closes_on_is_not_a_listing_call(exercise):
    value, controller, small = exercise
    controller.page_seconds = 30
    # The first attempt is one page, so the second is the first to begin
    # with only one scripted listing left.
    controller.pages = [listing(small, [0, 1, 2], 3), listing(small, range(4))]

    listing_calls = controller.listing

    def timed_out(deadline):
        if len(controller.pages) == 1:
            # The second attempt's state read, started inside the window and
            # timed out after it.
            value.env.clock.now += 600
            raise Failure("storage read timed out")
        return listing_calls(deadline)

    controller.listing = timed_out
    with pytest.raises(Failure, match="storage read timed out"):
        value.verify_tasks()
    assert value.env.refresh().recovery["stage"] == LISTING_STAGE


def test_a_permanent_error_near_the_window_end_is_not_hidden(exercise):
    value, controller, small = exercise
    controller.page_seconds = 30
    controller.pages = [listing(small, [0, 1, 2]), listing(small, range(4), 1) * 10]
    # 30 seconds before the window closes.
    controller.failing = (value.env.clock() + 570, Failure("permission denied"))
    with pytest.raises(Failure, match="permission denied"):
        value.verify_tasks()


def test_a_transient_error_that_outlasts_the_window_keeps_the_last_attempt(exercise):
    value, controller, small = exercise
    controller.page_seconds = 30
    controller.pages = [listing(small, [0, 1, 2]), listing(small, range(4), 1) * 10]
    controller.failing = (value.env.clock() + 570, TransportError("reset"))
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["outcomes"]["listing"]["attempt"] == 1
    assert record["reasons"] == ["oracle-not-passed"]


def test_an_error_that_says_something_is_not_hidden_by_an_earlier_attempt(
    exercise,
):
    value, controller, small = exercise
    controller.pages = [listing(small, [0, 1, 2]), listing(small, range(4))]
    readback = controller.readback
    attempts = []

    def second_refused(deadline):
        attempts.append(deadline)
        # The retry's first page is refused for good.
        if len(attempts) == 2:
            controller.task_errors.append(Failure("permission denied"))
        return readback(deadline)

    controller.readback = second_refused
    with pytest.raises(Failure, match="permission denied"):
        value.verify_tasks()
    assert len(attempts) == 2


def test_a_transient_listing_error_is_retried(exercise):
    value, controller, small = exercise
    controller.task_errors = [TransportError("reset")]
    controller.pages = [listing(small, range(4))]
    value.verify_tasks()
    assert value.env.refresh().recovery["verdict"] == "usable"


def test_a_listing_error_that_says_something_ends_the_listing(exercise):
    value, controller, small = exercise
    controller.task_errors = [Failure("permission denied")]
    controller.pages = [listing(small, range(4))]
    with pytest.raises(Failure, match="permission denied"):
        value.verify_tasks()


def test_a_queue_no_longer_paused_is_not_usable(exercise):
    value, controller, small = exercise
    controller.readbacks = [{"state": "RUNNING"}]
    controller.pages = [listing(small, range(4))]
    value.verify_tasks()
    assert value.env.refresh().recovery["reasons"] == ["queue-not-paused"]


def test_the_listing_needs_the_fault_sequence_and_the_finished_input(
    prepared, monkeypatch
):
    value, _, _ = built(prepared, monkeypatch)
    with pytest.raises(Failure, match="fault sequence and the input"):
        value.verify_tasks()


def test_the_exercise_needs_the_runs_own_controller(prepared, env):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    build = RESOURCE_EXERCISES[environment.approval.scenario]
    with pytest.raises(Failure, match="queue controller"):
        build(environment, upgrade, None)
    other = plan.Trial("ct-other", 100, 10, "sequence")
    with pytest.raises(Failure, match="queue controller"):
        build(environment, upgrade, Controller(environment, other))


def test_the_actors_bind_the_queue_to_the_cleanup_end_and_their_role(prepared):
    environment, application, _, _ = prepared
    controller = actors._controller(environment, application, object(), "runner")
    assert controller.api.deadline == environment.schedule.cleanup_end
    assert controller.trial == environment.approval.cloudtasks_plan
    with pytest.raises(Failure, match="wrong role"):
        actors._controller(environment, application, object(), "supervisor")
    with pytest.raises(Failure, match="belongs to its supervisor"):
        actors.barrier(environment)


def built(prepared, monkeypatch):
    """The exercise the supervisor builds for a four-record sequence trial."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    small = plan.Trial(environment.approval.run_id, 4, 1, "sequence")
    monkeypatch.setattr(
        type(environment.approval),
        "cloudtasks_plan",
        property(lambda self: small),
    )
    controller = Controller(environment, small)
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = RESOURCE_EXERCISES[environment.approval.scenario](
        environment, upgrade, controller
    )
    value.persist("baseline")
    return value, controller, small


def test_the_supervisor_builds_the_exercise_under_the_runs_time_limit(
    prepared, monkeypatch
):
    value, _, _ = built(prepared, monkeypatch)
    assert isinstance(value, CloudTasksExercise)
    assert value.deadline == value.env.schedule.started + TIME_LIMIT_SECONDS


@pytest.mark.parametrize(
    ("sequences", "verdict", "reasons"),
    [
        (range(4), "usable", []),
        ([0, 1, 2, 2], "inconclusive", ["oracle-not-passed"]),
        ([0, 1, 2], "inconclusive", ["oracle-not-passed"]),
    ],
)
def test_a_sequence_run_is_judged_by_its_listing_alone(
    prepared, monkeypatch, sequences, verdict, reasons
):
    """The exactly-once listing decides."""
    value, controller, small = built(prepared, monkeypatch)
    # The finished application's transition, which waits for the listing.
    value.persist("complete")
    assert value.stage == LISTING_STAGE
    controller.pages = [listing(small, sequences)] * 100
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert (record["stage"], record["verdict"]) == ("complete", verdict)
    assert record["reasons"] == reasons


def test_the_sequence_rescales_the_sink_through_its_parallelism_argument(
    prepared, monkeypatch
):
    """The approved upgrade names `--parallelism` once, as the patch needs."""
    value, _, _ = built(prepared, monkeypatch)
    spec = value.sequence_spec(1)
    args = spec["job"]["args"]
    assert args[args.index("--parallelism") + 1] == "1"
    assert (spec["job"]["parallelism"], spec["taskManager"]["replicas"]) == (1, 1)
    # Nothing else of the approved upgrade's arguments moves.
    expected = list(value.upgrade["spec"]["job"]["args"])
    expected[expected.index("--parallelism") + 1] = "1"
    assert args == expected


def test_a_sequence_run_whose_task_carries_another_records_body_is_not_usable(
    prepared, monkeypatch
):
    """Every name listed once is not enough: each body must be its own key."""
    value, controller, small = built(prepared, monkeypatch)
    value.persist("complete")
    pages = listing(small, range(4))
    pages[1][1]["httpRequest"]["body"] = pages[1][0]["httpRequest"]["body"]
    controller.pages = [pages] * 100
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert (record["verdict"], record["reasons"]) == (
        "inconclusive",
        ["oracle-not-passed"],
    )
    assert record["outcomes"]["listing"]["report"]["mismatched"] == 1
