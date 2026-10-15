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

import copy
from types import SimpleNamespace

import pytest
from flink_tier3.cloudtasks import actors, plan
from flink_tier3.cloudtasks.exercise import LISTING_STAGE, CloudTasksExercise
from flink_tier3.cloudtasks.lifecycle import require_cloudtasks_clean
from flink_tier3.cloudtasks.queue import Unsent
from flink_tier3.cloudtasks.verdict import (
    COMPLETE_EVENT,
    MEASUREMENT_EVENT,
    OBSERVED,
    PAGE_EVENT,
    SAMPLED_STAGES,
)
from flink_tier3.common import Failure, TransportError, digest
from flink_tier3.environment import Environment
from flink_tier3.model import APPROVAL_VERSION, Approval, validate_approval
from flink_tier3.policy import (
    CLOUDTASKS,
    CLOUDTASKS_OBSERVATIONS,
    CLOUDTASKS_STATE,
    POD_RESOURCES,
    POLL,
)
from flink_tier3.runner import Runner

from ..test_lifecycle import app, lifecycle, prepared_supervisor, rt
from ..test_lifecycle import env as env  # noqa: PLC0414 - shared fixture
from .test_recovery_plan import inputs as inputs  # noqa: PLC0414
from .test_recovery_plan import renderer as renderer  # noqa: PLC0414


@pytest.fixture
def prepared(env, inputs, renderer):
    """A dispatched Cloud Tasks approval over the shared fake cluster."""
    kube, store, approval, clock = env
    inputs.update(run_id=approval["run_id"], nonce=approval["nonce"])
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    clock.now = rt.timestamp(inputs["started_at"])
    application = bundle["application"]
    application.update(apiVersion="flink.apache.org/v1beta1", kind="FlinkDeployment")
    upgrade = copy.deepcopy(application)
    upgrade["spec"]["job"]["args"] = bundle["upgrade_application"]["spec"]["job"][
        "args"
    ]
    saved = approval["namespaces"].pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=CLOUDTASKS, uid=CLOUDTASKS + "-quota")
    kube.put(quota)
    saved.update(uid=CLOUDTASKS + "-uid", quota_uid=CLOUDTASKS + "-quota")
    approval["namespaces"][CLOUDTASKS] = saved
    approval.update(
        version=APPROVAL_VERSION,
        scenario="cloudtasks-recovery",
        started_at=proposal["started_at"],
        expires_at=proposal["expires_at"],
        cleanup_at=proposal["cleanup_at"],
        cloudtasks_trial=plan.trial("jm-replacement"),
        application_sha256=digest(application),
        upgrade_application_sha256=digest(upgrade),
    )
    approval["images"].pop("smoke")
    approval["images"]["application"] = inputs["application_image"]
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    return environment, application, upgrade, proposal


class Controller:
    """The queue controller's actor interface; its protocol has its own tests.

    It keeps the controller's refusals: a release with an unresolved call
    marker, and a cleanup before the release or the barrier.
    """

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
    assert (trial.records, trial.records_per_second) == (180000, 100)
    assert trial.run_id == value["run_id"]
    assert environment.approval.application_namespace == CLOUDTASKS
    smoke = Approval.from_dict({**value, "scenario": "smoke"})
    with pytest.raises(Failure, match="not describe a Cloud Tasks trial"):
        assert smoke.cloudtasks_plan is None


def test_admission_quota_is_three_pods_and_state_includes_staging(prepared):
    environment, _, _, _ = prepared
    runner = Runner(environment, cloudtasks=Controller(environment))
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


def test_the_runner_needs_its_queue_controller_in_its_own_environment(prepared, env):
    environment, application, _, _ = prepared
    with pytest.raises(Failure, match="another runner environment"):
        Runner(environment, cloudtasks=Controller(lifecycle(env, Runner).env))
    runner = Runner(environment)
    with pytest.raises(Failure, match="requires its queue controller"):
        runner.start({}, {}, application)


def test_admission_provisions_the_queue_before_the_quota_and_application(
    prepared, monkeypatch
):
    environment, application, _, _ = prepared
    controller = Controller(environment)
    runner = Runner(environment, cloudtasks=controller)
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
    runner = Runner(environment, cloudtasks=Controller(environment))
    runner.admission_open()
    environment.clock.now = (
        environment.schedule.started + CLOUDTASKS_OBSERVATIONS["startup_seconds"]
    )
    with pytest.raises(Failure, match="Cloud Tasks startup deadline"):
        runner.admission_open()


def test_settlement_releases_the_queue_at_once_without_stopping_the_run(env):
    environment = running(env)
    controller = Controller(environment)
    runner = Runner(environment, cloudtasks=controller)
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
    runner = Runner(environment, cloudtasks=controller)

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
    runner = Runner(environment, cloudtasks=Controller(environment))
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

    supervisor = rt.Supervisor(environment, cloudtasks=controller, quiesce=quiesce)

    def advance(seconds):
        env[3].sleep(seconds)
        if not controller.released():
            controller.release()

    environment.sleep = advance
    supervisor.cleanup.run("test", True)
    assert controller.cleanups == 2
    assert environment.refresh().phase == rt.Phase.CLEANED
    assert environment.refresh().success
    require_cloudtasks_clean(environment.refresh())


def test_an_unfinished_admission_waits_for_the_release_before_teardown(prepared):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    environment.records.set_phase(rt.Phase.READY)
    pending(environment)
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    supervisor = rt.Supervisor(
        environment, upgrade, cloudtasks=controller, quiesce=lambda: True
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
    plans = {
        "nonce": env[2]["nonce"],
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
    }
    for call in (
        lambda: runner.cleanup.scale_operator(0),
        lambda: runner.env.records.set_phase(rt.Phase.CLEANED),
        lambda: runner.env.records.settled(None),
        lambda: rt.verify_idle(runner.env),
        lambda: runner.finalize(plans),
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
        {"name": trial.queue_path + "/tasks/" + trial.task_id(n)} for n in sequences
    ]
    return [names[i : i + per_page] for i in range(0, len(names), per_page)] or [[]]


def full_coverage():
    """Every family read once in every sampled window."""
    return {
        stage: {"attempts": 1, "observed": sorted(OBSERVED)} for stage in SAMPLED_STAGES
    }


@pytest.fixture
def exercise(prepared, monkeypatch):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    small = plan.Trial(environment.approval.run_id, 4, 1)
    monkeypatch.setattr(
        type(environment.approval),
        "cloudtasks_plan",
        property(lambda self: small),
    )
    controller = Controller(environment, small)
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = CloudTasksExercise(environment, upgrade, controller)
    value.outcomes.update(upgrade={}, failover={})
    value.coverage = full_coverage()
    value.persist("baseline")
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


def test_a_listing_near_the_cleanup_start_still_writes_its_last_attempt(
    exercise,
):
    value, controller, small = exercise
    value.env.clock.now = value.env.schedule.cleanup_at - 300
    controller.page_seconds = 100
    controller.pages = [listing(small, [0, 1], per_page=2), listing(small, range(4), 1)]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["stage"] == "complete"
    assert record["outcomes"]["listing"]["attempt"] == 1
    assert value.env.clock() < value.env.schedule.cleanup_at


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


def test_a_window_that_read_no_family_is_not_usable(exercise):
    value, controller, small = exercise
    value.coverage = {**full_coverage(), "upgraded": {"attempts": 2, "observed": []}}
    controller.pages = [listing(small, range(4))]
    value.verify_tasks()
    record = value.env.refresh().recovery
    assert record["verdict"] == "inconclusive"
    assert record["reasons"] == [
        f"unobserved-{family}-in-upgraded" for family in OBSERVED
    ]
    # The record carries the coverage the verdict was decided over.
    assert record["coverage"] == value.coverage


def test_a_queue_no_longer_paused_is_not_usable(exercise):
    value, controller, small = exercise
    controller.readbacks = [{"state": "RUNNING"}]
    controller.pages = [listing(small, range(4))]
    value.verify_tasks()
    assert value.env.refresh().recovery["reasons"] == ["queue-not-paused"]


def test_the_listing_needs_both_recoveries_and_the_finished_input(prepared):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    value = CloudTasksExercise(environment, upgrade, controller)
    with pytest.raises(Failure, match="both recoveries"):
        value.verify_tasks()


def test_the_exercise_needs_the_runs_own_controller(prepared, env):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    with pytest.raises(Failure, match="queue controller"):
        CloudTasksExercise(environment, upgrade, None)
    other = plan.Trial("ct-other", 100, 10)
    with pytest.raises(Failure, match="queue controller"):
        CloudTasksExercise(environment, upgrade, Controller(environment, other))


def progress(value, lineage, processed, phase="initial", at=None):
    line = (
        f"{rt.utc(at or value.env.clock())} event=cloudtasks-progress "
        f"run_id={value.env.approval.run_id} phase={phase} lineage={lineage} "
        f"restored={str(phase == 'upgrade').lower()} processed={processed} "
        f"sequence={processed - 1}"
    )
    value.progress({"metadata": {"uid": "pod"}}, line)


def test_the_input_lineage_is_recorded_once_and_must_not_change(prepared):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    value = CloudTasksExercise(environment, upgrade, controller)
    lineage = "123e4567-e89b-12d3-a456-426614174000"
    progress(value, lineage, 1)
    assert environment.refresh().lineage == lineage
    progress(value, lineage, 2)
    with pytest.raises(Failure, match="lineage changed"):
        progress(value, lineage.replace("1", "2"), 3)


LINEAGE = "123e4567-e89b-12d3-a456-426614174000"


def pods(*names):
    return [
        {
            "metadata": {
                "namespace": CLOUDTASKS,
                "name": name,
                "uid": name + "-uid",
                "labels": {
                    "component": "jobmanager"
                    if name.startswith("jm")
                    else "taskmanager"
                },
            },
            "status": {"phase": "Running"},
        }
        for name in names
    ]


def test_the_upgrade_waits_for_flowing_input_and_a_whole_baseline(
    prepared, monkeypatch
):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    value = CloudTasksExercise(environment, upgrade, controller)
    begun = []

    class Begun(Exception):
        pass

    def operations(app):
        # The first step past every gate: the upgrade has begun.
        begun.append(environment.clock())
        raise Begun

    monkeypatch.setattr(value, "upgrade_operations", operations)
    running = pods("jm-1", "tm-1", "tm-2")

    def checkpoint():
        return {"trigger_timestamp": environment.clock() * 1000}

    def begin_upgrade(cp):
        try:
            value.begin_upgrade(None, cp, [], running)
        except Begun:
            pass

    begin_upgrade(checkpoint())
    assert begun == []
    environment.clock.now += 5
    progress(value, "123e4567-e89b-12d3-a456-426614174000", 10)
    # Input flows on stable Pods, but no single JobManager runs: no baseline.
    value.stable_pods = {pod["metadata"]["uid"] for pod in running}
    value.begin_upgrade(None, checkpoint(), [], running[1:])
    assert value.baseline_until is None
    value.stable_pods = None
    # Input flows, but the Pods are not yet the stable set.
    begin_upgrade(checkpoint())
    assert begun == [] and value.baseline_until is None
    value.stable_pods = {pod["metadata"]["uid"] for pod in running}
    begin_upgrade(checkpoint())
    opened = environment.clock()
    window = (
        CLOUDTASKS_OBSERVATIONS["warmup_seconds"]
        + CLOUDTASKS_OBSERVATIONS["baseline_seconds"]
    )
    assert value.baseline_until == opened + window
    assert value.deadline == opened + window + 120
    [event] = events(environment, "cloudtasks-baseline-window")
    assert event == {"started_at": rt.utc(opened), "until": rt.utc(opened + window)}
    assert begun == []
    # Input that kept flowing to the window's end, with a checkpoint triggered
    # before it closed: still the baseline.
    environment.clock.now = opened + window
    stale = {"trigger_timestamp": (opened + window - 1) * 1000}
    progress(value, "123e4567-e89b-12d3-a456-426614174000", 20)
    begin_upgrade(stale)
    assert begun == []
    begin_upgrade(checkpoint())
    assert begun == [opened + window]
    # Input that stopped advancing a minute ago is no longer flowing.
    environment.clock.now += 61
    begin_upgrade(checkpoint())
    assert begun == [opened + window]


@pytest.mark.parametrize(
    "kind,failover,rescale,pods",
    [
        ("jm-replacement", "jobmanager", None, 3),
        ("tm-replacement", "taskmanager", None, 3),
        ("rescale-out", "jobmanager", "out", 2),
        ("rescale-in", "jobmanager", "in", 3),
    ],
)
def test_the_approved_trial_names_the_exercises_faults(
    prepared, monkeypatch, kind, failover, rescale, pods
):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    trial = plan.Trial(environment.approval.run_id, 180000, 100, kind)
    monkeypatch.setattr(
        type(environment.approval), "cloudtasks_plan", property(lambda self: trial)
    )
    value = CloudTasksExercise(environment, upgrade, Controller(environment, trial))
    assert (value.failover, value.rescale) == (failover, rescale)
    # The JobManager and one TaskManager per initial sink subtask.
    assert value.expected_pods == pods


class Rest:
    """The job's REST service as the observer reads it.

    A listing names a vertex's metric ids; a `?get=` read answers only the ids
    it asks for that the vertex publishes, aggregated, as Flink's
    subtask-metrics and TaskManager-metrics endpoints do.
    """

    def __init__(self, answers, job_id="1" * 32):
        self.answers = answers
        self.job_id = job_id
        self.paths = []

    def _read(self, path):
        self.paths.append(path)
        base, _, query = path.partition("?")
        answer = self.answers[base or "/"]
        if not query.startswith("get="):
            return answer
        asked = query.removeprefix("get=").split("&")[0].split(",")
        return [
            {"id": item["id"], "min": 1, "max": 1, "sum": 1}
            for item in answer
            if item["id"] in asked
        ]

    def job(self, path):
        return self._read(path)

    def root(self, path):
        return self._read(path)


SINK, SOURCE = "sink-vertex", "source-vertex"


def answers():
    return {
        "/": {
            "vertices": [
                {"id": SOURCE, "name": "Source: Cloud Tasks input -> identity"},
                {"id": SINK, "name": "Cloud Tasks: Writer -> Cloud Tasks: Committer"},
            ]
        },
        f"/vertices/{SINK}/subtasks/metrics": [
            {"id": "numRecordsIn"},
            {"id": "inPoolUsage"},
            {"id": "Cloud_Tasks__Committer.tasksDeduplicated"},
        ],
        f"/vertices/{SOURCE}/subtasks/metrics": [
            {"id": "numRecordsOut"},
            {"id": "outPoolUsage"},
        ],
        "/taskmanagers": {"taskmanagers": [{"id": "tm-1"}]},
        "/taskmanagers/tm-1/metrics": [{"id": "Status.JVM.Memory.Heap.Used"}],
        "/checkpoints": {"latest": {"completed": {"id": 7}}},
        f"/checkpoints/details/7/subtasks/{SINK}": {
            "subtasks": [
                {
                    "index": 0,
                    "end_to_end_duration": 120,
                    "checkpoint": {"sync": 1, "async": 2},
                }
            ]
        },
    }


@pytest.fixture
def measured(prepared, monkeypatch):
    """A running Cloud Tasks exercise whose REST service is `Rest`."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    controller = Controller(environment, environment.approval.cloudtasks_plan)
    value = CloudTasksExercise(environment, upgrade, controller)
    services = []

    def service(env, svc, job_id):
        assert env is environment
        services.append(job_id)
        return Rest(answers(), job_id)

    monkeypatch.setattr("flink_tier3.exercise.FlinkRest", service)
    value.persist("baseline")
    return value, services


def sample(value, seconds=CLOUDTASKS_OBSERVATIONS["measure_seconds"]):
    """The next measurement interval's poll, with the job's REST attached."""
    value.env.clock.now += seconds
    value.attach_rest({"metadata": {"name": "rest"}}, value.rest_job)
    value.measure()
    return events(value.env, MEASUREMENT_EVENT)[-1]


def test_the_supervisor_loop_samples_through_the_service_it_attaches(measured):
    value, services = measured
    value.rest_job = "1" * 32
    running = {
        "metadata": {"generation": 1},
        "spec": {},
        "status": {"jobStatus": {"state": "RUNNING", "jobId": "1" * 32}},
    }
    value.attach_rest({"metadata": {"name": "rest"}}, "1" * 32)
    value.observe(running, {}, [])
    [reading] = events(value.env, MEASUREMENT_EVENT)
    assert reading["stage"] == "baseline"
    # Startup, before the baseline window opens: read, but no window's.
    assert reading["window"] is None
    assert set(reading) == {
        "stage",
        "window",
        "sink",
        "source",
        "taskmanagers",
        "checkpoints",
    }
    assert value.coverage == {}
    # A second poll inside the interval reads nothing.
    value.observe(running, {}, [])
    assert len(events(value.env, MEASUREMENT_EVENT)) == 1
    # The same job keeps its reader and its discovered ids.
    value.attach_rest({"metadata": {"name": "rest"}}, "1" * 32)
    assert services == ["1" * 32] and value.vertices is not None
    # A new job, as after the upgrade, gets a reader and discovery of its own.
    value.attach_rest({"metadata": {"name": "rest"}}, "2" * 32)
    assert services == ["1" * 32, "2" * 32] and value.vertices is None


def test_each_family_is_read_from_the_endpoint_that_owns_it(measured):
    value, _ = measured
    value.rest_job = "1" * 32
    reading = sample(value)
    assert value.vertices == {
        "source": SOURCE,
        "sink": SINK,
        "sink_metrics": [
            "numRecordsIn",
            "inPoolUsage",
            "Cloud_Tasks__Committer.tasksDeduplicated",
        ],
    }
    assert [item["id"] for item in reading["sink"]] == value.vertices["sink_metrics"]
    # The source answers its own metrics, which the sink does not publish.
    assert [item["id"] for item in reading["source"]] == [
        "numRecordsOut",
        "outPoolUsage",
    ]
    assert reading["taskmanagers"][0]["metrics"][0]["id"] == (
        "Status.JVM.Memory.Heap.Used"
    )
    assert reading["checkpoints"]["vertices"]["sink"][0]["end_to_end_duration"] == 120


def test_only_a_sample_inside_a_window_counts_toward_it(measured):
    value, _ = measured
    value.rest_job = "1" * 32
    timing = CLOUDTASKS_OBSERVATIONS
    # The baseline opens after its warm-up.
    value.baseline_until = (
        value.env.clock() + timing["warmup_seconds"] + timing["baseline_seconds"]
    )
    assert sample(value, 0)["window"] is None
    assert sample(value, timing["warmup_seconds"])["window"] == "baseline"
    value.persist("upgrade")
    assert sample(value)["window"] is None
    value.persist("upgraded")
    assert sample(value)["window"] == "upgraded"
    value.persist("failover")
    assert sample(value)["window"] is None
    value.persist("finishing")
    assert sample(value)["window"] == "finishing"
    # The rest of the input, after the window, counts toward none.
    value.env.clock.now = value.post_until - timing["measure_seconds"]
    assert sample(value)["window"] is None
    full = sorted(OBSERVED)
    assert value.coverage == {
        "baseline": {"attempts": 1, "observed": full},
        "upgraded": {"attempts": 1, "observed": full},
        "finishing": {"attempts": 1, "observed": full},
    }


@pytest.mark.parametrize(
    "change,unavailable",
    [
        ({"/": {"vertices": []}}, "expected one source and one sink, found 0"),
        (
            {
                "/": {
                    "vertices": [
                        {"id": "a", "name": "Source: one"},
                        {"id": "b", "name": "Source: two"},
                        {"id": SINK, "name": "sink"},
                    ]
                }
            },
            "expected one source and one sink, found 3",
        ),
    ],
)
def test_a_job_of_another_shape_is_a_named_absence(change, unavailable):
    from flink_tier3.cloudtasks.observe import observation, vertices

    state = vertices(Rest({**answers(), **change}))
    assert state == {"unavailable": unavailable}
    assert observation(None, state) == {"vertices": state}


def test_discovery_keeps_what_it_found_when_a_later_read_fails():
    from flink_tier3.cloudtasks.observe import vertices
    from flink_tier3.common import ApiError

    class Failing(Rest):
        def __init__(self, answers, failing):
            super().__init__(answers)
            self.failing = failing

        def _read(self, path):
            if path.split("?")[0] in self.failing:
                raise ApiError(503, "GET", path)
            return super()._read(path)

    known = vertices(Rest(answers()))
    assert vertices(Failing(answers(), {""}), known) == known
    assert (
        vertices(Failing(answers(), {f"/vertices/{SINK}/subtasks/metrics"}), known)
        == known
    )
    # Without anything found before, the failure is the named absence.
    assert "unavailable" in vertices(
        Failing(answers(), {f"/vertices/{SINK}/subtasks/metrics"})
    )
    # A later listing that lost an id keeps it: ids are unioned, never dropped.
    later = answers()
    later[f"/vertices/{SINK}/subtasks/metrics"] = [{"id": "busyTimeMsPerSecond"}]
    assert vertices(Rest(later), known)["sink_metrics"] == [
        *known["sink_metrics"],
        "busyTimeMsPerSecond",
    ]


def finishing(prepared):
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    trial = environment.approval.cloudtasks_plan
    value = CloudTasksExercise(environment, upgrade, Controller(environment, trial))
    value.persist("baseline")
    value.persist("finishing")
    assert value.post_until == (
        environment.clock() + CLOUDTASKS_OBSERVATIONS["post_recovery_seconds"]
    )
    value.boundary = environment.clock() - 1
    return value, trial


FINISHED = {"status": {"jobStatus": {"state": "FINISHED"}}, "metadata": {}}


@pytest.mark.parametrize("before", [1, None])
def test_input_that_finished_inside_the_post_recovery_window_fails(prepared, before):
    value, trial = finishing(prepared)
    if before is not None:
        value.env.clock.now = value.post_until - before
        progress(value, LINEAGE, trial.records, "upgrade")
    with pytest.raises(Failure, match="before the post-recovery window"):
        value.observe(FINISHED, {}, [])


def test_input_that_finished_after_the_window_completes_the_run(prepared):
    value, trial = finishing(prepared)
    value.env.clock.now = value.post_until
    progress(value, LINEAGE, trial.records, "upgrade")
    assert value.observe(FINISHED, {}, []) is True
    assert value.stage == LISTING_STAGE


def test_the_actors_bind_the_queue_to_the_cleanup_end_and_their_role(prepared):
    environment, application, _, _ = prepared
    controller = actors._controller(environment, application, object(), "runner")
    assert controller.api.deadline == environment.schedule.cleanup_end
    assert controller.trial == environment.approval.cloudtasks_plan
    with pytest.raises(Failure, match="wrong role"):
        actors._controller(environment, application, object(), "supervisor")
    with pytest.raises(Failure, match="belongs to its supervisor"):
        actors.barrier(environment)
