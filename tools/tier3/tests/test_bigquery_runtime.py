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
"""Common loop integration, without admitting a BigQuery deployment or paid run."""

import copy

import pytest
from flink_tier3.bigquery_handoff import require_bigquery_clean
from flink_tier3.runner import Runner
from test_tier3_lifecycle import app, lifecycle, prepared_supervisor, rt
from test_tier3_lifecycle import env as env  # noqa: PLC0414 - shared fixture


class Handoff:
    """Model only the actor interface; the real protocol has separate CAS tests."""

    def __init__(self, env):
        self.env = env
        self.calls = []
        self.pending = None
        self.value = None
        self.poll_error = None
        self.release_error = None
        self.cleanup_attempts = 0

    def poll(self):
        self.calls.append("poll")
        if self.poll_error:
            raise self.poll_error
        if self.pending:
            self.value = {"report": {"passed": False}, "observation": self.pending}

    def stop(self):
        self.env.records.request_stop()
        self.env.records._change(lambda r: r.bigquery.update(stopped=True))

    def release(self):
        self.calls.append("release")
        self.stop()
        if self.release_error:
            raise self.release_error
        self.env.records._change(lambda r: r.bigquery["handoff"].update(released=True))

    def released(self):
        state = self.env.refresh().bigquery["handoff"]
        return state["released"] and state["inflight"] is None

    def cleanup(self, quiesce):
        assert self.released()
        self.calls.append("cleanup")
        if quiesce() is not True:
            raise rt.Failure("BigQuery creators and writers are not quiescent")
        self.cleanup_attempts += 1
        if self.cleanup_attempts < 2:
            return False  # Cancellation can return before a query is DONE.
        self.env.records._change(lambda r: r.bigquery.update(cleaned=True))
        return True

    def request(self, name, *, deadline):
        self.env.require_running("stopped")
        self.pending = name
        self.calls.append(("request", name, deadline))

    def result(self, name):
        assert self.pending == name
        self.calls.append("result")
        return self.value


def pending(environment):
    environment.records._change(
        lambda r: setattr(
            r,
            "bigquery",
            {
                "cleaned": False,
                "stopped": False,
                "tables": {"0": {"receipt": {"creationTime": "123"}}},
                "queries": {"baseline": {"slot": 0}},
                "handoff": {"released": False, "inflight": None},
            },
        )
    )


def cleaned(environment):
    def edit(record):
        record.stop_requested = True
        record.bigquery.update(cleaned=True, stopped=True)
        record.bigquery["handoff"].update(released=True, inflight=None)

    environment.records._change(edit)


def running(env):
    supervisor, pod = prepared_supervisor(env)
    environment = supervisor.env
    environment.remember("application", env[0].put(app(env)))
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    pending(environment)
    return environment, pod


def plans(env):
    return {
        "nonce": env[2]["nonce"],
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
    }


def finish_job(environment):
    job = environment.root("supervisor")
    job["status"]["succeeded"] = 1
    environment.kube.put(job)


def test_runner_services_queries_then_releases_before_settlement(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    handoff.pending = "baseline"
    runner = Runner(environment, bigquery=handoff)
    sleeps = 0

    def advance(seconds):
        nonlocal sleeps
        env[3].sleep(seconds)
        sleeps += 1
        if sleeps == 1:
            assert handoff.value["report"]["passed"] is False
            environment.records.request_stop()
        else:
            assert handoff.calls == ["poll", "release"]
            cleaned(environment)
            finish_job(environment)

    environment.sleep = advance
    runner.settle()
    assert handoff.calls == ["poll", "release"]
    assert environment.refresh().idle
    assert not environment.refresh().success
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize(
    "trigger", ["request", "signal", "deadline", "completed", "evidence"]
)
def test_runner_stops_without_polling_when_execution_is_closed(env, trigger):
    environment, _ = running(env)
    handoff = Handoff(environment)
    runner = Runner(environment, bigquery=handoff)
    if trigger == "request":
        environment.records.request_stop()
    elif trigger == "signal":
        environment.stopping = True
    elif trigger == "deadline":
        env[3].now = environment.schedule.cleanup_at
    elif trigger == "completed":
        finish_job(environment)
    else:
        environment.records.mark_evidence_failed()

    def advance(seconds):
        env[3].sleep(seconds)
        cleaned(environment)
        finish_job(environment)

    environment.sleep = advance
    if trigger == "completed":
        # A dead supervisor cannot perform its cleanup on a later poll.
        with pytest.raises(rt.Failure, match="BigQuery resource cleanup"):
            runner.settle()
        assert not environment.refresh().idle
    else:
        runner.settle()
    assert handoff.calls == ["release"]


def test_runner_query_error_closes_admission_and_cannot_preserve_success(env):
    environment, _ = running(env)
    environment.records._change(lambda r: setattr(r, "success", True))
    handoff = Handoff(environment)
    handoff.poll_error = rt.Failure("query expired")
    runner = Runner(environment, bigquery=handoff)

    def advance(seconds):
        assert environment.refresh().stop_requested
        assert handoff.calls == ["poll", "release"]
        env[3].sleep(seconds)
        cleaned(environment)
        finish_job(environment)

    environment.sleep = advance
    runner.settle()
    assert not environment.refresh().success
    events = [v[0] for (bucket, key), v in env[1].data.items() if "/runner/" in key]
    assert any(v.get("event") == "bigquery-query-failed" for v in events)


def test_unresolved_release_still_tears_down_workload_but_retains_control(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    handoff.release_error = rt.Failure("unresolved create")
    finish_job(environment)
    runner = Runner(environment, bigquery=handoff)
    with pytest.raises(rt.Failure, match="BigQuery resource cleanup"):
        runner.settle(request_stop=True)
    assert environment.root("application") is None
    assert environment.refresh().phase == rt.Phase.CLEANING
    assert not environment.refresh().idle
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert "poll" not in handoff.calls
    assert ("scale", 0) not in env[0].calls


def test_supervisor_query_waits_with_heartbeat_and_does_not_grade_result(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)

    def advance(seconds):
        assert environment.refresh().heartbeat is not None
        env[3].sleep(seconds)
        handoff.poll()

    environment.sleep = advance
    result = supervisor.query("baseline", deadline=env[3]() + 60)
    assert result["report"]["passed"] is False
    assert environment.refresh().phase == rt.Phase.RUNNING
    assert not environment.refresh().success
    assert handoff.calls == [
        ("request", "baseline", env[3]() + 45),
        "result",
        "poll",
        "result",
    ]


@pytest.mark.parametrize("cancel", [False, True])
def test_query_wait_honors_deadline_and_cancellation(env, cancel):
    environment, _ = running(env)
    handoff = Handoff(environment)
    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)
    start = env[3]()

    def advance(seconds):
        env[3].sleep(seconds)
        if cancel:
            environment.records.request_stop()

    environment.sleep = advance
    with pytest.raises(rt.Failure, match="stopped|Bounded wait"):
        supervisor.query("baseline", deadline=start + 31)
    assert env[3]() <= start + 31
    assert handoff.calls.count("result") == (1 if cancel else 3)


@pytest.mark.parametrize("deadline", [True, "tomorrow", float("nan"), float("inf")])
def test_query_rejects_invalid_deadline_before_request(env, deadline):
    environment, _ = running(env)
    handoff = Handoff(environment)
    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)
    with pytest.raises(rt.Failure, match="execution window"):
        supervisor.query("baseline", deadline=deadline)
    assert handoff.calls == []


def test_cleanup_waits_for_release_and_external_fence_after_workload_removal(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    barriers = []

    def quiesce():
        assert environment.root("application") is None
        assert environment.refresh().stop_requested
        assert handoff.released()
        barriers.append(env[3]())
        return len(barriers) > 1

    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=quiesce)

    def advance(seconds):
        env[3].sleep(seconds)
        if not handoff.released():
            assert handoff.calls == []
            handoff.release()

    environment.sleep = advance
    supervisor.cleanup.run("test", True)
    assert len(barriers) == 5
    assert handoff.cleanup_attempts == 2
    assert environment.refresh().phase == rt.Phase.CLEANED
    assert environment.refresh().success
    require_bigquery_clean(environment.refresh())


@pytest.mark.parametrize("blocked", ["release", "fence", "queries"])
def test_cleanup_deadline_retains_operator_and_lock(env, blocked):
    environment, _ = running(env)
    handoff = Handoff(environment)
    env[3].now = environment.schedule.expires_at - 60
    if blocked != "release":
        handoff.release()
    if blocked == "queries":
        handoff.cleanup_attempts = -1000
    supervisor = rt.Supervisor(
        environment, bigquery=handoff, quiesce=lambda: blocked != "fence"
    )
    with pytest.raises(rt.Failure, match="Bounded wait"):
        supervisor.cleanup.run("test")
    assert environment.root("application") is None
    assert environment.refresh().phase == rt.Phase.CLEANING
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert ("scale", 0) not in env[0].calls


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: setattr(r, "bigquery", False),
        lambda r: r.bigquery.update(cleaned=False),
        lambda r: r.bigquery.update(stopped=False),
        lambda r: setattr(r, "stop_requested", False),
        lambda r: r.bigquery.update(handoff=None),
        lambda r: r.bigquery["handoff"].update(released=False),
        lambda r: r.bigquery["handoff"].update(inflight={"id": "unresolved"}),
        lambda r: r.bigquery["handoff"].pop("inflight"),
    ],
)
def test_incomplete_bigquery_blocks_all_shared_completion_paths(env, mutation):
    runner = lifecycle(env, Runner)
    pending(runner.env)
    cleaned(runner.env)
    runner.env.records._change(mutation)
    for call in (
        lambda: runner.cleanup.scale_operator(0),
        lambda: runner.env.records.set_phase(rt.Phase.CLEANED),
        lambda: runner.env.records.settled(False),
        lambda: rt.verify_idle(runner.env),
        lambda: runner.finalize(plans(env)),
    ):
        with pytest.raises(rt.Failure, match="BigQuery resource cleanup"):
            call()
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    assert env[1].read(runner.env.records.path)[0] is not None
    assert env[1].read("runs/test-1310/result.json")[0] is None


def test_finalize_retains_exact_bigquery_control_and_rejects_stale_receipt(env):
    runner = lifecycle(env, Runner)
    pending(runner.env)
    cleaned(runner.env)
    saved = copy.deepcopy(runner.env.refresh().bigquery)
    env[1].before_write = lambda: runner.env.records._change(
        lambda r: r.bigquery["queries"].update(final={"slot": 1})
    )
    with pytest.raises(rt.Failure, match="control changed"):
        runner.finalize(plans(env))
    receipt = env[1].read("runs/test-1310/result.json")[0]
    assert receipt["bigquery"] == saved
    assert env[1].read(rt.ENVIRONMENT)[0] is not None
    with pytest.raises(rt.Failure, match="receipt conflicts"):
        runner.finalize(plans(env))
    runner.env.records._change(lambda r: setattr(r, "bigquery", saved))
    assert runner.finalize(plans(env)) is False
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert env[1].read(runner.env.records.path)[0] is None
    assert env[1].read("runs/test-1310/result.json")[0] == receipt


def test_handoff_cannot_cross_actor_environments_and_cleanup_needs_fence(env):
    environment, _ = running(env)
    other = lifecycle(env).env
    handoff = Handoff(other)
    with pytest.raises(rt.Failure, match="another runner environment"):
        Runner(environment, bigquery=handoff)
    with pytest.raises(rt.Failure, match="supervisor environment"):
        rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)
    with pytest.raises(rt.Failure, match="quiescence barrier"):
        rt.Supervisor(other, bigquery=handoff)


def test_supervise_failure_runs_handoff_cleanup_before_returning(env):
    environment, pod = running(env)
    application = environment.root("application")
    application["status"] = {"jobStatus": {"state": "FAILED"}}
    env[0].put(application)
    handoff = Handoff(environment)
    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)

    def advance(seconds):
        env[3].sleep(seconds)
        if not handoff.released():
            assert environment.refresh().stop_requested
            assert environment.root("application") is None
            handoff.release()

    environment.sleep = advance
    supervisor.supervise(pod["metadata"]["uid"])
    assert handoff.calls == ["release", "cleanup", "cleanup"]
    assert environment.refresh().phase == rt.Phase.CLEANED
    assert not environment.refresh().success


@pytest.mark.parametrize("operation", ["settled", "cleaned"])
def test_completion_rechecks_bigquery_after_a_control_conflict(env, operation):
    runner = lifecycle(env, Runner)
    runner.env.records.begin_cleanup("test")
    pending(runner.env)
    cleaned(runner.env)
    env[1].before_write = lambda: runner.env.records._change(
        lambda r: r.bigquery.update(cleaned=False)
    )
    with pytest.raises(rt.Failure, match="BigQuery resource cleanup"):
        if operation == "settled":
            runner.env.records.settled(False)
        else:
            runner.env.records.set_phase(rt.Phase.CLEANED)
    assert not runner.env.refresh().idle
    assert runner.env.refresh().phase == rt.Phase.CLEANING


def test_failed_stop_write_still_removes_owned_workload(env, monkeypatch):
    from test_tier3_lifecycle import obj

    environment, _ = running(env)
    application = environment.root("application")
    pod = obj("Pod", "writer", owner=application["metadata"]["uid"])
    env[0].put(pod)

    def unavailable():
        raise rt.ApiError(503, "POST", environment.records.path)

    monkeypatch.setattr(environment.records, "request_stop", unavailable)
    with pytest.raises(rt.Failure, match="BigQuery resource cleanup"):
        rt.Cleanup(environment).run("stop write unavailable")
    assert environment.evidence_failed
    assert environment.root("application") is None
    assert env[0].get("Pod", rt.SMOKE, "writer") is None
    assert not environment.refresh().idle
    assert env[1].read(rt.ENVIRONMENT)[0] is not None


def test_cleanup_without_resource_intent_does_not_touch_attached_handoff(env):
    environment, _ = running(env)
    environment.records._change(lambda r: setattr(r, "bigquery", None))
    handoff = Handoff(environment)
    cleanup = rt.Cleanup(environment, bigquery=handoff, quiesce=lambda: True)
    cleanup.run("no BigQuery initialization")
    assert environment.root("application") is None
    assert environment.refresh().phase == rt.Phase.CLEANED
    assert handoff.calls == []
    assert not environment.refresh().success


def test_blocked_release_reports_once_but_retries_transient_control_failure(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    handoff.release_error = rt.Failure("control unavailable")
    runner = Runner(environment, bigquery=handoff)
    environment.records.request_stop()
    sleeps = 0

    def advance(seconds):
        nonlocal sleeps
        env[3].sleep(seconds)
        sleeps += 1
        if sleeps == 3:
            handoff.release_error = None
        elif sleeps == 4:
            assert handoff.released()
            cleaned(environment)
            finish_job(environment)

    environment.sleep = advance
    runner.settle()
    assert sleeps == 4
    assert handoff.calls == ["release"] * 4
    events = [v[0] for (_, key), v in env[1].data.items() if "/runner/" in key]
    blocked = [v for v in events if v.get("event") == "bigquery-release-blocked"]
    assert len(blocked) == 1
    assert blocked[0]["payload"]["cause"] == "control unavailable"
    assert environment.refresh().idle
    assert not environment.refresh().success


def test_barrier_without_handoff_is_rejected_at_construction(env):
    environment = lifecycle(env).env
    with pytest.raises(rt.Failure, match="barrier requires a BigQuery handoff"):
        rt.Supervisor(environment, quiesce=lambda: True)


def test_observation_can_complete_before_a_deadline_equal_to_cleanup(env):
    environment, _ = running(env)
    handoff = Handoff(environment)
    supervisor = rt.Supervisor(environment, bigquery=handoff, quiesce=lambda: True)

    def advance(seconds):
        env[3].sleep(seconds)
        handoff.poll()

    environment.sleep = advance
    result = supervisor.query("baseline", deadline=environment.schedule.cleanup_at)
    assert result["observation"] == "baseline"
    assert env[3]() < environment.schedule.cleanup_at


def test_failed_stop_write_still_records_cleanup_phase_and_reason(env, monkeypatch):
    environment, _ = running(env)
    cleaned(environment)
    request_stop = environment.records.request_stop
    calls = []

    def fail_once():
        calls.append("stop")
        if len(calls) == 1:
            raise rt.ApiError(503, "POST", environment.records.path)
        return request_stop()

    monkeypatch.setattr(environment.records, "request_stop", fail_once)
    handoff = Handoff(environment)
    rt.Cleanup(environment, bigquery=handoff, quiesce=lambda: True).run(
        "transient stop write"
    )
    record = environment.refresh()
    assert calls == ["stop", "stop"]
    assert record.phase == rt.Phase.CLEANED
    assert record.reason == "transient stop write"
    assert record.state_clean
    assert record.evidence_failed
    assert not record.success
    assert environment.root("application") is None
