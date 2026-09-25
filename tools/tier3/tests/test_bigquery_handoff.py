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
"""Actor interleavings use shared conditional storage without threads or sleeps."""

import copy
from types import SimpleNamespace

import pytest
from flink_tier3.bigquery_handoff import BigQueryHandoff
from flink_tier3.bigquery_lifecycle import BigQueryLifecycle
from flink_tier3.common import ApiError, Failure, TransportError, digest, json_bytes
from flink_tier3.model import Phase
from flink_tier3.records import Records
from test_bigquery_lifecycle import Resources
from test_bigquery_lifecycle import (
    setup as setup,  # noqa: PLC0414 - register shared pytest fixture
)
from test_bigquery_resources import NOW, Response, client, job, page, table


class QueryResources(Resources):
    def job(self, slot):
        self.calls.append(("job", slot))
        return copy.deepcopy(self.jobs.get(slot))

    def results(self, slot):
        assert self.jobs[slot]["status"]["state"] == "DONE"
        return super().results(slot)


@pytest.fixture
def actors(setup):
    original, runner_env, _, app = setup
    now = [NOW]
    runner_env.clock = lambda: now[0]
    supervisor_env = copy.copy(runner_env)
    supervisor_env.actor = "supervisor"
    supervisor_env.records = Records(
        runner_env.store, runner_env.approval, runner_env.clock
    )
    api = QueryResources(original.plan)
    binding = {
        "runner_token": "b" * 32,
        "evidence_bytes": 3 * 256 * 1024,
        "query_until": NOW + 1800,
    }
    runner = BigQueryHandoff(BigQueryLifecycle(runner_env, api, app), **binding)
    # The fixture's bare controller wrote an intent; a real run starts without.
    runner_env.records._change(lambda record: setattr(record, "bigquery", None))
    # Constructed as production constructs it: before the runner has written a
    # binding, and without its token.
    supervisor = BigQueryHandoff(
        BigQueryLifecycle(supervisor_env, api, app), **dict(binding, runner_token=None)
    )
    runner.initialize()
    return SimpleNamespace(
        runner=runner,
        supervisor=supervisor,
        env=runner_env,
        api=api,
        now=now,
        binding=binding,
    )


def start(a):
    a.runner.provision()
    a.env.records.set_phase(Phase.READY)
    a.env.records.set_phase(Phase.RUNNING)


def complete(a, name="baseline"):
    a.supervisor.request(name, deadline=NOW + 100)
    assert a.runner.poll() is None
    slot = a.runner.controller._read()["queries"][name]["slot"]
    a.api.jobs[slot]["status"]["state"] = "DONE"
    assert a.runner.poll() == name
    return slot


def transport(a, *responses, supervisor=False):
    actor = a.supervisor if supervisor else a.runner
    api, http = client(
        actor.controller.plan,
        *responses,
        deadline=NOW + 3600,
        clock=lambda: a.now[0],
    )
    actor.controller.api = api
    return api, http


def test_provisioning_rest_calls_share_the_startup_deadline(actors):
    a = actors
    a.now[0] = NOW + 598
    plan = a.runner.controller.plan
    # Every destination is checked absent before the one intent write, then
    # each is created.
    replies = [Response({}, 404) for _ in range(plan.trial.destinations)]
    for destination in range(plan.trial.destinations):
        replies.extend([Response({}, 404), table(plan, destination)])
    api, http = transport(a, *replies)
    a.runner.provision()
    assert len(http.calls) == 3 * plan.trial.destinations
    assert all(call[2]["timeout"] == 2 for call in http.calls)
    assert api.deadline == NOW + 3600
    assert a.runner.controller._read()["handoff"]["inflight"] is None


def test_late_provisioning_read_does_not_start_a_create_and_retains_marker(actors):
    a = actors
    a.now[0] = NOW + 599

    class LateAbsent(Response):
        def __enter__(self):
            a.now[0] += 1
            return self

    response = LateAbsent({}, 404)
    _, http = transport(a, response)
    with pytest.raises(Failure, match="deadline"):
        a.runner.provision()
    assert len(http.calls) == 1
    assert http.calls[0][0] == "GET"
    assert response.closed
    assert a.runner.controller._read()["tables"] == {}
    with pytest.raises(Failure, match="in flight or unresolved"):
        a.runner.release()


def test_submit_status_and_paginated_collection_use_one_requested_deadline(actors):
    a = actors
    start(a)
    plan = a.runner.controller.plan
    a.supervisor.request("final", deadline=NOW + 100)
    a.now[0] = NOW + 97

    class FirstPage(Response):
        def __exit__(self, *args):
            super().__exit__(*args)
            a.now[0] += 1

    _, http = transport(
        a,
        *(table(plan, i) for i in range(plan.trial.destinations)),
        Response({}, 404),
        job(plan),
        job(plan),
        job(plan),
        FirstPage(page(plan, 0, 5, "next")),
        page(plan, 5),
    )
    assert a.runner.poll() == "final"
    assert len(http.calls) == plan.trial.destinations + 6
    assert [call[2]["timeout"] for call in http.calls] == [3] * (
        len(http.calls) - 1
    ) + [2]
    assert a.supervisor.result("final")["report"]["verdict"] == "pass"


def test_expired_collection_stops_pagination_without_archiving_evidence(actors):
    a = actors
    start(a)
    a.supervisor.request("final", deadline=NOW + 100)
    a.runner.poll()
    plan = a.runner.controller.plan
    a.now[0] = NOW + 99

    class LastPage(Response):
        def __exit__(self, *args):
            super().__exit__(*args)
            a.now[0] += 1

    _, http = transport(a, job(plan), job(plan), LastPage(page(plan, 0, 5, "next")))
    with pytest.raises(Failure, match="deadline"):
        a.runner.poll()
    assert len(http.calls) == 3
    state = a.runner.controller._read()
    assert state["queries"]["final"]["evidence"] is None
    assert state["handoff"]["inflight"] is None
    a.runner.release()
    assert a.supervisor.released()


def test_cleanup_uses_its_own_deadline_after_query_window_has_closed(actors):
    a = actors
    start(a)
    a.supervisor.request("final", deadline=NOW + 100)
    a.runner.poll()
    a.runner.release()
    a.now[0] = NOW + 1801
    plan = a.supervisor.controller.plan
    replies = [job(plan, state="RUNNING"), {"job": job(plan)}]
    for destination in range(plan.trial.destinations):
        replies.extend(
            [
                table(plan, destination),
                table(plan, destination),
                Response(b"", 204),
                Response({}, 404),
            ]
        )
    api, http = transport(a, *replies, supervisor=True)
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 1803)
    assert len(http.calls) == 2 + 4 * plan.trial.destinations
    assert http.calls[1][0] == "POST"
    assert http.calls[1][1].endswith("/cancel")
    assert all(call[2]["timeout"] == 2 for call in http.calls)
    assert api.deadline == NOW + 3600


def test_expired_cleanup_cannot_issue_service_deletions(actors):
    a = actors
    start(a)
    a.runner.release()
    _, http = transport(a, supervisor=True)
    with pytest.raises(Failure, match="deadline"):
        a.supervisor.cleanup(lambda: True, deadline=NOW)
    assert http.calls == []
    assert not a.supervisor.controller._read()["cleaned"]


def test_request_submit_collect_and_read_through_distinct_actor_records(actors):
    a = actors
    start(a)
    assert a.runner.poll() is None
    before = list(a.api.calls)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.supervisor.request("baseline", deadline=NOW + 100)
    assert a.supervisor.result("baseline") is None
    assert a.api.calls == before
    assert a.runner.poll() is None
    assert a.runner.poll() is None
    assert a.api.calls.count(("submit", 0)) == 1
    a.api.jobs[0]["status"]["state"] = "DONE"
    assert a.runner.poll() == "baseline"
    before = list(a.api.calls)
    result = a.supervisor.result("baseline")
    assert result["report"]["passed"] is True
    assert a.api.calls == before
    assert a.runner.poll() is None
    assert a.api.calls.count(("results", 0)) == 1
    complete(a, "recovery")
    complete(a, "final")
    with pytest.raises(Failure, match="budget exhausted"):
        a.supervisor.request("extra", deadline=NOW + 100)
    assert len(a.api.jobs) == 3


@pytest.mark.parametrize(
    "key,value",
    [
        ("runner_token", "c" * 32),
        ("evidence_bytes", 12345),
        ("query_until", NOW + 1700),
    ],
)
def test_binding_cannot_change(actors, key, value):
    binding = dict(actors.binding, **{key: value})
    replacement = BigQueryHandoff(actors.runner.controller, **binding)
    with pytest.raises(Failure, match="binding"):
        replacement.initialize()
    assert actors.api.calls == []


@pytest.mark.parametrize(
    "key,value",
    [
        ("runner_token", "bad"),
        ("evidence_bytes", True),
        ("evidence_bytes", 0),
        ("evidence_bytes", 2),
        ("evidence_bytes", 100 * 1024**2 + 1),
        ("query_until", float("nan")),
        ("query_until", float("inf")),
        ("query_until", True),
        ("query_until", 0),
        ("query_until", NOW + 4000),
    ],
)
def test_binding_values_are_bounded(actors, key, value):
    with pytest.raises(ValueError):
        BigQueryHandoff(
            actors.runner.controller, **dict(actors.binding, **{key: value})
        )


def rewrite(a, **binding):
    """Replace the durable binding, as a second runner or a forger would."""

    def edit(state):
        state["handoff"]["binding"] = dict(state["handoff"]["binding"], **binding)

    a.runner.controller._change(edit)


def test_the_supervisor_adopts_the_token_the_runner_wrote(actors):
    assert actors.supervisor.binding["runner_token"] is None
    assert actors.supervisor.released() is False
    assert actors.supervisor.binding == actors.runner.binding


def test_the_supervisor_cannot_be_bound_before_the_runner_writes(setup):
    """Its Pod starts first; until the runner initializes there is nothing to adopt."""
    controller, env, _, app = setup
    env.clock = lambda: NOW
    # The runner has created the resource intent but not yet its binding.
    controller.initialize()
    observer = copy.copy(env)
    observer.actor = "supervisor"
    observer.records = Records(env.store, env.approval, env.clock)
    supervisor = BigQueryHandoff(
        BigQueryLifecycle(observer, controller.api, app),
        runner_token=None,
        evidence_bytes=3000,
        query_until=NOW + 10,
    )
    with pytest.raises(Failure, match="Missing or replaced"):
        supervisor.released()
    assert supervisor.binding["runner_token"] is None


@pytest.mark.parametrize(
    "field, value",
    [("evidence_bytes", 12345), ("query_until", NOW + 1700), ("version", 2)],
)
def test_the_supervisor_refuses_a_binding_its_approval_does_not_fix(
    actors, field, value
):
    """Adopted, never inferred: another budget or window is refused, not learned."""
    rewrite(actors, **{field: value})
    with pytest.raises(Failure, match="Missing or replaced"):
        actors.supervisor.released()
    assert actors.supervisor.binding["runner_token"] is None


@pytest.mark.parametrize("token", ["bad", None, 7, "B" * 32, "b" * 33, "b" * 31])
def test_the_supervisor_refuses_a_token_that_is_not_one(actors, token):
    rewrite(actors, runner_token=token)
    with pytest.raises(Failure, match="Missing or replaced"):
        actors.supervisor.released()
    assert actors.supervisor.binding["runner_token"] is None


def test_the_supervisor_refuses_a_replacement_once_it_has_adopted(actors):
    """The first read pins it; a second runner's binding is refused thereafter."""
    actors.supervisor.released()
    rewrite(actors, runner_token="c" * 32)
    with pytest.raises(Failure, match="Missing or replaced"):
        actors.supervisor.released()
    assert actors.supervisor.binding["runner_token"] == "b" * 32


def test_only_the_supervisor_may_be_bound_without_a_token(actors):
    with pytest.raises(ValueError, match="Only the supervisor"):
        BigQueryHandoff(
            actors.runner.controller, **dict(actors.binding, runner_token=None)
        )


def test_initialize_refuses_resources_without_handoff(setup):
    controller, env, _, _ = setup
    env.clock = lambda: NOW
    controller.provision()
    handoff = BigQueryHandoff(
        controller, runner_token="b" * 32, evidence_bytes=3000, query_until=NOW + 10
    )
    with pytest.raises(Failure, match="predate"):
        handoff.initialize()


def test_the_intent_is_never_written_without_its_binding(setup, monkeypatch):
    """One write: a stop or crash cannot leave an intent release cannot clear."""
    controller, env, _, _ = setup
    env.clock = lambda: NOW
    # The fixture's bare controller wrote an intent; a real run starts without.
    env.records._change(lambda record: setattr(record, "bigquery", None))
    handoff = BigQueryHandoff(
        controller, runner_token="b" * 32, evidence_bytes=3000, query_until=NOW + 10
    )
    written = []
    change = env.records._change

    def recording(edit):
        result = change(edit)
        written.append(copy.deepcopy(env.refresh().bigquery))
        return result

    monkeypatch.setattr(env.records, "_change", recording)
    handoff.initialize()
    first = next(state for state in written if state is not None)
    assert first.get("handoff", {}).get("binding") == handoff.binding


def test_a_stop_straight_after_the_intent_leaves_a_releasable_run(setup, monkeypatch):
    """The moment two writes would have split is still one the runner releases."""
    controller, env, _, _ = setup
    env.clock = lambda: NOW
    env.records._change(lambda record: setattr(record, "bigquery", None))
    handoff = BigQueryHandoff(
        controller, runner_token="b" * 32, evidence_bytes=3000, query_until=NOW + 10
    )
    change = env.records._change

    def stopping(edit):
        result = change(edit)
        if env.refresh().bigquery is not None and not env.refresh().stop_requested:
            env.records.request_stop()
        return result

    monkeypatch.setattr(env.records, "_change", stopping)
    with pytest.raises(Failure, match="admission has stopped"):
        handoff.initialize()
    monkeypatch.setattr(env.records, "_change", change)
    handoff.release()
    assert handoff.released() is True


def test_initialize_never_adopts_an_intent_written_without_a_binding(setup):
    controller, env, _, _ = setup
    env.clock = lambda: NOW
    handoff = BigQueryHandoff(
        controller, runner_token="b" * 32, evidence_bytes=3000, query_until=NOW + 10
    )
    with pytest.raises(Failure, match="predates its actor binding"):
        handoff.initialize()
    assert "handoff" not in env.refresh().bigquery


def test_initialize_refuses_expired_window(actors):
    actors.now[0] = actors.binding["query_until"]
    with pytest.raises(Failure, match="window expired"):
        actors.runner.initialize()


@pytest.mark.parametrize(
    "actor,method,args,kwargs",
    [
        ("supervisor", "initialize", (), {}),
        ("supervisor", "provision", (), {}),
        ("supervisor", "poll", (), {}),
        ("supervisor", "release", (), {}),
        ("runner", "request", ("x",), {"deadline": NOW + 10}),
        ("runner", "result", ("x",), {}),
        ("runner", "cleanup", (lambda: True,), {"deadline": NOW + 3600}),
    ],
)
def test_actor_roles(actors, actor, method, args, kwargs):
    with pytest.raises(Failure, match="actor"):
        getattr(getattr(actors, actor), method)(*args, **kwargs)
    assert actors.api.calls == []


@pytest.mark.parametrize("deadline", [NOW, NOW - 1, NOW + 1801, float("nan"), True])
def test_request_deadline(actors, deadline):
    start(actors)
    with pytest.raises(ValueError, match="window"):
        actors.supervisor.request("baseline", deadline=deadline)
    assert actors.runner.controller._read()["handoff"]["requests"] == {}


def test_request_serialization_and_immutable_deadline(actors):
    start(actors)
    actors.supervisor.request("baseline", deadline=NOW + 100)
    with pytest.raises(Failure, match="cannot be changed"):
        actors.supervisor.request("baseline", deadline=NOW + 101)
    with pytest.raises(Failure, match="still pending"):
        actors.supervisor.request("recovery", deadline=NOW + 100)
    with pytest.raises(ValueError, match="label"):
        actors.supervisor.request("../bad", deadline=NOW + 100)
    with pytest.raises(Failure, match="not requested"):
        actors.supervisor.result("unknown")


def test_stop_wins_a_request_cas_conflict(actors):
    start(actors)
    actors.env.store.before_write = actors.runner.stop
    with pytest.raises(Failure, match="stopped"):
        actors.supervisor.request("baseline", deadline=NOW + 100)
    assert actors.env.store.conflicts == 1
    assert actors.runner.controller._read()["handoff"]["requests"] == {}


def test_deadline_rechecked_after_submit(actors):
    start(actors)
    actors.supervisor.request("baseline", deadline=NOW + 100)
    actors.api.after_submit = lambda slot: actors.now.__setitem__(0, NOW + 100)
    with pytest.raises(Failure, match="deadline expired"):
        actors.runner.poll()
    assert not any(call[0] == "job" for call in actors.api.calls)
    actors.runner.release()


@pytest.mark.parametrize("stage", ["provision", "submit", "collect"])
def test_stop_during_call_cannot_release_or_delete_until_return(actors, stage):
    a = actors
    if stage != "provision":
        start(a)
        a.supervisor.request("baseline", deadline=NOW + 100)
    if stage == "collect":
        a.runner.poll()
        a.api.jobs[0]["status"]["state"] = "DONE"

    def stop_inside_call(*args):
        a.supervisor.stop()
        with pytest.raises(Failure, match="in flight"):
            a.runner.release()
        with pytest.raises(Failure, match="not released"):
            a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
        assert not any(call[0] == "delete" for call in a.api.calls)

    if stage == "provision":
        a.api.after_create = stop_inside_call
        with pytest.raises(Failure, match="Admission closed"):
            a.runner.provision()
        # Provision aborted before completing every table: conservative marker.
        assert (
            a.runner.controller._read()["handoff"]["inflight"]["operation"]
            == "provision"
        )
        return
    if stage == "submit":
        a.api.after_submit = stop_inside_call
        with pytest.raises(Failure, match="stopped"):
            a.runner.poll()
    else:
        a.api.on_results = stop_inside_call
        assert a.runner.poll() == "baseline"
        assert a.supervisor.result("baseline")["report"]["passed"]
    assert a.runner.controller._read()["handoff"]["inflight"] is None
    a.runner.release()
    a.api.jobs[0]["status"]["state"] = "DONE"
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    assert not a.api.tables


@pytest.mark.parametrize("stage", ["provision", "submit"])
def test_ambiguous_create_survives_new_protocol_object_and_blocks_cleanup(
    actors, stage
):
    a = actors

    def lost(*args):
        raise TransportError("Lost response")

    if stage == "provision":
        a.api.after_create = lost
        action = a.runner.provision
    else:
        start(a)
        a.supervisor.request("baseline", deadline=NOW + 100)
        a.api.after_submit = lost
        action = a.runner.poll
    with pytest.raises(TransportError):
        action()
    restored = BigQueryHandoff(a.runner.controller, **a.binding)
    calls = list(a.api.calls)
    with pytest.raises(Failure, match="in flight"):
        restored.release()
    with pytest.raises(Failure, match="not released"):
        a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    assert a.api.calls == calls
    assert a.runner.controller._read()["handoff"]["inflight"] is not None


def test_cleanup_requires_both_release_and_external_barrier(actors):
    a = actors
    start(a)
    complete(a)
    with pytest.raises(Failure, match="not released"):
        a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    a.runner.release()
    a.runner.release()
    for barrier in (False, None, 1):
        with pytest.raises(Failure, match="not quiescent"):
            a.supervisor.cleanup(lambda barrier=barrier: barrier, deadline=NOW + 3600)
    assert not any(call[0] == "delete" for call in a.api.calls)
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    assert a.env.refresh().phase == Phase.RUNNING
    assert a.env.refresh().stop_requested
    assert a.env.refresh().bigquery["cleaned"]
    with pytest.raises(Failure):
        a.runner.provision()
    with pytest.raises(Failure):
        a.runner.poll()
    assert a.supervisor.result("baseline")["report"]["passed"]


def test_pending_job_blocks_delete_after_release(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.poll()
    a.runner.release()
    assert not a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    assert not any(call[0] == "delete" for call in a.api.calls)
    a.api.jobs[0]["status"]["state"] = "DONE"
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)


def test_read_failure_clears_marker_without_repeating_submit(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    original = a.api.job

    def lost(slot):
        raise TransportError("Lost read")

    a.api.job = lost
    with pytest.raises(TransportError):
        a.runner.poll()
    assert a.runner.controller._read()["handoff"]["inflight"] is None
    a.api.job = original
    assert a.runner.poll() is None
    assert a.api.calls.count(("submit", 0)) == 1
    a.runner.release()


@pytest.mark.parametrize(
    "damage",
    [
        "generation",
        "hash",
        "intent_sha256",
        "observation",
        "slot",
        "missing_result",
        "oversized",
    ],
)
def test_supervisor_refuses_replaced_or_unbound_evidence(actors, damage):
    a = actors
    start(a)
    slot = complete(a)
    path = a.runner.controller.prefix + f"queries/{slot}.json"
    artifact, generation = a.env.store.read(path)
    if damage in ("intent_sha256", "observation", "slot"):
        artifact[damage] = "wrong"
    elif damage == "missing_result":
        del artifact["result"]
    elif damage == "oversized":
        artifact["padding"] = "x" * a.runner.per_query
    elif damage == "hash":
        artifact["result"]["report"]["passed"] = False
    new_generation = a.env.store.write(path, artifact, generation)
    if damage != "generation":

        def pointer(state):
            value = state["queries"]["baseline"]["evidence"]
            value["generation"] = new_generation
            if damage != "hash":
                value["sha256"] = digest(artifact)

        a.runner.controller._change(pointer)
    with pytest.raises(Failure, match="evidence"):
        a.supervisor.result("baseline")


def test_oversized_result_is_never_written_and_can_release(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.poll()
    a.api.jobs[0]["status"]["state"] = "DONE"
    a.api.results = lambda slot: {"padding": "x" * a.runner.per_query}
    with pytest.raises(Failure, match="byte budget"):
        a.runner.poll()
    assert a.env.store.read(a.runner.controller.prefix + "queries/0.json")[0] is None
    assert a.supervisor.result("baseline") is None
    a.runner.release()
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)


def test_cached_collection_rechecks_byte_budget(actors):
    a = actors
    start(a)
    complete(a)
    artifact, _ = a.env.store.read(a.runner.controller.prefix + "queries/0.json")
    size = len(json_bytes(artifact))
    controller = a.runner.controller
    assert controller.collect_query("baseline", max_bytes=size) == artifact["result"]
    with pytest.raises(Failure, match="byte budget"):
        controller.collect_query("baseline", max_bytes=size - 1)
    with pytest.raises(ValueError):
        controller.collect_query("baseline", max_bytes=True)


def test_lost_owner_refuses_release_and_evidence_read(actors):
    start(actors)
    complete(actors)
    actors.env.owner = False
    actors.supervisor.env.owner = False
    with pytest.raises(Failure, match="ownership"):
        actors.runner.release()
    with pytest.raises(Failure, match="ownership"):
        actors.supervisor.result("baseline")


def test_deadline_rechecked_when_request_cas_retries(actors):
    a = actors
    start(a)

    def advance():
        a.now[0] = NOW + 100
        a.runner.controller._change(lambda state: None)

    a.env.store.before_write = advance
    with pytest.raises(Failure, match="deadline expired"):
        a.supervisor.request("baseline", deadline=NOW + 100)
    assert a.env.store.conflicts == 1
    assert a.runner.controller._read()["handoff"]["requests"] == {}


def test_stop_wins_call_start_cas_conflict(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.controller.reserve_query("baseline")
    a.env.store.before_write = a.supervisor.stop
    with pytest.raises(Failure, match="stopped"):
        a.runner.poll()
    assert a.env.store.conflicts == 1
    assert not any(call[0] == "submit" for call in a.api.calls)


def test_release_cannot_race_an_already_started_query(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.controller.reserve_query("baseline")
    # An attempt to release between the old record read and call-marker write
    # changes the generation, making admission retry against the stopped record.
    a.env.store.before_write = a.runner.release
    with pytest.raises(Failure, match="stopped"):
        a.runner.poll()
    assert a.env.store.conflicts == 1
    assert a.runner.controller._read()["handoff"]["released"]
    assert not any(call[0] == "submit" for call in a.api.calls)


@pytest.mark.parametrize("extra", [0, -1])
def test_new_collection_byte_boundary(actors, extra):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.poll()
    a.api.jobs[0]["status"]["state"] = "DONE"
    result = a.api.results(0)
    artifact = {
        "intent_sha256": digest(a.runner.controller.intent),
        "observation": "baseline",
        "slot": 0,
        "result": result,
    }
    size = len(json_bytes(artifact))
    if extra == 0:
        assert a.runner.controller.collect_query("baseline", max_bytes=size) == result
    else:
        with pytest.raises(Failure, match="byte budget"):
            a.runner.controller.collect_query("baseline", max_bytes=size + extra)
        assert (
            a.env.store.read(a.runner.controller.prefix + "queries/0.json")[0] is None
        )


@pytest.mark.parametrize("outcome", ["before", "after", "persistent"])
def test_collection_finish_retries_only_the_control_acknowledgement(actors, outcome):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.runner.poll()
    a.api.jobs[0]["status"]["state"] = "DONE"
    write = a.env.store.write
    attempts = []

    def flaky(name, data, *args, **kwargs):
        current, _ = a.env.store.read(a.env.records.path)
        marker = current["bigquery"]["handoff"]["inflight"]
        if (
            name == a.env.records.path
            and marker is not None
            and marker["operation"] == "collect:baseline"
            and data["bigquery"]["handoff"]["inflight"] is None
        ):
            attempts.append(marker["id"])
            if outcome == "after" and len(attempts) == 1:
                write(name, data, *args, **kwargs)
                raise Failure("Lost control acknowledgement")
            if outcome == "persistent" or len(attempts) == 1:
                raise ApiError(503, "POST", name)
        return write(name, data, *args, **kwargs)

    a.env.store.write = flaky
    if outcome == "persistent":
        with pytest.raises(ApiError):
            a.runner.poll()
        assert len(attempts) == 3
        with pytest.raises(Failure, match="in flight"):
            a.runner.release()
    else:
        assert a.runner.poll() == "baseline"
        assert len(attempts) == (1 if outcome == "after" else 2)
        a.runner.release()
    assert len(set(attempts)) == 1
    assert a.api.calls.count(("submit", 0)) == 1
    assert a.api.calls.count(("results", 0)) == 1
    assert a.supervisor.result("baseline")["report"]["passed"]


@pytest.mark.parametrize("clears_on_retry", [False, True])
def test_lost_clear_cannot_clear_another_invocation_of_the_same_operation(
    actors, clears_on_retry
):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    write = a.env.store.write
    replacement = {"operation": "status:baseline", "id": "c" * 32}
    read = a.env.store.read
    foreign_reads = []

    def observe_foreign(name, *args, **kwargs):
        value, generation = read(name, *args, **kwargs)
        if (
            name == a.env.records.path
            and value["bigquery"]["handoff"]["inflight"] == replacement
        ):
            foreign_reads.append(name)
            if clears_on_retry and len(foreign_reads) == 2:
                value["bigquery"]["handoff"]["inflight"] = None
                generation = write(name, value, generation)
        return value, generation

    def replace_call(name, data, *args, **kwargs):
        current, generation = a.env.store.read(a.env.records.path)
        marker = current["bigquery"]["handoff"]["inflight"]
        if (
            name == a.env.records.path
            and marker is not None
            and marker["operation"] == "status:baseline"
            and data["bigquery"]["handoff"]["inflight"] is None
        ):
            # Model the acknowledgement landing, then a later call claiming
            # the cleared marker before the first caller sees a lost response.
            a.env.store.write = write
            write(name, data, *args, **kwargs)
            current, generation = a.env.store.read(name)
            current["bigquery"]["handoff"]["inflight"] = replacement
            write(name, current, generation)
            a.env.store.read = observe_foreign
            raise Failure("Lost control acknowledgement")
        return write(name, data, *args, **kwargs)

    a.env.store.write = replace_call
    with pytest.raises(Failure, match="identity changed"):
        a.runner.poll()
    a.env.store.read = read
    assert len(foreign_reads) == 1
    assert a.runner.controller._read()["handoff"]["inflight"] == replacement
    with pytest.raises(Failure, match="in flight"):
        a.runner.release()


def test_expired_unreserved_request_aborts_observation_but_allows_cleanup(actors):
    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.now[0] = NOW + 101
    with pytest.raises(Failure, match="deadline expired"):
        a.runner.poll()
    with pytest.raises(Failure, match="still pending"):
        a.supervisor.request("recovery", deadline=NOW + 200)
    assert a.supervisor.result("baseline") is None
    assert not a.api.jobs
    a.runner.release()
    assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
    assert not a.api.tables


def test_unknown_actor_cannot_stop_run(actors):
    actors.env.actor = "other"
    with pytest.raises(Failure, match="Unexpected"):
        actors.runner.stop()
    assert not actors.env.refresh().stop_requested


def test_real_handoff_is_serviced_by_runner_settlement(actors):
    from flink_tier3.environment import Environment
    from flink_tier3.model import Schedule
    from flink_tier3.runner import Runner

    a = actors
    start(a)
    a.supervisor.request("baseline", deadline=NOW + 100)
    a.env.stopping = a.env.evidence_failed = False
    a.env.schedule = Schedule.for_window(NOW, NOW + 3600)
    a.env.emit = lambda event, payload: a.env.records.evidence(event, payload, "runner")
    job = {"status": {}}
    a.env.root = lambda _key: job
    a.env.wait = lambda predicate, deadline: Environment.wait(
        a.env, predicate, deadline
    )
    a.env.roots = {}
    runner = Runner(a.env, bigquery=a.runner)
    runner.adopt_root = lambda _key: None
    polls = 0

    def advance(seconds):
        nonlocal polls
        a.now[0] += seconds
        polls += 1
        if polls == 1:
            assert a.api.calls.count(("submit", 0)) == 1
            a.api.jobs[0]["status"]["state"] = "DONE"
        elif polls == 2:
            assert a.supervisor.result("baseline")["report"]["passed"] is True
            a.supervisor.stop()
        else:
            assert a.supervisor.released()
            assert a.supervisor.cleanup(lambda: True, deadline=NOW + 3600)
            job["status"]["succeeded"] = 1

    a.env.sleep = advance

    class Settled(Exception):
        pass

    def after_wait(_reason, _success):
        from flink_tier3.bigquery_handoff import require_bigquery_clean

        require_bigquery_clean(a.env.refresh())
        raise Settled

    runner.cleanup.run = after_wait
    with pytest.raises(Settled):
        runner.settle()
    assert polls == 3
    assert not a.api.tables
    assert a.api.calls.count(("submit", 0)) == 1
    assert a.api.calls.count(("results", 0)) == 1


@pytest.mark.parametrize("pending", [False, 1])
def test_common_cleanup_waits_for_real_handoff_fence(actors, pending):
    from flink_tier3.cleanup import Cleanup
    from flink_tier3.environment import Environment

    a = actors
    start(a)
    a.runner.release()
    env = a.supervisor.env
    env.wait = lambda predicate, deadline: Environment.wait(env, predicate, deadline)
    waits = []

    def sleep(seconds):
        assert not any(call[0] == "delete" for call in a.api.calls)
        waits.append(seconds)
        a.now[0] += seconds

    env.sleep = sleep
    barriers = iter([pending, True, True])
    cleanup = Cleanup(env, bigquery=a.supervisor, quiesce=lambda: next(barriers))
    cleanup.finish_bigquery(NOW + 60)
    assert waits == [15]
    assert not a.api.tables
    assert a.env.refresh().bigquery["cleaned"]


def test_common_cleanup_rechecks_real_external_fence_before_deletion(actors):
    from flink_tier3.cleanup import Cleanup
    from flink_tier3.environment import Environment

    a = actors
    start(a)
    a.runner.release()
    env = a.supervisor.env
    env.wait = lambda predicate, deadline: Environment.wait(env, predicate, deadline)
    barriers = iter([True, False])
    cleanup = Cleanup(env, bigquery=a.supervisor, quiesce=lambda: next(barriers))
    with pytest.raises(Failure, match="not quiescent"):
        cleanup.finish_bigquery(NOW + 60)
    assert len(a.api.tables) == 10
    assert not any(call[0] == "delete" for call in a.api.calls)
    assert not a.env.refresh().bigquery["cleaned"]


def test_common_cleanup_skips_real_handoff_without_resource_intent(actors):
    from flink_tier3.cleanup import Cleanup

    a = actors
    a.env.records._change(lambda r: setattr(r, "bigquery", None))
    cleanup = Cleanup(a.supervisor.env, bigquery=a.supervisor, quiesce=lambda: True)
    cleanup.finish_bigquery(NOW + 60)
    assert a.api.calls == []
    assert a.env.refresh().bigquery is None


def test_an_intent_without_a_binding_requires_external_incident_recovery(actors):
    """Only a bare controller writes one now; initialization writes both at once."""
    from flink_tier3.bigquery_handoff import require_bigquery_clean

    a = actors
    a.env.records._change(lambda r: r.bigquery.pop("handoff"))
    a.env.records.request_stop()
    with pytest.raises(Failure, match="Admission closed"):
        a.runner.initialize()
    with pytest.raises(Failure, match="actor binding"):
        a.runner.release()
    with pytest.raises(Failure, match="BigQuery resource cleanup"):
        require_bigquery_clean(a.env.refresh())
    assert a.api.calls == []
