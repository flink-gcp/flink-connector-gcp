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
"""Shared record reservations composed with production Pub/Sub message helpers."""

import base64
import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests
import test_tier3_lifecycle as settlement
from flink_tier3.common import ApiError, Failure, TransportError, json_bytes
from flink_tier3.model import Phase
from flink_tier3.pubsub_lifecycle import PubSubLifecycle
from flink_tier3.pubsub_traffic import PubSubTraffic, TrafficLimits
from flink_tier3.records import Records
from test_pubsub_lifecycle import Environment
from test_pubsub_messages import Http, Response, output
from test_pubsub_resources import FakeIamHttp


@pytest.fixture
def setup(request):
    app = {
        "metadata": {"name": "lifecycle-1361", "namespace": "tier3-pubsub"},
        "spec": {
            "job": {
                "args": ["--run-id=lifecycle-1361", "--records-per-subscription=1000"]
            }
        },
    }
    env = Environment(app)
    now = [1800000000]
    env.clock = lambda: now[0]
    env.schedule = SimpleNamespace(cleanup_at=now[0] + 200, expires_at=now[0] + 300)
    env.stopping = env.evidence_failed = False
    guarded, events = [], []
    provisioner = PubSubLifecycle(
        env, FakeIamHttp(), app, lambda *args: guarded.append(args)
    )
    env.resource_http = provisioner.http
    provisioner.initialize()
    provisioner.prepare()
    http = Http(events)
    limit_values = {
        "publish_calls": 3,
        "pull_calls": 3,
        "input_messages": 4,
        "input_bytes": 1000,
        "output_messages": 4,
        "pubsub_requests": 8,
        "evidence_bytes": 200000,
        "admit_until": now[0] + 100,
    }

    limit_values.update(getattr(request, "param", {}))
    limits = TrafficLimits(**limit_values)

    def make(actor="runner", selected=None):
        actor_env = copy.copy(env)
        actor_env.actor = actor
        actor_env.records = Records(env.store, env.approval, env.clock)
        controller = PubSubLifecycle(
            actor_env, http, app, lambda *args: guarded.append(args)
        )
        return PubSubTraffic(controller, app, selected or limits)

    traffic = make()
    traffic.initialize()
    env.records.set_phase(Phase.READY)
    env.records.set_phase(Phase.RUNNING)
    return make, env, http, limits, now, guarded


def used(env):
    return env.refresh().pubsub["traffic"]["used"]


def evidence(env):
    return {
        name: value
        for (bucket, name), (value, _) in env.store.data.items()
        if name.startswith("runs/")
    }


def test_publish_reserves_exact_input_and_evidence_before_service(setup):
    make, env, http, _, _, _ = setup
    http.responses.append(Response({"messageIds": ["id-1", "id-2"]}))
    result = make().publish(1, 998, 2)
    values = used(env)
    assert values["publish_calls"] == 1 and values["input_messages"] == 2
    assert values["input_bytes"] == sum(
        len(base64.b64decode(message["data"], validate=True))
        for message in http.calls[0][1]["messages"]
    )
    assert values["pubsub_requests"] == 1 and values["pull_calls"] == 0
    assert values["evidence_bytes"] == sum(
        len(json_bytes(v)) for v in evidence(env).values()
    )
    assert result["message_ids"] == ["id-1", "id-2"]


def test_restart_cannot_reset_or_overspend_input_budget(setup):
    make, env, http, _, _, _ = setup
    http.responses.extend(
        [Response({"messageIds": ["a", "b"]}), Response({"messageIds": ["c", "d"]})]
    )
    make().publish(0, 0, 2)
    make().publish(1, 0, 2)
    with pytest.raises(Failure, match="input_messages"):
        make().publish(0, 2, 1)
    assert used(env)["input_messages"] == 4 and len(http.calls) == 2
    with pytest.raises(Failure, match="admission"):
        make().initialize()
    assert used(env)["input_messages"] == 4


def test_simultaneous_reservations_cannot_spend_the_last_message_twice(setup):
    make, env, http, _, _, _ = setup
    http.responses.extend(
        [
            Response({"messageIds": ["a", "b", "c"]}),
            Response({"messageIds": ["winner"]}),
        ]
    )
    make().publish(0, 0, 3)
    env.store.before_write = lambda: make().publish(1, 0, 1)
    with pytest.raises(Failure, match="input_messages"):
        make().publish(0, 3, 1)
    assert used(env)["input_messages"] == 4
    assert env.store.conflicts == 1 and len(http.calls) == 2


def test_changed_limits_or_input_domain_refuse_before_data_io(setup):
    make, env, http, limits, _, _ = setup
    with pytest.raises(Failure, match="binding"):
        make(selected=replace(limits, input_messages=5)).publish(0, 0, 1)
    traffic = make()
    changed = {
        "metadata": {"name": env.approval.run_id},
        "spec": {"job": {"args": ["--records-per-subscription=999"]}},
    }
    with pytest.raises(Failure, match="application"):
        PubSubTraffic(traffic.controller, changed, limits)
    assert not http.calls and used(env)["input_messages"] == 0


def test_lost_call_reservation_response_sends_nothing_and_keeps_charge(setup):
    make, env, http, _, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if (
            name == env.records.path
            and value["pubsub"]["traffic"]["used"]["publish_calls"] == 1
        ):
            raise Failure("lost reservation response")
        return result

    env.store.write = write
    with pytest.raises(Failure, match="lost reservation"):
        make().publish(0, 0, 1)
    assert used(env)["publish_calls"] == 1 and used(env)["input_messages"] == 1
    assert not http.calls and not evidence(env)


def test_publish_timeout_consumes_call_message_and_request_reservations(setup):
    make, env, http, _, _, _ = setup
    http.responses.append(requests.exceptions.Timeout())
    with pytest.raises(TransportError):
        make().publish(0, 0, 1)
    assert used(env)["pubsub_requests"] == 1 and used(env)["input_messages"] == 1
    with pytest.raises(ApiError):
        make().publish(0, 0, 1)
    assert used(env)["input_messages"] == 2 and len(http.calls) == 1
    assert env.refresh().evidence_failed
    with pytest.raises(Failure, match="admission"):
        make("supervisor").collect("after-collision", max_messages=1)
    assert len(http.calls) == 1
    # Failed create-only upload is conservatively charged as well.
    assert used(env)["evidence_bytes"] > sum(
        len(json_bytes(v)) for v in evidence(env).values()
    )


def test_empty_pulls_reserve_requested_deliveries_and_share_actor_budget(setup):
    make, env, http, _, _, _ = setup
    http.responses.extend([Response({}), Response({})])
    make("supervisor").collect("one", max_messages=2)
    make("supervisor").collect("two", max_messages=2)
    with pytest.raises(Failure, match="output_messages"):
        make("supervisor").collect("three", max_messages=1)
    assert used(env)["pull_calls"] == 2 and used(env)["output_messages"] == 4
    assert used(env)["pubsub_requests"] == 2 and len(http.calls) == 2


def test_collect_charges_all_four_evidence_uploads_and_two_requests(setup):
    make, env, http, _, _, _ = setup
    http.responses.extend([Response({"receivedMessages": [output()]}), Response({})])
    make("supervisor").collect("one", max_messages=1)
    assert used(env)["pubsub_requests"] == 2 and used(env)["output_messages"] == 1
    assert len(evidence(env)) == 4
    assert used(env)["evidence_bytes"] == sum(
        len(json_bytes(v)) for v in evidence(env).values()
    )


@pytest.mark.parametrize(
    "change",
    ["stop", "evidence", "deadline", "cleanup", "local-stop", "local-evidence"],
)
def test_stop_during_pull_retains_evidence_but_prevents_ack(setup, change):
    make, env, http, _, now, _ = setup
    traffic = make("supervisor")
    original = http.request

    def request(*args, **kwargs):
        response = original(*args, **kwargs)
        if change == "stop":
            env.records.request_stop()
        elif change == "evidence":
            env.records._change(lambda record: setattr(record, "evidence_failed", True))
        elif change == "deadline":
            now[0] += 100
        elif change == "cleanup":
            env.records.begin_cleanup("stopped")
            env.records._change(lambda record: record.pubsub.update(stage="cleaning"))
        elif change == "local-stop":
            traffic.env.stopping = True
        else:
            traffic.env.evidence_failed = True
        return response

    http.request = request
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="admission"):
        traffic.collect("one", max_messages=1)
    assert len(http.calls) == 1 and used(env)["pubsub_requests"] == 1
    assert any(path.endswith("/observations.json") for path in evidence(env))
    assert not any(path.endswith("/acknowledged.json") for path in evidence(env))


@pytest.mark.parametrize("change", ["owner", "expiry", "cleaned"])
def test_evidence_cannot_outlive_ownership_or_approval(setup, change):
    make, env, http, _, now, _ = setup
    traffic = make("supervisor")
    original = http.request

    def request(*args, **kwargs):
        response = original(*args, **kwargs)
        if change == "owner":
            traffic.env.owner = False
        elif change == "expiry":
            now[0] += 300
        else:
            env.records._change(lambda record: record.pubsub.update(stage="cleaned"))
        return response

    http.request = request
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure):
        traffic.collect("one", max_messages=1)
    assert len(http.calls) == 1 and len(evidence(env)) == 1


def test_failing_evidence_upload_is_charged_and_does_not_ack(setup):
    make, env, http, _, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        if name.endswith("/response.json"):
            raise Failure("evidence unavailable")
        return original(name, value, generation, **kwargs)

    env.store.write = write
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="evidence unavailable"):
        make("supervisor").collect("one", max_messages=1)
    assert len(http.calls) == 1
    assert used(env)["evidence_bytes"] > sum(
        len(json_bytes(v)) for v in evidence(env).values()
    )


@pytest.mark.parametrize(
    "actor, operation",
    [
        ("runner", lambda t: t.publish(0, 0, True)),
        ("runner", lambda t: t.publish(2, 0, 1)),
        ("supervisor", lambda t: t.collect("../bad")),
        ("supervisor", lambda t: t.collect("ok", max_messages=0)),
    ],
)
def test_invalid_helper_arguments_do_not_reserve(setup, actor, operation):
    make, env, http, _, _, guards = setup
    guards.clear()
    with pytest.raises(Failure):
        operation(make(actor))
    assert not any(used(env).values()) and not http.calls and not guards


def test_wrong_actor_does_not_reserve(setup):
    make, env, http, _, _, guards = setup
    guards.clear()
    with pytest.raises(Failure, match="runner"):
        make("supervisor").publish(0, 0, 1)
    with pytest.raises(Failure, match="supervisor"):
        make().collect("one", max_messages=1)
    assert not any(used(env).values()) and not http.calls and not guards


@pytest.mark.parametrize(
    "setup,key",
    [
        ({"publish_calls": 1}, "publish_calls"),
        ({"input_bytes": len(b"v1|lifecycle-1361|0|0")}, "input_bytes"),
        ({"pubsub_requests": 1}, "pubsub_requests"),
    ],
    indirect=["setup"],
)
def test_publish_limits_refuse_the_next_service_call(setup, key):
    make, env, http, limits, _, _ = setup
    http.responses.append(Response({"messageIds": ["a"]}))
    make().publish(0, 0, 1)
    with pytest.raises(Failure, match=key):
        make().publish(0, 1, 1)
    assert used(env)[key] == getattr(limits, key)
    assert len(http.calls) == 1


@pytest.mark.parametrize("setup", [{"pull_calls": 1}], indirect=True)
def test_pull_call_limit_counts_an_empty_response(setup):
    make, env, http, _, _, _ = setup
    http.responses.append(Response({}))
    make("supervisor").collect("one", max_messages=1)
    with pytest.raises(Failure, match="pull_calls"):
        make("supervisor").collect("two", max_messages=1)
    assert used(env)["pull_calls"] == 1 and len(http.calls) == 1


@pytest.mark.parametrize("setup", [{"pubsub_requests": 1}], indirect=True)
def test_shared_request_limit_prevents_ack_after_durable_observations(setup):
    make, env, http, _, _, _ = setup
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="pubsub_requests"):
        make("supervisor").collect("one", max_messages=1)
    assert any(p.endswith("/observations.json") for p in evidence(env))
    assert len(http.calls) == 1 and used(env)["pubsub_requests"] == 1


def test_evidence_limit_allows_exact_boundary_then_refuses_upload_and_ack(setup):
    make, env, http, limits, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if name.endswith("/intent.json"):
            # A concurrent actor has reserved the rest of the common byte budget.
            remaining = limits.evidence_bytes - used(env)["evidence_bytes"]
            make()._reserve({"evidence_bytes": remaining}, "runner", admit=False)
        return result

    env.store.write = write
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="evidence_bytes"):
        make("supervisor").collect("one", max_messages=1)
    assert used(env)["evidence_bytes"] == limits.evidence_bytes
    assert len(evidence(env)) == 1 and len(http.calls) == 1


def test_concurrent_actors_cannot_reserve_last_evidence_byte_twice(setup):
    make, env, _, limits, _, _ = setup
    make()._reserve(
        {"evidence_bytes": limits.evidence_bytes - 1}, "runner", admit=False
    )
    env.store.before_write = lambda: make("supervisor")._reserve(
        {"evidence_bytes": 1}, "supervisor", admit=False
    )
    with pytest.raises(Failure, match="evidence_bytes"):
        make()._reserve({"evidence_bytes": 1}, "runner", admit=False)
    assert used(env)["evidence_bytes"] == limits.evidence_bytes
    assert env.store.conflicts == 1


def test_stop_after_input_intent_prevents_publication(setup):
    make, env, http, _, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if name.endswith("/intent.json"):
            env.records.request_stop()
        return result

    env.store.write = write
    with pytest.raises(Failure, match="admission"):
        make().publish(0, 0, 1)
    assert not http.calls and used(env)["pubsub_requests"] == 0
    assert len(evidence(env)) == 1


def test_cas_retry_rechecks_admission_deadline(setup):
    make, env, http, _, now, _ = setup

    def conflict():
        env.records._change(lambda r: setattr(r, "reason", "concurrent update"))
        now[0] += 100

    env.store.before_write = conflict
    with pytest.raises(Failure, match="admission"):
        make().publish(0, 0, 1)
    assert env.store.conflicts == 1
    assert not any(used(env).values()) and not http.calls


@pytest.mark.parametrize("stage", ["initialized", "preparing", "cleaning", "cleaned"])
def test_initialization_requires_prepared_resources(setup, stage):
    make, env, http, _, _, _ = setup

    def reset(record):
        record.phase = Phase.APPROVED
        record.pubsub.pop("traffic")
        record.pubsub["stage"] = stage

    env.records._change(reset)
    with pytest.raises(Failure, match="prepared"):
        make().initialize()
    assert "traffic" not in env.refresh().pubsub and not http.calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("publish_calls", True),
        ("input_bytes", 0),
        ("output_messages", 200001),
        ("admit_until", float("nan")),
        ("admit_until", float("inf")),
        ("admit_until", True),
    ],
)
def test_invalid_limits_refuse_construction(setup, field, value):
    _, _, _, limits, _, _ = setup
    with pytest.raises(Failure):
        replace(limits, **{field: value})


def test_deadline_cannot_extend_past_cleanup(setup):
    make, env, _, limits, _, _ = setup
    with pytest.raises(Failure, match="deadline"):
        make(selected=replace(limits, admit_until=env.schedule.cleanup_at + 1))


final_env = settlement.env


def test_cleanup_and_final_receipt_preserve_real_traffic_reservations(setup, final_env):
    make, env, http, _, _, _ = setup
    http.responses.append(Response({"messageIds": ["a"]}))
    make().publish(0, 0, 1)
    before = copy.deepcopy(env.refresh().pubsub["traffic"])
    controller = make("supervisor").controller
    controller.http = env.resource_http
    controller.cleanup(lambda: True)
    cleaned = env.refresh().pubsub
    assert cleaned["stage"] == "cleaned" and cleaned["traffic"] == before

    # Exercise settlement of the complete snapshot through its existing fixture.
    runner = settlement.lifecycle(final_env, settlement.cli.runner_api.Runner)
    runner.cleanup.scale_operator(1)

    def attach(record):
        record.pubsub = copy.deepcopy(cleaned)
        record.stop_requested = True

    runner.env.records._change(attach)
    runner.cleanup.scale_operator(0)
    runner.finalize(
        {
            "nonce": final_env[2]["nonce"],
            "roots": settlement.cli.wf.ROOTS,
            "empty": True,
        }
    )
    assert final_env[1].read(runner.env.records.path)[0] is None
    receipt = final_env[1].read("runs/test-1310/result.json")[0]
    assert receipt["pubsub"] == cleaned


@pytest.mark.parametrize("lost", ["reservation", "upload"])
def test_lost_evidence_response_retains_charge_without_ack(setup, lost):
    make, env, http, _, _, _ = setup
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        previous = used(env)["evidence_bytes"]
        result = original(name, value, generation, **kwargs)
        response_reservation = (
            name == env.records.path
            and value["pubsub"]["traffic"]["used"]["evidence_bytes"] > previous
            and len(evidence(env)) == 1
        )
        if (lost == "reservation" and response_reservation) or (
            lost == "upload" and name.endswith("/response.json")
        ):
            raise Failure("lost evidence response")
        return result

    env.store.write = write
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="lost evidence"):
        make("supervisor").collect("one", max_messages=1)
    saved = evidence(env)
    assert len(http.calls) == 1 and used(env)["pubsub_requests"] == 1
    assert len(saved) == (1 if lost == "reservation" else 2)
    assert used(env)["evidence_bytes"] >= sum(
        len(json_bytes(v)) for v in saved.values()
    )
    assert used(env)["evidence_bytes"] > len(json_bytes(next(iter(saved.values()))))


def test_missing_binding_refuses_and_pre_running_initialization_is_idempotent(setup):
    make, env, http, _, _, _ = setup
    env.records._change(lambda r: r.pubsub.pop("traffic"))
    with pytest.raises(Failure, match="binding"):
        make().publish(0, 0, 1)
    assert not http.calls
    env.records._change(lambda r: setattr(r, "phase", Phase.READY))
    make().initialize()
    saved = env.refresh().pubsub["traffic"]
    make().initialize()
    assert env.refresh().pubsub["traffic"] == saved


@pytest.mark.parametrize("failure", ["upload", "byte-budget", "lost-upload-response"])
def test_evidence_failure_stops_current_and_restarted_actors(setup, failure):
    make, env, http, limits, _, _ = setup
    original = env.store.write
    traffic = make("supervisor")

    def write(name, value, generation="0", **kwargs):
        if name.endswith("/response.json"):
            if failure == "lost-upload-response":
                original(name, value, generation, **kwargs)
            raise Failure("response upload failed")
        result = original(name, value, generation, **kwargs)
        if failure == "byte-budget" and name.endswith("/intent.json"):
            make()._reserve(
                {"evidence_bytes": limits.evidence_bytes - used(env)["evidence_bytes"]},
                "runner",
                admit=False,
            )
        return result

    env.store.write = write
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure):
        traffic.collect("one", max_messages=1)
    assert traffic.env.evidence_failed
    assert env.refresh().evidence_failed
    env.store.write = original
    for actor in (traffic, make("supervisor"), make()):
        with pytest.raises(Failure, match="admission"):
            if actor.env.actor == "runner":
                actor.publish(0, 0, 1)
            else:
                actor.collect("two", max_messages=1)
    assert len(http.calls) == 1


def test_failed_failure_record_keeps_local_latch_and_reports_restart_barrier(setup):
    make, env, http, _, _, _ = setup
    traffic = make("supervisor")
    original = env.store.write

    def write(name, value, generation="0", **kwargs):
        if name.endswith("/response.json") or (
            name == env.records.path and value["evidence_failed"]
        ):
            raise Failure("storage unavailable")
        return original(name, value, generation, **kwargs)

    env.store.write = write
    http.responses.append(Response({"receivedMessages": [output()]}))
    with pytest.raises(Failure, match="stop actors independently before restarting"):
        traffic.collect("one", max_messages=1)
    assert traffic.env.evidence_failed and not env.refresh().evidence_failed
    with pytest.raises(Failure, match="admission"):
        traffic.collect("two", max_messages=1)
    assert len(http.calls) == 1
