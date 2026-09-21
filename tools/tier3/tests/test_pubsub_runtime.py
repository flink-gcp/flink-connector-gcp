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
"""Common settlement composed with real Pub/Sub adapters and synthetic services."""

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests
from flink_tier3.cleanup import Cleanup, verify_idle
from flink_tier3.common import Failure, TransportError, digest
from flink_tier3.environment import Environment
from flink_tier3.model import Approval, Phase
from flink_tier3.policy import BIGQUERY, PUBSUB, PUBSUB_STATE, inventory_namespaces
from flink_tier3.pubsub_handoff import PubSubHandoff
from flink_tier3.pubsub_lifecycle import PubSubLifecycle, require_pubsub_clean
from flink_tier3.pubsub_traffic import PubSubTraffic, TrafficLimits
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_pubsub_handoff import Service, actors, deletions
from test_pubsub_messages import Response
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt


@pytest.fixture
def runtime(env, request):
    kube, store, value, clock = env
    approved_smoke = Approval.from_dict(value)
    namespaces = copy.deepcopy(approved_smoke.namespaces)
    previous = namespaces.pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=PUBSUB, uid=PUBSUB + "-quota")
    kube.put(quota)
    namespaces[PUBSUB] = {
        **previous,
        "uid": PUBSUB + "-uid",
        "quota_uid": PUBSUB + "-quota",
    }
    application = obj("FlinkDeployment", approved_smoke.run_id, PUBSUB)
    application["metadata"]["annotations"] = {rt.NONCE: approved_smoke.nonce}
    application["spec"] = {
        "image": rt.GAR + "pubsub-recovery@sha256:" + "e" * 64,
        "job": {
            "args": [
                "--run-id=" + approved_smoke.run_id,
                "--records-per-subscription=1000",
            ]
        },
    }
    # Internal composition only: the serialized value is deliberately not a
    # valid runnable approval. A separate test asserts that admission refuses it.
    approval = replace(
        approved_smoke,
        scenario="pubsub-recovery",
        namespaces=namespaces,
        application_sha256=digest(application),
        images={
            **approved_smoke.images,
            "application": application["spec"]["image"],
        },
    )
    service, guards = Service(), []
    limits = TrafficLimits(4, 4, 4, 1000, 4, 10, 200000, clock() + 1000)

    def make(role, token):
        environment = Environment(kube, store, approval, clock, clock.sleep, actor=role)
        controller = PubSubLifecycle(
            environment, service, application, lambda *args: guards.append(args)
        )
        handoff = PubSubHandoff(
            PubSubTraffic(controller, application, limits), actor_token=token
        )
        return handoff

    sender = make("runner", "b" * 32)
    observer = make("supervisor", "c" * 32)
    stage = getattr(request, "param", "running")
    if stage != "uninitialized":
        sender.initialize()
    if stage == "running":
        sender.prepare()
        sender.env.records.set_phase(Phase.READY)
        observer.join()
        sender.env.records.set_phase(Phase.RUNNING, success=True)
        sender.env.remember("application", kube.put(application))
        operator = kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
        operator["spec"]["replicas"] = operator["status"]["replicas"] = 1
        kube.put(operator)
    checkpoint = "runs/" + approval.run_id + "/checkpoints/data"
    store.write(checkpoint, {"retained": True}, bucket=PUBSUB_STATE)
    return SimpleNamespace(
        sender=sender,
        observer=observer,
        make=make,
        kube=kube,
        store=store,
        clock=clock,
        service=service,
        approval=approval,
        application=application,
        checkpoint=checkpoint,
        guards=guards,
    )


def operator_replicas(a):
    return a.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"]


def checkpoint_exists(a):
    return a.store.read(a.checkpoint, PUBSUB_STATE)[0] is not None


def supervise_on_wait(runner, callback):
    called = False

    def wait(predicate, deadline):
        nonlocal called
        if not called:
            called = True
            callback()
        return Environment.wait(runner.env, predicate, deadline)

    runner.env.wait = wait


def test_runner_releases_before_supervisor_cleanup_and_complete_settlement(runtime):
    a = runtime
    a.service.messages.responses.append(Response({"messageIds": ["one"]}))
    a.sender.publish(0, 0, 1)
    runner = Runner(a.sender.env, pubsub=a.sender)
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    events = []

    def service_call(method, _name):
        if method == "DELETE":
            assert a.kube.get("FlinkDeployment", PUBSUB, a.approval.run_id) is None
            assert operator_replicas(a) == 1 and checkpoint_exists(a)
            assert a.observer.released()
            events.append("service-delete")

    a.service.resources.before = service_call
    original = a.kube.request

    def request(method, path, body=None, **kwargs):
        if method == "PATCH" and body[-1].get("value") == 0:
            require_pubsub_clean(a.sender.env.refresh())
            assert not checkpoint_exists(a)
            events.append("operator-stop")
        return original(method, path, body, **kwargs)

    a.kube.request = request

    def cleanup():
        assert actors(a.sender.env)["runner"]["released"]
        assert a.sender.env.refresh().stop_requested
        assert not actors(a.sender.env)["supervisor"]["released"]
        with pytest.raises(Failure, match="traffic admission has stopped"):
            a.observer.collect("too-late", max_messages=1)
        supervisor.cleanup.run("synthetic completion", True)

    supervise_on_wait(runner, cleanup)
    runner.settle()
    assert events.count("service-delete") == 6
    assert events.index("operator-stop") > events.index("service-delete")
    assert not a.service.resources.resources and operator_replicas(a) == 0
    assert a.sender.env.refresh().idle and a.sender.env.refresh().success
    assert a.sender.env.refresh().pubsub["traffic"]["used"]["input_messages"] == 1
    saved = copy.deepcopy(a.sender.env.refresh().pubsub)
    # Settlement success is not a deployed recovery verdict: no Pub/Sub exercise
    # has yet supplied the final success criteria.
    assert (
        runner.finalize(
            {
                "nonce": a.approval.nonce,
                "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
                "empty": True,
            }
        )
        is False
    )
    result = a.store.read("runs/" + a.approval.run_id + "/result.json")[0]
    assert result["pubsub"] == saved and result["idle"]
    assert a.store.read(runner.env.records.path)[0] is None


def test_lost_runner_release_acknowledgement_retries_without_claiming_success(runtime):
    a = runtime
    original = a.store.write
    lost = []

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if (
            name == a.sender.env.records.path
            and not lost
            and (value["pubsub"]["handoff"]["actors"]["runner"]["released"])
        ):
            lost.append(name)
            raise Failure("lost release acknowledgement")
        return result

    a.store.write = write
    release = a.sender.release
    attempts = []

    def release_with_observation():
        attempts.append(a.clock())
        return release()

    a.sender.release = release_with_observation
    runner = Runner(a.sender.env, pubsub=a.sender)
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    supervise_on_wait(runner, lambda: supervisor.cleanup.run("stopped", True))
    runner.settle()
    assert len(lost) == 1
    assert len(attempts) == 2
    assert a.sender.env.refresh().idle and not a.sender.env.refresh().success
    assert a.observer.released() and not a.service.resources.resources
    notices = [
        a.store.read(item["name"])[0]
        for item in a.store.objects("runs/" + a.approval.run_id + "/runner/")
    ]
    notices = [item for item in notices if item["event"] == "pubsub-release-blocked"]
    assert len(notices) == 1


def test_unresolved_publication_blocks_common_cleanup_without_auto_reclamation(runtime):
    a = runtime
    a.service.messages.responses.append(requests.Timeout())
    with pytest.raises(TransportError):
        a.sender.publish(0, 0, 1)
    marker = copy.deepcopy(actors(a.sender.env)["runner"]["inflight"])
    cleanup = Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    with pytest.raises(Failure, match="Bounded wait"):
        cleanup.finish_pubsub(a.clock() + 30)
    assert actors(a.sender.env)["runner"]["inflight"] == marker
    assert actors(a.sender.env)["runner"]["fenced_call"] is None
    assert (
        not deletions(a.service) and checkpoint_exists(a) and operator_replicas(a) == 1
    )
    assert not a.sender.env.refresh().idle


@pytest.mark.parametrize("proof", [False, None, 1])
def test_common_cleanup_retains_state_and_operator_until_exact_external_proof(
    runtime, proof
):
    a = runtime
    a.sender.release()
    cleanup = Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: proof)
    with pytest.raises(Failure, match="Bounded wait"):
        cleanup.run("unproven")
    assert a.kube.get("FlinkDeployment", PUBSUB, a.approval.run_id) is None
    assert (
        not deletions(a.service) and checkpoint_exists(a) and operator_replicas(a) == 1
    )
    assert a.sender.env.refresh().phase == Phase.CLEANING
    assert not a.sender.env.refresh().idle


def test_quiescence_is_rechecked_inside_resource_cleanup(runtime):
    a = runtime
    a.sender.release()
    proofs = iter([True, False])
    cleanup = Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: next(proofs))
    with pytest.raises(Failure, match="not quiescent"):
        cleanup.finish_pubsub(a.clock() + 30)
    assert not deletions(a.service) and checkpoint_exists(a)


@pytest.mark.parametrize("runtime", ["initialized"], indirect=True)
def test_abort_before_supervisor_join_can_release_and_clean_empty_intent(runtime):
    a = runtime
    a.sender.release()
    Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: True).run("early abort")
    assert actors(a.sender.env)["supervisor"] is None
    require_pubsub_clean(a.sender.env.refresh())
    with pytest.raises(Failure, match="admission has stopped"):
        a.observer.join()
    assert not a.service.resources.calls
    assert not checkpoint_exists(a)


def test_replacement_supervisor_cannot_implicitly_release_original_actor(runtime):
    a = runtime
    a.sender.release()
    replacement = a.make("supervisor", "d" * 32)
    cleanup = Cleanup(replacement.env, pubsub=replacement, quiesce=lambda: True)
    with pytest.raises(Failure, match="identity changed"):
        cleanup.finish_pubsub(a.clock() + 30)
    assert not deletions(a.service)
    assert not actors(a.sender.env)["supervisor"]["released"]


def test_runner_without_supervisor_handoff_cannot_delete_service_or_state(runtime):
    a = runtime
    a.sender.release()
    with pytest.raises(Failure, match="Pub/Sub"):
        Cleanup(a.sender.env).run("lost supervisor")
    assert (
        not deletions(a.service) and checkpoint_exists(a) and operator_replicas(a) == 1
    )
    assert not a.sender.env.refresh().idle


@pytest.mark.parametrize("operation", ["phase", "settled", "idle"])
def test_direct_shared_completion_refuses_unfinished_pubsub(runtime, operation):
    a = runtime
    a.sender.env.records.begin_cleanup("test")
    with pytest.raises(Failure, match="Pub/Sub"):
        if operation == "phase":
            a.sender.env.records.set_phase(Phase.CLEANED)
        elif operation == "settled":
            a.sender.env.records.settled(False)
        else:
            verify_idle(a.sender.env)
    control = a.sender.env.refresh()
    assert control.phase == Phase.CLEANING and not control.idle
    assert not deletions(a.service)


def test_attached_handoff_must_belong_to_the_same_original_actor(runtime):
    a = runtime
    before = a.sender.env.refresh().to_dict()
    with pytest.raises(Failure, match="original runner"):
        Runner(a.observer.env, pubsub=a.observer)
    with pytest.raises(Failure, match="original runner"):
        Runner(a.sender.env, pubsub=a.observer)
    with pytest.raises(Failure, match="supervisor environment"):
        Supervisor(a.sender.env, pubsub=a.sender, quiesce=lambda: True)
    with pytest.raises(Failure, match="quiescence barrier"):
        Supervisor(a.observer.env, pubsub=a.observer)
    assert a.sender.env.refresh().to_dict() == before


def test_namespace_and_state_routing_do_not_enable_admission(runtime):
    a = runtime
    runner = Runner(a.sender.env, pubsub=a.sender)
    assert runner.namespace == PUBSUB
    assert runner.cleanup.state_prefixes() == [
        ("runs/" + a.approval.run_id + "/", PUBSUB_STATE)
    ]
    assert PUBSUB in inventory_namespaces(PUBSUB)
    for previous in (rt.SMOKE, "tier3-cloudtasks", BIGQUERY):
        assert PUBSUB not in inventory_namespaces(previous)
    with pytest.raises(Failure, match="Invalid run approval identity"):
        Approval.from_dict(a.approval.to_dict())
    calls = list(a.kube.calls)
    with pytest.raises(Failure, match="admission is not implemented"):
        runner.start({}, {}, a.application)
    assert a.kube.calls == calls
    supervisor = Supervisor(a.observer.env, pubsub=a.observer, quiesce=lambda: True)
    before = a.observer.env.refresh().to_dict()
    with pytest.raises(Failure, match="supervision is not implemented"):
        supervisor.supervise("unused-pod")
    assert a.kube.calls == calls
    assert a.observer.env.refresh().to_dict() == before


def test_failed_service_cleanup_keeps_state_and_operator_for_same_actor_retry(runtime):
    a = runtime
    a.sender.release()
    cleanup = Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: True)

    def fail(method, _name):
        if method == "DELETE":
            raise TransportError("uncertain service deletion")

    a.service.resources.before = fail
    with pytest.raises(TransportError, match="uncertain service deletion"):
        cleanup.run("failed deletion")
    assert checkpoint_exists(a) and operator_replicas(a) == 1
    assert a.sender.env.refresh().pubsub["stage"] == "cleaning"
    assert not a.sender.env.refresh().idle
    a.service.resources.before = lambda *_args: None
    cleanup.run("retry cleanup")
    assert not a.service.resources.resources
    assert not checkpoint_exists(a) and operator_replicas(a) == 0


def test_unresolved_supervisor_pull_is_not_released_by_common_cleanup(runtime):
    a = runtime
    a.service.messages.responses.append(requests.Timeout())
    with pytest.raises(TransportError):
        a.observer.collect("uncertain", max_messages=1)
    marker = copy.deepcopy(actors(a.sender.env)["supervisor"]["inflight"])
    a.sender.release()
    with pytest.raises(Failure, match="in flight or unresolved"):
        Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: True).run(
            "failed pull"
        )
    assert actors(a.sender.env)["supervisor"]["inflight"] == marker
    assert actors(a.sender.env)["supervisor"]["fenced_call"] is None
    assert (
        not deletions(a.service) and checkpoint_exists(a) and operator_replicas(a) == 1
    )


def test_pubsub_kubernetes_inventory_keeps_legacy_scenarios_unchanged():
    from flink_tier3.kubernetes import Kubernetes

    calls = []

    class Http:
        def json(self, method, url, body=None, **kwargs):
            calls.append((method, url))
            return {"items": []}

    client = Kubernetes("https://kubernetes.invalid", Http())
    assert client.path("Pod", PUBSUB).endswith("/namespaces/tier3-pubsub/pods")
    assert client.inventory(PUBSUB) == []
    assert any("/namespaces/tier3-pubsub/" in url for _, url in calls)
    calls.clear()
    assert client.inventory(BIGQUERY) == []
    assert not any("/namespaces/tier3-pubsub/" in url for _, url in calls)


@pytest.mark.parametrize("runtime", ["uninitialized"], indirect=True)
@pytest.mark.parametrize("boundary", ["before", "after"])
def test_initialization_crash_has_no_partial_binding_and_can_settle(runtime, boundary):
    a = runtime
    operator = a.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
    operator["spec"]["replicas"] = operator["status"]["replicas"] = 1
    a.kube.put(operator)
    original = a.store.write
    attempts = []

    def write(name, value, generation="0", **kwargs):
        if name != a.sender.env.records.path or value.get("pubsub") is None:
            return original(name, value, generation, **kwargs)
        # Even the first attempted persistent state must include its authority.
        assert value["pubsub"]["handoff"]["actors"]["runner"]["token"] == "b" * 32
        attempts.append(copy.deepcopy(value))
        if boundary == "before":
            raise SystemExit("crash before write")
        original(name, value, generation, **kwargs)
        raise SystemExit("crash after write")

    a.store.write = write
    with pytest.raises(SystemExit, match="crash"):
        a.sender.initialize()
    a.store.write = original
    assert len(attempts) == 1
    replacement = a.make("supervisor", "d" * 32)
    state = replacement.env.refresh().pubsub
    if boundary == "before":
        assert state is None
    else:
        assert state["handoff"]["actors"]["runner"]["token"] == "b" * 32
        assert state["handoff"]["actors"]["supervisor"] is None
        with pytest.raises(Failure, match="quiescence is unproven"):
            replacement.reclaim(lambda _snapshot: False)
        assert checkpoint_exists(a) and operator_replicas(a) == 1
        proofs = []

        def fenced(snapshot):
            proofs.append(snapshot)
            return True

        replacement.reclaim(fenced)
        assert proofs[0]["actors"]["runner"]["token"] == "b" * 32
        assert actors(replacement.env)["runner"]["released"]
    Cleanup(replacement.env, pubsub=replacement, quiesce=lambda: True).run("crash")
    assert replacement.env.refresh().phase == Phase.CLEANED
    assert not checkpoint_exists(a) and operator_replicas(a) == 0
    assert verify_idle(replacement.env)
    assert not a.service.resources.calls and not a.service.messages.calls


@pytest.mark.parametrize("runtime", ["uninitialized"], indirect=True)
def test_lost_initialization_acknowledgement_keeps_original_binding(runtime):
    a = runtime
    original = a.store.write
    writes = []

    def write(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if name == a.sender.env.records.path and value.get("pubsub") is not None:
            writes.append(copy.deepcopy(value["pubsub"]))
            if len(writes) == 1:
                raise Failure("lost initialization acknowledgement")
        return result

    a.store.write = write
    with pytest.raises(Failure, match="lost initialization"):
        a.sender.initialize()
    saved = copy.deepcopy(a.sender.env.refresh().pubsub)
    assert saved["handoff"]["actors"]["runner"]["token"] == "b" * 32
    a.sender.initialize()
    assert a.sender.env.refresh().pubsub == saved
    with pytest.raises(Failure, match="identity changed"):
        a.make("runner", "e" * 32).initialize()
    assert len(writes) == 2 and all(state == saved for state in writes)
    a.sender.release()
    Cleanup(a.observer.env, pubsub=a.observer, quiesce=lambda: True).run("lost ack")
    assert not checkpoint_exists(a) and not a.service.resources.calls


@pytest.mark.parametrize("runtime", ["uninitialized"], indirect=True)
@pytest.mark.parametrize("race", ["runner", "stop"])
def test_initialization_cas_rechecks_binding_and_admission(runtime, race):
    a = runtime
    competitor = a.make("runner", "e" * 32)
    a.store.before_write = (
        competitor.initialize if race == "runner" else a.sender.env.records.request_stop
    )
    with pytest.raises(Failure, match="identity changed|admission has stopped"):
        a.sender.initialize()
    assert a.store.conflicts == 1
    state = a.sender.env.refresh().pubsub
    if race == "runner":
        assert state["handoff"]["actors"]["runner"]["token"] == "e" * 32
    else:
        assert state is None and a.sender.env.refresh().stop_requested
    assert not a.service.resources.calls and not a.service.messages.calls
