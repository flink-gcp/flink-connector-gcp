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
"""Pub/Sub admission composed end to end over synthetic Kubernetes and Pub/Sub."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from flink_tier3 import runtime
from flink_tier3.common import ApiError, Failure, TransportError, digest
from flink_tier3.environment import Environment
from flink_tier3.model import Phase
from flink_tier3.policy import PUBSUB, SYSTEM
from flink_tier3.pubsub import BASE
from flink_tier3.pubsub_access import CONSUME, MEMBERS, PUBLISH, expectations, pulls
from flink_tier3.pubsub_guard import PubSubGuard, admission_deadline
from flink_tier3.pubsub_handoff import PubSubHandoff
from flink_tier3.pubsub_lifecycle import PubSubLifecycle
from flink_tier3.pubsub_quiesce import barrier
from flink_tier3.pubsub_traffic import PubSubTraffic
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_bigquery_approval import mount
from test_pubsub_actors import prepared as prepared  # noqa: PLC0414
from test_pubsub_bundle import approval as approval  # noqa: PLC0414
from test_pubsub_handoff import Service
from test_pubsub_messages import Response
from test_pubsub_plan import inputs as inputs  # noqa: PLC0414
from test_pubsub_plan import renderer as renderer  # noqa: PLC0414
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, supervisor_pod

STAMP = "2026-09-21T00:00:00Z "


class Identity:
    """One identity's view of the shared synthetic service.

    Permission tests answer for this member alone, from the installed
    policies, and only after ``lag`` earlier tests, as a propagating grant
    would. An admission pull finds nothing, because nothing is published yet.
    """

    def __init__(self, service, role, events, lag=0):
        self.service, self.member, self.events = service, MEMBERS[role], events
        self.role, self.lag, self.tests = role, lag, 0
        # Grants the explicit policies do not show, as an inherited one.
        self.inherited = {}
        self.refused_pulls = 0
        self.clock, self.cost = SimpleNamespace(now=0), 0

    def request(self, method, url, **kwargs):
        name, _, operation = url.removeprefix(BASE).partition(":")
        if operation == "testIamPermissions":
            self.tests += 1
            # A request takes this long, which the fake clock otherwise hides.
            self.clock.now += self.cost
            self.events.append(("test", self.role))
            held = set(self.inherited.get(name, ()))
            if self.tests > self.lag:
                policy = self.service.resources.policies.get(name, {})
                for binding in policy.get("bindings", []):
                    if self.member in binding["members"]:
                        held.add(
                            PUBLISH
                            if binding["role"] == "roles/pubsub.publisher"
                            else CONSUME
                        )
            asked = kwargs["json"]["permissions"]
            return Response({"permissions": sorted(set(asked) & held)})
        if operation == "pull" and kwargs["json"].get("returnImmediately"):
            self.events.append(("peek", self.role))
            if self.refused_pulls:
                # The data plane can see a grant later than the permission test.
                self.refused_pulls -= 1
                return Response({}, status=403)
            return Response({})
        if operation == "publish":
            self.events.append(("publish", self.role))
            count = len(kwargs["json"]["messages"])
            self.published = getattr(self, "published", 0) + count
            return Response(
                {"messageIds": [f"{name}-{self.published - i}" for i in range(count)]}
            )
        if method == "PUT":
            self.events.append(("create", name.rsplit("/", 1)[-1]))
        elif operation == "setIamPolicy":
            self.events.append(("grant", name.rsplit("/", 1)[-1]))
        return self.service.request(method, url, **kwargs)


def workload_log(plan, **change):
    """The lines a passing probe Pod prints, as the log API returns them."""
    expected = expectations(plan, "workload")
    lines = [
        {"event": "identity", "email": MEMBERS["workload"]},
        {
            "event": "attempt",
            "attempt": 1,
            "seen": {name: want["held"] for name, want in expected.items()},
        },
        *({"event": "pulled", "name": name} for name in pulls(plan, "workload")),
        {"event": "passed", "attempts": 1},
    ]
    for line in lines:
        line.update(change.get(line["event"], {}))
    return "".join(STAMP + json.dumps(line) + "\n" for line in lines).encode()


def actor(environment, application, http, token):
    controller = PubSubLifecycle(
        environment, http, application, PubSubGuard(environment)
    )
    return PubSubHandoff(
        PubSubTraffic(
            controller, application, environment.approval.pubsub_traffic_limits
        ),
        actor_token=token,
        settled=lambda: True,
    )


@pytest.fixture
def admitting(prepared, monkeypatch):
    environment, bundle = prepared
    kube, events, service = environment.kube, [], Service()
    application = bundle["application"]
    observer = Environment(
        kube,
        environment.store,
        environment.approval.to_dict(),
        environment.clock,
        environment.sleep,
        actor="supervisor",
    )
    sender = actor(
        environment, application, Identity(service, "runner", events), "b" * 32
    )
    receiver = actor(
        observer, application, Identity(service, "supervisor", events), "c" * 32
    )
    runner = Runner(environment, pubsub=sender)
    supervisor = Supervisor(
        observer,
        bundle["recovery_application"],
        pubsub=receiver,
        quiesce=barrier(observer),
    )
    probe_log = {"text": workload_log(environment.approval.pubsub_plan)}
    create = kube.create

    def created(manifest, dry_run=False):
        result = create(manifest, dry_run)
        if dry_run:
            return result
        if manifest["kind"] == "Job":
            pod = supervisor_pod(
                (kube, None, environment.approval.to_dict(), None), result
            )
            observer.records.claim_supervisor(pod["metadata"]["uid"])
            observer.records.heartbeat()
        elif manifest["kind"] == "Pod":
            events.append(("probe", "created"))
            result["status"] = {"phase": "Succeeded"}
            result = kube.put(result)
        elif manifest["kind"] == "FlinkDeployment":
            events.append(("application", "created"))
        return result

    def logs(pod, _since=None):
        if pod["metadata"]["name"].endswith("-access-probe"):
            return probe_log["text"]
        return b"final log\n"

    delete = kube.delete

    def deleted(value, force=False):
        if value["kind"] == "Pod" and value["metadata"]["name"].endswith("-probe"):
            events.append(("probe", "deleted"))
        return delete(value, force)

    monkeypatch.setattr(kube, "create", created)
    monkeypatch.setattr(kube, "logs", logs)
    monkeypatch.setattr(kube, "delete", deleted)
    participated = []
    original_sleep = environment.sleep

    def sleep(seconds):
        original_sleep(seconds)
        state = environment.refresh().pubsub or {}
        if not participated and state.get("traffic") is not None:
            participated.append(True)
            supervisor.participate()
            events.append(("supervisor", "participated"))

    monkeypatch.setattr(environment, "sleep", sleep)
    config = obj("ConfigMap", "source", SYSTEM)
    config["data"] = bundle["delivery"]["config"]["data"]
    return SimpleNamespace(
        environment=environment,
        observer=observer,
        runner=runner,
        supervisor=supervisor,
        sender=sender,
        receiver=receiver,
        service=service,
        events=events,
        probe_log=probe_log,
        args=(
            config,
            obj("Job", "supervisor", SYSTEM),
            application,
            copy.deepcopy(bundle["delivery"]["probe"]),
        ),
    )


def positions(events, *wanted):
    return [events.index(event) for event in wanted]


def test_admission_runs_the_ordered_preparation_before_the_application(admitting):
    a = admitting
    a.runner.start(*a.args)
    control = a.environment.refresh()
    assert control.phase == Phase.RUNNING
    events = a.events
    creates = [i for i, e in enumerate(events) if e[0] == "create"]
    grants = [i for i, e in enumerate(events) if e[0] == "grant"]
    tests = [i for i, e in enumerate(events) if e[0] == "test"]
    publishes = [i for i, e in enumerate(events) if e[0] == "publish"]
    probe_created, probe_deleted, participated, application = positions(
        events,
        ("probe", "created"),
        ("probe", "deleted"),
        ("supervisor", "participated"),
        ("application", "created"),
    )
    # Created before granted, granted before any identity is probed.
    assert len(creates) == 6 and len(grants) == 6
    assert max(creates) < min(grants) < max(grants) < min(tests)
    # The runner probes itself, then the workload's Pod runs and is removed,
    # then the supervisor has joined and probed itself.
    assert max(grants) < min(tests) < probe_created < probe_deleted < participated
    # Nothing is published until every identity passed, and the application
    # is created last, with its input waiting.
    assert participated < min(publishes) and max(publishes) < application
    cohort = a.environment.approval.pubsub_trial["records_per_subscription"] // 3
    assert len(publishes) == 2 * -(-cohort // 100)
    cohorts = control.pubsub["cohorts"]
    assert list(cohorts) == ["before_checkpoint"]
    assert cohorts["before_checkpoint"]["requested_at"] is None
    assert (
        cohorts["before_checkpoint"]["started_at"]
        <= cohorts["before_checkpoint"]["published_at"]
    )
    access = control.pubsub["access"]
    assert set(access) == {"runner", "workload", "supervisor"}
    assert access["supervisor"]["pulled"] == pulls(
        a.environment.approval.pubsub_plan, "supervisor"
    )
    assert control.pubsub["handoff"]["actors"]["supervisor"] is not None
    assert "probe" not in [k for k, v in control.roots.items() if a.environment.root(k)]
    assert control.roots["application"]["namespace"] == PUBSUB


def test_later_cohorts_run_under_the_actors_production_guards(admitting):
    """The cohort methods' bounds are those the production guard reserves."""
    a = admitting
    a.runner.start(*a.args)
    a.receiver.request_cohort("after_checkpoint", deadline=a.observer.clock() + 120)
    published = a.events.count(("publish", "runner"))
    assert a.sender.serve() == "after_checkpoint"
    cohort = a.environment.approval.pubsub_trial["records_per_subscription"] // 3
    assert a.events.count(("publish", "runner")) - published == 2 * -(-cohort // 100)
    assert a.receiver.cohorts()["after_checkpoint"]["published_at"] is not None
    assert a.sender.serve() is None


def test_admission_rechecks_itself_before_every_batch(admitting, monkeypatch):
    a = admitting
    publish, admission_open = a.sender.publish, a.runner.admission_open
    sent = []

    def published(*args, **kwargs):
        sent.append(args)
        return publish(*args, **kwargs)

    def checked():
        if sent:
            raise Failure("Admission closed between batches")
        return admission_open()

    monkeypatch.setattr(a.sender, "publish", published)
    monkeypatch.setattr(a.runner, "admission_open", checked)
    with pytest.raises(Failure, match="between batches"):
        a.runner.start(*a.args)
    assert len(sent) == 1


def kinds(events, kind):
    return [e for e in events if e[0] == kind]


def test_grants_still_propagating_are_waited_for(admitting):
    a = admitting
    runner_http = a.sender.controller.http
    runner_http.lag = 6 * 3  # three whole attempts see no grant yet
    start = a.environment.clock()
    a.runner.start(*a.args)
    access = a.environment.refresh().pubsub["access"]["runner"]
    assert access["attempts"] == 4
    assert a.environment.clock() - start >= 3 * 15


def test_grants_that_never_take_effect_stop_admission_cleanably(admitting):
    a = admitting
    a.sender.controller.http.lag = 10**6
    with pytest.raises(Failure, match="did not take effect before the deadline"):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish") and ("application", "created") not in a.events
    # Every write was answered, so the failure left nothing in flight.
    actors = a.environment.refresh().pubsub["handoff"]["actors"]
    assert actors["runner"]["inflight"] is None
    a.sender.release()
    # The supervisor joined while the runner waited, so it releases too.
    a.receiver.release()
    a.receiver.cleanup(lambda: True)
    assert a.environment.refresh().pubsub["stage"] == "cleaned"
    assert not a.service.resources.resources


def test_access_an_identity_must_not_have_is_refused_without_waiting(admitting):
    """A grant the explicit policies cannot show, such as an inherited one."""
    a = admitting
    plan = a.environment.approval.pubsub_plan
    output = plan.subscriptions()[-1]["name"]
    a.sender.controller.http.inherited = {output: {CONSUME}}
    # Its own grants are still propagating: the refusal must not wait for them.
    a.sender.controller.http.lag = 18
    start = a.environment.clock()
    with pytest.raises(Failure, match="holds access it must not have"):
        a.runner.start(*a.args)
    assert a.environment.clock() == start


@pytest.mark.parametrize(
    "change, match",
    [
        ({"identity": {"email": MEMBERS["runner"]}}, "did not pass"),
        ({"passed": {"event": "refused"}}, "did not pass"),
        ({"attempt": {"seen": {}}}, "other resources"),
        ({"pulled": {"name": "projects/p/subscriptions/other"}}, "do not meet"),
        ("failed", "did not succeed"),
        ("oversized", "read ceiling"),
        ("malformed", "malformed"),
    ],
)
def test_the_runner_judges_the_workload_probe_by_its_own_reading(
    admitting, monkeypatch, change, match
):
    a = admitting
    plan = a.environment.approval.pubsub_plan
    if change == "oversized":
        a.probe_log["text"] = b"x" * (64 * 1024)
    elif change == "malformed":
        a.probe_log["text"] = (STAMP + "not json\n").encode()
    elif change != "failed":
        a.probe_log["text"] = workload_log(plan, **change)
    if change == "failed":
        create = a.environment.kube.create

        def failing(manifest, dry_run=False):
            result = create(manifest, dry_run)
            if manifest["kind"] == "Pod" and not dry_run:
                result["status"] = {"phase": "Failed"}
                a.environment.kube.put(result)
            return result

        monkeypatch.setattr(a.environment.kube, "create", failing)
    with pytest.raises(Failure, match=match):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish") and ("application", "created") not in a.events
    # The Pod stays for cleanup, which deletes the probe root with the rest.
    assert "probe" in a.environment.refresh().roots


def test_drift_found_by_the_final_readback_stops_before_any_input(admitting):
    a = admitting
    original = a.supervisor.participate

    def participate():
        original()
        name = next(n for n in a.service.resources.resources if "/subscriptions/" in n)
        a.service.resources.resources[name]["ackDeadlineSeconds"] += 1

    a.supervisor.participate = participate
    with pytest.raises(Failure, match="settings"):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish")


@pytest.mark.parametrize(
    "tamper, match",
    [
        ("probe-args", "access probe differs"),
        ("probe-command", "access probe differs"),
        ("probe-account", "access probe differs"),
        ("probe-namespace", "access probe differs"),
        ("probe-name", "access probe differs"),
        ("probe-restart", "access probe differs"),
        ("probe-image", "image differs"),
        ("probe-resources", "resources exceed or differ"),
        ("probe-spot", "Spot"),
        ("probe-missing", "access probe differs"),
        ("application", "Application differs from the approved manifest"),
        ("recovery", "Application differs from the approved manifest"),
        ("unguarded", "authenticated, guarded handoff"),
        ("unsettled", "authenticated, guarded handoff"),
    ],
)
def test_tampered_admission_inputs_are_refused_before_any_write(
    admitting, tamper, match
):
    a = admitting
    config, job, application, probe = a.args
    container = probe["spec"]["containers"][0]
    if tamper == "probe-args":
        container["args"] = ["{}"]
    elif tamper == "probe-command":
        container["command"] = ["python3", "-c", "print(1)"]
    elif tamper == "probe-account":
        probe["spec"]["serviceAccountName"] = "default"
    elif tamper == "probe-namespace":
        probe["metadata"]["namespace"] = SYSTEM
    elif tamper == "probe-name":
        probe["metadata"]["name"] += "-2"
    elif tamper == "probe-restart":
        probe["spec"]["restartPolicy"] = "OnFailure"
    elif tamper == "probe-image":
        container["image"] += "0"
    elif tamper == "probe-resources":
        container["resources"]["limits"]["cpu"] = "2"
    elif tamper == "probe-spot":
        del probe["spec"]["affinity"]
    elif tamper == "probe-missing":
        probe = None
    elif tamper == "application":
        application = {**application, "spec": {**application["spec"], "extra": 1}}
    elif tamper == "recovery":
        recovery = json.loads(config["data"]["upgrade-application.json"])
        recovery["spec"]["job"]["parallelism"] = 9
        config["data"]["upgrade-application.json"] = json.dumps(recovery)
    elif tamper == "unguarded":
        a.sender.controller.before_operation = lambda *args: None
    else:
        a.sender.settled = None
    before = a.environment.refresh().to_dict()
    with pytest.raises(Failure, match=match):
        a.runner.start(config, job, application, probe)
    assert a.environment.refresh().to_dict() == before
    assert not a.events


def test_a_supervisor_that_never_joins_stops_admission_at_its_deadline(
    admitting, monkeypatch
):
    a = admitting
    a.supervisor.participate = lambda: None
    with pytest.raises(Failure, match="Bounded wait expired"):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish")
    assert a.environment.clock() >= admission_deadline(a.environment) - 15


def test_admission_closes_at_its_deadline(admitting):
    a = admitting
    a.environment.clock.now = admission_deadline(a.environment)
    with pytest.raises(Failure, match="admission deadline expired"):
        a.runner.start(*a.args)
    assert not a.events


def supervisor_uid(a):
    return a.observer.refresh().supervisor_pod


def runner_releases_while_supervisor_waits(a, monkeypatch, *, settle_first=True):
    """The runner settles concurrently: here, on the supervisor's first wait."""
    original, released = a.observer.sleep, []

    def sleep(seconds):
        original(seconds)
        if not released:
            released.append(True)
            a.sender.release()

    monkeypatch.setattr(a.observer, "sleep", sleep)
    return released


def test_a_supervised_run_stops_at_the_exercise_boundary_and_cleans(
    admitting, monkeypatch
):
    a = admitting
    a.runner.start(*a.args)
    runner_releases_while_supervisor_waits(a, monkeypatch)
    a.supervisor.supervise(supervisor_uid(a))
    control = a.environment.refresh()
    assert control.pubsub["stage"] == "cleaned"
    assert "exercise is not implemented (#1431)" in control.reason
    assert not a.service.resources.resources
    actors = control.pubsub["handoff"]["actors"]
    assert actors["runner"]["released"] and actors["supervisor"]["released"]


def test_a_joined_supervisor_stopped_during_admission_cleans_itself(
    admitting, monkeypatch
):
    """Its authority cannot be released by a replacement, so it is not left to one."""
    a = admitting
    a.sender.initialize()
    a.sender.prepare()
    a.environment.records.set_phase(Phase.READY)
    a.supervisor.participate()
    a.observer.stopping = True
    runner_releases_while_supervisor_waits(a, monkeypatch)
    a.supervisor.conclude("terminated", False)
    assert a.environment.refresh().pubsub["stage"] == "cleaned"
    assert not a.service.resources.resources


def replacement(a):
    # Its own process: none of the fixture's hooks on the runner's sleep.
    observer = Environment(
        a.environment.kube,
        a.environment.store,
        a.environment.approval.to_dict(),
        a.environment.clock,
        a.environment.clock.sleep,
        actor="supervisor",
    )
    handoff = actor(
        observer,
        a.args[2],
        Identity(a.service, "supervisor", a.events),
        "d" * 32,
    )
    return Supervisor(
        observer,
        json.loads(a.args[0]["data"]["upgrade-application.json"]),
        pubsub=handoff,
        quiesce=barrier(observer),
    )


@pytest.mark.parametrize(
    "case, reclaims",
    [
        ("Failed", True),
        ("absent", True),
        ("Running", False),
        ("unreleased", False),
        # A writer still in the namespace is waited for, not refused.
        ("writer-leaves", True),
    ],
)
def test_a_replacement_reclaims_a_joined_supervisor_only_once_it_ended(
    admitting, monkeypatch, case, reclaims
):
    a = admitting
    prepare_for_supervisor(a)
    a.supervisor.participate()
    kube = a.environment.kube
    job = kube.put(obj("Job", "lifecycle", SYSTEM))
    pod = kube.put(
        supervisor_pod((kube, None, a.environment.approval.to_dict(), None), job)
    )
    if case == "absent":
        kube.data.pop(("Pod", SYSTEM, pod["metadata"]["name"]))
    else:
        pod["status"] = {"phase": "Running" if case == "Running" else "Failed"}
        kube.put(pod)
    successor = replacement(a)
    successor.cleanup.former_supervisor = pod["metadata"]["uid"]
    with pytest.raises(Failure, match="replaced after joining"):
        successor.participate()
    assert a.environment.refresh().stop_requested
    if case != "unreleased":
        a.sender.release()
    if case == "writer-leaves":
        writer = kube.put(obj("Pod", "stray-writer", PUBSUB))
        original = successor.env.sleep

        def sleep(seconds):
            original(seconds)
            if kube.get("Pod", PUBSUB, "stray-writer"):
                kube.data.pop(("Pod", PUBSUB, writer["metadata"]["name"]))

        monkeypatch.setattr(successor.env, "sleep", sleep)
    if not reclaims:
        with pytest.raises(Failure, match="Bounded wait expired"):
            successor.cleanup.finish_pubsub(a.environment.clock() + 60)
        assert a.service.resources.resources
        return
    successor.cleanup.finish_pubsub(a.environment.clock() + 60)
    state = a.environment.refresh().pubsub
    assert state["stage"] == "cleaned" and not a.service.resources.resources
    assert state["handoff"]["actors"]["supervisor"]["released"]


def test_a_valid_pubsub_delivery_reaches_the_entrypoint_s_cluster_step(
    prepared, tmp_path
):
    """The delivery carries the recovery manifest the supervisor factory checks."""
    environment, bundle = prepared
    mount(
        tmp_path,
        environment,
        bundle["application"],
        bundle["recovery_application"],
    )
    _, approved, application, upgrade, cells = runtime.verify_delivery(tmp_path)
    assert approved.scenario == "pubsub-recovery"
    assert application == bundle["application"]
    assert upgrade == bundle["recovery_application"] and cells is None
    with pytest.raises(FileNotFoundError, match="serviceaccount"):
        runtime.supervisor_main(tmp_path)


@pytest.mark.parametrize(
    "case, match",
    [
        ("missing-upgrade", "missing upgrade-application.json"),
        ("changed-upgrade", "upgrade differs from approval"),
    ],
)
def test_the_entrypoint_refuses_a_pubsub_delivery_without_its_recovery(
    prepared, tmp_path, case, match
):
    environment, bundle = prepared
    upgrade = (
        None
        if case == "missing-upgrade"
        else {**bundle["recovery_application"], "x": 1}
    )
    mount(tmp_path, environment, bundle["application"], upgrade)
    with pytest.raises(Failure, match=match):
        runtime.supervisor_main(tmp_path)


def prepare_for_supervisor(a):
    a.sender.initialize()
    a.sender.prepare()
    a.environment.records.set_phase(Phase.READY)


@pytest.mark.parametrize(
    "who, role",
    [("receiver", "workload"), ("receiver", "runner"), ("sender", "supervisor")],
)
def test_access_is_recorded_only_by_the_identity_s_prober(admitting, who, role):
    a = admitting
    prepare_for_supervisor(a)
    controller = getattr(a, who).controller
    with controller.reserve("record"), pytest.raises(Failure, match="recorded by"):
        controller.record_access(role, {"attempts": 1})
    assert "access" not in a.environment.refresh().pubsub


def failed_probe(a, monkeypatch):
    create = a.environment.kube.create

    def failing(manifest, dry_run=False):
        result = create(manifest, dry_run)
        if manifest["kind"] == "Pod" and not dry_run:
            result["status"] = {"phase": "Failed"}
            a.environment.kube.put(result)
        return result

    monkeypatch.setattr(a.environment.kube, "create", failing)
    with pytest.raises(Failure, match="did not succeed"):
        a.runner.start(*a.args)


def test_a_probe_pod_is_audited_as_a_control_shape_in_the_application_namespace(
    admitting, monkeypatch
):
    a = admitting
    failed_probe(a, monkeypatch)
    _items, pods = a.runner.cleanup.audit()
    probes = [p for p in pods if p["metadata"]["name"].endswith("-access-probe")]
    assert len(probes) == 1 and probes[0]["metadata"]["namespace"] == PUBSUB


def test_cleanup_removes_a_probe_pod_the_runner_left(admitting, monkeypatch):
    a = admitting
    failed_probe(a, monkeypatch)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    a.supervisor.cleanup.run("probe failed", False)
    assert not [
        pod
        for pod in a.environment.kube.items("Pod", PUBSUB)
        if pod["metadata"]["name"].endswith("-access-probe")
    ]
    assert a.environment.refresh().pubsub["stage"] == "cleaned"


def test_the_application_waits_until_the_probe_pod_is_gone(admitting, monkeypatch):
    """No other Pod of the run stands beside the application when it starts."""
    a = admitting
    kube = a.environment.kube
    delete, create = kube.delete, kube.create
    terminating = []

    def slow_delete(value, force=False):
        if value["kind"] == "Pod" and value["metadata"]["name"].endswith("-probe"):
            current = kube.get("Pod", PUBSUB, value["metadata"]["name"])
            current["metadata"]["deletionTimestamp"] = "2026-09-21T00:00:00Z"
            kube.put(current)
            terminating.append(value)
            return True
        return delete(value, force)

    def created(manifest, dry_run=False):
        if manifest["kind"] == "FlinkDeployment" and not dry_run:
            assert not [
                p
                for p in kube.items("Pod", PUBSUB)
                if p["metadata"]["name"].endswith("-probe")
            ], "application created while the probe Pod still terminates"
        result = create(manifest, dry_run)
        if manifest["kind"] == "Pod" and not dry_run:
            # The supervisor is already in, so nothing else waits between the
            # probe's deletion and the application's creation.
            a.supervisor.participate()
        return result

    original_sleep = a.environment.sleep

    def sleep(seconds):
        original_sleep(seconds)
        while terminating:
            delete(terminating.pop())

    monkeypatch.setattr(kube, "delete", slow_delete)
    monkeypatch.setattr(kube, "create", created)
    monkeypatch.setattr(a.environment, "sleep", sleep)
    a.runner.start(*a.args)
    assert a.environment.refresh().phase == Phase.RUNNING


def test_a_recovery_manifest_repinned_but_running_another_job_is_refused(admitting):
    """A digest pins a manifest; only the trial's job says the manifest is right."""
    a = admitting
    config, job, application, probe = a.args
    recovery = json.loads(config["data"]["upgrade-application.json"])
    recovery["spec"]["job"]["args"][-1] = "--entry-point=table"
    config["data"]["upgrade-application.json"] = json.dumps(recovery)
    a.environment.approval = replace(
        a.environment.approval, upgrade_application_sha256=digest(recovery)
    )
    with pytest.raises(Failure, match="approved trial"):
        a.runner.start(config, job, application, probe)
    assert not a.events


def test_cleanup_deletes_a_left_probe_before_forcing_anything(admitting, monkeypatch):
    a = admitting
    failed_probe(a, monkeypatch)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    kube = a.environment.kube
    probe_uid = a.environment.refresh().roots["probe"]["uid"]
    start = len(kube.calls)
    a.supervisor.cleanup.run("probe failed", False)
    calls = kube.calls[start:]
    deleted = calls.index(("delete", "Pod", probe_uid))
    quotas = [i for i, call in enumerate(calls) if call[0] == "patch"]
    assert not quotas or deleted < quotas[0]


def test_the_actors_pull_what_they_read_and_retry_a_refused_pull(admitting):
    a = admitting
    a.receiver.controller.http.refused_pulls = 1
    a.runner.start(*a.args)
    peeks = [event for event in a.events if event[0] == "peek"]
    # The runner reads no subscription; the supervisor reads the output.
    assert peeks == [("peek", "supervisor"), ("peek", "supervisor")]
    access = a.environment.refresh().pubsub["access"]["supervisor"]
    assert access["pull_attempts"] == 2


def test_the_workload_probe_is_kept_as_evidence(admitting, monkeypatch):
    a = admitting
    kept = []
    original = a.environment.records.evidence

    def evidence(name, value, *args, **kwargs):
        kept.append((name, value))
        return original(name, value, *args, **kwargs)

    monkeypatch.setattr(a.environment.records, "evidence", evidence)
    a.runner.start(*a.args)
    [(_, value)] = [entry for entry in kept if entry[0] == "pubsub-workload-probe"]
    assert value["phase"] == "Succeeded"
    assert '"event": "passed"' in value["text"]


def test_a_supervisor_that_joined_but_did_not_record_access_does_not_admit(
    admitting,
):
    a = admitting
    a.supervisor.participate = lambda: a.receiver.join()
    with pytest.raises(Failure, match="Bounded wait expired"):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish")


def test_the_application_quota_opens_before_the_probe_pod(admitting):
    a = admitting
    kube = a.environment.kube
    a.runner.start(*a.args)
    calls = kube.calls
    quota = next(
        i
        for i, call in enumerate(calls)
        if call[0] == "patch" and call[1] == "ResourceQuota" and PUBSUB in call[2]
    )
    probe = calls.index(("create", "Pod", False))
    assert quota < probe


def test_a_supervisor_job_that_ends_during_the_probe_stops_admission(
    admitting, monkeypatch
):
    a = admitting
    kube = a.environment.kube
    create = kube.create

    def created(manifest, dry_run=False):
        result = create(manifest, dry_run)
        if manifest["kind"] == "Pod" and not dry_run:
            result["status"] = {"phase": "Running"}
            kube.put(result)
            job = a.environment.root("supervisor")
            job["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
            kube.put(job)
        return result

    monkeypatch.setattr(kube, "create", created)
    with pytest.raises(Failure, match="Supervisor stopped"):
        a.runner.start(*a.args)
    assert not kinds(a.events, "publish")


def test_an_existing_probe_name_is_refused_before_creation(admitting, monkeypatch):
    a = admitting
    quota = a.runner.cleanup.quota

    def opened(namespace, admission):
        quota(namespace, admission)
        if namespace == PUBSUB and admission == "run":
            # Appears after the last audit, before the probe is created.
            a.environment.kube.put(obj("Pod", a.args[3]["metadata"]["name"], PUBSUB))

    monkeypatch.setattr(a.runner.cleanup, "quota", opened)
    with pytest.raises(Failure, match="name already exists"):
        a.runner.start(*a.args)
    assert ("probe", "created") not in a.events


def test_a_probe_whose_creation_response_was_lost_is_adopted(admitting):
    a = admitting
    probe = a.args[3]
    a.environment.records.intend("probe", probe)
    created = a.environment.kube.put({**probe, "metadata": {**probe["metadata"]}})
    created["metadata"]["uid"] = "probe-uid"
    a.environment.kube.put(created)
    a.runner.adopt_root("probe")
    assert a.environment.refresh().roots["probe"]["namespace"] == PUBSUB


def test_a_released_actor_cannot_probe_and_access_is_recorded_once_prepared(
    admitting,
):
    a = admitting
    a.sender.initialize()
    with (
        a.sender.controller.reserve("record"),
        pytest.raises(Failure, match="not prepared"),
    ):
        a.sender.controller.record_access("runner", {"attempts": 1})
    a.sender.prepare()
    a.environment.records.set_phase(Phase.READY)
    a.sender.release()
    with pytest.raises(Failure, match="released"):
        a.sender.access(deadline=admission_deadline(a.environment))


def test_participation_stops_on_a_stop_and_does_not_probe_twice(admitting):
    a = admitting
    prepare_for_supervisor(a)
    a.supervisor.participate()
    tests = len(kinds(a.events, "test"))
    a.supervisor.participate()
    assert len(kinds(a.events, "test")) == tests
    a.environment.records.request_stop()
    with pytest.raises(Failure, match="Cancellation"):
        a.supervisor.participate()


def test_a_stopped_supervisor_waits_for_the_runner_before_deleting(
    admitting, monkeypatch
):
    a = admitting
    prepare_for_supervisor(a)
    a.supervisor.participate()
    a.observer.stopping = True
    kube = a.environment.kube
    delete = kube.delete
    order = []

    def deleted(value, force=False):
        order.append(("delete", value["kind"]))
        return delete(value, force)

    monkeypatch.setattr(kube, "delete", deleted)
    original = a.observer.sleep

    def sleep(seconds):
        original(seconds)
        if ("released",) not in order:
            order.append(("released",))
            a.sender.release()

    monkeypatch.setattr(a.observer, "sleep", sleep)
    a.supervisor.conclude("terminated", False)
    assert order[0] == ("released",)


def supervised_by(a, job, pod):
    """Record the Job as the run's supervisor root, its Pod as the holder."""
    a.environment.remember("supervisor", job)
    a.observer.records.claim_supervisor(pod["metadata"]["uid"])


@pytest.mark.parametrize(
    "former, released, reclaimed",
    [
        ("Failed", True, True),
        ("Pending", True, False),
        ("Failed", False, False),
    ],
)
def test_a_replacement_supervisor_reclaims_through_its_own_supervision(
    admitting, monkeypatch, former, released, reclaimed
):
    """Driven through `supervise`, which records the holder it displaces."""
    a = admitting
    kube = a.environment.kube
    job = kube.put(obj("Job", "lifecycle", SYSTEM))
    original = kube.put(
        supervisor_pod((kube, None, a.environment.approval.to_dict(), None), job)
    )
    supervised_by(a, job, original)
    prepare_for_supervisor(a)
    a.supervisor.participate()
    original["status"] = {"phase": former}
    kube.put(original)
    successor = replacement(a)
    pod = obj("Pod", "supervisor-pod-2", SYSTEM, job["metadata"]["uid"])
    pod["spec"] = copy.deepcopy(original["spec"])
    pod["status"] = {"phase": "Running"}
    kube.put(pod)
    if released:
        runner_released_on_first_wait(a, successor, monkeypatch)
    if reclaimed:
        successor.supervise(pod["metadata"]["uid"])
        reason = a.environment.refresh().reason
        assert "replaced after joining" in reason
    else:
        # A former Pod that may still run, or a runner that never released,
        # keeps the run and its lock rather than reclaiming.
        with pytest.raises(Failure, match="Bounded wait expired"):
            successor.supervise(pod["metadata"]["uid"])
    state = a.environment.refresh().pubsub
    assert successor.cleanup.former_supervisor == original["metadata"]["uid"]
    if reclaimed:
        assert state["stage"] == "cleaned" and not a.service.resources.resources
        assert state["handoff"]["actors"]["supervisor"]["released"]
    else:
        assert state["stage"] != "cleaned" and a.service.resources.resources


def runner_released_on_first_wait(a, successor, monkeypatch):
    original, done = successor.env.sleep, []

    def sleep(seconds):
        original(seconds)
        if not done:
            done.append(True)
            a.sender.release()

    monkeypatch.setattr(successor.env, "sleep", sleep)


def test_a_local_stop_ends_the_wait_for_access(admitting, monkeypatch):
    a = admitting
    http = a.sender.controller.http
    http.lag = 10**6
    original = http.request

    def request(method, url, **kwargs):
        if url.endswith(":testIamPermissions"):
            a.environment.stopping = True  # a SIGTERM while grants propagate
        return original(method, url, **kwargs)

    monkeypatch.setattr(http, "request", request)
    start = a.environment.clock()
    with pytest.raises(Failure, match="admission has stopped"):
        a.runner.start(*a.args)
    assert a.environment.clock() - start < 60


def test_slow_tests_end_the_wait_before_a_round_could_overrun_the_deadline(admitting):
    """Each round sends six tests in turn; the wait budgets all of them."""
    a = admitting
    http = a.sender.controller.http
    http.lag, http.clock, http.cost = 10**6, a.environment.clock, 10
    with pytest.raises(Failure, match="did not take effect before the deadline"):
        a.runner.start(*a.args)


def test_a_supervisor_with_an_unguarded_handoff_refuses_to_supervise(admitting):
    a = admitting
    a.receiver.controller.before_operation = lambda *args: None
    with pytest.raises(Failure, match="requires its authenticated handoff"):
        a.supervisor.supervise("unused-pod")


def test_a_stopped_supervisor_leaves_the_application_until_the_runner_released(
    admitting, monkeypatch
):
    """Admission may still be creating it; release says that has settled."""
    a = admitting
    prepare_for_supervisor(a)
    a.supervisor.participate()
    kube = a.environment.kube
    application = kube.put(
        obj("FlinkDeployment", a.environment.approval.run_id, PUBSUB)
    )
    a.environment.remember("application", application)
    a.observer.stopping = True
    delete = kube.delete
    order = []

    def deleted(value, force=False):
        order.append(("delete", value["kind"]))
        return delete(value, force)

    monkeypatch.setattr(kube, "delete", deleted)
    original = a.observer.sleep

    def sleep(seconds):
        original(seconds)
        if ("released",) not in order:
            order.append(("released",))
            a.sender.release()

    monkeypatch.setattr(a.observer, "sleep", sleep)
    a.supervisor.conclude("terminated", False)
    assert order.index(("released",)) < order.index(("delete", "FlinkDeployment"))


def intended_probe(a, **change):
    """The runner persisted its intent; the Pod it names is returned, unplaced."""
    probe = copy.deepcopy(a.args[3])
    a.environment.records.intend("probe", probe)
    landed = copy.deepcopy(probe)
    landed["metadata"]["uid"] = "probe-uid"
    landed["status"] = {"phase": "Succeeded"}
    landed["spec"]["containers"][0].update(change)
    return landed


def probe_pods(a):
    return [
        pod
        for pod in a.environment.kube.items("Pod", PUBSUB)
        if pod["metadata"]["name"].endswith("-access-probe")
    ]


@pytest.mark.parametrize(
    "lands", ["before-cleanup", "during-cleanup", "after-the-loop"]
)
def test_cleanup_deletes_a_probe_only_the_runner_s_intent_names(
    admitting, monkeypatch, lands
):
    """A lost creation response leaves the Pod unrecorded, and it may land late."""
    a = admitting
    prepare_for_supervisor(a)
    landed = intended_probe(a)
    cleanup = a.supervisor.cleanup
    if lands == "after-the-loop":
        # The deletion loop found the namespace clear; the barrier is next.
        finish = cleanup.finish_pubsub

        def late(deadline):
            a.environment.kube.put(copy.deepcopy(landed))
            return finish(deadline)

        monkeypatch.setattr(cleanup, "finish_pubsub", late)
    else:
        # The first inventory precedes the loop; the second is its first pass.
        original, calls = cleanup.inventory, []
        at = 1 if lands == "before-cleanup" else 2

        def inventory():
            calls.append(True)
            if len(calls) == at:
                a.environment.kube.put(copy.deepcopy(landed))
            return original()

        monkeypatch.setattr(cleanup, "inventory", inventory)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    cleanup.run("probe response lost", False)
    assert not probe_pods(a)
    assert a.environment.refresh().pubsub["stage"] == "cleaned"


def test_cleanup_leaves_a_probe_pod_that_differs_from_the_intent(admitting):
    """The runner's adoption refuses it too; the barrier reports it instead."""
    a = admitting
    prepare_for_supervisor(a)
    landed = intended_probe(a, image="example.invalid/other@sha256:" + "0" * 64)
    a.environment.kube.put(landed)
    cleanup = a.supervisor.cleanup
    emitted = []
    cleanup.env.emit = lambda event, payload: emitted.append((event, payload))
    assert cleanup.adopt_intended_probe(cleanup.inventory()) is None
    assert "probe" not in cleanup.env.roots
    assert ("probe-adoption-refused", {"uid": "probe-uid"}) in emitted


def test_cleanup_deletes_an_intended_probe_it_could_not_record(admitting, monkeypatch):
    a = admitting
    prepare_for_supervisor(a)
    a.environment.kube.put(intended_probe(a))
    cleanup = a.supervisor.cleanup
    remember = cleanup.env.remember

    def refuse_probe(key, obj):
        if key == "probe":
            raise Failure("control record unavailable")
        return remember(key, obj)

    monkeypatch.setattr(cleanup.env, "remember", refuse_probe)
    passes, deleted = [], []
    inventory, delete = cleanup.inventory, cleanup.env.kube.delete

    def counted():
        passes.append(True)
        return inventory()

    def recorded(obj):
        if obj["metadata"]["name"].endswith("-access-probe"):
            deleted.append(len(passes))
        return delete(obj)

    monkeypatch.setattr(cleanup, "inventory", counted)
    monkeypatch.setattr(cleanup.env.kube, "delete", recorded)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    cleanup.run("probe response lost", False)
    assert not probe_pods(a)
    assert cleanup.env.evidence_failed
    # Deleted on the loop's first pass, not left for the force window.
    assert deleted[0] == 2


def test_a_late_probe_is_deleted_again_after_a_failed_delete(admitting, monkeypatch):
    """Recorded on the first poll, it is still retried from its root."""
    a = admitting
    prepare_for_supervisor(a)
    landed = intended_probe(a)
    cleanup = a.supervisor.cleanup
    finish, delete, failed = cleanup.finish_pubsub, cleanup.env.kube.delete, []

    def late(deadline):
        a.environment.kube.put(copy.deepcopy(landed))
        return finish(deadline)

    def flaky(obj):
        if obj["metadata"]["name"].endswith("-access-probe") and not failed:
            failed.append(True)
            raise ApiError(500, "DELETE", obj["metadata"]["name"])
        return delete(obj)

    monkeypatch.setattr(cleanup, "finish_pubsub", late)
    monkeypatch.setattr(cleanup.env.kube, "delete", flaky)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    cleanup.run("probe response lost", False)
    assert failed and not probe_pods(a)
    assert "probe" in cleanup.env.roots
    assert a.environment.refresh().pubsub["stage"] == "cleaned"


def test_a_transient_read_does_not_end_the_late_probe_s_reaping(admitting, monkeypatch):
    """The barrier retries its own unreadable listings; the reap waits a poll."""
    a = admitting
    prepare_for_supervisor(a)
    landed = intended_probe(a)
    cleanup = a.supervisor.cleanup
    finish, get, failed = cleanup.finish_pubsub, cleanup.env.kube.get, []

    def late(deadline):
        a.environment.kube.put(copy.deepcopy(landed))
        return finish(deadline)

    def flaky(kind, namespace, name):
        if name.endswith("-access-probe") and not failed:
            failed.append(True)
            raise TransportError("connection reset")
        return get(kind, namespace, name)

    monkeypatch.setattr(cleanup, "finish_pubsub", late)
    monkeypatch.setattr(cleanup.env.kube, "get", flaky)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    cleanup.run("probe response lost", False)
    assert failed and not probe_pods(a)
    assert a.environment.refresh().pubsub["stage"] == "cleaned"


def test_a_late_probe_in_a_replaced_namespace_is_left_alone(admitting, monkeypatch):
    """The reap reads one Pod, but still only in the approved namespace."""
    a = admitting
    prepare_for_supervisor(a)
    landed = intended_probe(a)
    cleanup = a.supervisor.cleanup
    finish, namespace = cleanup.finish_pubsub, cleanup.env.kube.namespace

    def replaced(name):
        found = namespace(name)
        if name == PUBSUB:
            found["metadata"]["uid"] = "replacement-uid"
        return found

    def late(deadline):
        a.environment.kube.put(copy.deepcopy(landed))
        monkeypatch.setattr(cleanup.env.kube, "namespace", replaced)
        return finish(deadline)

    monkeypatch.setattr(cleanup, "finish_pubsub", late)
    a.sender.release()
    runner_releases_while_supervisor_waits(a, monkeypatch)
    with pytest.raises(Failure, match="Namespace identity changed"):
        cleanup.run("probe response lost", False)
    assert probe_pods(a)
