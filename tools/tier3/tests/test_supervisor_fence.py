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
"""A replaced supervisor Pod is fenced out of the run it no longer holds."""

import copy

import pytest
from flink_tier3 import lifecycle as cli

from .cloudtasks.test_session import (
    CELL_A,
    CELL_B,
    CellWorld,
    admitted_session,
    evidence_events,
    session_approval,
    session_environment,
)
from .test_lifecycle import (
    app,
    claim_as_supervisor,
    lifecycle,
    obj,
    prepared_supervisor,
    ready_admission,
    rt,
    seed_record,
)
from .test_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture

HOLDER, REPLACEMENT = "supervisor-pod-uid", "replacement-pod-uid"


def records(env):
    """A process's own view of the control record, as each Pod has one."""
    return rt.Records(env[1], rt.Approval.from_dict(env[2]), env[3])


def mutations(env):
    """Every Kubernetes call that creates, changes or removes something."""
    return [
        call
        for call in env[0].calls
        if call[0] in ("create", "delete", "patch", "scale")
    ]


def replacement_pod(env, pod):
    """The Job's next attempt, with the attempt it replaced left terminal."""
    taken = env[0].get("Pod", rt.SYSTEM, pod["metadata"]["name"])
    taken["status"]["phase"] = "Failed"
    taken["status"]["conditions"] = [{"type": "DisruptionTarget", "status": "True"}]
    env[0].put(taken)
    replacement = copy.deepcopy(pod)
    replacement["metadata"].update(name="replacement-pod", uid=REPLACEMENT)
    replacement["status"] = {"phase": "Running"}
    env[0].put(replacement)
    return replacement


# --- the fence ------------------------------------------------------------------


def test_a_claim_landing_inside_a_write_refuses_that_write(env):
    """The fence is part of the compare-and-set, not a read before it."""
    holder = records(env)
    holder.claim_supervisor(HOLDER)
    before = holder.read()[0].heartbeat
    env[1].before_write = lambda: records(env).claim_supervisor(REPLACEMENT)
    conflicts = env[1].conflicts
    with pytest.raises(rt.Superseded):
        holder.heartbeat()
    assert env[1].conflicts == conflicts + 1  # the retry saw the new holder
    control = holder.read()[0]
    assert control.supervisor_pod == REPLACEMENT
    assert control.heartbeat == before


@pytest.mark.parametrize(
    "write",
    [
        lambda r: r.heartbeat(),
        lambda r: r.heartbeat(started=True),
        lambda r: r.request_stop(),
        lambda r: r.begin_cleanup("taken"),
        lambda r: r.mark_evidence_failed(),
        lambda r: r.observe({"x": {"uid": "x", "namespace": rt.SMOKE}}),
        lambda r: r.remember_root("cell:x", {"uid": "x", "namespace": rt.SMOKE}),
        lambda r: r.intend("queue"),
        lambda r: r.set_phase(rt.Phase.READY),
        lambda r: r.cell_done("x", "completed", "finished"),
        lambda r: r.record_export("x", {"manifest_sha256": "x"}),
        lambda r: r.record_lineage("11111111-1111-4111-8111-111111111111"),
        lambda r: r.checkpoint(),
        lambda r: r.operations("supervisor", {}),
    ],
    ids=[
        "heartbeat",
        "start",
        "stop",
        "cleanup",
        "evidence",
        "observe",
        "root",
        "intent",
        "phase",
        "cell",
        "export",
        "lineage",
        "checkpoint",
        "operations",
    ],
)
def test_every_write_of_a_displaced_pod_is_refused(env, write):
    holder = records(env)
    holder.claim_supervisor(HOLDER)
    records(env).claim_supervisor(REPLACEMENT)
    before = holder.read()[0].to_dict()
    with pytest.raises(rt.Superseded):
        write(holder)
    assert holder.read()[0].to_dict() == before


def test_the_runner_and_recovery_are_not_fenced(env):
    records(env).claim_supervisor(HOLDER)
    runner = records(env)  # never claims, so it holds no token
    runner.set_phase(rt.Phase.READY)
    runner.request_stop()
    runner.begin_cleanup("external settlement")
    assert runner.read()[0].phase == rt.Phase.CLEANING


def test_superseded_is_not_a_failure():
    """Cleanup absorbs a failed control write and carries on deleting."""
    assert not issubclass(rt.Superseded, rt.Failure)


# --- the takeover window --------------------------------------------------------


@pytest.mark.parametrize("phase", [rt.Phase.APPROVED, rt.Phase.READY, rt.Phase.RUNNING])
def test_a_replacement_takes_over_before_supervision_starts(env, phase):
    """RUNNING included: admission can finish while the taken Pod terminates."""
    records(env).claim_supervisor(HOLDER)
    runner = records(env)
    steps = {
        rt.Phase.APPROVED: [],
        rt.Phase.READY: [rt.Phase.READY],
        rt.Phase.RUNNING: [rt.Phase.READY, rt.Phase.RUNNING],
    }
    for step in steps[phase]:
        runner.set_phase(step)
    records(env).claim_supervisor(REPLACEMENT)
    assert runner.read()[0].supervisor_pod == REPLACEMENT


def test_a_replacement_is_refused_once_supervision_has_started(env):
    holder = records(env)
    holder.claim_supervisor(HOLDER)
    holder.heartbeat(started=True)
    with pytest.raises(rt.Failure, match="cannot be claimed"):
        records(env).claim_supervisor(REPLACEMENT)
    assert holder.read()[0].supervisor_pod == HOLDER
    holder.heartbeat()  # still the holder


def test_a_claim_landing_inside_the_start_refuses_the_start(env):
    holder = records(env)
    holder.claim_supervisor(HOLDER)
    env[1].before_write = lambda: records(env).claim_supervisor(REPLACEMENT)
    with pytest.raises(rt.Superseded):
        holder.heartbeat(started=True)
    control = holder.read()[0]
    assert control.supervisor_pod == REPLACEMENT
    assert not control.supervision_started


def test_a_start_landing_inside_a_claim_refuses_the_claim(env):
    """The claim's rule is re-checked on its retry, not only before it."""
    holder = records(env)
    holder.claim_supervisor(HOLDER)
    env[1].before_write = lambda: holder.heartbeat(started=True)
    conflicts = env[1].conflicts
    with pytest.raises(rt.Failure, match="cannot be claimed"):
        records(env).claim_supervisor(REPLACEMENT)
    assert env[1].conflicts == conflicts + 1
    control = holder.read()[0]
    assert control.supervisor_pod == HOLDER and control.supervision_started


@pytest.mark.parametrize(
    "state",
    [
        {"stop_requested": True},
        {"evidence_failed": True},
        {"phase": "cleaning"},
        {"phase": "cleaned"},
    ],
    ids=["stopped", "evidence-failed", "cleaning", "cleaned"],
)
def test_a_replacement_is_refused_on_a_run_that_is_ending(env, state):
    records(env).claim_supervisor(HOLDER)
    seed_record(env, **state)
    with pytest.raises(rt.Failure, match="cannot be claimed"):
        records(env).claim_supervisor(REPLACEMENT)


@pytest.mark.parametrize(
    "state",
    [{"stop_requested": True}, {"phase": "cleaning"}, {"supervision_started": True}],
    ids=["stopped", "cleaning", "started"],
)
def test_the_first_pod_claims_an_unclaimed_run_whatever_its_state(env, state):
    """Only displacing a holder is bounded; the first Pod supervises as before."""
    seed_record(env, **state)
    records(env).claim_supervisor(HOLDER)
    assert records(env).read()[0].supervisor_pod == HOLDER


# --- the supervisor -------------------------------------------------------------


def admitted_smoke(env):
    """A smoke run the runner has admitted, with its application running."""
    supervisor, pod = prepared_supervisor(env)
    admission = lifecycle(env, cli.runner_api.Runner)
    admission.env.records.set_phase(rt.Phase.READY)
    admission.cleanup.quota(rt.SYSTEM, "run")
    admission.cleanup.scale_operator(1)
    admission.cleanup.quota(rt.SMOKE, "run")
    admission.create_application(app(env))
    admission.env.records.set_phase(rt.Phase.RUNNING)
    env[0].calls.clear()
    return supervisor, pod


def test_a_supervisor_displaced_during_admission_stops_without_touching_the_run(
    env, monkeypatch
):
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    env[0].calls.clear()
    sleep = supervisor.env.sleep

    def replaced(seconds):
        records(env).claim_supervisor(REPLACEMENT)
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", replaced)
    with pytest.raises(rt.Superseded) as refused:
        supervisor.supervise(pod["metadata"]["uid"])
    # The admission heartbeat was the write the fence refused.
    assert isinstance(refused.value.__context__, rt.Superseded)
    control = supervisor.env.refresh()
    assert control.supervisor_pod == REPLACEMENT
    assert control.phase == rt.Phase.READY
    assert not control.stop_requested
    assert not mutations(env)


def test_a_displaced_supervisor_cannot_clean_up_the_holders_run(env):
    """The #1390 defect: the loser reached `Cleanup.run()` and deleted."""
    supervisor, pod = admitted_smoke(env)
    supervisor.env.records.claim_supervisor(pod["metadata"]["uid"])
    records(env).claim_supervisor(REPLACEMENT)
    with pytest.raises(rt.Superseded):
        supervisor.cleanup.run("taken", False)
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.RUNNING
    assert control.supervisor_pod == REPLACEMENT
    assert env[0].get("FlinkDeployment", rt.SMOKE, env[2]["run_id"])
    assert not mutations(env)


def test_a_displaced_supervisor_that_fails_for_its_own_reason_cleans_nothing(
    env, monkeypatch
):
    """It learns of the displacement when its conclusion re-reads the claim."""
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    env[0].calls.clear()
    sleep = supervisor.env.sleep

    def replaced_then_failed(seconds):
        # The next write this Pod makes is its own stop request.
        records(env).claim_supervisor(REPLACEMENT)
        supervisor.env.evidence_failed = True
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", replaced_then_failed)
    with pytest.raises(rt.Superseded):
        supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert not control.stop_requested and not control.evidence_failed
    assert not mutations(env)


@pytest.mark.parametrize("phase", [rt.Phase.READY, rt.Phase.RUNNING])
def test_a_signal_before_supervision_leaves_the_run_to_the_replacement(env, phase):
    """The measured preemption: stopping here would spend the whole session."""
    if phase == rt.Phase.RUNNING:
        supervisor, pod = admitted_smoke(env)
    else:
        supervisor, pod = prepared_supervisor(env)
        supervisor.env.records.set_phase(rt.Phase.READY)
        env[0].calls.clear()
    supervisor.env.stopping = True
    with pytest.raises(rt.Failure, match="terminated before supervision began"):
        supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == phase
    assert not control.stop_requested
    assert not control.supervision_started
    assert not mutations(env)
    assert "supervisor-replaceable" in evidence_events(env)
    records(env).claim_supervisor(REPLACEMENT)


def test_a_signal_after_supervision_started_still_returns_the_run_to_idle(
    env, monkeypatch
):
    supervisor, pod = admitted_smoke(env)
    sleep = supervisor.env.sleep

    def signalled(seconds):
        supervisor.env.stopping = True
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", signalled)
    monkeypatch.setattr(env[0], "logs", lambda *_args: b"ordinary log\n")
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.supervision_started
    assert control.phase == rt.Phase.CLEANED and not control.success


def test_a_start_whose_response_was_lost_still_closes_the_window(env, monkeypatch):
    """The record decides, so a signal after a landed start still cleans up."""
    supervisor, pod = admitted_smoke(env)
    heartbeat = supervisor.env.records.heartbeat

    def lost(*args, started=False, **kwargs):
        heartbeat(*args, started=started, **kwargs)
        if started:
            supervisor.env.stopping = True
            raise rt.Failure("Lost start response")

    monkeypatch.setattr(supervisor.env.records, "heartbeat", lost)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.supervision_started
    assert control.phase == rt.Phase.CLEANED and not control.success
    assert "supervisor-replaceable" not in evidence_events(env)
    with pytest.raises(rt.Failure, match="cannot be claimed"):
        records(env).claim_supervisor(REPLACEMENT)


def test_a_displaced_pod_that_is_also_signalled_reports_only_its_displacement(
    env, monkeypatch
):
    """It must not announce the run as open: the run is already the holder's."""
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    env[0].calls.clear()
    sleep = supervisor.env.sleep

    def replaced_and_signalled(seconds):
        records(env).claim_supervisor(REPLACEMENT)
        supervisor.env.stopping = True
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", replaced_and_signalled)
    with pytest.raises(rt.Superseded):
        supervisor.supervise(pod["metadata"]["uid"])
    assert "supervisor-replaceable" not in evidence_events(env)
    assert not mutations(env)


def test_a_replacement_carries_a_session_its_predecessor_never_started(
    env, monkeypatch
):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    taken, pod, _runner = admitted_session(env, manifests)
    taken.env.stopping = True
    with pytest.raises(rt.Failure, match="terminated before supervision began"):
        taken.supervise(pod["metadata"]["uid"])
    assert world.deployments_seen == []
    assert not taken.env.ledger.read()[0]["cells"]
    replacement = rt.Supervisor(
        session_environment(env, taken.env.queues.actor()), cells=manifests
    )
    replacement.supervise(replacement_pod(env, pod)["metadata"]["uid"])
    assert world.deployments_seen == [CELL_A["id"], CELL_B["id"]]
    control = replacement.env.refresh()
    assert control.supervisor_pod == REPLACEMENT
    assert control.phase == rt.Phase.CLEANED and control.success
    ledger = replacement.env.ledger.read()[0]["cells"]
    assert {entry["status"] for entry in ledger.values()} == {"completed"}


def test_a_displaced_session_supervisor_claims_no_cell(env, monkeypatch):
    """The ledger claim follows the fenced start, so a loser never reaches it."""
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    taken, pod, _runner = admitted_session(env, manifests)
    heartbeat = taken.env.records.heartbeat

    def displaced(*args, started=False, **kwargs):
        if started:
            records(env).claim_supervisor(REPLACEMENT)
        return heartbeat(*args, started=started, **kwargs)

    monkeypatch.setattr(taken.env.records, "heartbeat", displaced)
    with pytest.raises(rt.Superseded):
        taken.supervise(pod["metadata"]["uid"])
    assert world.deployments_seen == []
    assert not taken.env.ledger.read()[0]["cells"]
    assert taken.env.queues.get() is not None  # the queue is the holder's


# --- the runner and the Job -----------------------------------------------------


def test_a_job_between_pod_attempts_is_not_finished():
    """A disrupted attempt with replacements left is not a finished Job."""
    assert rt.job_finished({"status": {"failed": 1}}) is False
    assert rt.job_finished({"status": {"failed": 1, "succeeded": 1}}) is True
    failed = {"type": "Failed", "status": "True"}
    assert rt.job_finished({"status": {"failed": 3, "conditions": [failed]}}) is True
    assert rt.job_finished({"status": {}}) is False


def copied(env, pod, name, phase):
    """Another Pod shaped and owned like `pod`."""
    copy_ = copy.deepcopy(
        env[0].get(pod["kind"], pod["metadata"]["namespace"], pod["metadata"]["name"])
    )
    copy_["metadata"].update(name=name, uid=name + "-uid")
    copy_["status"]["phase"] = phase
    return env[0].put(copy_)


def filled_to_the_ceiling(env):
    """An admitted run whose live Pods reach the ceiling exactly."""
    supervisor, pod = admitted_smoke(env)
    replacement = replacement_pod(env, pod)
    _items, pods = supervisor.cleanup.audit()
    live = [p for p in pods if p["status"].get("phase") not in ("Failed", "Succeeded")]
    for index in range(supervisor.cleanup.ceilings["pods"] - len(live)):
        copied(env, replacement, f"live-{index}", "Running")
    return supervisor, pod, replacement


def test_the_audit_admits_retained_supervisor_attempts_beside_a_full_run(env):
    supervisor, pod, replacement = filled_to_the_ceiling(env)
    copied(env, pod, "taken-again", "Failed")  # the Job retains up to two
    supervisor.cleanup.audit()
    copied(env, replacement, "one-too-many", "Running")
    with pytest.raises(rt.Failure, match="ceiling exceeded"):
        supervisor.cleanup.audit()


def test_a_terminal_pod_of_anything_else_still_counts(env):
    """Only the Job's backoff limit bounds retained attempts; nothing else does."""
    supervisor, _pod, _replacement = filled_to_the_ceiling(env)
    application = env[0].get("FlinkDeployment", rt.SMOKE, env[2]["run_id"])
    task = obj("Pod", "evicted-task", owner=application["metadata"]["uid"])
    task["spec"] = {
        "containers": [
            {
                "name": "flink-main-container",
                "image": env[2]["images"]["smoke"],
                "resources": {
                    k: rt.POD_RESOURCES["smoke"] for k in ("requests", "limits")
                },
            }
        ],
        "nodeSelector": {"cloud.google.com/gke-spot": "true"},
    }
    task["status"]["phase"] = "Failed"
    env[0].put(task)
    with pytest.raises(rt.Failure, match="ceiling exceeded"):
        supervisor.cleanup.audit()


def test_the_runner_waits_for_the_pod_that_claimed_the_run(env, monkeypatch):
    """A heartbeat alone may come from a Pod that never claimed the run."""
    _supervisor, pod = prepared_supervisor(env)
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    env[0].data.pop(("Job", rt.SYSTEM, "supervisor"))
    seed_record(env, roots={})
    runner = lifecycle(env, cli.runner_api.Runner)
    create = env[0].create

    def created(value, dry_run=False):
        result = create(value, dry_run)
        if value["kind"] == "Job" and not dry_run:
            env[0].put(pod)
            records(env).heartbeat()  # a Pod that has not claimed the run
        return result

    monkeypatch.setattr(env[0], "create", created)
    with pytest.raises(rt.Failure, match="Bounded wait expired"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert not env[0].get("FlinkDeployment", rt.SMOKE, env[2]["run_id"])


def test_the_runner_does_not_read_a_replaced_attempts_claim_as_ready(env, monkeypatch):
    _supervisor, pod = prepared_supervisor(env)
    env[0].data.pop(("Pod", rt.SYSTEM, pod["metadata"]["name"]))
    env[0].data.pop(("Job", rt.SYSTEM, "supervisor"))
    seed_record(env, roots={})
    runner = lifecycle(env, cli.runner_api.Runner)
    create = env[0].create

    def created(value, dry_run=False):
        result = create(value, dry_run)
        if value["kind"] == "Job" and not dry_run:
            env[0].put(pod)
            claim_as_supervisor(env, pod)
            replacement_pod(env, pod)  # running, and not yet claimed
        return result

    monkeypatch.setattr(env[0], "create", created)
    with pytest.raises(rt.Failure, match="Bounded wait expired"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert not env[0].get("FlinkDeployment", rt.SMOKE, env[2]["run_id"])


def test_admission_stops_once_the_supervisor_job_has_ended(env, monkeypatch):
    """A signalled supervisor no longer writes a stop, so the Job is the signal."""
    _supervisor, _pod, runner = ready_admission(env, monkeypatch)
    get = env[0].get

    def ended_after_operator_ready(kind, namespace, name):
        result = get(kind, namespace, name)
        if kind == "Deployment" and result["status"].get("readyReplicas") == 1:
            job = get("Job", rt.SYSTEM, "supervisor")
            job["status"] = {
                "failed": 1,
                "conditions": [{"type": "Failed", "status": "True"}],
            }
            env[0].put(job)
        return result

    monkeypatch.setattr(env[0], "get", ended_after_operator_ready)
    with pytest.raises(rt.Failure, match="Supervisor stopped"):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            app(env),
        )
    assert not any(call[:2] == ("create", "FlinkDeployment") for call in env[0].calls)


def test_a_job_between_attempts_keeps_admission_open(env):
    prepared_supervisor(env)
    runner = lifecycle(env, cli.runner_api.Runner)
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"] = {"failed": 1}
    env[0].put(job)
    runner.env.refresh()
    runner.supervisor_open()


# --- a conclusion that is not a hand-over ---------------------------------------


def test_a_signal_with_a_local_evidence_failure_still_stops_the_run(env):
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    supervisor.env.stopping = True
    supervisor.env.evidence_failed = True
    with pytest.raises(rt.Failure, match="Admission unfinished"):
        supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.stop_requested and control.evidence_failed
    assert "supervisor-replaceable" not in evidence_events(env)


def test_a_signal_on_a_run_already_stopping_cleans_it_up(env):
    """No replacement may claim a stopped run, so this Pod must clean it."""
    supervisor, pod = admitted_smoke(env)
    records(env).request_stop()
    supervisor.env.stopping = True
    supervisor.supervise(pod["metadata"]["uid"])
    assert supervisor.env.refresh().phase == rt.Phase.CLEANED
    assert "supervisor-replaceable" not in evidence_events(env)


def test_a_signal_after_bigquery_provisioning_does_not_leave_the_run(env):
    """Only this Pod's handoff can clean BigQuery resources."""
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    seed_record(env, bigquery={"intent": "provisioned"})
    supervisor.env.stopping = True
    with pytest.raises(rt.Failure, match="Admission unfinished"):
        supervisor.supervise(pod["metadata"]["uid"])
    assert supervisor.env.refresh().stop_requested
    assert "supervisor-replaceable" not in evidence_events(env)


def failing_first_heartbeat(supervisor, monkeypatch):
    """A storage failure before supervision starts, with the run RUNNING."""
    heartbeat = supervisor.env.records.heartbeat
    failed = []

    def failing(*args, **kwargs):
        if not failed:
            failed.append(True)
            raise rt.Failure("Storage unavailable")
        return heartbeat(*args, **kwargs)

    monkeypatch.setattr(supervisor.env.records, "heartbeat", failing)


def test_cleanup_before_supervision_closes_the_window_first(env, monkeypatch):
    """Cleanup absorbs a failed write, so the window must close before it runs."""
    supervisor, pod = admitted_smoke(env)
    failing_first_heartbeat(supervisor, monkeypatch)
    refused = []

    def lost_begin(reason):
        try:
            records(env).claim_supervisor(REPLACEMENT)
        except rt.Failure as error:
            refused.append(error)
        raise rt.Failure("Lost cleanup response")

    monkeypatch.setattr(supervisor.env.records, "begin_cleanup", lost_begin)
    supervisor.supervise(pod["metadata"]["uid"])
    assert refused, "a replacement claimed a run this Pod went on to clean"
    control = supervisor.env.refresh()
    assert control.stop_requested and control.supervisor_pod == pod["metadata"]["uid"]


def test_a_failed_stop_before_cleanup_leaves_the_run_untouched(env, monkeypatch):
    supervisor, pod = admitted_smoke(env)
    failing_first_heartbeat(supervisor, monkeypatch)

    def lost_stop():
        raise rt.Failure("Storage unavailable")

    monkeypatch.setattr(supervisor.env.records, "request_stop", lost_stop)
    with pytest.raises(rt.Failure, match="Storage unavailable"):
        supervisor.supervise(pod["metadata"]["uid"])
    assert supervisor.env.refresh().phase == rt.Phase.RUNNING
    assert not mutations(env)


# --- a supervisor that is gone and will not be replaced -------------------------


def lost_holder(env, *, started):
    """The holder Pod disrupted, its replacement Pending, the Job between attempts."""
    supervisor, pod = admitted_smoke(env)
    holder = claim_as_supervisor(env, pod)
    if started:
        holder.heartbeat(started=True)
    replacement = replacement_pod(env, pod)
    replacement["status"]["phase"] = "Pending"
    env[0].put(replacement)
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"] = {"failed": 1}
    env[0].put(job)
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.env.refresh()
    return supervisor, runner


def test_the_runner_settles_a_started_run_whose_holder_is_gone(env):
    """A Pending replacement could not claim it, so waiting for it is waiting for nothing."""
    _supervisor, runner = lost_holder(env, started=True)
    before = env[3]()
    runner.settle(request_stop=True)
    control = runner.env.refresh()
    assert control.phase == rt.Phase.CLEANED
    assert not env[0].get("FlinkDeployment", rt.SMOKE, env[2]["run_id"])
    # Settled on the first poll, not at the runner's own deadline.
    assert env[3]() - before < rt.POLL + runner.env.schedule.operator_grace + 1


def test_before_the_start_a_pending_replacement_gets_the_readiness_allowance(env):
    _supervisor, runner = lost_holder(env, started=False)
    control = runner.env.refresh()
    assert not runner.supervisor_lost()
    env[3].now = runner.env.schedule.readiness_until(rt.timestamp(control.heartbeat))
    assert runner.supervisor_lost()
    assert runner.env.refresh().stop_requested  # no replacement may claim it now


def test_a_holder_lost_before_its_first_heartbeat_is_timed_from_its_claim(env):
    _supervisor, pod = admitted_smoke(env)
    records(env).claim_supervisor(pod["metadata"]["uid"])  # and gone at once
    replacement = replacement_pod(env, pod)
    replacement["status"]["phase"] = "Pending"
    env[0].put(replacement)
    runner = lifecycle(env, cli.runner_api.Runner)
    control = runner.env.refresh()
    assert control.heartbeat is None
    assert not runner.supervisor_lost()
    claimed = rt.timestamp(control.supervisor_claimed_at)
    env[3].now = runner.env.schedule.readiness_until(claimed)
    assert runner.supervisor_lost()


def test_a_replacement_claiming_while_the_runner_decides_keeps_the_run(env):
    """The runner's decision is a write conditional on the holder it saw."""
    _supervisor, runner = lost_holder(env, started=False)
    control = runner.env.refresh()
    env[3].now = runner.env.schedule.readiness_until(rt.timestamp(control.heartbeat))
    runner.cleanup.inventory()  # record what is there, so the next write is the decision
    env[1].before_write = lambda: records(env).claim_supervisor(REPLACEMENT)
    assert not runner.supervisor_lost()
    control = runner.env.refresh()
    assert control.supervisor_pod == REPLACEMENT and not control.stop_requested


def test_a_claimed_pending_replacement_is_not_lost(env):
    """A replacement that has claimed but is not yet running holds the run."""
    _supervisor, runner = lost_holder(env, started=False)
    records(env).claim_supervisor(REPLACEMENT)
    env[3].now += 3600
    assert not runner.supervisor_lost()


def test_a_running_holder_or_an_unclaimed_run_is_not_lost(env):
    _supervisor, pod = admitted_smoke(env)
    runner = lifecycle(env, cli.runner_api.Runner)
    assert not runner.supervisor_lost()  # nothing claimed
    claim_as_supervisor(env, pod).heartbeat(started=True)
    assert not runner.supervisor_lost()  # the holder runs


def test_a_claim_with_no_recorded_time_leaves_the_job_to_decide(env):
    """A record that names a holder but no time cannot start an allowance."""
    _supervisor, runner = lost_holder(env, started=False)
    seed_record(env, heartbeat=None, supervisor_claimed_at=None)
    env[3].now += 3600
    assert not runner.supervisor_lost()
    assert not runner.env.refresh().stop_requested
