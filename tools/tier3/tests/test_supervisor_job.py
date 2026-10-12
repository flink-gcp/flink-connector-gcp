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
"""The supervisor Job replaces no Pod, so its end is what stops the run."""

import pytest
from flink_tier3 import lifecycle as cli

from .test_lifecycle import (
    app,
    evidence_events,
    lifecycle,
    obj,
    prepared_supervisor,
    ready_admission,
    rt,
)
from .test_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture


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


def test_admission_stops_once_the_supervisor_job_has_ended(env, monkeypatch):
    """A supervisor the infrastructure took away fails the Job, which no
    replacement keeps open."""
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


def test_a_failed_job_ends_the_runners_settlement_wait(env):
    _supervisor, _pod = prepared_supervisor(env)
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    env[0].put(job)
    runner = lifecycle(env, cli.runner_api.Runner)
    started = env[3]()
    runner.settle(request_stop=True)
    assert runner.env.refresh().idle
    assert env[3]() - started < runner.env.schedule.post_cleanup_grace * 3


def test_a_signal_before_admission_completes_stops_the_run(env):
    supervisor, pod = prepared_supervisor(env)
    supervisor.env.records.set_phase(rt.Phase.READY)
    supervisor.env.stopping = True
    with pytest.raises(rt.Failure, match="Admission unfinished"):
        supervisor.supervise(pod["metadata"]["uid"])
    assert supervisor.env.refresh().stop_requested
    assert "admission-stopped" in evidence_events(env)


def test_a_signal_after_admission_returns_the_run_to_idle(env, monkeypatch):
    supervisor, pod = admitted_smoke(env)
    sleep = supervisor.env.sleep

    def signalled(seconds):
        supervisor.env.stopping = True
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", signalled)
    monkeypatch.setattr(env[0], "logs", lambda *_args: b"ordinary log\n")
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and not control.success


def test_a_supervisor_pod_outside_the_job_is_refused(env):
    supervisor, _pod = prepared_supervisor(env)
    with pytest.raises(rt.Failure, match="not owned by the approved Job"):
        supervisor.supervise("another-pod-uid")
