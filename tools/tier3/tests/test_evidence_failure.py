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
"""Every way a run's evidence fails leaves its operation and cause behind (#1683).

Run `bq1313-fl-10-a1` ended `success: false` with a usable verdict and no
record of which write failed; these paths set the flag without saying why.
"""

import json

import pytest
from flink_tier3 import lifecycle as cli

from .test_lifecycle import (
    app,
    evidence_events,
    lifecycle,
    obj,
    prepared_supervisor,
    rt,
    seed_record,
)
from .test_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture

PLANS = {"roots": cli.wf.ROOTS, "empty": True}


def failing(monkeypatch, target, name, message="Transient storage failure"):
    def fail(*_args, **_kwargs):
        raise rt.Failure(message)

    monkeypatch.setattr(target, name, fail)


def recorded(env):
    return lifecycle(env).env.refresh().evidence_failure


def test_a_failed_evidence_write_names_its_event(env, monkeypatch, capsys):
    actor = lifecycle(env)
    failing(monkeypatch, actor.env.records, "evidence")
    actor.env.emit("inventory", [])
    failure = actor.env.evidence_failure
    assert actor.env.evidence_failed
    assert failure["operation"] == "evidence:inventory"
    assert failure["actor"] == "supervisor"
    assert failure["cause"] == "Failure: Transient storage failure"
    # The line on standard output is the copy that survives a failed store.
    printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert printed["event"] == "evidence-failed"
    assert printed["operation"] == "evidence:inventory"


def test_the_first_cause_is_the_one_kept(env, monkeypatch):
    actor = lifecycle(env)
    failing(monkeypatch, actor.env.records, "evidence", "first")
    actor.env.emit("inventory", [])
    failing(monkeypatch, actor.env.records, "evidence", "second")
    actor.env.emit("pod-log", {})
    assert actor.env.evidence_failure["cause"] == "Failure: first"


def test_a_failed_uid_observation_is_recorded_as_an_event(env, monkeypatch):
    actor = lifecycle(env)
    application = env[0].put(app(env))
    actor.env.remember("application", application)
    env[0].put(obj("Deployment", "jm", owner=application["metadata"]["uid"]))
    failing(monkeypatch, actor.env.records, "observe")
    actor.cleanup.inventory()
    assert actor.env.evidence_failure["operation"] == "observe"
    [event] = [
        value
        for (_bucket, path), (value, _gen) in env[1].data.items()
        if isinstance(value, dict) and value.get("event") == "evidence-failed"
    ]
    assert event["payload"]["operation"] == "observe"
    assert "Transient storage failure" in event["payload"]["cause"]


@pytest.mark.parametrize("write", ["request_stop", "begin_cleanup"])
def test_a_failed_cleanup_write_reaches_the_cleaned_record(env, monkeypatch, write):
    """The cleanup.py writes that once set the flag with nothing else."""
    actor = lifecycle(env)
    # A run already cleaning, whose service state is cleaned and stopped, so
    # only the failed write stands between it and the cleaned record.
    seed_record(
        env,
        phase="cleaning",
        stop_requested=True,
        pubsub={"stage": "cleaned", "resources": []},
    )
    failing(monkeypatch, actor.env.records, write)
    actor.cleanup.run("evidence path", success=True)
    control = actor.env.refresh()
    assert control.phase == rt.Phase.CLEANED
    assert control.evidence_failed and not control.success
    assert control.evidence_failure["operation"] == write.replace("_", "-")
    assert "Transient storage failure" in control.evidence_failure["cause"]
    assert "evidence-failed" in evidence_events(env)


def test_a_failed_cleaned_record_still_prints_its_cause(env, monkeypatch, capsys):
    actor = lifecycle(env)
    failing(monkeypatch, actor.env.records, "set_phase")
    actor.cleanup.run("evidence path", success=True)
    assert actor.env.evidence_failure["operation"] == "record-cleaned"
    assert '"operation": "record-cleaned"' in capsys.readouterr().out


@pytest.mark.parametrize("cause", ["unfinished", "unreadable"])
def test_a_failed_final_log_export_says_why(env, monkeypatch, cause):
    _supervisor, _pod = prepared_supervisor(env)
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    if cause == "unreadable":
        job["status"]["succeeded"] = 1
        env[0].put(job)
        failing(monkeypatch, env[0], "logs", "Pod log is gone")
    runner = lifecycle(env, cli.runner_api.Runner)
    runner.settle(request_stop=True)
    control = runner.env.refresh()
    assert control.idle and control.final_log_attempted
    assert control.evidence_failure["operation"] == "supervisor-final-log"
    assert control.evidence_failure["actor"] == "runner"
    assert control.evidence_failure["cause"] == (
        "The supervisor Job had not ended"
        if cause == "unfinished"
        else "Failure: Pod log is gone"
    )


def test_the_receipt_carries_the_cause_beside_a_usable_verdict(env):
    """What `bq1313-fl-10-a1`'s receipt lacked: why `success` is false."""
    runner = lifecycle(env, cli.runner_api.Runner)
    failure = {
        "operation": "begin-cleanup",
        "actor": "supervisor",
        "at": "2026-10-10T01:47:00Z",
        "cause": "ApiError: 429",
    }
    seed_record(env, phase="cleaned", idle=True, success=True)
    runner.env.records.mark_evidence_failed(failure)
    plans = {"nonce": env[2]["nonce"], **PLANS}
    assert runner.finalize(plans) is False
    receipt = env[1].read("runs/test-1310/result.json")[0]
    assert receipt["success"] is False
    assert receipt["evidence_failure"] == failure


def test_a_receipt_without_a_failure_names_none(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    seed_record(env, phase="cleaned", idle=True, success=True)
    assert runner.finalize({"nonce": env[2]["nonce"], **PLANS}) is True
    assert "evidence_failure" not in env[1].read("runs/test-1310/result.json")[0]


def test_a_probe_the_record_cannot_name_is_recorded_as_lost(env, monkeypatch):
    actor = lifecycle(env)
    probe = env[0].put(obj("Pod", "test-1310-access-probe"))
    seed_record(
        env,
        probe_intent={
            "kind": "Pod",
            "metadata": {
                "namespace": probe["metadata"]["namespace"],
                "name": probe["metadata"]["name"],
            },
        },
    )
    actor.env.refresh()
    failing(monkeypatch, actor.env.records, "remember_root")
    assert actor.cleanup.adopt_intended_probe([probe]) == probe
    assert actor.env.evidence_failure["operation"] == "remember-probe"


def test_the_record_keeps_the_first_cause_another_actor_wrote(env):
    """The supervisor's cause stays when the runner's settlement fails later."""
    first = {"operation": "begin-cleanup", "actor": "supervisor", "cause": "A"}
    later = {"operation": "supervisor-final-log", "actor": "runner", "cause": "B"}
    records = lifecycle(env).env.records
    records.mark_evidence_failed(first)
    records.final_log(later)
    records.settled(later)
    assert recorded(env) == first
