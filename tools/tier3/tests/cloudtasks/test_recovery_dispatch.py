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
"""The Cloud Tasks dispatch boundary: what it admits, and what it refuses first."""

import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from flink_tier3 import lifecycle as cli
from flink_tier3.cloudtasks import plan
from flink_tier3.common import Failure, digest
from flink_tier3.model import APPROVAL_VERSION, Approval, validate_approval
from flink_tier3.policy import CLOUDTASKS, SMOKE, WINDOWS

from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_recovery_plan import inputs as inputs  # noqa: PLC0414
from .test_recovery_plan import renderer as renderer  # noqa: PLC0414
from .test_recovery_runtime import prepared as prepared  # noqa: PLC0414

DIGEST = "sha256:" + "d" * 64
SHA = "e" * 40
NOW = cli.rt.timestamp("2026-10-13T00:00:00Z")
WINDOW = WINDOWS["cloudtasks-recovery"]


def args(tmp_path, **overrides):
    value = {
        "scenario": "cloudtasks-recovery",
        "application_digest": DIGEST,
        "cloudtasks_trial": "jm-replacement",
        "run_id": "ct-1769-0001",
        "kubeconfig": tmp_path / "kubeconfig",
        "directory": tmp_path,
    }
    value.update(overrides)
    return SimpleNamespace(**value)


class Recorded:
    """A runner that records what dispatch asked of it."""

    def __init__(self, env):
        self.env, self.calls = env, []

    def start(self, config, job, application):
        self.calls.append(("start", config, job, application))

    def settle(self, request_stop=False):
        self.calls.append(("settle", request_stop))


@pytest.fixture
def cluster(env, renderer, monkeypatch):
    """Everything outside the rig faked at its boundary, the rig itself real."""
    kube, store, reference, _ = env
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", SHA)
    monkeypatch.setenv("GITHUB_ACTOR", "octocat")
    monkeypatch.setattr(cli.time, "time", lambda: NOW)
    monkeypatch.setattr(cli, "external", lambda kubeconfig, idle=False: kube)
    asked = []
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda kubeconfig: SimpleNamespace(can_i=lambda *args: asked.append(args)),
    )
    namespaces = copy.deepcopy(reference["namespaces"])
    namespaces[CLOUDTASKS] = namespaces.pop(SMOKE)
    snapshot = (
        namespaces,
        reference["operator_uid"],
        reference["images"]["operator"],
        reference["baseline_uids"],
    )
    snapshots = []

    def snapshotted(kube, namespace):
        snapshots.append(namespace)
        return snapshot

    monkeypatch.setattr(cli.wf, "snapshot", snapshotted)
    monkeypatch.setattr(
        cli.wf,
        "execution",
        lambda kind, nonce: {"kind": kind, "nonce": nonce, "sha": SHA},
    )
    monkeypatch.setattr(cli.wf, "image_receipts", lambda session, images, expiry: {})
    monkeypatch.setattr(cli.rt, "authorized_session", lambda token: None)
    monkeypatch.setattr(cli.rt, "GoogleToken", lambda: None)
    locks = []
    monkeypatch.setattr(
        cli.rt,
        "EnvironmentLock",
        lambda store: SimpleNamespace(acquire=lambda owner: locks.append(owner)),
    )
    seen = {}

    @contextmanager
    def runner(env, application, *, credentials=None):
        seen.update(env=env, application=application, runner=Recorded(env))
        yield seen["runner"]

    monkeypatch.setattr(cli.resource_actors, "runner", runner)
    return SimpleNamespace(
        store=store, locks=locks, seen=seen, asked=asked, snapshots=snapshots
    )


@pytest.mark.parametrize("kind", plan.KINDS)
def test_an_admitted_dispatch_runs_the_rig_with_the_delivery_its_approval_fixes(
    cluster, renderer, tmp_path, kind
):
    cli.start(args(tmp_path, cloudtasks_trial=kind), cluster.store)
    seen = cluster.seen
    approval = seen["env"].approval
    validate_approval(approval.to_dict())
    assert approval.version == APPROVAL_VERSION
    assert approval.scenario == "cloudtasks-recovery"
    assert approval.cloudtasks_trial == plan.trial(kind)
    assert approval.cloudtasks_plan.kind == kind
    [(_, _, _, _, options)] = renderer
    assert options["cloudtasks_trial"] == kind
    assert approval.started_at == cli.rt.utc(NOW)
    assert approval.expires_at == cli.rt.utc(NOW + WINDOW)
    assert approval.images["application"] == (
        cli.rt.GAR + "cloudtasks-recovery@" + DIGEST
    )
    assert seen["env"].actor == "runner"
    assert cluster.asked == [
        (True, "create", "flink.apache.org", "flinkdeployments", CLOUDTASKS)
    ]
    assert set(cluster.snapshots) == {CLOUDTASKS}
    runner, application = seen["runner"], seen["application"]
    (_, config, supervisor, started), settled = runner.calls
    assert settled == ("settle", False)
    assert started is application
    assert json.loads(config["data"]["approval.json"]) == approval.to_dict()
    assert supervisor["spec"]["activeDeadlineSeconds"] == (
        approval.schedule.active_seconds(NOW)
    )
    assert digest(application) == approval.application_sha256
    assert cluster.locks == [approval.lock_owner]
    documents = {
        name: cluster.store.read(f"runs/ct-1769-0001/{name}")[0]
        for name in ("approval.json", "application.json", "upgrade-application.json")
    }
    assert Approval.from_dict(documents["approval.json"]) == approval
    assert documents["application.json"] == application
    assert (
        digest(documents["upgrade-application.json"])
        == approval.upgrade_application_sha256
    )


# "none" is what the workflow sends when its input is left alone.
@pytest.mark.parametrize("value", ["none", None, "", "upgrade-failover", "RESCALE-IN"])
def test_a_dispatch_without_a_trial_kind_is_refused_before_anything(
    env, tmp_path, monkeypatch, value
):
    _, store, *_ = env
    monkeypatch.setattr(
        cli, "external", lambda *_: (_ for _ in ()).throw(AssertionError("cluster"))
    )
    before = copy.deepcopy(store.data)
    with pytest.raises(Failure, match="--cloudtasks-trial kind"):
        cli.start(args(tmp_path, cloudtasks_trial=value), store)
    assert store.data == before


@pytest.mark.parametrize("value", [None, "latest", "sha256:" + "d" * 63])
def test_a_dispatch_without_a_digest_is_refused_before_anything(
    env, tmp_path, monkeypatch, value
):
    _, store, *_ = env
    monkeypatch.setattr(
        cli, "external", lambda *_: (_ for _ in ()).throw(AssertionError("cluster"))
    )
    before = copy.deepcopy(store.data)
    with pytest.raises(Failure, match="sha256 --application-digest"):
        cli.start(args(tmp_path, application_digest=value), store)
    assert store.data == before


def test_the_assembled_approval_is_the_one_the_model_admits(prepared):
    environment, _, _, proposal = prepared
    reference = environment.approval.to_dict()
    trial = plan.trial("tm-replacement")
    approval = cli.resource_approval(
        scenario="cloudtasks-recovery",
        run_id=reference["run_id"],
        nonce=reference["nonce"],
        sha=reference["sha"],
        trial=trial,
        proposal=dict(
            proposal,
            application_sha256=reference["application_sha256"],
            upgrade_application_sha256=reference["upgrade_application_sha256"],
        ),
        namespaces=reference["namespaces"],
        operator_uid=reference["operator_uid"],
        baseline=reference["baseline_uids"],
        images=reference["images"],
        owner=reference["lock_owner"],
        actor="octocat",
    )
    validate_approval(approval)
    assert approval["scenario"] == "cloudtasks-recovery"
    # A copy, so a later edit cannot reach the dispatched trial.
    assert approval["cloudtasks_trial"] == trial
    assert approval["cloudtasks_trial"] is not trial
