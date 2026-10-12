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
"""The Pub/Sub dispatch boundary: the approval it builds, and where it stops."""

import copy
import json
import signal
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from flink_tier3 import lifecycle as cli
from flink_tier3 import repository
from flink_tier3.common import Failure, digest
from flink_tier3.model import APPROVAL_VERSION, validate_approval
from flink_tier3.policy import CLEANUP_SECONDS, MIB, PUBSUB, SMOKE, WINDOWS

from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_plan import FIXTURE_TRIALS
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414

DIGEST = "sha256:" + "d" * 64
SHA = "e" * 40
NOW = cli.rt.timestamp("2026-09-21T00:00:00Z")
WINDOW = WINDOWS["pubsub-recovery"]


@pytest.fixture
def reviewed(monkeypatch):
    """Resolve trial names against the tests' fixture directory."""
    monkeypatch.setattr(repository, "ROOT", FIXTURE_TRIALS.parent)
    monkeypatch.setattr(cli, "PUBSUB_TRIALS", Path(FIXTURE_TRIALS.name))


@pytest.fixture
def dispatching(monkeypatch):
    """The workflow's environment, a frozen clock, and a cluster that records."""
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", SHA)
    monkeypatch.setenv("GITHUB_ACTOR", "octocat")
    monkeypatch.setattr(cli.time, "time", lambda: NOW)
    touched = []

    def external(kubeconfig, idle=False):
        touched.append(("external", kubeconfig, idle))
        raise AssertionError("dispatch reached the cluster")

    monkeypatch.setattr(cli, "external", external)
    return touched


def args(tmp_path, **overrides):
    value = {
        "scenario": "pubsub-recovery",
        "pubsub_trial": "example-wiring",
        "application_digest": DIGEST,
        "run_id": "ps-1429-0001",
        "kubeconfig": tmp_path / "kubeconfig",
        "directory": tmp_path / "lifecycle",
    }
    value.update(overrides)
    return SimpleNamespace(**value)


def test_the_trial_resolves_from_its_reviewed_file_and_the_live_digest(
    reviewed, trial, tmp_path
):
    resolved, image = cli.pubsub_inputs(args(tmp_path))
    assert resolved == trial
    assert image == cli.rt.GAR + "pubsub-recovery@" + DIGEST


def test_the_tests_example_trial_is_not_dispatchable(tmp_path):
    """Only reviewed files resolve, and the example is not one of them."""
    with pytest.raises(Failure, match="Unreadable Pub/Sub trial file"):
        cli.pubsub_inputs(args(tmp_path))


@pytest.mark.parametrize(
    "change, match",
    [
        ({"pubsub_trial": None}, "reviewed --pubsub-trial"),
        ({"pubsub_trial": ""}, "reviewed --pubsub-trial"),
        ({"pubsub_trial": "../sessions/example-wiring"}, "reviewed --pubsub-trial"),
        ({"pubsub_trial": "example-wiring.toml"}, "reviewed --pubsub-trial"),
        ({"pubsub_trial": "Example-Wiring"}, "reviewed --pubsub-trial"),
        ({"pubsub_trial": "absent"}, "Unreadable Pub/Sub trial file"),
        ({"application_digest": None}, "sha256 --application-digest"),
        ({"application_digest": "latest"}, "sha256 --application-digest"),
    ],
)
def test_a_trial_that_does_not_resolve_is_refused(reviewed, tmp_path, change, match):
    with pytest.raises(Failure, match=match):
        cli.pubsub_inputs(args(tmp_path, **change))


def test_the_window_starts_at_admission_and_lasts_the_scenarios_policy(
    monkeypatch,
):
    monkeypatch.setattr(cli.time, "time", lambda: NOW + 0.7)
    assert cli.window(WINDOW) == (NOW, NOW + 3600)


@pytest.mark.parametrize(
    "change, env_change, match",
    [
        ({}, {"GITHUB_REF": "refs/heads/feature"}, "must run from main"),
        ({"rig_sha": "main"}, {}, "exact rig commit"),
        ({}, {"GITHUB_SHA": ""}, "exact rig commit"),
        ({"pubsub_trial": "absent"}, {}, "Unreadable Pub/Sub trial file"),
        ({"application_digest": "latest"}, {}, "sha256 --application-digest"),
        ({"run_id": "Not A Run"}, {}, "Invalid run ID"),
    ],
)
def test_a_dispatch_is_refused_before_it_touches_the_cluster_or_the_store(
    env, reviewed, dispatching, monkeypatch, tmp_path, change, env_change, match
):
    _, store, *_ = env
    for key, value in env_change.items():
        monkeypatch.setenv(key, value)
    before = copy.deepcopy((store.data, store.blobs, store.serial))
    value = args(tmp_path)
    for key, item in change.items():
        setattr(value, key, item)
    with pytest.raises(Failure, match=match):
        cli.start(value, store)
    assert dispatching == []
    assert (store.data, store.blobs, store.serial) == before


def test_a_reused_run_id_is_refused_before_it_touches_the_cluster(
    env, reviewed, dispatching, tmp_path
):
    _, store, *_ = env
    store.write("runs/ps-1429-0001/approval.json", {"old": True})
    before = copy.deepcopy((store.data, store.blobs, store.serial))
    with pytest.raises(Failure, match="already has immutable"):
        cli.start(args(tmp_path), store)
    assert dispatching == []
    assert (store.data, store.blobs, store.serial) == before


@pytest.fixture
def cluster(env, reviewed, renderer, dispatching, monkeypatch, tmp_path):
    """Everything outside the rig faked at its boundary, the rig itself real."""
    kube, store, reference, _ = env
    # The workflow sets this for the step; `idle=true` in it releases the lock.
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "github-output"))
    connected = []

    def external(kubeconfig, idle=False):
        connected.append(idle)
        return kube

    monkeypatch.setattr(cli, "external", external)
    checked = []
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda kubeconfig: SimpleNamespace(can_i=lambda *a: checked.append(a)),
    )
    namespaces = copy.deepcopy(reference["namespaces"])
    namespaces[PUBSUB] = namespaces.pop(SMOKE)
    snapshot = (
        namespaces,
        reference["operator_uid"],
        reference["images"]["operator"],
        reference["baseline_uids"],
    )
    snapshots = []

    events = []

    def observe(kube, namespace):
        snapshots.append(namespace)
        events.append("snapshot")
        return snapshot

    monkeypatch.setattr(cli.wf, "snapshot", observe)
    monkeypatch.setattr(
        cli.wf,
        "execution",
        lambda kind, nonce: {"kind": kind, "nonce": nonce, "sha": SHA},
    )
    receipts = []
    monkeypatch.setattr(
        cli.wf,
        "image_receipts",
        lambda session, images, expiry: receipts.append((images, expiry)) or {},
    )
    monkeypatch.setattr(cli.rt, "authorized_session", lambda token: None)
    monkeypatch.setattr(cli.rt, "GoogleToken", lambda: None)

    locks = []

    def acquire(owner):
        locks.append(owner)
        events.append("lock")

    monkeypatch.setattr(
        cli.rt, "EnvironmentLock", lambda store: SimpleNamespace(acquire=acquire)
    )
    artifacts = []
    write = cli.rt.write_artifact

    def artifact(store, run_id, name, value):
        artifacts.append(name)
        return write(store, run_id, name, value)

    monkeypatch.setattr(cli.rt, "write_artifact", artifact)
    fallback = []
    monkeypatch.setattr(
        cli,
        "runner_for",
        lambda approval, kube, store: SimpleNamespace(
            settle=lambda request_stop=False: fallback.append(request_stop)
        ),
    )
    bound = []
    original = cli.bind

    def bind(approved, delivery, when):
        bound.append((approved.to_dict(), when))
        return original(approved, delivery, when)

    monkeypatch.setattr(cli, "bind", bind)
    rendered = []
    render = cli.pubsub_plan.prepare

    def proposal(**kwargs):
        rendered.append(render(**kwargs))
        return rendered[-1]

    monkeypatch.setattr(cli.pubsub_plan, "prepare", proposal)
    return SimpleNamespace(
        kube=kube,
        store=store,
        reference=reference,
        connected=connected,
        rendered=rendered,
        checked=checked,
        snapshots=snapshots,
        receipts=receipts,
        bound=bound,
        locks=locks,
        fallback=fallback,
        events=events,
        artifacts=artifacts,
        snapshot=snapshot,
    )


class Recorded:
    """A runner that records what dispatch asked of it."""

    def __init__(self, env, fail=None):
        self.env, self.calls, self.fail = env, [], fail

    def start(self, config, job, application, probe):
        self.calls.append(("start", config, job, application, probe))
        # A termination while the run is admitted stops it rather than
        # killing the process before it settles.
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        self.stopped = self.env.stopping
        if self.fail:
            raise Failure(self.fail)

    def settle(self, request_stop=False):
        self.calls.append(("settle", request_stop))


def install(monkeypatch, fail=None, enter=None):
    """A runner factory that records the environment and token it is handed."""
    seen = {}

    @contextmanager
    def runner(env, *, runner_token, credentials=None):
        if enter:
            raise Failure(enter)
        seen.update(env=env, token=runner_token)
        seen["runner"] = Recorded(env, fail)
        yield seen["runner"]

    monkeypatch.setattr(cli.pubsub_actors, "runner", runner)
    return seen


def test_an_admitted_dispatch_runs_the_rig_with_the_delivery_its_approval_fixes(
    cluster, trial, tmp_path, monkeypatch
):
    seen = install(monkeypatch)
    cli.start(args(tmp_path), cluster.store)
    assert cluster.connected == [True]
    assert cluster.checked == [
        (True, "create", "flink.apache.org", "flinkdeployments", PUBSUB),
        # Admission creates the workload access probe Pod.
        (True, "create", "", "pods", PUBSUB),
    ]
    # Idle before the lock, and again once it is held.
    assert cluster.snapshots == [PUBSUB, PUBSUB]
    # Dispatch renders once and binds its approval into that delivery once;
    # nothing renders it again.
    assert len(cluster.rendered) == 1
    [(approval, when)] = cluster.bound
    assert when == NOW
    validate_approval(approval)
    assert approval["version"] == APPROVAL_VERSION
    assert approval["scenario"] == "pubsub-recovery"
    # The window starts at admission and lasts the scenario's policy.
    assert approval["started_at"] == cli.rt.utc(NOW)
    assert approval["expires_at"] == cli.rt.utc(NOW + WINDOW)
    assert approval["cleanup_at"] == cli.rt.utc(NOW + WINDOW - CLEANUP_SECONDS)
    assert approval["pubsub_trial"] == trial
    assert approval["lock_owner"]["run_id"] == "ps-1429-0001"
    assert approval["actor"] == "octocat"
    assert set(approval["namespaces"]) == {PUBSUB, cli.rt.SYSTEM}
    assert approval["images"] == {
        "operator": cluster.reference["images"]["operator"],
        "supervisor": cli.rt.GAR + "lifecycle-tools@sha256:" + "9" * 64,
        "application": cli.rt.GAR + "pubsub-recovery@" + DIGEST,
    }
    # The image receipts cover exactly the approved images, to the approved end.
    assert cluster.receipts == [(approval["images"], approval["expires_at"])]
    # The token is minted in this process and handed to nothing but the runner.
    assert seen["env"].actor == "runner"
    assert len(seen["token"]) == 32
    assert seen["token"] not in json.dumps(approval)
    rendered, runner = cluster.rendered[0], seen["runner"]
    delivery = rendered["delivery"]
    assert json.loads(delivery["config"]["data"]["approval.json"]) == approval
    assert delivery["supervisor"]["spec"]["activeDeadlineSeconds"] == (
        cli.rt.Schedule.for_window(NOW, NOW + WINDOW).active_seconds(NOW)
    )
    assert runner.calls == [
        (
            "start",
            delivery["config"],
            delivery["supervisor"],
            rendered["application"],
            delivery["probe"],
        ),
        ("settle", False),
    ]
    assert cluster.locks == [approval["lock_owner"]]
    assert cluster.fallback == []
    # Idle is rechecked once the lock is held.
    assert cluster.events == ["snapshot", "lock", "snapshot"]
    assert cluster.artifacts == [
        "approval.json",
        "application.json",
        "images.json",
        "upgrade-application.json",
    ]
    assert runner.stopped is True
    # What the workflow's plan proof and finalization need: the owner it
    # releases and the idle signal that runs them.
    owner = json.loads((tmp_path / "lifecycle" / "owner.json").read_text())
    assert owner == approval["lock_owner"]
    assert (tmp_path / "github-output").read_text() == "idle=true\n"
    documents = {
        name: cluster.store.read(f"runs/ps-1429-0001/{name}")[0]
        for name in (
            "approval.json",
            "application.json",
            "images.json",
            "upgrade-application.json",
        )
    }
    assert documents["approval.json"] == approval
    assert documents["application.json"] == rendered["application"]
    assert documents["upgrade-application.json"] == rendered["recovery_application"]
    assert documents["images.json"] is None or documents["images.json"] == {}
    control, _ = cluster.store.read("_control/runs/ps-1429-0001.json")
    assert control["nonce"] == approval["nonce"] and control["phase"] == "approved"


def test_a_failed_start_still_settles_with_a_stop(cluster, tmp_path, monkeypatch):
    seen = install(monkeypatch, fail="admission broke")
    with pytest.raises(Failure, match="admission broke"):
        cli.start(args(tmp_path), cluster.store)
    assert seen["runner"].calls[-1] == ("settle", True)
    assert cluster.fallback == []
    assert (tmp_path / "github-output").read_text() == "idle=true\n"


def test_a_foundation_that_changed_under_the_lock_admits_nothing(
    cluster, tmp_path, monkeypatch
):
    seen = install(monkeypatch)
    observed = []

    def observe(kube, namespace):
        observed.append(namespace)
        if len(observed) == 1:
            return cluster.snapshot
        namespaces, _, image, baseline = cluster.snapshot
        return namespaces, "replaced-operator", image, baseline

    monkeypatch.setattr(cli.wf, "snapshot", observe)
    with pytest.raises(Failure, match="Foundation changed"):
        cli.start(args(tmp_path), cluster.store)
    assert len(cluster.locks) == 1 and "runner" not in seen
    assert cluster.store.read("_control/runs/ps-1429-0001.json")[0] is None


def test_a_runner_that_cannot_be_built_after_the_lock_is_settled_by_a_plain_one(
    cluster, tmp_path, monkeypatch
):
    """Nothing is admitted and no Pub/Sub state exists, but the lock is held."""
    install(monkeypatch, enter="authentication refused")
    with pytest.raises(Failure, match="authentication refused"):
        cli.start(args(tmp_path), cluster.store)
    assert len(cluster.locks) == 1
    assert cluster.fallback == [True]
    assert (tmp_path / "github-output").read_text() == "idle=true\n"


def test_a_delivery_that_refuses_does_so_before_the_lock(
    cluster, monkeypatch, tmp_path
):
    render = cli.pubsub_plan.prepare

    def oversized(**kwargs):
        rendered = render(**kwargs)
        rendered["delivery"]["config"]["data"]["padding"] = "x" * MIB
        return rendered

    monkeypatch.setattr(cli.pubsub_plan, "prepare", oversized)
    with pytest.raises(Failure, match="ConfigMap exceeds"):
        cli.start(args(tmp_path), cluster.store)
    assert cluster.bound and cluster.locks == []


def test_the_approval_pins_the_manifests_the_proposal_rendered(
    cluster, trial, tmp_path, monkeypatch
):
    """The recovery manifest, not the initial one, becomes the upgrade digest."""
    install(monkeypatch)
    cli.start(args(tmp_path), cluster.store)
    [(approval, _)] = cluster.bound
    [rendered] = cluster.rendered
    initial, recovery = rendered["application"], rendered["recovery_application"]
    # `example-wiring` rescales out, so the two manifests differ.
    assert digest(initial) != digest(recovery)
    assert approval["application_sha256"] == digest(initial)
    assert approval["upgrade_application_sha256"] == digest(recovery)
    assert "recovery_application_sha256" not in approval
    assert (
        approval["images"]["supervisor"]
        == (rendered["proposal"]["images"]["supervisor"])
    )


def test_the_approval_holds_a_deep_copy_of_the_trial(trial):
    """A later edit to the trial dispatch holds cannot reach the approval."""
    approval = cli.pubsub_approval(
        run_id="ps-1429-0001",
        nonce="a" * 32,
        sha=SHA,
        trial=trial,
        proposal={
            "started_at": cli.rt.utc(NOW),
            "expires_at": cli.rt.utc(NOW + WINDOW),
            "cleanup_at": cli.rt.utc(NOW + WINDOW - CLEANUP_SECONDS),
            "application_sha256": "3" * 64,
            "recovery_application_sha256": "4" * 64,
        },
        namespaces={},
        operator_uid="operator-uid",
        baseline=[],
        images={},
        owner={},
        actor="octocat",
    )
    before = copy.deepcopy(approval["pubsub_trial"])
    trial["records_per_subscription"] = 2
    assert approval["pubsub_trial"] == before
    assert approval["upgrade_application_sha256"] == "4" * 64


def test_each_reviewed_trial_is_one_of_the_four_and_plans_its_input():
    checkout = Path(__file__).resolve().parents[4]
    reviewed = sorted((checkout / cli.PUBSUB_TRIALS).glob("*.toml"))
    trials = {path.stem: cli.pubsub_plan.load_reviewed_trial(path) for path in reviewed}
    assert set(trials) == set(cli.pubsub_plan.TRIALS)
    for name, reviewed_trial in trials.items():
        assert reviewed_trial["trial"] == name
        assert reviewed_trial["entry_point"] == "datastream"
        assert reviewed_trial["records_per_subscription"] == 1000
        cli.pubsub_plan.input_plan("ps-1361-0001", reviewed_trial)
