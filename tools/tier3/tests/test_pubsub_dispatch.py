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
from pathlib import Path
from types import SimpleNamespace

import pytest
from flink_tier3 import approval_bundle
from flink_tier3 import lifecycle as cli
from flink_tier3 import pubsub_bundle as bundles
from flink_tier3.common import Failure, digest
from flink_tier3.model import validate_approval
from flink_tier3.policy import PUBSUB, PUBSUB_CEILINGS, SMOKE
from test_pubsub_plan import FIXTURE_TRIALS
from test_pubsub_plan import renderer as renderer  # noqa: PLC0414
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414

DIGEST = "sha256:" + "d" * 64
SHA = "e" * 40
NOW = cli.rt.timestamp("2026-09-21T00:00:00Z")
WINDOW = PUBSUB_CEILINGS["seconds"]
EXPIRY = cli.rt.utc(NOW + WINDOW + 60)
PHRASE = (
    "APPROVE ONE PUBSUB TRIAL: 7 PODS, 60 MINUTES, "
    "1000 RECORDS PER SUBSCRIPTION, 100000 REQUESTS"
)


@pytest.fixture
def reviewed(monkeypatch):
    """Resolve trial names against the tests' fixture directory."""
    monkeypatch.setattr(cli, "ROOT", FIXTURE_TRIALS.parent)
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

    monkeypatch.setattr(cli.wf, "external", external)
    return touched


def args(tmp_path, **overrides):
    value = {
        "scenario": "pubsub-recovery",
        "pubsub_trial": "example-wiring",
        "application_digest": DIGEST,
        "run_id": "ps-1429-0001",
        "sha": SHA,
        "expires_at": EXPIRY,
        "approve": PHRASE,
        "kubeconfig": tmp_path / "kubeconfig",
        "directory": tmp_path / "lifecycle",
    }
    value.update(overrides)
    return SimpleNamespace(**value)


def test_the_phrase_names_the_policy_and_the_trial_ceilings(trial):
    assert cli.pubsub_phrase(trial) == PHRASE
    trial.update(records_per_subscription=10, total_request_limit=30000)
    assert cli.pubsub_phrase(trial) == (
        "APPROVE ONE PUBSUB TRIAL: 7 PODS, 60 MINUTES, "
        "10 RECORDS PER SUBSCRIPTION, 30000 REQUESTS"
    )


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


@pytest.mark.parametrize("slack", [0, 1, 599, 600])
def test_the_window_starts_at_admission_and_ends_by_the_typed_expiry(slack):
    started, end = cli.service_window(
        NOW + 0.7, cli.rt.utc(NOW + WINDOW + slack), WINDOW, "Pub/Sub"
    )
    assert started == NOW
    assert end == NOW + WINDOW


@pytest.mark.parametrize("slack", [-1, 601])
def test_an_expiry_that_would_not_bound_the_run_is_refused(slack):
    with pytest.raises(Failure, match="Pub/Sub expiry must be 60 to 70 minutes"):
        cli.service_window(NOW, cli.rt.utc(NOW + WINDOW + slack), WINDOW, "Pub/Sub")


@pytest.mark.parametrize(
    "change, env_change, match",
    [
        ({"approve": cli.APPROVAL}, {}, "explicitly approve"),
        ({"approve": cli.BIGQUERY_APPROVAL}, {}, "explicitly approve"),
        # Another trial's numbers do not approve this one.
        (
            {"approve": PHRASE.replace("1000 RECORDS", "2000 RECORDS")},
            {},
            "explicitly approve",
        ),
        (
            {"approve": PHRASE.replace("100000 REQUESTS", "30000 REQUESTS")},
            {},
            "explicitly approve",
        ),
        ({"sha": "f" * 40}, {}, "explicitly approve"),
        ({}, {"GITHUB_REF": "refs/heads/feature"}, "explicitly approve"),
        ({"pubsub_trial": "absent"}, {}, "Unreadable Pub/Sub trial file"),
        ({"application_digest": "latest"}, {}, "sha256 --application-digest"),
        ({"run_id": "Not A Run"}, {}, "Invalid run ID"),
        ({"expires_at": cli.rt.utc(NOW + WINDOW - 1)}, {}, "60 to 70 minutes"),
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
    monkeypatch.setattr(approval_bundle, "check_revision", lambda revision: None)
    # The workflow sets this for the step; `idle=true` in it releases the lock.
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "github-output"))
    connected = []

    def external(kubeconfig, idle=False):
        connected.append(idle)
        return kube

    monkeypatch.setattr(cli.wf, "external", external)
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

    def observe(kube, namespace):
        snapshots.append(namespace)
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

    def lock(store):
        raise AssertionError("Pub/Sub dispatch must not reach the lock")

    monkeypatch.setattr(cli.rt, "EnvironmentLock", lock)
    bundled = []
    original = bundles.prepare

    def prepare(approval, *, prepared_at):
        bundled.append(copy.deepcopy(approval))
        return original(approval, prepared_at=prepared_at)

    monkeypatch.setattr(bundles, "prepare", prepare)
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
        bundled=bundled,
    )


def test_a_verified_approval_is_refused_before_the_lock(cluster, trial, tmp_path):
    """The approval and its bundle are built and checked; nothing is admitted."""
    store, kube = cluster.store, cluster.kube
    before = copy.deepcopy((store.data, store.blobs, store.serial, kube.data))
    calls = len(kube.calls)
    with pytest.raises(Failure, match="admission is not implemented"):
        cli.start(args(tmp_path), store)
    # Neither the lock, evidence, control, a cluster change, nor a local document
    # or step output that finalization reads.
    assert (store.data, store.blobs, store.serial, kube.data) == before
    assert len(kube.calls) == calls
    assert not (tmp_path / "lifecycle").exists()
    assert not (tmp_path / "github-output").exists()
    assert cluster.connected == [True]
    assert cluster.checked == [
        (True, "create", "flink.apache.org", "flinkdeployments", PUBSUB)
    ]
    assert cluster.snapshots == [PUBSUB]
    [approval] = cluster.bundled
    validate_approval(approval, NOW)
    assert approval["version"] == 5
    assert approval["scenario"] == "pubsub-recovery"
    assert approval["started_at"] == cli.rt.utc(NOW)
    assert approval["expires_at"] == cli.rt.utc(NOW + WINDOW)
    assert approval["cleanup_at"] == cli.rt.utc(NOW + WINDOW - 900)
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


def test_a_bundle_that_refuses_does_so_before_the_lock(cluster, monkeypatch, tmp_path):
    def refuse(revision):
        raise Failure("Repository revision differs from approval")

    monkeypatch.setattr(approval_bundle, "check_revision", refuse)
    with pytest.raises(Failure, match="revision differs"):
        cli.start(args(tmp_path), cluster.store)
    assert cluster.bundled


def test_the_approval_pins_the_manifests_the_proposal_rendered(
    cluster, trial, tmp_path
):
    """The recovery manifest, not the initial one, becomes the upgrade digest."""
    with pytest.raises(Failure, match="admission is not implemented"):
        cli.start(args(tmp_path), cluster.store)
    [approval] = cluster.bundled
    # The first render is dispatch's own; the bundle re-renders after it.
    rendered = cluster.rendered[0]
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
            "cleanup_at": cli.rt.utc(NOW + WINDOW - 900),
            "runtime_sha256": "1" * 64,
            "delivery_sha256": "2" * 64,
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
    trial["traffic_limits"]["publish_calls"] = 1
    trial["records_per_subscription"] = 2
    assert approval["pubsub_trial"] == before
    assert approval["upgrade_application_sha256"] == "4" * 64
