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
"""The BigQuery dispatch boundary: what it admits, and what it refuses first."""

import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from flink_tier3 import bigquery_bundle as bundles
from flink_tier3 import bigquery_plan as plan
from flink_tier3 import lifecycle as cli
from flink_tier3.bundle import delivery_digest, source_digest
from flink_tier3.common import Failure
from flink_tier3.model import Approval, validate_approval
from flink_tier3.policy import BIGQUERY, BIGQUERY_CEILINGS, SMOKE
from test_bigquery_approval import prepared as prepared  # noqa: PLC0414
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414

DIGEST = "sha256:" + "d" * 64
SHA = "e" * 40
# Inside both pricing reviews, so the approval model admits it; frozen so the
# suite does not start failing the day those reviews age out.
NOW = cli.rt.timestamp("2026-09-21T00:00:00Z")
WINDOW = BIGQUERY_CEILINGS["seconds"]
EXPIRY = cli.rt.utc(NOW + WINDOW + 60)
# The real collaborators, before any fixture replaces them.
RUNNER_FOR = cli.runner_for
ENVIRONMENT_LOCK = cli.rt.EnvironmentLock


@pytest.fixture
def reviewed(trial):
    """The trial `args` names, as dispatch resolves it."""
    return trial


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
        "scenario": "bigquery-recovery",
        "trial": "eo-10",
        "application_digest": DIGEST,
        "run_id": "bq-1424-0001",
        "sha": SHA,
        "expires_at": EXPIRY,
        "kubeconfig": tmp_path / "kubeconfig",
        "directory": tmp_path,
    }
    value.update(overrides)
    return SimpleNamespace(**value)


def test_the_phrase_names_the_run_and_no_amount():
    assert cli.BIGQUERY_APPROVAL == "APPROVE ONE BIGQUERY TRIAL: 6 PODS, 90 MINUTES"


def test_the_trial_resolves_from_its_name_and_the_live_digest(reviewed, tmp_path):
    trial, image = cli.trial_inputs(args(tmp_path))
    assert trial == reviewed
    assert image == cli.rt.GAR + "bigquery-recovery@" + DIGEST


@pytest.mark.parametrize(
    "change, match",
    [
        ({"trial": None}, "one of alo-10"),
        ({"trial": "none"}, "one of alo-10"),
        ({"trial": "eo-10.json"}, "one of alo-10"),
        ({"application_digest": None}, "sha256 --application-digest"),
        ({"application_digest": "latest"}, "sha256 --application-digest"),
    ],
)
def test_a_trial_that_does_not_resolve_is_refused(reviewed, tmp_path, change, match):
    with pytest.raises(Failure, match=match):
        cli.trial_inputs(args(tmp_path, **change))


@pytest.mark.parametrize("slack", [0, 1, 599, 600])
def test_the_window_starts_at_admission_and_ends_by_the_typed_expiry(slack):
    started, end = cli.bigquery_window(NOW + 0.7, cli.rt.utc(NOW + WINDOW + slack))
    # Whole seconds, and never after the moment it was admitted.
    assert started == NOW
    assert end == NOW + WINDOW


@pytest.mark.parametrize("slack", [-1, 601])
def test_an_expiry_that_would_not_bound_the_run_is_refused(slack):
    """Earlier than the run's own end outlasts the approval; much later misleads."""
    with pytest.raises(Failure, match="90 to 100 minutes"):
        cli.bigquery_window(NOW, cli.rt.utc(NOW + WINDOW + slack))


@pytest.mark.parametrize(
    "change, env_change, match",
    [
        ({"approve": cli.APPROVAL}, {}, "explicitly approve"),
        ({"approve": cli.CLOUDTASKS_APPROVAL}, {}, "explicitly approve"),
        # The old phrase carried an amount; it no longer approves anything.
        (
            {"approve": "APPROVE ONE BIGQUERY TRIAL: 6 PODS, 90 MINUTES, USD 3.00"},
            {},
            "explicitly approve",
        ),
        ({"sha": "f" * 40}, {}, "explicitly approve"),
        ({}, {"GITHUB_REF": "refs/heads/feature"}, "explicitly approve"),
        ({"trial": "none"}, {}, "one of alo-10"),
        ({"run_id": "Not A Run"}, {}, "Invalid run ID"),
        ({"expires_at": cli.rt.utc(NOW + WINDOW - 1)}, {}, "90 to 100 minutes"),
    ],
)
def test_a_dispatch_is_refused_before_it_touches_the_cluster_or_the_store(
    env, reviewed, dispatching, monkeypatch, tmp_path, change, env_change, match
):
    _, store, *_ = env
    for key, value in env_change.items():
        monkeypatch.setenv(key, value)
    before = copy.deepcopy(store.data)
    value = args(tmp_path, approve=cli.BIGQUERY_APPROVAL)
    for key, item in change.items():
        setattr(value, key, item)
    with pytest.raises(Failure, match=match):
        cli.start(value, store)
    # The recording client is the one dispatch would have used: it was never
    # built, and not a byte was written, neither the lock nor any evidence.
    assert dispatching == []
    assert store.data == before


def test_a_reused_run_id_is_refused_before_it_touches_the_cluster(
    env, reviewed, dispatching, tmp_path
):
    _, store, *_ = env
    store.write("runs/bq-1424-0001/approval.json", {"old": True})
    with pytest.raises(Failure, match="already has immutable"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), store)
    assert dispatching == []


class Recorded:
    """A runner that records what dispatch asked of it."""

    def __init__(self, env, fail=None):
        self.env, self.calls, self.fail = env, [], fail

    def start(self, config, job, application):
        self.calls.append(("start", config, job, application))
        if self.fail:
            raise Failure(self.fail)

    def settle(self, request_stop=False):
        self.calls.append(("settle", request_stop))


@pytest.fixture
def cluster(env, reviewed, renderer, dispatching, monkeypatch):
    """Everything outside the rig faked at its boundary, the rig itself real."""
    kube, store, reference, _ = env
    monkeypatch.setattr(plan, "source_digest", source_digest)
    monkeypatch.setattr(bundles, "_check_revision", lambda revision: None)
    monkeypatch.setattr(cli.wf, "external", lambda kubeconfig, idle=False: kube)
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda kubeconfig: SimpleNamespace(can_i=lambda *_args: None),
    )
    namespaces = copy.deepcopy(reference["namespaces"])
    namespaces[BIGQUERY] = namespaces.pop(SMOKE)
    snapshot = (
        namespaces,
        reference["operator_uid"],
        reference["images"]["operator"],
        reference["baseline_uids"],
    )
    monkeypatch.setattr(cli.wf, "snapshot", lambda kube, namespace: snapshot)
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
    fallback = []
    monkeypatch.setattr(
        cli,
        "runner_for",
        lambda approval, kube, store: SimpleNamespace(
            settle=lambda request_stop=False: fallback.append(request_stop)
        ),
    )
    return SimpleNamespace(kube=kube, store=store, locks=locks, fallback=fallback)


def install(monkeypatch, fail=None, enter=None):
    """A runner factory that checks the bundle it is handed, as the real one does."""
    seen = {}

    @contextmanager
    def runner(env, bundle, *, runner_token, credentials=None):
        if enter:
            raise Failure(enter)
        # The bundle dispatch hands over must be the one this approval admits:
        # the real factory re-renders and compares it byte for byte.
        bundles.validate(bundle, env.approval.to_dict())
        seen.update(env=env, bundle=bundle, token=runner_token)
        seen["runner"] = Recorded(env, fail)
        yield seen["runner"]

    monkeypatch.setattr(cli.bigquery_actors, "runner", runner)
    return seen


def test_an_admitted_dispatch_runs_the_rig_with_the_bundle_its_approval_fixes(
    cluster, reviewed, tmp_path, monkeypatch
):
    seen = install(monkeypatch)
    cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    approval = seen["env"].approval
    validate_approval(approval.to_dict(), NOW)
    assert seen["env"].actor == "runner"
    assert approval.started_at == cli.rt.utc(NOW)
    assert approval.expires_at == cli.rt.utc(NOW + WINDOW)
    assert approval.images["application"] == cli.rt.GAR + "bigquery-recovery@" + DIGEST
    # The token is minted in this process and handed to nothing but the runner.
    assert len(seen["token"]) == 32
    assert seen["token"] not in json.dumps(approval.to_dict())
    assert seen["token"] not in json.dumps(seen["bundle"])
    bundle, runner = seen["bundle"], seen["runner"]
    assert runner.calls == [
        (
            "start",
            bundle["delivery"]["config"],
            bundle["delivery"]["supervisor"],
            bundle["application"],
        ),
        ("settle", False),
    ]
    assert cluster.locks == [approval.lock_owner]
    assert cluster.fallback == []
    documents = {
        name: cluster.store.read(f"runs/bq-1424-0001/{name}")[0]
        for name in (
            "approval.json",
            "application.json",
            "images.json",
            "upgrade-application.json",
        )
    }
    # The stored document is the dict dispatch validated; the ConfigMap embeds
    # its normalized form. They must be the same approval, not the same bytes.
    assert Approval.from_dict(documents["approval.json"]) == approval
    assert documents["application.json"] == bundle["application"]
    assert documents["upgrade-application.json"] == bundle["upgrade_application"]
    assert documents["images.json"] == {}
    assert json.loads(bundle["delivery"]["config"]["data"]["approval.json"]) == (
        approval.to_dict()
    )
    assert approval.delivery_sha256 == delivery_digest()


def test_a_failed_start_still_settles_with_a_stop(
    cluster, reviewed, tmp_path, monkeypatch
):
    seen = install(monkeypatch, fail="admission broke")
    with pytest.raises(Failure, match="admission broke"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    assert seen["runner"].calls[-1] == ("settle", True)
    assert cluster.fallback == []


def test_a_runner_that_cannot_be_built_after_the_lock_is_settled_by_a_plain_one(
    cluster, reviewed, tmp_path, monkeypatch
):
    """Nothing is admitted and no BigQuery state exists, but the lock is held."""
    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    install(monkeypatch, enter="authentication refused")
    with pytest.raises(Failure, match="authentication refused"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    assert len(cluster.locks) == 1
    assert cluster.fallback == [True]
    # Idle is what gates the workflow's plan proof and finalization, the only
    # steps that release the lock.
    assert output.read_text() == "idle=true\n"


def test_the_plain_fallback_leaves_a_run_the_workflow_finalizes(
    cluster, reviewed, tmp_path, monkeypatch
):
    """The real fallback and lock: the workflow's finalization releases the lock."""
    monkeypatch.setattr(cli, "runner_for", RUNNER_FOR)
    monkeypatch.setattr(cli.rt, "EnvironmentLock", ENVIRONMENT_LOCK)
    # The shared fixture holds another run's lock and records in the smoke
    # namespace; this run starts from an unlocked store and owns BigQuery's.
    for key in [key for key in cluster.store.data if key[1].startswith("_control/")]:
        del cluster.store.data[key]
    for (kind, namespace, name), value in list(cluster.kube.data.items()):
        if namespace == SMOKE:
            cluster.kube.data[(kind, BIGQUERY, name)] = value
    identity = cluster.kube.namespace
    monkeypatch.setattr(
        cluster.kube,
        "namespace",
        lambda namespace: identity(SMOKE if namespace == BIGQUERY else namespace),
    )
    install(monkeypatch, enter="authentication refused")
    with pytest.raises(Failure, match="authentication refused"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    approval = json.loads((tmp_path / "approval.json").read_text())
    assert cluster.store.read(cli.rt.ENVIRONMENT)[0] == approval["lock_owner"]
    plans = {"nonce": approval["nonce"], "roots": list(cli.wf.ROOTS), "empty": True}
    assert RUNNER_FOR(approval, cluster.kube, cluster.store).finalize(plans) is False
    assert not [key for key in cluster.store.data if key[1].startswith("_control/")]


def test_one_schedulable_node_is_refused_before_the_lock(
    cluster, reviewed, tmp_path, monkeypatch
):
    """The 2026-09-23 pilot lost its supervisor to a system Pod on one node."""
    install(monkeypatch)
    ready = {
        "metadata": {"labels": {"kubernetes.io/arch": "amd64"}},
        "spec": {},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    draining = {**ready, "spec": {"unschedulable": True}}
    cluster.kube.node_list = [ready, draining]
    before = copy.deepcopy(cluster.store.data)
    with pytest.raises(Failure, match="none or at least two"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    assert cluster.locks == []
    assert cluster.store.data == before
    # With a second node the supervisor could use, the same dispatch proceeds.
    cluster.kube.node_list = [ready, copy.deepcopy(ready)]
    cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    assert len(cluster.locks) == 1


def test_a_bundle_that_refuses_does_so_before_the_lock(
    cluster, reviewed, tmp_path, monkeypatch
):
    """Revision, render and ConfigMap size are all checked without holding anything."""
    install(monkeypatch)
    before = copy.deepcopy(cluster.store.data)

    def refuse(revision):
        raise Failure("Repository revision differs from approval")

    monkeypatch.setattr(bundles, "_check_revision", refuse)
    with pytest.raises(Failure, match="revision differs"):
        cli.start(args(tmp_path, approve=cli.BIGQUERY_APPROVAL), cluster.store)
    assert cluster.locks == []
    assert cluster.store.data == before


def test_the_assembled_approval_is_the_one_the_model_admits(prepared):
    """Assembled from a verified proposal, it must pass the v4 contract as is."""
    environment, _, _, proposal = prepared
    reference = environment.approval.to_dict()
    approval = cli.bigquery_approval(
        run_id=reference["run_id"],
        nonce=reference["nonce"],
        sha=reference["sha"],
        trial=reference["bigquery_trial"],
        proposal=dict(
            proposal,
            application_sha256=reference["application_sha256"],
            upgrade_application_sha256=reference["upgrade_application_sha256"],
            runtime_sha256=reference["runtime_sha256"],
            delivery_sha256=reference["delivery_sha256"],
        ),
        namespaces=reference["namespaces"],
        operator_uid=reference["operator_uid"],
        baseline=reference["baseline_uids"],
        images=reference["images"],
        owner=reference["lock_owner"],
        actor="octocat",
    )
    validate_approval(approval, environment.clock())
    assert approval["version"] == 4
    assert approval["scenario"] == "bigquery-recovery"
    # A copy, so a later edit to the trial the dispatch holds cannot reach it.
    assert approval["bigquery_trial"] == reference["bigquery_trial"]
    assert approval["bigquery_trial"] is not reference["bigquery_trial"]
