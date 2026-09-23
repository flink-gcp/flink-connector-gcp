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
"""Offline proposal contracts; cloud access and admission are never invoked."""

import copy
import json
from decimal import Decimal

import pytest
from flink_tier3 import bigquery_plan as plan
from flink_tier3 import render as command
from flink_tier3.bigquery import Trial
from flink_tier3.bigquery_resources import ResourcePlan
from flink_tier3.common import Failure, digest
from flink_tier3.model import validate_approval


@pytest.fixture
def trial():
    return plan.trial("eo-10")


@pytest.fixture
def inputs(trial):
    return {
        "run_id": "proposal-1312",
        "nonce": "a" * 32,
        "started_at": "2026-09-21T00:00:00Z",
        "expires_at": "2026-09-21T01:30:00Z",
        "active_seconds": 5220,
        "revision": "b" * 40,
        "application_image": plan.GAR + "bigquery-recovery@sha256:" + "c" * 64,
        "trial": trial,
    }


@pytest.fixture
def renderer(monkeypatch):
    calls = []

    def render(run_id, nonce, expiry, seconds, **options):
        calls.append((run_id, nonce, expiry, seconds, options))
        mode = options["bigquery_mode"]
        initial = {
            "metadata": {
                "name": run_id,
                "namespace": "tier3-bigquery",
                "annotations": {
                    "flink-gcp.io/approval": nonce,
                    "flink-gcp.io/expires-at": expiry,
                },
            },
            "spec": {
                "image": options["application_image"],
                "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2Gi"}},
                "taskManager": {"replicas": 2, "resource": {"cpu": 1, "memory": "2Gi"}},
                "job": {
                    "args": [
                        "--run-id",
                        run_id,
                        "--phase",
                        "initial",
                        "--mode",
                        mode,
                        "--destinations",
                        str(options["bigquery_destinations"]),
                        "--records",
                        "28800" if mode == "ALO" else "1843200",
                        "--bytes-per-second",
                        "1048576",
                        "--require-restored",
                        "false",
                    ]
                },
            },
        }
        for manager in ("jobManager", "taskManager"):
            initial["spec"][manager]["podTemplate"] = {
                "spec": {
                    "containers": [
                        {
                            "resources": {
                                category: dict(plan.POD_RESOURCES["smoke"])
                                for category in ("requests", "limits")
                            }
                        }
                    ]
                }
            }
        upgrade = copy.deepcopy(initial)
        upgrade["spec"]["job"]["args"][3] = "upgrade"
        upgrade["spec"]["job"]["args"][-1] = "true"
        delivery = {
            "config": {
                "data": {
                    "approval.json": "{}",
                    "application.json": json.dumps(initial),
                    "upgrade-application.json": json.dumps(upgrade),
                }
            },
            "supervisor": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        category: dict(plan.POD_RESOURCES["supervisor"])
                                        for category in ("requests", "limits")
                                    },
                                    "image": plan.GAR
                                    + "lifecycle-tools@sha256:"
                                    + "d" * 64,
                                }
                            ]
                        }
                    }
                }
            },
        }
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", render)
    monkeypatch.setattr(plan, "source_digest", lambda: "e" * 64)
    return calls


@pytest.mark.parametrize("mode,records", [("ALO", 28800), ("EO", 1843200)])
@pytest.mark.parametrize("destinations", [10, 50])
def test_complete_identity_and_budget_bindings(
    inputs, renderer, mode, records, destinations
):
    inputs["trial"].update(mode=mode, destinations=destinations)
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    assert proposal["kind"] == "bigquery-trial-proposal"
    assert proposal["approved"] is False
    assert proposal["runtime_sha256"] == "e" * 64
    assert proposal["application_sha256"] == digest(bundle["application"])
    assert proposal["upgrade_application_sha256"] == digest(
        bundle["upgrade_application"]
    )
    assert proposal["supervisor_sha256"] == digest(bundle["delivery"]["supervisor"])
    assert proposal["application_sha256"] != proposal["upgrade_application_sha256"]
    resource = proposal["resources"]
    assert resource["trial"] == {
        "run_id": inputs["run_id"],
        "mode": mode,
        "destinations": destinations,
        "records": records,
    }
    adapter_plan = ResourcePlan(**{**resource, "trial": Trial(**resource["trial"])})
    assert adapter_plan.job_body(11)["configuration"]["query"][
        "maximumBytesBilled"
    ] == str(4 * 1024**3)
    assert proposal["limits"]["input_bytes"] == 1800 * 1024**2
    assert proposal["limits"]["query_bytes"] == 48 * 1024**3
    assert proposal["cleanup_at"] == "2026-09-21T01:15:00Z"
    assert proposal["limits"]["pods"] == 6
    # The supervisor's shape is shared, and it grew so the scheduler cannot
    # squeeze it onto a busy node; the trial's estimate grows with it.
    assert proposal["cost"]["usd"] == "2.34915625"
    assert json.loads(bundle["delivery"]["config"]["data"]["proposal.json"]) == proposal
    assert len(renderer) == 1
    with pytest.raises(Failure, match="Incomplete run approval"):
        validate_approval(proposal)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 2),
        ("mode", "wrong"),
        ("destinations", True),
        ("destinations", 10.0),
        ("destinations", 11),
        # The query budget is the scenario's; a trial cannot carry one.
        ("query_slots", 12),
        ("additional_cost_usd", "5.00"),
        ("extra", "unsupported"),
    ],
)
def test_invalid_trial_refused_before_render(inputs, renderer, field, value):
    inputs["trial"][field] = value
    with pytest.raises(ValueError):
        plan.prepare(**inputs)
    assert renderer == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../bad"),
        ("nonce", "a" * 31),
        ("revision", "b" * 39),
        ("application_image", plan.GAR + "smoke@sha256:" + "c" * 64),
        ("started_at", "2026-09-21T00:00:01Z"),
        ("expires_at", "2026-09-21T01:29:59Z"),
        ("active_seconds", 5221),
        ("started_at", "2026-09-21T00:00:00"),
    ],
)
def test_invalid_delivery_identity_refused_before_render(
    inputs, renderer, field, value
):
    inputs[field] = value
    with pytest.raises((ValueError, Failure)):
        plan.prepare(**inputs)
    assert renderer == []


def test_a_late_start_renders_and_a_fractional_one_does_not(inputs, renderer):
    """The rates' age informs the estimate; it no longer closes the proposal."""
    inputs.update(started_at="2027-01-21T00:00:00Z", expires_at="2027-01-21T01:30:00Z")
    assert plan.prepare(**inputs)["proposal"]["started_at"] == inputs["started_at"]
    renderer.clear()
    inputs.update(
        started_at="2026-09-21T00:00:00.5Z", expires_at="2026-09-21T01:30:00.5Z"
    )
    with pytest.raises(ValueError, match="90-minute"):
        plan.prepare(**inputs)
    assert renderer == []


@pytest.mark.parametrize("destinations", [10.0, True, "10"])
def test_a_trial_is_one_of_the_four_by_type_as_well_as_value(destinations):
    """An approval is JSON read back; `10.0` compares equal to `10` in Python."""
    with pytest.raises(ValueError, match="ALO/EO and 10/50"):
        plan.validate_trial({"mode": "EO", "destinations": destinations})
    plan.validate_trial({"mode": "EO", "destinations": 10})


def test_the_estimate_charges_the_whole_query_budget():
    # 12 slots of 4 GiB at USD 6.25 per TiB, on top of the Pods and the reserve.
    query = Decimal(48) / 1024 * Decimal("6.25")
    assert plan.estimate() - query - plan.RESERVE_USD > 0
    assert plan.estimate().quantize(Decimal("0.01")) == Decimal("2.35")


def test_the_four_trials_are_each_mode_at_each_destination_count():
    assert {name: plan.trial(name) for name in plan.TRIALS} == {
        f"{mode.lower()}-{count}": {"mode": mode, "destinations": count}
        for mode in ("ALO", "EO")
        for count in (10, 50)
    }
    with pytest.raises(Failure, match="one of alo-10, eo-10, alo-50, eo-50"):
        plan.trial("none")


@pytest.mark.parametrize(
    "change,match",
    [
        ("initial", "proposed input"),
        ("upgrade", "more than phase"),
        ("approval", "unapproved manifests"),
        ("config", "unapproved manifests"),
    ],
)
def test_renderer_drift_does_not_produce_bound_proposal(
    inputs, renderer, monkeypatch, change, match
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        if change == "initial":
            initial["spec"]["job"]["args"][5] = "wrong"
        elif change == "upgrade":
            upgrade = copy.deepcopy(initial)
        elif change == "approval":
            delivery["config"]["data"]["approval.json"] = '{"approved":true}'
        else:
            delivery["config"]["data"]["application.json"] = "{}"
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match=match):
        plan.prepare(**inputs)


def test_cli_emits_proposal_and_rejects_cross_scenario_flags(
    tmp_path, renderer, capsys
):
    path = tmp_path / "trial.json"
    path.write_text("{}")
    args = [
        "--scenario",
        "bigquery-recovery",
        "--run-id",
        "proposal-1312",
        "--nonce",
        "a" * 32,
        "--started-at",
        "2026-09-21T00:00:00Z",
        "--expires-at",
        "2026-09-21T01:30:00Z",
        "--active-seconds",
        "5220",
        "--revision",
        "b" * 40,
        "--application-image",
        plan.GAR + "bigquery-recovery@sha256:" + "c" * 64,
        "--trial",
        "eo-10",
    ]
    command.main(args)
    assert json.loads(capsys.readouterr().out)["proposal"]["approved"] is False
    for option in (
        ["--cells-file", "unused"],
        ["--flink-version", "1.20.4"],
        ["--expression", "application"],
        ["--target", "wrong"],
        # A trial is named; the file form belongs to Pub/Sub proposals.
        ["--trial-file", str(path)],
    ):
        with pytest.raises(SystemExit) as error:
            command.main(args + option)
        assert error.value.code == 2
    assert len(renderer) == 1


def test_integral_fractional_timestamp_is_canonical_in_render(inputs, renderer):
    inputs["expires_at"] = "2026-09-21T01:30:00.000Z"
    bundle = plan.prepare(**inputs)
    assert renderer[0][2] == bundle["proposal"]["expires_at"] == "2026-09-21T01:30:00Z"
    assert (
        bundle["application"]["metadata"]["annotations"]["flink-gcp.io/expires-at"]
        == bundle["proposal"]["expires_at"]
    )


@pytest.mark.parametrize(
    "path,value,match",
    [
        (("metadata", "name"), "wrong", "proposed identity"),
        (("metadata", "namespace"), "tier3-smoke", "proposed identity"),
        (
            ("metadata", "annotations", "flink-gcp.io/approval"),
            "b" * 32,
            "proposed identity",
        ),
        (
            ("metadata", "annotations", "flink-gcp.io/expires-at"),
            "2026-09-21T02:00:00Z",
            "proposed identity",
        ),
        (
            ("spec", "image"),
            plan.GAR + "bigquery-recovery@sha256:" + "f" * 64,
            "proposed identity",
        ),
        (("spec", "jobManager", "replicas"), 2, "Pod budget"),
        (("spec", "taskManager", "replicas"), 3, "Pod budget"),
        (("spec", "jobManager", "resource", "cpu"), 2, "Pod budget"),
        (("spec", "taskManager", "resource", "memory"), "4Gi", "Pod budget"),
    ],
)
def test_consistent_renderer_identity_drift_is_rejected(
    inputs, renderer, monkeypatch, path, value, match
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        for application, key in (
            (initial, "application.json"),
            (upgrade, "upgrade-application.json"),
        ):
            target = application
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            delivery["config"]["data"][key] = json.dumps(application)
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match=match):
        plan.prepare(**inputs)


@pytest.mark.parametrize("manager", ["jobManager", "taskManager"])
@pytest.mark.parametrize("key", ["replicas", "resource"])
def test_missing_manager_budget_is_a_controlled_failure(
    inputs, renderer, monkeypatch, manager, key
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        del initial["spec"][manager][key]
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match="proposed Pod budget"):
        plan.prepare(**inputs)


@pytest.mark.parametrize("role", ["jobManager", "taskManager", "supervisor"])
@pytest.mark.parametrize("category", ["requests", "limits"])
@pytest.mark.parametrize("resource", ["cpu", "memory", "ephemeral-storage"])
def test_actual_pod_resource_drift_is_rejected(
    inputs, renderer, monkeypatch, role, category, resource
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        if role == "supervisor":
            pods = [delivery["supervisor"]["spec"]["template"]["spec"]]
        else:
            pods = [
                application["spec"][role]["podTemplate"]["spec"]
                for application in (initial, upgrade)
            ]
        for pod in pods:
            pod["containers"][0]["resources"][category][resource] = "20Gi"
        delivery["config"]["data"]["application.json"] = json.dumps(initial)
        delivery["config"]["data"]["upgrade-application.json"] = json.dumps(upgrade)
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match="proposed resource budget"):
        plan.prepare(**inputs)


@pytest.mark.parametrize("change", ["extra", "init", "missing"])
def test_unpriced_containers_or_resources_are_rejected(
    inputs, renderer, monkeypatch, change
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        pod = delivery["supervisor"]["spec"]["template"]["spec"]
        if change == "extra":
            pod["containers"].append(copy.deepcopy(pod["containers"][0]))
        elif change == "init":
            pod["initContainers"] = [{"name": "extra"}]
        else:
            del pod["containers"][0]["resources"]
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match="proposed (container|resource) budget"):
        plan.prepare(**inputs)


@pytest.mark.parametrize("key", ["containers", "initContainers", "ephemeralContainers"])
def test_shared_template_containers_are_rejected(inputs, renderer, monkeypatch, key):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        for application, name in (
            (initial, "application.json"),
            (upgrade, "upgrade-application.json"),
        ):
            application["spec"]["podTemplate"] = {"spec": {key: [{"name": "unpriced"}]}}
            delivery["config"]["data"][name] = json.dumps(application)
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match="Shared Pod template adds an unpriced container"):
        plan.prepare(**inputs)
