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
import re

import pytest
from flink_tier3 import render as command
from flink_tier3.bigquery import plan
from flink_tier3.bigquery.oracle import Trial
from flink_tier3.bigquery.resources import ResourcePlan
from flink_tier3.common import Failure, digest
from flink_tier3.model import validate_approval
from flink_tier3.policy import POD_RESOURCES
from flink_tier3.repository import ROOT


@pytest.fixture
def trial():
    return plan.trial("eo-10-sequence")


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
        # What `lifecycle/bigquery.cue` renders, written out rather than read
        # from the plan the proposal checks against.
        sizes = (2, 2)
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
                "taskManager": {
                    "replicas": sizes[0],
                    "resource": {"cpu": 1, "memory": "2Gi"},
                },
                "job": {
                    "parallelism": sizes[0],
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
                        str(options["bigquery_records"]),
                        "--bytes-per-second",
                        "1048576",
                        "--parallelism",
                        str(sizes[0]),
                        "--require-restored",
                        "false",
                        # `pkg/bigquery.#Application` renders the sink inputs
                        # at their defaults, in this order, and the uploader's
                        # bound only with the uploader on.
                        *(
                            [
                                "--staging-format",
                                "AVRO",
                                "--max-concurrent-checkpoint-finalizations",
                                "1",
                                "--max-concurrent-destinations",
                                "8",
                                "--max-staging-file-bytes",
                                "16777216",
                                "--max-open-destinations",
                                "16",
                                "--background-staging-upload-enabled",
                                *(
                                    ["true", "--max-in-flight-upload-operations", "2"]
                                    if options["bigquery_uploader"] == "true"
                                    else ["false"]
                                ),
                            ]
                            if mode == "FILE_LOADS"
                            else []
                        ),
                    ],
                },
            },
        }
        for manager in ("jobManager", "taskManager"):
            initial["spec"][manager]["podTemplate"] = {
                "spec": {
                    "containers": [
                        {
                            "resources": {
                                category: dict(POD_RESOURCES["smoke"])
                                for category in ("requests", "limits")
                            }
                        }
                    ]
                }
            }
        upgrade = copy.deepcopy(initial)
        args = upgrade["spec"]["job"]["args"]
        args[3] = "upgrade"
        args[args.index("--require-restored") + 1] = "true"
        args[args.index("--parallelism") + 1] = str(sizes[1])
        upgrade["spec"]["job"]["parallelism"] = sizes[1]
        upgrade["spec"]["taskManager"]["replicas"] = sizes[1]
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
                                        category: dict(POD_RESOURCES["supervisor"])
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
    return calls


@pytest.mark.parametrize("name", list(plan.TRIALS))
def test_complete_identity_and_budget_bindings(inputs, renderer, name):
    inputs["trial"] = plan.trial(name)
    mode, destinations, uploader = plan.TRIALS[name]
    records = plan.SEQUENCE_RECORDS[mode]
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    assert proposal["kind"] == "bigquery-trial-proposal"
    assert proposal["approved"] is False
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
        "uploader": uploader,
    }
    adapter_plan = ResourcePlan(**{**resource, "trial": Trial(**resource["trial"])})
    assert adapter_plan.job_body(0)["configuration"]["query"][
        "maximumBytesBilled"
    ] == str(4 * 1024**3)
    assert proposal["limits"]["input_bytes"] == 360 * 1024**2
    # The one query the sequence makes.
    assert proposal["limits"]["query_bytes"] == 4 * 1024**3
    assert proposal["cleanup_at"] == "2026-09-21T01:15:00Z"
    # The ResourceQuotas bound what a run may hold; the proposal counts no Pods.
    assert set(proposal["limits"]) == {
        "seconds",
        "cleanup_seconds",
        "input_bytes",
        "query_bytes",
    }
    assert "cost" not in proposal
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
    """No review date closes the proposal; a whole-second window is all it needs."""
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
def test_a_trial_is_one_of_the_three_by_type_as_well_as_value(destinations):
    """An approval is JSON read back; `10.0` compares equal to `10` in Python."""
    with pytest.raises(ValueError, match="Trial must be one of"):
        plan.validate_trial(
            {**plan.trial("eo-10-sequence"), "destinations": destinations}
        )
    plan.validate_trial(plan.trial("eo-10-sequence"))


def test_the_trials_are_the_sequence_once_per_delivery_method():
    """FILE_LOADS with the uploader on, as the connector runs it by default."""
    assert {name: plan.trial(name) for name in plan.TRIALS} == {
        "alo-10-sequence": {
            "mode": "ALO",
            "destinations": 10,
            "records": 5760,
        },
        "eo-10-sequence": {
            "mode": "EO",
            "destinations": 10,
            "records": 368640,
        },
        "fl-10-uploader-sequence": {
            "mode": "FILE_LOADS",
            "destinations": 10,
            "uploader": True,
            "records": 368640,
        },
    }
    message = "one of alo-10-sequence, eo-10-sequence, fl-10-uploader-sequence$"
    for name in ("none", "eo-10", "fl-10-uploader", "eo-10-tm-replacement"):
        with pytest.raises(Failure, match=message):
            plan.trial(name)


def test_file_loads_is_a_trial_at_ten_destinations_only():
    """One FILE_LOADS trial asks about recovery, not about scale."""
    with pytest.raises(ValueError, match="Trial must be one of"):
        plan.validate_trial(
            {**plan.trial("fl-10-uploader-sequence"), "destinations": 50}
        )


def test_a_file_loads_upgrade_changes_only_phase_and_restoration(inputs, renderer):
    """The sink inputs follow the restoration flag, so it is not the last one."""
    inputs["trial"] = plan.trial("fl-10-uploader-sequence")
    bundle = plan.prepare(**inputs)
    initial = bundle["application"]["spec"]["job"]["args"]
    upgrade = bundle["upgrade_application"]["spec"]["job"]["args"]
    assert initial[-4:] == upgrade[-4:]
    assert [i for i, (a, b) in enumerate(zip(initial, upgrade)) if a != b] == [
        3,
        initial.index("--require-restored") + 1,
    ]
    assert bundle["proposal"]["limits"]["input_bytes"] == 360 * 1024**2


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


def test_cli_emits_proposal_and_rejects_cross_scenario_flags(renderer, capsys):
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
        "--bigquery-trial",
        "eo-10-sequence",
    ]
    command.main(args)
    assert json.loads(capsys.readouterr().out)["proposal"]["approved"] is False
    for option in (
        ["--expression", "application"],
        # The Pub/Sub trial belongs to Pub/Sub proposals.
        ["--pubsub-trial", "sequence"],
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


@pytest.mark.parametrize(
    "name,uploader",
    [
        # The uploader only for FILE_LOADS, only as true, and never as a string.
        ("eo-10-sequence", True),
        ("fl-10-uploader-sequence", False),
        ("fl-10-uploader-sequence", "true"),
        ("fl-10-uploader-sequence", 1),
    ],
)
def test_the_uploader_is_refused_outside_its_cells(name, uploader):
    with pytest.raises(ValueError, match="Trial must be one of"):
        plan.validate_trial({**plan.trial(name), "uploader": uploader})


def test_an_uploader_trial_renders_and_proposes_the_uploader(inputs, renderer):
    inputs["trial"] = plan.trial("fl-10-uploader-sequence")
    bundle = plan.prepare(**inputs)
    assert renderer[0][4]["bigquery_uploader"] == "true"
    assert bundle["proposal"]["resources"]["trial"]["uploader"] is True
    for application in ("application", "upgrade_application"):
        assert bundle[application]["spec"]["job"]["args"][-4:] == [
            "--background-staging-upload-enabled",
            "true",
            "--max-in-flight-upload-operations",
            "2",
        ]


@pytest.mark.parametrize(
    "phase,field,match",
    [
        ("initial", "job", "proposed input"),
        ("initial", "taskManager", "proposed Pod budget"),
        ("upgrade", "job", "more than phase"),
        ("upgrade", "taskManager", "more than phase"),
    ],
)
def test_a_rendered_size_other_than_the_trials_is_refused(
    inputs, renderer, monkeypatch, phase, field, match
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        app = initial if phase == "initial" else upgrade
        key = "parallelism" if field == "job" else "replicas"
        app["spec"][field][key] = 1
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match=match):
        plan.prepare(**inputs)


def checkpoint_intervals():
    """Each mode's checkpoint interval, as the CUE package configures it."""
    source = (ROOT / "kubernetes/pkg/bigquery/application.cue").read_text()
    [table] = re.findall(r"let interval = \{([^}]*)\}\[run\.mode\]", source)
    return {
        mode: int(seconds) for mode, seconds in re.findall(r'(\w+): "(\d+) s"', table)
    }


def test_the_intervals_are_the_packages():
    assert checkpoint_intervals() == {"ALO": 30, "EO": 30, "FILE_LOADS": 120}


@pytest.mark.parametrize(
    "value",
    [
        # The sequence only at its cells and with its mode's input, and no
        # trial without it.
        {"mode": "ALO", "destinations": 10},
        {"mode": "EO", "destinations": 10, "fault": "tm-replacement"},
        {"mode": "ALO", "destinations": 10, "records": 5761},
        {"mode": "EO", "destinations": 10, "records": 5760},
        {"mode": "ALO", "destinations": 50, "records": 5760},
        {"mode": "EO", "destinations": 50, "records": 368640},
        # The fault, which every trial named until #2279, is no field.
        {"mode": "ALO", "destinations": 10, "fault": "sequence", "records": 5760},
        # FILE_LOADS runs it with the uploader on, as the connector does.
        {"mode": "FILE_LOADS", "destinations": 10, "records": 368640},
    ],
)
def test_the_sequence_is_refused_outside_its_cell(value):
    with pytest.raises(
        ValueError,
        match="one of alo-10-sequence, eo-10-sequence, fl-10-uploader-sequence$",
    ):
        plan.validate_trial(value)


@pytest.mark.parametrize(
    "name,records",
    [
        # Six minutes at 1 MiB/s: 64 KiB rows, or 1 KiB rows.
        ("alo-10-sequence", 5760),
        ("eo-10-sequence", 368640),
        ("fl-10-uploader-sequence", 368640),
    ],
)
def test_the_sequence_proposes_its_input_at_the_default_sizes(
    inputs, renderer, name, records
):
    """Six minutes of the mode's rows, started and upgraded at two writers."""
    inputs["trial"] = plan.trial(name)
    bundle = plan.prepare(**inputs)
    options = renderer[0][4]
    assert "bigquery_fault" not in options
    assert options["bigquery_records"] == records
    resources = bundle["proposal"]["resources"]["trial"]
    assert "fault" not in resources
    assert resources["records"] == records
    assert bundle["proposal"]["limits"]["input_bytes"] == 360 * 1024**2
    for application in ("application", "upgrade_application"):
        spec = bundle[application]["spec"]
        assert spec["job"]["parallelism"] == spec["taskManager"]["replicas"] == 2
        args = spec["job"]["args"]
        assert args[args.index("--records") + 1] == str(records)


PROPOSAL = [
    "--started-at",
    "2026-09-21T00:00:00Z",
    "--revision",
    "b" * 40,
    "--application-image",
    plan.GAR + "pubsub-recovery@sha256:" + "c" * 64,
]


@pytest.mark.parametrize(
    "scenario,extra,message",
    [
        (
            "smoke",
            ["--bigquery-trial", "eo-10-sequence"],
            "trial inputs apply only to service trial proposals",
        ),
        (
            "pubsub-recovery",
            [
                *PROPOSAL,
                "--pubsub-trial",
                "sequence",
                "--bigquery-trial",
                "eo-10-sequence",
            ],
            "pubsub-recovery requires --pubsub-trial, --started-at",
        ),
    ],
)
def test_the_bigquery_trial_flag_is_refused_outside_its_scenario(
    capsys, scenario, extra, message
):
    argv = [
        "--run-id",
        "proposal-1312",
        "--nonce",
        "a" * 32,
        "--expires-at",
        "2026-09-21T01:30:00Z",
        "--active-seconds",
        "5220",
        "--scenario",
        scenario,
        *extra,
    ]
    with pytest.raises(SystemExit) as error:
        command.main(argv)
    assert error.value.code == 2
    assert message in capsys.readouterr().err
