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
"""Reject unusable offline proposals before rendering or cloud access."""

import copy
import json
from pathlib import Path

import pytest
from flink_tier3 import lifecycle
from flink_tier3 import render as command
from flink_tier3.common import Failure
from flink_tier3.model import validate_approval
from flink_tier3.pubsub import plan


@pytest.fixture
def trial():
    return {
        "version": 3,
        "trial": "rescale-out",
        "entry_point": "datastream",
        "records_per_subscription": 1000,
        "traffic_limits": dict(plan.COUNTER_CEILINGS),
        "total_request_limit": 100000,
    }


@pytest.fixture
def inputs(trial, monkeypatch):
    monkeypatch.setattr(
        plan, "render", lambda *a, **k: pytest.fail("Invalid input reached CUE")
    )
    return {
        "run_id": "proposal-1361",
        "nonce": "a" * 32,
        "started_at": "2026-09-21T00:00:00Z",
        "expires_at": "2026-09-21T01:00:00Z",
        "active_seconds": 3420,
        "revision": "b" * 40,
        "application_image": plan.GAR + "pubsub-recovery@sha256:" + "c" * 64,
        "trial": trial,
    }


@pytest.fixture
def renderer(monkeypatch):
    """A render that answers with the manifests the real CUE produces."""
    calls = []

    def pod(role):
        return {
            "containers": [
                {
                    "resources": {
                        category: dict(plan.POD_RESOURCES[role])
                        for category in ("requests", "limits")
                    }
                }
            ]
        }

    def application(run_id, nonce, expiry, options, parallelism, phase):
        spec = {
            "image": options["application_image"],
            "podTemplate": {"spec": pod("smoke")},
            "job": {
                "parallelism": parallelism,
                "args": [
                    f"--run-id={run_id}",
                    f"--phase={phase}",
                    f"--records-per-subscription={options['pubsub_records']}",
                    f"--parallelism={parallelism}",
                    f"--require-restored={str(phase == 'upgrade').lower()}",
                    f"--entry-point={options['pubsub_entry_point']}",
                ],
            },
        }
        for manager, replicas in (("jobManager", 1), ("taskManager", parallelism)):
            spec[manager] = {
                "replicas": replicas,
                "resource": {"cpu": 1, "memory": "2Gi"},
                "podTemplate": {"spec": pod("smoke")},
            }
        return {
            "apiVersion": "flink.apache.org/v1beta1",
            "kind": "FlinkDeployment",
            "metadata": {
                "name": run_id,
                "namespace": "tier3-pubsub",
                "annotations": {
                    "flink-gcp.io/approval": nonce,
                    "flink-gcp.io/expires-at": expiry,
                },
            },
            "spec": spec,
        }

    def render(run_id, nonce, expiry, seconds, **options):
        calls.append((run_id, nonce, expiry, seconds, options))
        kind = options["pubsub_trial"]
        initial = application(
            run_id, nonce, expiry, options, 1 if kind == "rescale-out" else 2, "initial"
        )
        recovery = application(
            run_id,
            nonce,
            expiry,
            options,
            1 if kind == "rescale-in" else 2,
            "upgrade" if kind.startswith("rescale-") else "initial",
        )
        supervisor = pod("supervisor")
        supervisor["containers"][0]["image"] = (
            plan.GAR + "lifecycle-tools@sha256:" + "9" * 64
        )
        delivery = {
            "config": {
                "data": {
                    "approval.json": "{}",
                    "application.json": json.dumps(initial),
                    "upgrade-application.json": json.dumps(recovery),
                }
            },
            "supervisor": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "activeDeadlineSeconds": seconds,
                    "template": {"spec": supervisor},
                }
            },
        }
        probe = pod("supervisor")
        # Placed as the supervisor is: the rendered Pod excludes Spot.
        probe["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-spot",
                                    "operator": "NotIn",
                                    "values": ["true"],
                                }
                            ]
                        }
                    ]
                }
            }
        }
        probe["containers"][0].update(
            image=supervisor["containers"][0]["image"],
            command=["python3", "-I", "-c", options["pubsub_probe_source"]],
            args=[options["pubsub_probe"]],
        )
        delivery["probe"] = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": run_id + "-access-probe",
                "namespace": "tier3-pubsub",
                "annotations": {"flink-gcp.io/approval": nonce},
            },
            "spec": {**probe, "serviceAccountName": "pubsub", "restartPolicy": "Never"},
        }
        return copy.deepcopy(initial), copy.deepcopy(recovery), delivery

    monkeypatch.setattr(plan, "render", render)
    return calls


@pytest.mark.parametrize("kind", plan.TRIALS)
def test_the_fake_render_satisfies_the_proposal_checks(inputs, renderer, kind):
    """The dispatch and bundle tests stand on it, so it must pass as the CUE does."""
    inputs["trial"]["trial"] = kind
    proposal = plan.prepare(**inputs)["proposal"]
    assert proposal["approved"] is False
    assert renderer[0][4]["pubsub_trial"] == kind


@pytest.mark.parametrize(
    "kind, initial, recovery",
    [
        ("jm-replacement", (2, "initial"), (2, "initial")),
        ("tm-replacement", (2, "initial"), (2, "initial")),
        ("rescale-out", (1, "initial"), (2, "upgrade")),
        ("rescale-in", (2, "initial"), (1, "upgrade")),
    ],
)
def test_the_trial_fixes_each_manifests_job(trial, kind, initial, recovery):
    trial.update(trial=kind, entry_point="table", records_per_subscription=40)
    jobs = plan.manifest_jobs("run-1430", trial)
    assert jobs == tuple(
        (
            parallelism,
            [
                "--run-id=run-1430",
                f"--phase={phase}",
                "--records-per-subscription=40",
                f"--parallelism={parallelism}",
                f"--require-restored={str(phase == 'upgrade').lower()}",
                "--entry-point=table",
            ],
        )
        for parallelism, phase in (initial, recovery)
    )


@pytest.mark.parametrize("kind", plan.TRIALS)
def test_rendered_manifests_run_the_trial_they_were_rendered_for(
    inputs, renderer, kind
):
    inputs["trial"]["trial"] = kind
    rendered = plan.prepare(**inputs)
    plan.require_trial_jobs(
        inputs["run_id"],
        inputs["trial"],
        rendered["application"],
        rendered["recovery_application"],
    )


def _tamper(job, change):
    if change == "parallelism":
        job["parallelism"] = 3 - job["parallelism"]
    elif change == "parallelism-bool":
        job["parallelism"] = job["parallelism"] == 1
    elif change == "dropped":
        job["args"].pop()
    elif change == "extra":
        job["args"].append("--phase=upgrade")
    elif change == "reordered":
        job["args"].reverse()
    elif change == "missing":
        del job["args"]
    else:
        index = next(i for i, a in enumerate(job["args"]) if a.startswith(change))
        flag, value = job["args"][index].split("=", 1)
        flipped = {
            "--run-id": lambda: "other-run",
            "--phase": lambda: "initial" if value == "upgrade" else "upgrade",
            "--records-per-subscription": lambda: "999",
            "--parallelism": lambda: str(3 - int(value)),
            "--require-restored": lambda: "false" if value == "true" else "true",
            "--entry-point": lambda: "table",
        }[flag]()
        job["args"][index] = flag + "=" + flipped


@pytest.mark.parametrize("which", ["application", "recovery_application"])
@pytest.mark.parametrize(
    "change",
    [
        "--run-id",
        "--phase",
        "--records-per-subscription",
        "--parallelism",
        "--require-restored",
        "--entry-point",
        "parallelism",
        "parallelism-bool",
        "dropped",
        "extra",
        "reordered",
        "missing",
    ],
)
def test_pinned_manifests_must_run_exactly_the_trial(inputs, renderer, which, change):
    """A digest pins a manifest; only this says the manifest runs the trial."""
    rendered = plan.prepare(**inputs)
    _tamper(rendered[which]["spec"]["job"], change)
    with pytest.raises(Failure, match="differ from the approved trial"):
        plan.require_trial_jobs(
            inputs["run_id"],
            inputs["trial"],
            rendered["application"],
            rendered["recovery_application"],
        )


@pytest.mark.parametrize("manifest", [None, [], {}, {"spec": []}, {"spec": {}}])
def test_malformed_manifests_are_refused_rather_than_raising(inputs, manifest):
    with pytest.raises(Failure, match="differ from the approved trial"):
        plan.require_trial_jobs(inputs["run_id"], inputs["trial"], manifest, manifest)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("version", 1),
        ("version", 2),
        ("trial", "combined"),
        ("entry_point", "sql"),
        ("entry_point", "Table"),
        ("entry_point", None),
        ("trial", None),
        ("records_per_subscription", 1),
        ("records_per_subscription", 2),
        ("records_per_subscription", 10001),
        ("records_per_subscription", 2.0),
        ("records_per_subscription", True),
        ("traffic_limits", {}),
        ("traffic_limits", []),
        ("total_request_limit", 29999),
        ("total_request_limit", 100001),
        ("total_request_limit", True),
        # Spend is approved from the estimate before dispatch, not proposed here.
        ("additional_cost_usd", "10.00"),
    ],
)
def test_invalid_trial_refused_before_render(inputs, field, value):
    inputs["trial"][field] = value
    with pytest.raises(Failure):
        plan.prepare(**inputs)


@pytest.mark.parametrize("counter", plan.COUNTER_CEILINGS)
@pytest.mark.parametrize("value", [True, 0, -1, 1000000000])
def test_invalid_counter_refused(inputs, counter, value):
    inputs["trial"]["traffic_limits"][counter] = value
    with pytest.raises(Failure, match="traffic limit"):
        plan.prepare(**inputs)


@pytest.mark.parametrize(
    "counter,value",
    [
        ("input_messages", 1999),
        ("input_bytes", 1),
        # Three cohorts of 333, 333 and 334: four batches each per input, and
        # seven pulls each for both inputs' output, beside the exercise's one
        # pull per poll for 2,700 seconds, 180; each pull reserves a whole
        # batch of 100, and a nonempty one also acknowledges.
        ("publish_calls", 23),
        ("output_messages", 20099),
        ("pull_calls", 200),
        ("pubsub_requests", 245),
    ],
)
def test_incomplete_pass_cannot_be_proposed(inputs, counter, value):
    inputs["trial"]["traffic_limits"][counter] = value
    with pytest.raises(Failure, match="complete input/output pass"):
        plan.prepare(**inputs)


def test_the_minimum_complete_pass_is_accepted(trial):
    trial["traffic_limits"].update(
        publish_calls=24, pull_calls=201, pubsub_requests=246, output_messages=20100
    )
    planned = plan.input_plan("proposal-1361", trial)
    assert planned["publish_calls"] == 24


def test_each_cohort_needs_a_separate_output_pull(inputs):
    inputs["trial"]["records_per_subscription"] = 3
    inputs["trial"]["traffic_limits"]["pull_calls"] = 2
    with pytest.raises(Failure, match="complete input/output pass"):
        plan.prepare(**inputs)


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../foreign"),
        ("nonce", "a" * 31),
        ("revision", "main"),
        ("application_image", plan.GAR + "pubsub-recovery:latest"),
        ("application_image", plan.GAR + "smoke@sha256:" + "c" * 64),
        ("expires_at", "2026-09-21T01:00:01Z"),
        ("started_at", "2026-09-21T00:00:00.500Z"),
        ("active_seconds", 3420.0),
        ("active_seconds", 3600),
    ],
)
def test_invalid_identity_or_window_refused(inputs, field, value):
    inputs[field] = value
    with pytest.raises(Failure):
        plan.prepare(**inputs)


@pytest.mark.parametrize(
    "text",
    [
        "{}",
        "[]",
        "{",
    ],
)
def test_strict_file_input(tmp_path, text):
    path = tmp_path / "trial.json"
    path.write_text(text)
    with pytest.raises(Failure):
        plan.load_trial(path)


def test_duplicate_keys_and_size_guard_on_otherwise_valid_input(trial, tmp_path):
    path = tmp_path / "trial.json"
    valid = json.dumps(trial)
    # The exact limit remains readable, so a rejected file is not malformed JSON.
    path.write_text(valid.ljust(8192))
    assert plan.load_trial(path) == trial
    for text, reason in (
        ('{"version": 1, ' + valid[1:], "Duplicate trial field: version"),
        (
            valid.replace('"pull_calls":', '"pull_calls": 1, "pull_calls":'),
            "Duplicate trial field: pull_calls",
        ),
        (valid.ljust(8193), "Trial input exceeds 8 KiB"),
    ):
        path.write_text(text)
        with pytest.raises(Failure, match=reason):
            plan.load_trial(path)


def test_missing_unknown_fields_and_file(trial, tmp_path):
    with pytest.raises(Failure):
        plan.load_trial(tmp_path / "missing.json")
    for candidate in (
        {**trial, "approved": True},
        {k: v for k, v in trial.items() if k != "version"},
        {k: v for k, v in trial.items() if k != "entry_point"},
    ):
        with pytest.raises(Failure):
            plan.validate_trial(candidate)
    with pytest.raises(Failure):
        validate_approval({"scenario": "pubsub-recovery", "version": 4})


def test_nested_json_is_a_trial_error(tmp_path, monkeypatch):
    path = tmp_path / "trial.json"
    path.write_text("[" * 4000 + "]" * 4000)
    # Python versions differ in the C decoder's nesting limit.
    with pytest.raises(Failure):
        plan.load_trial(path)

    def recursion_limit(*args, **kwargs):
        raise RecursionError("decoder nesting limit")

    monkeypatch.setattr(plan.json, "loads", recursion_limit)
    with pytest.raises(
        Failure, match="Invalid Pub/Sub trial proposal: decoder nesting limit"
    ):
        plan.load_trial(path)


def test_cli_routes_pubsub_without_authentication(trial, tmp_path, monkeypatch, capsys):
    path = tmp_path / "trial.json"
    path.write_text(json.dumps(trial))
    calls = []

    def prepare(**kwargs):
        calls.append(kwargs)
        return {"proposal": {"approved": False}}

    monkeypatch.setattr(plan, "prepare", prepare)
    arguments = [
        "--scenario",
        "pubsub-recovery",
        "--run-id",
        "example",
        "--nonce",
        "a" * 32,
        "--started-at",
        "2026-09-21T00:00:00Z",
        "--expires-at",
        "2026-09-21T01:00:00Z",
        "--active-seconds",
        "3420",
        "--revision",
        "b" * 40,
        "--application-image",
        "synthetic",
        "--trial-file",
        str(path),
    ]
    command.main(arguments)
    assert calls[0]["trial"] == trial
    assert json.loads(capsys.readouterr().out)["proposal"]["approved"] is False
    for extra in (
        ["--expression", "application"],
        ["--flink-version", "1.20.4"],
        ["--cells-file", "cells.toml"],
    ):
        with pytest.raises(SystemExit):
            command.main(arguments + extra)
    assert len(calls) == 1


REVIEWED = Path(__file__).parents[4] / lifecycle.PUBSUB_TRIALS
# The tests' example trial, kept where no dispatch can name it.
FIXTURE_TRIALS = Path(__file__).parent.parent / "fixtures/pubsub-trials"


def test_every_reviewed_trial_file_is_a_valid_trial():
    """A dispatch names one of these; a broken one would refuse only on the day."""
    from flink_tier3.policy import RUN_ID

    if not REVIEWED.exists():
        return
    for path in sorted(REVIEWED.iterdir()):
        assert path.suffix == ".toml", path
        assert RUN_ID.fullmatch(path.stem), path
        # The one-pass feasibility check dispatch reaches only after the cluster.
        plan.input_plan("feasibility", plan.load_reviewed_trial(path))


def test_no_reviewed_trial_is_the_tests_example():
    """Admission would run any file there, and the example authorizes nothing."""
    assert not (REVIEWED / "example-wiring.toml").exists()
    example = plan.load_reviewed_trial(FIXTURE_TRIALS / "example-wiring.toml")
    for path in REVIEWED.glob("*.toml"):
        assert plan.load_reviewed_trial(path) != example, path


def test_the_example_trial_is_the_one_the_tests_use(trial):
    assert plan.load_reviewed_trial(FIXTURE_TRIALS / "example-wiring.toml") == trial


@pytest.mark.parametrize(
    "text",
    [
        "",
        "version = 3\nversion = 3\n",
        "version = ",
        # JSON is the offline renderer's format, not a reviewed file's.
        '{"version": 3}',
    ],
)
def test_a_reviewed_trial_file_that_does_not_parse_or_validate_is_refused(
    tmp_path, text
):
    path = tmp_path / "trial.toml"
    path.write_text(text)
    with pytest.raises(Failure):
        plan.load_reviewed_trial(path)


def test_a_missing_reviewed_trial_file_is_refused(tmp_path):
    with pytest.raises(Failure, match="Unreadable Pub/Sub trial file"):
        plan.load_reviewed_trial(tmp_path / "missing.toml")


def test_a_reviewed_trial_file_that_is_not_utf8_is_refused(tmp_path):
    path = tmp_path / "trial.toml"
    path.write_bytes(b"# caf\xe9\nversion = 3\n")
    with pytest.raises(Failure, match="Unreadable Pub/Sub trial file"):
        plan.load_reviewed_trial(path)
