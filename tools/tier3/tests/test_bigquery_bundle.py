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
"""Approval-bound delivery generation and tamper checks without admission."""

import copy
import json
import subprocess

import pytest
from flink_tier3 import bigquery_bundle as bundles
from flink_tier3 import bigquery_plan as plan
from flink_tier3 import cli
from flink_tier3.bundle import delivery_digest, source_digest
from flink_tier3.common import Failure, json_bytes
from flink_tier3.model import Approval
from flink_tier3.policy import BIGQUERY, BIGQUERY_CEILINGS, SMOKE
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414


@pytest.fixture
def approval(env, inputs, renderer, monkeypatch):
    monkeypatch.setattr(plan, "source_digest", source_digest)
    monkeypatch.setattr(bundles, "_check_revision", lambda revision: None)
    value = copy.deepcopy(env[2])
    inputs.update(run_id=value["run_id"], nonce=value["nonce"])
    proposed = plan.prepare(**inputs)["proposal"]
    value.update(
        version=4,
        scenario="bigquery-recovery",
        started_at=proposed["started_at"],
        expires_at=proposed["expires_at"],
        cleanup_at=proposed["cleanup_at"],
        runtime_sha256=source_digest(),
        delivery_sha256=delivery_digest(),
        application_sha256=proposed["application_sha256"],
        upgrade_application_sha256=proposed["upgrade_application_sha256"],
        ceilings={
            **BIGQUERY_CEILINGS,
            "additional_cost_usd": inputs["trial"]["additional_cost_usd"],
        },
        bigquery_trial=copy.deepcopy(inputs["trial"]),
    )
    value["namespaces"][BIGQUERY] = value["namespaces"].pop(SMOKE)
    value["images"].pop("smoke")
    value["images"].update(proposed["images"])
    return value


@pytest.mark.parametrize(
    "prepared_at,seconds",
    [("2026-09-21T00:00:00Z", 5220), ("2026-09-21T00:10:00Z", 4620)],
)
def test_delivery_binds_approval_and_shortens_job_budget(
    approval, prepared_at, seconds
):
    before = copy.deepcopy(approval)
    value = bundles.prepare(approval, prepared_at=prepared_at)
    assert value["admission_enabled"] is False
    assert value["proposal"]["approved"] is False
    data = value["delivery"]["config"]["data"]
    assert json.loads(data["approval.json"]) == Approval.from_dict(approval).to_dict()
    assert json.loads(data["application.json"]) == value["application"]
    assert json.loads(data["upgrade-application.json"]) == value["upgrade_application"]
    assert value["delivery"]["supervisor"]["spec"]["activeDeadlineSeconds"] == seconds
    assert bundles.validate(value, approval) == value
    assert approval == before


@pytest.mark.parametrize(
    "key",
    [
        "runtime_sha256",
        "delivery_sha256",
        "application_sha256",
        "upgrade_application_sha256",
        "supervisor",
    ],
)
def test_mismatched_approval_refuses_delivery(approval, key):
    if key == "supervisor":
        approval["images"][key] = approval["images"][key][:-64] + "0" * 64
    else:
        approval[key] = "0" * 64
    with pytest.raises(Failure, match="differs"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


@pytest.mark.parametrize(
    "at",
    [
        "2026-09-20T23:59:59Z",
        "2026-09-21T00:00:00.500Z",
        "2026-09-21T01:15:00Z",
        "2026-09-21T01:30:00Z",
    ],
)
def test_preparation_stays_in_the_approved_window(approval, at):
    with pytest.raises(Failure):
        bundles.prepare(approval, prepared_at=at)


@pytest.mark.parametrize(
    "change",
    [
        "approval",
        "source",
        "command",
        "deadline",
        "application",
        "upgrade",
        "proposal",
        "extra",
        "enabled",
    ],
)
def test_verification_uses_external_approval_and_complete_rendering(approval, change):
    value = bundles.prepare(approval, prepared_at=approval["started_at"])
    if change == "approval":
        value["approval"]["bigquery_trial"]["repetition"] = 2
        value["delivery"]["config"]["data"]["approval.json"] = json.dumps(
            value["approval"]
        )
    elif change == "source":
        value["delivery"]["config"]["data"]["flink_tier3_runtime.py"] = "foreign source"
    elif change == "command":
        value["delivery"]["supervisor"]["spec"]["template"]["spec"]["containers"][0][
            "command"
        ] = ["foreign"]
    elif change == "deadline":
        value["delivery"]["supervisor"]["spec"]["activeDeadlineSeconds"] += 1
    elif change in ("application", "upgrade"):
        value["application" if change == "application" else "upgrade_application"][
            "spec"
        ]["job"]["args"][-1] = "foreign"
    elif change == "proposal":
        value["proposal"]["resources"]["query_slots"] = 1
    elif change == "extra":
        value["delivery"]["foreign"] = {"kind": "Job"}
    else:
        value["admission_enabled"] = True
    with pytest.raises(Failure, match="Bundle differs"):
        bundles.validate(value, approval)


def test_other_approval_and_malformed_bundle_are_rejected(env):
    with pytest.raises(Failure, match="BigQuery"):
        bundles.prepare(env[2], prepared_at=env[2]["started_at"])
    for value in (None, [], {}):
        with pytest.raises(Failure):
            bundles.validate(value, env[2])
    with pytest.raises(Failure, match="JSON object"):
        bundles.prepare([], prepared_at=env[2]["started_at"])


def test_configmap_size_counts_utf8_bytes(approval, monkeypatch):
    original = plan.render

    def oversized(*args, **kwargs):
        initial, upgrade, delivery = original(*args, **kwargs)
        delivery["config"]["data"]["oversized"] = "é" * (512 * 1024)
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", oversized)
    with pytest.raises(Failure, match="ConfigMap exceeds"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


def test_configmap_size_includes_embedded_approval(approval, monkeypatch):
    original = plan.prepare

    def at_limit(*args, **kwargs):
        proposed = original(*args, **kwargs)
        data = proposed["delivery"]["config"]["data"]
        assert json.loads(data["approval.json"]) == {}
        data["padding"] = ""
        used = sum(
            len(key.encode()) + len(value.encode()) for key, value in data.items()
        )
        data["padding"] = "x" * (1024**2 - used)
        assert (
            sum(len(k.encode()) + len(v.encode()) for k, v in data.items()) == 1024**2
        )
        return proposed

    monkeypatch.setattr(plan, "prepare", at_limit)
    with pytest.raises(Failure, match="ConfigMap exceeds"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


def test_cli_round_trip_and_refusal(approval, tmp_path, capsys):
    approved = tmp_path / "approval.json"
    output = tmp_path / "bundle.json"
    approved.write_bytes(json_bytes(approval))
    cli.main(
        [
            "bigquery-bundle",
            "prepare",
            "--approval-file",
            str(approved),
            "--prepared-at",
            approval["started_at"],
        ]
    )
    output.write_text(capsys.readouterr().out)
    cli.main(
        [
            "bigquery-bundle",
            "verify",
            "--approval-file",
            str(approved),
            "--bundle-file",
            str(output),
        ]
    )
    assert json.loads(capsys.readouterr().out) == {
        "run_id": approval["run_id"],
        "matches_approval": True,
        "admission_enabled": False,
    }
    value = json.loads(output.read_text())
    value["approval"]["actor"] = "foreign"
    output.write_text(json.dumps(value))
    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "bigquery-bundle",
                "verify",
                "--approval-file",
                str(approved),
                "--bundle-file",
                str(output),
            ]
        )
    assert error.value.code == 2
    assert not capsys.readouterr().out


@pytest.mark.parametrize(
    "change",
    ["none", "revision", "tracked", "untracked", "ignored", "unicode", "environment"],
)
def test_approved_checkout_and_cue_inputs_are_required(tmp_path, monkeypatch, change):
    def git(*args):
        return subprocess.run(
            [
                "git",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=/dev/null",
                *args,
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    kubernetes = tmp_path / "kubernetes"
    kubernetes.mkdir()
    (kubernetes / "common.cue").write_text("package test\n")
    git("add", "kubernetes")
    git(
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "Fixture",
    )
    revision = git("rev-parse", "HEAD")
    monkeypatch.setattr(bundles.workflow, "ROOT", tmp_path)
    if change == "environment":
        monkeypatch.setenv("GIT_DIR", str(tmp_path / ".git"))
        monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path))
        monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / ".git/index"))
        monkeypatch.setattr(bundles.workflow, "ROOT", tmp_path.parent)
    if change == "revision":
        revision = "0" * 40
    elif change == "tracked":
        (kubernetes / "common.cue").write_text("package changed\n")
    elif change in ("untracked", "ignored", "unicode"):
        (kubernetes / ("追加.cue" if change == "unicode" else "extra.cue")).write_text(
            "package test\n"
        )
        if change == "ignored":
            (tmp_path / ".git/info/exclude").write_text("extra.cue\n")
    if change == "none":
        bundles._check_revision(revision)
    else:
        with pytest.raises(Failure):
            bundles._check_revision(revision)


@pytest.mark.parametrize("failure", ["missing", "timeout", "exit"])
def test_git_failures_are_bounded_and_reported(monkeypatch, failure):
    def run(args, **kwargs):
        assert 0 < kwargs.get("timeout", float("inf")) <= 60
        if failure == "missing":
            raise FileNotFoundError("git is missing")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return subprocess.CompletedProcess(args, 1, "", "not a repository")

    monkeypatch.setattr(bundles.subprocess, "run", run)
    with pytest.raises(Failure, match="Cannot verify the approved repository revision"):
        bundles._check_revision("b" * 40)


def test_proposal_window_does_not_bypass_common_pricing_freshness(approval):
    approval.update(
        started_at="2026-10-15T00:00:00Z",
        cleanup_at="2026-10-15T01:15:00Z",
        expires_at="2026-10-15T01:30:00Z",
    )
    Approval.from_dict(approval)
    with pytest.raises(Failure, match="Pricing review is older than 30 days"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


@pytest.mark.parametrize("duplicate", ["approval", "bundle"])
def test_cli_refuses_duplicate_json_fields(approval, tmp_path, capsys, duplicate):
    approved, artifact = tmp_path / "approval.json", tmp_path / "bundle.json"
    approved.write_text(json.dumps(approval))
    artifact.write_text("{}")
    (approved if duplicate == "approval" else artifact).write_text('{"a": 1, "a": 2}')
    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "bigquery-bundle",
                "verify",
                "--approval-file",
                str(approved),
                "--bundle-file",
                str(artifact),
            ]
        )
    assert error.value.code == 2
    output = capsys.readouterr()
    assert "Duplicate JSON field" in output.err
    assert not output.out


def test_an_internally_consistent_bundle_cannot_supply_its_own_approval(approval):
    other = copy.deepcopy(approval)
    other["bigquery_trial"]["repetition"] = 2
    value = bundles.prepare(other, prepared_at=other["started_at"])
    assert bundles.validate(value, other) == value
    with pytest.raises(Failure, match="separately supplied approval"):
        bundles.validate(value, approval)


@pytest.mark.parametrize("changed_after_render", [False, True])
def test_checkout_drift_refuses_preparation(
    approval, monkeypatch, changed_after_render
):
    checks = []

    def check(revision):
        assert revision == approval["sha"]
        checks.append(revision)
        if len(checks) > int(changed_after_render):
            raise Failure("Checkout changed")

    monkeypatch.setattr(bundles, "_check_revision", check)
    with pytest.raises(Failure, match="Checkout changed"):
        bundles.prepare(approval, prepared_at=approval["started_at"])
