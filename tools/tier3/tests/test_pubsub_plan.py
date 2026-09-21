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

import json

import pytest
from flink_tier3 import pubsub_plan as plan
from flink_tier3 import render as command
from flink_tier3.common import Failure
from flink_tier3.model import validate_approval


@pytest.fixture
def trial():
    return {
        "version": 1,
        "trial": "rescale-out",
        "records_per_subscription": 1000,
        "traffic_limits": dict(plan.COUNTER_CEILINGS),
        "total_request_limit": 100000,
        "additional_cost_usd": "10.00",
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


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("version", 2),
        ("trial", "combined"),
        ("trial", None),
        ("records_per_subscription", 1),
        ("records_per_subscription", 10001),
        ("records_per_subscription", 2.0),
        ("records_per_subscription", True),
        ("traffic_limits", {}),
        ("traffic_limits", []),
        ("total_request_limit", 29999),
        ("total_request_limit", 100001),
        ("total_request_limit", True),
        ("additional_cost_usd", "0.99"),
        ("additional_cost_usd", "10.01"),
        ("additional_cost_usd", 10),
        ("additional_cost_usd", "NaN"),
        ("additional_cost_usd", "5"),
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
        ("publish_calls", 19),
        ("output_messages", 1999),
        ("pull_calls", 19),
        ("pubsub_requests", 59),
    ],
)
def test_incomplete_pass_cannot_be_proposed(inputs, counter, value):
    inputs["trial"]["traffic_limits"][counter] = value
    with pytest.raises(Failure, match="complete input/output pass"):
        plan.prepare(**inputs)


def test_each_cohort_needs_a_separate_output_pull(inputs):
    inputs["trial"]["records_per_subscription"] = 2
    inputs["trial"]["traffic_limits"]["pull_calls"] = 1
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
