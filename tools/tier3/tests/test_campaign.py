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
"""Which attempt a campaign runs next, from the finished attempt's receipt."""

import pytest
from flink_tier3 import lifecycle as cli
from flink_tier3.campaign import next_attempt, parse
from flink_tier3.common import Failure
from flink_tier3.exercise import INTERRUPTED, REPLACED

from .test_lifecycle import env as env  # noqa: PLC0414
from .test_lifecycle import lifecycle, seed_record

USABLE = {"success": True, "application_created": True, "reason": "finished"}


def stopped(**changes):
    return {"success": False, "application_created": True, **changes}


def test_a_usable_attempt_moves_to_the_next_kind_and_the_last_ends_it():
    following = next_attempt(
        USABLE, "bt-tm-a3", "tm-replacement", "rescale-out,rescale-in", "2"
    )
    assert following == {
        "run_id": "bt-ro-a1",
        "kind": "rescale-out",
        "campaign": "rescale-in",
        "repeats": "2",
    }
    assert next_attempt(USABLE, "bt-ri-a1", "rescale-in", "", "2") is None


@pytest.mark.parametrize(
    "receipt",
    [
        # Nothing ran: a supervisor preempted, or a create that never answered.
        stopped(application_created=False, reason="external settlement"),
        stopped(reason=INTERRUPTED),
    ],
)
def test_a_platform_stop_repeats_the_kind_while_repeats_remain(receipt):
    following = next_attempt(receipt, "bt-tm-a1", "tm-replacement", "rescale-out", "1")
    assert following == {
        "run_id": "bt-tm-a2",
        "kind": "tm-replacement",
        "campaign": "rescale-out",
        "repeats": "0",
    }
    assert (
        next_attempt(receipt, "bt-tm-a2", "tm-replacement", "rescale-out", "0") is None
    )


@pytest.mark.parametrize(
    "reason",
    [
        # A TaskManager whose JVM died is replaced, so a replacement may be
        # the connector's; so may a restart or an eviction under its limits.
        REPLACED,
        "Unplanned container restart; trial is inconclusive",
        "Unplanned workload eviction; trial is inconclusive",
        "recovery exercise finished",
    ],
)
def test_any_other_result_stops_the_campaign(reason):
    receipt = stopped(reason=reason)
    assert (
        next_attempt(receipt, "bt-tm-a1", "tm-replacement", "rescale-out", "3") is None
    )


@pytest.mark.parametrize(
    ("run_id", "kind", "campaign", "repeats"),
    [
        ("bt-tm-a1", "rescale-in", "", "1"),
        ("bt1855", "tm-replacement", "rescale-in", "0"),
        ("bt-tm-a1", "tm-replacement", "rescale-sideways", "0"),
        ("bt-tm-a1", "tm-replacement", "", "10"),
    ],
)
def test_a_campaign_that_could_not_name_its_attempts_is_refused(
    run_id, kind, campaign, repeats
):
    with pytest.raises(Failure):
        parse(run_id, kind, campaign, repeats)


def test_the_receipt_says_whether_the_application_was_created_and_why_it_stopped(env):
    runner = lifecycle(env, cli.runner_api.Runner)
    plans = {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    # The intent outlives a create that admission's last check stopped.
    seed_record(env, phase="cleaned", application_intent=True, reason=INTERRUPTED)
    receipt = runner.receipt(runner.env.refresh(), plans)
    assert (receipt["application_created"], receipt["reason"]) == (False, INTERRUPTED)
    seed_record(env, roots={"application": {"kind": "FlinkDeployment", "uid": "u"}})
    assert runner.receipt(runner.env.refresh(), plans)["application_created"] is True


def test_next_prints_the_attempt_as_workflow_outputs(env, capsys, monkeypatch):
    store = env[1]
    store.write(
        "runs/bt-tm-a1/result.json",
        {
            "scenario": "bigtable-recovery",
            "bigtable_trial": {"kind": "tm-replacement"},
            **stopped(application_created=False),
        },
    )
    monkeypatch.setattr(cli.rt, "Storage", lambda: store)
    cli.main(
        ["next", "--run-id", "bt-tm-a1", "--campaign", "rescale-in", "--repeats", "1"]
    )
    assert capsys.readouterr().out.splitlines() == [
        "run_id=bt-tm-a2",
        "kind=tm-replacement",
        "campaign=rescale-in",
        "repeats=0",
        "trial_input=bigtable_trial",
    ]
