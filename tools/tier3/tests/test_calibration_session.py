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
"""The reviewed calibration sessions match the numbers the preregistration states."""

from decimal import Decimal
from pathlib import Path

import flink_tier3 as rt
from flink_tier3 import protocol
from flink_tier3.supervisor import HookChain, SessionHooks

ROOT = Path(__file__).resolve().parents[3]
SESSIONS = ROOT / "kubernetes/lifecycle/sessions"
PREREGISTRATION = ROOT / "docs/adr/evidence/0162-cloudtasks-assessment-1246.md"


def test_calibration_session_matches_the_preregistered_numbers():
    session = rt.load_session(SESSIONS / "calibration-1246.toml")
    assert session["campaign"] == "calibration-1246"
    plan = rt.validate_cells(session["cells"], manifest=False)
    assert plan == {
        "cells": 10,
        "task_creations": 11539316,
        "records": 1923195,
        "plan_seconds": 11018,
        "largest_parallelism": 16,
    }
    window = plan["plan_seconds"] + rt.CLOUDTASKS_CEILINGS["cleanup_seconds"]
    assert window == 11918
    cost = rt.estimated_session_cost(window, session["cells"])
    assert cost.quantize(Decimal("0.01")) == Decimal("8.31")
    assert cost <= Decimal(rt.CLOUDTASKS_CEILINGS["additional_cost_usd"])
    ids = [cell["id"] for cell in session["cells"]]
    assert ids[0] == "k01-pace-10" and ids[-1] == "interrupt-control-k11"
    delay = next(c for c in session["cells"] if c["id"] == "k03-delay-control")
    assert delay["control_delay_millis"] == 100 and delay["offered_rate"] == 20
    counts_only = next(c for c in session["cells"] if c["id"] == "k07-counts-only")
    assert counts_only["emit_attempts"] is False
    for cell in session["cells"]:
        warm, observation = protocol.window(cell["checkpoint_seconds"])
        assert (cell["warmup_seconds"], cell["observation_seconds"]) == (
            warm,
            observation,
        )
    text = PREREGISTRATION.read_text()
    for figure in ("11,539,316", "1,923,195", "11,018", "11,918", "USD 8.31"):
        assert figure in text, figure


def test_flink120_calibration_repeat_matches_the_preregistration():
    session = rt.load_session(SESSIONS / "calibration-1246-flink120.toml")
    assert session["campaign"] == "calibration-1246-flink120"
    plan = rt.validate_cells(session["cells"], manifest=False)
    assert plan["plan_seconds"] == 4110 and plan["task_creations"] == 236800
    cost = rt.estimated_session_cost(plan["plan_seconds"] + 900, session["cells"])
    assert cost.quantize(Decimal("0.01")) == Decimal("1.29")
    text = PREREGISTRATION.read_text()
    assert "4,110 s" in text and "236,800" in text and "USD 1.29" in text


def test_hook_chain_runs_every_hook_in_order_and_stops_on_failure():
    calls = []

    class Recording(SessionHooks):
        def __init__(self, name, fail=False):
            self.name, self.fail = name, fail

        def poll(self, session, cell, app, pods):
            calls.append((self.name, "poll"))

        def after_cell(self, session, cell, outcome):
            calls.append((self.name, "after_cell", outcome))
            if self.fail:
                raise rt.Failure(self.name + " failed")

        def at_session_end(self, session, outcomes):
            calls.append((self.name, "end"))

    chain = HookChain(Recording("observer"), Recording("collector", fail=True))
    chain.poll(None, {"id": "c"}, None, [])
    chain.at_session_end(None, {})
    try:
        chain.after_cell(None, {"id": "c"}, "completed")
    except rt.Failure as error:
        assert "collector failed" in str(error)
    else:
        raise AssertionError("the failing hook must stop the chain")
    assert calls == [
        ("observer", "poll"),
        ("collector", "poll"),
        ("observer", "end"),
        ("collector", "end"),
        ("observer", "after_cell", "completed"),
        ("collector", "after_cell", "completed"),
    ]
