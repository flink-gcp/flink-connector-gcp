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
"""Hold the proposal's request floor to what a synthetic run sends."""

import math

import pytest
from flink_tier3.kubernetes import INVENTORY
from flink_tier3.policy import PUBSUB, inventory_namespaces
from flink_tier3.pubsub import plan

from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_actors import prepared as prepared  # noqa: PLC0414
from .test_admission import admitting as admitting  # noqa: PLC0414
from .test_bundle import approval as approval  # noqa: PLC0414
from .test_exercise import World
from .test_exercise import trial as trial  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414


class Wire:
    """Each fake operation as the requests its real transport sends.

    A stored document is read with a metadata request and a generation-matched
    download, or one request when absent; every other storage operation, and
    every Kubernetes and Pub/Sub call, is one request; a listing is one per
    page of a thousand; and the inventory lists every kind in every namespace
    the Pub/Sub scenario inventories. Retries are not counted, and neither is
    an operation a fake makes inside another, which its transport would not.
    """

    def __init__(self, a, monkeypatch):
        self.phase = "admission"
        self.counts = {}
        self.depth = 0
        kube, store = a.environment.kube, a.environment.store
        inventory = len(inventory_namespaces(PUBSUB)) * len(INVENTORY)

        def wrap(target, name, cost):
            original = getattr(target, name)

            def counted(*args, **kwargs):
                self.depth += 1
                try:
                    result = original(*args, **kwargs)
                finally:
                    self.depth -= 1
                if not self.depth:
                    self.counts[self.phase] = self.counts.get(self.phase, 0) + cost(
                        result
                    )
                return result

            monkeypatch.setattr(target, name, counted)

        for name in ("namespace", "get", "items", "patch", "delete", "create"):
            wrap(kube, name, lambda _: 1)
        for name in ("request", "logs"):
            wrap(kube, name, lambda _: 1)
        wrap(kube, "inventory", lambda _: inventory)
        wrap(store, "read", lambda result: 1 if result[0] is None else 2)
        for name in ("write", "write_bytes", "delete", "metadata", "rewrite"):
            wrap(store, name, lambda _: 1)
        for name in ("objects", "prefixes"):
            wrap(store, name, lambda result: max(1, math.ceil(len(result) / 1000)))
        wrap(a.service, "request", lambda _: 1)

    def labelled(self, monkeypatch, target, name, phase, calls):
        original = getattr(target, name)

        def call(*args, **kwargs):
            previous, self.phase = self.phase, phase
            before = self.counts.get(phase, 0)
            try:
                return original(*args, **kwargs)
            finally:
                calls.append(self.counts.get(phase, 0) - before)
                self.phase = previous

        monkeypatch.setattr(target, name, call)


def measure(a, monkeypatch):
    wire = Wire(a, monkeypatch)
    publishes, pulls, relay, polls = [], [], [], []
    wire.labelled(monkeypatch, a.sender, "publish", "publish", publishes)
    wire.labelled(monkeypatch, a.receiver, "collect", "collect", pulls)
    world = World(a, monkeypatch)
    admission = wire.counts.get("admission", 0)
    # The simulated application and Operator are not the rig's requests.
    wire.labelled(monkeypatch, world.relay, "advance", "relay", relay)
    # One poll of the runner's settlement, as its wait runs it.
    wire.phase = "runner"
    a.environment.refresh()
    a.environment.root("supervisor")
    a.runner.supervisor_lost()
    a.sender.serve()
    runner = wire.counts["runner"]
    wire.phase = "supervisor"
    simulated = world.runner

    def poll():
        polls.append(wire.counts.get("supervisor", 0))
        previous, wire.phase = wire.phase, "simulated"
        try:
            simulated()
        finally:
            wire.phase = previous

    monkeypatch.setattr(world, "runner", poll)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    deltas = [later - earlier for earlier, later in zip([0, *polls], polls)]
    # The first poll joins and probes the supervisor's own access, the last
    # concludes: those count toward admission and settlement.
    admission += deltas[0]
    steady = deltas[1:-1]
    # The supervisor's last poll, its cleanup, and then the runner's settlement.
    settlement = wire.counts["supervisor"] - polls[-2]
    wire.phase = "settlement"
    job = a.environment.root("supervisor")
    job.setdefault("status", {})["succeeded"] = 1
    a.environment.kube.put(job)
    a.runner.settle()
    settlement += wire.counts["settlement"]
    return {
        "admission": admission,
        "supervisor": max(steady),
        "runner": runner,
        "publish": max(publishes),
        "collect": max(pulls),
        "settlement": settlement,
    }


CONSTANTS = {
    "admission": plan.ADMISSION_REQUESTS,
    "supervisor": plan.SUPERVISOR_POLL_REQUESTS,
    "runner": plan.RUNNER_POLL_REQUESTS,
    "publish": plan.PUBLISH_REQUESTS,
    "collect": plan.COLLECT_REQUESTS,
    "settlement": plan.SETTLEMENT_REQUESTS,
}


def test_the_request_floor_covers_what_a_synthetic_run_sends(admitting, monkeypatch):
    measured = measure(admitting, monkeypatch)
    for name, constant in CONSTANTS.items():
        # Each constant covers its measurement and stays within half again of
        # it, so a constant left behind by a cheaper rig is noticed too.
        assert measured[name] <= constant <= 1.5 * measured[name], (name, measured)


def test_the_floor_sums_the_rig_s_requests_for_one_pass():
    assert plan.POLLS == 228
    sent = 1500 + 528 + 228 * (160 + 100) + 1300 + 100 + 256 + 3 * 512
    # Each 512-request grant costs three more.
    assert plan.request_floor(0, 0) == sent + 3 * -(-sent // 512)
    sent += 24 * 50 + 201 * 75
    assert plan.request_floor(24, 201) == sent + 3 * -(-sent // 512)


@pytest.mark.parametrize("records, floor", [(1000, 81249), (10000, 103881)])
def test_a_ceiling_below_the_floor_is_refused(records, floor):
    trial = {
        "version": 3,
        "trial": "rescale-out",
        "entry_point": "datastream",
        "records_per_subscription": records,
        "traffic_limits": dict(plan.COUNTER_CEILINGS),
        "total_request_limit": floor,
    }
    assert plan.input_plan("run-a", trial)["request_floor"] == floor
    trial["total_request_limit"] = floor - 1
    with pytest.raises(plan.Failure, match=f"below the {floor} requests"):
        plan.input_plan("run-a", trial)
