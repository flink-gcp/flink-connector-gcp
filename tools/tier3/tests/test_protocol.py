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
"""The pinned #1246 protocol and the quantities the sessions derive from it."""

import itertools
import tomllib
from pathlib import Path

import pytest
from flink_tier3 import model, protocol
from flink_tier3.bundle import delivered_sources, package_sources
from flink_tier3.common import Failure

PIN = Path(protocol.__file__).parent / protocol.PROTOCOL_FILE


def test_pin_holds_the_420_preregistered_cells_and_their_rules():
    loaded = protocol.load()
    cells = loaded["cells"]
    assert len(cells) == 420
    assert len({cell["cell_id"] for cell in cells}) == 420
    combos = {
        (c["flink"], c["shape"], c["body_bytes"], c["repetition"], c["arm"])
        for c in cells
    }
    assert combos == set(
        itertools.product(
            protocol.LINES,
            protocol.SHAPES,
            protocol.BODIES,
            protocol.REPETITIONS,
            model.ARMS,
        )
    )
    for cell in cells:
        warm, observation = protocol.window(cell["checkpoint_seconds"])
        assert (cell["warmup_seconds"], cell["observation_seconds"]) == (
            warm,
            observation,
        )
        assert cell["channel_pool_size"] == 1
    assert loaded["capacity_search"]["growth_factor"] == 2
    assert loaded["support_thresholds"]["general"]["throughput_ratio_min"] == 0.7
    # The serial warm-up plus observation time of the pinned cells is 40 hours.
    assert (
        sum(c["warmup_seconds"] + c["observation_seconds"] for c in cells) == 40 * 3600
    )


def test_pin_ships_with_the_package_but_not_with_the_supervisor_delivery():
    sources = package_sources()
    assert protocol.PROTOCOL_FILE in sources
    # `runtime_sha256` still covers the pin, because it hashes the complete
    # package; nothing the supervisor entrypoint imports reaches it, so it is
    # not in the delivery that `delivery_sha256` pins.
    assert protocol.PROTOCOL_FILE not in delivered_sources()
    assert "analyze.py" not in delivered_sources()
    assert PIN.read_text().startswith("# Copyright 2026 The flink-gcp authors")
    assert len(protocol.DRAFT_SHA256) == 64
    assert len(protocol.protocol_sha256()) == 64


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda d: d.update(order_seed=1), "identity"),
        (lambda d: d["capacity_search"].update(growth_factor=3), "rules"),
        (lambda d: d["support_thresholds"]["general"].update(p95_ratio_max=3), "rules"),
        (lambda d: d["cells"].pop(), "420"),
        (lambda d: d["cells"][0].update(cell_id=d["cells"][1]["cell_id"]), "unique"),
        (lambda d: d["cells"][0].update(warmup_seconds=30), "window"),
        (lambda d: d["cells"][0].update(parallelism=4), "shape"),
        (lambda d: d["cells"][0].update(channel_pool_size=4), "domains"),
        (lambda d: d["cells"][0].update(extra=1), "fields"),
    ],
)
def test_corrupted_pins_are_refused(tmp_path, mutate, message):
    data = tomllib.loads(PIN.read_text())
    mutate(data)
    lines = [f"status = {data['status']!r}".replace("'", '"')]
    lines += [f"version = {data['version']}", f"order_seed = {data['order_seed']}"]
    lines += ["pending = []", "[capacity_search]"]
    lines += [f"{k} = {v}" for k, v in data["capacity_search"].items()]
    lines += ["[support_thresholds]"]
    lines += [
        f"{k} = {v}"
        for k, v in data["support_thresholds"].items()
        if not isinstance(v, dict)
    ]
    for group in ("general", "constrained"):
        lines += [f"[support_thresholds.{group}]"]
        lines += [f"{k} = {v}" for k, v in data["support_thresholds"][group].items()]
    for cell in data["cells"]:
        lines += ["[[cells]]"]
        for k, v in cell.items():
            lines += [f"{k} = {v!r}".replace("'", '"')]
    copy = tmp_path / "protocol.toml"
    copy.write_text("\n".join(lines) + "\n")
    with pytest.raises(Failure, match=message):
        protocol.load(copy)


def test_session_cells_derive_records_and_attempt_limits_from_the_rate():
    cells = protocol.load()["cells"]
    s1 = next(c for c in cells if c["shape"] == "s1" and c["arm"] == "UNNAMED")
    cell = protocol.session_cell(s1, 10)
    model.validate_cell(cell, manifest=False)
    assert cell["record_limit"] == 2430 and cell["attempt_limit"] == 3645
    assert cell["id"] == s1["cell_id"] and cell["distribution"] == "even"
    s7 = next(c for c in cells if c["shape"] == "s7")
    skew = protocol.session_cell(s7, 1000, suffix="-q1")
    model.validate_cell(skew, manifest=False)
    assert skew["distribution"] == "skew" and skew["id"].endswith("-q1")
    assert skew["record_limit"] == 601000
    assert skew["attempt_limit"] == 811350  # 1.5 x the 90 % hot subtask share
    even16 = protocol.session_cell(next(c for c in cells if c["shape"] == "s3"), 1000)
    assert even16["attempt_limit"] == 56345  # 1.5 x ceil(records / 16), rounded up
    # The largest window at the largest rate still fits the application's
    # ten-million record limit, so no legal rate can exceed it.
    assert protocol.session_cell(s7, 10000)["record_limit"] == 6010000
    for rate in (0, 10001, 10.0):
        with pytest.raises(Failure, match="Offered rate"):
            protocol.session_cell(s1, rate)
    with pytest.raises(Failure, match="label grammar"):
        protocol.derived_id("a" * 40, "-x2")
    control = protocol.session_cell(s1, 20, control_delay_millis=100)
    model.validate_cell(control, manifest=False)
    counts_only = protocol.session_cell(s1, 20, emit=False)
    assert counts_only["emit_attempts"] is False


def test_capacity_search_doubles_then_bisects_to_ten_percent():
    state = {"initial": 100}
    trace = []
    truth = 1450  # the highest sustainable rate in this synthetic search
    for _ in range(40):
        status, rate = protocol.next_probe(state)
        trace.append((status, rate))
        if status == "frozen":
            break
        if rate <= truth:
            state["accepted"] = rate
        else:
            state["rejected"] = rate
    assert trace[:5] == [
        ("searching", 100),
        ("searching", 200),
        ("searching", 400),
        ("searching", 800),
        ("searching", 1600),
    ]
    assert trace[-1][0] == "frozen"
    frozen = trace[-1][1]
    assert frozen <= truth
    assert (state["rejected"] - frozen) / state["rejected"] <= 0.1
    assert protocol.next_probe({"initial": 5, "rejected": 1}) == ("frozen", 0)
    assert protocol.next_probe({"initial": 5, "rejected": 8}) == ("searching", 4)
    assert protocol.next_probe({"initial": 5, "accepted": 9000}) == (
        "searching",
        10000,
    )
    # The protocol admits no rate above 10,000, so accepting it ends the
    # search; asking for the same probe again would buy the same answer.
    assert protocol.next_probe({"initial": 5, "accepted": 10000}) == (
        "ceiling",
        10000,
    )


def test_a_search_that_reaches_the_ceiling_terminates():
    state = {"initial": 25}
    seen = []
    for _ in range(40):
        status, rate = protocol.next_probe(state)
        if status in ("frozen", "ceiling"):
            break
        seen.append(rate)
        state["accepted"] = rate  # every rate this instrument offers is met
    assert (status, rate) == ("ceiling", 10000)
    assert seen[-2:] == [6400, 10000]


def test_the_documented_probe_counts_follow_from_the_pinned_search():
    """The planning figures of the calibration evidence document.

    `docs/adr/evidence/0162-cloudtasks-assessment-1246.md` states how many
    probes each planning capacity needs; the numbers are the search's, so the
    search owns them.
    """

    def probes(capacity, initial=25):
        state = {"initial": initial, "accepted": None, "rejected": None}
        offered = []
        while True:
            status, rate = protocol.next_probe(state)
            if status in ("frozen", "ceiling"):
                return offered
            offered.append(rate)
            if rate <= capacity:
                state["accepted"] = max(state["accepted"] or 0, rate)
            else:
                state["rejected"] = min(state["rejected"] or rate, rate)

    counts = {capacity: len(probes(capacity)) for capacity in (25, 400, 500, 3000)}
    assert counts == {25: 6, 400: 10, 500: 9, 3000: 11}
    # Four groups at 25, four at 400, twelve at 500 and eight at 3,000.
    assert 4 * 6 + 4 * 10 + 12 * 9 + 8 * 11 == 260
