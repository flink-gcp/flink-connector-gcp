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
"""ReadRows chunk merging and the three tables' oracles."""

import base64

import pytest
from flink_tier3.bigtable.instance import Instance, merge
from flink_tier3.bigtable.oracle import AtLeastOnce, ExactlyOnce, Relay
from flink_tier3.bigtable.trial import Trial
from flink_tier3.bigtable.verdict import data_reasons
from flink_tier3.common import Failure, TransportError, timestamp
from flink_tier3.verdict import sequence_verdict

from ..doubles import NOW, Http

TRIAL = Trial("bt-oracle", 20, 10, "2026-10-20T01:05:00Z", "sequence")
ONE = (1).to_bytes(8, "big")
# The change stream's end in microseconds, the unit of a relay cell's commit.
END = int(timestamp(TRIAL.change_stream_end)) * 10**6


def b64(value):
    return base64.b64encode(
        value if isinstance(value, bytes) else value.encode()
    ).decode()


def test_row_keys_are_the_applications():
    """RecoveryOptions.rowKey: `<n mod 16 in hex>/<n, seven digits>`."""
    keys = TRIAL.row_keys()
    assert (
        keys["0/0000000"] == 0 and keys["f/0000015"] == 15 and keys["0/0000016"] == 16
    )
    assert len(keys) == TRIAL.records


def test_chunks_merge_into_rows_by_the_protocol():
    responses = [
        {
            "chunks": [
                # A row whose first attempt is reset before it commits.
                {
                    "rowKey": b64("0/0000000"),
                    "familyName": "agg",
                    "qualifier": b64("count"),
                    "timestampMicros": "1000",
                    "value": b64(b"\0" * 8),
                },
                {"resetRow": True},
                {
                    "rowKey": b64("0/0000000"),
                    "familyName": "agg",
                    "qualifier": b64("count"),
                    "timestampMicros": "1000",
                    "value": b64(ONE),
                },
                # The family carries over; a new qualifier, its value split in two.
                {
                    "familyName": "flink_commit",
                    "qualifier": b64("m1"),
                    "timestampMicros": "7",
                    "value": b64("ab"),
                    "valueSize": 4,
                },
                {"value": b64("cd"), "commitRow": True},
            ]
        },
        {
            "chunks": [
                # The relay writes at timestamp 0, which JSON leaves out.
                {
                    "rowKey": b64("1/0000001"),
                    "familyName": "r",
                    "qualifier": b64("initial/u/0"),
                    "value": b64(ONE),
                    "commitRow": True,
                },
            ]
        },
        # A response that carries no chunks, as a scan's last one may.
        {"lastScannedRowKey": b64("z")},
    ]
    assert merge(responses) == (
        [
            (
                "0/0000000",
                [("agg", "count", 1000, ONE), ("flink_commit", "m1", 7, b"abcd")],
            ),
            ("1/0000001", [("r", "initial/u/0", 0, ONE)]),
        ],
        b64("1/0000001"),
    )


def test_a_stream_that_fails_after_it_began_is_a_transient_failure():
    """Otherwise the rows after it would read as missing from the table."""
    with pytest.raises(TransportError):
        merge([{"chunks": []}, {"error": {"code": 14, "status": "UNAVAILABLE"}}])


def test_pages_resume_after_the_last_key_as_the_service_sent_it():
    raw = base64.b64encode(b"\x80foreign").decode()
    page = [
        {
            "chunks": [
                {
                    "rowKey": raw,
                    "familyName": "r",
                    "qualifier": b64("q"),
                    "commitRow": True,
                }
            ]
        }
    ]
    http = Http(page, [])
    instance = Instance(http, TRIAL, NOW + 600, lambda: NOW)
    rows, after = instance.read_rows("relay")
    assert (rows[0][0], after) == ("\\x80foreign", raw)
    assert instance.read_rows("relay", after) == ([], None)
    (_, first, request), (_, _, resumed) = http.calls
    assert first.endswith("/instances/bt1316-bt-oracle/tables/relay:readRows")
    assert "rows" not in request["json"]
    assert resumed["json"]["rows"] == {"rowRanges": [{"startKeyOpen": raw}]}


def test_a_stream_ending_inside_a_row_is_refused():
    with pytest.raises(Failure, match="ended inside a row"):
        merge(
            [
                {
                    "chunks": [
                        {
                            "rowKey": b64("k"),
                            "familyName": "r",
                            "qualifier": b64("q"),
                        }
                    ]
                }
            ]
        )


def eo_row(n, sum_value=1, markers=1, extra=()):
    key = f"{n % 16:x}/{n:07d}"
    cells = [("agg", "count", 1000, sum_value.to_bytes(8, "big", signed=True))]
    cells += [("flink_commit", f"m{i}", 1, b"") for i in range(markers)]
    return key, cells + list(extra)


def test_the_exactly_once_oracle_passes_only_one_contribution_and_one_marker():
    check = ExactlyOnce(TRIAL)
    check.add([eo_row(n) for n in range(TRIAL.records)])
    assert check.report()["verdict"] == "pass"
    check = ExactlyOnce(TRIAL)
    rows = [eo_row(n) for n in range(2, TRIAL.records)]
    rows[0] = eo_row(2, sum_value=2)
    rows[1] = eo_row(3, markers=2)
    rows[2] = eo_row(4, extra=[("other", "q", 0, b"")])
    check.add([*rows, ("zz", [])])
    report = check.report()
    assert report["verdict"] == "fail"
    assert {
        k: report[k]
        for k in (
            "missing",
            "wrong_sum",
            "wrong_markers",
            "foreign",
            "foreign_families",
        )
    } == {
        "missing": 2,
        "wrong_sum": 1,
        "wrong_markers": 1,
        "foreign": 1,
        "foreign_families": 1,
    }
    assert report["examples"]["missing"] == ["0/0000000", "1/0000001"]
    # A SUM above one is still an applied identity, which the relay must carry.
    assert "2/0000002" in check.applied


def test_the_at_least_once_oracle_counts_replays_without_failing():
    check = AtLeastOnce(TRIAL)
    rows = [eo_row(n, markers=0) for n in range(TRIAL.records)]
    rows[5] = eo_row(5, sum_value=3, markers=0)
    check.add(rows)
    report = check.report()
    assert (
        report["verdict"],
        report["extra_contributions"],
        report["rows_with_extra"],
    ) == ("pass", 2, 1)
    check = AtLeastOnce(TRIAL)
    check.add(rows[1:])
    assert check.report()["verdict"] == "fail"


def relay_row(n, deliveries=1, at=END - 10**6):
    """A relayed identity's row, each delivery holding the commit time `at`.

    The value is the commit time in microseconds in decimal, as the relay
    application writes it with `Long.toString`.
    """
    key = f"{n % 16:x}/{n:07d}"
    commit = str(at).encode()
    return key, [("r", f"initial/u/{i}", 0, commit) for i in range(deliveries)]


EVERY = [relay_row(n) for n in range(TRIAL.records)]


def test_the_relay_oracle_requires_every_applied_identity_and_counts_replays():
    applied = set(TRIAL.row_keys())
    check = Relay(TRIAL)
    check.add([relay_row(n, 2 if n == 7 else 1) for n in range(TRIAL.records)])
    report = check.report(applied, True)
    assert (report["verdict"], report["replayed_rows"], report["extra_deliveries"]) == (
        "pass",
        1,
        1,
    )
    # An identity eo applied that the relay never delivered fails it, and a
    # relayed identity eo did not apply is reported as unapplied.
    check = Relay(TRIAL)
    check.add([relay_row(n) for n in range(1, TRIAL.records)])
    assert check.report(applied, True)["missing"] == 1
    check = Relay(TRIAL)
    check.add([relay_row(n) for n in range(TRIAL.records)])
    report = check.report(applied - {"0/0000000"}, True)
    assert (report["verdict"], report["unapplied"]) == ("fail", 1)


def test_the_relay_oracle_fails_on_a_commit_after_the_change_streams_end():
    """The end is inclusive, so a commit at it passes and one after it fails."""
    applied = set(TRIAL.row_keys())
    check = Relay(TRIAL)
    check.add([relay_row(n, at=END) for n in range(TRIAL.records)])
    report = check.report(applied, True)
    assert (report["verdict"], report["after_end"]) == ("pass", 0)
    # A row counts once however many of its deliveries came after the end,
    # and one after the end fails its row beside one before it, in either
    # order: cells arrive in qualifier order, not commit order.
    late, early = str(END + 1).encode(), str(END - 1).encode()
    check = Relay(TRIAL)
    check.add(
        [
            *(relay_row(n) for n in range(3)),
            (
                "3/0000003",
                [("r", "initial/u/0", 0, early), ("r", "upgrade/v/0", 0, late)],
            ),
            (
                "4/0000004",
                [("r", "initial/u/0", 0, late), ("r", "upgrade/v/0", 0, early)],
            ),
            relay_row(5, deliveries=2, at=END + 1),
            *(relay_row(n) for n in range(6, TRIAL.records)),
        ]
    )
    report = check.report(applied, True)
    assert (report["verdict"], report["after_end"], report["deliveries"]) == (
        "fail",
        3,
        TRIAL.records + 3,
    )
    assert report["examples"]["after_end"] == ["3/0000003", "4/0000004", "5/0000005"]


def test_the_relay_oracle_compares_commits_with_the_end_to_the_microsecond():
    trial = Trial("bt-oracle", 20, 10, "2026-10-20T01:05:00.500000Z", "sequence")
    end = END + 500_000
    for commit, after_end in ((end, 0), (end + 1, trial.records)):
        check = Relay(trial)
        check.add([relay_row(n, at=commit) for n in range(trial.records)])
        assert check.report(set(trial.row_keys()), True)["after_end"] == after_end


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"x",
        b"+1",
        b"-1",
        b" 1",
        b"1\n",
        b"1_0",
        b"1.0",
        b"01",
        # A commit before the end with a leading zero, which `int()` accepts.
        b"0" + str(END - 1).encode(),
        str(2**63).encode(),
        b"1" * 20,
    ],
)
def test_a_delivery_without_a_commit_time_counts_as_a_wrong_value(value):
    """The relay writes `Long.toString` of the commit time; any other value fails.

    The cell is still a delivery, and the row's other deliveries are still
    compared with the end.
    """
    check = Relay(TRIAL)
    late = str(END + 1).encode()
    check.add(
        [
            (
                "0/0000000",
                [("r", "initial/u/0", 0, value), ("r", "upgrade/v/0", 0, late)],
            ),
            *EVERY[1:],
        ]
    )
    report = check.report(set(TRIAL.row_keys()), True)
    assert (
        report["verdict"],
        report["wrong_value"],
        report["after_end"],
        report["deliveries"],
    ) == ("fail", 1, 1, TRIAL.records + 1)
    assert report["examples"]["wrong_value"] == ["0/0000000"]


@pytest.mark.parametrize("values", [[b"x"], [b"x", b"01"]])
def test_a_wrong_value_alone_fails_the_relay_once_per_row(values):
    cells = [("r", f"initial/u/{i}", 0, value) for i, value in enumerate(values)]
    check = Relay(TRIAL)
    check.add([("0/0000000", cells), *EVERY[1:]])
    report = check.report(set(TRIAL.row_keys()), True)
    assert (report["verdict"], report["wrong_value"], report["after_end"]) == (
        "fail",
        1,
        0,
    )


@pytest.mark.parametrize("value", [b"0", str(2**63 - 1).encode()])
def test_every_long_the_relay_can_write_is_a_commit_time(value):
    check = Relay(TRIAL)
    check.add([("0/0000000", [("r", "initial/u/0", 0, value)]), *EVERY[1:]])
    assert check.report(set(TRIAL.row_keys()), True)["wrong_value"] == 0


def test_only_a_delivery_of_an_input_row_is_read_as_a_commit():
    """A foreign row or family fails the relay as such, not as a wrong value."""
    check = Relay(TRIAL)
    check.add(
        [
            ("0/0000000", [*EVERY[0][1], ("x", "q", 0, b"x")]),
            *EVERY[1:],
            ("zz", [("r", "initial/u/0", 0, b"x")]),
        ]
    )
    report = check.report(set(TRIAL.row_keys()), True)
    assert (
        report["verdict"],
        report["foreign"],
        report["foreign_families"],
        report["wrong_value"],
    ) == ("fail", 1, 1, 0)


@pytest.mark.parametrize(
    ("rows", "unapplied", "reason"),
    [
        # Only an applied identity missing, which a commit after the end
        # leaves unrelayed.
        (EVERY[1:], set(), "relay-oracle-unjudged"),
        # A relayed identity `eo` did not apply, a row no input names, a
        # cell outside the relay's family, and a commit after the change
        # stream's end fail it whenever the input finished.
        (EVERY, {"0/0000000"}, "relay-oracle-not-passed"),
        ([*EVERY[1:], ("zz", EVERY[0][1])], set(), "relay-oracle-not-passed"),
        (
            [*EVERY[1:], (EVERY[0][0], [("x", "q", 0, b"")])],
            set(),
            "relay-oracle-not-passed",
        ),
        (
            [*EVERY[2:], relay_row(1, at=END + 1)],
            set(),
            "relay-oracle-not-passed",
        ),
        (
            [*EVERY[2:], (EVERY[1][0], [("r", "initial/u/0", 0, b"x")])],
            set(),
            "relay-oracle-not-passed",
        ),
    ],
)
def test_an_unjudged_relay_fails_only_on_what_timing_cannot_explain(
    rows, unapplied, reason
):
    """An unjudged relay can run again as it is only when it just lacks rows."""
    check = Relay(TRIAL)
    check.add(rows)
    oracles = {
        "eo": {"verdict": "pass"},
        "alo": {"verdict": "pass"},
        "relay": check.report(set(TRIAL.row_keys()) - unapplied, False),
    }
    decided = sequence_verdict(
        {"stage": "complete", "outcomes": {"oracles": oracles}}, data_reasons
    )
    assert decided["reasons"] == [reason]
