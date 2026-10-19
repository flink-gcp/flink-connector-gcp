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
from flink_tier3.common import Failure, TransportError

from ..bigquery.test_resources import NOW, Http

TRIAL = Trial("bt-oracle", 20, 10, "2026-10-20T01:05:00Z")
ONE = (1).to_bytes(8, "big")


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


def relay_row(n, deliveries=1):
    key = f"{n % 16:x}/{n:07d}"
    return key, [("r", f"initial/u/{i}", 0, b"") for i in range(deliveries)]


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
