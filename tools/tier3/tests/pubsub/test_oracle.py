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
"""The Python port of the relay's output oracle, against the Java report's cases."""

import base64
from pathlib import Path

import pytest
from flink_tier3.common import Failure
from flink_tier3.pubsub.oracle import COUNTERS, Report, reconcile

CASES = (
    Path(__file__).resolve().parents[4]
    / "kubernetes/apps/pubsub/src/test/resources/oracle-cases.txt"
)
ATTEMPT = "a0000001-0000-4000-8000-000000000001"


def shared_cases():
    cases, case = [], None
    for line in CASES.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        kind, _, rest = line.partition(" ")
        if kind == "case":
            # Every case ends with exactly one result line.
            assert case is None or "result" in case, case["name"]
            name, records = rest.split(" ")
            case = {"name": name, "records": int(records.removeprefix("records="))}
            case["accepts"] = []
        elif kind == "accept":
            case["accepts"].append(tuple(rest.split(" ", 1)))
        else:
            assert kind in ("expect", "reject", "refuse") and "result" not in case
            case["result"] = line
            cases.append(pytest.param(case, id=case["name"]))
    assert case is not None and "result" in case
    return cases


@pytest.mark.parametrize("case", shared_cases())
def test_each_shared_case_is_decided_as_the_java_report_decides_it(case):
    report = Report("oracle", case["records"])
    try:
        for output_message_id, payload in case["accepts"]:
            report.accept(output_message_id, payload)
        counters = report.complete()
    except Failure as error:
        assert case["result"] in ("refuse", "reject " + str(error))
    else:
        assert case["result"] == "expect " + " ".join(
            f"{name}={counters[name]}" for name in COUNTERS
        )


def test_the_shared_cases_cover_every_refusal_and_every_counter():
    cases = [param.values[0] for param in shared_cases()]
    results = {case["result"] for case in cases}
    assert len(cases) == len({case["name"] for case in cases}) == 24
    rejections = {result for result in results if result.startswith("reject ")}
    assert len(rejections) == 11
    counted = [result for result in results if "duplicates=1" in result]
    assert counted and all(f"{name}=1" in counted[0].split() for name in COUNTERS[1:])


def payload(index=0, sequence=0, attempt=ATTEMPT, observation=None, message="aW4"):
    observation = observation or f"b{index:07d}-0000-4000-8000-{sequence:012d}"
    return (
        f"v1|oracle|{index}|{sequence}|{message}|{attempt}|{observation}|initial|false"
    )


def encode(text):
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def line(output_message_id, text):
    return encode(output_message_id) + "\t" + encode(text)


def test_reconcile_reports_what_is_missing_without_refusing():
    result = reconcile([line("o-1", payload())], "oracle", 1)
    assert result == {
        "lines": 1,
        "rejected": None,
        "missing_inputs": 1,
        "logical_inputs": 1,
        "input_publication_duplicates": 0,
        "repeated_input_processing": 0,
        "output_publication_duplicates": 0,
        "repeated_output_delivery": 0,
    }


def test_reconcile_counts_a_repeated_line_as_redelivery():
    lines = [line("o-1", payload()), line("o-2", payload(1, message=encode("other")))]
    result = reconcile([*lines, lines[0]], "oracle", 1)
    assert result["missing_inputs"] == 0
    assert result["repeated_output_delivery"] == 1


def test_reconcile_names_the_first_refused_line_and_counts_nothing():
    lines = [line("o-1", payload()), "not a line", line("o-2", "foreign")]
    assert reconcile(lines, "oracle", 1) == {
        "lines": 3,
        "rejected": "Expected bounded base64url ID and payload separated by a tab",
        "rejected_line": 2,
    }


@pytest.mark.parametrize(
    "value,message",
    [
        ("a\tb\tc", "separated by a tab"),
        ("YQ==\t" + encode(payload()), "Noncanonical"),
        ("\t" + encode(payload()), "lacks an output message ID"),
        ("YQ\t" + "A" * 4094, "4096 characters"),
        # Java's String.length() counts UTF-16 units, so an astral character
        # outside base64url still counts two toward the bound before decoding.
        ("YQ\t" + "\U0001f600" * 2047, "4096 characters"),
    ],
)
def test_a_line_is_refused_as_the_report_s_reader_refuses_it(value, message):
    with pytest.raises(Failure, match=message):
        Report("oracle", 1).accept_line(value)


def test_bounds_count_utf16_units_as_java_does():
    report = Report("oracle", 1)
    # 128 astral characters are 256 UTF-16 units, the largest ID allowed.
    report.accept("\U0001f600" * 128, payload())
    with pytest.raises(Failure, match="bound"):
        report.accept("\U0001f600" * 128 + "x", payload())


def test_the_line_bound_holds_across_accepted_lines():
    report = Report("oracle", 1)
    report.lines = 200000 - 1
    report.accept("o-1", payload())
    with pytest.raises(Failure, match="bound"):
        report.accept("o-1", payload())


@pytest.mark.parametrize(
    "sequence,message",
    [
        # Java's own NumberFormatException names these; the port refuses them
        # as noncanonical, and the shared cases require only the refusal.
        ("", "canonical decimal"),
        ("x", "canonical decimal"),
        ("00", "canonical decimal"),
        # Integer.parseInt reads digits of any script; only the canonical
        # rendering refuses them.
        ("٠", "canonical decimal"),
        ("-1", "outside this run's logical input domain"),
    ],
)
def test_input_identity_parses_as_integer_parse_int_does(sequence, message):
    text = payload().replace("|0|0|", f"|0|{sequence}|")
    with pytest.raises(Failure, match=message):
        Report("oracle", 1).accept("o-1", text)


@pytest.mark.parametrize(
    "attempt",
    [
        "",
        "a0000001000040008000000000000001",
        "{" + ATTEMPT + "}",
        "urn:uuid:" + ATTEMPT,
    ],
)
def test_only_a_canonical_uuid_names_an_attempt(attempt):
    with pytest.raises(Failure, match="must be UUIDs"):
        Report("oracle", 1).accept("o-1", payload(attempt=attempt))
