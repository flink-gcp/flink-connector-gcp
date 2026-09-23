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
"""Recomputing a deployed BigQuery verdict from an exported evidence set.

The seam between the exercise that writes this evidence and the analyzer that
reads it is crossed by `test_bigquery_execution`; these cases build the shapes
a real run does not produce.
"""

import json

import pytest
from flink_tier3 import analyze
from flink_tier3.bigquery_analyze import SCENARIO, counts, render
from flink_tier3.bigquery_verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    OBSERVED,
    SAMPLED_STAGES,
)
from flink_tier3.common import INCONCLUSIVE, INCONSISTENT, TAMPERED, UNEXPORTED, USABLE

RUN = "bq-1423-0001"
ATTEMPTS = 3
WINDOW = {"attempts": ATTEMPTS, "observed": sorted(OBSERVED)}
SAMPLES = ATTEMPTS * len(SAMPLED_STAGES)
# One returned id per sink family, and a TaskManager that published a memory
# metric: the reading a healthy poll emits.
FULL = {
    "sink": [
        {"id": "busyTimeMsPerSecond"},
        {"id": "inPoolUsage"},
        {"id": "Sink__Writer.openDestinations"},
    ],
    "taskmanagers": [
        {"id": "tm-0", "metrics": [{"id": "Status.JVM.Memory.Direct.MemoryUsed"}]}
    ],
}
# What the same poll emits when the job exposed nothing.
BLIND = {"sink": {"unavailable": "503"}, "taskmanagers": {"unavailable": "503"}}


def record(**overrides):
    value = {
        "stage": COMPLETE_STAGE,
        "at": "2026-09-22T00:00:00Z",
        "outcomes": {
            "upgrade": {"checkpoint": {"id": 1}},
            "failover": {"checkpoint": {"id": 2}},
            "query": {"report": {"verdict": "pass"}},
        },
        "coverage": {stage: dict(WINDOW) for stage in SAMPLED_STAGES},
        "verdict": USABLE,
        "reasons": [],
    }
    value.update(overrides)
    return value


def samples_of(recovery, reading=None):
    """One measurement event per attempt the record accounts for, by stage.

    Each carries the reading the supervisor emitted beside the record, because
    that is what the analyzer folds: an event shaped only as `{"stage": ...}`
    would prove the counts agree and nothing about what was observed.
    """
    return [
        (MEASUREMENT_EVENT, {"stage": stage, **(FULL if reading is None else reading)})
        for stage, window in (recovery or {}).get("coverage", {}).items()
        for _ in range(window["attempts"])
    ]


def write_run(root, recovery=None, receipt=None, events=None, run_id=RUN):
    """One exported BigQuery run directory, as a download of `runs/<id>/`."""
    run_dir = root / run_id
    (run_dir / "supervisor").mkdir(parents=True, exist_ok=True)
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": run_id, "scenario": SCENARIO, "flink_version": "2.2.1"})
    )
    entries = list(events if events is not None else samples_of(recovery))
    if recovery is not None:
        entries.insert(0, (COMPLETE_EVENT, recovery))
    for index, (event, payload) in enumerate(entries):
        (run_dir / "supervisor" / f"{index:05d}.json").write_text(
            json.dumps(
                {
                    "event": event,
                    "at": f"2026-09-22T00:00:{index:02d}Z",
                    "payload": payload,
                }
            )
        )
    if receipt is not None:
        (run_dir / "result.json").write_text(json.dumps(receipt))
    return run_dir


def receipt_for(recovery, **overrides):
    value = {
        "scenario": SCENARIO,
        "idle": True,
        "success": recovery.get("verdict") == USABLE,
        "recovery": recovery,
    }
    value.update(overrides)
    return value


def assessed(root, expected=1):
    report = analyze.analyze(root)
    assert len(report["bigquery"]) == expected
    return report, report["bigquery"][0]


def test_a_complete_run_is_usable_and_its_verdict_is_recomputed(tmp_path):
    value = record()
    write_run(tmp_path, value, receipt_for(value))
    report, item = assessed(tmp_path)
    assert item == {
        "run_id": RUN,
        "status": USABLE,
        "verdict": USABLE,
        "reasons": [],
        "problems": [],
        "samples": SAMPLES,
    }
    assert "Deployed BigQuery runs" in analyze.render_markdown(report)


def test_an_incomplete_observation_is_inconclusive_with_its_reasons(tmp_path):
    reasons = [f"unobserved-{name}-in-finishing" for name in OBSERVED]
    coverage = {
        "baseline": dict(WINDOW),
        "finishing": {"attempts": ATTEMPTS, "observed": []},
    }
    value = record(coverage=coverage, verdict=INCONCLUSIVE, reasons=reasons)
    # The readings say the same thing the record does: the finishing window
    # asked three times and the job exposed nothing.
    events = [
        *samples_of({"coverage": {"baseline": dict(WINDOW)}}),
        *samples_of({"coverage": {"finishing": dict(WINDOW)}}, reading=BLIND),
    ]
    write_run(tmp_path, value, receipt_for(value), events=events)
    _, item = assessed(tmp_path)
    assert item["status"] == INCONCLUSIVE
    assert item["problems"] == []
    assert item["reasons"] == reasons


def test_a_completed_record_without_its_completion_evidence_is_unexported(tmp_path):
    """An absent object: only the record it should have accompanied names it."""
    value = record()
    write_run(tmp_path, None, receipt_for(value), events=samples_of(value))
    _, item = assessed(tmp_path)
    assert item["status"] == UNEXPORTED
    assert item["problems"] == ["missing-" + COMPLETE_EVENT]
    assert item["verdict"] is None


def test_a_fabricated_receipt_with_no_evidence_at_all_is_still_a_forgery(tmp_path):
    """The same shape as a truncated export, and the readings tell them apart.

    A receipt whose `recovery` claims a completed, usable run, in a directory
    holding no completion record and no readings, must not take the benign
    label the missing object alone would earn it: nothing backs the claim.
    """
    value = record()
    write_run(tmp_path, None, receipt_for(value), events=[])
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == [
        "coverage-unsupported-by-its-readings",
        "missing-" + COMPLETE_EVENT,
        "receipt-success-mismatch",
        "verdict-unsupported-by-its-inputs",
    ]
    assert item["samples"] == 0


def test_a_minimal_fabrication_is_caught_by_the_verdict_alone(tmp_path):
    """No coverage to overstate, so only the claimed verdict is left to check."""
    value = {"stage": COMPLETE_STAGE, "verdict": USABLE, "reasons": []}
    write_run(tmp_path, None, receipt_for(value), events=[])
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert "verdict-unsupported-by-its-inputs" in item["problems"]


def test_a_run_that_never_completed_is_inconclusive_not_an_evidence_problem(tmp_path):
    """The archetypal case: both recoveries proved, then the query slots ran out."""
    aborted = record(stage="visibility")
    del aborted["verdict"], aborted["reasons"]
    # The record was written at the run's last transition; the samples after
    # it are readings the record legitimately does not account for.
    events = [*samples_of(aborted), *samples_of(aborted)[:2]]
    write_run(tmp_path, None, receipt_for(aborted), events=events)
    _, item = assessed(tmp_path)
    assert item["status"] == INCONCLUSIVE
    assert item["verdict"] == INCONCLUSIVE
    assert item["reasons"] == ["recovery-incomplete"]
    assert item["problems"] == []
    assert item["samples"] == SAMPLES + 2


@pytest.mark.parametrize(
    "missing, problem",
    [
        ("result.json", "missing-result.json"),
        (COMPLETE_EVENT, "missing-" + COMPLETE_EVENT),
    ],
)
def test_an_evidence_set_missing_the_run_s_own_account_is_unexported(
    tmp_path, missing, problem
):
    value = record()
    receipt = receipt_for(value)
    receipt["recovery"] = None
    write_run(
        tmp_path,
        value if missing == "result.json" else None,
        None if missing == "result.json" else receipt,
        events=samples_of(value),
    )
    _, item = assessed(tmp_path)
    assert item["status"] == UNEXPORTED
    assert item["verdict"] is None
    assert item["problems"] == [problem]
    assert item["samples"] == SAMPLES


def test_a_verdict_its_own_inputs_do_not_support_is_tampered(tmp_path):
    """The receipt says usable; the record beside it says nothing was read."""
    value = record(coverage={}, verdict=USABLE, reasons=[])
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["verdict"] == INCONCLUSIVE
    assert "verdict-unsupported-by-its-inputs" in item["problems"]


@pytest.mark.parametrize("reasons", [5, "unobserved-task-in-baseline", {}])
def test_a_reasons_field_that_is_not_a_list_is_classified_rather_than_raising(
    tmp_path, reasons
):
    """`recovery-complete` payloads are unvalidated, and this reads them."""
    value = record(reasons=reasons)
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == ["verdict-unsupported-by-its-inputs"]


def test_a_coverage_the_readings_deny_is_tampered_however_consistent_it_is(tmp_path):
    """The forgery the recomputation exists to catch.

    A run whose sink was down for the whole trial read nothing, so the honest
    record is `inconclusive` with empty windows. Edit that record to claim
    every family in both windows, leave the attempt counts alone and set the
    verdict and the receipt to match, and every check that reads only the
    record agrees with itself. The readings beside it do not.
    """
    forged = record()
    blind = samples_of(forged, reading=BLIND)
    write_run(tmp_path, forged, receipt_for(forged), events=blind)
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == [
        "coverage-unsupported-by-its-readings",
        # The forged receipt claims a success the readings do not support.
        "receipt-success-mismatch",
        "verdict-unsupported-by-its-inputs",
    ]
    # The verdict reported is the one the readings support, not the claim.
    assert item["verdict"] == INCONCLUSIVE
    assert item["samples"] == SAMPLES


@pytest.mark.parametrize("event", [MEASUREMENT_EVENT, COMPLETE_EVENT])
@pytest.mark.parametrize("name", ["00001.json", "99999.json"])
def test_an_unreadable_record_is_an_incomplete_export_not_a_forgery(
    tmp_path, event, name
):
    """A partial download must not be reported as a dishonest run.

    Whatever that record held is missing from the rebuilt coverage, so the
    record would look like one claiming more than its readings support. The
    answer is to fetch the evidence again, not to accuse the run.
    """
    value = record()
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value))
    (tmp_path / RUN / "supervisor" / name).write_text(
        json.dumps(
            {"event": event, "at": "2026-09-22T00:01:00Z", "payload": "baseline"}
        )
    )
    _, item = assessed(tmp_path)
    assert item["status"] == UNEXPORTED
    assert item["problems"] == ["malformed-evidence:1"]
    assert item["verdict"] is None


def test_a_reading_the_fold_cannot_place_is_an_incomplete_export_too(tmp_path):
    """An object, but with no window to fold into: lost the same way."""
    value = record()
    events = [*samples_of(value), (MEASUREMENT_EVENT, dict(FULL))]
    write_run(tmp_path, value, receipt_for(value), events=events)
    _, item = assessed(tmp_path)
    assert item["status"] == UNEXPORTED
    assert item["problems"] == ["malformed-evidence:1"]


@pytest.mark.parametrize(
    "observed", [[["connector"]], [{"a": 1}], [None], ["connector", 3]]
)
def test_an_observed_list_of_the_wrong_element_type_is_classified_not_raised(
    tmp_path, observed
):
    """`set` of an unhashable element would abort the whole directory."""
    coverage = {stage: dict(WINDOW) for stage in SAMPLED_STAGES}
    coverage["baseline"] = dict(coverage["baseline"], observed=observed)
    value = record(coverage=coverage)
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == ["coverage-unsupported-by-its-readings"]
    # Decided over the readings, which are untouched by the edited claim.
    assert item["verdict"] == USABLE


# Readings that match the claimed coverage, so nothing but the verdict field
# can decide the outcome: a mismatched coverage is an overstatement on its own
# and would force `tampered` whatever this rule did.
@pytest.mark.parametrize(
    "stored, status, problem",
    [
        ("USABLE", TAMPERED, "verdict-unsupported-by-its-inputs"),
        ("usable ", TAMPERED, "verdict-unsupported-by-its-inputs"),
        ("pass", TAMPERED, "verdict-unsupported-by-its-inputs"),
        (True, TAMPERED, "verdict-unsupported-by-its-inputs"),
        ({}, TAMPERED, "verdict-unsupported-by-its-inputs"),
        (INCONCLUSIVE, INCONSISTENT, "verdict-disagrees-with-its-readings"),
    ],
)
def test_only_a_claim_other_than_the_conservative_one_is_a_forgery(
    tmp_path, stored, status, problem
):
    """A typo must not take the favourable label off a claim, and a record
    that under-reports must not be accused of making one."""
    value = record(verdict=stored, reasons=[])
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value))
    _, item = assessed(tmp_path)
    assert item["status"] == status
    assert item["problems"] == [problem]
    # The readings support a usable run; only the stored claim disagrees.
    assert item["verdict"] == USABLE


def test_a_window_claiming_fewer_families_than_it_read_is_not_a_forgery(tmp_path):
    """Short of its readings is a disagreement; beyond them is the forgery."""
    coverage = {stage: dict(WINDOW) for stage in SAMPLED_STAGES}
    coverage["finishing"] = dict(coverage["finishing"], observed=["task"])
    value = record(coverage=coverage, verdict=INCONCLUSIVE)
    value["reasons"] = [
        f"unobserved-{name}-in-finishing" for name in OBSERVED if name != "task"
    ]
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value))
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["problems"] == [
        "readings-the-record-does-not-account-for",
        "verdict-disagrees-with-its-readings",
    ]
    # Decided over what the readings support, so the under-claim does not stick.
    assert item["verdict"] == USABLE


def test_a_window_with_no_readings_at_all_is_unsupported(tmp_path):
    """The wholly fabricated window, not merely one with attempts moved in."""
    value = record()
    only_baseline = {"coverage": {"baseline": dict(WINDOW)}}
    write_run(tmp_path, value, receipt_for(value), events=samples_of(only_baseline))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert "coverage-unsupported-by-its-readings" in item["problems"]


def test_a_receipt_that_disagrees_with_the_evidence_is_inconsistent(tmp_path):
    value = record()
    receipt = receipt_for(value)
    receipt.update(recovery=dict(value, reasons=["edited"]), success=False)
    write_run(tmp_path, value, receipt)
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["verdict"] == USABLE
    assert item["problems"] == ["receipt-record-mismatch"]


def test_a_receipts_own_copy_of_the_record_is_excluded_but_not_accused(tmp_path):
    """A deliberate boundary, pinned so it cannot drift unnoticed.

    `success` is the claim a reader acts on, and it is checked as one. The
    receipt's embedded copy of the record is not: when it disagrees with the
    completion evidence the run wrote, the run is excluded as inconsistent and
    the verdict reported stays the one the readings support. Upgrading that to
    forgery would mean treating a sub-document nobody's `success` rests on as
    a published claim.
    """
    value = record(verdict=INCONCLUSIVE)
    value["reasons"] = [f"unobserved-{name}-in-baseline" for name in OBSERVED]
    coverage = {"baseline": {"attempts": ATTEMPTS, "observed": []}}
    coverage["finishing"] = dict(WINDOW)
    value["coverage"] = coverage
    events = [
        *samples_of({"coverage": {"baseline": dict(WINDOW)}}, reading=BLIND),
        *samples_of({"coverage": {"finishing": dict(WINDOW)}}),
    ]
    receipt = receipt_for(value)
    receipt["recovery"] = dict(value, verdict=USABLE, reasons=[])
    write_run(tmp_path, value, receipt, events=events)
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["problems"] == ["receipt-record-mismatch"]
    assert item["verdict"] == INCONCLUSIVE


def test_a_receipt_claiming_success_the_verdict_does_not_support_is_a_forgery(
    tmp_path,
):
    """The runner computes `success` from a usable verdict and nothing else."""
    value = record(stage="visibility", verdict=INCONCLUSIVE)
    value["reasons"] = ["recovery-incomplete"]
    write_run(tmp_path, value, receipt_for(value, success=True))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == ["receipt-success-mismatch"]


def test_a_fabrication_that_claims_only_through_the_receipt_is_still_a_forgery(
    tmp_path,
):
    """The claim need not be in the record: `success` is what a run is read for.

    A record carrying nothing but a completed stage asserts no verdict and no
    coverage, so every check that reads the record stays silent. The receipt
    beside it says the run succeeded, and no reading supports that.
    """
    write_run(
        tmp_path,
        None,
        receipt_for({"stage": COMPLETE_STAGE}, success=True),
        events=[],
    )
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert item["problems"] == [
        "missing-" + COMPLETE_EVENT,
        "receipt-success-mismatch",
    ]
    # What the readings support, which is what makes the exclusion legible.
    assert item["verdict"] == INCONCLUSIVE


def test_a_receipt_withholding_success_from_a_usable_run_is_not_a_problem(tmp_path):
    """`success` is a conjunction the runner may refuse for its own reasons."""
    value = record()
    write_run(tmp_path, value, receipt_for(value, success=False))
    _, item = assessed(tmp_path)
    assert item["status"] == USABLE
    assert item["problems"] == []


def test_a_receipt_naming_another_scenario_is_inconsistent(tmp_path):
    value = record()
    write_run(tmp_path, value, receipt_for(value, scenario="generic-recovery"))
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["problems"] == ["receipt-scenario-mismatch"]


def test_a_record_claiming_more_samples_than_it_has_readings_is_tampered(tmp_path):
    value = record()
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value)[:-1])
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert "coverage-unsupported-by-its-readings" in item["problems"]
    assert item["samples"] == SAMPLES - 1


def test_a_completed_record_short_of_its_own_readings_is_inconsistent(tmp_path):
    """Not a forgery: it claims nothing the evidence denies, but disagrees."""
    value = record()
    events = samples_of(value)
    write_run(tmp_path, value, receipt_for(value), events=[*events, events[0]])
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["problems"] == ["readings-the-record-does-not-account-for"]


def test_attempts_moved_between_windows_do_not_balance_out(tmp_path):
    """One total can be moved; a per-stage count cannot, so this is caught."""
    coverage = {
        "baseline": {"attempts": ATTEMPTS * 2, "observed": sorted(OBSERVED)},
        "finishing": {"attempts": 0, "observed": sorted(OBSERVED)},
    }
    value = record(coverage=coverage)
    write_run(tmp_path, value, receipt_for(value), events=samples_of(record()))
    _, item = assessed(tmp_path)
    assert item["status"] == TAMPERED
    assert "coverage-unsupported-by-its-readings" in item["problems"]


def test_a_repeated_completion_is_excluded_rather_than_read_twice(tmp_path):
    """Two accounts of one run disagree with each other; neither is a claim."""
    first = record(verdict=INCONCLUSIVE, reasons=["recovery-incomplete"])
    second = record()
    write_run(
        tmp_path,
        None,
        receipt_for(second),
        events=[(COMPLETE_EVENT, first), (COMPLETE_EVENT, second), *samples_of(second)],
    )
    _, item = assessed(tmp_path)
    assert item["status"] == INCONSISTENT
    assert item["problems"] == [f"repeated-{COMPLETE_EVENT}:2"]
    # The later record is the one read: the earlier one would not recompute.
    assert item["verdict"] == USABLE


def test_every_bigquery_run_is_assessed_in_the_order_the_analyzer_found_them(tmp_path):
    good = record()
    bad = record(coverage={}, verdict=USABLE, reasons=[])
    write_run(tmp_path, good, receipt_for(good), run_id="bq-0001")
    write_run(tmp_path, bad, receipt_for(bad), run_id="bq-0002")
    report, _ = assessed(tmp_path, expected=2)
    assert [item["run_id"] for item in report["bigquery"]] == ["bq-0001", "bq-0002"]
    assert [item["status"] for item in report["bigquery"]] == [USABLE, TAMPERED]
    assert counts(report["bigquery"]) == "tampered 1, usable 1"
    assert "bigquery runs: tampered 1, usable 1" in analyze.summary_line(
        report, tmp_path
    )


def test_the_rendered_section_names_each_column_of_each_run(tmp_path):
    good = record()
    write_run(tmp_path, good, receipt_for(good), run_id="bq-0001")
    write_run(tmp_path, good, None, run_id="bq-0002")
    report, _ = assessed(tmp_path, expected=2)
    section = render(report["bigquery"])
    assert "| run | status | verdict | samples | reasons | problems |" in section
    assert f"| bq-0001 | usable | usable | {SAMPLES} | - | - |" in section
    assert (
        f"| bq-0002 | unexported | - | {SAMPLES} | - | missing-result.json |" in section
    )
    assert "`baseline` and `finishing` windows" in section
    assert f"not `{COMPLETE_STAGE}`" in section


def test_a_directory_without_a_bigquery_run_reports_no_section(tmp_path):
    value = record()
    run_dir = write_run(tmp_path, value, receipt_for(value))
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": RUN, "scenario": "cloudtasks", "cells": []})
    )
    report = analyze.analyze(tmp_path)
    assert report["bigquery"] == []
    assert render([]) == ""
    assert counts([]) == ""
    rendered = analyze.render_markdown(report)
    assert "Deployed BigQuery runs" not in rendered
    assert "bigquery runs" not in analyze.summary_line(report, tmp_path)
