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
"""Recompute a deployed Cloud Tasks run's verdict from its exported evidence.

The Pod decided the verdict from the listing pages it exported; this decides
it again from those and the record, so a reader holding only the downloaded
evidence directory can check the stored verdict against them. The vocabulary
is the analysis vocabulary the other sections use, `usable` and
`inconclusive`, and a run whose evidence cannot be read has no verdict.

The listing pages' event name comes from ``flink_tier3.cloudtasks.verdict``,
which the exercise writes them under, so a rename cannot reach only one
side.
"""

from collections import Counter

from ..recovery_analysis import (
    INCOMPLETE_NOTE,
    SINGLE_FAULT_NOTE,
    approval_checks,
    approval_trial,
    assess_sequence,
    fault_column,
    judged,
    unreadable,
    verdict_problems,
)
from ..verdict import sequence_verdict
from .oracle import assess as assess_pages
from .trial import Trial
from .verdict import PAGE_EVENT, data_reasons

SCENARIO = "cloudtasks-recovery"
FIELD = "cloudtasks_trial"
PAGE_KEYS = ("ids", "foreign", "dispatched", "mismatched")


def _pages(run, attempt):
    """The listing pages one attempt exported, by index, and how many are malformed."""
    pages, malformed = {}, 0
    for entry in run.events_of(PAGE_EVENT):
        payload = entry["payload"]
        if payload.get("attempt") != attempt:
            continue
        index = payload.get("page")
        if (
            type(index) is not int
            or index in pages
            or not all(
                isinstance(payload.get(key), list)
                and all(isinstance(item, str) for item in payload[key])
                for key in PAGE_KEYS
            )
            or not _mismatched_are_listed(payload)
        ):
            malformed += 1
            continue
        pages[index] = {key: payload[key] for key in PAGE_KEYS}
    return pages, malformed


def _mismatched_are_listed(payload):
    """Whether each id the page reports a wrong body for is one it lists.

    The supervisor names a wrong body only for a task on the same page, so
    an id the page does not list, or lists fewer times, is a damaged export.
    """
    return Counter(payload["mismatched"]) <= Counter(payload["ids"])


# A run without a verdict has no rebuilt report.
BLANK = {"report": None}


def assess(run):
    """One Cloud Tasks run's verdict, beside the faults it injected (ADR-0204).

    A completed run is judged from the pages of the listing attempt its record
    names, recomputing the oracle report rather than reading the stored one; a
    page missing from the export, or a completed record without a listing,
    leaves the run without a verdict. A page carries the ids whose body the
    supervisor found wrong, not the bodies, so that check is re-counted, not
    re-run.
    """
    return assess_sequence(
        run,
        _decide,
        **approval_checks(FIELD, Trial),
        blank=BLANK,
    )


def _decide(run, record, complete):
    if not complete:
        # Decided by the data alone, so the record is judged as it stands.
        decided = sequence_verdict(record, data_reasons)
        return judged(
            run, record, decided, verdict_problems(record, decided, False), report=None
        )
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    listing = outcomes.get("listing")
    if not isinstance(listing, dict):
        # A completed run is decided by its listing, so a record without one
        # leaves the verdict nothing to be decided from.
        return unreadable(run, record, ["missing-listing-record"])
    attempt, count = listing.get("attempt"), listing.get("pages")
    # The supervisor exports at least one page, an empty queue's included.
    if type(attempt) is not int or type(count) is not int or count < 1:
        return unreadable(run, record, ["malformed-listing-record"])
    pages, malformed = _pages(run, attempt)
    if malformed:
        return unreadable(run, record, [f"malformed-pages:{malformed}"])
    if sorted(pages) != list(range(count)):
        missing = len(set(range(count)) - set(pages))
        extra = len(set(pages) - set(range(count)))
        counts = {"missing-pages": missing, "extra-pages": extra}
        return unreadable(
            run, record, [f"{name}:{n}" for name, n in counts.items() if n]
        )
    report = assess_pages(
        approval_trial(run, FIELD, Trial), [pages[index] for index in range(count)]
    )
    problems = []
    if listing.get("report") != report:
        problems.append("report-differs-from-pages")
    decided = sequence_verdict(
        {
            **record,
            "outcomes": {**outcomes, "listing": {**listing, "report": report}},
        },
        data_reasons,
    )
    problems.extend(verdict_problems(record, decided, True))
    return judged(run, record, decided, problems, report=report)


def assess_runs(runs):
    """Every Cloud Tasks run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def render(assessments):
    """The report's Cloud Tasks section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Cloud Tasks runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the oracle report is "
            "rebuilt from the listing pages the run exported; the queue's paused "
            "state is read from the record."
        ),
        "",
        (
            "| run | verdict | stored | listed | missing | foreign | "
            "duplicates | dispatched | mismatched | faults | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        report = item["report"] if isinstance(item["report"], dict) else {}
        lines.append(
            "| {run} | {verdict} | {stored} | {listed} | {missing} | "
            "{foreign} | {duplicates} | {dispatched} | {mismatched} | {faults} | "
            "{reasons} | {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
                faults=fault_column(item),
                reasons=", ".join(item["reasons"]) or "-",
                problems=", ".join(item["problems"]) or "-",
                **{
                    key: report.get(key, "-")
                    for key in (
                        "listed",
                        "missing",
                        "foreign",
                        "duplicates",
                        "dispatched",
                        "mismatched",
                    )
                },
            )
        )
    lines.extend(
        [
            "",
            (
                "A usable `sequence` verdict (ADR-0204) establishes that every "
                "input sequence became exactly one task in the paused queue through "
                "the faults listed; it makes no claim about a handler, since a "
                "paused queue dispatches nothing. The run is judged by its data "
                "alone."
            ),
            SINGLE_FAULT_NOTE,
            INCOMPLETE_NOTE,
        ]
    )
    return "\n".join(lines) + "\n"
