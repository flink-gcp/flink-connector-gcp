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
it again from those pages and the record, so a reader holding only the
downloaded evidence directory can check the stored verdict against them. The
vocabulary is the analysis vocabulary the other sections use, `usable` and
`inconclusive`, and a run whose evidence cannot be read has no verdict.

Every event name and stage comes from ``flink_tier3.cloudtasks.verdict``,
which the exercise writes from, so a rename cannot reach only one side.
"""

from ..recovery_analysis import unreadable, verdict_problems
from .oracle import assess as assess_pages
from .trial import Trial
from .verdict import COMPLETE_EVENT, COMPLETE_STAGE, PAGE_EVENT, verdict

SCENARIO = "cloudtasks-recovery"
PAGE_KEYS = ("ids", "foreign", "dispatched")


def _trial(run):
    approval = run.approval if isinstance(run.approval, dict) else {}
    value = approval.get("cloudtasks_trial")
    if not isinstance(value, dict):
        return None
    try:
        return Trial(run.run_id, **value)
    except (TypeError, ValueError):
        return None


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
        ):
            malformed += 1
            continue
        pages[index] = {key: payload[key] for key in PAGE_KEYS}
    return pages, malformed


def _result(run, record, decided, problems, report):
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(problems),
        "report": report,
    }


def _unreadable(run, record, problems):
    # A Cloud Tasks run takes no measurement samples.
    value = {**unreadable(run, record, problems, 0), "report": None}
    del value["samples"]
    return value


def assess(run):
    """One Cloud Tasks run's recomputed verdict, its stored one and its problems.

    A completed run is judged from the pages of the listing attempt its record
    names, recomputing the oracle report rather than reading the stored one; a
    page missing from the export leaves the run without a verdict. A run that
    never completed exported its account of itself, which says so, and is
    `inconclusive`.
    """
    trial = _trial(run)
    result = run.result if isinstance(run.result, dict) else None
    if trial is None:
        return _unreadable(run, None, ["approval-without-cloudtasks-trial"])
    if result is None:
        return _unreadable(run, None, ["missing-result.json"])
    completions = [entry for entry in run.events if entry["event"] == COMPLETE_EVENT]
    if any(not isinstance(entry["payload"], dict) for entry in completions):
        return _unreadable(run, None, ["malformed-evidence:" + COMPLETE_EVENT])
    complete = bool(completions)
    record = completions[-1]["payload"] if complete else result.get("recovery")
    if not isinstance(record, dict):
        return _unreadable(run, None, ["missing-" + COMPLETE_EVENT])
    if not complete:
        if record.get("stage") == COMPLETE_STAGE:
            return _unreadable(run, record, ["missing-" + COMPLETE_EVENT])
        decided = verdict(record)
        return _result(
            run, record, decided, verdict_problems(record, decided, False), None
        )
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    listing = outcomes.get("listing")
    if not isinstance(listing, dict):
        decided = verdict(record)
        return _result(
            run, record, decided, verdict_problems(record, decided, True), None
        )
    attempt, count = listing.get("attempt"), listing.get("pages")
    if type(attempt) is not int or type(count) is not int:
        return _unreadable(run, record, ["malformed-listing-record"])
    pages, malformed = _pages(run, attempt)
    if malformed:
        return _unreadable(run, record, [f"malformed-pages:{malformed}"])
    if sorted(pages) != list(range(count)):
        missing = len(set(range(count)) - set(pages))
        extra = len(set(pages) - set(range(count)))
        counts = {"missing-pages": missing, "extra-pages": extra}
        return _unreadable(
            run, record, [f"{name}:{n}" for name, n in counts.items() if n]
        )
    report = assess_pages(trial, [pages[index] for index in range(count)])
    problems = []
    if listing.get("report") != report:
        problems.append("report-differs-from-pages")
    decided = verdict(
        {**record, "outcomes": {**outcomes, "listing": {**listing, "report": report}}}
    )
    problems.extend(verdict_problems(record, decided, True))
    return _result(run, record, decided, problems, report)


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
            "rebuilt from the listing pages the run exported; the recoveries and "
            "the queue's paused state are read from the record."
        ),
        "",
        (
            "| run | verdict | stored | listed | missing | foreign | duplicates | "
            "dispatched | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        report = item["report"] if isinstance(item["report"], dict) else {}
        lines.append(
            "| {run} | {verdict} | {stored} | {listed} | {missing} | {foreign} | "
            "{duplicates} | {dispatched} | {reasons} | {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
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
                    )
                },
            )
        )
    lines.extend(
        [
            "",
            (
                "A usable verdict establishes that every input sequence became "
                "exactly one task in the paused queue through both recoveries; it "
                "makes no claim about a handler, since a paused queue dispatches "
                "nothing."
            ),
            (
                f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which "
                "is inconclusive rather than an evidence problem."
            ),
        ]
    )
    return "\n".join(lines) + "\n"
