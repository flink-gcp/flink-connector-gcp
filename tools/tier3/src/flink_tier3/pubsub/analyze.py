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
"""Recompute a deployed Pub/Sub recovery run's verdict from its exported evidence.

The supervisor decided the verdict and the receipt carries it; this rebuilds
what the evidence can show on its own and asks whether the record agrees, the
way the BigQuery section does. The output oracle is rebuilt from every
collected batch, the input identities from the runner's publication receipts,
the observation coverage from the measurement events, and the replay cohort's
output from the attempts the fault started from the observations themselves.
Whether the boundary held and which observations were collected before the
fault are read from the record, held to the earlier transitions that wrote
the same outcomes and to what the observations say about the attempts it
names; rebuilding them outright would re-run the exercise offline.

It imports nothing from ``analyze``, which calls it.
"""

import json
import re

from ..common import INCONCLUSIVE, UNEXPORTED, USABLE, Failure
from ..recovery_analysis import (
    accounted,
    measurements,
    rebuild,
    receipt_problems,
    status,
    verdict_problems,
)
from .messages import COHORTS, cohort_ranges, output_line
from .oracle import COUNTERS, reconcile
from .output import parse
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    RESCALES,
    oracle_reasons,
    request_reasons,
    started_by_fault,
    summarize,
    verdict,
)

SCENARIO = "pubsub-recovery"
REPLAY = COHORTS[1]
# The domain `cohort_ranges` accepts, so a forged approval excludes one run
# rather than aborting the directory's analysis.
MIN_RECORDS, MAX_RECORDS = 3, 10000
BATCH = re.compile(r"(out-[0-9a-f]{8})-([0-9]{6})")
# The problems that are a record claiming more than its evidence shows. Every
# other problem is a disagreement that asserts nothing in the record's favour,
# so it is inconsistent rather than forged; under-reporting is not tampering.
OVERSTATEMENTS = (
    "coverage-unsupported-by-its-readings",
    "oracle-overstates-its-evidence",
    "replay-understated",
    "replay-overstated",
    "outcomes-differ-from-earlier-records",
    "fault-unreadable",
    "fault-names-a-later-attempt",
    "fault-kind-differs-from-the-approval",
    "observed-unreadable",
    "observations-differ-from-pull-response",
    "replay-ids-overstated",
    "verdict-unsupported-by-its-inputs",
    "receipt-success-mismatch",
)


def _read(path):
    """A JSON object, None when absent, or the string "malformed"."""
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return "malformed"
    return value if isinstance(value, dict) else "malformed"


def _identity(approval):
    trial = approval.get("pubsub_trial")
    trial = trial if isinstance(trial, dict) else {}
    records = trial.get("records_per_subscription")
    nonce = approval.get("nonce")
    if (
        type(records) is not int
        or not MIN_RECORDS <= records <= MAX_RECORDS
        or not isinstance(nonce, str)
    ):
        return None
    return records, nonce, trial.get("trial")


def _published(directory, records):
    """Each logical input's published IDs, and the publication documents that fail.

    A publication's path names its input index and interval, and the publish
    API binds returned IDs to request order, so the receipt alone maps each ID
    to its input. An intent without a receipt is an unknown outcome.
    """
    published, malformed = {}, 0
    for path in sorted((directory / "input").glob("*/*/intent.json")):
        index, interval = path.parent.parent.name, path.parent.name
        start, _, count = interval.partition("-")
        if (
            index not in ("0", "1")
            or not start.isdecimal()
            or not count.isdecimal()
            or not 0 < int(count)
            or int(start) + int(count) > records
        ):
            malformed += 1
            continue
        response = _read(path.parent / "response.json")
        if response is None:
            continue
        span = range(int(start), int(start) + int(count))
        ids = response.get("messageIds") if isinstance(response, dict) else None
        if (
            not isinstance(ids, list)
            or len(ids) != len(span)
            or not all(isinstance(value, str) and value for value in ids)
        ):
            malformed += 1
            continue
        for sequence, message_id in zip(span, ids, strict=True):
            published.setdefault((int(index), sequence), []).append(message_id)
    return published, malformed


def _derived(received):
    """The collector's lines for a pull response's messages, or None."""
    lines = []
    for item in received:
        message = item.get("message") if isinstance(item, dict) else None
        if not isinstance(message, dict):
            return None
        try:
            lines.append(output_line(message))
        except Failure:
            return None
    return lines


def _collected(directory):
    """Every collected line in batch order, and what fails, is lost or differs.

    Batch IDs carry the collector's prefix and a zero-padded counter from 1,
    so their sort is the order the supervisor collected them in and a gap in a
    counter is a batch the download lost. The lines are derived again from
    each saved pull response, as the collector derived them, so an edited
    `observations.json` cannot change what the oracle reads.
    """
    lines, malformed, counters, differing = [], 0, {}, 0
    for path in sorted((directory / "output").glob("*/observations.json")):
        match = BATCH.fullmatch(path.parent.name)
        value = _read(path)
        tsv = value.get("tsv") if isinstance(value, dict) else None
        response = _read(path.parent / "response.json")
        received = (
            response.get("receivedMessages", []) if isinstance(response, dict) else None
        )
        derived = _derived(received) if isinstance(received, list) else None
        if (
            match is None
            or not isinstance(tsv, str)
            or value.get("count") != len(tsv.splitlines())
            or derived is None
        ):
            malformed += 1
            continue
        if derived != tsv.splitlines():
            differing += 1
        counters.setdefault(match.group(1), []).append(int(match.group(2)))
        lines.extend(derived)
    lost = sum(max(numbers) - len(numbers) for numbers in counters.values())
    return lines, malformed, lost, differing


def _attempts(lines, run_id, records):
    """The parsed observations, and each attempt's phase and restoration states."""
    observations = [item for line in lines if (item := parse(line, run_id, records))]
    states = {}
    for item in observations:
        states.setdefault(item["attempt"], set()).add((item["phase"], item["restored"]))
    return observations, states


def _fault_problems(fault, states, approved):
    """What the record's fault claims that its evidence or its approval denies."""
    if not isinstance(fault, dict):
        return []
    before = fault.get("before")
    expected = fault.get("expected_replay")
    if (
        not isinstance(before, list)
        or not all(isinstance(a, str) for a in before)
        or not isinstance(expected, list)
        or not all(
            isinstance(identity, list)
            and len(identity) == 2
            and all(type(part) is int for part in identity)
            for identity in expected
        )
    ):
        return ["fault-unreadable"]
    problems = []
    if fault.get("kind") != approved:
        problems.append("fault-kind-differs-from-the-approval")
    # Every attempt before the fault is the initial job's and restored nothing;
    # a restored or upgraded one named here hides a replay from the rebuild.
    if any(
        state != ("initial", False)
        for attempt in before
        for state in states.get(attempt, ())
    ):
        problems.append("fault-names-a-later-attempt")
    return problems


def _replay_problems(outcomes, observations, records):
    """The record's replay counts against what the observations hold."""
    fault, recovery, after = (outcomes.get(k) for k in ("fault", "recovery", "after"))
    if not all(isinstance(value, dict) for value in (fault, recovery, after)):
        return [], outcomes
    cohort = cohort_ranges(records)[REPLAY]
    expected = {tuple(identity) for identity in fault["expected_replay"]}
    seen = {
        (item["input_index"], item["sequence"])
        for item in observations
        if cohort["start"] <= item["sequence"] < cohort["start"] + cohort["count"]
        and started_by_fault(
            item["attempt"],
            item["restored"],
            item["phase"],
            fault["before"],
            fault.get("kind"),
        )
    }
    problems = []
    # Recovery counted the expected replay at its first chance, so it can have
    # seen no more of it than the run's whole evidence holds.
    replayed = recovery.get("replayed")
    if recovery.get("expected_replay") != len(expected) or (
        type(replayed) is int and replayed > len(seen & expected)
    ):
        problems.append("replay-overstated")
    # The completion record asked whether each replay came under an ID the
    # input was processed under before the fault, collected by then; the
    # export keeps no collection time, so this asks the looser question, any
    # ID a pre-fault attempt processed at all. Only the completion's claim is
    # asked: the whole export is the evidence it was made over, while the
    # recovery's was made before later replays arrived.
    original = {}
    for item in observations:
        if item["attempt"] in fault["before"]:
            identity = (item["input_index"], item["sequence"])
            original.setdefault(identity, set()).add(item["input_message_id"])
    preserved = all(
        item["input_message_id"]
        in original.get((item["input_index"], item["sequence"]), ())
        for item in observations
        if (item["input_index"], item["sequence"]) in expected
        and started_by_fault(
            item["attempt"],
            item["restored"],
            item["phase"],
            fault["before"],
            fault.get("kind"),
        )
    )
    # Matched by identity: `1 == True` in Python.
    if not preserved and after.get("replay_ids_preserved") is True:
        problems.append("replay-ids-overstated")
    claimed = after.get("replay_by_new_attempts")
    if claimed != len(seen):
        # A rescale's verdict reads this count, so claiming fewer is the
        # overstatement; a replacement's does not, so any difference only
        # disagrees.
        problems.append(
            "replay-understated"
            if fault.get("kind") in RESCALES
            and type(claimed) is int
            and claimed < len(seen)
            else "replay-disagrees-with-its-evidence"
        )
    return problems, {
        **outcomes,
        "after": {**after, "replay_by_new_attempts": len(seen)},
    }


def _earlier_outcomes(run, outcomes):
    """Whether every earlier transition wrote the outcomes the record now holds.

    Each `recovery-<stage>` record carries the outcomes gathered so far, so an
    outcome edited only in the final record differs from the one the
    transition after it recorded.
    """
    for entry in run.events:
        if (
            not entry["event"].startswith("recovery-")
            or entry["event"] == COMPLETE_EVENT
        ):
            continue
        payload = entry["payload"]
        earlier = payload.get("outcomes") if isinstance(payload, dict) else None
        if isinstance(earlier, dict) and any(
            outcomes.get(name) != value for name, value in earlier.items()
        ):
            return ["outcomes-differ-from-earlier-records"]
    return []


def _observed_problems(record, lines, complete):
    """The record's count of collected lines against the lines exported.

    Returns the problems and whether the export is short of the record.
    """
    if not complete:
        return [], False
    observed = record.get("observed")
    counted = (
        [observed.get("observations"), observed.get("foreign")]
        if isinstance(observed, dict)
        else None
    )
    if counted is None or not all(type(value) is int for value in counted):
        return ["observed-unreadable"], False
    claimed = sum(counted)
    if len(lines) < claimed:
        return [f"missing-output-batches:{claimed - len(lines)}"], True
    if len(lines) > claimed:
        return [f"output-the-record-does-not-account-for:{len(lines) - claimed}"], False
    return [], False


def _oracle_problems(claimed, rebuilt):
    """How the record's oracle compares with the one its batches rebuild."""
    if claimed == rebuilt:
        return []
    if oracle_reasons(rebuilt) and not oracle_reasons(claimed):
        return ["oracle-overstates-its-evidence"]
    return ["oracle-disagrees-with-its-evidence"]


def _excluded(run, problems, samples, assessed=UNEXPORTED):
    return {
        "run_id": run.run_id,
        "status": assessed,
        "verdict": None,
        "reasons": [],
        "problems": sorted(problems),
        "samples": samples,
        "oracle": None,
        "extra_publications": None,
    }


def assess(run):
    """One Pub/Sub run's status, its recomputed verdict and its problems.

    `unexported` is an evidence set missing what its own record names,
    `tampered` a record claiming more than its evidence shows, and
    `inconsistent` evidence that disagrees with itself; none of them is a
    verdict. A run that never completed exported its account of itself, which
    says so, and is `inconclusive`.
    """
    approval = run.approval if isinstance(run.approval, dict) else {}
    identity = _identity(approval)
    readings, malformed = measurements(run, MEASUREMENT_EVENT, COMPLETE_EVENT)
    result = run.result if isinstance(run.result, dict) else None
    if identity is None:
        return _excluded(run, ["approval-without-pubsub-trial"], len(readings))
    if result is None:
        return _excluded(run, ["missing-result.json"], len(readings))
    if malformed:
        return _excluded(run, [f"malformed-evidence:{malformed}"], len(readings))
    records, nonce, approved = identity
    completions = run.events_of(COMPLETE_EVENT)
    complete = bool(completions)
    record = completions[-1]["payload"] if complete else result.get("recovery")
    if not isinstance(record, dict):
        return _excluded(run, ["missing-" + COMPLETE_EVENT], len(readings))
    directory = run.directory / "pubsub" / "messages" / nonce
    published, bad_inputs = _published(directory, records)
    lines, bad_batches, lost, differing = _collected(directory)
    if bad_inputs or bad_batches:
        return _excluded(
            run,
            [f"malformed-message-evidence:{bad_inputs + bad_batches}"],
            len(readings),
        )
    problems = []
    if differing:
        problems.append("observations-differ-from-pull-response")
    missing = not complete and record.get("stage") == COMPLETE_STAGE
    if missing:
        problems.append("missing-" + COMPLETE_EVENT)
    if len(completions) > 1:
        problems.append(f"repeated-{COMPLETE_EVENT}:{len(completions)}")
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    observations, states = _attempts(lines, run.run_id, records)
    # What does not depend on every batch being present is asked first, so a
    # record that overstates itself is not excused by a partial download.
    coverage = rebuild(readings, summarize)
    problems.extend(accounted(record.get("coverage"), coverage, complete))
    problems.extend(_earlier_outcomes(run, outcomes))
    problems.extend(_fault_problems(outcomes.get("fault"), states, approved))
    counted, short = _observed_problems(record, lines, complete)
    problems.extend(counted)
    if lost:
        short = True
        problems.append(f"missing-output-batches:{lost}")
    unreceipted = {
        (item["input_index"], item["sequence"])
        for item in observations
        if (item["input_index"], item["sequence"]) not in published
    }
    if complete and unreceipted:
        short = True
        problems.append(f"missing-input-receipts:{len(unreceipted)}")
    if short:
        # A partial download: an oracle rebuilt from it would charge the
        # record with what the export lost, so it is not rebuilt. The verdict
        # is still asked over the record's own oracle and outcomes, which can
        # only flatter it: a stored `usable` that even they refuse, or a
        # successful receipt beside it, overstates the record regardless of
        # what the download lost.
        flattered = verdict({**record, "outcomes": outcomes, "coverage": coverage})
        if flattered["verdict"] != USABLE:
            if record.get("verdict") not in (None, INCONCLUSIVE):
                problems.append("verdict-unsupported-by-its-inputs")
            if result.get("success") is True:
                problems.append("receipt-success-mismatch")
        return _excluded(
            run,
            problems,
            len(readings),
            status(problems, OVERSTATEMENTS, True, None),
        )
    unpublished = sum(
        1
        for item in observations
        if (item["input_index"], item["sequence"]) in published
        and item["input_message_id"]
        not in published[(item["input_index"], item["sequence"])]
    )
    if unpublished:
        problems.append(f"unpublished-input-ids:{unpublished}")
    oracle = reconcile(lines, run.run_id, records)
    if "fault-unreadable" not in problems:
        replay, outcomes = _replay_problems(outcomes, observations, records)
        problems.extend(replay)
    if complete or record.get("stage") == COMPLETE_STAGE:
        problems.extend(_oracle_problems(record.get("oracle"), oracle))
    decided = verdict(
        {**record, "outcomes": outcomes, "coverage": coverage, "oracle": oracle}
    )
    problems.extend(
        verdict_problems(
            record, decided, complete, "verdict-disagrees-with-its-evidence"
        )
    )
    # The receipt also withholds success from a run whose request meter
    # stopped it or did not count all of it, whatever the evidence decides.
    limit = approval["pubsub_trial"].get("total_request_limit")
    reasons = decided["reasons"] + request_reasons(result.get("requests"), limit)
    reported = {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
    problems.extend(receipt_problems(result, record, complete, SCENARIO, reported))
    assessed = status(problems, OVERSTATEMENTS, missing, reported["verdict"])
    return {
        "run_id": run.run_id,
        "status": assessed,
        "verdict": None if assessed == UNEXPORTED else reported["verdict"],
        "reasons": reasons,
        "problems": sorted(problems),
        "samples": len(readings),
        "oracle": oracle,
        "extra_publications": sum(len(ids) - 1 for ids in published.values()),
    }


def assess_runs(runs):
    """Every Pub/Sub run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def _counters(oracle):
    if not isinstance(oracle, dict) or oracle.get("rejected") is not None:
        return "-"
    return " / ".join(str(oracle.get(name, "-")) for name in COUNTERS[1:])


def render(assessments):
    """The report's Pub/Sub section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Pub/Sub runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the output oracle, "
            "the input identities, the coverage and the replay cohort's output "
            "from new attempts are rebuilt; whether the boundary held and which "
            "observations preceded the fault are read from the record, held to "
            "its earlier transitions."
        ),
        "",
        (
            "| run | status | verdict | samples | logical inputs | duplicates | "
            "extra publications | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracle = item["oracle"] if isinstance(item["oracle"], dict) else {}
        extra = item["extra_publications"]
        lines.append(
            "| {run} | {status} | {verdict} | {samples} | {inputs} | {duplicates} | "
            "{extra} | {reasons} | {problems} |".format(
                run=item["run_id"],
                status=item["status"],
                verdict=item["verdict"] or "-",
                samples=item["samples"],
                inputs=oracle.get("logical_inputs", "-"),
                duplicates=_counters(oracle),
                extra="-" if extra is None else extra,
                reasons=", ".join(item["reasons"]) or "-",
                problems=", ".join(item["problems"]) or "-",
            )
        )
    lines.extend(
        [
            "",
            (
                "Duplicates are, in order, input publication duplicates, repeated "
                "input processing, output publication duplicates and repeated output "
                "delivery, as the output oracle counts them; extra publications "
                "counts the runner's own receipts beyond one per input. They are "
                "reported apart from the verdict and decide nothing."
            ),
            (
                "A usable verdict establishes completeness and the trial's recovery, "
                "not ordering across the replay and not exactly-once output: unique "
                "message IDs and a deduplicated count cannot show either."
            ),
            (
                f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which "
                "is inconclusive rather than an evidence problem."
            ),
        ]
    )
    return "\n".join(lines) + "\n"
