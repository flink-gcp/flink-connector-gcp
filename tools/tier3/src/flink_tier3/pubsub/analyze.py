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

The supervisor decided the verdict and the receipt carries it; this decides it
again from what the evidence can show on its own, the way the BigQuery
section does. The output oracle is rebuilt from every saved pull response and
the input identities from the runner's publication receipts. A fault-sequence
run (ADR-0204) is decided by the rebuilt oracle and the receipts' check of
every output alone, and lists the faults it injected.

It imports nothing from ``analyze``, which calls it.
"""

import json
import re

from ..common import Failure
from ..exercise import SEQUENCE_KIND
from ..recovery_analysis import (
    PUBSUB_SINGLE_FAULT_KINDS,
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
    malformed_records,
    recovery_record,
    unreadable,
    verdict_problems,
)
from .messages import output_line
from .oracle import COUNTERS, input_ids, published_ids, reconcile
from .output import RETRIED_EVENT, parse
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    sequence_verdict,
)

SCENARIO = "pubsub-recovery"
# The domain `cohort_ranges` accepts, so an unreadable approval leaves one run
# without a verdict rather than aborting the directory's analysis.
MIN_RECORDS, MAX_RECORDS = 3, 10000
BATCH = re.compile(r"(out-[0-9a-f]{8})-([0-9]{6})")


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
    return records, nonce


def _published(directory, records):
    """Each logical input's published IDs, and the publication documents that fail.

    A publication's path names its input index and interval, and the publish
    API binds returned IDs to request order, so the receipt alone maps each ID
    to its input. An intent without a receipt is an unknown outcome.
    """
    receipts, malformed = [], 0
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
        ids = response.get("messageIds") if isinstance(response, dict) else None
        if (
            not isinstance(ids, list)
            or len(ids) != int(count)
            or not all(isinstance(value, str) and value for value in ids)
        ):
            malformed += 1
            continue
        receipts.append((int(index), int(start), ids))
    return published_ids(receipts), malformed


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


def _kind(approval):
    """The approved trial's kind, or None when the approval holds no trial."""
    trial = approval.get("pubsub_trial")
    return trial.get("trial") if isinstance(trial, dict) else None


def _retried(run, record):
    """The batches a refused pull left without observations.

    Each is named by a `pubsub-pull-retried` event when it happens, so a run
    that stopped before its record listed them still names them; a completed
    record lists them too.
    """
    batches = [
        (event.get("payload") or {}).get("batch")
        for event in run.events_of(RETRIED_EVENT)
    ]
    listed = record.get("retried_pulls")
    batches += listed if isinstance(listed, list) else []
    return {
        (match.group(1), int(match.group(2)))
        for batch in batches
        if isinstance(batch, str) and (match := BATCH.fullmatch(batch))
    }


def _collected(directory, retried=frozenset()):
    """Every collected line in batch order, what fails, and what is lost.

    Batch IDs carry the collector's prefix and a zero-padded counter from 1,
    so their sort is the order the supervisor collected them in and a gap in a
    counter is a batch the download lost. A batch is the collector's once its
    `observations.json` was saved; its lines are derived again from the saved
    pull response, as the collector derived them, so that file's content is
    not read. A batch whose pull was refused holds no observations, and is
    not lost.
    """
    lines, malformed, counters = [], 0, {}
    for prefix, number in retried:
        counters.setdefault(prefix, set()).add(number)
    for path in sorted((directory / "output").glob("*/observations.json")):
        match = BATCH.fullmatch(path.parent.name)
        response = _read(path.parent / "response.json")
        received = (
            response.get("receivedMessages", []) if isinstance(response, dict) else None
        )
        derived = _derived(received) if isinstance(received, list) else None
        if match is None or derived is None:
            malformed += 1
            continue
        counters.setdefault(match.group(1), set()).add(int(match.group(2)))
        lines.extend(derived)
    lost = sum(max(numbers) - len(numbers) for numbers in counters.values())
    return lines, malformed, lost


def _observations(lines, run_id, records):
    """The parsed observations of this run."""
    return [item for line in lines if (item := parse(line, run_id, records))]


def _missing(record, lines, complete):
    """How many collected lines the export is short of the record's own count."""
    observed = record.get("observed") if complete else None
    counted = (
        [observed.get("observations"), observed.get("foreign")]
        if isinstance(observed, dict)
        else None
    )
    if counted is None or not all(type(value) is int for value in counted):
        return 0
    return max(0, sum(counted) - len(lines))


def _unreadable(run, record, problems):
    return {
        **unreadable(run, record, problems),
        "oracle": None,
        "extra_publications": None,
        "faults": None,
    }


def assess(run):
    """One Pub/Sub run's recomputed verdict, its stored one and its problems.

    A run whose evidence lacks what the verdict is decided from has no
    verdict: a partial download would otherwise lower it for what is only
    missing from the export. A run that never completed exported its account
    of itself, which says so, and is `inconclusive`. A run of a single-fault
    kind has no verdict here.
    """
    approval = run.approval if isinstance(run.approval, dict) else {}
    identity = _identity(approval)
    malformed = malformed_records(run, COMPLETE_EVENT)
    result = run.result if isinstance(run.result, dict) else None
    kind = _kind(approval)
    if kind in PUBSUB_SINGLE_FAULT_KINDS:
        record = recovery_record(run, COMPLETE_EVENT)
        return _unreadable(run, record, [SINGLE_FAULT])
    if identity is None or kind != SEQUENCE_KIND:
        return _unreadable(run, None, ["approval-without-pubsub-trial"])
    if result is None:
        return _unreadable(run, None, ["missing-result.json"])
    completions = run.events_of(COMPLETE_EVENT)
    complete = bool(completions)
    record = completions[-1]["payload"] if complete else result.get("recovery")
    if malformed:
        return _unreadable(run, record, [f"malformed-evidence:{malformed}"])
    if not isinstance(record, dict):
        return _unreadable(run, None, ["missing-" + COMPLETE_EVENT])
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return _unreadable(run, record, ["missing-" + COMPLETE_EVENT])
    records, nonce = identity
    directory = run.directory / "pubsub" / "messages" / nonce
    published, bad_inputs = _published(directory, records)
    lines, bad_batches, lost = _collected(directory, _retried(run, record))
    if bad_inputs or bad_batches:
        return _unreadable(
            run,
            record,
            [f"malformed-message-evidence:{bad_inputs + bad_batches}"],
        )
    problems = []
    if lost:
        problems.append(f"missing-output-batches:{lost}")
    # A batch lost from the end leaves no gap in a counter; the record's own
    # count of what it collected shows it.
    if missing := _missing(record, lines, complete):
        problems.append(f"missing-output-lines:{missing}")
    observations = _observations(lines, run.run_id, records)
    unreceipted = {
        (item["input_index"], item["sequence"])
        for item in observations
        if (item["input_index"], item["sequence"]) not in published
    }
    if complete and unreceipted:
        problems.append(f"missing-input-receipts:{len(unreceipted)}")
    if problems:
        return _unreadable(run, record, problems)
    # The publication receipts bind each returned ID to its input, so an
    # output naming an input under an ID no receipt holds was not published by
    # this run.
    # An unfinished run can hold an output whose publication response was
    # lost, which is an unknown outcome rather than an unpublished input.
    inputs = input_ids(observations, published)
    oracle = reconcile(lines, run.run_id, records)
    # The verdict itself checks every output's input ID against the receipts,
    # as the supervisor did.
    decided = sequence_verdict({**record, "oracle": oracle, "inputs": inputs})
    problems.extend(verdict_problems(record, decided, complete))
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(problems),
        "oracle": oracle,
        "extra_publications": sum(len(ids) - 1 for ids in published.values()),
        "faults": _faults(record),
    }


def _faults(record):
    """The kinds of the faults a sequence run's record says it injected, in order."""
    outcomes = record.get("outcomes")
    faults = outcomes.get("faults") if isinstance(outcomes, dict) else None
    return [
        fault["kind"]
        for fault in (faults if isinstance(faults, list) else ())
        if isinstance(fault, dict) and isinstance(fault.get("kind"), str)
    ]


def assess_runs(runs):
    """Every Pub/Sub run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def _counters(oracle):
    if not isinstance(oracle, dict) or oracle.get("rejected") is not None:
        return "-"
    return " / ".join(str(oracle.get(name, "-")) for name in COUNTERS[1:5])


def render(assessments):
    """The report's Pub/Sub section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Pub/Sub runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the output oracle "
            "and the input identities are rebuilt; the faults are read from the "
            "record."
        ),
        "",
        (
            "| run | verdict | stored | logical inputs | duplicates | "
            "covered redeliveries | extra publications | faults | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracle = item["oracle"] if isinstance(item["oracle"], dict) else {}
        extra = item["extra_publications"]
        lines.append(
            "| {run} | {verdict} | {stored} | {inputs} | {duplicates} | "
            "{covered} | {extra} | {faults} | {reasons} | {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
                inputs=oracle.get("logical_inputs", "-"),
                duplicates=_counters(oracle),
                covered=(
                    "-"
                    if oracle.get("rejected") is not None
                    else oracle.get("covered_redeliveries", "-")
                ),
                extra="-" if extra is None else extra,
                faults=", ".join(item.get("faults") or ()) or "-",
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
                "delivery, as the output oracle counts them. Covered redeliveries "
                "are input messages a restored attempt processed again although "
                "an earlier run had processed them before a checkpoint barrier "
                "in the restored checkpoint's lineage, which covered them; restore "
                "deduplication exists to suppress them. The count is an upper "
                "bound where the job restored one checkpoint twice and the "
                "second restore removed a subtask. Extra "
                "publications "
                "counts the runner's own receipts beyond one per input. They are "
                "reported apart from the verdict and decide nothing."
            ),
            (
                "A usable verdict establishes completeness through the faults "
                "listed, not ordering and not exactly-once output: unique message "
                "IDs and a deduplicated count cannot show either."
            ),
            (
                "A `sequence` run (ADR-0204) is judged by its output alone: the "
                "oracle, and every output's input message ID against the "
                "publication receipts."
            ),
            SINGLE_FAULT_NOTE,
            (
                f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which "
                "is inconclusive rather than an evidence problem."
            ),
        ]
    )
    return "\n".join(lines) + "\n"
