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
"""What the deployed-run sections share when they read exported evidence.

Each section rebuilds what its own evidence can show, and `assess_sequence`
decides every section's verdict from it by the same steps, through the
scenario's own `data_reasons`. These are the steps that read the same
exported shapes and apply the same rules to them, kept once so the sections
cannot drift. Every event name and stage they look for comes from
``flink_tier3.verdict``, which the exercise writes from, so a rename cannot
reach only one side and leave a section reporting missing evidence.
"""

from .verdict import COMPLETE_EVENT, COMPLETE_STAGE, sequence_verdict

# The problem a run of a single-fault kind is reported with. Every trial now
# runs the fault sequence (ADR-0204), so such a run is evidence archived from
# an earlier revision, which only that revision's rules can judge.
SINGLE_FAULT = "single-fault-trial"
# The sentence each section's prose says that with.
SINGLE_FAULT_NOTE = (
    f"A single-fault run (ADR-0165) has no verdict here and is reported as "
    f"`{SINGLE_FAULT}`; it is analysed at the revision its approval's `sha` names."
)
# The kinds a single-fault trial named before the fault sequence replaced them.
# Any other kind is a malformed trial, not an archived run.
SINGLE_FAULT_KINDS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")
# Pub/Sub's, which add the kinds its restore-deduplication trials named under
# the reactive and adaptive schedulers.
PUBSUB_SINGLE_FAULT_KINDS = (
    *SINGLE_FAULT_KINDS,
    "reactive-scale-out",
    "reactive-scale-in",
    "reactive-tm-eviction",
    "reactive-tm-kill",
    "adaptive-scale-in",
)
# The faults a BigQuery single-fault trial's receipt named. Its trials named a
# fault rather than a kind, and a JobManager replacement, the default, named
# none, so `jm-replacement` never appeared.
BIGQUERY_SINGLE_FAULTS = ("tm-replacement", "rescale-out", "rescale-in")
# The sentence each section's prose says of a run that did not complete.
INCOMPLETE_NOTE = (
    f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which is "
    "inconclusive rather than an evidence problem."
)


def malformed_records(run):
    """How many of the run's completed records are not objects.

    Such a record is counted rather than skipped, so that a truncated download
    is reported as one rather than read.
    """
    return sum(
        1
        for entry in run.events
        if entry["event"] == COMPLETE_EVENT and not isinstance(entry["payload"], dict)
    )


def unreadable(run, record, problems):
    """A run whose evidence decides no verdict, and the verdict it stored."""
    return {
        "run_id": run.run_id,
        "verdict": None,
        "stored": record.get("verdict") if isinstance(record, dict) else None,
        "reasons": [],
        "problems": sorted(problems),
    }


def names_single_fault(run, field):
    """Whether the approval's trial under `field` names a single-fault kind.

    The trial refuses such a kind, so this tells an archived single-fault run
    apart from a trial that is missing or malformed, which a kind-less trial
    of the first runs also is.
    """
    approval = run.approval if isinstance(run.approval, dict) else {}
    trial = approval.get(field)
    return isinstance(trial, dict) and trial.get("kind") in SINGLE_FAULT_KINDS


def approval_checks(field, build):
    """The `archived` and `trial_problem` checks of a trial the approval holds.

    The trial is the approval's under `field`, as `build` makes it. A run is
    archived when that trial names a single-fault kind, and its trial is a
    problem, named after `field`, when it is missing or `build` refuses it.
    """
    problem = "approval-without-" + field.replace("_", "-")

    def archived(run):
        return names_single_fault(run, field)

    def trial_problem(run):
        return problem if approval_trial(run, field, build) is None else None

    return {"archived": archived, "trial_problem": trial_problem}


def approval_trial(run, field, build):
    """The approval's trial under `field`, as `build` makes it, or None.

    None stands for a trial that is missing, or that `build` refuses as
    malformed.
    """
    approval = run.approval if isinstance(run.approval, dict) else {}
    value = approval.get(field)
    if not isinstance(value, dict):
        return None
    try:
        return build(run.run_id, **value)
    except (TypeError, ValueError):
        return None


def verdict_problems(record, decided, complete):
    """Whether the verdict the run stored is the one its evidence recomputes to.

    A run that stored none and did not complete has nothing to compare; a
    completed record without one differs.
    """
    stored, reasons = record.get("verdict"), record.get("reasons")
    if (stored is not None or complete) and (
        stored != decided["verdict"]
        or (reasons if isinstance(reasons, list) else None) != decided["reasons"]
    ):
        return ["verdict-differs-from-the-record"]
    return []


def counts(assessments):
    """How many runs carry each verdict, for the analyzer's summary line.

    A run whose evidence decides no verdict is counted as `unreadable`.
    """
    table = {}
    for item in assessments:
        name = item["verdict"] or "unreadable"
        table[name] = table.get(name, 0) + 1
    return ", ".join(f"{name} {number}" for name, number in sorted(table.items()))


def recovery_record(run):
    """The run's account of its recoveries: the completed record, else the receipt's."""
    completions = run.events_of(COMPLETE_EVENT)
    if completions:
        return completions[-1]["payload"]
    return run.result.get("recovery") if isinstance(run.result, dict) else None


def judged(run, record, decided, problems, **details):
    """A run whose evidence decided its verdict, beside the one it stored."""
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(problems),
        **details,
    }


def by_record(data_reasons, details=None):
    """The decision of a section whose record holds everything its verdict reads.

    The verdict is recomputed from the record by the scenario's
    `data_reasons`, and `details` adds what the section reports beside it.
    """

    def decide(run, record, complete):
        decided = sequence_verdict(record, data_reasons)
        return judged(
            run,
            record,
            decided,
            verdict_problems(record, decided, complete),
            **(details(run, record) if details else {}),
        )

    return decide


def recorded_oracles(run, record):
    """The oracle reports the record holds, which the section shows."""
    outcomes = record.get("outcomes")
    oracles = outcomes.get("oracles") if isinstance(outcomes, dict) else None
    return {"oracles": oracles if isinstance(oracles, dict) else None}


def oracle_figure(oracles, component, *keys):
    """One value of a component's oracle report, or `-` when it is not a figure.

    `keys` lead into the report, one nesting level each. A value that is
    missing, null, or an object or a list, which only a damaged export holds
    where a figure belongs, renders as `-`.
    """
    value = oracles.get(component) if isinstance(oracles, dict) else None
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return "-" if value is None or isinstance(value, (dict, list)) else value


def fault_kinds(faults):
    """The kinds of the faults a record says it injected, in order."""
    return [
        fault["kind"]
        for fault in (faults if isinstance(faults, list) else ())
        if isinstance(fault, dict) and isinstance(fault.get("kind"), str)
    ]


def fault_column(item):
    """A run's injected faults in order, or `-` when it names none."""
    return ", ".join(fault_kinds(item.get("faults"))) or "-"


def assess_sequence(
    run, decide, *, archived, trial_problem, malformed_stored=False, blank=None
):
    """One fault-sequence run's assessment, beside the faults its record names.

    A run whose evidence lacks what the verdict is decided from, or holds a
    record nothing can read, has no verdict; re-export it and read it again.
    The checks run in order: a run of a single-fault kind, which `archived`
    names, has none here; then `trial_problem` names why the trial cannot be
    read, if it cannot; then the receipt must be present, no completed record
    may be malformed, and the run's record must be readable. A run that never
    completed exported its account of itself, which says so, and is decided
    by `decide(run, record, complete)` as a completed one is.

    `malformed_stored` keeps the stored verdict beside a malformed record,
    which preserves each section's output from before the sections shared
    this: BigQuery's and Pub/Sub's showed it, the others `-`. `blank` holds
    the section's own fields of a run without a verdict, as defaults that an
    assessment naming them overrides. The faults are the record's unless the
    assessment names them itself.
    """
    record = recovery_record(run)
    malformed = malformed_records(run)
    complete = bool(run.events_of(COMPLETE_EVENT))
    if archived(run):
        assessment = unreadable(run, record, [SINGLE_FAULT])
    elif (problem := trial_problem(run)) is not None:
        assessment = unreadable(run, None, [problem])
    elif not isinstance(run.result, dict):
        assessment = unreadable(run, None, ["missing-result.json"])
    elif malformed:
        assessment = unreadable(
            run,
            record if malformed_stored else None,
            [f"malformed-evidence:{malformed}"],
        )
    elif not isinstance(record, dict):
        assessment = unreadable(run, None, ["missing-" + COMPLETE_EVENT])
    # A record that says it completed, with no completion evidence beside it,
    # is an export missing a file.
    elif not complete and record.get("stage") == COMPLETE_STAGE:
        assessment = unreadable(run, record, ["missing-" + COMPLETE_EVENT])
    else:
        assessment = decide(run, record, complete)
    outcomes = record.get("outcomes") if isinstance(record, dict) else None
    faults = outcomes.get("faults") if isinstance(outcomes, dict) else None
    return {
        "faults": faults if isinstance(faults, list) else [],
        **(blank or {}),
        **assessment,
    }
