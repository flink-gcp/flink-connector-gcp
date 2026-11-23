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

Each section rebuilds what its own evidence can show and decides its own
verdict; these are the steps that read the same exported shapes and apply the
same rules to them, kept once so the sections cannot drift.
"""

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


def malformed_records(run, complete_event):
    """How many of the run's completed records are not objects.

    Such a record is counted rather than skipped, so that a truncated download
    is reported as one rather than read.
    """
    return sum(
        1
        for entry in run.events
        if entry["event"] == complete_event and not isinstance(entry["payload"], dict)
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


def single_fault(run, record):
    """A run of a single-fault kind: no verdict, and the one it stored."""
    return unreadable(run, record, [SINGLE_FAULT])


def names_single_fault(run, field):
    """Whether the approval's trial under `field` names a single-fault kind.

    The trial refuses such a kind, so this tells an archived single-fault run
    apart from a trial that is missing or malformed, which a kind-less trial
    of the first runs also is.
    """
    approval = run.approval if isinstance(run.approval, dict) else {}
    trial = approval.get(field)
    return isinstance(trial, dict) and trial.get("kind") in SINGLE_FAULT_KINDS


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


def recovery_record(run, complete_event):
    """The run's account of its recoveries: the completed record, else the receipt's."""
    completions = run.events_of(complete_event)
    if completions:
        return completions[-1]["payload"]
    return run.result.get("recovery") if isinstance(run.result, dict) else None
