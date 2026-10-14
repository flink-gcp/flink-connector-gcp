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
same rules to them, kept once so the sections cannot drift. The BigQuery and
Pub/Sub sections share the verdict steps. The timing of each recovery is read
from the record of the shared ``RecoveryExercise`` as the BigQuery and Cloud
Tasks scenarios run it; the Pub/Sub exercise records its one fault in a shape
of its own, which this reading does not cover.
"""

from .common import Failure, timestamp

# The recoveries the shared exercise proves, in the order it runs them, and
# the fault each one is when its proof names no other. A rescale's proof
# carries its direction, and a TaskManager replacement's the Pod it replaced;
# the names are the Pub/Sub trials' where that scenario has the fault.
RECOVERIES = {"upgrade": "savepoint-upgrade", "failover": "jm-replacement"}
RESCALES = {"out": "rescale-out", "in": "rescale-in"}
# Epoch milliseconds a JSON reader can still divide: 2**53 is in the year
# 287,396, and a larger integer can overflow the report's seconds.
LATEST_MILLISECONDS = 2**53


def measurements(run, measurement_event, complete_event):
    """Every sample the run emitted, and how many of its records are unreadable.

    A payload that is not an object, or a sample whose stage is not a string,
    is lost to the fold; it is counted here, because the rebuilt coverage would
    otherwise lower the verdict for what is only a truncated download.
    """
    readings = run.events_of(measurement_event)
    malformed = sum(
        1
        for entry in run.events
        if entry["event"] in (measurement_event, complete_event)
        and not isinstance(entry["payload"], dict)
    ) + sum(
        1 for entry in readings if not isinstance(entry["payload"].get("stage"), str)
    )
    return readings, malformed


def rebuild(readings, summarize):
    """Coverage folded from the readings themselves, in any order."""
    coverage = {}
    for entry in readings:
        stage = entry["payload"].get("stage")
        if isinstance(stage, str):
            coverage = summarize(coverage, stage, entry["payload"])
    return coverage


def missing_samples(claimed, rebuilt):
    """How many samples the record counted that the export does not hold.

    Each window's attempt count in the record is what the run had read by
    then; fewer readings in the export are a partial download, which would
    lower the rebuilt coverage. A count the record does not carry is not
    compared.
    """
    claimed = claimed if isinstance(claimed, dict) else {}
    missing = 0
    for window, entry in claimed.items():
        attempts = entry.get("attempts") if isinstance(entry, dict) else None
        if type(attempts) is int:
            against = rebuilt.get(window, {}).get("attempts", 0)
            missing += max(0, attempts - against)
    return missing


def unreadable(run, record, problems, samples):
    """A run whose evidence decides no verdict, and the verdict it stored."""
    return {
        "run_id": run.run_id,
        "verdict": None,
        "stored": record.get("verdict") if isinstance(record, dict) else None,
        "reasons": [],
        "problems": sorted(problems),
        "samples": samples,
    }


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


def milliseconds(value):
    """A Flink REST timestamp, or None when the export does not hold one."""
    if type(value) is int and 0 < value < LATEST_MILLISECONDS:
        return value
    return None


def _fault(name, proof):
    """The fault the proof names, or None when it names one unreadably."""
    if name == "failover":
        return "tm-replacement" if "replaced" in proof else RECOVERIES[name]
    if "rescale" not in proof:
        return RECOVERIES[name]
    rescale = proof["rescale"]
    direction = rescale.get("direction") if isinstance(rescale, dict) else None
    # A list or an object would not even be a key to look up.
    return RESCALES.get(direction) if isinstance(direction, str) else None


def _timing(began, proof):
    restored = proof.get("restored")
    checkpoint = proof.get("checkpoint")
    if not isinstance(restored, dict) or not isinstance(checkpoint, dict):
        return None
    restored_at = milliseconds(restored.get("restore_timestamp"))
    acknowledged = milliseconds(checkpoint.get("latest_ack_timestamp"))
    if (
        restored_at is None
        or acknowledged is None
        or type(restored.get("id")) is not int
        or type(checkpoint.get("id")) is not int
        or type(restored.get("is_savepoint")) is not bool
    ):
        return None
    try:
        start = round(timestamp(began) * 1000)
    except Failure:
        return None
    return {
        "began_at": began,
        "restored_from": "savepoint" if restored["is_savepoint"] else "checkpoint",
        "restored_id": restored["id"],
        "restored_ms": restored_at - start,
        "checkpoint_id": checkpoint["id"],
        "checkpoint_ms": acknowledged - start,
    }


def recovery_timings(run, record):
    """How long each recovery the record proved took, by Flink's own account.

    Each is timed from the run's ``recovery-<name>`` stage record to the
    proof's ``restore_timestamp`` and to the last acknowledgement of the
    checkpoint that proved it, in milliseconds; ADR-0165 records what each
    figure bounds. A recovery the record did not prove is absent, and one whose
    fault, proof or single start the export does not hold readably maps to None.
    """
    outcomes = record.get("outcomes") if isinstance(record, dict) else None
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    timings = {}
    for name in RECOVERIES:
        proof = outcomes.get(name)
        if not isinstance(proof, dict):
            continue
        starts = [
            entry["payload"].get("at") for entry in run.events_of("recovery-" + name)
        ]
        fault = _fault(name, proof)
        timing = _timing(starts[0], proof) if len(starts) == 1 else None
        timings[name] = (
            None if fault is None or timing is None else {"fault": fault, **timing}
        )
    return timings


def render_timings(assessments):
    """A report table of each run's recovery timings, or nothing without one."""
    rows = [
        (item["run_id"], name, timing)
        for item in assessments
        for name, timing in (item.get("recoveries") or {}).items()
    ]
    if not rows:
        return []
    lines = [
        "",
        (
            "Each recovery is timed from the supervisor's record of its stage, "
            "just before the upgrade patch or the Pod deletion. `restored after` "
            "is Flink's restore of the checkpointed state, which precedes the "
            "tasks' deployment; `acknowledged after` is the last acknowledgement "
            "of the checkpoint that proved the restored job, which follows its "
            "first input past the fault."
        ),
        "",
        (
            "| run | recovery | fault | began | restored from | restored after "
            "| proving checkpoint | acknowledged after |"
        ),
        "| --- | --- | --- | --- | --- | ---: | --- | ---: |",
    ]
    for run_id, name, timing in rows:
        if timing is None:
            lines.append(f"| {run_id} | {name} | unreadable | - | - | - | - | - |")
            continue
        lines.append(
            f"| {run_id} | {name} | {timing['fault']} | {timing['began_at']} | "
            f"{timing['restored_from']} {timing['restored_id']} | "
            f"{timing['restored_ms'] / 1000:.1f} s | chk-{timing['checkpoint_id']} | "
            f"{timing['checkpoint_ms'] / 1000:.1f} s |"
        )
    return lines
