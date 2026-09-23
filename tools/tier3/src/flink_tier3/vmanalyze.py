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
"""Offline analysis of single-host campaigns under the preregistered rule.

The input is the campaign directories ``vmcampaign`` leaves, collected to the
reviewing host; nothing here contacts a service. What transfers from the
cluster analyzer is the arithmetic and the per-cell reconciliation, not its
evidence contract: ``evidence.Reconciler`` reads this rig's receipts and rows
from a local tree, and ``analyze.build_groups`` applies the thresholds. What is
this module's own is deciding which runs are cells at all.

**The journal is the ledger of record.** A run's state comes from the
campaign's ``state.json``; ``outcome.json`` is corroboration. A run the journal
still calls ``CLAIMED`` was lost, whatever its directory holds: the controller
writes the outcome before the journal's finish, and a controller that died
between the two left an outcome the campaign never accepted. A run whose
outcome, detail or command line disagrees with the journal and plan is
``inconsistent``, and a run whose files no longer match the digests the
controller recorded is ``evidence-modified``. Neither is a measurement.

**The evidence of a run lives inside its directory.** The controller gives
the probe ``vmcampaign.evidence_root`` of the run's probe directory, so the
digests it records cover the rows and receipts and the run moves as one
directory; a command line other than the one the controller builds from the
plan is ``inconsistent``. Each campaign's frozen inputs must name the Flink
line its launcher runs (``flink``) and the protocol it was planned against
(``protocolSha256``).

**The observation window is the preregistered one**, and the rig does not yet
record what it is built from. :func:`checkpoint_completions` is the one place
that knows where checkpoint completions come from, and
:func:`observation_window` the one place that applies the rule; the
preregistration revision (#1448) may replace either. Without completions a
cell has no window and is inconclusive; there is no fallback window.

**A repeat is admitted only for lost evidence.** A pinned cell whose run was
lost, failed before observing, outlived its reservation or left incomplete
evidence may be measured once more as ``<cell>-x2``, by a run handed out
after that loss. A probe fails before observing only when no source start
receipt exists. A restart or a window too short is the job's own behaviour on
this host rather than lost evidence, so it is not repeated, and a repeat of a
cell that needed none is not counted.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

from . import analyze as cluster
from . import vmcampaign
from .analyze import (
    INCREMENTAL,
    analyze_rows,
    base_cell_id,
    build_groups,
    creator_order,
    probe_index,
    protocol_index,
    show,
    source_bounds,
)
from .campaign import CLAIMED, FAILED, MANIFEST, OBSERVED, PENDING, STATE
from .common import INCONCLUSIVE, INCONSISTENT, USABLE, Failure, digest
from .evidence import RECEIPT, DirectorySource, Reconciler, cell_kind, cell_prefix
from .protocol import LINES, load, protocol_sha256, session_cell

#: Groups are keyed by line, shape and body; every campaign of one assessment
#: contributes to the same group, so the key carries no campaign of its own.
ASSESSMENT = "vm"

#: What a run leaves that the controller did not digest: its own record, and
#: the mark that its settle hook owes nothing. Either may also leave the
#: private temporary file a write that died midway was publishing through.
UNDIGESTED = (vmcampaign.OUTCOME, vmcampaign.SETTLED)

#: The probe's checkpoint completions. Provisional: the probe does not write
#: this yet, and the preregistration revision (#1448) settles how it will.
CHECKPOINTS = "checkpoints.jsonl"

LOST = "lost"
RUN_FAILED = "failed"
#: A probe that failed after its source started: the job's own behaviour
#: under the cell's load, not evidence that was lost.
FAILED_OBSERVING = "failed-while-observing"
OVERRUN = "reservation-overrun"
MODIFIED = "evidence-modified"
DUPLICATE = "cell-executed-twice"
UNADMITTED = "repeat-not-admitted"

#: What an unusable pinned cell may be repeated for: evidence that was lost
#: rather than a measurement that came out badly. ``failed`` is a probe that
#: failed before its source started.
REPEATABLE = (LOST, RUN_FAILED, OVERRUN, "incomplete")

SLOW_EXIT = "; did not exit before its reservation"

ACCEPTANCE = {
    "applied": [
        (
            "The job completed: the journal records OBSERVED and the probe printed "
            "OBSERVATION."
        ),
        "The cell did not restart: one source incarnation (evidence.Reconciler).",
        "The observation window holds at least six ordinary checkpoints.",
        (
            "Rows equal rows_exported and the terminal counts, with no unexplained "
            "duplicate (evidence.Reconciler)."
        ),
        (
            "The run's files verify against the SHA-256 digests the controller "
            "recorded, which stand in for the exported objects' recorded hashes."
        ),
    ],
    "not_checked": [
        (
            "The GCS receipt contract (exported.json, generations, NO_OVERWRITE): "
            "this rig has none; the preregistration revision (#1448) owes its "
            "replacement."
        ),
        (
            "The calibration items for k01 to k11: the VM rig's calibration cells "
            "are the preregistration revision's (#1448)."
        ),
        (
            "Sustained backlog growth across checkpoint boundaries: the probe records "
            "no source output series."
        ),
        "The capacity search: probe cells (-qN) are listed, not analyzed.",
    ],
}


# --- the window -------------------------------------------------------------


def checkpoint_completions(run_directory):
    """``{checkpoint id: completion wall millis}``, or ``None`` if unrecorded.

    Each line of the probe's ``checkpoints.jsonl`` is ``{"id": n,
    "completedMillis": t}`` in the probe JVM's wall clock, the clock its
    receipts use. A file that is absent, or that does not read as exactly
    that, gives no completions rather than some of them.
    """
    path = Path(run_directory) / vmcampaign.PROBE / CHECKPOINTS
    try:
        text = path.read_text()
    except (FileNotFoundError, NotADirectoryError):
        return None
    completions = {}
    for line in text.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            return None
        if (
            not isinstance(entry, dict)
            or set(entry) != {"id", "completedMillis"}
            or not all(_count(entry[key]) for key in entry)
            or entry["id"] in completions
        ):
            return None
        completions[entry["id"]] = entry["completedMillis"]
    return completions


def observation_window(completions, start_ms, last_ms, warmup_seconds):
    """The preregistered window over this rig's completions, or why there is none.

    The rule itself is ``analyze.observation_window``'s, shared with the
    cluster: from the first completed ordinary checkpoint after warm-up to
    the last completed before end of input, holding at least six. This rig
    places each completion at the instant the probe recorded, in the clock
    of the source receipts that bound it.
    """
    if start_ms is None or last_ms is None:
        return None, "receipts-missing"
    if completions is None:
        return None, "checkpoints-unrecorded"
    placed = {
        key: {"id": key, "ack": Fraction(at, 1000)} for key, at in completions.items()
    }
    span, failure = cluster.observation_window(
        placed, set(completions), start_ms, warmup_seconds, last_ms
    )
    if span is not None:
        span["clock"] = "probe-wall"
    return span, failure


# --- campaigns ----------------------------------------------------------------


def _read(path):
    try:
        return json.loads(Path(path).read_bytes())
    except (OSError, ValueError) as error:
        raise Failure(f"Unreadable campaign file {path}") from error


def read_campaign(directory, protocol_digest):
    """A campaign's manifest and state, refused unless they describe one plan."""
    directory = Path(directory)
    manifest = _read(directory / MANIFEST)
    state = _read(directory / STATE)
    name = manifest.get("campaignId")
    plan = manifest.get("plan")
    if not isinstance(plan, list) or digest(plan) != state.get("planSha256"):
        raise Failure(f"Campaign {name} state was not written for its plan")
    planned = {str(run["order"]): run["cellId"] for run in plan}
    runs = state.get("runs")
    if (
        not isinstance(runs, dict)
        or not all(isinstance(run, dict) for run in runs.values())
        or planned != {k: v.get("cellId") for k, v in runs.items()}
    ):
        raise Failure(f"Campaign {name} state no longer matches its plan")
    if any(run.get("state") not in STATES for run in runs.values()):
        raise Failure(f"Campaign {name} state holds a run in no known state")
    inputs = manifest.get("inputs") or {}
    if inputs.get("flink") not in LINES:
        raise Failure(f"Campaign {name} inputs name no supported Flink line")
    if inputs.get("protocolSha256") != protocol_digest:
        raise Failure(f"Campaign {name} was planned against another protocol")
    return {"directory": directory, "manifest": manifest, "state": state}


def _carried(run):
    return {k: v for k, v in run.items() if k not in ("order", "predecessorOrder")}


def check_chain(campaigns):
    """Every successor continues a given campaign, and each is continued once."""
    by_id = {}
    for entry in campaigns:
        name = entry["manifest"]["campaignId"]
        if name in by_id:
            raise Failure(f"Campaign {name} was given twice")
        by_id[name] = entry
    continued = set()
    for entry in campaigns:
        manifest = entry["manifest"]
        link = manifest.get("predecessor")
        if link is None:
            if any("predecessorOrder" in run for run in manifest["plan"]):
                raise Failure(f"Campaign {manifest['campaignId']} names no predecessor")
            continue
        name = manifest["campaignId"]
        before = by_id.get(link.get("campaignId"))
        if before is None:
            raise Failure(
                f"Campaign {name} continues {link.get('campaignId')}, which was not given"
            )
        if link.get("campaignId") in continued:
            raise Failure(f"Campaign {link['campaignId']} is continued twice")
        continued.add(link["campaignId"])
        state = before["state"]
        if (
            link.get("planSha256") != state["planSha256"]
            or link.get("stopped") != state.get("stopped")
            or state.get("stopped") is None
        ):
            raise Failure(f"Campaign {name} does not continue {link['campaignId']}")
        if manifest["inputs"] != before["manifest"]["inputs"]:
            raise Failure(f"Campaign {name} changed its predecessor's inputs")
        # A successor carries exactly the runs its predecessor never claimed,
        # as they were planned: another queue or reservation is another run.
        # Order and predecessorOrder are the planner's to rewrite at every
        # link, so neither side's is compared.
        planned = {run["order"]: run for run in before["manifest"]["plan"]}
        pending = {
            int(order): _carried(planned[int(order)])
            for order, run in state["runs"].items()
            if run["state"] == PENDING
        }
        carried = {
            run.get("predecessorOrder"): _carried(run) for run in manifest["plan"]
        }
        if carried != pending:
            raise Failure(
                f"Campaign {name} does not carry its predecessor's unspent runs"
            )


# --- runs ---------------------------------------------------------------------


def _count(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _options(arguments):
    """The probe's ``--name value`` pairs; ``None`` for a line it would refuse."""
    if len(arguments) % 2:
        return None
    options = {}
    for name, value in zip(arguments[::2], arguments[1::2], strict=True):
        if not name.startswith("--") or name[2:] in options:
            return None
        options[name[2:]] = value
    return options


def executed_cell(options):
    """The cell the probe ran, in the shape ``evidence.Reconciler`` reads."""
    try:
        rate = Fraction(options["offered-rate"])
        return {
            "id": options["cell-id"],
            "arm": options["arm"],
            "body_bytes": int(options["body-bytes"]),
            "parallelism": int(options["parallelism"]),
            "concurrency": int(options["concurrency"]),
            "checkpoint_seconds": int(options["checkpoint-seconds"]),
            "channel_pool_size": int(options.get("channel-pool-size", "1")),
            "distribution": options["distribution"],
            # The probe parses a double; the protocol admits only whole rates.
            "offered_rate": int(rate) if rate.denominator == 1 else rate,
            "warmup_seconds": int(options["warmup-seconds"]),
            "observation_seconds": int(options["observation-seconds"]),
            "record_limit": int(options["record-limit"]),
            "attempt_limit": int(options["attempt-limit"]),
            "control_delay_millis": int(options.get("control-delay-millis", "0")),
            "emit_attempts": {"true": True, "false": False}[
                options.get("emit-attempts", "true")
            ],
        }
    except (KeyError, ValueError, ZeroDivisionError):
        return None


def _undigested(name):
    return any(
        name == kept or (name.startswith(kept) and name.endswith(".writing"))
        for kept in UNDIGESTED
    )


def verify_files(run_directory, recorded):
    """Problems between a run's files and the digests recorded for them.

    The digests are taken the way the controller took them, by
    ``vmcampaign.files``, so the two sides cannot disagree on what a name is.
    """
    if not isinstance(recorded, dict):
        return ["no-digests"]
    actual = {
        name: sha
        for name, sha in vmcampaign.files(run_directory).items()
        if not _undigested(name)
    }
    return sorted(
        [f"missing:{name}" for name in recorded.keys() - actual.keys()]
        + [f"unrecorded:{name}" for name in actual.keys() - recorded.keys()]
        + [
            f"changed:{name}"
            for name in recorded.keys() & actual.keys()
            if recorded[name] != actual[name]
        ]
    )


def expected_detail(outcome, reserves):
    """The journal detail the controller writes for this outcome record."""
    summary = outcome.get("summary")
    if summary is None:
        return f"outlived its reservation of {reserves['seconds']} seconds"
    text = vmcampaign.detail(summary)
    return text + SLOW_EXIT if outcome.get("killed") else text


#: What ``analyze.build_groups`` reads of the conditions a run executed.
CONDITIONS = (
    "arm",
    "parallelism",
    "concurrency",
    "checkpoint_seconds",
    "distribution",
    "channel_pool_size",
    "warmup_seconds",
    "observation_seconds",
    "offered_rate",
)
STATES = (PENDING, CLAIMED, OBSERVED, FAILED)


def _record(entry, run, journal_run):
    manifest = entry["manifest"]
    return {
        "campaign": ASSESSMENT,
        "campaign_id": manifest["campaignId"],
        "order": run["order"],
        "flink": manifest["inputs"]["flink"],
        "cell_id": run["cellId"],
        "protocol_cell_id": base_cell_id(run["cellId"]),
        "probe": probe_index(run["cellId"]),
        "repeat": run["cellId"].endswith("-x2"),
        "journal": {
            "state": journal_run["state"],
            "detail": journal_run.get("detail"),
        },
        # When the run was handed out, and when its outcome was settled or,
        # for a lost run, its campaign stopped: a repeat must follow the loss.
        "claimed": journal_run.get("claimed"),
        "ended": _instant(
            journal_run.get("finished")
            if journal_run["state"] != CLAIMED
            else (entry["state"].get("stopped") or {}).get("at")
        ),
        "flags": [],
        "status": None,
        "reasons": [],
        "files": None,
        "summary": None,
        "reconciliation": None,
        "window": None,
        "throughput": None,
        "rows": None,
        "body_bytes_executed": None,
        **dict.fromkeys(CONDITIONS),
    }


def analyze_run(entry, run):
    """One executed run as a cell record; ``None`` for a run never claimed."""
    manifest, directory = entry["manifest"], entry["directory"]
    journal_run = entry["state"]["runs"][str(run["order"])]
    if journal_run["state"] == PENDING:
        return None
    record = _record(entry, run, journal_run)
    reasons = record["reasons"]
    run_directory = vmcampaign.run_directory(directory, run["order"], run["cellId"])
    try:
        outcome = _read(run_directory / vmcampaign.OUTCOME)
    except Failure:
        outcome = None
    if not isinstance(outcome, dict):
        outcome = None
    if journal_run["state"] == CLAIMED:
        # The campaign stopped holding this run; its cell was spent unmeasured.
        record["status"] = LOST
        reasons.append("claimed-when-stopped")
        if outcome is not None:
            reasons.append("outcome-never-finished")
        return record
    if outcome is None:
        record.update(status=INCONSISTENT, reasons=["outcome-unreadable"])
        return record
    inputs = manifest["inputs"]
    launcher = inputs.get("launcher") or []
    argv = outcome.get("argv") or []
    arguments = run.get("arguments") or []
    probe = argv[len(launcher)] if len(argv) > len(launcher) else ""
    # The directory as the controller named it on the campaign host: collected
    # to a reviewing host, the run has moved, and the root its evidence was
    # written under has not.
    if not isinstance(probe, str) or not (
        probe.startswith("/")
        and probe.endswith(f"/{run_directory.name}/{vmcampaign.PROBE}")
        and argv
        == [
            *launcher,
            probe,
            inputs.get("endpoint", "-"),
            *arguments,
            vmcampaign.EVIDENCE_ROOT,
            vmcampaign.evidence_root(probe),
        ]
    ):
        reasons.append("argv-differs-from-plan")
    if outcome.get("outcome") != journal_run["state"]:
        reasons.append("outcome-differs-from-journal")
    if expected_detail(outcome, run["reserves"]) != journal_run.get("detail"):
        reasons.append("detail-differs-from-journal")
    options = _options(arguments)
    cell = None if options is None else executed_cell(options)
    summary = outcome.get("summary")
    # The controller checks the line's run and cell; its arm is this check's.
    if cell is not None and summary is not None and summary.get("arm") != cell["arm"]:
        reasons.append("summary-differs-from-arguments")
    if options is not None and vmcampaign.EVIDENCE_ROOT[2:] in options:
        cell = None
    if cell is None or cell["id"] != run["cellId"]:
        reasons.append("arguments-unreadable")
    if reasons:
        record["status"] = INCONSISTENT
        return record
    problems = verify_files(run_directory, outcome.get("files"))
    record["files"] = {"verified": not problems, "problems": problems}
    if problems:
        record.update(status=MODIFIED, reasons=["digests-differ"])
        return record
    record.update({field: cell[field] for field in CONDITIONS})
    record["body_bytes_executed"] = cell["body_bytes"]
    record["executed"] = cell
    record["summary"] = summary
    if outcome.get("killed"):
        record["flags"].append("slow-exit" if summary is not None else "killed")
    if summary is not None and summary.get("teardown") != "-":
        record["flags"].append("teardown")
    source = DirectorySource(
        run_directory / vmcampaign.PROBE / vmcampaign.EVIDENCE_DIRECTORY
    )
    if journal_run["state"] == FAILED:
        if summary is None:
            record.update(status=OVERRUN, reasons=[journal_run["detail"]])
        elif source_started(source, options["run-id"], cell["id"]):
            record.update(status=FAILED_OBSERVING, reasons=[summary["reason"]])
        else:
            record.update(status=RUN_FAILED, reasons=[summary["reason"]])
        return record
    return observe(record, source, run_directory, options, cell)


def source_started(source, run_id, cell_id):
    """Whether any source incarnation of the cell wrote its start receipt."""
    for obj in source.list(cell_prefix(run_id, cell_id)):
        match = RECEIPT.search(obj["name"])
        if match and match[1] == "source" and match[3] == "start":
            return True
    return False


def observe(record, source, run_directory, options, cell):
    """The reconciliation, window and row statistics of an observed run."""
    run_id = options["run-id"]
    try:
        reconciliation = Reconciler(source, run_id, cell, cell_kind(cell)).run()
    except Failure as error:
        record.update(status="invalid", reasons=["reconciler-failed:" + str(error)])
        return record
    record["reconciliation"] = {
        "status": reconciliation.status,
        "reasons": list(reconciliation.reasons),
        "counts": dict(reconciliation.counts),
    }
    if reconciliation.status != "complete":
        record.update(
            status=reconciliation.status, reasons=list(reconciliation.reasons)
        )
        return record
    receipts = reconciliation.receipts
    start_ms, last_ms = source_bounds(receipts)
    span, failure = observation_window(
        checkpoint_completions(run_directory),
        start_ms,
        last_ms,
        cell["warmup_seconds"],
    )
    record["window"] = span
    rows = analyze_rows(
        SimpleNamespace(source=source, run_id=run_id),
        cell,
        creator_order(receipts),
        span,
    )
    rows.pop("completions")
    record["rows"] = rows
    if span is None:
        record.update(status=INCONCLUSIVE, reasons=[failure])
        return record
    record["throughput"] = Fraction(rows["distinct_ok_in_window"]) / span["seconds"]
    record["status"] = USABLE
    return record


# --- the assessment -----------------------------------------------------------


#: What a run's limits and controls must be for the cell its ID names; the
#: shape, body, arm, line and window are ``analyze.build_groups``'s to compare.
LIMITS = (
    "offered_rate",
    "record_limit",
    "attempt_limit",
    "control_delay_millis",
    "emit_attempts",
)


def limits(record, index):
    """Refuses a run whose limits or controls differ from the cell its ID names."""
    protocol_cell = index.get(record["protocol_cell_id"])
    cell = record.get("executed")
    if protocol_cell is None or cell is None:
        return
    suffix = record["cell_id"][len(record["protocol_cell_id"]) :]
    try:
        expected = session_cell(protocol_cell, cell["offered_rate"], suffix)
    except Failure:
        differs = ["offered_rate"]
    else:
        differs = [key for key in LIMITS if cell[key] != expected[key]]
    if differs:
        record["reasons"].append("limits-differ-from-protocol:" + ",".join(differs))
        if record["status"] == USABLE:
            record["status"] = INCONCLUSIVE


def admit(records):
    """Marks cells executed twice, and repeats their pinned cell did not need."""
    executed = {}
    for record in records:
        executed.setdefault(record["cell_id"], []).append(record)
    for runs in executed.values():
        if len(runs) > 1:
            for record in runs:
                record["status"] = DUPLICATE
                record["reasons"].append("cell-executed-twice")
    for record in records:
        if not record["repeat"] or record["status"] == DUPLICATE:
            continue
        pinned = executed.get(record["protocol_cell_id"], [])
        # A repeat answers a loss that had already happened when it was
        # handed out; one handed out earlier answers nothing.
        if (
            len(pinned) != 1
            or pinned[0]["status"] not in REPEATABLE
            or not _before(pinned[0], record)
        ):
            if record["status"] == USABLE:
                record["status"] = UNADMITTED
            record["reasons"].append("repeat-not-admitted")


def _instant(text):
    """Seconds since the epoch of a journal timestamp, or ``None``."""
    try:
        return datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError):
        return None


def _before(earlier, later):
    """Whether ``later`` was handed out after ``earlier`` was lost.

    Campaigns may run side by side, so a repeat claimed after its pinned run
    was claimed may still have started before that run failed.
    """
    return (
        earlier["ended"] is not None
        and isinstance(later["claimed"], int | float)
        and earlier["ended"] < later["claimed"]
    )


def analyze(directories, protocol_path=None):
    protocol = load(protocol_path)
    protocol_digest = protocol_sha256(protocol_path)
    campaigns = [read_campaign(d, protocol_digest) for d in directories]
    campaigns.sort(key=lambda entry: entry["manifest"]["campaignId"])
    check_chain(campaigns)
    records = []
    for entry in campaigns:
        for run in entry["manifest"]["plan"]:
            record = analyze_run(entry, run)
            if record is not None:
                records.append(record)
    records.sort(key=lambda r: (r["cell_id"], r["campaign_id"], r["order"]))
    index = protocol_index(protocol)
    for record in records:
        limits(record, index)
    admit(records)
    probes = [r for r in records if r["probe"] is not None]
    for record in probes:
        record["reasons"].append("capacity-probe")
    # Only a run whose conditions are known can be compared with its cell; a
    # lost or inconsistent run is reported above and has none to compare.
    measured = [r for r in records if r["probe"] is None and "executed" in r]
    groups, unmatched = build_groups(measured, index)
    for record in records:
        record.pop("executed", None)
        record.pop("claimed", None)
        record.pop("ended", None)
    return {
        "protocol_sha256": protocol_digest,
        "acceptance": ACCEPTANCE,
        "campaigns": [_campaign_summary(entry, records) for entry in campaigns],
        "cells": records,
        "groups": groups,
        "unmatched_cells": unmatched,
        "not_analyzed": sorted(r["cell_id"] for r in probes),
    }


def _campaign_summary(entry, records):
    manifest, state = entry["manifest"], entry["state"]
    name = manifest["campaignId"]
    mine = [r for r in records if r["campaign_id"] == name]
    counts = dict.fromkeys((PENDING, CLAIMED, OBSERVED, FAILED), 0)
    for run in state["runs"].values():
        counts[run["state"]] += 1
    return {
        "campaign_id": name,
        "flink": manifest["inputs"]["flink"],
        "predecessor": (manifest.get("predecessor") or {}).get("campaignId"),
        "stopped": state.get("stopped"),
        "runs": counts,
        "lost": [r["cell_id"] for r in mine if r["status"] == LOST],
        # ADR-0162 asks for the series, not the count: a reservation that is
        # systematically short shows as many of these rather than as a stop.
        "reservation_overruns": [r["cell_id"] for r in mine if r["status"] == OVERRUN],
        "slow_exits": [r["cell_id"] for r in mine if "slow-exit" in r["flags"]],
    }


# --- report -----------------------------------------------------------------


def render_markdown(report):
    lines = [
        "# Cloud Tasks single-host campaign analysis",
        "",
        (
            f"The analyzer read {len(report['campaigns'])} campaigns holding "
            f"{len(report['cells'])} executed cells. Verdict labels follow the "
            "preregistration in docs/adr/evidence/0162-cloudtasks-assessment-1246.md "
            "and state no support or release decision."
        ),
        "",
        "## Campaigns",
        "",
    ]
    for campaign in report["campaigns"]:
        stopped = campaign["stopped"]
        lines.append(
            f"Campaign {campaign['campaign_id']} on Flink {campaign['flink']} "
            + (
                f"continues {campaign['predecessor']} and "
                if campaign["predecessor"]
                else ""
            )
            + ("ran to its end" if stopped is None else f"stopped: {stopped['reason']}")
            + f"; runs {campaign['runs']}."
        )
        for key, label in (
            ("lost", "Lost"),
            ("reservation_overruns", "Outlived their reservation"),
            ("slow_exits", "Kept their outcome but did not exit"),
        ):
            if campaign[key]:
                lines.append(f"{label}: {', '.join(campaign[key])}.")
    lines += ["", "## Cells", ""]
    for cell in report["cells"]:
        reasons = ", ".join(cell["reasons"]) or "none"
        p95 = (cell.get("rows") or {}).get("p95_nanos")
        lines.append(
            f"Cell {cell['cell_id']} of campaign {cell['campaign_id']} is "
            f"{cell['status']} (reasons: {reasons}) with throughput "
            f"{show(cell['throughput'])} records per second and p95 "
            f"{'n/a' if p95 is None else str(p95) + ' ns'}."
        )
    lines += ["", "## Groups", ""]
    if not report["groups"]:
        lines.append("No protocol group was found.")
    for group in report["groups"]:
        lines.append(
            f"Group {group['flink']} {group['shape']} {group['body_bytes']} B is "
            f"{group['verdict']}."
        )
        for arm, comparison in group["comparisons"].items():
            lines.append(
                f"{arm} against UNNAMED is {comparison['label']} with throughput "
                f"ratio {show(comparison['throughput_ratio'])} and p95 ratio "
                f"{show(comparison['p95_ratio'])}"
                + (
                    f" (reasons: {', '.join(comparison['reasons'])})."
                    if comparison["reasons"]
                    else "."
                )
            )
        for arm, cost in group["incremental_cost"].items():
            if cost is not None:
                lines.append(
                    f"{arm} against {INCREMENTAL[arm]} costs throughput ratio "
                    f"{show(cost['throughput_ratio'])} and p95 ratio "
                    f"{show(cost['p95_ratio'])}."
                )
    for key, label in (
        ("unmatched_cells", "Cells outside the protocol pin"),
        ("not_analyzed", "Capacity probes, listed and not analyzed"),
    ):
        if report[key]:
            lines += ["", f"{label}: {', '.join(report[key])}."]
    lines += ["", "## Acceptance items", ""]
    lines += [f"Applied: {item}" for item in report["acceptance"]["applied"]]
    lines += [f"Not checked: {item}" for item in report["acceptance"]["not_checked"]]
    return "\n".join(lines) + "\n"


def write_reports(report, out):
    return cluster.write_reports(report, out, markdown=render_markdown)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="flink-tier3 vm-analyze",
        description="Offline analysis of collected single-host campaign "
        "directories; never contacts a service.",
    )
    parser.add_argument("campaigns", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, default=None)
    args = parser.parse_args(argv)
    try:
        report = analyze(args.campaigns, args.protocol)
        out = write_reports(report, args.out)
    except Failure as error:
        print("analysis failed: " + str(error))
        return 1
    counts = {}
    for cell in report["cells"]:
        counts[cell["status"]] = counts.get(cell["status"], 0) + 1
    status = ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
    print(
        f"analyzed {len(report['cells'])} cells in {len(report['campaigns'])} "
        f"campaigns ({status or 'none'}); {len(report['groups'])} groups; "
        f"reports in {out}"
    )
    return 0
