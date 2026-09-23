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
"""Durable admission state for the single-host campaign.

The campaign is a sequence of forked JVMs on one machine. It can be stopped
and resumed, and it must not re-run a cell it already spent: that cell's tasks
exist in a real queue, so a second observation of it measures against a queue
that is not empty. Keeping that from happening is the whole job of this
module, and everything below is in service of it.

Three mechanisms carry it, all taken from ``Stage2CampaignJournal``, the
Bigtable rig's journal this transfers from:

* **One claim at a time.** A campaign that is a sequence has at most one run
  in flight, so ``claim`` refuses while any run is still claimed. That also
  catches a lost outcome: in a one-at-a-time campaign a leftover claim means
  the previous run's result never landed.
* **A lock across read-modify-write.** Every mutation takes an exclusive lock
  on ``campaign.lock`` and holds it from the read to the write. Renaming a
  whole file makes one writer's bytes land all at once; it does nothing about
  two writers interleaving, which is how the same run gets handed out twice.
* **A durable write.** The payload is written to a private temporary file in
  the same directory, fsynced, renamed, and the directory fsynced. A rename
  alone survives a killed process; the fsyncs are the standard sequence for
  surviving a host crash on the rig's Linux filesystem as well. That second
  claim is the filesystem's and has not been tested against a power cut here,
  and on macOS ``fsync`` does not flush the drive's own cache.

What stops a campaign is deliberately narrow, and follows ADR-0166's
2026-09-19 refinement: a run that fails, measures nothing or is censored is
recorded ``FAILED`` and the campaign proceeds, because stopping on the first
failure would leave most cells unmeasured after one checkpoint timeout. What
stops it is losing track of a run — an owner that died holding a claim, a
claim whose heartbeat expired, a leftover claim, or an outcome that could not
be recorded — and a next run that no longer fits the budget. Failures are counted in their own state rather than folded into
a completion total, so a campaign that finished every run and measured
nothing is visibly not a measured campaign.
"""

import contextlib
import copy
import fcntl
import json
import os
import tempfile
from pathlib import Path

from .common import Failure, digest, json_bytes, utc
from .policy import CELL_ID, RUN_ID

#: A claim whose owner has not written a heartbeat for this long is expired.
HEARTBEAT_EXPIRY_SECONDS = 300

#: What a run can be. A run leaves ``CLAIMED`` exactly once.
PENDING = "PENDING"
CLAIMED = "CLAIMED"
OBSERVED = "OBSERVED"
FAILED = "FAILED"

TERMINAL = (OBSERVED, FAILED)

#: What a run reserves and a campaign budgets, in the order messages name them.
RESOURCES = ("bytes", "seconds")

MANIFEST = "manifest.json"
STATE = "state.json"
LOCK = "campaign.lock"


def _write(path, payload):
    """Publishes JSON through a private temporary file, fsynced, then renamed.

    The private name keeps two writers from truncating each other's bytes
    under one well-known name. The fsyncs ask the kernel to put the data and
    the rename on the disk rather than leave them in the page cache.
    """
    directory = path.parent
    handle, temporary = tempfile.mkstemp(
        dir=directory, prefix=path.name, suffix=".writing"
    )
    try:
        with os.fdopen(handle, "wb") as file:
            file.write(json_bytes(payload))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    except OSError as error:
        Path(temporary).unlink(missing_ok=True)
        raise Failure(f"Could not write campaign file {path.name}") from error
    opened = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(opened)
    finally:
        os.close(opened)


def _read(path):
    try:
        return json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise Failure(f"Unreadable campaign file {path.name}") from error


def plan_campaign(directory, campaign_id, runs, inputs, budgets):
    """Freezes a campaign's plan and the inputs it was planned against.

    ``inputs`` are the frozen inputs — the protocol file's digest, the
    measurement classpath's digest, whatever else must not move under a
    resumed campaign. :class:`Journal` refuses to open a campaign whose inputs
    no longer digest the same, because a campaign that silently changed what
    it was measuring is worse than one that stopped. The plan travels in the
    manifest, and its digest is recorded in the state file beside it, so an
    edit to either file alone — a reordered entry, a changed reservation, a
    repeated cell — refuses to open. A digest kept in the same file as the
    bytes it covers would be edited along with them; kept in the other file it
    catches everything short of a deliberate, matching edit to both, which is
    tampering by whoever owns the host and outside what this defends against.
    """
    directory = Path(directory)
    if not RUN_ID.match(campaign_id):
        raise Failure("Campaign id must be a lowercase DNS-style name")
    if not runs:
        raise Failure("A campaign plans at least one run")
    if [run["order"] for run in runs] != list(range(1, len(runs) + 1)):
        raise Failure("Campaign runs are ordered 1..n with no gaps")
    for run in runs:
        _require_run(run)
    _require_distinct_cells(runs)
    manifest = {
        "campaignId": campaign_id,
        "inputs": inputs,
        "budgets": _require_budgets(budgets),
        "plan": runs,
    }
    directory.mkdir(parents=True, exist_ok=True)
    # Under the campaign's lock, so two planners cannot both pass the check:
    # the second would otherwise rewrite a state the first had already begun
    # claiming from, and hand its spent runs out again.
    with _exclusive(directory):
        if (directory / MANIFEST).exists():
            raise Failure("A campaign is already planned here")
        _write(directory / STATE, _initial(runs))
        # Last, so a manifest present means the state beside it is whole.
        _write(directory / MANIFEST, manifest)
    return manifest


def _require_distinct_cells(runs):
    # The invariant itself, rather than a digest of the plan: a cell that
    # appears twice is a cell the campaign would spend twice.
    cells = [run["cellId"] for run in runs]
    if len(set(cells)) != len(cells):
        raise Failure("A campaign plans each cell at most once")


@contextlib.contextmanager
def _exclusive(directory):
    with open(Path(directory) / LOCK, "a+") as guard:
        fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(guard.fileno(), fcntl.LOCK_UN)


def _initial(runs):
    return {
        "planSha256": digest(runs),
        "stopped": None,
        "runs": {
            str(run["order"]): {"state": PENDING, "cellId": run["cellId"]}
            for run in runs
        },
        "consumed": dict.fromkeys(RESOURCES, 0),
    }


def _require_budgets(budgets):
    if set(budgets) != set(RESOURCES):
        raise Failure(f"A campaign budget names {' and '.join(RESOURCES)}")
    for name, value in budgets.items():
        _require_positive(value, f"Campaign budget {name}")
    return dict(budgets)


def _require_run(run):
    order = run.get("order")
    if not isinstance(run.get("cellId"), str) or not CELL_ID.match(run["cellId"]):
        raise Failure(f"Run {order} must name a cell")
    reservation = run.get("reserves")
    if not isinstance(reservation, dict) or set(reservation) != set(RESOURCES):
        raise Failure(f"Run {order} must reserve {' and '.join(RESOURCES)}")
    for name, value in reservation.items():
        _require_positive(value, f"Run {order} reserved {name}")


def _require_positive(value, what):
    # `bool` is an `int` in Python, so `True` would otherwise pass as a budget
    # of one byte rather than as the mistake it is.
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise Failure(f"{what} must be a positive integer")


def _require_consumed(consumed, order):
    if set(consumed) != set(RESOURCES):
        raise Failure(f"Run {order} must report what it consumed")
    for name, value in consumed.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise Failure(f"Run {order} consumed {name} must not be negative")
    return dict(consumed)


class Journal:
    """The campaign's admission state, opened against its frozen inputs."""

    def __init__(self, directory, inputs, *, clock, alive):
        """Opens a planned campaign.

        ``clock`` returns seconds as a float and ``alive`` is given the owner
        record a claim was taken with and answers whether that owner is still
        running. Both are arguments rather than module functions so a test can
        drive a whole campaign — including an owner that dies holding a claim —
        without waiting or forking.
        """
        self.directory = Path(directory)
        self._clock = clock
        self._alive = alive
        self.manifest = _read(self.directory / MANIFEST)
        self.plan = self.manifest["plan"]
        if digest(inputs) != digest(self.manifest["inputs"]):
            raise Failure("Campaign inputs differ from the ones it was planned against")
        self._locked(self._require_plan_matches)
        # Opening is the resume path, so a campaign that lost a run while
        # nobody was looking says so before it is asked anything else.
        self._locked(self._supervise)

    def _locked(self, mutation):
        """Runs one read-modify-write under an exclusive lock on the campaign.

        Held across the read and the write, which is the part that matters: a
        rename publishes one writer's bytes whole, and two unsynchronised
        readers still compute their next state from the same stale one.

        A mutation that changed the state and then refused — stopping the
        campaign and telling its caller so — is written before the refusal
        travels. Otherwise the stop would live only in the exception, and the
        next process to open the campaign would find it running.
        """
        with _exclusive(self.directory):
            state = _read(self.directory / STATE)
            before = copy.deepcopy(state)
            try:
                return mutation(state)
            finally:
                if state != before:
                    _write(self.directory / STATE, state)

    def _require_plan_matches(self, state):
        # Claims read the plan by position, so a reordered entry would hand out
        # one order under another's cell; a changed reservation or arm would
        # run under parameters the campaign was not planned with.
        if digest(self.plan) != state.get("planSha256"):
            raise Failure(
                "Campaign plan differs from the one its state was planned with"
            )
        # The digest covers the manifest; this covers the state's side of the
        # same fact. Swapped run records would otherwise put a finished run's
        # PENDING neighbour under its order and hand its cell out again.
        planned = {str(run["order"]): run["cellId"] for run in self.plan}
        recorded = {order: run["cellId"] for order, run in state["runs"].items()}
        if planned != recorded:
            raise Failure(
                "Campaign state no longer matches the plan it was written for"
            )
        # What neither check can see is a hand edit to a run's own state, say
        # OBSERVED back to PENDING: this file is the ledger of record, and
        # nothing short of an append-only log could tell that edit from a
        # legitimate write.

    def stopped(self):
        """Why the campaign stopped, or ``None`` while it may still hand out runs."""
        return self._locked(self._supervise)

    def stop(self, reason):
        """Stops the campaign, keeping the first reason.

        The first stop is the one that explains the campaign; a later one is a
        consequence of it, and overwriting would hide why the run really ended.
        """
        return self._locked(lambda state: self._stop(state, reason))

    def _stop(self, state, reason):
        if state["stopped"] is not None:
            return state["stopped"]
        state["stopped"] = {"reason": reason, "at": utc(self._clock())}
        return state["stopped"]

    def _supervise(self, state):
        """Checks every claim, stopping the campaign if one was lost.

        A claim whose owner is gone is an interrupted observation, not a run to
        hand out again: the cell was spent when its tasks were created.
        """
        if state["stopped"] is not None:
            return state["stopped"]
        now = self._clock()
        for order, run in _ordered(state):
            if run["state"] != CLAIMED:
                continue
            if not self._alive(run["owner"]):
                return self._stop(state, f"run {order} lost its owner while claimed")
            if now - run["heartbeat"] > HEARTBEAT_EXPIRY_SECONDS:
                return self._stop(state, f"run {order} outlived its supervision")
        return None

    def counts(self):
        """Run states, budget, and the stop reason if the campaign has one.

        The stop travels with the counts because a report built from counts
        alone would show a tidy campaign that had in fact lost a run.
        """
        return self._locked(self._counts)

    def _counts(self, state):
        self._supervise(state)
        counts = dict.fromkeys((PENDING, CLAIMED, OBSERVED, FAILED), 0)
        for run in state["runs"].values():
            counts[run["state"]] += 1
        return {
            "runs": counts,
            "consumed": dict(state["consumed"]),
            "remaining": {
                name: self.manifest["budgets"][name] - state["consumed"][name]
                for name in RESOURCES
            },
            "stopped": state["stopped"],
        }

    def claim(self, owner):
        """Hands out the next pending run, or ``None`` when the campaign is done.

        Raises when the campaign has stopped, rather than returning ``None``
        for both. A driver is a ``while`` loop over this call, and a stop that
        looked like completion would carry a campaign of 400 cells into
        analysis having measured two.

        The reservation is taken now, not at finish: a run that overruns has
        already spent what it reserved, and a campaign that admitted the next
        one against an unspent budget would be over it.
        """
        run = self._locked(lambda state: self._claim(state, owner))
        return copy.deepcopy(run)

    def _claim(self, state, owner):
        lost = self._supervise(state)
        if lost is not None:
            raise Failure(f"Campaign stopped: {lost['reason']}")
        for order, run in _ordered(state):
            if run["state"] == CLAIMED:
                # One at a time, so a claim still standing means the run before
                # this one ended without its outcome reaching the journal.
                stop = self._stop(state, f"run {order} recorded no outcome")
                raise Failure(f"Campaign stopped: {stop['reason']}")
        for order, run in _ordered(state):
            if run["state"] != PENDING:
                continue
            reserves = self.plan[int(order) - 1]["reserves"]
            for name in RESOURCES:
                if (
                    reserves[name]
                    > self.manifest["budgets"][name] - state["consumed"][name]
                ):
                    stop = self._stop(
                        state, f"run {order} does not fit the remaining {name}"
                    )
                    raise Failure(f"Campaign stopped: {stop['reason']}")
            run.update(
                state=CLAIMED,
                owner=owner,
                claimed=self._clock(),
                heartbeat=self._clock(),
            )
            for name in RESOURCES:
                state["consumed"][name] += reserves[name]
            return self.plan[int(order) - 1]
        return None

    def heartbeat(self, order, owner):
        """Records that the run's owner is still working, refreshing its claim."""
        self._locked(lambda state: self._heartbeat(state, order, owner))

    def _heartbeat(self, state, order, owner):
        run = self._claimed(state, order, owner)
        run["heartbeat"] = self._clock()

    def finish(self, order, owner, outcome, consumed, detail=None):
        """Records a claimed run's outcome, exactly once.

        An outcome that is neither ``OBSERVED`` nor ``FAILED`` stops the
        campaign rather than being stored: a controller that cannot say what a
        run produced has lost the run, and the alternative is a campaign that
        reports itself complete over cells that measured nothing.
        """
        self._locked(
            lambda state: self._finish(state, order, owner, outcome, consumed, detail)
        )

    def _finish(self, state, order, owner, outcome, consumed, detail):
        run = self._claimed(state, order, owner)
        if outcome not in TERMINAL:
            stop = self._stop(state, f"run {order} recorded no outcome")
            raise Failure(f"Campaign stopped: {stop['reason']}")
        actual = _require_consumed(consumed, order)
        run["state"] = outcome
        run["finished"] = utc(self._clock())
        run["consumed"] = actual
        if detail is not None:
            run["detail"] = detail
        # The reservation was taken at claim; settle the difference so a run
        # that used less gives it back and one that used more is charged.
        reserves = self.plan[int(order) - 1]["reserves"]
        for name in RESOURCES:
            state["consumed"][name] += actual[name] - reserves[name]

    def _claimed(self, state, order, owner):
        # Every call that acts on a claim observes it first, so a claim that
        # outlived its supervision is stopped rather than revived by a late
        # heartbeat or outcome from the owner that let it lapse.
        self._supervise(state)
        if state["stopped"] is not None:
            raise Failure(f"Campaign stopped: {state['stopped']['reason']}")
        run = state["runs"].get(str(order))
        if run is None:
            raise Failure(f"Campaign has no run {order}")
        if run["state"] != CLAIMED:
            raise Failure(f"Run {order} is {run['state']}, not {CLAIMED}")
        if run["owner"] != owner:
            raise Failure(f"Run {order} is held by another owner")
        return run


def _ordered(state):
    return sorted(state["runs"].items(), key=lambda item: int(item[0]))
