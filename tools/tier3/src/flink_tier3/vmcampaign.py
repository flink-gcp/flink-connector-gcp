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
"""Runs a planned single-host campaign, one forked probe JVM per run.

This is the part of the campaign that ADR-0162 keeps under review: take the
next run from the journal, fork the probe, keep the claim alive while it runs,
stop it when it outlives its reservation, and turn what it printed into an
outcome. It also names where the probe writes its receipts and rows: inside
the run's own directory, so the digests recorded with the outcome cover them.
It runs on the campaign's host. What is specific to that host is not
here: the VM and the classpath are in place before a controller starts, and
the per-run work — the queue a run writes to, moving its evidence off the
machine — arrives through the ``prepare`` and ``settle`` hooks, whose source
stays outside tracked source.

Two rules decide what happens to the campaign after a run:

* **A run whose outcome is known is recorded, and the campaign proceeds.**
  That includes a run the probe reports as failed and a run this controller
  stopped at its deadline and saw exit: the cell was spent either way, and
  the process is gone, so nothing of it can leak into the next run.
* **A run whose outcome is not known stops the campaign.** No summary line, a
  line that does not parse, a line for another run, a process that would not
  die, or a hook that failed. Proceeding would let a campaign report itself
  complete over cells that measured nothing.

The controller stops its runs by process group, and assumes nothing else
signals that group. Under systemd that means ``KillMode=mixed`` or
``process``: the default signals the whole cgroup, so stopping the service
would reach the run too, and an interrupt would no longer wait for it.
"""

import contextlib
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from .campaign import FAILED, OBSERVED, Journal
from .common import Failure

#: What the probe prints first on its summary line, and how many fields it has.
PREFIX = "PROBE1246"
FIELDS = 13

#: How long the controller waits on a run between heartbeats.
CADENCE_SECONDS = 30.0
#: How long a stopped run gets to exit after SIGTERM, and then after SIGKILL.
TERM_GRACE_SECONDS = 10.0
KILL_GRACE_SECONDS = 20.0

RUNS = "runs"
WORKER = "worker.json"
OUTCOME = "outcome.json"
SETTLED = "settled.json"
DRIVING = "controller.lock"
PROBE = "probe"
STDOUT = "stdout.log"
STDERR = "stderr.log"

#: What the probe writes that is scratch rather than evidence.
SCRATCH = ("checkpoints",)

#: The probe option naming where its receipts and rows go. The controller
#: sets it, because only the controller knows the run's directory.
EVIDENCE_ROOT = "--evidence-root"
#: Where the probe's receipts and rows go, below its own directory.
EVIDENCE_DIRECTORY = "evidence"

_PROBE_OUTCOMES = {"OBSERVATION": OBSERVED, "FAILED": FAILED}
_COUNT = re.compile(r"[0-9]+\Z")
#: What a campaign's absolute path may hold. The probe checks its evidence root
#: with java.net.URI, which refuses a space or a '#', and Flink's Path quotes
#: a '%' again, so an encoded root would name another directory; a path that
#: needs neither is taken as it is.
_PLAIN_PATH = re.compile(r"/[A-Za-z0-9._/-]*\Z")


class Unrecordable(Exception):
    """The run ended and nothing it left says what it measured."""


def start_time(pid):
    """What identifies the process's start, or ``None`` if there is none.

    The start is what makes a PID an identity: a PID alone is reused by an
    unrelated process once its owner exits. On Linux it is the start in clock
    ticks since boot, read from ``/proc`` with the boot's own id, so neither a
    stepped wall clock nor a reboot can make two processes look the same.
    Elsewhere — a developer's macOS — it is what ``ps`` prints.

    A process that is gone answers ``None``. One that cannot be looked up
    raises instead: reading an unanswered question as "dead" would stop a
    campaign whose owner is working.
    """
    proc = Path("/proc")
    if (proc / "self" / "stat").exists():
        try:
            stat = (proc / str(pid) / "stat").read_text()
        except (FileNotFoundError, ProcessLookupError):
            return None
        # The command name is in parentheses and may itself hold spaces or
        # parentheses, so the fields are counted from the last one.
        fields = stat[stat.rindex(")") + 2 :].split()
        boot = (proc / "sys" / "kernel" / "random" / "boot_id").read_text().strip()
        return f"{boot}:{fields[19]}"
    try:
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            text=True,
            env={**os.environ, "LC_ALL": "C"},
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise Failure(f"Could not look up process {pid}") from error
    text = result.stdout.strip()
    if text:
        return text
    if result.returncode == 1:
        return None
    raise Failure(f"Could not look up process {pid}")


def identity(pid):
    """An owner record for a live process on this host."""
    return {"host": socket.gethostname(), "pid": pid, "started": start_time(pid)}


def alive(owner):
    """Whether the process an owner record names is still that process.

    A record from another host is never alive here: the journal travels with
    its directory, and a PID on this host says nothing about one elsewhere.
    """
    return (
        owner.get("host") == socket.gethostname()
        and start_time(owner["pid"]) == owner["started"]
    )


def option(arguments, name):
    """The value that follows ``name`` in a run's probe arguments, if any.

    Public so a host hook reads the run's queue from the same arguments the
    probe is given, rather than parsing them a second way.
    """
    for index, argument in enumerate(arguments[:-1]):
        if argument == name:
            return arguments[index + 1]
    return None


def evidence_root(probe):
    """Where a run's probe writes its receipts and rows: inside the run.

    Inside the run's own directory, so the digests recorded with its outcome
    cover them and the run is collected as one directory. The probe appends
    ``<run id>/cells/<cell id>/`` to this root, the layout ``evidence``
    reads below ``runs/``.
    """
    return f"file://{probe}/{EVIDENCE_DIRECTORY}/runs/"


def parse(stdout, run_id, cell_id):
    """The journal outcome and the fields of the probe's summary line.

    Exactly one complete line may start with the prefix. None means the probe
    never reached the end of its run; two mean the output is not the one
    run's. Only ``\\n`` ends a line, which is all the probe writes: a failure
    text may carry a form feed or a Unicode line separator, and splitting on
    those would cut a recordable failure into an unrecordable one. A last
    line with no ``\\n`` was cut short by a stop and is not a line at all.
    """
    lines = [line for line in stdout.split("\n")[:-1] if line.startswith(PREFIX + ",")]
    if not lines:
        raise Unrecordable("printed no summary line")
    if len(lines) > 1:
        raise Unrecordable(f"printed {len(lines)} summary lines")
    fields = lines[0].split(",")
    if len(fields) != FIELDS:
        raise Unrecordable(f"printed a summary of {len(fields)} fields, not {FIELDS}")
    if fields[1] != run_id or fields[2] != cell_id:
        raise Unrecordable(
            f"printed a summary for {fields[1]}/{fields[2]}, not {run_id}/{cell_id}"
        )
    outcome = _PROBE_OUTCOMES.get(fields[4])
    if outcome is None:
        raise Unrecordable(f"printed an unknown outcome {fields[4]!r}")
    # The probe writes a reason exactly when it failed; one without the other
    # is a line this parser does not understand, not a result to guess at.
    if (outcome == FAILED) != (fields[5] != "-"):
        raise Unrecordable("printed an outcome that disagrees with its reason")
    for index in (6, 7, 8, 9):
        if not _COUNT.match(fields[index]):
            raise Unrecordable(f"printed a non-numeric field {index + 1}")
    return outcome, {
        "runId": fields[1],
        "cellId": fields[2],
        "arm": fields[3],
        "outcome": fields[4],
        "reason": fields[5],
        "elapsedNanos": int(fields[6]),
        "samples": int(fields[7]),
        "heapPeakBytes": int(fields[8]),
        "gcMillis": int(fields[9]),
        "extremes": fields[10],
        "unobserved": fields[11],
        "teardown": fields[12],
    }


def detail(fields):
    """The short text the journal keeps; the evidence directory keeps the rest."""
    text = "observation" if fields["reason"] == "-" else fields["reason"]
    if fields["teardown"] != "-":
        # A slow shutdown does not undo a measurement, so it rides beside the
        # outcome rather than changing it.
        text += f"; teardown: {fields['teardown']}"
    return text


def _group_gone(group):
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        # A member that is not ours is still a member.
        return False
    return False


def _wait_group(group, until):
    """Waits for a group's members to go, which their parents reap."""
    while not _group_gone(group):
        if time.monotonic() >= until:
            return False
        time.sleep(0.05)
    return True


def _size(directory):
    return sum(
        path.stat().st_size for path in Path(directory).rglob("*") if path.is_file()
    )


def _sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            sha.update(block)
    return sha.hexdigest()


def files(directory, tick=None):
    """Each file below ``directory`` by relative name, with its SHA-256.

    ``tick`` is called after each file. A run's rows are inside its
    directory, so hashing it can outlast a claim's expiry on a slow disk, and
    the controller refreshes the claim from here rather than around the call.
    """
    digests = {}
    for path in sorted(Path(directory).rglob("*")):
        if path.is_file():
            digests[str(path.relative_to(directory))] = _sha256(path)
            if tick is not None:
                tick()
    return digests


def _write_json(path, payload):
    """Publishes JSON whole: a reader never sees half of it."""
    path = Path(path)
    handle, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=path.name, suffix=".writing"
    )
    with os.fdopen(handle, "w") as file:
        file.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def run_directory(directory, order, cell_id):
    """Where a run's record and evidence live, below its campaign."""
    return Path(directory) / RUNS / f"{order:03d}-{cell_id}"


def _owes_nothing(directory, run):
    _write_json(run_directory(directory, run["order"], run["cellId"]) / SETTLED, {})


class Controller:
    """Drives one campaign directory from this process.

    ``inputs`` are the frozen inputs the campaign was planned with. They carry
    the launcher — the ``java`` command, its flags, the frozen classpath and
    the probe's main class, as one list — and optionally the ``endpoint`` the
    probe is given, ``-`` for the real service. Taking both from the inputs
    rather than from arguments means a resumed campaign cannot fork a
    different JVM, or point it somewhere else, than its digest names.

    ``prepare(run, heartbeat)`` runs before the fork; it may call
    ``heartbeat()`` while it works, and must undo its own partial work before
    raising. ``settle(run, record)`` runs once the run is over — after its
    outcome is recorded, with ``record`` as written to ``outcome.json``, or
    with ``None`` when the run's outcome is unknown and the campaign stops.
    A settle that raises, or a controller that dies before it returns — or
    before it was called at all — leaves the run owed, and the next
    controller settles it before claiming anything. So ``settle`` must
    tolerate running twice for one run, and ``settle(run, None)`` may follow
    a ``prepare`` that a crash cut short or never started. It must not write
    into the run's directory: an analyzer reads every file there against the
    digests in ``outcome.json``, and only ``settled.json`` follows them.
    """

    def __init__(
        self,
        directory,
        inputs,
        *,
        prepare=None,
        settle=None,
        cadence=CADENCE_SECONDS,
        term_grace=TERM_GRACE_SECONDS,
        kill_grace=KILL_GRACE_SECONDS,
    ):
        launcher = inputs.get("launcher")
        if (
            not isinstance(launcher, list)
            or not launcher
            or not all(isinstance(part, str) for part in launcher)
        ):
            raise Failure("Campaign inputs carry no launcher")
        self.directory = Path(directory)
        if not _PLAIN_PATH.match(str(self.directory.absolute())):
            raise Failure(
                "Campaign directory path must be plain: letters, digits, '.', '_', "
                "'-' and '/'"
            )
        self.launcher = launcher
        self.endpoint = inputs.get("endpoint", "-")
        self.prepare = prepare
        self.settle = settle
        self.cadence = cadence
        self.term_grace = term_grace
        self.kill_grace = kill_grace
        self.owner = identity(os.getpid())
        self.interrupted = False
        # Opening supervises: a claim whose owner is gone stops the campaign.
        self.journal = Journal(self.directory, inputs, clock=time.time, alive=alive)
        self.orphans = self._reap_orphans()
        # A plan cannot know where a run's evidence goes: a successor carries
        # its predecessor's arguments into runs with new orders in a new
        # directory. Refused before any claim, so no cell is spent on it.
        for run in self.journal.plan:
            if EVIDENCE_ROOT in (run.get("arguments") or []):
                raise Failure(f"Run {run['order']} names its own {EVIDENCE_ROOT}")

    def _reap_orphans(self):
        """Stops runs a dead controller left running, and names them.

        A controller killed outright leaves its forked run in a group of its
        own, still writing to a real queue; the journal stops the campaign for
        the lost claim, and this stops the process. It refuses to open over a
        claim whose owner is alive — two controllers on one campaign would
        each stop the other's run — and it acts only on a worker whose PID
        still has its recorded start, or whose PID is gone while its group
        is not — what the run started outlives it there. A PID that now
        names another process is left alone.

        Two limits. A controller killed between the fork and writing
        ``worker.json`` leaves a run nothing here can name. And off Linux the
        start comes from ``ps`` to the second, so a PID reused within the
        same second would pass for the run; the rig is Linux, where the start
        is in clock ticks.
        """
        cells = {run["order"]: run["cellId"] for run in self.journal.plan}
        reaped = []
        for order, owner in self.journal.claims().items():
            if owner == self.owner:
                # This process stopped the campaign over its own run, which
                # it had already seen exit.
                continue
            if alive(owner):
                raise Failure(
                    f"Campaign is driven by process {owner['pid']} on {owner['host']}"
                )
            evidence = run_directory(self.directory, order, cells[order])
            record = evidence / WORKER
            if not record.exists():
                continue
            worker = json.loads(record.read_text())
            if worker["host"] != socket.gethostname():
                continue
            current = start_time(worker["pid"])
            if current is not None and current != worker["started"]:
                # The PID now names another process.
                continue
            if current is None and _group_gone(worker["pid"]):
                continue
            # Either the run itself, or what it left in its group after it
            # exited. A group's id is not handed out again while any member
            # remains, so a group that still exists is still the run's.
            if not self._stop_group(worker["pid"], order=None):
                raise Failure(f"Orphaned run {evidence.name} would not exit")
            reaped.append(evidence.name)
        return reaped

    def interrupt(self, *_):
        """Asks the campaign to end after the current run, without losing it."""
        self.interrupted = True

    def drive(self):
        """Runs until the campaign is done, stopped or interrupted.

        An interrupt is honoured only between runs. A run already forked is
        waited for and recorded, so a restarted controller finds a campaign
        with no claim standing and continues from the next run.
        """
        with self._driving():
            return self._drive()

    @contextlib.contextmanager
    def _driving(self):
        """Holds the campaign for this controller alone while it drives.

        A claim keeps two controllers apart only while a run is claimed; the
        settling after a run, and the settling a new controller owes first,
        happen with none standing. An exclusive lock taken without waiting
        covers the whole drive, and the kernel releases it when the process
        dies, so a crashed controller never holds a campaign it cannot drive.
        """
        with open(self.directory / DRIVING, "a+") as guard:
            try:
                fcntl.flock(guard.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise Failure("Campaign is driven by another controller") from error
            try:
                yield
            finally:
                fcntl.flock(guard.fileno(), fcntl.LOCK_UN)

    def _drive(self):
        previous = {
            sent: signal.signal(sent, self.interrupt)
            for sent in (signal.SIGTERM, signal.SIGINT)
        }
        try:
            self._settle_owed()
            while not self.interrupted:
                run = self.journal.claim(self.owner)
                if run is None:
                    break
                self.execute(run)
        finally:
            for sent, handler in previous.items():
                signal.signal(sent, handler)
        return self.journal.counts()

    def execute(self, run):
        """Forks one claimed run and records what became of it.

        The deadline counts from the claim, so the reservation covers the
        whole run: preparing it, the JVM, stopping it, and recording it.
        """
        begun = time.monotonic()
        order = run["order"]
        arguments = run.get("arguments")
        if not isinstance(arguments, list) or not all(
            isinstance(argument, str) for argument in arguments
        ):
            self._stop(order, "carries no probe arguments")
        run_id = option(arguments, "--run-id")
        if run_id is None:
            self._stop(order, "names no --run-id")
        evidence = run_directory(self.directory, order, run["cellId"])
        try:
            evidence.mkdir(parents=True)
        except FileExistsError:
            self._stop(order, "already has an evidence directory")
        if self.prepare is not None:
            try:
                self.prepare(run, lambda: self.journal.heartbeat(order, self.owner))
            # A hook is the host's code, and whatever it raises leaves this
            # run unstarted; which exception it was is for the reason to say.
            except Exception as error:  # noqa: BLE001
                # Prepare undoes its own partial work, so nothing is owed.
                _owes_nothing(self.directory, run)
                self._stop(order, f"could not be prepared: {error}")
        try:
            record = self._observe(run, run_id, evidence, begun)
        except BaseException:
            # The campaign stops for the reason already in flight; settling
            # is still owed whatever prepare set up, and cannot replace it. A
            # settle that fails here is left owed for the next controller.
            with contextlib.suppress(Exception):
                self._settle(run, None)
            raise
        try:
            self._settle(run, record)
        except Exception as error:  # noqa: BLE001
            # The outcome is recorded; what is lost is the host's cleanup,
            # and the next run should not start without it.
            self._stop(order, f"could not be settled: {error}")
        return record["outcome"]

    def _settle(self, run, record):
        """Calls the settle hook, if any, and records that the run owes none.

        The claim is released before settling, so the marker is what tells a
        controller started after a crash that a run is still owed its
        settling, rather than claiming the next run beside it. It is written
        with or without a hook, so a campaign resumed with one is not handed
        runs that finished before it was given.
        """
        if self.settle is not None:
            self.settle(run, record)
        _owes_nothing(self.directory, run)

    def _settle_owed(self):
        """Settles every run a previous controller left unsettled.

        A run still claimed lost its outcome, so it is settled without one;
        a finished run is settled with the record it left. A settle hook is
        therefore called again after a crash and must tolerate that.
        """
        claimed = self.journal.claims()
        for owner in claimed.values():
            # A claim opened after this controller was built is another
            # controller's run in progress, not a run owed its settling.
            if owner != self.owner and alive(owner):
                raise Failure(
                    f"Campaign is driven by process {owner['pid']} on {owner['host']}"
                )
        failures = []
        for run in self.journal.plan:
            evidence = run_directory(self.directory, run["order"], run["cellId"])
            if not evidence.is_dir() or (evidence / SETTLED).exists():
                continue
            lost = run["order"] in claimed or not (evidence / OUTCOME).exists()
            record = None if lost else json.loads((evidence / OUTCOME).read_text())
            try:
                self._settle(run, record)
            except Exception as error:  # noqa: BLE001
                failures.append(f"run {run['order']} could not be settled: {error}")
        if failures:
            # Named here rather than only through the journal, whose stop
            # keeps its first reason: a campaign that lost a run is already
            # stopped for that, and the cleanup it is still owed would vanish.
            self.journal.stop(failures[0])
            raise Failure("; ".join(failures))

    def _observe(self, run, run_id, evidence, begun):
        order = run["order"]
        deadline = begun + run["reserves"]["seconds"]
        probe = (evidence / PROBE).absolute()
        argv = [
            *self.launcher,
            str(probe),
            self.endpoint,
            *run["arguments"],
            EVIDENCE_ROOT,
            evidence_root(probe),
        ]
        with (
            open(evidence / STDOUT, "wb") as stdout,
            open(evidence / STDERR, "wb") as stderr,
        ):
            child = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                # Its own group, so stopping it reaches whatever it started.
                start_new_session=True,
            )
        try:
            forked = time.time()
            _write_json(
                evidence / WORKER,
                {
                    "host": socket.gethostname(),
                    "pid": child.pid,
                    "started": start_time(child.pid),
                },
            )
            killed, status = self._wait(child, order, deadline)
        except BaseException:
            # Whatever interrupted the wait, the run must not outlive the
            # controller that was supervising it.
            self._stop_group(child.pid, order, child)
            raise
        # The probe exited, but something it started may still be in its group.
        if not _group_gone(child.pid) and not self._stop_group(child.pid, order):
            self._stop(order, "left a process that would not exit")
        # Recording is local disk work, but a large evidence directory on a slow
        # disk still takes time, so the claim is refreshed before it and
        # between hashing the evidence and measuring it; finishing supervises
        # the claim itself.
        self.journal.heartbeat(order, self.owner)
        for name in SCRATCH:
            shutil.rmtree(evidence / PROBE / name, ignore_errors=True)
        record = {
            "argv": argv,
            "pid": child.pid,
            "forked": forked,
            "exitStatus": status,
            "killed": killed,
        }
        stdout = (evidence / STDOUT).read_text(errors="replace")
        try:
            outcome, fields = parse(stdout, run_id, run["cellId"])
        except Unrecordable as reason:
            if not killed:
                record["unrecordable"] = str(reason)
                self._record(evidence, record, order)
                self._stop(order, str(reason))
            outcome, fields = FAILED, None
            text = f"outlived its reservation of {run['reserves']['seconds']} seconds"
        else:
            record["summary"] = fields
            text = detail(fields)
            if killed:
                # The probe prints its line and then waits for its threads, so
                # a JVM that would not exit afterwards is a slow shutdown, not
                # a lost measurement.
                text += "; did not exit before its reservation"
        record["outcome"] = outcome
        self._record(evidence, record, order)
        self.journal.heartbeat(order, self.owner)
        consumed = {
            "bytes": _size(evidence),
            "seconds": math.ceil(time.monotonic() - begun),
        }
        self.journal.finish(order, self.owner, outcome, consumed, text)
        return record

    def _wait(self, child, order, deadline):
        """Waits on the run, heartbeating, until it exits or its deadline passes."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 and child.poll() is None:
                if not self._stop_group(child.pid, order, child):
                    self._stop(order, "outlived its reservation and would not exit")
                return True, child.returncode
            try:
                return False, child.wait(timeout=max(0, min(self.cadence, remaining)))
            except subprocess.TimeoutExpired:
                self.journal.heartbeat(order, self.owner)

    def _stop_group(self, group, order, child=None):
        """Stops a run's whole group; answers whether it is gone.

        It heartbeats before the first signal and after each grace, so
        stopping a run leaves no longer gap in the claim's heartbeats than
        waiting on it does.
        """
        if order is not None:
            self._heartbeat(order)
        for sent, grace in (
            (signal.SIGTERM, self.term_grace),
            (signal.SIGKILL, self.kill_grace),
        ):
            try:
                os.killpg(group, sent)
            except ProcessLookupError:
                pass
            until = time.monotonic() + grace
            if child is not None:
                try:
                    child.wait(timeout=grace)
                except subprocess.TimeoutExpired:
                    pass
            if (child is None or child.poll() is not None) and _wait_group(
                group, until
            ):
                return True
            if order is not None:
                self._heartbeat(order)
        return False

    def _heartbeat(self, order):
        # A heartbeat that fails here means the campaign stopped underneath
        # the controller; stopping the run still has to finish.
        try:
            self.journal.heartbeat(order, self.owner)
        except Failure:
            pass

    def _record(self, evidence, record, order):
        record["files"] = files(
            evidence, tick=lambda: self.journal.heartbeat(order, self.owner)
        )
        _write_json(evidence / OUTCOME, record)

    def _stop(self, order, reason):
        stop = self.journal.stop(f"run {order} {reason}")
        raise Failure(f"Campaign stopped: {stop['reason']}")
