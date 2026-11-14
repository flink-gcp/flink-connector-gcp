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
"""The Firestore exercise's late seed records, its reads and the verdict they decide."""

import dataclasses
from types import SimpleNamespace

import pytest
from flink_tier3.common import TransportError
from flink_tier3.exercise import TIME_LIMIT_SECONDS, RecoveryExercise
from flink_tier3.firestore import database
from flink_tier3.firestore.database import Databases
from flink_tier3.firestore.exercise import (
    ORACLE_STAGE,
    FirestoreExercise,
    FirestoreSequenceExercise,
    firestore_exercise,
)
from flink_tier3.firestore.trial import LATE_RECORDS, MODES, Trial
from flink_tier3.firestore.verdict import OBSERVED, SAMPLED_STAGES

from ..cloudtasks.test_plan import inputs as inputs  # noqa: PLC0414
from ..cloudtasks.test_plan import renderer as renderer  # noqa: PLC0414
from ..cloudtasks.test_runtime import prepared as prepared  # noqa: PLC0414
from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_documents import COMMITTED, READ_AT, Firestore, stored


def plan_line(trial, mode):
    """The JobManager's line for a plan of three splits, in the enumerator's words."""
    what = (
        "collection group 'seed'"
        if mode == "native"
        else "a query of kind 'seed' in the default namespace"
    )
    return (
        "2026-10-28 14:43:09,123 INFO  io.github.flink.gcp.connector.firestore.source"
        ".batch.enumerator.FirestoreBatchSplitEnumerator [] - Planned 3 split(s) for"
        f" {what} of {trial.database_path(mode)} at read time"
        " 2026-10-28T00:20:00.123456000Z and parallelism 2."
    )


@pytest.fixture
def baseline(prepared, monkeypatch):
    """A Firestore exercise in its baseline, over a five-record input and six seed records."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    small = Trial(environment.approval.run_id, 5, 1, 6, 1)
    environment.approval = dataclasses.replace(
        environment.approval, scenario="firestore-recovery"
    )
    monkeypatch.setattr(
        type(environment.approval), "firestore_plan", property(lambda self: small)
    )
    service = Firestore(environment.clock, small)
    service.databases = {mode: {} for mode in MODES}
    service.ready_at = {mode: 0 for mode in MODES}
    service.data = stored(small, late=False)
    controller = SimpleNamespace(
        env=environment,
        trial=small,
        listing=lambda until: Databases(service, small, until, environment.clock),
    )
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = FirestoreExercise(environment, upgrade, controller)
    value.persist("baseline")
    return value, service


@pytest.fixture
def exercise(baseline):
    """The exercise once both recoveries are proved and the input finished."""
    value, service = baseline
    environment = value.env
    service.data = stored(value.trial)
    late = {"commit_time": COMMITTED, "records": LATE_RECORDS}
    value.outcomes.update(
        upgrade={}, failover={}, late_seed={mode: late for mode in MODES}
    )
    value.coverage = {
        window: {"attempts": 1, "observed": sorted(OBSERVED)}
        for window in SAMPLED_STAGES
    }
    # Progress counts only after the exercise's boundary, the window's start.
    environment.sleep(10)
    value.samples[("tm", 0)] = {
        "at": environment.clock(),
        "phase": "upgrade",
        "processed": 5,
        "lineage": "x",
        "sequence": 4,
        "pod_uid": "tm",
    }
    value.persist("complete")
    assert value.stage == ORACLE_STAGE
    return value, service


def test_every_collection_is_read_to_its_last_page_and_judged(exercise, monkeypatch):
    monkeypatch.setattr(database, "PAGE", 2)
    value, service = exercise
    service.errors[("runQuery", "datastore")] = TransportError("timeout")
    value.verify_tables()
    record = value.env.refresh().recovery
    assert (record["stage"], record["verdict"], record["reasons"]) == (
        "complete",
        "usable",
        [],
    )
    oracles = record["outcomes"]["oracles"]
    for mode in MODES:
        assert oracles[mode + "_sink"]["matched"] == 5
        assert oracles[mode + "_batch"]["matched"] == 6
        assert oracles[mode + "_batch"]["seed"]["matched"] == 6 + LATE_RECORDS
    assert oracles["read_times"] == {mode: READ_AT for mode in MODES}
    # Each database's first page is read now and every later one at its time.
    for mode in MODES:
        times = [query[3] for query in service.queries if query[0] == mode]
        assert times[0] is None and set(times[1:]) == {READ_AT}


def test_a_copy_missing_in_one_database_fails_that_component_only(exercise):
    value, service = exercise
    del service.data["datastore"]["batch_out"][
        min(service.data["datastore"]["batch_out"])
    ]
    value.verify_tables()
    record = value.env.refresh().recovery
    assert record["reasons"] == ["datastore_batch-oracle-not-passed"]


def test_late_records_wait_for_a_copy_and_the_upgrade_for_both_records(
    baseline, monkeypatch
):
    """Records written before the plan would be copied; an upgrade before
    them leaves the batch oracle nothing to judge."""
    value, service = baseline
    upgrades = []
    monkeypatch.setattr(
        RecoveryExercise, "begin_upgrade", lambda self, *args: upgrades.append(args)
    )
    copies = service.data["datastore"].pop("batch_out")
    value.begin_upgrade({}, {}, [], [])
    assert upgrades == [] and list(value.outcomes["late_seed"]) == ["native"]
    assert service.writes == [("native", LATE_RECORDS)]
    service.data["datastore"]["batch_out"] = copies
    # A commit that said nothing, or aborted with 409, waits for the next poll.
    for error in (TransportError("timeout"), 409):
        service.errors[("commit", "datastore")] = error
        value.begin_upgrade({}, {}, [], [])
        assert upgrades == [] and "datastore" not in value.outcomes["late_seed"]
    value.begin_upgrade({}, {}, [], [])
    assert service.writes[-1] == ("datastore", LATE_RECORDS)
    assert set(service.data["datastore"]["seed"]) >= set(value.trial.late_keys())
    recorded = value.env.refresh().recovery["outcomes"]["late_seed"]
    assert recorded == {
        mode: {"commit_time": COMMITTED, "records": LATE_RECORDS} for mode in MODES
    }
    assert len(upgrades) == 1


def test_each_batch_plan_is_read_from_the_jobmanager_log(baseline):
    value, _ = baseline
    pod = {"metadata": {"uid": "jm", "labels": {"component": "jobmanager"}}}
    lines = "".join(plan_line(value.trial, mode) + "\n" for mode in MODES)
    # Overlapping log reads return a line again, which is the same plan.
    value.progress(pod, lines)
    value.progress(pod, lines)
    one = (
        "2026-10-28 14:43:10,000 INFO  x [] - Planned one split for a query of kind"
        f" 'seed' in the default namespace of {value.trial.database_path('datastore')}"
        " at read time 2026-10-28T00:21:00Z: the query is read as one split because"
        " it has a limit."
    )
    value.progress(pod, one + "\n")
    # Fewer splits than the parallelism is a warning, which a semicolon follows.
    fewer = (
        "2026-10-28 14:43:11,000 WARN  x [] - Planned 1 split(s) for collection group"
        f" 'seed' of {value.trial.database_path('native')} at read time"
        " 2026-10-28T00:22:00Z and parallelism 2; the subtasks left without one"
        " finish immediately. The service returns fewer partitions than asked for"
        " when the collection group is small."
    )
    value.progress(pod, fewer + "\n")
    assert value.outcomes["batch_plan"] == {
        "native": {"splits": 1, "read_time": "2026-10-28T00:22:00Z", "plans": 2},
        "datastore": {"splits": 1, "read_time": "2026-10-28T00:21:00Z", "plans": 2},
    }


def built(prepared, monkeypatch, kind):
    """The exercise the supervisor builds for a small trial of `kind`."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    small = Trial(environment.approval.run_id, 5, 1, 6, 1, kind=kind)
    environment.approval = dataclasses.replace(
        environment.approval, scenario="firestore-recovery"
    )
    monkeypatch.setattr(
        type(environment.approval), "firestore_plan", property(lambda self: small)
    )
    service = Firestore(environment.clock, small)
    service.databases = {mode: {} for mode in MODES}
    service.ready_at = {mode: 0 for mode in MODES}
    service.data = stored(small)
    controller = SimpleNamespace(
        env=environment,
        trial=small,
        listing=lambda until: Databases(service, small, until, environment.clock),
    )
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = firestore_exercise(environment, upgrade, controller)
    value.persist("baseline")
    return value, service


@pytest.fixture
def sequence(prepared, monkeypatch):
    """The fault-sequence exercise over the same small input."""
    return built(prepared, monkeypatch, "sequence")


@pytest.mark.parametrize("kind", ["jm-replacement", "tm-replacement", "rescale-in"])
def test_a_single_fault_trial_keeps_its_exercise(prepared, monkeypatch, kind):
    value, _ = built(prepared, monkeypatch, kind)
    assert type(value) is FirestoreExercise


def test_the_sequence_trial_builds_the_sequence_exercise(sequence):
    value, _ = sequence
    assert isinstance(value, FirestoreSequenceExercise)
    assert value.measurement_event is None
    assert value.deadline == value.env.schedule.started + TIME_LIMIT_SECONDS


def test_the_first_fault_waits_for_both_late_seeds(sequence):
    value, service = sequence
    service.data = stored(value.trial, late=False)
    # The Datastore-mode read has not copied yet, so its read time is unplanned.
    copies = service.data["datastore"]["batch_out"]
    service.data["datastore"]["batch_out"] = {}
    assert value.ready_for_sequence() is False
    assert set(value.outcomes["late_seed"]) == {"native"}
    service.data["datastore"]["batch_out"] = copies
    assert value.ready_for_sequence() is True


def test_a_sequence_run_is_judged_by_its_data_alone(sequence):
    """No coverage, proofs or restore counts; the four oracles decide."""
    value, _ = sequence
    late = {"commit_time": COMMITTED, "records": LATE_RECORDS}
    value.outcomes.update(late_seed={mode: late for mode in MODES})
    value.outcomes["unplanned"] = ["Unplanned workload interruption"]
    value.env.sleep(10)
    value.samples[("tm", 0)] = {
        "at": value.env.clock(),
        "phase": "upgrade",
        "processed": 5,
        "lineage": "x",
        "sequence": 4,
        "pod_uid": "tm",
    }
    value.persist("complete")
    value.verify_tables()
    record = value.env.refresh().recovery
    assert record["verdict"] == "usable", record["reasons"]
