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
"""The run's two databases and their seeds, through the REST client."""

from types import SimpleNamespace

import pytest
import requests
from flink_tier3.common import ApiError, Failure, digest
from flink_tier3.firestore import plan
from flink_tier3.firestore.database import Databases
from flink_tier3.firestore.lifecycle import FirestoreLifecycle
from flink_tier3.firestore.trial import MODES, Trial
from flink_tier3.resource import require_resources_clean

from ..cloudtasks.test_recovery_lifecycle import NOW
from ..cloudtasks.test_recovery_lifecycle import Environment as QueueEnvironment
from .test_documents import Firestore

# The seed every dispatched trial writes.
TRIAL = Trial("fs-lifecycle", 100, 10, **plan.SEED)
DEADLINE = NOW + 600


def application():
    return {
        "metadata": {"name": TRIAL.run_id, "namespace": "tier3-firestore"},
        "spec": {"job": {"args": TRIAL.arguments("initial")}},
    }


class Environment(QueueEnvironment):
    def __init__(self):
        super().__init__()
        self.approval = SimpleNamespace(
            scenario="firestore-recovery",
            run_id=TRIAL.run_id,
            nonce="a" * 32,
            firestore_plan=TRIAL,
            application_sha256=digest(application()),
        )
        self.records.approval = self.approval


@pytest.fixture
def env():
    return Environment()


@pytest.fixture
def service(env):
    return Firestore(env.clock, TRIAL)


def controller(env, service, role="runner"):
    env.actor = role
    api = Databases(service, TRIAL, DEADLINE, env.clock)
    return FirestoreLifecycle(env, api, application())


def test_admission_creates_both_databases_and_seeds_each(env, service):
    runner = controller(env, service)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    # Neither name is taken, the Native-mode database is created and read
    # until it exists, then the Datastore-mode one under the same marker.
    assert service.calls[:4] == [
        ("get", "native"),
        ("get", "datastore"),
        ("create", "native"),
        ("get", "native"),
    ]
    assert ("create", "datastore") in service.calls
    assert receipt["databases"]["native"]["type"] == "FIRESTORE_NATIVE"
    assert receipt["databases"]["datastore"]["type"] == "DATASTORE_MODE"
    for mode in MODES:
        assert service.data[mode]["seed"].keys() == TRIAL.seed_keys().keys()
    # Each commit stays within the 500 writes either API takes.
    assert {count for _, count in service.writes} == {500}
    assert len(service.writes) == 2 * TRIAL.seed_records // 500
    state = env.refresh().firestore
    assert (state["creation_intent"], state["inflight"], state["databases"]) == (
        True,
        None,
        receipt,
    )
    assert state["intent"]["databases"] == [
        f"projects/flink-gcp/databases/fs1879-fs-lifecycle-{suffix}"
        for suffix in ("n", "d")
    ]


@pytest.mark.parametrize("mode", MODES)
def test_an_existing_database_is_never_this_runs(env, service, mode):
    service.databases[mode], service.ready_at[mode] = {}, 0
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(Failure, match="already exists"):
        runner.provision(DEADLINE)
    assert env.refresh().firestore["creation_intent"] is False
    assert ("create", "native") not in service.calls


@pytest.mark.parametrize("landed", [True, False])
def test_a_second_create_that_did_not_answer_is_read_back(env, service, landed):
    """The create may run on in the service after the transport gave up."""
    create = service.create

    def slow(mode, body):
        if mode == "datastore" and "slow" not in service.calls:
            service.calls.append("slow")
            if landed:
                create(mode, body)
            raise requests.exceptions.ReadTimeout()
        return create(mode, body)

    service.create = slow
    # A landed create reads back at once, so it is not created again.
    service.creating = 0
    runner = controller(env, service)
    runner.initialize()
    runner.provision(DEADLINE)
    # Landed, the read-back finds it; not landed, the create is repeated.
    assert service.calls.count(("create", "datastore")) == (1 if landed else 2)
    assert "firestore-create-uncertain" in [event for event, _ in env.events]


def test_a_database_that_reads_back_otherwise_is_not_admitted(env, service):
    create = service.create

    def elsewhere(mode, body):
        response = create(mode, body)
        service.databases[mode]["locationId"] = "nam5"
        return response

    service.create = elsewhere
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(Failure, match="differs from its configuration"):
        runner.provision(DEADLINE)
    assert "firestore-database-deviation" in [event for event, _ in env.events]
    # Nothing is seeded, and both databases are cleanup's to delete.
    assert service.writes == []
    state = env.refresh().firestore
    assert (state["creation_intent"], state["inflight"]) == (True, None)


def test_cleanup_deletes_both_databases_and_retries_until_they_are_gone(env, service):
    runner = controller(env, service)
    runner.initialize()
    runner.provision(DEADLINE)
    runner.release()
    supervisor = controller(env, service, "supervisor")
    service.errors[("delete", "native")] = 400
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    service.errors[("delete", "datastore")] = requests.exceptions.ReadTimeout()
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is True
    assert service.databases == {}
    assert [event for event, _ in env.events].count("firestore-delete-pending") == 2
    record = env.refresh()
    record.stop_requested = True
    require_resources_clean(record)


def test_a_refused_create_drops_the_intent(env, service):
    service.errors[("create", "native")] = 409
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    assert env.refresh().firestore["creation_intent"] is False
