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
"""The run's instance, database, seed and split points, through the REST client."""

from types import SimpleNamespace

import pytest
import requests
from flink_tier3.common import ApiError, Failure, digest
from flink_tier3.resource import require_resources_clean
from flink_tier3.spanner import plan
from flink_tier3.spanner.instance import TABLES, Instance
from flink_tier3.spanner.lifecycle import SpannerLifecycle
from flink_tier3.spanner.trial import Trial

from ..bigquery.test_resources import Response
from ..cloudtasks.test_recovery_lifecycle import NOW
from ..cloudtasks.test_recovery_lifecycle import Environment as QueueEnvironment

# The seed every dispatched trial writes.
TRIAL = Trial("sp-lifecycle", 100, 10, "2026-10-28T01:05:00Z", **plan.SEED)
DEADLINE = NOW + 600
INSTANCE, DATABASE = TRIAL.instance_path, TRIAL.database_path


def application():
    return {
        "metadata": {"name": TRIAL.run_id, "namespace": "tier3-spanner"},
        "spec": {"job": {"args": TRIAL.arguments("initial")}},
    }


class Environment(QueueEnvironment):
    def __init__(self):
        super().__init__()
        self.approval = SimpleNamespace(
            scenario="spanner-recovery",
            run_id=TRIAL.run_id,
            nonce="a" * 32,
            spanner_plan=TRIAL,
            application_sha256=digest(application()),
        )
        self.records.approval = self.approval


class Spanner:
    """The service's answers for one instance name, as #1877 measured them.

    The instance and the database read `CREATING` for `creating` seconds after
    their create; a database create in an instance that is not READY, and a
    commit to a database that is not, are refused with 400. A commit over
    80,000 mutations is refused with 400, a split-point entry of more than one
    key with 400, and a second split point within `window` seconds of the last
    with 429, as is a request carrying two.
    """

    def __init__(self, clock):
        self.clock = clock
        self.instance = self.database = None
        self.creating, self.ready_at = 30, {}
        self.statements, self.rows, self.splits = [], set(), []
        self.calls, self.errors, self.mutations = [], {}, []
        self.window, self.last_split = 60, None

    def request(self, method, url, json=None, **_):
        operation = self._operation(method, url.removeprefix("https://"))
        self.calls.append(operation)
        if operation in self.errors:
            error = self.errors.pop(operation)
            if isinstance(error, Exception):
                raise error
            return Response({}, error)
        return getattr(self, operation)(json)

    @staticmethod
    def _operation(method, url):
        path = url.removeprefix("spanner.googleapis.com/v1/")
        return {
            ("GET", INSTANCE): "get",
            ("POST", "projects/flink-gcp/instances"): "create",
            ("POST", INSTANCE + "/databases"): "create_database",
            ("GET", DATABASE): "read_database",
            ("GET", DATABASE + "/ddl"): "ddl",
            ("POST", DATABASE + "/sessions"): "session",
            ("POST", DATABASE + ":addSplitPoints"): "split",
            ("DELETE", INSTANCE): "delete",
        }.get((method, path)) or ("commit" if path.endswith(":commit") else path)

    def _ready(self, resource):
        """Whether `resource` exists and reads READY now, settling it on time."""
        if resource is None:
            return False
        if resource["state"] == "CREATING" and self.clock() >= self.ready_at.get(
            resource["name"], 0
        ):
            resource["state"] = "READY"
        return resource["state"] == "READY"

    def _created(self, resource):
        self.ready_at[resource["name"]] = self.clock() + self.creating
        return resource

    def _settling(self, resource):
        if resource is None:
            return Response({}, 404)
        self._ready(resource)
        return Response(resource)

    def get(self, _):
        return self._settling(self.instance)

    def create(self, body):
        if self.instance is not None:
            return Response({}, 409)
        assert body["instance"]["processingUnits"] == 100
        assert 4 <= len(body["instance"]["displayName"]) <= 30
        self.instance = self._created({"name": INSTANCE, "state": "CREATING"})
        return Response({"name": INSTANCE + "/operations/create"})

    def create_database(self, body):
        if not self._ready(self.instance):
            return Response({}, 400)
        if self.database is not None:
            return Response({}, 409)
        self.statements = list(body["extraStatements"])
        retention = (
            "2h"
            if any("version_retention_period = '2h'" in s for s in self.statements)
            else "1h"
        )
        self.database = self._created(
            {
                "name": DATABASE,
                "state": "CREATING",
                "versionRetentionPeriod": retention,
            }
        )
        return Response({"name": DATABASE + "/operations/create"})

    def read_database(self, _):
        return self._settling(self.database)

    def ddl(self, _):
        if self.database is None:
            return Response({}, 404)
        return Response({"statements": self.statements})

    def session(self, body):
        assert body == {"session": {"multiplexed": True}}
        return Response({"name": DATABASE + "/sessions/multiplexed"})

    def commit(self, body):
        assert body["singleUseTransaction"] == {"readWrite": {}}
        if not self._ready(self.database):
            return Response({}, 400)
        mutations = sum(
            len(write["values"]) * len(write["columns"])
            for mutation in body["mutations"]
            for write in mutation.values()
        )
        self.mutations.append(mutations)
        if mutations > 80000:
            return Response({}, 400)
        for mutation in body["mutations"]:
            self.rows.update(key for key, _ in mutation["insertOrUpdate"]["values"])
        return Response({"commitTimestamp": "2026-10-28T00:10:00Z"})

    def split(self, body):
        entries = body["splitPoints"]
        if any(len(entry["keys"]) != 1 for entry in entries):
            return Response({}, 400)
        now = self.clock()
        if len(entries) > 1 or (
            self.last_split is not None and now - self.last_split < self.window
        ):
            return Response({}, 429)
        self.last_split = now
        self.splits.append(entries[0]["keys"][0]["keyParts"][0])
        return Response({})

    def delete(self, _):
        self.instance = self.database = None
        return Response({})


@pytest.fixture
def env():
    return Environment()


@pytest.fixture
def service(env):
    return Spanner(env.clock)


def controller(env, service, role="runner"):
    env.actor = role
    api = Instance(service, TRIAL, DEADLINE, env.clock)
    return SpannerLifecycle(env, api, application())


def test_admission_creates_seeds_and_splits_the_database_and_keeps_the_readback(
    env, service
):
    runner = controller(env, service)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    assert service.calls[:3] == ["get", "create", "get"]
    assert receipt["database"]["tables"] == sorted(TABLES)
    assert receipt["database"]["versionRetentionPeriod"] == "2h"
    assert service.rows == set(TRIAL.seed_keys())
    # Each commit stays within the 80,000 mutations the service takes.
    assert len(service.mutations) == 12 and max(service.mutations) == 20000
    assert service.splits == ["4", "8", "c"] and service.calls.count("split") == 3
    state = env.refresh().spanner
    assert (state["creation_intent"], state["inflight"], state["instance"]) == (
        True,
        None,
        receipt,
    )


def test_an_existing_instance_is_never_this_runs(env, service):
    service.instance = {"name": INSTANCE, "state": "READY"}
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(Failure, match="already exists"):
        runner.provision(DEADLINE)
    assert env.refresh().spanner["creation_intent"] is False


def test_a_refused_create_drops_the_intent_and_a_later_failure_keeps_it(env, service):
    service.errors["create"] = 409
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    assert env.refresh().spanner["creation_intent"] is False
    other = Environment()
    service = Spanner(other.clock)
    # The runner's role lacking a permission the database create needs.
    service.errors["create_database"] = 403
    runner = controller(other, service)
    runner.initialize()
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    # The instance exists, so cleanup must delete it.
    state = other.refresh().spanner
    assert (state["creation_intent"], state["inflight"]) == (True, None)


@pytest.mark.parametrize("landed", [True, False])
def test_a_database_create_that_did_not_answer_is_read_back(env, service, landed):
    """The create may run on in the service after the transport gave up."""
    create = service.create_database

    def slow(body):
        if "slow" not in service.calls:
            service.calls.append("slow")
            if landed:
                create(body)
            raise requests.exceptions.ReadTimeout()
        return create(body)

    service.create_database = slow
    runner = controller(env, service)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    assert receipt["database"]["tables"] == sorted(TABLES)
    # Landed, the read-back finds it; not landed, the create is repeated.
    assert service.calls.count("create_database") == (1 if landed else 2)
    assert "spanner-create-uncertain" in [event for event, _ in env.events]


def test_a_create_never_seen_to_land_keeps_its_marker(env, service):
    """Its operation could still create the instance after cleanup found none."""
    service.create = lambda body: Response({"name": INSTANCE + "/operations/lost"})
    runner = controller(env, service)
    runner.initialize()
    # The client's deadline ends the reads before the wait between them does.
    with pytest.raises(Failure, match="deadline exceeded"):
        runner.provision(DEADLINE)
    assert env.refresh().spanner["inflight"] is not None
    with pytest.raises(Failure, match="in flight"):
        runner.release()


def test_a_split_point_refused_within_the_minute_is_retried(env, service):
    """The service counts its own minute; a 429 is a wait, not a failed run."""
    service.window = 70
    runner = controller(env, service)
    runner.initialize()
    runner.provision(DEADLINE)
    assert service.splits == ["4", "8", "c"]
    assert service.calls.count("split") > 3


def test_a_seed_commit_that_aborted_or_did_not_answer_is_repeated(env, service):
    """A single-use commit can abort with 409; each seed commit is an
    insert-or-update of fixed values, so a repeat writes nothing new."""
    commit = service.commit
    failures = [Response({}, 409), requests.exceptions.ReadTimeout()]

    def flaky(body):
        if failures:
            failure = failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        return commit(body)

    service.commit = flaky
    runner = controller(env, service)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    assert receipt["seed_rows"] == TRIAL.seed_rows
    assert service.rows == set(TRIAL.seed_keys())
    assert failures == [] and service.calls.count("commit") == 12 + 2


def test_cleanup_deletes_the_instance_and_retries_a_refused_deletion(env, service):
    runner = controller(env, service)
    runner.initialize()
    runner.provision(DEADLINE)
    runner.release()
    supervisor = controller(env, service, "supervisor")
    service.errors["delete"] = 400
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    service.errors["delete"] = requests.exceptions.ReadTimeout()
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is True
    assert service.instance is None
    assert [event for event, _ in env.events].count("spanner-delete-pending") == 2
    record = env.refresh()
    record.stop_requested = True
    require_resources_clean(record)


@pytest.mark.parametrize("deviation", ["retention", "change stream", "table"])
def test_a_database_that_reads_back_otherwise_is_not_admitted(env, service, deviation):
    """Without its retention a restored batch read resumes at a snapshot the
    database no longer keeps; without its change stream nothing is relayed,
    and without a table its component has nowhere to write."""
    create = service.create_database
    missing = {"change stream": "CREATE CHANGE STREAM", "table": "CREATE TABLE seed"}

    def deviating(body):
        response = create(body)
        if deviation == "retention":
            service.database["versionRetentionPeriod"] = "1h"
        else:
            service.statements = [
                s for s in service.statements if not s.startswith(missing[deviation])
            ]
        return response

    service.create_database = deviating
    runner = controller(env, service)
    runner.initialize()
    with pytest.raises(Failure, match="differs from its configuration"):
        runner.provision(DEADLINE)
    assert "spanner-database-deviation" in [event for event, _ in env.events]
    # Nothing is seeded, and the instance is cleanup's to delete.
    assert service.rows == set()
    state = env.refresh().spanner
    assert (state["creation_intent"], state["inflight"]) == (True, None)


def test_display_names_fit_spanner_and_differ_between_long_run_ids():
    # Spanner refuses a display name over 30 characters or one another
    # instance in the project already holds.
    first, second = ("a" * 14 + suffix for suffix in ("b" * 26, "c" * 26))
    names = {
        Trial(run_id, 100, 10, "2026-10-28T01:05:00Z", **plan.SEED).display_name
        for run_id in (first, second, "sp-lifecycle")
    }
    assert len(names) == 3
    assert all(4 <= len(name) <= 30 for name in names)
    assert "sp1360 sp-lifecycle" in names
