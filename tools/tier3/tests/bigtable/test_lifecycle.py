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
"""Instance intent, admission readback, release and cleanup over a fake instance."""

import copy
from types import SimpleNamespace

import pytest
from flink_tier3.bigtable.instance import ROUTING, TABLES
from flink_tier3.bigtable.lifecycle import BigtableLifecycle
from flink_tier3.bigtable.trial import Trial
from flink_tier3.common import ApiError, Failure, digest
from flink_tier3.resource import require_resources_clean

from ..cloudtasks.test_recovery_lifecycle import NOW
from ..cloudtasks.test_recovery_lifecycle import Environment as QueueEnvironment

TRIAL = Trial("bt-lifecycle", 100, 10, "2026-10-20T01:05:00Z")
DEADLINE = NOW + 600


def application():
    return {
        "metadata": {"name": TRIAL.run_id, "namespace": "tier3-bigtable"},
        "spec": {"job": {"args": TRIAL.arguments("initial")}},
    }


class Environment(QueueEnvironment):
    def __init__(self):
        super().__init__()
        self.approval = SimpleNamespace(
            scenario="bigtable-recovery",
            run_id=TRIAL.run_id,
            nonce="a" * 32,
            bigtable_plan=TRIAL,
            application_sha256=digest(application()),
        )
        self.records.approval = self.approval


class FakeInstance:
    """The service's answers for one instance name.

    The instance reads `CREATING` once after its create, and refuses tables
    and profiles until it is ready; deleting it is refused with 400 while
    `eo` still has a change stream, as the service refuses it.
    """

    def __init__(self):
        self.trial = TRIAL
        self.name = TRIAL.instance_path
        self.instance, self.tables, self.profile = None, {}, None
        self.calls, self.errors = [], {}

    def with_deadline(self, deadline):
        return self

    def _call(self, name):
        self.calls.append(name)
        if name in self.errors:
            raise self.errors.pop(name)

    def get(self):
        self._call("get")
        if self.instance and self.instance["state"] == "CREATING":
            self.instance["state"] = "READY"
            return {"state": "CREATING"}
        return copy.deepcopy(self.instance)

    def create(self):
        self._call("create")
        self.instance = {"state": "CREATING"}

    def _ready(self, operation):
        self._call(operation)
        if not self.instance or self.instance["state"] != "READY":
            raise ApiError(400, "POST", operation)

    def create_table(self, table):
        self._ready("create_table:" + table)
        self.tables[table] = copy.deepcopy(TABLES[table])

    def table(self, table):
        self._call("table:" + table)
        return copy.deepcopy(self.tables.get(table))

    def create_app_profile(self):
        self._ready("create_app_profile")
        self.profile = {"singleClusterRouting": dict(ROUTING)}

    def app_profile(self):
        self._call("app_profile")
        return copy.deepcopy(self.profile)

    def disable_change_stream(self):
        self._call("disable_change_stream")
        if "eo" in self.tables:
            self.tables["eo"].pop("changeStreamConfig", None)

    def delete(self):
        self._call("delete")
        if self.tables.get("eo", {}).get("changeStreamConfig"):
            raise ApiError(400, "DELETE", "instance")
        self.instance, self.tables, self.profile = None, {}, None


@pytest.fixture
def env():
    return Environment()


@pytest.fixture
def api():
    return FakeInstance()


def controller(env, api, role="runner"):
    env.actor = role
    return BigtableLifecycle(env, api, application())


def test_admission_creates_the_instance_and_its_contents_and_keeps_the_readback(
    env, api
):
    runner = controller(env, api)
    runner.initialize()
    receipt = runner.provision(DEADLINE)
    assert api.calls[:3] == ["get", "create", "get"]
    assert set(receipt["tables"]) == set(TABLES)
    state = env.refresh().bigtable
    assert (state["creation_intent"], state["inflight"], state["instance"]) == (
        True,
        None,
        receipt,
    )


def test_an_existing_instance_is_never_this_runs(env, api):
    api.instance = {"state": "READY"}
    runner = controller(env, api)
    runner.initialize()
    with pytest.raises(Failure, match="already exists"):
        runner.provision(DEADLINE)
    assert env.refresh().bigtable["creation_intent"] is False


def test_a_refused_create_drops_the_intent_and_a_later_failure_keeps_it(env, api):
    api.errors["create"] = ApiError(409, "POST", "instances")
    runner = controller(env, api)
    runner.initialize()
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    assert env.refresh().bigtable["creation_intent"] is False
    other, api = Environment(), FakeInstance()
    api.errors["create_table:alo"] = ApiError(503, "POST", "tables")
    runner = controller(other, api)
    runner.initialize()
    with pytest.raises(ApiError):
        runner.provision(DEADLINE)
    # The instance exists, so cleanup must delete it.
    state = other.refresh().bigtable
    assert (state["creation_intent"], state["inflight"]) == (True, None)


def test_a_create_never_seen_to_land_keeps_its_marker(env, api):
    """Its operation could still create the instance after cleanup found none."""
    api.create = lambda: api.calls.append("create")
    runner = controller(env, api)
    runner.initialize()
    with pytest.raises(Failure, match="did not become readable"):
        runner.provision(DEADLINE)
    assert env.refresh().bigtable["inflight"] is not None
    with pytest.raises(Failure, match="in flight"):
        runner.release()


def test_cleanup_disables_the_change_stream_before_the_deletion_it_retries(env, api):
    """A refused deletion, or a disable refused while the last one runs, is retried."""
    runner = controller(env, api)
    runner.initialize()
    runner.provision(DEADLINE)
    runner.release()
    supervisor = controller(env, api, "supervisor")
    api.errors["disable_change_stream"] = ApiError(409, "PATCH", "eo")
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    # The deletion races the change stream's disable, which is still running.
    api.errors["delete"] = ApiError(400, "DELETE", "instance")
    api.tables["eo"].pop("changeStreamConfig")
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is False
    assert supervisor.cleanup(lambda: True, deadline=DEADLINE) is True
    calls = [c for c in api.calls if c in ("disable_change_stream", "delete")]
    # Once the change stream reads disabled, it is not disabled again.
    assert calls == ["disable_change_stream", "delete", "delete"]
    assert api.instance is None
    record = env.refresh()
    record.stop_requested = True
    require_resources_clean(record)
