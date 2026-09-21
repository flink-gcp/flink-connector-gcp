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
"""What the quiescence barrier proves, against a fake cluster it can re-read."""

import pytest
from flink_tier3.bigquery_quiesce import (
    UNREAD_RETRIES,
    WRITER_KINDS,
    barrier,
    owned_writers,
)
from flink_tier3.common import ApiError, Failure, timestamp
from flink_tier3.environment import Environment
from flink_tier3.policy import BIGQUERY, SYSTEM
from test_bigquery_bundle import approval as approval  # noqa: PLC0414
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj


@pytest.fixture
def supervising(approval, env):
    """A supervisor environment whose application root can own writers."""
    kube, store, _, clock = env
    approval["namespaces"][BIGQUERY]["uid"] = BIGQUERY + "-uid"
    clock.now = timestamp(approval["started_at"])
    environment = Environment(
        kube, store, approval, clock, clock.sleep, actor="supervisor"
    )
    root = obj("FlinkDeployment", "flinkdeployment", BIGQUERY)
    environment.remember("application", kube.put(root))
    # The barrier runs after cleanup deleted the root: the reference survives
    # in control state, the object does not.
    kube.delete(root)
    return environment


def owned(kind, name, environment):
    value = obj(kind, name, BIGQUERY)
    # `root()` resolves against the cluster, and the root is already deleted;
    # the remembered reference is what ownership is derived from.
    value["metadata"]["ownerReferences"] = [
        {"uid": environment.roots["application"]["uid"]}
    ]
    return value


def owned_pod(environment, name="taskmanager"):
    return owned("Pod", name, environment)


def test_barrier_refuses_while_an_owned_writer_is_listed(supervising):
    quiesce = barrier(supervising)
    pod = owned_pod(supervising)
    supervising.kube.put(pod)
    assert quiesce() is False
    supervising.kube.delete(pod)
    assert quiesce() is True


def test_barrier_re_lists_so_a_replacement_between_calls_is_caught(supervising):
    """A snapshot taken once would call a resurrected writer quiescent."""
    quiesce = barrier(supervising)
    assert quiesce() is True
    supervising.kube.put(owned_pod(supervising, "replacement"))
    assert quiesce() is False


def test_barrier_covers_the_controllers_that_would_restore_a_writer(supervising):
    """An empty Pod list at one instant is not the same as no writer."""
    supervising.kube.put(owned("Deployment", "taskmanager", supervising))
    assert barrier(supervising)() is False
    assert "Deployment" in WRITER_KINDS


def test_barrier_ignores_an_object_this_run_does_not_own(supervising):
    supervising.kube.put(obj("Pod", "someone-else", BIGQUERY))
    assert barrier(supervising)() is True
    assert not owned_writers(supervising)


def test_barrier_belongs_to_the_supervisor(supervising):
    supervising.actor = "runner"
    with pytest.raises(Failure, match="belongs to the supervisor"):
        barrier(supervising)


@pytest.mark.parametrize("status", [429, 503])
def test_a_read_that_says_nothing_is_retried_rather_than_answered(supervising, status):
    """The second caller per poll turns any non-True into a failed cleanup."""
    listing, first = supervising.kube.items, WRITER_KINDS[0]
    tries = []

    def flaky(kind, namespace):
        if kind == first:
            tries.append(kind)
        if len(tries) < UNREAD_RETRIES:
            raise ApiError(status, "GET", "/api/v1/" + kind.lower())
        return listing(kind, namespace)

    supervising.kube.items = flaky
    assert barrier(supervising)() is True
    assert len(tries) == UNREAD_RETRIES


def test_a_read_that_says_something_is_not_swallowed(supervising):
    def refuse(kind, _namespace):
        raise ApiError(403, "GET", "/api/v1/" + kind.lower())

    supervising.kube.items = refuse
    with pytest.raises(ApiError):
        barrier(supervising)()


def test_a_writer_of_another_root_is_not_one_this_run_waits_for(supervising):
    """Seeding from every root would make the supervisor wait for its own Pod."""
    job = obj("Job", "lifecycle", BIGQUERY)
    supervising.remember("supervisor", supervising.kube.put(job))
    pod = obj("Pod", "lifecycle-abcde", BIGQUERY)
    pod["metadata"]["ownerReferences"] = [{"uid": job["metadata"]["uid"]}]
    supervising.kube.put(pod)
    assert barrier(supervising)() is True


def test_a_writer_outside_the_application_namespace_is_not_waited_for(supervising):
    """The inventory spans control namespaces this run does not clean here."""
    pod = owned_pod(supervising, "elsewhere")
    pod["metadata"]["namespace"] = SYSTEM
    supervising.kube.put(pod)
    assert barrier(supervising)() is True


def test_an_unreadable_cluster_gives_up_after_a_bounded_number_of_tries(supervising):
    """The cleanup window cannot bound this, so the count is what must."""
    tries = []

    def refuse(kind, _namespace):
        tries.append(kind)
        raise ApiError(503, "GET", "/api/v1/" + kind.lower())

    supervising.kube.items = refuse
    supervising.sleep = lambda _seconds: None
    with pytest.raises(Failure, match="could not be read"):
        barrier(supervising)()
    # Every attempt fails on the first kind it asks for.
    assert len(tries) == UNREAD_RETRIES


def test_controllers_are_listed_before_pods(supervising):
    """A Pod created between the listings is caught through its controller.

    The reverse order loses a create-then-collect that straddles them: the
    Pod is not in the first page and its controller is gone from the second.
    """
    listing, seen = supervising.kube.items, []

    def record(kind, namespace):
        seen.append(kind)
        return listing(kind, namespace)

    supervising.kube.items = record
    assert barrier(supervising)() is True
    assert seen[-1] == "Pod"
    assert seen.index("Deployment") < seen.index("Pod")
    assert set(seen) == set(WRITER_KINDS)


def test_a_middle_owner_collected_between_polls_does_not_orphan_its_pod(supervising):
    """`Cleanup.run` stopped growing `observed` before the barrier ever runs."""
    quiesce = barrier(supervising)
    controller = owned("ReplicaSet", "taskmanager", supervising)
    supervising.kube.put(controller)
    pod = obj("Pod", "taskmanager-0", BIGQUERY)
    pod["metadata"]["ownerReferences"] = [{"uid": controller["metadata"]["uid"]}]
    supervising.kube.put(pod)
    assert quiesce() is False
    # Background propagation deletes the owner at once and collects its children
    # afterwards, so the Pod outlives the ReplicaSet's disappearance from the
    # API and its only remaining link to a root goes with it. The fake cascades
    # on delete, so the middle object is removed directly to stage that window.
    supervising.kube.data.pop(("ReplicaSet", BIGQUERY, controller["metadata"]["name"]))
    assert quiesce() is False


def test_the_retry_survives_the_clock_cleanup_normally_runs_at(supervising):
    """Cleanup starts once the run reaches its expiry, which is that field.

    `Cleanup.run` computes its own deadline through `cleanup_window(now)`, so
    bounding the retry by `env.schedule.cleanup_end` would spend it before the
    first attempt on every ordinary cleanup.
    """
    supervising.clock.now = supervising.schedule.cleanup_end
    listing, first = supervising.kube.items, WRITER_KINDS[0]
    tries = []

    def flaky(kind, namespace):
        if kind == first:
            tries.append(kind)
        if len(tries) < UNREAD_RETRIES:
            raise ApiError(503, "GET", "/api/v1/" + kind.lower())
        return listing(kind, namespace)

    supervising.kube.items = flaky
    assert barrier(supervising)() is True
    assert len(tries) == UNREAD_RETRIES
