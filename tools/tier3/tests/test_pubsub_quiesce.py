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
"""The Pub/Sub cleanup barrier: no writer at all in the application namespace."""

from dataclasses import replace

import pytest
from flink_tier3.common import ApiError, Failure
from flink_tier3.environment import Environment
from flink_tier3.policy import PUBSUB, SYSTEM
from flink_tier3.pubsub_quiesce import barrier
from flink_tier3.quiesce import UNREAD_RETRIES, WRITER_KINDS
from test_pubsub_approval import prepared as prepared  # noqa: PLC0414
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj


@pytest.fixture
def supervising(prepared):
    runner, _ = prepared
    return Environment(
        runner.kube,
        runner.store,
        runner.approval,
        runner.clock,
        runner.sleep,
        actor="supervisor",
    )


@pytest.mark.parametrize("kind", WRITER_KINDS)
def test_any_writer_in_the_namespace_blocks_whoever_owns_it(supervising, kind):
    """The namespace's service account can create Pods of its own."""
    quiesce = barrier(supervising)
    assert quiesce() is True
    writer = supervising.kube.put(obj(kind, "unowned", PUBSUB))
    assert quiesce() is False
    supervising.kube.delete(writer)
    assert quiesce() is True


@pytest.mark.parametrize(
    "kind, namespace",
    [("ConfigMap", PUBSUB), ("Service", PUBSUB), ("Pod", SYSTEM), ("Job", SYSTEM)],
)
def test_non_writers_and_the_control_namespace_do_not_block(
    supervising, kind, namespace
):
    supervising.kube.put(obj(kind, "other", namespace))
    assert barrier(supervising)() is True


def test_a_transient_read_is_retried_and_an_exhausted_one_raises(supervising):
    listing, tries = supervising.kube.items, []

    def flaky(kind, namespace):
        if kind == WRITER_KINDS[0]:
            tries.append(kind)
            if len(tries) < UNREAD_RETRIES:
                raise ApiError(503, "GET", "/apis/" + kind.lower())
        return listing(kind, namespace)

    supervising.kube.items = flaky
    assert barrier(supervising)() is True
    assert len(tries) == UNREAD_RETRIES

    def unread(kind, namespace):
        raise ApiError(503, "GET", "/apis/" + kind.lower())

    supervising.kube.items = unread
    with pytest.raises(Failure, match="Pub/Sub quiescence could not be read"):
        barrier(supervising)()


def test_only_the_pubsub_supervisor_builds_the_barrier(prepared, supervising):
    runner, _ = prepared
    with pytest.raises(Failure, match="belongs to its supervisor"):
        barrier(runner)
    supervising.approval = replace(supervising.approval, scenario="smoke")
    with pytest.raises(Failure, match="belongs to its supervisor"):
        barrier(supervising)
