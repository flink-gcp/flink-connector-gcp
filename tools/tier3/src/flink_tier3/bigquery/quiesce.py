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
"""Prove the run's writers are gone, and say plainly what that does not prove."""

from ..common import Failure, ownership
from ..quiesce import namespace_writers, poll_writers


def owned_writers(env, seen=None):
    """Owned objects that could still write, from a listing taken right now.

    Scoped to the application's own roots and namespace. Seeding from every
    root would count the supervisor's own Pod, which runs in a control
    namespace for as long as it is asking the question, so the barrier could
    never return True. The namespace filter is the second half of that and is
    load-bearing on its own: `env.observed` is not scoped, and carries the
    supervisor's uids because the cleanup inventory seeds from every root.
    """
    roots = {
        ref["uid"]
        for key, ref in env.roots.items()
        if key == "application" or key.startswith("cell:")
    }
    items = namespace_writers(env)
    known = ownership(items, roots | set(env.observed) | set(seen or ()))
    if seen is not None:
        # `Cleanup.run`'s loop stopped growing `env.observed` before this
        # barrier ran, so an intermediate owner that appears and is collected
        # between two of these calls would take its children's only link to a
        # root with it. Remember what this barrier has seen itself.
        seen.update(
            obj["metadata"]["uid"] for obj in items if obj["metadata"]["uid"] in known
        )
    return [obj for obj in items if obj["metadata"]["uid"] in known]


def barrier(env):
    """Build the quiescence callback for this run's supervisor.

    What it proves, on every call and from a fresh listing rather than a
    snapshot: no object owned by this run's application roots that could run or
    restore a writer remains in the application namespace. The listing spans the
    control namespaces too, so it is scoped rather than taken whole: the
    supervisor asking the question runs in one of them. Deletes on this path are graceful,
    so a Pod leaving the API means its containers terminated; re-listing is
    what catches one that a controller puts back between two calls.

    What it does not prove, and no observer with these grants can: that no
    append is still in flight server-side, and that no buffered write stream
    remains open. The Storage Write API has no call that lists a table's
    streams, the connector deliberately never finalizes them, and their
    server-assigned names are never surfaced outside Flink's own state. The
    measurement does not rest on this: the query oracle reads only after the
    job reached FINISHED and its post-recovery window closed, which is where
    a settled read is established. What rests on this barrier is that cleanup
    does not delete a table while a Pod that could write to it is still alive.

    Beyond that, exclusivity is by grant rather than by observation: the only
    principal holding `bigquery.tables.updateData` on the dataset is the
    workload service account, reachable only through the Kubernetes service
    account in the namespace this listing just showed empty.
    """
    if env.actor != "supervisor":
        raise Failure("The quiescence barrier belongs to the supervisor")
    seen = set()
    return poll_writers(env, lambda: owned_writers(env, seen), "BigQuery")
