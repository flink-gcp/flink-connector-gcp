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
"""Prove no workload can still use the run's Pub/Sub grants before cleanup."""

from .common import Failure
from .quiesce import namespace_writers, poll_writers


def barrier(env):
    """Build the quiescence callback for this run's Pub/Sub supervisor.

    What it proves, from a fresh listing on every call: no object that could
    run or restore a writer remains in the application namespace, whoever
    owns it. Not only this run's, unlike the BigQuery barrier: the
    namespace's service account may create Pods, and any Pod running as it
    holds the workload's data grants on the run's subscriptions and output
    topic. Outside a run the namespace is expected to hold no such object;
    dispatch does not refuse one, and one left there keeps this barrier from
    passing, so cleanup keeps the lock until it is removed. Deletes on this
    path are graceful, so a Pod leaving the API means its containers
    terminated.

    What it does not prove: that no request a terminated Pod sent is still
    executing at the service, and that the runner, outside the cluster, has
    stopped. The handoff's releases carry the latter; the actor protocol's
    unsettled-call markers carry the former for the actors' own requests.
    """
    if env.actor != "supervisor" or env.approval.scenario != "pubsub-recovery":
        raise Failure("The Pub/Sub quiescence barrier belongs to its supervisor")
    return poll_writers(env, lambda: namespace_writers(env), "Pub/Sub")
