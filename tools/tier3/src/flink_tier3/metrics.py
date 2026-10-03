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
"""Flink metric primitives, shared by the scenarios that sample them."""

from .common import ApiError, TransportError
from .policy import MIB

AGGREGATES = "min,max,sum"
# The query service reports an operator metric as "<sanitised operator>.<name>",
# so the name is the suffix after the last dot of a discovered metric id.
JVM_METRICS = (
    "Status.JVM.Memory.Heap.Used",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.GarbageCollector.All.Time",
    "Status.JVM.GarbageCollector.All.Count",
    "Status.JVM.CPU.Load",
)


def metric_name(metric_id):
    return metric_id.rsplit(".", 1)[-1]


def sample(read):
    """Read one metric endpoint, naming an absent reading rather than raising."""
    try:
        return read()
    except (ApiError, TransportError) as error:
        return {"unavailable": str(error)}


def unavailable(value):
    return isinstance(value, dict) and "unavailable" in value


def subset(value, fields):
    value = value or {}
    return {field: value[field] for field in fields if field in value}


class FlinkRest:
    """Read the running job's REST API through its own Service, by proxy.

    The Cloud Tasks observer has equivalents bound to that scenario's fixed
    namespace and to its session's meter; this one takes the namespace from
    the approval so the same endpoints can be read from a recovery exercise.
    """

    def __init__(self, env, service, job_id):
        self.env, self.service, self.job_id = env, service, job_id

    def _read(self, suffix, limit):
        return self.env.kube.request(
            "GET",
            self.env.kube.path(
                "Service",
                self.env.approval.application_namespace,
                self.service["metadata"]["name"] + ":8081",
            )
            + "/proxy"
            + suffix,
            limit=limit,
        )

    def job(self, path, limit=MIB):
        return self._read(f"/jobs/{self.job_id}{path}", limit)

    def root(self, path, limit=MIB):
        return self._read(path, limit)
