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
"""Flink metric primitives and the observation coverage built from them, shared
by the scenarios that sample them."""

import json

from .common import ApiError, ReadCeilingExceeded, TransportError
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
    """Read one metric endpoint, naming an absent reading rather than raising.

    An answer too large to read, or one that is not UTF-8 JSON a parser can
    hold, is as absent as one the service refused: none of them is a reading,
    and none says anything about the run.
    """
    try:
        return read()
    except (
        ApiError,
        TransportError,
        ReadCeilingExceeded,
        json.JSONDecodeError,
        UnicodeDecodeError,
        RecursionError,
    ) as error:
        return {"unavailable": str(error)}


def subtask_metrics(rest, vertex, ids):
    """The named metrics of one vertex, aggregated over its subtasks."""
    if not ids:
        return {"unavailable": "no metric ids selected"}
    query = f"?get={','.join(ids)}&agg={AGGREGATES}"
    return sample(lambda: rest.job(f"/vertices/{vertex}/subtasks/metrics{query}"))


def fold_coverage(previous, window, observed):
    """Fold one sample into its window's coverage, keeping what it ever read.

    Coverage is monotonic within a window and never crosses one. A sample with
    no window counts nowhere.
    """
    coverage = {
        name: dict(entry) for name, entry in previous.items() if isinstance(entry, dict)
    }
    if window is None:
        return coverage
    entry = coverage.setdefault(window, {"attempts": 0, "observed": []})
    entry["attempts"] += 1
    entry["observed"] = sorted(set(entry["observed"]) | set(observed))
    return coverage


def coverage_reasons(coverage, windows, families):
    """Why recorded coverage falls short of every family in every window.

    The record is untyped JSON: a window that took no sample observed nothing,
    whatever it claims, and `True` is not a sample count.
    """
    coverage = coverage if isinstance(coverage, dict) else {}
    reasons = []
    for name in windows:
        entry = coverage.get(name)
        entry = entry if isinstance(entry, dict) else {}
        attempts = entry.get("attempts")
        observed = entry.get("observed")
        observed = observed if isinstance(observed, list) else []
        if type(attempts) is not int or attempts < 1:
            reasons.append("unsampled-" + name)
            observed = []
        reasons.extend(
            f"unobserved-{family}-in-{name}"
            for family in families
            if family not in observed
        )
    return reasons


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
