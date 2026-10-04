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
"""Sample what the deployed Pub/Sub relay's connector reports, through its REST service."""

from .metrics import metric_name, sample, subtask_metrics, unavailable

# The source reader's acknowledgement, lease and buffer readings
# (PubSubMetricNames.java). `pendingAcks` is what the reader holds leased:
# received or emitted and not yet acknowledged, so it is the population a
# fault returns to the service for redelivery.
SOURCE_METRICS = (
    "messagesReceived",
    "messagesAcked",
    "messagesNacked",
    "pendingAcks",
    "pendingCheckpoints",
    "bufferedMessages",
    "fetcherBufferedMessages",
    "subscriberShutdownsAbandoned",
    "subscriberFailuresUnreported",
)
# The sink writer's publication readings.
SINK_METRICS = (
    "inFlightMessages",
    "inFlightBytes",
    "activePublishers",
    "publisherShutdownsAbandoned",
)
FAMILIES = {
    "source": frozenset(SOURCE_METRICS),
    "sink": frozenset(SINK_METRICS),
}
WANTED = frozenset(SOURCE_METRICS) | frozenset(SINK_METRICS)


def connector_metrics(rest, vertex):
    """One vertex's Pub/Sub connector metrics, aggregated over its subtasks.

    A connector metric's id carries its operator's name, which differs between
    the entry points and with operator chaining, so the ids are listed and
    then asked for on every sample: a listing taken before the operators open
    holds none of them, and one taken on a restarted job may hold others.
    """
    listing = sample(lambda: rest.job(f"/vertices/{vertex}/subtasks/metrics"))
    if unavailable(listing):
        return listing
    ids = sorted(
        {
            item["id"]
            for item in (listing if isinstance(listing, list) else ())
            if isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and metric_name(item["id"]) in WANTED
        }
    )
    if not ids:
        return {"unavailable": "no Pub/Sub connector metric is listed"}
    return subtask_metrics(rest, vertex, ids)
