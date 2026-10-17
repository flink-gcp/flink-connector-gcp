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
"""Fixed deployment policy loaded from the reviewed source bundle."""

import re
import tomllib
from decimal import Decimal
from importlib.resources import files

with files(__package__).joinpath("policy.toml").open("rb") as stream:
    _policy = tomllib.load(stream)

PROJECT = _policy["environment"]["project"]
REGION = _policy["environment"]["region"]
CONTEXT = _policy["environment"]["context"]
SYSTEM = _policy["environment"]["system"]
SMOKE = _policy["environment"]["smoke"]
OPERATOR = _policy["environment"]["operator"]
EVIDENCE = _policy["environment"]["evidence"]
STATE = _policy["environment"]["state"]
ENVIRONMENT = _policy["environment"]["environment"]
GAR = _policy["environment"]["gar"]
REPOSITORY = _policy["environment"]["repository"]
PRICING_REVIEWED = _policy["environment"]["pricing_reviewed"]
COST_RATES = {key: Decimal(value) for key, value in _policy["cost_rates"].items()}
WINDOWS = _policy["windows"]
CLEANUP_SECONDS = _policy["cleanup"]["seconds"]
STATE_OBJECTS = _policy["state"]["objects"]
POD_RESOURCES = _policy["pod_resources"]
CLOUDTASKS = _policy["environment"]["cloudtasks"]
CLOUDTASKS_STATE = _policy["environment"]["cloudtasks_state"]
BIGQUERY = _policy["environment"]["bigquery"]
BIGQUERY_STATE = _policy["environment"]["bigquery_state"]
PUBSUB = _policy["environment"]["pubsub"]
PUBSUB_STATE = _policy["environment"]["pubsub_state"]

BIGQUERY_OBSERVATIONS = {
    # The measurement windows: a baseline after a warm-up, one after the
    # upgrade and one after the failover. Thirty minutes of input hold three
    # five-minute windows: with ten-minute baseline and post-recovery windows
    # and none after the upgrade, bq1313-fl-10-a1's input outlasted the last
    # by 232 s and bq1685-fl-10-a1's by 106 s. The warm-up is a minute, as for
    # Cloud Tasks; the three minutes it was had no recorded reason, and the two
    # it gives back are FILE_LOADS's margin.
    "warmup_seconds": 60,
    "baseline_seconds": 300,
    "post_upgrade_seconds": 300,
    "post_recovery_seconds": 300,
    # After the baseline, the upgrade waits for a checkpoint triggered after
    # it: two FILE_LOADS checkpoint intervals and a poll each, the longest
    # interval any BigQuery mode runs at.
    "upgrade_wait_seconds": 270,
    "startup_seconds": 600,
    # From admission to input flowing, which only the exercise waits for. A
    # cold Autopilot cluster provisions a node each for the supervisor, the
    # JobManager and the TaskManagers in turn; pilot bq1312-alo-10-a4 needed
    # about ten minutes for that and missed the startup budget by seconds.
    "input_seconds": 1200,
    "recovery_seconds": 300,
    "visibility_seconds": 600,
    # FILE_LOADS deletes the JobManager only once a listing shows a load job
    # of the run RUNNING, after the window that follows the upgrade. A commit
    # starts within any 120-second checkpoint interval and runs its loads
    # within seconds of starting, so the wait is one interval and a margin,
    # listed without a gap. Input keeps flowing while it waits, and must still
    # last the post-recovery window after the failover; the plan's tests hold
    # that budget for every mode.
    "in_flight_seconds": 130,
    "in_flight_poll_seconds": 1,
    # One measurement per this many seconds, not per poll. Six REST reads at
    # the transport's timeout would otherwise let a slow endpoint spend two
    # minutes of a window that is three, and a memory or GC trend does not
    # need a sample every fifteen seconds to be readable.
    "measure_seconds": 60,
}

CLOUDTASKS_OBSERVATIONS = {
    # From admission to the application's creation: the supervisor and
    # Operator readiness every scenario waits for, then the run's queue
    # created, paused and read back once it settles.
    "startup_seconds": 600,
    # From admission to input flowing, as for BigQuery: a cold Autopilot
    # cluster provisions a node each for the supervisor, the JobManager and the
    # TaskManagers in turn.
    "input_seconds": 1200,
    "recovery_seconds": 300,
    # From the finished application to a task listing that passes the oracle.
    "visibility_seconds": 600,
    # The measurement windows: a baseline after a warm-up, one after the
    # upgrade and one after the failover, read once a minute. With
    # ct-1362-a1's recoveries, each proved about a minute and a half after it
    # began, the last window ends about twenty minutes into the thirty minutes
    # of input.
    "warmup_seconds": 60,
    "baseline_seconds": 300,
    "post_upgrade_seconds": 300,
    "post_recovery_seconds": 300,
    # A checkpoint interval and a poll to see it complete, twice over.
    "upgrade_wait_seconds": 120,
    "measure_seconds": 60,
}

PUBSUB_ADMISSION = {
    # From admission to the application's creation. Beyond the supervisor and
    # Operator readiness every scenario waits for, Pub/Sub admission creates
    # and grants six resources, waits for the grants to take effect for each
    # identity and publishes the first cohort; IAM documents a policy change
    # as typically taking two minutes and potentially seven or longer.
    "startup_seconds": 900,
}


# Application namespaces first, then the control namespace.
NAMESPACES = (SMOKE, CLOUDTASKS, BIGQUERY, PUBSUB, SYSTEM)


RUN_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?\Z")


DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


SHA = re.compile(r"[0-9a-f]{40}\Z")


NONCE = "flink-gcp.io/approval"


MIB = 1024 * 1024


# The query evidence one BigQuery run may export, divided equally among its
# query slots.
QUERY_EVIDENCE_BYTES = 10 * MIB


POLL = 15


HTTP_TIMEOUT = 20
# Seconds before each retry of an idempotent Kubernetes or Cloud Storage read
# that met a transient failure; mutations are never retried this way.
READ_RETRY_DELAYS = (1, 2, 4)


# Cloud Storage sustains about one mutation a second on one object and refuses
# a faster rate with 429 without applying the write. A conditional update
# retries a refused write after each of these backoffs, in seconds before
# jitter.
RATE_LIMIT_BACKOFF = (1, 2, 4, 8, 16, 32)
# A lost generation race, in a conditional update or between a read's metadata
# and its download, is retried after a random wait of up to 0.25 seconds,
# doubling per race to at most 4: a Pub/Sub supervisor collecting output
# updates the record for every pull, and a runner that re-read at once lost
# five races in a row to it on 2026-10-10. The twentieth lost race gives up,
# after at most about 64 seconds of waiting.
GENERATION_RACES = 20
RACE_BACKOFF_FIRST, RACE_BACKOFF_CAP = 0.25, 4
# The most writes one update makes: nineteen lost races, six refused writes
# and the attempt that settles it.
UPDATE_ATTEMPTS = GENERATION_RACES + len(RATE_LIMIT_BACKOFF)


PUBSUB_EXERCISE = {
    # Admission's own window and one poll, so that a RUNNING the runner set
    # in its last moment is still seen.
    "startup_seconds": PUBSUB_ADMISSION["startup_seconds"] + POLL,
    # From the application's creation to its first cohort fully observed on
    # the output subscription. A cold Autopilot cluster provisions a node each
    # for the JobManager and the TaskManagers; BigQuery's pilot needed about
    # ten minutes for those and the supervisor's node.
    "input_seconds": 900,
    # To a completed checkpoint triggered after that observation: one
    # 120-second interval, one 120-second checkpoint timeout and a margin.
    "checkpoint_seconds": 300,
    # From the replay cohort's request to its processing observed, which must
    # also come before the next checkpoint completes for the boundary to hold.
    "boundary_seconds": 180,
    # From a cohort's request to its publication's start: the runner serves
    # requests once per poll.
    "cohort_seconds": 90,
    # From the fault to restored attempts having replayed what the fault
    # displaced: a replacement Pod may need a new node.
    "recovery_seconds": 600,
    # From the last cohort's request to its processing observed on both
    # inputs and a checkpoint completed after it.
    "after_seconds": 420,
    "measure_seconds": 60,
    # Output pulls per poll at most; an empty batch ends a poll's drain
    # sooner. Immediate pulls averaged 48-61 messages in #1361's trials, so
    # a 1,000-record trial's cohort, 666 output lines, takes about 14, and
    # one poll can collect it before the next 120-second checkpoint; much
    # smaller batches still spread it over several polls.
    "drain_pulls": 20,
}
