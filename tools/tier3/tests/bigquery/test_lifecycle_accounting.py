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
"""The immutable run artifacts' names and the query spend's billing evidence."""

import pytest
from flink_tier3.bigquery.lifecycle import billed_bytes
from flink_tier3.common import Failure
from flink_tier3.records import write_artifact

from ..test_lifecycle import env as env  # noqa: PLC0414

RUN = "bq-accounting"


def store_of(env):
    _kube, store, _approval, _clock = env
    return store


def test_an_unknown_document_is_not_silently_unaccounted(env):
    with pytest.raises(ValueError, match="Unknown run artifact"):
        write_artifact(store_of(env), RUN, "extra.json", {})


@pytest.mark.parametrize(
    "job",
    [
        {},
        {"statistics": {}},
        {"statistics": {"query": {}}},
        {"statistics": {"query": [1]}},
        {"statistics": [1]},
        {"statistics": {"query": {"totalBytesBilled": "not a number"}}},
    ],
)
def test_a_query_without_billing_evidence_is_refused(job):
    with pytest.raises(Failure, match="billing evidence is missing"):
        billed_bytes({"job": job})


def test_billed_bytes_reads_the_retained_job():
    result = {"job": {"statistics": {"query": {"totalBytesBilled": "4096"}}}}
    assert billed_bytes(result) == 4096
