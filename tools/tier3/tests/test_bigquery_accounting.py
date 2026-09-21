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
"""The immutable run artifacts and the query spend, against their allowances."""

import pytest
from flink_tier3.bigquery_lifecycle import billed_bytes
from flink_tier3.common import Failure
from flink_tier3.model import Approval
from flink_tier3.policy import BIGQUERY_CEILINGS
from flink_tier3.records import write_artifact
from test_bigquery_bundle import approval as approval  # noqa: PLC0414
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414

RUN = "bq-accounting"


def store_of(env):
    _kube, store, _approval, _clock = env
    return store


def test_run_artifacts_are_weighed_against_their_own_allowance(env):
    store = store_of(env)
    write_artifact(store, RUN, "approval.json", {"a": 1}, "bigquery-recovery")
    oversized = {"pad": "x" * BIGQUERY_CEILINGS["artifact_bytes"]}
    with pytest.raises(Failure, match="Run artifact ceiling"):
        write_artifact(store, RUN, "result.json", oversized, "bigquery-recovery")
    assert store.read(f"runs/{RUN}/result.json")[0] is None


def test_the_allowance_counts_only_the_documents_it_owns(env):
    """Receipts and query evidence live below the prefix and answer elsewhere.

    They are also why this cannot list the prefix: a supervisor's receipts
    reach the listing's object ceiling long before their byte ceiling, and
    this accounting runs when the final result is written.
    """
    store = store_of(env)
    store.write(
        f"runs/{RUN}/supervisor/{'a' * 8}.json",
        {"pad": "x" * BIGQUERY_CEILINGS["artifact_bytes"]},
    )
    store.write(
        f"runs/{RUN}/bigquery/queries/0.json",
        {"pad": "x" * BIGQUERY_CEILINGS["artifact_bytes"]},
    )
    listings = []
    original = store.objects
    store.objects = lambda *a, **k: (listings.append(a), original(*a, **k))[1]
    write_artifact(store, RUN, "approval.json", {"a": 1}, "bigquery-recovery")
    assert store.read(f"runs/{RUN}/approval.json")[0] == {"a": 1}
    assert listings == []


def test_an_unknown_document_is_not_silently_unaccounted(env):
    with pytest.raises(ValueError, match="Unknown run artifact"):
        write_artifact(store_of(env), RUN, "extra.json", {}, "bigquery-recovery")


def test_another_scenario_keeps_its_unweighed_writes(env):
    store = store_of(env)
    write_artifact(
        store,
        RUN,
        "approval.json",
        {"pad": "x" * BIGQUERY_CEILINGS["artifact_bytes"] * 2},
        "smoke",
    )
    assert store.read(f"runs/{RUN}/approval.json")[0] is not None


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


def test_the_receipt_budget_is_read_from_policy_not_restated(
    approval, env, monkeypatch
):
    """A literal that happens to agree today is not the same as reading it."""
    from flink_tier3 import records as records_module

    _kube, store, _smoke, clock = env
    ceilings = dict(BIGQUERY_CEILINGS, receipt_bytes_supervisor=1)
    monkeypatch.setattr(records_module, "BIGQUERY_CEILINGS", ceilings)
    records = records_module.Records(store, Approval.from_dict(approval), clock)
    with pytest.raises(Failure, match="Durable evidence ceiling"):
        records.evidence("probe", {"pad": "x" * 64})
