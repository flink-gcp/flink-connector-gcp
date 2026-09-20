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
"""Durable controller contracts using generation-checked storage and fake APIs."""

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from flink_tier3.bigquery import Trial
from flink_tier3.bigquery_lifecycle import BigQueryLifecycle
from flink_tier3.bigquery_resources import ResourcePlan
from flink_tier3.common import Failure, TransportError, digest
from flink_tier3.model import Phase, RunRecord
from flink_tier3.records import Records
from test_bigquery_resources import NOW, Response, client, job, page, table
from test_tier3_lifecycle import Store


class Environment:
    """Only the controller's caller contract; not production BQ admission."""

    def __init__(self, plan, application):
        self.approval = SimpleNamespace(
            scenario="bigquery-recovery",
            run_id=plan.trial.run_id,
            nonce=plan.nonce,
            bigquery_plan=plan,
            application_sha256=digest(application),
        )
        self.actor = "runner"
        self.owner = True
        self.store = Store()
        self.records = Records(self.store, self.approval, lambda: NOW)
        self.store.write(self.records.path, RunRecord(plan.nonce).to_dict())

    def assert_owner(self):
        if not self.owner:
            raise Failure("Environment ownership lost")

    def refresh(self):
        return self.records.read()[0]

    def admission_open(self):
        self.assert_owner()
        record = self.refresh()
        if (
            record.phase not in (Phase.APPROVED, Phase.READY)
            or record.stop_requested
            or record.evidence_failed
        ):
            raise Failure("Admission closed")

    def require_running(self, message):
        self.assert_owner()
        record = self.refresh()
        if (
            record.phase != Phase.RUNNING
            or record.stop_requested
            or record.evidence_failed
        ):
            raise Failure(message)


class Resources:
    def __init__(self, plan):
        self.plan = plan
        self.tables = {}
        self.jobs = {}
        self.calls = []
        self.after_create = None
        self.after_submit = None
        self.after_delete = None
        self.on_results = None

    def table(self, destination):
        self.calls.append(("table", destination))
        return copy.deepcopy(self.tables.get(destination))

    def ensure_table(self, destination):
        self.calls.append(("create", destination))
        self.tables.setdefault(destination, table(self.plan, destination))
        if self.after_create:
            callback, self.after_create = self.after_create, None
            callback(destination)
        return self.table(destination)

    def submit_query(self, slot):
        self.calls.append(("submit", slot))
        self.jobs.setdefault(slot, job(self.plan, slot, "RUNNING"))
        if self.after_submit:
            callback, self.after_submit = self.after_submit, None
            callback(slot)
        return copy.deepcopy(self.jobs[slot])

    def results(self, slot):
        self.calls.append(("results", slot))
        if self.on_results:
            self.on_results()
        return {"job": job(self.plan, slot), "rows": [], "report": {"passed": True}}

    def cancel_query(self, slot):
        self.calls.append(("cancel", slot))
        return copy.deepcopy(self.jobs.get(slot))

    def delete_table(self, destination, receipt):
        self.calls.append(("delete", destination))
        current = self.tables.get(destination)
        if current and current["creationTime"] != receipt["creationTime"]:
            raise Failure("Replacement table")
        self.tables.pop(destination, None)
        if self.after_delete:
            callback, self.after_delete = self.after_delete, None
            callback()


@pytest.fixture
def setup():
    plan = ResourcePlan(
        Trial("lifecycle-1312", "EO", 10, 23),
        "a" * 32,
        (NOW + 3600) * 1000,
        3,
        1024**3,
        60000,
    )
    app = {
        "metadata": {"name": plan.trial.run_id, "namespace": "tier3-bigquery"},
        "spec": {
            "job": {
                "args": [
                    "--run-id",
                    plan.trial.run_id,
                    "--phase",
                    "initial",
                    "--mode",
                    "EO",
                    "--destinations",
                    "10",
                    "--records",
                    "23",
                    "--bytes-per-second",
                    "1048576",
                    "--require-restored",
                    "false",
                ]
            }
        },
    }
    env = Environment(plan, app)
    api = Resources(plan)
    controller = BigQueryLifecycle(env, api, app)
    controller.initialize()
    return controller, env, api, app


def restart(setup):
    _, env, api, app = setup
    env.records = Records(env.store, env.approval, lambda: NOW)
    return BigQueryLifecycle(env, api, app)


def running(setup):
    controller, env, _, _ = setup
    controller.provision()
    env.records.set_phase(Phase.READY)
    env.records.set_phase(Phase.RUNNING)
    return controller


def lost():
    raise TransportError("Lost response")


def test_old_run_record_accepts_absent_bigquery_field():
    record = RunRecord("a" * 32).to_dict()
    del record["bigquery"]
    assert RunRecord.from_dict(record).bigquery is None


@pytest.mark.parametrize("field", ["nonce", "run_id", "scenario", "application_sha256"])
def test_application_identity_must_match(setup, field):
    _, env, api, app = setup
    setattr(env.approval, field, "wrong")
    with pytest.raises(Failure, match="approved application"):
        BigQueryLifecycle(env, api, app)


@pytest.mark.parametrize(
    "field,value",
    [
        ("query_slots", 4),
        ("maximum_bytes_billed", 42),
        ("query_timeout_ms", 1000),
        ("expires_ms", (NOW + 7200) * 1000),
    ],
)
def test_restart_cannot_change_plan(setup, field, value):
    _, env, api, app = setup
    api.plan = replace(api.plan, **{field: value})
    # Even a matching replacement approval cannot rewrite persisted intent.
    env.approval.bigquery_plan = api.plan
    with pytest.raises(Failure, match="replaced BigQuery resource intent"):
        BigQueryLifecycle(env, api, app).initialize()
    assert api.calls == []


@pytest.mark.parametrize("actor", ["supervisor", "recovery"])
def test_only_runner_can_create_or_collect(setup, actor):
    controller, env, api, _ = setup
    env.actor = actor
    for operation in (
        controller.initialize,
        controller.provision,
        lambda: controller.reserve_query("final"),
        lambda: controller.submit_query("final"),
        lambda: controller.collect_query("final"),
    ):
        with pytest.raises(Failure, match="Only the submitting runner"):
            operation()
    assert api.calls == []


def test_unrecorded_table_cannot_be_adopted_even_with_matching_labels(setup):
    controller, env, api, _ = setup
    api.tables[0] = table(api.plan)
    with pytest.raises(Failure, match="predates"):
        controller.provision()
    assert env.refresh().bigquery["tables"] == {}
    assert controller.cleanup(lambda: True)
    assert 0 in api.tables


def test_create_intent_precedes_api_and_survives_lost_response(setup):
    controller, env, api, _ = setup

    def accepted(destination):
        assert env.refresh().bigquery["tables"][str(destination)] == {
            "receipt": None,
            "deleted": False,
        }
        lost()

    api.after_create = accepted
    with pytest.raises(TransportError):
        controller.provision()
    restart(setup).provision()
    assert len(api.tables) == 10
    assert len(env.refresh().bigquery["tables"]) == 10
    assert env.refresh().bigquery["tables"]["0"]["receipt"]["creationTime"] == str(
        NOW * 1000
    )


def test_recorded_missing_table_is_never_recreated(setup):
    controller, _, api, _ = setup
    controller.provision()
    del api.tables[0]
    api.calls.clear()
    with pytest.raises(Failure, match="missing or replaced"):
        restart(setup).provision()
    assert not any(call[0] == "create" for call in api.calls)


def test_late_create_receipt_is_recorded_after_stop(setup):
    controller, env, api, _ = setup
    api.after_create = lambda _: controller.request_stop()
    with pytest.raises(Failure, match="admission has stopped"):
        controller.provision()
    assert env.refresh().bigquery["tables"]["0"]["receipt"] is not None
    assert set(api.tables) == {0}


def test_slot_allocation_is_durable_deduplicated_and_bounded(setup):
    controller = running(setup)
    assert controller.reserve_query("baseline") == 0
    controller = restart(setup)
    assert controller.reserve_query("baseline") == 0
    assert controller.reserve_query("recovered") == 1
    assert controller.reserve_query("final") == 2
    with pytest.raises(Failure, match="slot budget exhausted"):
        controller.reserve_query("retry")
    assert len(setup[1].refresh().bigquery["queries"]) == 3


def test_cas_conflict_preserves_concurrent_slot(setup):
    controller = running(setup)
    env = setup[1]
    env.store.before_write = lambda: controller.reserve_query("other")
    assert controller.reserve_query("final") == 1
    assert env.store.conflicts == 1
    assert controller.reserve_query("other") == 0


def test_cas_racing_stop_refuses_query_reservation(setup):
    controller = running(setup)
    env = setup[1]
    env.store.before_write = controller.request_stop
    with pytest.raises(Failure, match="admission has stopped"):
        controller.reserve_query("final")
    assert env.refresh().bigquery["queries"] == {}


def test_lost_submit_uses_same_job_after_restart(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")

    def accepted(slot):
        assert slot == 0
        assert env.refresh().bigquery["queries"]["final"]["stage"] == "submitting"
        lost()

    api.after_submit = accepted
    with pytest.raises(TransportError):
        controller.submit_query("final")
    restart(setup).submit_query("final")
    assert list(api.jobs) == [0]
    assert env.refresh().bigquery["queries"]["final"]["stage"] == "submitted"


def test_submit_response_does_not_regress_concurrently_collected_evidence(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    api.after_submit = lambda _: controller.collect_query("final")
    controller.submit_query("final")
    assert env.refresh().bigquery["queries"]["final"]["stage"] == "collected"
    with pytest.raises(Failure, match="cannot be resubmitted"):
        controller.submit_query("final")


def test_evidence_failure_does_not_mark_collected_or_allocate_new_query(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    env.store.fail_evidence = True
    with pytest.raises(Failure, match="Evidence unavailable"):
        controller.collect_query("final")
    assert env.refresh().bigquery["queries"]["final"]["evidence"] is None
    env.store.fail_evidence = False
    result = restart(setup).collect_query("final")
    assert result["report"]["passed"]
    assert len(api.jobs) == 1


def test_evidence_pointer_recovery_and_generation_replacement(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    # Fail the control write after the evidence object was accepted.
    api.on_results = lambda: setattr(
        env.store, "before_write", lambda: setattr(env.store, "before_write", lost)
    )
    with pytest.raises(TransportError):
        controller.collect_query("final")
    assert env.refresh().bigquery["queries"]["final"]["evidence"] is None
    api.on_results = None
    result = restart(setup).collect_query("final")
    api.calls.clear()
    assert restart(setup).collect_query("final") == result
    assert api.calls == []
    path = controller.prefix + "queries/0.json"
    value, generation = env.store.read(path)
    env.store.write(path, value, generation)
    with pytest.raises(Failure, match="replaced or removed"):
        controller.collect_query("final")


def test_existing_wrong_evidence_is_not_overwritten(setup):
    controller = running(setup)
    _, env, _, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    path = controller.prefix + "queries/0.json"
    env.store.write(path, {"foreign": True})
    with pytest.raises(Failure, match="different data"):
        controller.collect_query("final")
    assert env.store.read(path)[0] == {"foreign": True}


def test_cleanup_requires_durable_stop_and_external_barrier(setup):
    controller = running(setup)
    _, env, api, _ = setup
    api.calls.clear()

    def barrier():
        assert env.refresh().bigquery["stopped"]
        return False

    with pytest.raises(Failure, match="not quiescent"):
        controller.cleanup(barrier)
    assert api.calls == []
    assert len(api.tables) == 10


@pytest.mark.parametrize("state", [None, "PENDING", "RUNNING"])
def test_unknown_or_pending_submission_blocks_table_deletion(setup, state):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    api.jobs = {} if state is None else {0: job(api.plan, 0, state)}
    env.actor = "supervisor"
    api.calls.clear()
    assert not restart(setup).cleanup(lambda: True)
    assert api.calls == [("cancel", 0)]
    assert not env.refresh().bigquery["cleaned"]
    assert len(api.tables) == 10


def test_completed_and_only_reserved_slots_allow_component_cleanup(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    controller.reserve_query("unused")
    api.jobs[0] = job(api.plan)
    env.actor = "supervisor"
    api.calls.clear()
    assert restart(setup).cleanup(lambda: True)
    assert not api.tables
    assert [call for call in api.calls if call[0] == "cancel"] == [("cancel", 0)]
    record = env.refresh()
    assert record.bigquery["cleaned"]
    assert record.phase == Phase.RUNNING
    assert not record.idle and not record.state_clean and not record.success


def test_cleanup_recovers_lost_create_receipt_only_from_persisted_intent(setup):
    controller, env, api, _ = setup
    api.after_create = lambda _: lost()
    with pytest.raises(TransportError):
        controller.provision()
    env.actor = "supervisor"
    assert restart(setup).cleanup(lambda: True)
    assert not api.tables
    saved = env.refresh().bigquery["tables"]["0"]
    assert saved["receipt"] is not None and saved["deleted"]


def test_cleanup_refuses_replacement_and_resumes_lost_delete(setup):
    controller, env, api, _ = setup
    controller.provision()
    original = api.tables[0]["creationTime"]
    api.tables[0]["creationTime"] = str(NOW * 1000 + 1)
    with pytest.raises(Failure, match="Replacement table"):
        controller.cleanup(lambda: True)
    assert len(api.tables) == 10
    api.tables[0]["creationTime"] = original
    api.after_delete = lost
    with pytest.raises(TransportError):
        controller.cleanup(lambda: True)
    assert not env.refresh().bigquery["tables"]["0"]["deleted"]
    assert restart(setup).cleanup(lambda: True)
    assert not api.tables


def test_lost_owner_blocks_resource_operations(setup):
    controller, env, api, _ = setup
    env.owner = False
    with pytest.raises(Failure, match="ownership lost"):
        controller.provision()
    with pytest.raises(Failure, match="ownership lost"):
        controller.cleanup(lambda: True)
    assert api.calls == []


def test_real_adapter_collects_and_persists_paginated_aggregate(setup):
    controller = running(setup)
    _, env, api, app = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    real_api, http = client(
        api.plan,
        job(api.plan),
        page(api.plan, end=4, token="next"),
        page(api.plan, start=4),
    )
    controller = BigQueryLifecycle(env, real_api, app)
    result = controller.collect_query("final")
    assert len(result["rows"]) == 10
    assert len(http.calls) == 3
    assert controller.collect_query("final") == result
    assert len(http.calls) == 3


def test_real_adapter_cleanup_uses_receipt_and_confirms_absence(setup):
    controller, env, api, app = setup
    api.after_create = lambda _: lost()
    with pytest.raises(TransportError):
        controller.provision()
    real_api, http = client(
        api.plan, table(api.plan), table(api.plan), Response({}, 204), Response({}, 404)
    )
    assert BigQueryLifecycle(env, real_api, app).cleanup(lambda: True)
    assert [call[0] for call in http.calls] == ["GET", "GET", "DELETE", "GET"]


@pytest.mark.parametrize("flag", ["stop_requested", "evidence_failed"])
def test_run_stop_flags_close_query_admission(setup, flag):
    controller = running(setup)
    env, api = setup[1:3]
    controller.reserve_query("final")
    env.records._change(lambda record: setattr(record, flag, True))
    api.calls.clear()
    with pytest.raises(Failure, match="admission has stopped"):
        controller.submit_query("final")
    assert api.calls == []


def test_control_size_failure_does_not_consume_slot(setup, monkeypatch):
    import flink_tier3.bigquery_lifecycle as lifecycle
    from flink_tier3.common import json_bytes

    controller = running(setup)
    before = setup[1].refresh().bigquery
    monkeypatch.setattr(lifecycle, "MAX_CONTROL_BYTES", len(json_bytes(before)))
    with pytest.raises(Failure, match="control record exceeds"):
        controller.reserve_query("final")
    assert setup[1].refresh().bigquery == before


def test_collection_during_submission_check_cannot_regress_stage(setup):
    controller = running(setup)
    _, env, api, _ = setup
    controller.reserve_query("final")
    controller.submit_query("final")
    original = api.table

    def concurrent_collection(destination):
        api.table = original
        controller.collect_query("final")
        return original(destination)

    api.table = concurrent_collection
    api.calls.clear()
    with pytest.raises(Failure, match="cannot be resubmitted"):
        controller.submit_query("final")
    assert env.refresh().bigquery["queries"]["final"]["stage"] == "collected"
    assert not any(call[0] == "submit" for call in api.calls)


@pytest.mark.parametrize("change", ["namespace", "name", "arguments"])
def test_matching_hash_does_not_bypass_trial_binding(setup, change):
    _, env, api, app = setup
    if change == "arguments":
        app["spec"]["job"]["args"][7] = "ALO"
    else:
        app["metadata"][change] = "wrong"
    env.approval.application_sha256 = digest(app)
    with pytest.raises(Failure, match="approved application"):
        BigQueryLifecycle(env, api, app)


@pytest.mark.parametrize("mode", ["ALO", "EO"])
def test_fifty_destinations_preserve_all_receipts_and_cleanup(setup, mode):
    _, _, api, app = setup
    api.plan = replace(api.plan, trial=Trial("lifecycle-1312", mode, 50, 103))
    args = app["spec"]["job"]["args"]
    args[5], args[7], args[9] = mode, "50", "103"
    env = Environment(api.plan, app)
    controller = BigQueryLifecycle(env, api, app)
    controller.initialize()
    controller.provision()
    assert set(api.tables) == set(range(50))
    assert len(env.refresh().bigquery["tables"]) == 50
    assert controller.cleanup(lambda: True)
    assert not api.tables
