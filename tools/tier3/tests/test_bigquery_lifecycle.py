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
from flink_tier3.policy import BIGQUERY_STATE
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
        self.schedule = SimpleNamespace(started=NOW)
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
        self.deadlines = []
        # What a FILE_LOADS cleanup lists: connector jobs not yet DONE, and
        # temporary tables in the dataset.
        self.unfinished = []
        self.temporary = []

    def with_deadline(self, deadline):
        # This fake has no transport; composed REST tests enforce the deadline.
        self.deadlines.append(deadline)
        return self

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

    def unfinished_connector_jobs(self, since):
        self.calls.append(("connector-jobs", since))
        return copy.deepcopy(self.unfinished)

    def temporary_tables(self, since):
        self.calls.append(("temporary-tables", since))
        return [{"table": name, "creationTime": "1"} for name in self.temporary]

    def delete_temporary_table(self, name):
        self.calls.append(("delete-temporary", name))
        self.temporary.remove(name)

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


def test_a_failed_receipt_write_does_not_replace_the_create_failure(setup):
    """The receipts are one write now; losing it must not hide why creates stopped."""
    controller, env, api, _ = setup
    create = api.ensure_table

    def third_create_lost(destination):
        observed = create(destination)
        if destination == 2:
            lost()
        return observed

    api.ensure_table = third_create_lost
    change = controller._change
    intents = []

    def refuse_receipts(edit, **kwargs):
        if intents:
            raise Failure("Fixture storage refused the receipt write")
        intents.append(edit)
        return change(edit, **kwargs)

    controller._change = refuse_receipts
    with pytest.raises(TransportError, match="Lost response"):
        controller.provision()
    tables = env.refresh().bigquery["tables"]
    # Two receipts were gathered and their write refused; every intent
    # survives, so cleanup can still recover them.
    assert set(api.tables) == {0, 1, 2}
    assert len(tables) == api.plan.trial.destinations
    assert all(saved["receipt"] is None for saved in tables.values())


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
    # Every intent was persisted before the first create, so cleanup also
    # confirms the never-created tables absent.
    others = api.plan.trial.destinations - 1
    real_api, http = client(
        api.plan,
        table(api.plan),
        table(api.plan),
        Response({}, 204),
        Response({}, 404),
        *[Response({}, 404) for _ in range(2 * others)],
    )
    assert BigQueryLifecycle(env, real_api, app).cleanup(lambda: True)
    assert [call[0] for call in http.calls] == ["GET", "GET", "DELETE", "GET"] + [
        "GET"
    ] * (2 * others)


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
    provisioning = []
    change = controller._change
    controller._change = lambda edit, **kwargs: (
        provisioning.append(edit),
        change(edit, **kwargs),
    )[1]
    controller.provision()
    controller._change = change
    # bq1312-alo-50-a2 wrote twice per table and stopped halfway.
    assert len(provisioning) == 2
    assert set(api.tables) == set(range(50))
    assert len(env.refresh().bigquery["tables"]) == 50
    writes = []
    change = controller._change
    controller._change = lambda edit, **kwargs: (
        writes.append(edit),
        change(edit, **kwargs),
    )[1]
    assert controller.cleanup(lambda: True)
    assert not api.tables
    # bq1312-alo-50-a1 wrote once per table and hit Cloud Storage's rate limit;
    # now the stop request and the pass's result are the only two writes.
    assert len(writes) == 2
    state = env.refresh().bigquery
    assert state["cleaned"] and all(t["deleted"] for t in state["tables"].values())


def test_collected_queries_accumulate_the_bytes_the_trial_was_billed(setup):
    """Each query is refused above its own limit; nothing summed them before."""
    _, env, _, _ = setup
    controller = running(setup)
    assert env.refresh().bigquery["billed_bytes"] == 0
    for name in ("baseline", "final"):
        controller.reserve_query(name)
        controller.submit_query(name)
        controller.collect_query(name)
    billed = env.refresh().bigquery["billed_bytes"]
    assert billed > 0
    # A second collection returns on the evidence pointer it already has.
    controller.collect_query("final")
    assert env.refresh().bigquery["billed_bytes"] == billed


def test_a_conflicting_collection_does_not_bill_the_same_query_twice(setup):
    """`_change` re-reads and re-applies its edit, so accumulating would double."""
    _, env, _, _ = setup
    controller = running(setup)
    other = restart(setup)
    controller.reserve_query("final")
    controller.submit_query("final")
    # The second collector lands between this one's read and its write, so the
    # edit is re-applied against a record that already carries the same pointer.
    env.store.before_write = lambda: other.collect_query("final")
    controller.collect_query("final")
    assert env.store.conflicts == 1
    queries = env.refresh().bigquery["queries"]
    assert env.refresh().bigquery["billed_bytes"] == queries["final"]["billed"]


@pytest.fixture
def file_loads():
    plan = ResourcePlan(
        Trial("lifecycle-1312", "FILE_LOADS", 10, 23),
        "a" * 32,
        (NOW + 3600) * 1000,
        3,
        1024**3,
        60000,
    )
    app = {
        "metadata": {"name": plan.trial.run_id, "namespace": "tier3-bigquery"},
        "spec": {"job": {"args": plan.trial.arguments("initial")}},
    }
    env = Environment(plan, app)
    api = Resources(plan)
    controller = BigQueryLifecycle(env, api, app)
    controller.initialize()
    controller.provision()
    env.actor = "supervisor"
    api.calls.clear()
    return controller, env, api, app


LOAD = {
    "id": "flink-bq-load-" + "f" * 32 + "-c3-" + "0" * 16,
    "kind": "load",
    "table": "bq_lifecycle_1312_d0",
    "state": "RUNNING",
    "failed": False,
}


def test_a_file_loads_application_renders_its_sink_inputs():
    args = Trial("lifecycle-1312", "FILE_LOADS", 10, 23).arguments("initial")
    assert args[13:] == [
        "false",
        "--staging-format",
        "AVRO",
        "--max-concurrent-checkpoint-finalizations",
        "1",
        "--max-concurrent-destinations",
        "8",
        "--max-staging-file-bytes",
        "16777216",
        "--max-open-destinations",
        "16",
    ]


def test_cleanup_waits_for_a_load_job_still_running_at_the_barrier(file_loads):
    """The Pods are gone; a load they submitted is still the service's to run."""
    controller, env, api, _ = file_loads
    api.unfinished = [LOAD]
    assert not controller.cleanup(lambda: True)
    assert ("connector-jobs", NOW) in api.calls
    # Neither a temporary table nor a run table is touched while it runs.
    assert not [c for c in api.calls if c[0] in ("delete", "temporary-tables")]
    assert len(api.tables) == 10
    state = env.refresh().bigquery
    # Recorded, because one outliving the workload is a finding.
    assert state["file_loads"] == {
        "unfinished_jobs": [LOAD["id"]],
        "temporary_tables": [],
    }
    assert not state["cleaned"]
    api.unfinished = []
    assert controller.cleanup(lambda: True)
    assert not api.tables
    state = env.refresh().bigquery
    assert state["cleaned"]
    assert state["file_loads"] == {
        "unfinished_jobs": [LOAD["id"]],
        "temporary_tables": [],
        "staging": {"objects": 0, "bytes": 0, "names": []},
    }


def test_a_leftover_staged_object_is_counted_before_state_cleanup_deletes_it(
    file_loads,
):
    controller, env, _, _ = file_loads
    prefix = "runs/lifecycle-1312/staging/" + "f" * 32 + "/"
    for index in range(25):
        env.store.write_bytes(f"{prefix}d0/{index:02d}.avro", b"x" * 10, BIGQUERY_STATE)
    # Outside the staging prefix: a checkpoint is not a staged file.
    env.store.write_bytes("runs/lifecycle-1312/checkpoints/x", b"y", BIGQUERY_STATE)
    assert controller.cleanup(lambda: True)
    staging = env.refresh().bigquery["file_loads"]["staging"]
    assert staging["objects"] == 25 and staging["bytes"] == 250
    assert staging["names"] == [f"{prefix}d0/{index:02d}.avro" for index in range(20)]
    # The objects stay for state cleanup, which deletes the whole run prefix,
    # and a later pass keeps the count rather than reading the emptied prefix.
    assert len(env.store.objects(prefix, BIGQUERY_STATE)) == 25
    for index in range(25):
        env.store.delete(
            f"{prefix}d0/{index:02d}.avro",
            env.store._generation(f"{prefix}d0/{index:02d}.avro", BIGQUERY_STATE),
            BIGQUERY_STATE,
        )
    assert controller.cleanup(lambda: True)
    assert env.refresh().bigquery["file_loads"]["staging"]["objects"] == 25


def test_an_unreadable_staging_prefix_does_not_keep_the_tables(file_loads):
    """Storage failures must not keep paid resources alive."""
    controller, env, api, _ = file_loads

    def objects(prefix, bucket, maximum):
        raise Failure("Object inventory exceeds its count ceiling")

    env.store.objects = objects
    assert controller.cleanup(lambda: True)
    assert not api.tables
    assert env.refresh().bigquery["file_loads"]["staging"] == {
        "unreadable": "Object inventory exceeds its count ceiling"
    }


def test_temporary_tables_are_recorded_before_they_are_deleted(file_loads):
    controller, env, api, _ = file_loads
    temporary = "tmp_" + "f" * 32 + "_0123456789ab_c3_p0"
    api.temporary = [temporary]
    recorded = []
    api_delete = api.delete_temporary_table

    def delete(name):
        recorded.append(env.refresh().bigquery["file_loads"]["temporary_tables"])
        api_delete(name)

    api.delete_temporary_table = delete
    # The pass that deletes cannot also say nothing is left; the next one can.
    assert not controller.cleanup(lambda: True)
    assert recorded == [[temporary]]
    assert len(api.tables) == 10
    assert controller.cleanup(lambda: True)
    assert env.refresh().bigquery["file_loads"]["temporary_tables"] == [temporary]
    assert not api.tables


def test_a_file_loads_cleanup_adds_no_write_to_the_pass_that_settles(file_loads):
    """Cloud Storage refuses about one mutation a second on the control record."""
    controller, _, _, _ = file_loads
    writes = []
    change = controller._change
    controller._change = lambda edit, **kwargs: (
        writes.append(edit),
        change(edit, **kwargs),
    )[1]
    assert controller.cleanup(lambda: True)
    # The stop request and the pass's result, which carries the FILE_LOADS
    # account, as a Storage Write cleanup writes.
    assert len(writes) == 2


def test_a_storage_write_cleanup_lists_no_connector_job(setup):
    running(setup)
    _, env, api, _ = setup
    env.actor = "supervisor"
    assert restart(setup).cleanup(lambda: True)
    assert not [c for c in api.calls if c[0] in ("connector-jobs", "temporary-tables")]
    assert "file_loads" not in env.refresh().bigquery


def test_only_the_supervisor_observes_connector_jobs(file_loads):
    controller, env, api, _ = file_loads
    api.unfinished = [LOAD]
    assert controller.unfinished_connector_jobs() == [LOAD]
    env.actor = "runner"
    with pytest.raises(Failure, match="Only the supervisor"):
        controller.unfinished_connector_jobs()
