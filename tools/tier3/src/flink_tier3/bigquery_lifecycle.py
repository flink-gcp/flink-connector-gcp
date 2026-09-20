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
"""Persist BigQuery resource intent, query slots and cleanup progress."""

from __future__ import annotations

import copy
import re
from dataclasses import asdict

from .common import ApiError, Failure, digest, json_bytes
from .model import Phase

MAX_CONTROL_BYTES = 256 * 1024


class BigQueryLifecycle:
    """Resource controller for a future admitted BigQuery lifecycle actor.

    The caller owns budget approval and authenticated actor identity. Cleanup
    additionally requires an external creator/writer quiescence barrier; this
    controller's persisted stop flag alone cannot fence an in-flight API call.
    """

    def __init__(self, env, resources, application):
        self.env, self.api = env, resources
        self.plan = resources.plan
        trial = self.plan.trial
        approval = env.approval
        expected_args = [
            "--run-id",
            trial.run_id,
            "--phase",
            "initial",
            "--mode",
            trial.mode,
            "--destinations",
            str(trial.destinations),
            "--records",
            str(trial.records),
            "--bytes-per-second",
            "1048576",
            "--require-restored",
            "false",
        ]
        if (
            approval.scenario != "bigquery-recovery"
            or approval.run_id != trial.run_id
            or approval.nonce != self.plan.nonce
            or approval.bigquery_plan != self.plan
            or digest(application) != approval.application_sha256
            or application.get("metadata", {}).get("name") != trial.run_id
            or application.get("metadata", {}).get("namespace") != "tier3-bigquery"
            or application.get("spec", {}).get("job", {}).get("args") != expected_args
        ):
            raise Failure("BigQuery resources differ from the approved application")
        self.intent = {
            "version": 1,
            "plan": asdict(self.plan),
            "application_sha256": approval.application_sha256,
        }
        self.prefix = f"runs/{trial.run_id}/bigquery/"

    def _runner(self):
        if self.env.actor != "runner":
            raise Failure(
                "Only the submitting runner may create or collect BigQuery resources"
            )

    def _state(self, record):
        state = record.bigquery
        if not isinstance(state, dict) or state.get("intent") != self.intent:
            raise Failure("Missing or replaced BigQuery resource intent")
        return state

    def _read(self):
        self.env.assert_owner()
        return self._state(self.env.refresh())

    def _change(self, edit, *, open_only=False):
        self.env.assert_owner()

        def change(record):
            state = self._state(record)
            if open_only and (
                state["stopped"]
                or record.stop_requested
                or record.evidence_failed
                or record.phase in (Phase.CLEANING, Phase.CLEANED)
            ):
                raise Failure("BigQuery resource admission has stopped")
            edit(state)
            if len(json_bytes(state)) > MAX_CONTROL_BYTES:
                raise Failure("BigQuery control record exceeds 256 KiB")

        return self._state(self.env.records._change(change))

    def initialize(self):
        """Persist intent before any table or query write; never replace it."""
        self._runner()
        self.env.admission_open()

        def initialize(record):
            if record.bigquery is not None:
                self._state(record)
                return
            if (
                record.phase not in (Phase.APPROVED, Phase.READY)
                or record.stop_requested
                or record.evidence_failed
            ):
                raise Failure("BigQuery resource admission has stopped")
            record.bigquery = {
                "intent": copy.deepcopy(self.intent),
                "stopped": False,
                "tables": {},
                "queries": {},
                "cleaned": False,
            }

        self.env.assert_owner()
        self.env.records._change(initialize)

    def _admission(self, *, query=False):
        self._runner()
        if query:
            self.env.require_running("BigQuery query admission has stopped")
        else:
            self.env.admission_open()
        if self._read()["stopped"]:
            raise Failure("BigQuery resource admission has stopped")

    def provision(self):
        """Persist each create intent and receipt; resume only recorded intents."""
        for destination in range(self.plan.trial.destinations):
            self._admission()
            key = str(destination)
            saved = self._read()["tables"].get(key)
            if saved is None:
                if self.api.table(destination) is not None:
                    raise Failure(
                        "BigQuery table predates its persisted creation intent"
                    )

                def intend(state, key=key):
                    state["tables"].setdefault(key, {"receipt": None, "deleted": False})

                self._change(intend, open_only=True)
            self._admission()
            saved = self._read()["tables"][key]
            if saved["receipt"] is not None:
                self._table(destination, saved["receipt"])
                continue
            observed = self.api.ensure_table(destination)
            receipt = self._receipt(destination, observed)

            def remember(state, key=key, receipt=receipt):
                previous = state["tables"][key]["receipt"]
                if previous not in (None, receipt):
                    raise Failure("BigQuery table receipt cannot be replaced")
                state["tables"][key]["receipt"] = receipt

            # A stop may arrive after the service accepts the create. Keep its
            # receipt for cleanup even though no new admission is permitted.
            self._change(remember)

    def _receipt(self, destination, table):
        return {
            **self.plan.table_body(destination),
            "type": "TABLE",
            "creationTime": table["creationTime"],
        }

    def _table(self, destination, receipt):
        current = self.api.table(destination)
        if current is None or self._receipt(destination, current) != receipt:
            raise Failure("Recorded BigQuery table is missing or replaced")

    def reserve_query(self, name):
        """Allocate one durable slot per observation name; retries reuse it."""
        if not isinstance(name, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9-]{0,39}", name
        ):
            raise ValueError("Query observation name must be a short lowercase label")
        self._admission(query=True)

        def reserve(state):
            if name in state["queries"]:
                return
            slot = len(state["queries"])
            if slot >= self.plan.query_slots:
                raise Failure("BigQuery query slot budget exhausted")
            state["queries"][name] = {
                "slot": slot,
                "stage": "reserved",
                "evidence": None,
            }

        return self._change(reserve, open_only=True)["queries"][name]["slot"]

    def submit_query(self, name):
        """Mark the reserved slot before submission; never create a retry ID."""
        self._admission(query=True)
        state = self._read()
        saved = state["queries"].get(name)
        if saved is None:
            raise Failure("BigQuery query slot was not reserved")
        if saved["stage"] == "collected":
            raise Failure("A collected observation cannot be resubmitted")
        for destination in range(self.plan.trial.destinations):
            table = state["tables"].get(str(destination))
            if table is None or table["receipt"] is None or table["deleted"]:
                raise Failure("BigQuery query requires every table receipt")
            self._table(destination, table["receipt"])

        def submitting(value):
            item = value["queries"][name]
            if item["stage"] == "collected":
                raise Failure("A collected observation cannot be resubmitted")
            if item["stage"] == "reserved":
                item["stage"] = "submitting"

        self._change(submitting, open_only=True)
        self._admission(query=True)
        result = self.api.submit_query(saved["slot"])

        def submitted(value):
            item = value["queries"][name]
            if item["stage"] == "submitting":
                item["stage"] = "submitted"

        self._change(submitted)
        return result

    def collect_query(self, name, *, max_bytes=None):
        """Persist successful job evidence before recording its durable pointer."""
        self._runner()
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Query evidence limit must be a positive byte count")
        state = self._read()
        saved = state["queries"].get(name)
        if saved is None or saved["stage"] == "reserved":
            raise Failure("BigQuery query was not submitted")
        path = self.prefix + f"queries/{saved['slot']}.json"
        if saved["evidence"] is not None:
            value, generation = self.env.store.read(path)
            if (
                value is None
                or generation != saved["evidence"]["generation"]
                or digest(value) != saved["evidence"]["sha256"]
            ):
                raise Failure("BigQuery query evidence was replaced or removed")
            if max_bytes is not None and len(json_bytes(value)) > max_bytes:
                raise Failure("BigQuery query evidence exceeds its byte budget")
            return value["result"]
        result = self.api.results(saved["slot"])
        artifact = {
            "intent_sha256": digest(self.intent),
            "observation": name,
            "slot": saved["slot"],
            "result": result,
        }
        if max_bytes is not None and len(json_bytes(artifact)) > max_bytes:
            raise Failure("BigQuery query evidence exceeds its byte budget")
        try:
            generation = self.env.store.write(path, artifact)
        except ApiError as error:
            if error.status not in (409, 412):
                raise
            previous, generation = self.env.store.read(path)
            if previous != artifact:
                raise Failure(
                    "BigQuery query evidence path contains different data"
                ) from error
        pointer = {"generation": generation, "sha256": digest(artifact)}

        def remember(state):
            item = state["queries"][name]
            if item["evidence"] not in (None, pointer):
                raise Failure("BigQuery query evidence pointer cannot be replaced")
            item.update(stage="collected", evidence=pointer)

        self._change(remember)
        return result

    def request_stop(self):
        return self._change(lambda state: state.update(stopped=True))

    def cleanup(self, quiesce):
        """Converge one cleanup pass, returning False while queries are pending.

        quiesce must stop/join every creator and writer, exclude replacement and
        return True only after that barrier. It must cover other lifecycle
        processes and server-side in-flight requests, not just Flink Pods.
        Uncertain submissions with no readable job remain unresolved.
        """
        self.request_stop()
        if quiesce() is not True:
            raise Failure("BigQuery creators and writers are not quiescent")
        state = self._read()
        pending = False
        for saved in state["queries"].values():
            if saved["stage"] == "reserved":
                continue
            self.env.assert_owner()
            job = self.api.cancel_query(saved["slot"])
            # An absent job after an uncertain submit does not prove that a
            # late service operation cannot still appear.
            if job is None or job["status"]["state"] != "DONE":
                pending = True
        if pending:
            return False
        for key, saved in state["tables"].items():
            self.env.assert_owner()
            destination = int(key)
            current = self.api.table(destination)
            if current is not None:
                receipt = saved["receipt"]
                if receipt is None:
                    # Only this persisted create intent authorizes recovery of
                    # a lost response, after the external quiescence barrier.
                    receipt = self._receipt(destination, current)
                    self._change(
                        lambda value, key=key, receipt=receipt: value["tables"][
                            key
                        ].update(receipt=receipt)
                    )
                self.api.delete_table(destination, receipt)
            if self.api.table(destination) is not None:
                return False
            self._change(
                lambda value, key=key: value["tables"][key].update(deleted=True)
            )
        self._change(lambda value: value.update(cleaned=True))
        return True
