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
"""The caller contract a resource controller runs against, over generation-checked storage.

Its approval is a Cloud Tasks trial's; the other scenarios' controller tests
subclass it and name their own plan.
"""

from types import SimpleNamespace

from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import Failure, digest
from flink_tier3.model import Phase, RunRecord
from flink_tier3.records import Records

from .doubles import NOW
from .test_lifecycle import Store

TRIAL = Trial("ct-lifecycle", 100, 10)


def application(trial=TRIAL):
    return {
        "metadata": {"name": trial.run_id, "namespace": "tier3-cloudtasks"},
        "spec": {"job": {"args": trial.arguments("initial")}},
    }


class Environment:
    """The controller's caller contract, over generation-checked storage."""

    def __init__(self, trial=TRIAL):
        self.approval = SimpleNamespace(
            scenario="cloudtasks-recovery",
            run_id=trial.run_id,
            nonce="a" * 32,
            cloudtasks_plan=trial,
            application_sha256=digest(application(trial)),
        )
        self.actor = "runner"
        self.now = NOW
        self.owner = True
        self.store = Store()
        self.records = Records(self.store, self.approval, lambda: self.now)
        self.store.write(self.records.path, RunRecord("a" * 32).to_dict())
        self.events = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def emit(self, event, payload):
        self.events.append((event, payload))

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
