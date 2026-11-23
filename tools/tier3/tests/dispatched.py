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
"""A dispatched approval the shared rig tests run on, over the shared fake cluster.

The approval is a Cloud Tasks `sequence` trial's, the scenario that
needs nothing beyond its one queue; the exercise, fault-sequence and other
scenarios' tests change what they need of it. `renderer` stands in for what
`lifecycle/cloudtasks.cue` renders, as the CUE suite pins it.
"""

import copy
import json

import pytest
from flink_tier3 import proposal
from flink_tier3.cloudtasks import plan
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import digest
from flink_tier3.environment import Environment
from flink_tier3.model import APPROVAL_VERSION
from flink_tier3.policy import CLOUDTASKS, GAR, POD_RESOURCES

from .test_lifecycle import rt

RUN_ID = "ct-proposal"


@pytest.fixture
def inputs():
    return {
        "run_id": RUN_ID,
        "nonce": "a" * 32,
        "started_at": "2026-10-13T00:00:00Z",
        "expires_at": "2026-10-13T01:30:00Z",
        "active_seconds": 5220,
        "revision": "b" * 40,
        "application_image": GAR + "cloudtasks-recovery@sha256:" + "c" * 64,
        "trial": plan.trial("sequence"),
    }


@pytest.fixture
def renderer(monkeypatch):
    """What `lifecycle/cloudtasks.cue` renders, as the CUE suite pins it."""
    calls = []

    def render(run_id, nonce, expiry, seconds, **options):
        calls.append((run_id, nonce, expiry, seconds, options))
        trial = Trial(
            run_id,
            options["cloudtasks_records"],
            options["cloudtasks_records_per_second"],
            "sequence",
        )
        size = trial.parallelism("initial")
        resources = {
            category: dict(POD_RESOURCES["smoke"])
            for category in ("requests", "limits")
        }
        initial = {
            "metadata": {
                "name": run_id,
                "namespace": "tier3-cloudtasks",
                "annotations": {
                    "flink-gcp.io/approval": nonce,
                    "flink-gcp.io/expires-at": expiry,
                },
            },
            "spec": {
                "image": options["application_image"],
                "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2Gi"}},
                "taskManager": {
                    "replicas": size,
                    "resource": {"cpu": 1, "memory": "2Gi"},
                },
                "job": {"parallelism": size, "args": trial.arguments("initial")},
            },
        }
        for manager in ("jobManager", "taskManager"):
            initial["spec"][manager]["podTemplate"] = {
                "spec": {"containers": [{"resources": copy.deepcopy(resources)}]}
            }
        upgrade = copy.deepcopy(initial)
        upgrade["spec"]["job"]["args"] = trial.arguments("upgrade")
        upgrade["spec"]["job"]["parallelism"] = trial.parallelism("upgrade")
        upgrade["spec"]["taskManager"]["replicas"] = trial.parallelism("upgrade")
        delivery = {
            "config": {
                "data": {
                    "approval.json": "{}",
                    "application.json": json.dumps(initial),
                    "upgrade-application.json": json.dumps(upgrade),
                }
            },
            "supervisor": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        category: dict(POD_RESOURCES["supervisor"])
                                        for category in ("requests", "limits")
                                    },
                                    "image": GAR + "lifecycle-tools@sha256:" + "d" * 64,
                                }
                            ]
                        }
                    }
                }
            },
        }
        return initial, upgrade, delivery

    monkeypatch.setattr(proposal, "render", render)
    return calls


@pytest.fixture
def prepared(env, inputs, renderer):
    """A dispatched Cloud Tasks approval over the shared fake cluster."""
    kube, store, approval, clock = env
    inputs.update(run_id=approval["run_id"], nonce=approval["nonce"])
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    clock.now = rt.timestamp(inputs["started_at"])
    application = bundle["application"]
    application.update(apiVersion="flink.apache.org/v1beta1", kind="FlinkDeployment")
    upgrade = copy.deepcopy(application)
    upgrade["spec"]["job"]["args"] = bundle["upgrade_application"]["spec"]["job"][
        "args"
    ]
    saved = approval["namespaces"].pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=CLOUDTASKS, uid=CLOUDTASKS + "-quota")
    kube.put(quota)
    saved.update(uid=CLOUDTASKS + "-uid", quota_uid=CLOUDTASKS + "-quota")
    approval["namespaces"][CLOUDTASKS] = saved
    approval.update(
        version=APPROVAL_VERSION,
        scenario="cloudtasks-recovery",
        started_at=proposal["started_at"],
        expires_at=proposal["expires_at"],
        cleanup_at=proposal["cleanup_at"],
        cloudtasks_trial=plan.trial("sequence"),
        application_sha256=digest(application),
        upgrade_application_sha256=digest(upgrade),
    )
    approval["images"].pop("smoke")
    approval["images"]["application"] = inputs["application_image"]
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    return environment, application, upgrade, proposal
