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
"""The job's REST API, read through its Service, and the checkpoints event."""

from .policy import MIB

# The evidence event carrying each poll's checkpoint statistics of the job:
# the supervisor writes it, and an offline analyzer reads it.
CHECKPOINTS_EVENT = "checkpoints"


class FlinkRest:
    """Read the running job's REST API through its own Service, by proxy.

    The namespace comes from the approval, so the same endpoints can be read
    from every recovery exercise.
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
