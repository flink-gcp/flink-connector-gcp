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
"""The BigQuery lifecycle actors' OAuth session."""

from ..actor_auth import PRINCIPALS, SCOPES, USERINFO, ActorSession
from .resources import BASE

__all__ = ["PRINCIPALS", "SCOPES", "USERINFO", "BigQuerySession"]


class BigQuerySession(ActorSession):
    """One BigQuery actor's session: query, read and delete, nothing else."""

    SERVICE = "BigQuery"
    PREFIX = BASE + "/"
    METHODS = ("GET", "POST", "DELETE")
