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
CEILINGS = _policy["ceilings"]
POD_RESOURCES = _policy["pod_resources"]
RECOVERY = _policy["recovery"]

RUN_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?\Z")


SHA = re.compile(r"[0-9a-f]{40}\Z")


LABEL = "flink-gcp.io/run-id"


NONCE = "flink-gcp.io/approval"


MIB = 1024 * 1024


POLL = 15


HTTP_TIMEOUT = 20
