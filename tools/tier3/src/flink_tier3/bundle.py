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
"""Identity and delivery of the installed Tier-3 package's source and policy."""

import hashlib
import json
from importlib.resources import files


def package_sources(directory=None):
    directory = files("flink_tier3") if directory is None else directory
    return {
        path.name: path.read_bytes().decode("utf-8")
        for path in sorted(directory.iterdir(), key=lambda path: path.name)
        if path.is_file() and path.name.endswith((".py", ".toml"))
    }


def source_digest(directory=None):
    hashes = {
        "flink_tier3/" + name: hashlib.sha256(content.encode()).hexdigest()
        for name, content in package_sources(directory).items()
    }
    return hashlib.sha256(
        json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
