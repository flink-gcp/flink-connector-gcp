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
"""The repository checkout the checkout commands read.

The CLI sets `ROOT` once, from `--repository`, before it runs a checkout
command. Read it as `repository.ROOT` when it is used: a name imported from
this module is bound when it is imported, before the CLI sets it, and would
resolve every path from the working directory.
"""

from pathlib import Path

ROOT = Path.cwd()
