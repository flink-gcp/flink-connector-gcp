# Copyright 2026 laughingman7743
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
"""Shared plumbing for the scripts/ tests.

The scripts are executables with hyphenated names, not importable modules, so
unit tests load them through importlib while CLI-level tests run them as the
subprocesses they really are.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent


def load_script(filename: str):
    """Import a scripts/<filename> as a module despite the hyphens."""
    module_name = filename.removesuffix(".py").replace("-", "_")
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def ci_maven_args():
    return load_script("ci-maven-args.py")


@pytest.fixture(scope="session")
def check_option_docs():
    return load_script("check-option-docs.py")


@pytest.fixture(scope="session")
def check_flink_api_tiers():
    return load_script("check-flink-api-tiers.py")


@pytest.fixture(scope="session")
def check_notice():
    return load_script("check-notice.py")
