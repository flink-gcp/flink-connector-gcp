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
"""Tests for scripts/check-flink-release.sh.

The check is the only thing that announces a Flink release to this repository,
so a wrong green is the failure that matters: a pin that has fallen behind
Maven Central while the weekly job reports success. Flink 1.20.5 went unnoticed
that way for nearly five months (#1973), before the LTS half existed.

The metadata is synthetic, served through FLINK_METADATA_URL as a file:// URL,
so the outcome never depends on what Flink has released by the day the suite
runs.
"""

import subprocess

import pytest
from conftest import SCRIPTS

SCRIPT = SCRIPTS / "check-flink-release.sh"

# Shapes the real flink-core metadata carries: older 1.x minors, previews, and
# releases out of lexical order (1.20.10 sorts before 1.20.9 as a string).
RELEASES = (
    "1.19.3",
    "1.20.0",
    "1.20.9",
    "1.20.10",
    "2.0-preview1",
    "2.0.0",
    "2.2.1",
    "2.3.0",
    "2.4-SNAPSHOT",
)


def run(tmp_path, *args, releases=RELEASES):
    metadata = tmp_path / "maven-metadata.xml"
    versions = "".join(f"<version>{v}</version>" for v in releases)
    metadata.write_text(
        f"<metadata><versioning><versions>{versions}</versions></versioning></metadata>"
    )
    return subprocess.run(
        [str(SCRIPT), *args],
        env={"PATH": "/usr/bin:/bin", "FLINK_METADATA_URL": metadata.as_uri()},
        capture_output=True,
        text=True,
        check=False,
    )


def test_current_ceiling_and_lts_pass(tmp_path):
    result = run(tmp_path, "2.3.0", "1.20.10")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Supported range is current." in result.stdout
    assert "LTS patch is current." in result.stdout


def test_ceiling_alone_still_works_without_an_lts(tmp_path):
    result = run(tmp_path, "2.3.0")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "LTS" not in result.stdout


def test_a_newer_lts_patch_fails_with_the_edit_list(tmp_path):
    # 1.20.9 against a newest 1.20.10: a string comparison would call 1.20.9
    # the newer one and pass.
    result = run(tmp_path, "2.3.0", "1.20.9")
    assert result.returncode == 1
    assert "Flink 1.20.10 is released" in result.stdout
    assert "FLINK_LTS -> 1.20.10" in result.stdout
    assert "just verify-flink 1.20.10" in result.stdout
    assert "Supported range is current." in result.stdout


def test_a_newer_minor_of_another_line_is_not_an_lts_patch(tmp_path):
    # 1.19.3 and 2.x releases share the 1.x major or the version list, and
    # neither is a newer 1.20 patch.
    result = run(tmp_path, "2.3.0", "1.20.10", releases=(*RELEASES, "1.19.4", "2.3.1"))
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_newer_ceiling_minor_fails_and_does_not_hide_a_stale_lts(tmp_path):
    result = run(tmp_path, "2.2.1", "1.20.9")
    assert result.returncode == 1
    assert "Flink 2.3.0 is released" in result.stdout
    assert "Flink 1.20.10 is released" in result.stdout


def test_an_lts_pin_that_is_not_a_release_fails(tmp_path):
    # A typo ahead of every real patch would otherwise read as current.
    result = run(tmp_path, "2.3.0", "1.20.11")
    assert result.returncode == 1
    assert "1.20.11 is not a Flink release" in result.stdout


def test_no_release_of_the_lts_minor_fails(tmp_path):
    result = run(tmp_path, "2.3.0", "1.20.10", releases=("1.19.3", "2.3.0"))
    assert result.returncode == 1
    assert "Could not read any 1.20 release" in result.stdout


@pytest.mark.parametrize("lts", ["1.19.3", "1.21.0", "2.3.0"])
def test_an_lts_outside_the_1_20_line_is_rejected(tmp_path, lts):
    # 1.19.3 is a current release of its own minor, so a check that derived
    # the line from the pin would pass it; the -1.20 release line cannot use it.
    result = run(tmp_path, "2.3.0", lts)
    assert result.returncode == 2
    assert "must be a 1.20.x patch" in result.stderr
    assert "is current" not in result.stdout


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("2.3",),
        ("2.3.0", ""),
        ("2.3.0", "1.20"),
        ("2.3.0", "1.20.x"),
        ("2.3.0", "1.20.10", "garbage"),
    ],
)
def test_malformed_arguments_are_rejected(tmp_path, args):
    # An empty second argument is what a FLINK_LTS: '' pin becomes; the last
    # case is what an unquoted '1.20.10 garbage' pin would become.
    result = run(tmp_path, *args)
    assert result.returncode == 2
    assert result.stderr
    assert "is current" not in result.stdout


def test_unreadable_metadata_fails(tmp_path):
    result = subprocess.run(
        [str(SCRIPT), "2.3.0", "1.20.10"],
        env={
            "PATH": "/usr/bin:/bin",
            "FLINK_METADATA_URL": (tmp_path / "absent.xml").as_uri(),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "is current" not in result.stdout
