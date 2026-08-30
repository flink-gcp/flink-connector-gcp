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
"""Synthetic coverage for scripts/stage-release-guard.py."""

import pytest
from conftest import load_script


@pytest.fixture(scope="session")
def guard():
    return load_script("stage-release-guard.py")


class TestClassify:
    @pytest.mark.parametrize(
        "version,kind",
        [
            ("1.0.0", "bare"),
            ("0.1.0", "bare"),
            ("10.20.30", "bare"),
            ("1.0.0-1.20", "lts"),
            ("2.3.4-1.20", "lts"),
        ],
    )
    def test_well_formed(self, guard, version, kind):
        assert guard.classify(version) == kind

    @pytest.mark.parametrize(
        "version",
        [
            "v1.0.0",  # the tag-name shape
            "01.0.0",  # leading zero, not SemVer
            "1.00.0",
            "1.0.00",
            "1.0",
            "1.0.0-SNAPSHOT",
            "1.0.0-2.2",  # only the LTS suffix exists
            "1.0.0-1.20.4",  # the suffix is the minor, not a patch
            "1.0.0-1.20 ",  # trailing junk
            "1.0.0\n",  # $ would match before a trailing newline; \Z must not
            "1.0.0-1.20\n",
            "",
        ],
    )
    def test_rejected(self, guard, version):
        assert guard.classify(version) is None


class TestToolchainError:
    def test_lts_match_passes(self, guard):
        assert guard.toolchain_error("lts", "1.20.4", "flink1", None) is None

    @pytest.mark.parametrize(
        "version,compat",
        [
            ("2.2.1", "flink2"),  # the --define bypass outcome
            ("2.2.1", "flink1"),  # missing -Dflink.version
            ("1.20.4", "flink2"),  # compat overridden back
            ("1.19.1", "flink1"),  # a 1.x that is not the LTS
        ],
    )
    def test_lts_mismatch_names_effective_values(self, guard, version, compat):
        error = guard.toolchain_error("lts", version, compat, None)
        assert version in error and compat in error

    def test_bare_match_passes(self, guard):
        assert guard.toolchain_error("bare", "2.2.1", "flink2", "2.2.1") is None

    @pytest.mark.parametrize(
        "version,compat",
        [
            ("1.20.4", "flink2"),  # 1.x under a bare version
            ("2.3.0", "flink2"),  # not the pinned floor
            ("2.2.1", "flink1"),  # compat flipped without a version
        ],
    )
    def test_bare_mismatch_names_floor_and_effective(self, guard, version, compat):
        error = guard.toolchain_error("bare", version, compat, "2.2.1")
        assert "2.2.1" in error and version in error and compat in error


class TestPinnedFloor:
    def test_reads_the_pom_literal(self, guard):
        pom = "<properties>\n  <flink.version>2.2.1</flink.version>\n</properties>"
        assert guard.pinned_floor(pom) == "2.2.1"

    def test_missing_literal_is_fatal(self, guard):
        with pytest.raises(SystemExit):
            guard.pinned_floor("<properties/>")


class TestMain:
    def test_malformed_version_probes_nothing(self, guard):
        def explode(expression, extra):  # pragma: no cover - must not run
            raise AssertionError("evaluate must not be called")

        message = guard.main(["v1.0.0"], evaluate_fn=explode)
        assert "no leading v" in message

    def test_lts_probe_carries_the_compat_flag_and_extras(self, guard):
        calls = []

        def fake(expression, extra):
            calls.append((expression, tuple(extra)))
            return {"flink.version": "1.20.4", "flink.compat": "flink1"}[expression]

        assert guard.main(["1.0.0-1.20", "-Dx=y"], evaluate_fn=fake) is None
        assert calls == [
            ("flink.version", ("-Dflink.compat=flink1", "-Dx=y")),
            ("flink.compat", ("-Dflink.compat=flink1", "-Dx=y")),
        ]

    def test_bare_compares_against_the_pom_literal_not_a_probe(self, guard):
        # An environment channel (MAVEN_OPTS, a settings profile) shifts every
        # probe equally; the expectation must come from the pom text instead.
        def fake(expression, extra):
            return {"flink.version": "2.3.0", "flink.compat": "flink2"}[expression]

        message = guard.main(
            ["1.0.0"],
            evaluate_fn=fake,
            pom_reader=lambda: "<flink.version>2.2.1</flink.version>",
        )
        assert "2.2.1" in message and "2.3.0" in message

    def test_bare_match_is_silent(self, guard):
        def fake(expression, extra):
            return {"flink.version": "2.2.1", "flink.compat": "flink2"}[expression]

        assert (
            guard.main(
                ["1.0.0"],
                evaluate_fn=fake,
                pom_reader=lambda: "<flink.version>2.2.1</flink.version>",
            )
            is None
        )

    def test_no_arguments_is_usage(self, guard):
        assert "usage" in guard.main([], evaluate_fn=None)
