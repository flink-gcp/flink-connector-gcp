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
"""Synthetic coverage for scripts/assert-surefire-boundaries.py."""

import pytest
from conftest import load_script


@pytest.fixture(scope="session")
def boundaries():
    return load_script("assert-surefire-boundaries.py")


def suite(body: str, tests: int = 2, skipped: int = 0) -> str:
    return f'<testsuite tests="{tests}" skipped="{skipped}">{body}</testsuite>'


class TestBoundaryError:
    def test_exact_and_parameterized_names_pass(self, boundaries):
        xml = suite('<testcase name="alpha"/><testcase name="beta(boolean)[1]"/>')
        assert boundaries.boundary_error(xml, ["alpha", "beta"]) is None

    def test_missing_name_fails_by_name(self, boundaries):
        xml = suite('<testcase name="alpha"/>')
        assert "'nope'" in boundaries.boundary_error(xml, ["nope"])

    def test_a_renamed_test_with_a_suffix_does_not_satisfy_its_old_boundary(
        self, boundaries
    ):
        xml = suite('<testcase name="alphaLegacy"/>')
        assert "'alpha'" in boundaries.boundary_error(xml, ["alpha"])

    def test_a_name_is_not_a_regex(self, boundaries):
        # The bash predecessor interpolated names into an ERE, where a.b
        # matched axb.
        xml = suite('<testcase name="axb"/>')
        assert "'a.b'" in boundaries.boundary_error(xml, ["a.b"])

    def test_skipped_tests_fail(self, boundaries):
        xml = suite('<testcase name="alpha"/>', skipped=1)
        assert "skipped" in boundaries.boundary_error(xml, ["alpha"])

    def test_zero_tests_fail(self, boundaries):
        assert "no executed tests" in boundaries.boundary_error(suite("", tests=0), [])

    def test_a_testsuites_wrapper_is_rejected(self, boundaries):
        # The bash predecessor counted grep lines; a one-line multi-suite
        # report mixed one suite's counts with another's names.
        xml = (
            "<testsuites>"
            + suite('<testcase name="other"/>', tests=1)
            + suite('<testcase name="required"/>', tests=1, skipped=1)
            + "</testsuites>"
        )
        assert "<testsuites>" in boundaries.boundary_error(xml, ["required"])

    def test_cdata_faking_a_suite_does_not_reject(self, boundaries):
        # The bash predecessor's occurrence count tripped on diagnostic
        # output that merely mentioned a testsuite element.
        xml = suite(
            '<testcase name="alpha"><system-out>'
            "<![CDATA[diagnostic: <testsuite fake>]]></system-out></testcase>"
        )
        assert boundaries.boundary_error(xml, ["alpha"]) is None

    def test_a_property_name_does_not_satisfy_a_boundary(self, boundaries):
        # Only <testcase> names count; the bash predecessor grepped the
        # whole document.
        xml = suite(
            '<properties><property name="required" value="x"/></properties>'
            '<testcase name="other"/><testcase name="pad"/>'
        )
        assert "'required'" in boundaries.boundary_error(xml, ["required"])

    def test_malformed_xml_is_rejected(self, boundaries):
        assert "not well-formed" in boundaries.boundary_error("<testsuite", ["a"])


class TestMain:
    def test_missing_report_names_the_path(self, boundaries, tmp_path, capsys):
        assert boundaries.main([str(tmp_path / "absent.xml"), "a"]) == 1
        assert "did the suite run" in capsys.readouterr().err

    def test_green_report_exits_zero(self, boundaries, tmp_path, capsys):
        report = tmp_path / "TEST-ok.xml"
        report.write_text(suite('<testcase name="alpha"/>'), encoding="utf-8")
        assert boundaries.main([str(report), "alpha"]) == 0
        assert "all 1 required boundaries" in capsys.readouterr().out

    def test_no_arguments_is_usage(self, boundaries, capsys):
        assert boundaries.main([]) == 2
        assert "usage" in capsys.readouterr().err
