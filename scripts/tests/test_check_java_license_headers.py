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


def write_java(root, relative, header):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "package example;\n", encoding="utf-8")
    return path


def test_complete_project_and_asf_headers_are_accepted(
    tmp_path, check_java_license_headers
):
    write_java(
        tmp_path,
        "a/src/main/java/Owned.java",
        check_java_license_headers.PROJECT_HEADER,
    )
    write_java(
        tmp_path, "b/src/main/java/Copied.java", check_java_license_headers.ASF_HEADER
    )

    assert check_java_license_headers.invalid_headers(tmp_path) == []


def test_complete_preserved_third_party_header_is_accepted(
    tmp_path, check_java_license_headers
):
    preserved = (
        "/*\n * Copyright 2023 Google LLC\n"
        + check_java_license_headers.APACHE_HEADER_SUFFIX
    )
    write_java(tmp_path, "a/src/main/java/Preserved.java", preserved)

    assert check_java_license_headers.invalid_headers(tmp_path) == []


def test_one_line_and_truncated_headers_are_rejected(
    tmp_path, check_java_license_headers
):
    one_line = write_java(
        tmp_path,
        "a/src/main/java/OneLine.java",
        "/* Copyright 2026 laughingman7743. Licensed under the Apache License, Version 2.0. */\n",
    )
    truncated = write_java(
        tmp_path,
        "a/src/test/java/Truncated.java",
        check_java_license_headers.PROJECT_HEADER.split(" * Unless required", 1)[0]
        + " */\n",
    )

    assert check_java_license_headers.invalid_headers(tmp_path) == [
        one_line.relative_to(tmp_path),
        truncated.relative_to(tmp_path),
    ]


def test_generated_and_non_java_files_are_outside_the_check(
    tmp_path, check_java_license_headers
):
    write_java(tmp_path, "a/target/generated-sources/Generated.java", "")
    (tmp_path / "notes.md").write_text("no header\n", encoding="utf-8")

    assert check_java_license_headers.invalid_headers(tmp_path) == []
