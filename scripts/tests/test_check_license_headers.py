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


def write_java(root, relative, header):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "package example;\n", encoding="utf-8")
    return path


def test_complete_project_and_asf_headers_are_accepted(tmp_path, check_license_headers):
    write_java(
        tmp_path,
        "a/src/main/java/Owned.java",
        check_license_headers.PROJECT_HEADER,
    )
    write_java(
        tmp_path, "b/src/main/java/Copied.java", check_license_headers.ASF_HEADER
    )

    assert check_license_headers.invalid_headers(tmp_path) == []


def test_complete_preserved_third_party_header_is_accepted(
    tmp_path, check_license_headers
):
    preserved = (
        "/*\n * Copyright 2023 Google LLC\n"
        + check_license_headers.APACHE_HEADER_SUFFIX
    )
    write_java(tmp_path, "a/src/main/java/Preserved.java", preserved)

    assert check_license_headers.invalid_headers(tmp_path) == []


def test_an_unrecorded_copyright_holder_is_rejected(tmp_path, check_license_headers):
    # The header is otherwise perfect: same licence body, same shape, only the
    # holder differs. Before the holder was pinned this passed, which is what
    # let a file missed by a holder sweep stay green — and what would let an
    # adapted third-party file ship with no provenance record.
    stranger = (
        "/*\n * Copyright 2026 Somebody Else\n"
        + check_license_headers.APACHE_HEADER_SUFFIX
    )
    path = write_java(tmp_path, "a/src/main/java/Stranger.java", stranger)

    assert check_license_headers.invalid_headers(tmp_path) == [
        path.relative_to(tmp_path)
    ]


def test_a_non_java_file_naming_an_unapproved_holder_is_reported(
    tmp_path, check_license_headers
):
    # The structural check reads Java only, but the holder moved across every
    # file type at once. A markdown page, an OpenTofu file or a POM left behind
    # by a partial sweep would otherwise be invisible.
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "page.md").write_text(
        "<!--\nCopyright 2026 Somebody Else\n-->\n", encoding="utf-8"
    )
    (tmp_path / "main.tf").write_text(
        "# Copyright 2026 The flink-gcp authors\n", encoding="utf-8"
    )

    assert check_license_headers.unapproved_holders(tmp_path) == [
        ((tmp_path / "docs" / "page.md").relative_to(tmp_path), "Somebody Else")
    ]


def test_build_output_and_third_party_licences_are_outside_the_holder_check(
    tmp_path, check_license_headers
):
    # Vendored bundles and the licence texts the uber-jars ship carry other
    # projects' copyright lines by law. Reaching them would make the check
    # unrunnable after a docs build.
    for relative in (
        "docs/public/vendor.js",
        "docs/resources/gen.json",
        "docs/static/api/index.html",
        "module/target/classes/Thing.properties",
        "module/src/main/resources/META-INF/licenses/LICENSE.re2j",
        "node_modules/pkg/index.js",
        ".venv/lib/mod.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("Copyright 2011 Somebody Else\n", encoding="utf-8")

    assert check_license_headers.unapproved_holders(tmp_path) == []


def test_the_founders_name_no_longer_passes(tmp_path, check_license_headers):
    # The holder this project moved away from. Pinned as its own case because it
    # is the one string a partial sweep leaves behind, and a regex that merely
    # accepts "some holder" would not notice it.
    old = (
        "/*\n * Copyright 2026 laughingman7743\n"
        + check_license_headers.APACHE_HEADER_SUFFIX
    )
    path = write_java(tmp_path, "a/src/main/java/Old.java", old)

    assert check_license_headers.invalid_headers(tmp_path) == [
        path.relative_to(tmp_path)
    ]


def test_one_line_and_truncated_headers_are_rejected(tmp_path, check_license_headers):
    one_line = write_java(
        tmp_path,
        "a/src/main/java/OneLine.java",
        "/* Copyright 2026 The flink-gcp authors. Licensed under the Apache License, Version 2.0. */\n",
    )
    truncated = write_java(
        tmp_path,
        "a/src/test/java/Truncated.java",
        check_license_headers.PROJECT_HEADER.split(" * Unless required", 1)[0]
        + " */\n",
    )

    assert check_license_headers.invalid_headers(tmp_path) == [
        one_line.relative_to(tmp_path),
        truncated.relative_to(tmp_path),
    ]


def test_generated_and_non_java_files_are_outside_the_check(
    tmp_path, check_license_headers
):
    write_java(tmp_path, "a/target/generated-sources/Generated.java", "")
    (tmp_path / "notes.md").write_text("no header\n", encoding="utf-8")

    assert check_license_headers.invalid_headers(tmp_path) == []
