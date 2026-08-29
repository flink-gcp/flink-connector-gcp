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
"""Firing controls for the shared Java syntax boundary."""

import pytest
from java_ast import (
    JavaSource,
    JavaSyntaxError,
    code_named_children,
    declaration_target,
    string_literal_content,
)


@pytest.mark.parametrize(
    "source",
    [
        "class Broken {",
        "class Broken { void method( {} }",
    ],
)
def test_error_and_missing_nodes_fail_closed(source):
    with pytest.raises(
        JavaSyntaxError, match=r"Broken\.java:\d+:\d+.*(?:ERROR|MISSING)"
    ):
        JavaSource.parse("Broken.java", source)


def test_utf8_byte_offsets_translate_to_python_offsets():
    parsed = JavaSource.parse("Unicode.java", "/** café */ class Unicode {}")
    declaration = next(parsed.nodes("class_declaration"))
    assert parsed.start(declaration) == len("/** café */ ")


def test_text_blocks_are_string_literals_with_three_character_delimiters():
    parsed = JavaSource.parse(
        "Text.java", 'class Text { String value = """\n  text\n  """; }'
    )
    literal = next(parsed.nodes("string_literal"))
    assert string_literal_content(parsed, literal) == "\n  text\n  "


def test_code_named_children_exclude_comments():
    parsed = JavaSource.parse(
        "Call.java", 'class Call { void f() { call(/* note */ "value"); } }'
    )
    arguments = next(parsed.nodes("argument_list"))
    assert [child.type for child in code_named_children(arguments)] == [
        "string_literal"
    ]


def test_package_and_import_targets_come_from_identifier_children():
    parsed = JavaSource.parse(
        "Spacing.java",
        "package\tdemo.source;\n"
        "import\n static org.apache.flink.Util.call;\n"
        "import org.apache.flink.*;\n"
        "class X {}",
    )
    package = next(parsed.nodes("package_declaration"))
    imports = list(parsed.nodes("import_declaration"))
    assert declaration_target(parsed, package) == "demo.source"
    assert declaration_target(parsed, imports[0]) == "org.apache.flink.Util.call"
    assert declaration_target(parsed, imports[1]) == "org.apache.flink.*"
