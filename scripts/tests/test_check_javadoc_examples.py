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
"""Synthetic coverage for scripts/check-javadoc-examples.py.

The checker owns a Java lexical scan as well as the two-way snippet inventory.
These tests use temporary trees so a new repository example changes the real
check, not this suite's fixtures.
"""

from pathlib import Path

import pytest


def write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def source(example: str, marker: str) -> str:
    return f"""package demo;

/**
 * Example.
 * {marker}
 * <pre>{{@code
{example}
 * }}</pre>
 */
final class Api {{}}
"""


def backing(example: str, tag: str = "example") -> str:
    return f"""package demo;

final class Backing {{
    void example() {{
        // tag::{tag}[]
{example}
        // end::{tag}[]
    }}
}}
"""


@pytest.fixture()
def tree(tmp_path, check_javadoc_examples, monkeypatch):
    main = tmp_path / "flink-demo/src/main/java/demo/Api.java"
    snippets = tmp_path / "validation/javadoc"
    monkeypatch.setattr(check_javadoc_examples, "ROOT", tmp_path)
    monkeypatch.setattr(check_javadoc_examples, "JAVADOC_SOURCES", snippets)
    monkeypatch.setattr(
        check_javadoc_examples,
        "MAIN_SOURCE_PATTERN",
        "flink-*/src/main/java*/**/*.java",
    )
    return main, snippets


def audit(check_javadoc_examples):
    runnable, partial, problems, infrastructure = check_javadoc_examples.validate()
    return runnable, partial, problems, infrastructure


def clean_tree(main, snippets):
    example = ' * String value = "ok";'
    write(
        main,
        source(
            example,
            '<!-- javadoc-example file="Backing.java" tag="example" -->',
        ),
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))


def test_a_runnable_and_an_abbreviated_example_pass(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    partial = source(
        " * call(...);",
        "<p><b>Abbreviated, not compiled:</b> the assertion is application-specific.\n"
        ' * <!-- javadoc-example partial="application-specific assertion" -->',
    )
    write(main.with_name("Partial.java"), partial)

    assert audit(check_javadoc_examples) == (1, 1, [], False)


def test_an_unclassified_block_fails(tree, check_javadoc_examples):
    main, snippets = tree
    write(main, source(' * String value = "ok";', ""))
    snippets.mkdir(parents=True)
    write(snippets / "Unused.java", backing('        String unused = "x";'))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("no immediately preceding" in problem for problem in problems)


def test_an_unclassified_block_in_a_flink_compat_source_root_fails(
    tree, check_javadoc_examples
):
    main, snippets = tree
    clean_tree(main, snippets)
    compat = (
        main.parents[2]
        / "java-flink2/io/github/flink/gcp/connector/demo/CrossVersionApi.java"
    )
    write(compat, source(' * String value = "unchecked";', ""))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any(
        "CrossVersionApi.java" in problem and "no immediately preceding" in problem
        for problem in problems
    )


@pytest.mark.parametrize(
    "marker",
    [
        '<!-- javadoc-example file="Backing.java" -->',
        '<!-- javadoc-example tag="example" file="Backing.java" -->',
        '<!-- javadoc-example partial="" -->',
    ],
)
def test_a_malformed_marker_fails(tree, check_javadoc_examples, marker):
    main, snippets = tree
    write(main, source(' * String value = "ok";', marker))
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("malformed javadoc-example marker" in problem for problem in problems)


def test_a_marker_separated_from_its_block_fails(tree, check_javadoc_examples):
    main, snippets = tree
    write(
        main,
        source(
            ' * String value = "ok";',
            '<!-- javadoc-example file="Backing.java" tag="example" -->\n'
            " * <p>Intervening prose.",
        ),
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("not immediately followed" in problem for problem in problems)
    assert any("no immediately preceding" in problem for problem in problems)


def test_a_partial_example_needs_visible_abbreviation_prose(
    tree, check_javadoc_examples
):
    main, snippets = tree
    write(
        main,
        source(
            " * call(...);",
            '<!-- javadoc-example partial="application-specific assertion" -->',
        ),
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("Abbreviated, not compiled" in problem for problem in problems)


def test_displayed_and_compiled_code_must_match(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    write(snippets / "Backing.java", backing("        Integer value = 1;"))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert len(problems) == 1
    assert "displayed code differs" in problems[0]
    assert "-Integer value = 1;" in problems[0]
    assert '+String value = "ok";' in problems[0]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            (
                '// tag::example[]\nString a = "a";\n// tag::example[]\n'
                "// end::example[]"
            ),
            "found 2 start and 1 end",
        ),
        ('// end::example[]\nString a = "a";\n// tag::example[]', "before"),
        ("// tag::example[]\n// end::example[]", "empty"),
    ],
)
def test_invalid_backing_markers_fail(tree, check_javadoc_examples, text, message):
    main, snippets = tree
    write(
        main,
        source(
            ' * String value = "ok";',
            '<!-- javadoc-example file="Backing.java" tag="example" -->',
        ),
    )
    write(snippets / "Backing.java", f"final class Backing {{\n{text}\n}}")

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any(message in problem for problem in problems)


def test_an_unused_backing_region_fails(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    write(snippets / "Unused.java", backing('        String unused = "x";'))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any(
        "is not referenced by any Javadoc block" in problem for problem in problems
    )


def test_a_backing_source_without_regions_fails(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    write(snippets / "Stale.java", "final class Stale {}")

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any(
        "contains no tagged Javadoc example regions" in problem for problem in problems
    )


def test_backing_file_names_must_be_unique(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    write(
        snippets / "nested/Backing.java",
        backing('        String nested = "x";', tag="nested"),
    )

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("duplicates backing file name" in problem for problem in problems)


def test_a_malformed_backing_marker_fails(tree, check_javadoc_examples):
    main, snippets = tree
    write(
        main,
        source(
            ' * String value = "ok";',
            '<!-- javadoc-example file="Backing.java" tag="example" -->',
        ),
    )
    write(
        snippets / "Backing.java",
        "final class Backing {\n"
        "// tag::example[] trailing text\n"
        'String value = "ok";\n'
        "// end::example[]\n"
        "}",
    )

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("malformed source tag" in problem for problem in problems)


def test_a_marker_trailing_code_on_its_line_fails(tree, check_javadoc_examples):
    main, snippets = tree
    write(
        main,
        source(
            ' * String value = "ok";',
            '<!-- javadoc-example file="Backing.java" tag="example" -->',
        ),
    )
    write(
        snippets / "Backing.java",
        "final class Backing {\n"
        'String dropped = "wrong"; // tag::example[]\n'
        'String value = "ok";\n'
        "// end::example[]\n"
        "}",
    )

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("malformed source tag" in problem for problem in problems)


def test_one_backing_region_cannot_serve_two_blocks(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    write(main.with_name("Second.java"), main.read_text(encoding="utf-8"))

    _, _, problems, _ = audit(check_javadoc_examples)

    assert any("referenced by more than one" in problem for problem in problems)


def test_comment_shaped_text_in_java_literals_is_ignored(tree, check_javadoc_examples):
    main, snippets = tree
    clean_tree(main, snippets)
    real = main.read_text(encoding="utf-8")
    decoys = '''
final class Decoys {
    String ordinary = "/** <pre>{@code unchecked(); }</pre> */";
    String text = """
            /** <pre>{@code unchecked(); }</pre> */
            // tag::decoy[]
            """;
    // /** <pre>{@code unchecked(); }</pre> */
    /* /** <pre>{@code unchecked(); }</pre> */
}
'''
    write(main, decoys + real)
    backing_source = (snippets / "Backing.java").read_text(encoding="utf-8")
    write(
        snippets / "Backing.java",
        backing_source.replace(
            "final class Backing {",
            'final class Backing {\n    String decoy = """\n// tag::decoy[]\n""";',
        ),
    )

    assert audit(check_javadoc_examples) == (1, 0, [], False)


def test_missing_inputs_are_infrastructure_failures(tree, check_javadoc_examples):
    _, snippets = tree

    assert audit(check_javadoc_examples)[3] is True

    snippets.mkdir(parents=True)

    assert audit(check_javadoc_examples)[3] is True
