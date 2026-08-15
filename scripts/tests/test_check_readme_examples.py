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
"""Synthetic coverage for scripts/check-readme-examples.py."""

from pathlib import Path

import pytest


def write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def backing(example: str, tag: str = "example") -> str:
    return f"""final class Backing {{
    void example() {{
        // tag::{tag}[]
{example}
        // end::{tag}[]
    }}
}}
"""


def runnable(example: str, marker: str | None = None, fence: str = "```") -> str:
    marker = marker or '<!-- readme-example file="Backing.java" tag="example" -->'
    return f"""# Demo

{marker}
{fence}java
{example}
{fence}
"""


@pytest.fixture()
def tree(tmp_path, check_readme_examples, monkeypatch):
    readme = tmp_path / "flink-demo/README.md"
    snippets = tmp_path / "validation"
    monkeypatch.setattr(check_readme_examples, "ROOT", tmp_path)
    monkeypatch.setattr(check_readme_examples, "SNIPPET_SOURCES", snippets)
    monkeypatch.setattr(check_readme_examples, "README_PATTERN", "flink-*/README.md")
    return readme, snippets


def audit(check_readme_examples):
    return check_readme_examples.validate()


def clean_tree(readme, snippets):
    write(readme, runnable('String value = "ok";'))
    write(snippets / "Backing.java", backing('        String value = "ok";'))


def test_runnable_and_abbreviated_examples_pass(tree, check_readme_examples):
    readme, snippets = tree
    clean_tree(readme, snippets)
    second = readme.parent.parent / "flink-partial/README.md"
    write(
        second,
        """Abbreviated, not compiled: application setup is omitted.

<!-- readme-example partial="application setup" -->
```java
call(...);
```
""",
    )

    assert audit(check_readme_examples) == (1, 1, [], False)


def test_unclassified_java_fence_fails(tree, check_readme_examples):
    readme, snippets = tree
    write(readme, "# Demo\n\n```java\ncall();\n```\n")
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("no immediately preceding" in problem for problem in problems)


@pytest.mark.parametrize(
    "marker",
    [
        '<!-- readme-example file="Backing.java" -->',
        '<!-- readme-example tag="example" file="Backing.java" -->',
        '<!-- readme-example partial="" -->',
    ],
)
def test_malformed_markers_fail(tree, check_readme_examples, marker):
    readme, snippets = tree
    write(readme, runnable('String value = "ok";', marker=marker))
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("malformed readme-example marker" in problem for problem in problems)


def test_marker_must_be_immediately_before_fence(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """<!-- readme-example file="Backing.java" tag="example" -->

```java
String value = "ok";
```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    _, _, problems, _ = audit(check_readme_examples)

    assert any("not immediately followed" in problem for problem in problems)
    assert any("no immediately preceding" in problem for problem in problems)


def test_partial_example_needs_visible_explanation(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """<!-- readme-example partial="application setup" -->
```java
call(...);
```
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("Abbreviated, not compiled" in problem for problem in problems)


@pytest.mark.parametrize(
    "introduction",
    [
        "<!-- Abbreviated, not compiled: application setup is omitted. -->",
        "<!-- hidden context\n\nAbbreviated, not compiled: application setup is omitted. -->",
        "<!-- hidden context\n\nAbbreviated, not compiled: application setup is omitted.",
        "Abbreviated, not compiled:",
    ],
)
def test_partial_explanation_must_be_visible_and_nonempty(
    tree, check_readme_examples, introduction
):
    readme, snippets = tree
    write(
        readme,
        f"""{introduction}

<!-- readme-example partial="application setup" -->
```java
call(...);
```
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("visible README prose" in problem for problem in problems)


def test_fenced_html_comment_text_does_not_hide_partial_explanation(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """```text
<!-- literal example
```

Abbreviated, not compiled: application setup is omitted.

<!-- readme-example partial="application setup" -->
```java
call(...);
```
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 1, [], False)


def test_displayed_and_compiled_code_must_match(tree, check_readme_examples):
    readme, snippets = tree
    clean_tree(readme, snippets)
    write(snippets / "Backing.java", backing("        Integer value = 1;"))

    _, _, problems, _ = audit(check_readme_examples)

    assert len(problems) == 1
    assert "flink-demo/README.md:4" in problems[0]
    assert "Backing.java" in problems[0]
    assert "tag `example`" in problems[0]
    assert "displayed code differs" in problems[0]
    assert "-Integer value = 1;" in problems[0]
    assert '+String value = "ok";' in problems[0]


def test_boundary_blank_lines_must_match(tree, check_readme_examples):
    readme, snippets = tree
    clean_tree(readme, snippets)
    write(snippets / "Backing.java", backing('\n        String value = "ok";'))

    _, _, problems, _ = audit(check_readme_examples)

    assert len(problems) == 1
    assert "displayed code differs" in problems[0]


def test_top_level_content_indentation_must_match(tree, check_readme_examples):
    readme, snippets = tree
    write(readme, runnable('    String value = "ok";'))
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    _, _, problems, _ = audit(check_readme_examples)

    assert len(problems) == 1
    assert "displayed code differs" in problems[0]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            '// tag::example[]\nString a = "a";\n// tag::example[]\n// end::example[]',
            "found 2 start and 1 end",
        ),
        ('// end::example[]\nString a = "a";\n// tag::example[]', "before"),
        ("// tag::example[]\n// end::example[]", "empty"),
    ],
)
def test_invalid_backing_markers_fail(tree, check_readme_examples, text, message):
    readme, snippets = tree
    write(readme, runnable('String value = "ok";'))
    write(snippets / "Backing.java", f"final class Backing {{\n{text}\n}}")

    _, _, problems, _ = audit(check_readme_examples)

    assert any(message in problem for problem in problems)


def test_missing_backing_region_fails(tree, check_readme_examples):
    readme, snippets = tree
    write(readme, runnable('String value = "ok";'))
    write(snippets / "Backing.java", backing('        String value = "ok";', "other"))

    _, _, problems, _ = audit(check_readme_examples)

    assert any("no valid backing region" in problem for problem in problems)


def test_backing_file_names_must_be_unique(tree, check_readme_examples):
    readme, snippets = tree
    clean_tree(readme, snippets)
    write(
        snippets / "nested/Backing.java",
        backing('        String other = "x";', "other"),
    )

    _, _, problems, _ = audit(check_readme_examples)

    assert any("duplicates backing file name" in problem for problem in problems)


def test_one_region_cannot_serve_two_readme_blocks(tree, check_readme_examples):
    readme, snippets = tree
    clean_tree(readme, snippets)
    second = readme.parent.parent / "flink-second/README.md"
    write(second, readme.read_text(encoding="utf-8"))

    _, _, problems, _ = audit(check_readme_examples)

    assert any("referenced by more than one README" in problem for problem in problems)


@pytest.mark.parametrize(
    ("opening", "closing", "indentation"),
    [
        ("~~~~java", "~~~~", ""),
        ("````java", "````", ""),
        ("  ```JAVA linenos", "  ```", "  "),
    ],
)
def test_commonmark_fence_variants_are_checked(
    tree, check_readme_examples, opening, closing, indentation
):
    readme, snippets = tree
    write(
        readme,
        f"""<!-- readme-example file="Backing.java" tag="example" -->
{opening}
{indentation}String value = "ok";
{closing}
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_fence_shaped_text_inside_non_java_fence_is_ignored(
    tree, check_readme_examples
):
    readme, snippets = tree
    clean_tree(readme, snippets)
    write(
        readme,
        readme.read_text(encoding="utf-8")
        + "\n````text\n"
        + '<!-- readme-example file="Shown.java" tag="shown" -->\n'
        + "```java\nunchecked();\n```\n````\n",
    )

    assert audit(check_readme_examples) == (1, 0, [], False)


@pytest.mark.parametrize(
    "text",
    [
        "> ````text\n> ```java\n> unchecked();\n> ```\n> ````\n",
        "- ````text\n  ```java\n  unchecked();\n  ```\n  ````\n",
        "- ````text\n\n  ```java\n  unchecked();\n  ```\n  ````\n",
        (
            "*   item\n\n    ````text\n    ```java\n    unchecked();\n"
            "    ```\n    ````\n"
        ),
        ("- item\n\n    ````text\n  ```java\n  unchecked();\n  ```\n    ````\n"),
        ("> - ````text\n>\n>   ```java\n>   unchecked();\n>   ```\n>   ````\n"),
        ("- > ````text\n  > ```java\n  > unchecked();\n  > ```\n  > ````\n"),
        ("> - > ````text\n>   > ```java\n>   > unchecked();\n>   > ```\n>   > ````\n"),
    ],
)
def test_java_fence_text_inside_non_java_container_fence_is_ignored(
    tree, check_readme_examples, text
):
    readme, snippets = tree
    write(readme, text)
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_sibling_container_fence_does_not_close_quote_fence(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """> ````text
> content
- ````
  ```java
  literal();
  ```
  ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_sibling_list_item_starts_a_new_container_fence(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """- ````text
  literal
- ````
  ```java
  literal();
  ```
  ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_list_container_closer_may_use_extra_indentation(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """- ````text
    literal
    ````
<!-- readme-example file="Backing.java" tag="example" -->
```java
String value = "ok";
```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_unclosed_quote_fence_ends_with_its_container(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """> ````text
> content
<!-- readme-example file="Backing.java" tag="example" -->
```java
String value = "ok";
```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_compound_quote_list_fence_ends_with_the_list(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """> - ````text
>   literal
>
> ```java
> unchecked();
> ````
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_implicit_list_continuation_inside_quote_ends_with_the_list(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """> - item
>   ````text
>   literal
> ```java
> unchecked();
> ````
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_implicit_quote_inside_list_ends_with_the_list(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """- item
  > ````text
  > literal
> ```java
> unchecked();
> ````
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_provisional_scan_does_not_extend_list_fence_past_its_boundary(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """- item

  ````text
  literal
outside
```java
unchecked();
````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("no immediately preceding" in problem for problem in problems)


def test_nonblank_quote_content_outdent_ends_the_list_fence(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """> - ````text
>   literal
> outside
> ```java
> unchecked();
> ````
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_quote_level_fence_after_list_fence_shields_its_content(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """> - ````text
>   literal
> ````
> ```java
> literal();
> ```
> ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_sibling_list_inside_quote_starts_a_new_container_fence(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """> - ````text
>   literal
> - ````
>   ```java
>   literal();
>   ```
>   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_backtick_in_info_does_not_open_a_fence(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """```text`invalid
<!-- readme-example file="Backing.java" tag="example" -->
```java
String value = "ok";
```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


@pytest.mark.parametrize(
    "text",
    [
        "paragraph\n2. ```java\nliteral();\n2. ````\n",
        "# Heading\nparagraph\n2. ```java\nliteral();\n2. ````\n",
        "-     ```java\n      literal();\n      ```\n",
        "    - ```java\n      literal();\n      ```\n",
    ],
)
def test_fence_shaped_text_in_non_container_code_is_ignored(
    tree, check_readme_examples, text
):
    readme, snippets = tree
    write(readme, text)
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_zero_padded_one_ordered_item_may_interrupt_a_paragraph(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """paragraph
01. ```java
    literal();
    ````
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_one_ordered_item_interrupts_list_shaped_paragraph_text(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """paragraph
2. still paragraph
1. ```java
   literal();
   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_ordered_sibling_follows_list_continuation_text(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """1. first
   continuation
2. ```java
   literal();
   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


@pytest.mark.parametrize(
    "text",
    [
        "2. first\n   continuation\n3. ```java\n   literal();\n   ````\n",
        ("1. first\n   # Heading\n   paragraph\n2. ```java\n   literal();\n   ````\n"),
    ],
)
def test_ordered_sibling_follows_established_list_content(
    tree, check_readme_examples, text
):
    readme, snippets = tree
    write(readme, text)
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_outdented_paragraph_ends_established_list_before_ordered_marker(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """1. first
   # Heading
outside
2. ```java
literal();
2. ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_indented_heading_separates_invalid_ordered_marker_from_current_item(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """paragraph
2. still paragraph
   # Heading
3. ```java
   literal();
   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_lazy_continuation_after_indented_paragraph_stays_in_list(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """1. first
   # Heading
   paragraph in list
lazy continuation
2. ```java
   literal();
   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_indented_continuation_does_not_reopen_ended_list(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """1. first
   # Heading
outside paragraph
   continued
2. ```java
literal();
2. ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


def test_older_list_paragraph_does_not_override_nearest_block_boundary(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """1. first
   # H1
   paragraph in list
   # H2
outside
2. ```java
literal();
2. ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    assert audit(check_readme_examples) == (0, 0, [], False)


@pytest.mark.parametrize(
    "opening",
    [
        "> ```java",
        "- ```java",
        "> - ```java",
        "- > ```java",
        ">  - ```java",
        ">   > ```java",
        ">\t```java",
    ],
)
def test_nested_java_fence_is_rejected(tree, check_readme_examples, opening):
    readme, snippets = tree
    write(readme, f"{opening}\nunchecked();\n```\n")
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


@pytest.mark.parametrize(
    "text",
    [
        "*   item\n\n    ```java\n    unchecked();\n    ```\n",
        "- item\n\n  ```java\n  unchecked();\n  ```\n",
        "1. item\n\n   ```java\n   unchecked();\n   ```\n",
        "- item\n\n\t```java\n\tunchecked();\n\t```\n",
        "* item\n\n    > ```java\n    > unchecked();\n    > ```\n",
        "-\n  ```java\n  unchecked();\n  ```\n",
        "- item\nlazy continuation\n\n  ```java\n  unchecked();\n  ```\n",
        "- outer\n  1. inner\n\n  ```java\n  unchecked();\n  ```\n",
        "- outer\n  # Heading\n\n    ```java\n    unchecked();\n    ```\n",
        "- item\n```text`invalid\n\n  ```java\n  unchecked();\n  ```\n",
        (
            "- first paragraph\n\n  second paragraph\nlazy continuation\n\n"
            "  ```java\n  unchecked();\n  ```\n"
        ),
        (
            "- first paragraph\n\n  second paragraph\nlazy continuation\n\n"
            "   ```java\n   unchecked();\n   ```\n"
        ),
    ],
)
def test_list_continuation_java_fence_is_rejected(tree, check_readme_examples, text):
    readme, snippets = tree
    write(readme, text)
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_five_space_list_padding_uses_one_space_fallback(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """-     code

  Abbreviated, not compiled: illustrative only.
  <!-- readme-example partial="illustrative" -->
  ```java
  literal();
  ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_thematic_break_does_not_create_a_list_continuation(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """- - -

  <!-- readme-example file="Backing.java" tag="example" -->
  ```java
  String value = "ok";
  ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


@pytest.mark.parametrize(
    "boundary",
    [
        "***",
        "# Heading",
        '<!-- readme-example file="Backing.java" tag="example" -->',
        "<div>outside</div>",
        "<script></script>",
    ],
)
def test_block_boundary_ends_lazy_list_continuation(
    tree, check_readme_examples, boundary
):
    readme, snippets = tree
    marker = '<!-- readme-example file="Backing.java" tag="example" -->'
    prefix = f"- item\n{boundary}\n"
    if boundary != marker:
        prefix += f"\n  {marker}\n"
    write(
        readme,
        f"""{prefix}  ```java
  String value = "ok";
  ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_top_level_indented_paragraph_does_not_continue_an_old_list(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """- first

outside paragraph
  continued line
lazy continuation
  <!-- readme-example file="Backing.java" tag="example" -->
  ```java
  String value = "ok";
  ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_one_space_top_level_paragraph_does_not_continue_an_old_list(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """- first

 outside paragraph
  <!-- readme-example file="Backing.java" tag="example" -->
  ```java
  String value = "ok";
  ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_ordered_list_content_indentation_bounds_ownership(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """1. first

  outside paragraph
   <!-- readme-example file="Backing.java" tag="example" -->
   ```java
   String value = "ok";
   ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_non_interrupting_ordered_marker_does_not_create_list_continuation(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """paragraph
2. item

   <!-- readme-example file="Backing.java" tag="example" -->
   ```java
   String value = "ok";
   ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


@pytest.mark.parametrize("list_like_lines", ["2. first\n3. second", "- "])
def test_non_interrupting_list_like_lines_do_not_create_list_continuation(
    tree, check_readme_examples, list_like_lines
):
    readme, snippets = tree
    write(
        readme,
        f"""paragraph
{list_like_lines}

   <!-- readme-example file="Backing.java" tag="example" -->
   ```java
   String value = "ok";
   ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_long_non_interrupting_ordered_marker_run_is_bounded(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        "paragraph\n"
        + "2. item\n" * 1100
        + """
   <!-- readme-example file="Backing.java" tag="example" -->
   ```java
   String value = "ok";
   ```
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    assert audit(check_readme_examples) == (1, 0, [], False)


def test_long_established_list_with_indented_blocks_is_bounded(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        "2. first\n"
        + "   # Heading\n3. item\n" * 1100
        + """   # Heading
4. ```java
   literal();
   ````
""",
    )
    write(snippets / "Unused.java", "final class Unused {}\n")

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_ordered_marker_after_heading_creates_list_continuation(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """# Heading
2. item

   ```java
   unchecked();
   ```
""",
    )
    snippets.mkdir(parents=True)

    _, _, problems, _ = audit(check_readme_examples)

    assert any("nested Java code fence" in problem for problem in problems)


def test_unclosed_java_fence_fails(tree, check_readme_examples):
    readme, snippets = tree
    write(
        readme,
        """<!-- readme-example file="Backing.java" tag="example" -->
```java
String value = "ok";
""",
    )
    write(snippets / "Backing.java", backing('        String value = "ok";'))

    _, _, problems, _ = audit(check_readme_examples)

    assert any("not closed" in problem for problem in problems)


def test_missing_readmes_are_infrastructure_failures(tree, check_readme_examples):
    _, snippets = tree

    assert audit(check_readme_examples)[3] is True

    snippets.mkdir(parents=True)

    assert audit(check_readme_examples)[3] is True


def test_missing_or_empty_snippet_sources_are_infrastructure_failures(
    tree, check_readme_examples
):
    readme, snippets = tree
    write(
        readme,
        """Abbreviated, not compiled: application setup is omitted.

<!-- readme-example partial="application setup" -->
```java
call(...);
```
""",
    )

    assert audit(check_readme_examples)[3] is True

    snippets.mkdir(parents=True)

    assert audit(check_readme_examples)[3] is True
