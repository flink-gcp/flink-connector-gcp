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
"""Synthetic coverage for scripts/check-declared-api-tiers.py (issue #1567).

Every fixture is a temporary tree, so a repository source gaining or losing an
annotation changes the real check rather than this suite. Each top-level
declaration kind fails without a tier, so a parser change that stops reading
one of them cannot pass silently; the shapes the rule deliberately leaves
alone — a nested type, package-info.java, a test source — are pinned too.
What counts as a tier is java_ast.flink_tier, shared with check-javadoc-links,
so its qualification cases are pinned here through the checker's own verdict.
"""

from pathlib import Path

import pytest

PACKAGE = "package io.github.demo;\n\n"
INTERNAL = "import org.apache.flink.annotation.Internal;\n\n"


def write(
    root: Path,
    text: str,
    name: str = "Demo.java",
    tree: str = "java",
    module: str = "conn",
):
    path = root / module / "src" / "main" / tree / "io" / "github" / "demo" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture()
def root(tmp_path, check_declared_api_tiers, monkeypatch):
    monkeypatch.setattr(check_declared_api_tiers, "ROOT", tmp_path)
    return tmp_path


@pytest.mark.parametrize(
    "tier", ["Public", "PublicEvolving", "Experimental", "Internal"]
)
def test_every_tier_annotation_passes(root, check_declared_api_tiers, tier):
    write(
        root,
        f"{PACKAGE}import org.apache.flink.annotation.{tier};\n\n"
        f"@{tier}\npublic final class Demo {{}}\n",
    )

    assert check_declared_api_tiers.check(root) == (1, [])


@pytest.mark.parametrize(
    "declaration",
    [
        "enum Demo { A }",
        "interface Demo {}",
        "@interface Demo {}",
        "record Demo(int value) {}",
    ],
)
def test_every_annotated_declaration_kind_passes(
    root, check_declared_api_tiers, declaration
):
    write(root, f"{PACKAGE}{INTERNAL}@Internal\n{declaration}\n")

    assert check_declared_api_tiers.check(root) == (1, [])


def test_a_fully_qualified_tier_passes_without_an_import(
    root, check_declared_api_tiers
):
    write(root, f"{PACKAGE}@org.apache.flink.annotation.Internal\nclass Demo {{}}\n")

    assert check_declared_api_tiers.check(root) == (1, [])


def test_a_tier_written_after_a_modifier_passes(root, check_declared_api_tiers):
    write(root, f"{PACKAGE}{INTERNAL}public @Internal final class Demo {{}}\n")

    assert check_declared_api_tiers.check(root) == (1, [])


@pytest.mark.parametrize(
    "declaration, kind",
    [
        ("final class Demo {}", "class"),
        ("enum Demo { A }", "enum"),
        ("interface Demo {}", "interface"),
        ("@interface Demo {}", "annotation type"),
        ("record Demo(int value) {}", "record"),
    ],
)
def test_an_unannotated_top_level_type_fails(
    root, check_declared_api_tiers, declaration, kind
):
    write(root, f"{PACKAGE}/** Javadoc. */\n{declaration}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert problems == [
        (
            f"conn/src/main/java/io/github/demo/Demo.java:4: top-level {kind} 'Demo' "
            "carries no Flink API tier annotation. Add @Internal (import "
            "org.apache.flink.annotation.Internal) unless the type belongs on the "
            "published surface; a public entry point takes @Public, @PublicEvolving or "
            "@Experimental instead (ADR-0124, ADR-0141)."
        )
    ]


def test_another_annotation_is_not_a_tier(root, check_declared_api_tiers):
    write(
        root,
        f"{PACKAGE}import javax.annotation.concurrent.ThreadSafe;\n\n"
        "@ThreadSafe\nfinal class Demo {}\n",
    )

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


@pytest.mark.parametrize(
    "annotation",
    [
        "import org.apache.flink.annotation.VisibleForTesting;\n\n@VisibleForTesting",
        "@org.apache.flink.annotation.VisibleForTesting",
    ],
)
def test_a_flink_annotation_outside_the_tiers_is_not_a_tier(
    root, check_declared_api_tiers, annotation
):
    write(root, f"{PACKAGE}{annotation}\nfinal class Demo {{}}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_a_tier_name_imported_from_another_package_is_not_a_tier(
    root, check_declared_api_tiers
):
    # The simple name alone would pass; the import is what makes it Flink's.
    write(
        root, f"{PACKAGE}import com.example.Internal;\n\n@Internal\nclass Demo {{}}\n"
    )

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_a_tier_name_qualified_by_another_package_is_not_a_tier(
    root, check_declared_api_tiers
):
    write(root, f"{PACKAGE}@com.example.Internal\nclass Demo {{}}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


@pytest.mark.parametrize("package", ["com.example", "com.example.annotation"])
def test_a_foreign_qualified_tier_name_is_not_a_tier_beside_flinks_import(
    root, check_declared_api_tiers, package
):
    # The file imports Flink's Internal, but the declaration names another.
    write(root, f"{PACKAGE}{INTERNAL}@{package}.Internal\nclass Demo {{}}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_a_static_import_does_not_shadow_the_tier_import(
    root, check_declared_api_tiers
):
    write(
        root,
        f"{PACKAGE}import org.apache.flink.annotation.Internal;\n"
        "import static com.example.Constants.Internal;\n\n"
        "@Internal\nclass Demo {}\n",
    )

    assert check_declared_api_tiers.check(root) == (1, [])


def test_a_wildcard_import_does_not_make_a_tier(root, check_declared_api_tiers):
    # AvoidStarImport forbids the shape; the checker fails closed on it.
    write(
        root,
        f"{PACKAGE}import org.apache.flink.annotation.*;\n\n@Internal\nclass Demo {{}}\n",
    )

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_an_unimported_tier_name_is_not_a_tier(root, check_declared_api_tiers):
    # Resolves to a same-package annotation in Java, not to Flink's.
    write(root, f"{PACKAGE}@Internal\nclass Demo {{}}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_a_nested_type_inherits_its_enclosing_tier(root, check_declared_api_tiers):
    write(
        root,
        f"{PACKAGE}{INTERNAL}@Internal\nfinal class Demo {{\n"
        "    static final class Nested {}\n"
        "    enum Kind { A }\n"
        "}\n",
    )

    assert check_declared_api_tiers.check(root) == (1, [])


def test_a_tier_on_a_member_does_not_count_for_its_type(root, check_declared_api_tiers):
    write(
        root,
        f"{PACKAGE}{INTERNAL}final class Demo {{\n"
        "    @Internal\n    static final class Nested {}\n"
        "}\n",
    )

    _, problems = check_declared_api_tiers.check(root)

    assert len(problems) == 1


def test_every_top_level_type_in_a_file_is_checked(root, check_declared_api_tiers):
    write(
        root,
        f"{PACKAGE}{INTERNAL}class First {{}}\n\n@Internal\nfinal class Demo {{}}\n\n"
        "class Second {}\n",
    )

    _, problems = check_declared_api_tiers.check(root)

    assert [problem.split("'")[1] for problem in problems] == ["First", "Second"]


def test_the_report_names_the_line_the_declaration_starts_on(
    root, check_declared_api_tiers
):
    # The modifiers open the declaration, which is where a tier would go.
    write(root, f"{PACKAGE}@Deprecated\nfinal class Demo {{}}\n")

    _, problems = check_declared_api_tiers.check(root)

    assert problems[0].startswith("conn/src/main/java/io/github/demo/Demo.java:3:")


def test_package_info_declares_nothing_to_annotate(root, check_declared_api_tiers):
    write(
        root,
        "/** The demo package. */\npackage io.github.demo;\n",
        name="package-info.java",
    )

    assert check_declared_api_tiers.check(root) == (1, [])


@pytest.mark.parametrize("tree", ["java-flink1", "java-flink2"])
def test_a_flink_version_compat_root_is_read(root, check_declared_api_tiers, tree):
    write(root, f"{PACKAGE}final class Demo {{}}\n", tree=tree)

    count, problems = check_declared_api_tiers.check(root)

    assert count == 1
    assert len(problems) == 1 and problems[0].startswith(f"conn/src/main/{tree}/")


def test_a_tier3_job_module_is_read(root, check_declared_api_tiers):
    write(root, f"{PACKAGE}final class Demo {{}}\n", module="kubernetes/apps/smoke")

    count, problems = check_declared_api_tiers.check(root)

    assert count == 1
    assert len(problems) == 1
    assert problems[0].startswith("kubernetes/apps/smoke/src/main/java/")


def test_a_module_nested_elsewhere_is_not_read(root, check_declared_api_tiers):
    write(root, f"{PACKAGE}final class Demo {{}}\n", module="tools/nested")

    assert check_declared_api_tiers.check(root) == (0, [])


def test_a_test_source_is_not_read(root, check_declared_api_tiers):
    path = root / "conn" / "src" / "test" / "java" / "io" / "github" / "DemoTest.java"
    path.parent.mkdir(parents=True)
    path.write_text("package io.github;\n\nclass DemoTest {}\n", encoding="utf-8")

    assert check_declared_api_tiers.check(root) == (0, [])


def test_main_exits_1_and_names_the_type(root, check_declared_api_tiers, capsys):
    write(root, f"{PACKAGE}final class Demo {{}}\n")

    assert check_declared_api_tiers.main() == 1
    err = capsys.readouterr().err
    assert "'Demo' carries no Flink API tier annotation" in err
    assert "1 top-level main types carry no Flink API tier annotation" in err


def test_main_exits_0_on_a_clean_tree(root, check_declared_api_tiers, capsys):
    write(root, f"{PACKAGE}{INTERNAL}@Internal\nfinal class Demo {{}}\n")

    assert check_declared_api_tiers.main() == 0
    assert "1 main sources" in capsys.readouterr().out


def test_main_exits_2_on_a_syntax_error(root, check_declared_api_tiers, capsys):
    write(root, f"{PACKAGE}final class Demo {{\n")

    assert check_declared_api_tiers.main() == 2
    assert "syntax" in capsys.readouterr().err


def test_main_exits_2_when_no_source_matches(root, check_declared_api_tiers, capsys):
    # A moved source layout must not read as a clean tree.
    assert check_declared_api_tiers.main() == 2
    assert "No main sources match" in capsys.readouterr().err


def test_main_exits_2_on_an_unreadable_source(root, check_declared_api_tiers, capsys):
    # The glob matches a directory named like a source; reading it fails.
    (root / "conn" / "src" / "main" / "java" / "Odd.java").mkdir(parents=True)

    assert check_declared_api_tiers.main() == 2
    assert "Odd.java" in capsys.readouterr().err
