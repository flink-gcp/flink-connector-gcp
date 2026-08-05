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
"""Tests for scripts/check-option-docs.py (issues #89, #249).

Synthetic fixtures only — a module tree and a reference page built in tmp_path,
with ROOT and CONFIG monkeypatched onto it. The real tree's verdict is already
CI's own `Check the configuration reference` job; what has no other cover is
the parsing, and the direction that matters is a checker quietly finding less
than it should, which reads exactly like a clean tree.

Exit codes are the assertion surface: 0 clean, 1 policy violation, 2 an
infrastructure or config-authoring error.
"""

import pytest

SETTER_LINE = "    public {ret} {name}({arg} value) {{ return this; }}"


def options_class(name, *setters, ret="Builder", arg="int"):
    """A `*Options.java` whose nested Builder declares the given setters."""
    body = "\n".join(
        SETTER_LINE.format(ret=ret, name=setter, arg=arg) for setter in setters
    )
    return (
        f"package io.github.x;\n\n"
        f"public class {name} {{\n"
        f"  public static class Builder {{\n{body}\n  }}\n"
        f"}}\n"
    )


def option_page(*rows, header="Option", title="# Reference"):
    """A page carrying one table; `header` is what opts it in (or does not)."""
    lines = [title, "", f"| {header} | Default | Description |", "|---|---|---|"]
    lines += [f"| {row} | none | Sets it. |" for row in rows]
    return "\n".join(lines) + "\n"


def write_source(root, module, name, body):
    path = root / module / "src" / "main" / "java" / "io" / "github" / "x" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return str(path.relative_to(root))


def write_page(root, name, body):
    path = root / "docs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return f"docs/{name}"


def write_config(root, builders=(), config_options=(), exempt=(), extra=()):
    lines = []
    for module, page in builders:
        lines += ["[[builders]]", f'module = "{module}"', f'page = "{page}"', ""]
    for source, page in config_options:
        lines += ["[[config_options]]", f'source = "{source}"', f'page = "{page}"', ""]
    lines += ["[exempt]", *(f'"{key}" = "because."' for key in exempt), ""]
    lines += ["[extra]", *(f'"{key}" = "because."' for key in extra), ""]
    (root / "option-docs.toml").write_text("\n".join(lines))


@pytest.fixture()
def root(tmp_path, check_option_docs, monkeypatch):
    monkeypatch.setattr(check_option_docs, "ROOT", tmp_path)
    monkeypatch.setattr(check_option_docs, "CONFIG", tmp_path / "option-docs.toml")
    return tmp_path


def exit_code(module) -> int:
    try:
        return module.main()
    except SystemExit as error:
        return error.code


# --- the opt-in rule: an option table is one headed exactly `Option` ---


@pytest.mark.parametrize(
    "header",
    [
        "Property",
        "Type",
        # Exactly `Option`, not merely starting with it: the metadata and
        # type-mapping tables the same pages carry must stay outside the check,
        # and the header is the only thing holding them there.
        "Options",
        "Option key",
        "option",
    ],
)
def test_only_option_headed_tables_are_read(root, check_option_docs, header):
    page = root / "page.md"
    page.write_text(option_page("`documented`", header=header))
    assert check_option_docs.option_table_entries(page) == {}


def test_option_headed_table_yields_its_rows_with_line_numbers(root, check_option_docs):
    page = root / "page.md"
    page.write_text(option_page("`first`", "`second`"))
    # Title, blank, header, separator, then the rows.
    assert check_option_docs.option_table_entries(page) == {"first": 5, "second": 6}


def test_one_cell_may_name_several_options(root, check_option_docs):
    page = root / "page.md"
    page.write_text(option_page("`subscription` / `subscriptions`"))
    assert set(check_option_docs.option_table_entries(page)) == {
        "subscription",
        "subscriptions",
    }


def test_argument_lists_distinguish_overloads_without_becoming_names(
    root, check_option_docs
):
    page = root / "page.md"
    page.write_text(option_page("`timePartitioning(type)`"))
    assert set(check_option_docs.option_table_entries(page)) == {"timePartitioning"}


def test_a_non_table_line_ends_the_table(root, check_option_docs):
    page = root / "page.md"
    page.write_text(
        option_page("`inside`")
        + "\n"
        + option_page("`outside`", header="Property", title="## Type mapping")
    )
    assert set(check_option_docs.option_table_entries(page)) == {"inside"}


def test_only_the_first_column_counts(root, check_option_docs):
    page = root / "page.md"
    page.write_text("| Option | Default |\n|---|---|\n| `real` | `notAnOption` |\n")
    assert set(check_option_docs.option_table_entries(page)) == {"real"}


# --- the source side: what counts as a builder setter ---


def test_every_source_glob_and_builder_return_shape_is_found(root, check_option_docs):
    # One class per entry in SOURCE_GLOBS: dropping any of the three would stop
    # a whole surface being checked, silently — a source builder's options are
    # as real as a sink builder's.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "nested"))
    write_source(
        root,
        "conn",
        "ASinkBuilder.java",
        options_class("ASinkBuilder", "sink", ret="ASinkBuilder<T>"),
    )
    write_source(
        root,
        "conn",
        "ASourceBuilder.java",
        options_class("ASourceBuilder", "source", ret="ASourceBuilder<T>"),
    )
    assert check_option_docs.builder_setters("conn", set()) == {
        "AOptions": {"nested"},
        "ASinkBuilder": {"sink"},
        "ASourceBuilder": {"source"},
    }


COMMENTED_OUT_SETTER = """\
package io.github.x;

public class AOptions {
  public static class Builder {
    /* Disabled until the shape settles:
    public Builder ghost(int value) { return this; }
    */

    /** Prefer {@link #real(int)} over the one above. */
    public Builder real(int value) { return this; }
  }
}
"""


def test_a_setter_inside_a_comment_is_not_a_declaration(root, check_option_docs):
    # Indented exactly as a real one, which is what makes it reachable by the
    # line-anchored pattern: only the comment blanking keeps it out.
    write_source(root, "conn", "AOptions.java", COMMENTED_OUT_SETTER)
    assert check_option_docs.builder_setters("conn", set()) == {"AOptions": {"real"}}


def test_an_options_source_with_no_setter_is_an_infrastructure_error(
    root, check_option_docs
):
    write_source(root, "conn", "AOptions.java", "package io.github.x;\nclass A {}\n")
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("conn", set())
    assert error.value.code == 2


def test_a_claimed_source_is_skipped_rather_than_parsed(root, check_option_docs):
    claimed = write_source(
        root, "conn", "ConnectorOptions.java", "package io.github.x;\nclass A {}\n"
    )
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "real"))
    assert check_option_docs.builder_setters("conn", {claimed}) == {
        "AOptions": {"real"}
    }


def test_a_module_without_main_sources_is_an_infrastructure_error(
    root, check_option_docs
):
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("absent", set())
    assert error.value.code == 2


def test_a_mapped_module_that_declares_no_options_is_an_infrastructure_error(
    root, check_option_docs
):
    # Mapped to a page but carrying nothing this script recognises: the mapping
    # is wrong, or the sources moved. Either way it must not read as "nothing
    # to check, so everything is documented".
    write_source(root, "conn", "Sink.java", "package io.github.x;\nclass S {}\n")
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("conn", set())
    assert error.value.code == 2


# --- both directions, end to end ---


def clean_tree(root, setter="topic", row="`topic`", **config):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", setter))
    page = write_page(root, "conn.md", option_page(row))
    write_config(root, builders=[("conn", page)], **config)
    return page


def test_a_documented_option_passes(root, check_option_docs):
    clean_tree(root)
    assert exit_code(check_option_docs) == 0


def test_an_undocumented_setter_fails(root, check_option_docs, capsys):
    clean_tree(root, row="`other`", extra=["other"])
    assert exit_code(check_option_docs) == 1
    assert "AOptions.topic is a builder option" in capsys.readouterr().err


def test_a_row_no_builder_declares_fails(root, check_option_docs, capsys):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    page = write_page(root, "conn.md", option_page("`topic`", "`renamedAway`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "`renamedAway`, which no builder in conn declares" in err
    assert "docs/conn.md:6:" in err


def test_exempt_forgives_a_setter_with_no_row(root, check_option_docs):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "a", "b"))
    page = write_page(root, "conn.md", option_page("`a`"))
    write_config(root, builders=[("conn", page)], exempt=["AOptions.b"])
    assert exit_code(check_option_docs) == 0


def test_extra_forgives_a_row_with_no_setter(root, check_option_docs):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "a"))
    page = write_page(root, "conn.md", option_page("`a`", "`format`"))
    write_config(root, builders=[("conn", page)], extra=["format"])
    assert exit_code(check_option_docs) == 0


@pytest.mark.parametrize(
    ("table", "entry"), [("exempt", "AOptions.unused"), ("extra", "unused")]
)
def test_an_allowlist_entry_that_never_fires_fails(
    root, check_option_docs, capsys, table, entry
):
    # An entry that forgives nothing is a claim nobody can check, and the four
    # this check shipped with were all dead on arrival.
    clean_tree(root, **{table: [entry]})
    assert exit_code(check_option_docs) == 1
    assert f'[{table}] entry "{entry}"' in capsys.readouterr().err


def test_a_module_with_options_and_no_mapping_fails(root, check_option_docs, capsys):
    clean_tree(root)
    write_source(root, "unmapped", "BOptions.java", options_class("BOptions", "knob"))
    assert exit_code(check_option_docs) == 1
    assert "unmapped declares options" in capsys.readouterr().err


def test_a_missing_config_file_is_an_infrastructure_error(root, check_option_docs):
    assert exit_code(check_option_docs) == 2


def test_a_page_the_config_names_but_does_not_exist_is_infrastructure(
    root, check_option_docs
):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_config(root, builders=[("conn", "docs/absent.md")])
    assert exit_code(check_option_docs) == 2


# --- the Table API surface: ConfigOptions rather than builder setters ---


CONFIG_OPTIONS_SOURCE = """\
package io.github.x;

/** Not a key: {{@code ConfigOptions.key("commented.out")}}. */
public class ConnectorOptions {{
  public static final ConfigOption<String> A =
      ConfigOptions.key("{first}").stringType().noDefaultValue();
  public static final ConfigOption<String> B =
      ConfigOptions.key("{second}").stringType().noDefaultValue();
}}
"""


def table_api_tree(root, page_rows, first="topic.id", second="sink.retries"):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    builder_page = write_page(root, "conn.md", option_page("`topic`"))
    source = write_source(
        root,
        "conn",
        "ConnectorOptions.java",
        CONFIG_OPTIONS_SOURCE.format(first=first, second=second),
    )
    table_page = write_page(root, "table.md", option_page(*page_rows))
    return builder_page, source, table_page


def test_config_option_keys_are_checked_against_their_page(root, check_option_docs):
    builder_page, source, table_page = table_api_tree(
        root, ["`topic.id`", "`sink.retries`"]
    )
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(source, table_page)],
    )
    assert exit_code(check_option_docs) == 0


def test_extra_forgives_a_row_the_table_api_source_does_not_declare(
    root, check_option_docs
):
    # The direction the real config depends on: all three [extra] entries
    # (`format`, `scan.parallelism`, `sink.parallelism`) are Flink's own
    # FactoryUtil keys documented on the Table API page, so they fire in this
    # loop and in no other.
    builder_page, source, table_page = table_api_tree(
        root, ["`topic.id`", "`sink.retries`", "`format`"]
    )
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(source, table_page)],
        extra=["format"],
    )
    assert exit_code(check_option_docs) == 0


def test_an_undocumented_config_option_key_fails(root, check_option_docs, capsys):
    builder_page, source, table_page = table_api_tree(root, ["`topic.id`"])
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(source, table_page)],
    )
    assert exit_code(check_option_docs) == 1
    assert "`sink.retries` is a ConfigOption" in capsys.readouterr().err


def test_a_key_the_source_does_not_declare_fails(root, check_option_docs, capsys):
    builder_page, source, table_page = table_api_tree(
        root, ["`topic.id`", "`sink.retries`", "`commented.out`"]
    )
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(source, table_page)],
    )
    assert exit_code(check_option_docs) == 1
    assert "`commented.out`" in capsys.readouterr().err


def test_a_config_options_source_with_no_keys_is_infrastructure(
    root, check_option_docs
):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    builder_page = write_page(root, "conn.md", option_page("`topic`"))
    source = write_source(
        root, "conn", "ConnectorOptions.java", "package io.github.x;\nclass C {}\n"
    )
    table_page = write_page(root, "table.md", option_page("`topic.id`"))
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(source, table_page)],
    )
    assert exit_code(check_option_docs) == 2


# --- hardening shared with check-metric-docs.py (PR #302's self-review) ---


def test_a_fenced_example_table_is_not_read(root, check_option_docs):
    # A snippet showing what an option table looks like must earn no coverage
    # credit: deleting the real table while the example remains has to fail.
    page = root / "page.md"
    page.write_text("# t\n\n```\n" + option_page("`example`") + "```\n")
    assert check_option_docs.option_table_entries(page) == {}


def test_a_comment_marker_inside_a_string_is_not_a_comment(root, check_option_docs):
    # `"http://…"` carries `//`; naive comment blanking would erase the rest of
    # the line, taking any declaration sharing it along.
    blanked = check_option_docs.blank_comments(
        'String u = "http://e"; ConfigOptions.key("real.key") // gone\n'
    )
    assert '"http://e"' in blanked
    assert '"real.key"' in blanked
    assert "// gone" not in blanked


@pytest.mark.parametrize(
    "body",
    [
        # Not TOML at all.
        "[[builders]\n",
        # No [[builders]] mapping.
        '[exempt]\n[extra]\n"x" = "y"\n',
        # An entry missing its page.
        '[[builders]]\nmodule = "conn"\n',
        # A [[config_options]] entry missing its source.
        '[[builders]]\nmodule = "conn"\npage = "docs/conn.md"\n[[config_options]]\npage = "docs/t.md"\n',
    ],
)
def test_a_malformed_config_is_an_infrastructure_error(root, check_option_docs, body):
    # The docstring promises exit 2 for malformed config, not a traceback.
    (root / "option-docs.toml").write_text(body)
    assert exit_code(check_option_docs) == 2
