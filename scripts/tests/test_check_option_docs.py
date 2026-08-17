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


def options_class(name, *setters, ret="Builder", arg="int", annotation=None):
    """A `*Options.java` whose nested Builder declares the given setters.

    `annotation` is written above the top-level type, which is what the
    unmapped-builder guard reads. Absent by default, since the guard treats an
    unannotated class as needing a decision rather than as exempt.
    """
    body = "\n".join(
        SETTER_LINE.format(ret=ret, name=setter, arg=arg) for setter in setters
    )
    above = f"{annotation}\n" if annotation else ""
    return (
        f"package io.github.x;\n\n"
        f"{above}"
        f"public class {name} {{\n"
        f"  public static class Builder {{\n{body}\n  }}\n"
        f"}}\n"
    )


def option_page(*rows, header="Option", title="# Reference"):
    """A page carrying one table; `header` is what opts it in (or does not)."""
    lines = [title, "", f"| {header} | Default | Description |", "|---|---|---|"]
    lines += [f"| {row} | none | Sets it. |" for row in rows]
    return "\n".join(lines) + "\n"


def write_source(root, module, name, body, pkg="x", tree="java"):
    path = root / module / "src" / "main" / tree / "io" / "github" / pkg / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return str(path.relative_to(root))


def write_page(root, name, body):
    path = root / "docs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return f"docs/{name}"


def write_config(
    root, builders=(), config_options=(), exempt=(), extra=(), value_builders=()
):
    """The config. A `[[builders]]` entry is `(module, page)`, or `(module, page,
    sources)` when it also names classes the globs cannot see."""
    lines = []
    for module, page, *rest in builders:
        lines += ["[[builders]]", f'module = "{module}"', f'page = "{page}"']
        for sources in rest:
            lines += ["sources = [", *(f'  "{s}",' for s in sources), "]"]
        lines += [""]
    for source, page in config_options:
        lines += ["[[config_options]]", f'source = "{source}"', f'page = "{page}"', ""]
    lines += ["[exempt]", *(f'"{key}" = "because."' for key in exempt), ""]
    lines += ["[extra]", *(f'"{key}" = "because."' for key in extra), ""]
    lines += [
        "[value_builders]",
        *(f'"{key}" = "because."' for key in value_builders),
        "",
    ]
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
    write_source(
        root,
        "conn",
        "GenericOptions.java",
        """package io.github.x;

public class GenericOptions<T> {
  public static class Builder<T> {
    public Builder<T> provider(Provider<? super T> value) { return this; }
  }
}
""",
    )
    assert check_option_docs.builder_setters("conn", set(), []) == {
        "AOptions": {"nested"},
        "ASinkBuilder": {"sink"},
        "ASourceBuilder": {"source"},
        "GenericOptions": {"provider"},
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
    assert check_option_docs.builder_setters("conn", set(), []) == {
        "AOptions": {"real"}
    }


def test_an_options_source_with_no_setter_is_an_infrastructure_error(
    root, check_option_docs
):
    write_source(root, "conn", "AOptions.java", "package io.github.x;\nclass A {}\n")
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("conn", set(), [])
    assert error.value.code == 2


def test_a_claimed_source_is_skipped_rather_than_parsed(root, check_option_docs):
    claimed = write_source(
        root, "conn", "ConnectorOptions.java", "package io.github.x;\nclass A {}\n"
    )
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "real"))
    assert check_option_docs.builder_setters("conn", {claimed}, []) == {
        "AOptions": {"real"}
    }


def test_a_module_without_main_sources_is_an_infrastructure_error(
    root, check_option_docs, capsys
):
    # The message is asserted because the two guards are otherwise one branch:
    # a module with no source root also has no options, so deleting this one
    # leaves the run exiting 2 by a message pointing at the sources rather than
    # at the config line that names a module which is not there.
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("absent", set(), [])
    assert error.value.code == 2
    assert "has no java* source root" in capsys.readouterr().err


def test_a_mapped_module_that_declares_no_options_is_an_infrastructure_error(
    root, check_option_docs, capsys
):
    # Mapped to a page but carrying nothing this script recognises: the mapping
    # is wrong, or the sources moved. Either way it must not read as "nothing
    # to check, so everything is documented".
    write_source(root, "conn", "Sink.java", "package io.github.x;\nclass S {}\n")
    with pytest.raises(SystemExit) as error:
        check_option_docs.builder_setters("conn", set(), [])
    assert error.value.code == 2
    assert "No options sources found" in capsys.readouterr().err


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


# --- a builder the globs cannot see: `sources`, and the guard behind it (#328) ---
#
# `PubSubDeadLetterQueue` is named for what it is rather than for the options it
# takes, so from the day it landed neither direction of this check read it.
# Every case below is aimed at that: a mechanism that reaches a class only
# sometimes reads exactly like a clean tree.


def dlq_tree(root, *rows, setters=("topic", "flushTimeout"), mapped=True, **config):
    """A module whose options live in a class no SOURCE_GLOBS pattern matches."""
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    queue = write_source(root, "conn", "Queue.java", options_class("Queue", *setters))
    page = write_page(root, "conn.md", option_page("`topic`", *rows))
    write_config(root, builders=[("conn", page, [queue] if mapped else [])], **config)
    return queue


def test_a_sources_entry_is_checked_for_coverage(root, check_option_docs, capsys):
    dlq_tree(root)
    assert exit_code(check_option_docs) == 1
    assert "Queue.flushTimeout is a builder option" in capsys.readouterr().err


def test_a_sources_entry_is_checked_for_staleness(root, check_option_docs, capsys):
    dlq_tree(root, "`flushTimeout`", "`renamedAway`")
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "`renamedAway`, which no builder in conn declares" in err
    # `renamedAway` is stale under every configuration, so without this the test
    # would pass with the `sources` mechanism disabled entirely — its setters
    # would simply be missing from `real` and `flushTimeout` would be stale too.
    assert "flushTimeout" not in err


def test_a_mapped_source_satisfies_both_directions(root, check_option_docs):
    dlq_tree(root, "`flushTimeout`")
    assert exit_code(check_option_docs) == 0


def test_the_same_tree_unmapped_fails(root, check_option_docs, capsys):
    # The discriminator for the three above: without the entry the rows are the
    # same and the class is simply unread, which is the state #328 was filed on.
    dlq_tree(root, "`flushTimeout`", mapped=False)
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "Queue.java declares public builder setters" in err
    assert "flushTimeout, topic" in err


def test_an_internal_builder_is_not_reported(root, check_option_docs):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Sink.java",
        options_class("Sink", "physicalDataType", annotation="@Internal"),
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 0


@pytest.mark.parametrize(
    "elsewhere",
    [
        # Nested inside the class it would be speaking for. Indentation alone
        # keeps this one out, since INTERNAL is anchored at a line start.
        "  @Internal\n  public static class Nested {}\n",
        # A second top-level type, which Java allows as long as it is not public
        # — at column 0, where only the *scoping* to the first type's own block
        # keeps it from being read as that type's annotation.
        "}\n\n@Internal\nfinal class Helper {\n",
    ],
)
def test_an_internal_elsewhere_in_the_file_does_not_exempt(
    root, check_option_docs, capsys, elsewhere
):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Queue.java",
        "package io.github.x;\n\n"
        "@Experimental\n"
        "public class Queue {\n"
        "  public static class Builder {\n"
        "    public Builder knob(int value) { return this; }\n"
        "  }\n"
        f"{elsewhere}"
        "}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 1
    assert (
        "Queue.java declares public builder setters (knob)" in capsys.readouterr().err
    )


def test_a_value_builder_entry_exempts_an_unmapped_public_builder(
    root, check_option_docs
):
    # The third answer to the unmapped-public-builder guard. `Record` is public
    # on purpose — it is what a deserializer receives — and its builder sets
    # record fields, so neither "write a reference row" nor "@Internal" is
    # true of it.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    record = write_source(
        root,
        "conn",
        "Record.java",
        "package io.github.x;\n\n"
        "@Public\n"
        "public final class Record {\n"
        "  public static final class Builder {\n"
        "    public Builder field(int value) { return this; }\n"
        "  }\n"
        "}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)], value_builders=[record])
    assert exit_code(check_option_docs) == 0


def test_a_value_builder_entry_that_never_fires_is_reported(
    root, check_option_docs, capsys
):
    # The same rule the other two allowlists carry: an entry that forgives
    # nothing is a claim nobody can check. Here the class is @Internal, so the
    # guard would have passed it anyway.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    record = write_source(
        root,
        "conn",
        "Record.java",
        "package io.github.x;\n\n"
        "@Internal\n"
        "public final class Record {\n"
        "  public static final class Builder {\n"
        "    public Builder field(int value) { return this; }\n"
        "  }\n"
        "}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)], value_builders=[record])
    assert exit_code(check_option_docs) == 1
    assert "never fires" in capsys.readouterr().err


def test_the_guard_names_the_value_builder_answer(root, check_option_docs, capsys):
    # The message has to name all three answers, or the next person meeting it
    # reaches for @Internal on a published type — which is what this entry
    # exists to avoid.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Record.java",
        "package io.github.x;\n\n"
        "@Public\n"
        "public final class Record {\n"
        "  public static final class Builder {\n"
        "    public Builder field(int value) { return this; }\n"
        "  }\n"
        "}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "[value_builders]" in err
    assert "@Internal" in err


def test_two_sources_entries_sharing_a_class_name_are_merged(
    root, check_option_docs, capsys
):
    # Same shape as the two-compat-roots case, reached the other way: two
    # packages may hold a `Queue.java`, and the key is the class name because
    # `[exempt]` is `Class.setter`. Assigning would drop the first one's setters
    # out of `real`, so its row would read as stale.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    first = write_source(root, "conn", "Queue.java", options_class("Queue", "one"))
    second = write_source(
        root, "conn", "Queue.java", options_class("Queue", "two"), pkg="y"
    )
    page = write_page(root, "conn.md", option_page("`topic`", "`one`", "`two`"))
    write_config(root, builders=[("conn", page, [first, second])])
    assert exit_code(check_option_docs) == 0


def test_an_unmapped_builder_in_an_unmapped_module_is_reported(
    root, check_option_docs, capsys
):
    clean_tree(root)
    write_source(root, "other", "Queue.java", options_class("Queue", "knob"))
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "other/src/main/java/io/github/x/Queue.java declares public builder" in err
    # The stray-module guard only sees SOURCE_GLOBS matches, so it says nothing
    # here — which is the hole this one covers.
    assert "other declares options" not in err


def test_an_unmapped_module_s_glob_match_is_reported_once(
    root, check_option_docs, capsys
):
    # The other way round: the stray-module guard already owns this file, and a
    # second problem saying it "matches no SOURCE_GLOBS pattern" would be false
    # as well as noisy — and its remedy, `@Internal`, would silence the wrong
    # guard. This is the path the next connector walks in on.
    clean_tree(root)
    write_source(root, "other", "BOptions.java", options_class("BOptions", "knob"))
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "other declares options" in err
    assert "declares public builder setters" not in err


@pytest.mark.parametrize(
    "annotation",
    [
        "@Internal",
        "@org.apache.flink.annotation.Internal",
        # An annotation carrying arguments must not end the block early, or the
        # `@Internal` above it drops out of what is searched.
        '@Internal\n@SuppressWarnings("unchecked")',
    ],
)
def test_a_package_private_internal_class_is_exempt(
    root, check_option_docs, annotation
):
    # 22 of this repository's `@Internal` main sources are package-private, so a
    # guard that only reads a `public` type would report them by a message
    # telling you to add the annotation they already carry.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Helper.java",
        f"package io.github.x;\n\n{annotation}\nfinal class Helper {{\n"
        f"  static class Builder {{\n"
        f"    public Builder knob(int value) {{ return this; }}\n  }}\n}}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 0


@pytest.mark.parametrize(
    "declaration",
    [
        # A different annotation whose name merely starts with `Internal` — gax's
        # `@InternalApi` is one this repository already sees.
        "@InternalApi\nfinal class Helper {",
        # A shape TOP_LEVEL_TYPE cannot parse at all (annotation on the same line
        # as the declaration). Unparseable must mean reported, not exempted.
        "@Deprecated public final class Helper {",
    ],
)
def test_a_class_the_exemption_cannot_confirm_is_reported(
    root, check_option_docs, capsys, declaration
):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Helper.java",
        f"package io.github.x;\n\n{declaration}\n"
        f"  static class Builder {{\n"
        f"    public Builder knob(int value) {{ return this; }}\n  }}\n}}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 1
    assert "Helper.java declares public builder setters" in capsys.readouterr().err


def test_an_internal_inside_a_string_literal_does_not_exempt(
    root, check_option_docs, capsys
):
    # blank_comments keeps string literals, so an unanchored search would read
    # this one as the annotation — the guard's only silent-pass direction.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Queue.java",
        'package io.github.x;\n\n@SuppressWarnings("@Internal")\npublic class Queue {\n'
        "  public static class Builder {\n"
        "    public Builder knob(int value) { return this; }\n  }\n}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 1
    assert "Queue.java declares public builder setters" in capsys.readouterr().err


def test_setters_of_the_same_class_name_in_two_roots_are_merged(
    root, check_option_docs
):
    # Every connector holds a `CrossVersionSink.java` in both compat roots
    # (ADR-0054), so keying by class name and *assigning* would let the root
    # sorted last hide the other's options. Both rows documented, so the
    # assigning version fails on the one it dropped being a stale row — asserting
    # only that the *other* one is undocumented would pass either way.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "flinkTwo"))
    compat = root / "conn" / "src" / "main" / "java-flink1" / "AOptions.java"
    compat.parent.mkdir(parents=True, exist_ok=True)
    compat.write_text(options_class("AOptions", "flinkOne"))
    page = write_page(root, "conn.md", option_page("`flinkOne`", "`flinkTwo`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 0


def test_a_javadoc_between_the_annotation_and_the_declaration_still_exempts(
    root, check_option_docs
):
    # Comments are blanked to blank lines before the guard reads the block, so a
    # block that only accepted contiguous annotation lines would lose the
    # annotation to a docstring sitting under it.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    write_source(
        root,
        "conn",
        "Helper.java",
        "package io.github.x;\n\n@Internal\n/** Why this exists. */\n"
        "final class Helper {\n  static class Builder {\n"
        "    public Builder knob(int value) { return this; }\n  }\n}\n",
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page)])
    assert exit_code(check_option_docs) == 0


def test_the_stray_module_guard_reads_the_compat_roots_too(
    root, check_option_docs, capsys
):
    # An options class in a compat root of an *unmapped* module. The reach guard
    # skips it — it matches a glob, so nothing is unreachable about it — leaving
    # the stray-module guard as the only thing that can report it.
    clean_tree(root)
    write_source(
        root,
        "other",
        "BOptions.java",
        options_class("BOptions", "knob"),
        tree="java-flink2",
    )
    assert exit_code(check_option_docs) == 1
    assert "other declares options" in capsys.readouterr().err


def test_a_config_options_path_spelled_with_a_leading_dot_still_counts_as_claimed(
    root, check_option_docs
):
    # Same normalisation, the other table: unnormalised, the entry no longer
    # matches `claimed`, so the Table API class is scanned as a builder source
    # and exits 2 for declaring no setter.
    builder_page, source, table_page = table_api_tree(
        root, ["`topic.id`", "`sink.retries`"]
    )
    write_config(
        root,
        builders=[("conn", builder_page)],
        config_options=[(f"./{source}", table_page)],
    )
    assert exit_code(check_option_docs) == 0


def test_a_sources_path_spelled_with_a_leading_dot_still_counts_as_mapped(
    root, check_option_docs
):
    # The path is both joined to ROOT and compared as a string against a
    # `relative_to(ROOT)` one. Unnormalised, the join reads the class and the
    # comparison does not, so it is checked *and* reported as reached by nothing.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    queue = write_source(root, "conn", "Queue.java", options_class("Queue", "knob"))
    page = write_page(root, "conn.md", option_page("`topic`", "`knob`"))
    write_config(root, builders=[("conn", page, [f"./{queue}"])])
    assert exit_code(check_option_docs) == 0


def test_a_sources_entry_naming_a_setterless_file_is_infrastructure(
    root, check_option_docs
):
    dlq_tree(root, "`flushTimeout`", setters=())
    assert exit_code(check_option_docs) == 2


@pytest.mark.parametrize(
    "path",
    [
        "conn/src/main/java/io/github/x/Absent.java",
        # A typo in the module segment: existence is checked before placement,
        # because "does not exist" locates a typo where "does not live in one of
        # its main source roots" only puzzles.
        "conm/src/main/java/io/github/x/Queue.java",
    ],
)
def test_a_sources_entry_naming_a_missing_file_says_so(
    root, check_option_docs, capsys, path
):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page, [path])])
    assert exit_code(check_option_docs) == 2
    assert f"{path} does not exist" in capsys.readouterr().err


def test_a_sources_entry_outside_the_module_is_infrastructure(root, check_option_docs):
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    elsewhere = write_source(
        root, "other", "Queue.java", options_class("Queue", "knob")
    )
    page = write_page(root, "conn.md", option_page("`topic`", "`knob`"))
    write_config(root, builders=[("conn", page, [elsewhere])])
    assert exit_code(check_option_docs) == 2


def test_a_sources_entry_duplicating_a_glob_match_is_infrastructure(
    root, check_option_docs
):
    # It changes nothing, so it is the same dead claim a never-firing allowlist
    # entry is.
    source = write_source(
        root, "conn", "AOptions.java", options_class("AOptions", "topic")
    )
    page = write_page(root, "conn.md", option_page("`topic`"))
    write_config(root, builders=[("conn", page, [source])])
    assert exit_code(check_option_docs) == 2


def compat_source(root, name, body):
    path = root / "conn" / "src" / "main" / "java-flink2" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def test_a_builder_in_a_compat_source_root_is_reported(root, check_option_docs, capsys):
    # `java-flink1` / `java-flink2` are source roots too (ADR-0054); a builder
    # in one must not be invisible for living there.
    clean_tree(root)
    compat_source(root, "Queue.java", options_class("Queue", "knob"))
    assert exit_code(check_option_docs) == 1
    assert "java-flink2/Queue.java declares public builder" in capsys.readouterr().err


def test_a_glob_match_in_a_compat_source_root_is_scanned(
    root, check_option_docs, capsys
):
    # The other half: the guard and the glob scan must agree about which roots
    # exist, or a compat-root options class is reported as unmapped by a message
    # claiming it matches no pattern — while it matches one nothing read.
    clean_tree(root)
    compat_source(root, "BOptions.java", options_class("BOptions", "knob"))
    assert exit_code(check_option_docs) == 1
    err = capsys.readouterr().err
    assert "BOptions.knob is a builder option" in err
    assert "declares public builder setters" not in err


# --- hardening shared with check-metric-docs.py (PR #302's self-review) ---


@pytest.mark.parametrize("fence", ["```", "~~~"])
def test_a_fenced_example_table_is_not_read(root, check_option_docs, fence):
    # A snippet showing what an option table looks like must earn no coverage
    # credit: deleting the real table while the example remains has to fail.
    page = root / "page.md"
    page.write_text(f"# t\n\n{fence}\n" + option_page("`example`") + f"{fence}\n")
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


@pytest.mark.parametrize(
    "sources",
    [
        # One string rather than a list of them: iterating it yields characters.
        '"AOptions.java"',
        # A list holding something that is not a path at all.
        "[3]",
    ],
)
def test_a_malformed_sources_list_is_named_as_one(
    root, check_option_docs, capsys, sources
):
    # Over an otherwise clean tree, so nothing but the type check can be what
    # ends the run — and asserting the message rather than the code, because a
    # string still exits 2 without the check, by a message about a path called
    # `A`. `[3]` does not exit at all without it; it raises TypeError.
    write_source(root, "conn", "AOptions.java", options_class("AOptions", "topic"))
    page = write_page(root, "conn.md", option_page("`topic`"))
    (root / "option-docs.toml").write_text(
        f'[[builders]]\nmodule = "conn"\npage = "{page}"\nsources = {sources}\n'
    )
    assert exit_code(check_option_docs) == 2
    assert "sources of the [[builders]] entry for conn" in capsys.readouterr().err
