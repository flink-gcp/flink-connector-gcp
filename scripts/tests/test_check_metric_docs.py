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
"""Tests for scripts/check-metric-docs.py (issue #296).

Synthetic fixtures only — a module tree and a documentation page built in
tmp_path, with ROOT and CONFIG monkeypatched onto it, the same shape
test_check_option_docs.py has. The real tree's verdict is CI's own job; what
has no other cover is the parsing, and the direction that matters is a checker
quietly finding less than it should, which reads exactly like a clean tree.

Exit codes are the assertion surface: 0 clean, 1 policy violation, 2 an
infrastructure or config-authoring error.
"""

import pytest


def camel(constant):
    head, *tail = constant.lower().split("_")
    return head + "".join(word.capitalize() for word in tail)


def inventory_class(name, *constants):
    """A `*MetricNames.java` declaring each constant as its camelCase literal."""
    body = "\n".join(
        f'    public static final String {constant} = "{camel(constant)}";'
        for constant in constants
    )
    return f"package io.github.x;\n\npublic final class {name} {{\n{body}\n}}\n"


def metrics_class(
    inventory, counters=(), gauges=(), extra_lines=(), name="WriterMetrics"
):
    """A writer-metrics class registering the given inventory constants."""
    lines = [
        f"        this.{camel(c)} = metricGroup.counter({inventory}.{c});"
        for c in counters
    ]
    lines += [f"        metricGroup.gauge({inventory}.{g}, () -> 0);" for g in gauges]
    lines += [f"        {line}" for line in extra_lines]
    body = "\n".join(lines)
    return (
        f"package io.github.x;\n\n"
        f"public class {name} {{\n"
        f"    {name}(MetricGroup metricGroup) {{\n{body}\n    }}\n"
        f"}}\n"
    )


def subgroup_class(name, group_constant, group, leaves):
    """A base.metrics-style registrar: one addGroup segment, fixed leaves."""
    constants = [f'    public static final String {group_constant} = "{group}";']
    constants += [
        f'    public static final String {constant} = "{camel(constant)}";'
        for constant in leaves
    ]
    registrations = " ".join(f"group.counter({constant});" for constant in leaves)
    body = "\n".join(constants)
    return (
        f"package io.github.x;\n\n"
        f"public final class {name} {{\n{body}\n"
        f"    static void register(MetricGroup metricGroup, String key) {{\n"
        f"        MetricGroup group = metricGroup.addGroup({group_constant}, key);\n"
        f"        {registrations}\n"
        f"    }}\n"
        f"}}\n"
    )


def metric_page(*rows, header="Metric", second="Type", title="# Connector"):
    """A page carrying one table; `header` is what opts it in (or does not)."""
    lines = [title, "", f"| {header} | {second} | Meaning |", "|---|---|---|"]
    lines += [f"| {name} | {kind} | Meaning. |" for name, kind in rows]
    return "\n".join(lines) + "\n"


def write_source(root, module, name, body, tree="src/main/java"):
    path = root / module
    for part in tree.split("/"):
        path = path / part
    path = path / "io" / "github" / "x" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return str(path.relative_to(root))


def write_page(root, name, body):
    path = root / "docs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return f"docs/{name}"


def write_config(root, connectors=(), subgroups=(), exempt=(), extra=()):
    lines = []
    for module, page in connectors:
        lines += ["[[connectors]]", f'module = "{module}"', f'page = "{page}"', ""]
    for source in subgroups:
        lines += ["[[subgroups]]", f'source = "{source}"', ""]
    lines += ["[exempt]", *(f'"{key}" = "because."' for key in exempt), ""]
    lines += ["[extra]", *(f'"{key}" = "because."' for key in extra), ""]
    (root / "metric-docs.toml").write_text("\n".join(lines))


@pytest.fixture()
def root(tmp_path, check_metric_docs, monkeypatch):
    monkeypatch.setattr(check_metric_docs, "ROOT", tmp_path)
    monkeypatch.setattr(check_metric_docs, "CONFIG", tmp_path / "metric-docs.toml")
    return tmp_path


def exit_code(module) -> int:
    try:
        return module.main()
    except SystemExit as error:
        return error.code


def connector(root, counters=("ROWS_SENT",), gauges=()):
    """One module: an inventory class plus a writer registering all of it."""
    write_source(
        root,
        "conn",
        "AMetricNames.java",
        inventory_class("AMetricNames", *counters, *gauges),
    )
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=counters, gauges=gauges),
    )


def clean_tree(root, rows=(("`rowsSent`", "counter"),), **config):
    connector(root)
    page = write_page(root, "conn.md", metric_page(*rows))
    write_config(root, connectors=[("conn", page)], **config)
    return page


# --- the opt-in rule: a metrics table is one headed exactly `Metric` ---


@pytest.mark.parametrize(
    "header",
    [
        "Name",
        # Exactly `Metric`, not merely starting with it: every other table the
        # same pages carry must stay outside the check, and the header is the
        # only thing holding them there.
        "Metrics",
        "Metric name",
        "metric",
    ],
)
def test_only_metric_headed_tables_are_read(root, check_metric_docs, header):
    page = root / "page.md"
    page.write_text(metric_page(("`documented`", "counter"), header=header))
    rows, problems = check_metric_docs.metric_table_rows(page)
    assert rows == [] and problems == []


def test_metric_headed_table_yields_rows_with_types_and_lines(root, check_metric_docs):
    page = root / "page.md"
    page.write_text(metric_page(("`first`", "counter"), ("`second`", "gauge")))
    rows, problems = check_metric_docs.metric_table_rows(page)
    # Title, blank, header, separator, then the rows.
    assert rows == [(["first"], "counter", 5), (["second"], "gauge", 6)]
    assert problems == []


def test_one_cell_may_name_a_pair(root, check_metric_docs):
    # The real pages write `destination.T.recordsSend`, `destination.T.sendErrors`
    # as one row sharing one meaning; both names must count.
    page = root / "page.md"
    page.write_text(metric_page(("`a.T.x`, `a.T.y`", "counter")))
    (rows, _) = check_metric_docs.metric_table_rows(page)
    assert rows == [(["a.T.x", "a.T.y"], "counter", 5)]


def test_a_non_table_line_ends_the_table(root, check_metric_docs):
    page = root / "page.md"
    page.write_text(
        metric_page(("`inside`", "counter"))
        + "\n"
        + metric_page(("`outside`", "counter"), header="Name", title="## Other")
    )
    rows, _ = check_metric_docs.metric_table_rows(page)
    assert [names for names, _, _ in rows] == [["inside"]]


@pytest.mark.parametrize("fence", ["```", "~~~"])
def test_a_fenced_example_table_is_not_read(root, check_metric_docs, fence):
    # A snippet showing what a metrics table looks like must earn no coverage
    # credit: deleting the real table while the example remains has to fail.
    page = root / "page.md"
    page.write_text(
        f"# t\n\n{fence}\n" + metric_page(("`example`", "counter")) + f"{fence}\n"
    )
    rows, problems = check_metric_docs.metric_table_rows(page)
    assert rows == [] and problems == []


def test_a_metric_table_without_a_type_column_fails(root, check_metric_docs):
    # The Type cell is what the kind check reads; a Metric table without one
    # has nowhere to be right or wrong, which must not read as clean.
    page = root / "page.md"
    page.write_text(metric_page(("`orphan`", "counter"), second="Meaning"))
    rows, problems = check_metric_docs.metric_table_rows(page)
    assert rows == []
    assert len(problems) == 1 and "needs `Type` as its second column" in problems[0]


# --- the source side: inventory and registrations ---


def sources_of(check_metric_docs, module):
    return check_metric_docs.blanked_sources(module)


def test_registrations_resolve_constants_across_line_wraps(root, check_metric_docs):
    # The formatter wraps long registrations (`metricGroup.counter(\n
    # Names.X, ...)`); the pattern must reach across the newline.
    connector(root)
    write_source(
        root,
        "conn",
        "ReaderMetrics.java",
        metrics_class(
            "AMetricNames",
            name="ReaderMetrics",
            extra_lines=[
                "this.x =",
                "        metricGroup.counter(",
                "                AMetricNames.ROWS_SENT, new ThreadSafeSimpleCounter());",
            ],
        ),
    )
    sources = sources_of(check_metric_docs, "conn")
    constants = check_metric_docs.inventory("conn", sources)
    registered, used, problems = check_metric_docs.registrations(
        "conn", sources, constants
    )
    assert registered == {"rowsSent": "counter"} and problems == []
    assert used == {("AMetricNames", "ROWS_SENT")}


def test_a_registration_inside_a_comment_is_not_a_registration(root, check_metric_docs):
    connector(root)
    write_source(
        root,
        "conn",
        "ReaderMetrics.java",
        "package io.github.x;\n\n"
        "public class ReaderMetrics {\n"
        '    // was: metricGroup.counter("ghost");\n'
        "    /** See {@code metricGroup.gauge(AMetricNames.GHOST, x)}. */\n"
        "    void f() {}\n"
        "}\n",
    )
    sources = sources_of(check_metric_docs, "conn")
    constants = check_metric_docs.inventory("conn", sources)
    registered, _, problems = check_metric_docs.registrations(
        "conn", sources, constants
    )
    assert registered == {"rowsSent": "counter"} and problems == []


def test_a_comment_marker_inside_a_string_does_not_swallow_its_line(
    root, check_metric_docs
):
    # `"http://…"` carries `//`; naive comment blanking would erase the rest of
    # the line, and a registration sharing it would silently vanish.
    connector(root, counters=("ROWS_SENT", "OTHER"))
    write_source(
        root,
        "conn",
        "ReaderMetrics.java",
        metrics_class(
            "AMetricNames",
            name="ReaderMetrics",
            extra_lines=[
                'String u = "http://e"; this.o = metricGroup.counter(AMetricNames.OTHER);'
            ],
        ),
    )
    # connector() already registers OTHER in WriterMetrics; rebuild it so the
    # tricky line is the only registration of OTHER.
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=("ROWS_SENT",)),
    )
    sources = sources_of(check_metric_docs, "conn")
    constants = check_metric_docs.inventory("conn", sources)
    registered, _, problems = check_metric_docs.registrations(
        "conn", sources, constants
    )
    assert registered == {"rowsSent": "counter", "other": "counter"}
    assert problems == []


def test_a_registration_outside_the_inventory_fails(root, check_metric_docs, capsys):
    clean_tree(root)
    write_source(
        root,
        "conn",
        "RogueMetrics.java",
        metrics_class(
            "AMetricNames",
            name="RogueMetrics",
            extra_lines=['metricGroup.counter("offBook");'],
        ),
    )
    assert exit_code(check_metric_docs) == 1
    assert "outside the module's *MetricNames.java inventory" in capsys.readouterr().err


def test_an_inventory_constant_nothing_registers_fails(root, check_metric_docs, capsys):
    write_source(
        root,
        "conn",
        "AMetricNames.java",
        inventory_class("AMetricNames", "ROWS_SENT", "GHOST"),
    )
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=("ROWS_SENT",)),
    )
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "AMetricNames.GHOST names `ghost` but nothing registers it" in err


def test_a_dead_constant_does_not_hide_behind_a_duplicate_literal(
    root, check_metric_docs, capsys
):
    # Two constants carrying the same literal: the check is per constant, so
    # the unregistered one still fails even though its name is registered.
    write_source(
        root,
        "conn",
        "AMetricNames.java",
        "package io.github.x;\n\n"
        "public final class AMetricNames {\n"
        '    public static final String ROWS_SENT = "rowsSent";\n'
        '    public static final String LEGACY = "rowsSent";\n'
        "}\n",
    )
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=("ROWS_SENT",)),
    )
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "AMetricNames.LEGACY names `rowsSent` but nothing registers it" in err


def test_a_name_registered_as_both_kinds_fails_once(root, check_metric_docs, capsys):
    connector(root)
    write_source(
        root,
        "conn",
        "OtherMetrics.java",
        metrics_class("AMetricNames", name="OtherMetrics", gauges=("ROWS_SENT",)),
    )
    write_source(
        root,
        "conn",
        "ThirdMetrics.java",
        metrics_class("AMetricNames", name="ThirdMetrics", gauges=("ROWS_SENT",)),
    )
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    # One problem, not one per conflicting occurrence.
    assert err.count("both a counter and a gauge") == 1


def test_a_num_prefixed_name_fails(root, check_metric_docs, capsys):
    connector(root, counters=("NUM_ROWS_SENT",))
    page = write_page(root, "conn.md", metric_page(("`numRowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    assert "takes Flink's `num` prefix" in capsys.readouterr().err


def test_a_registration_in_a_compat_source_root_is_scanned(root, check_metric_docs):
    # src/main/java-flink1 and java-flink2 are main sources too; a metric
    # registered only there must not read as a dead inventory entry.
    write_source(
        root,
        "conn",
        "AMetricNames.java",
        inventory_class("AMetricNames", "ROWS_SENT", "SEAM"),
    )
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=("ROWS_SENT",)),
    )
    write_source(
        root,
        "conn",
        "SeamMetrics.java",
        metrics_class("AMetricNames", name="SeamMetrics", counters=("SEAM",)),
        tree="src/main/java-flink2",
    )
    page = write_page(
        root, "conn.md", metric_page(("`rowsSent`", "counter"), ("`seam`", "counter"))
    )
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 0


# --- both directions, end to end ---


def test_a_documented_metric_passes(root, check_metric_docs):
    clean_tree(root)
    assert exit_code(check_metric_docs) == 0


def test_an_undocumented_metric_fails(root, check_metric_docs, capsys):
    connector(root, counters=("ROWS_SENT", "ROWS_LOST"))
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "registers `rowsLost` (counter) but no `Metric`-headed table" in err


def test_a_fenced_table_earns_no_coverage_credit(root, check_metric_docs, capsys):
    # The end-to-end half of the fence rule: the only table naming the metric
    # sits in a snippet, so coverage must still fail.
    connector(root)
    page = write_page(
        root,
        "conn.md",
        "# t\n\n```\n" + metric_page(("`rowsSent`", "counter")) + "```\n",
    )
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 1
    assert "registers `rowsSent` (counter) but no" in capsys.readouterr().err


def test_a_row_nothing_registers_fails(root, check_metric_docs, capsys):
    clean_tree(root, rows=(("`rowsSent`", "counter"), ("`renamedAway`", "counter")))
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "names `renamedAway`, which conn does not register" in err
    assert "docs/conn.md:6:" in err


def test_a_kind_mismatch_fails(root, check_metric_docs, capsys):
    clean_tree(root, rows=(("`rowsSent`", "gauge"),))
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "documented as a gauge but conn registers it as a counter" in err


def test_a_type_cell_leading_with_neither_kind_fails(root, check_metric_docs, capsys):
    clean_tree(root, rows=(("`rowsSent`", "Counter"),))
    assert exit_code(check_metric_docs) == 1
    assert "must lead with `counter` or `gauge`" in capsys.readouterr().err


# --- the (Flink standard) marker is load-bearing ---


def test_the_standard_marker_exempts_an_unregistered_row(root, check_metric_docs):
    clean_tree(
        root,
        rows=(
            ("`rowsSent`", "counter"),
            ("`numRecordsSend`", "counter (Flink standard)"),
        ),
    )
    assert exit_code(check_metric_docs) == 0


def test_the_standard_marker_on_a_registered_name_fails(
    root, check_metric_docs, capsys
):
    # The marker exempts a row from the registration requirement, so a name we
    # do register must not be allowed to wear it — that is what would let a
    # stale row hide behind it.
    clean_tree(root, rows=(("`rowsSent`", "counter (Flink standard)"),))
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "marked (Flink standard) but conn registers it itself" in err


# --- subgroup templates: errorClass.CODE.errors and friends ---


def subgroup_tree(root, page_rows, use=True, subgroup_in_config=True):
    source = write_source(
        root,
        "base",
        "ErrorCounters.java",
        subgroup_class("ErrorCounters", "GROUP", "errorClass", ["ERRORS"]),
    )
    connector(root)
    if use:
        write_source(
            root,
            "conn",
            "Writer.java",
            "package io.github.x;\n\n"
            "public class Writer {\n"
            "    private final ErrorCounters errorClasses = null;\n"
            "}\n",
        )
    page = write_page(root, "conn.md", metric_page(*page_rows))
    write_config(
        root,
        connectors=[("conn", page)],
        subgroups=[source] if subgroup_in_config else [],
    )
    return source


def test_a_used_subgroups_templated_row_passes(root, check_metric_docs):
    subgroup_tree(
        root,
        [("`rowsSent`", "counter"), ("`errorClass.CODE.errors`", "counter")],
    )
    assert exit_code(check_metric_docs) == 0


def test_a_used_subgroup_without_its_row_fails(root, check_metric_docs, capsys):
    subgroup_tree(root, [("`rowsSent`", "counter")])
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "uses ErrorCounters but no `Metric`-headed table documents" in err


def test_a_templated_row_for_an_unused_subgroup_fails(root, check_metric_docs, capsys):
    subgroup_tree(
        root,
        [("`rowsSent`", "counter"), ("`errorClass.CODE.errors`", "counter")],
        use=False,
    )
    assert exit_code(check_metric_docs) == 1
    assert "conn does not use ErrorCounters" in capsys.readouterr().err


def test_a_templated_row_no_subgroup_source_registers_fails(
    root, check_metric_docs, capsys
):
    subgroup_tree(
        root,
        [("`rowsSent`", "counter"), ("`errorClass.CODE.wrongLeaf`", "counter")],
    )
    assert exit_code(check_metric_docs) == 1
    assert "which no [[subgroups]] source registers" in capsys.readouterr().err


def test_a_templated_rows_kind_is_checked_too(root, check_metric_docs, capsys):
    subgroup_tree(
        root,
        [("`rowsSent`", "counter"), ("`errorClass.CODE.errors`", "gauge")],
    )
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "documented as a gauge but ErrorCounters registers it as a counter" in err


def test_the_standard_marker_on_a_templated_row_fails(root, check_metric_docs, capsys):
    # The templated half of the marker guard: a subgroup leaf is registered by
    # this repository too, so the marker cannot exempt its row either.
    subgroup_tree(
        root,
        [
            ("`rowsSent`", "counter"),
            ("`errorClass.CODE.errors`", "counter (Flink standard)"),
        ],
    )
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "this repository registers it, through ErrorCounters" in err


def test_a_lowercase_middle_segment_is_a_name_not_a_placeholder(
    root, check_metric_docs, capsys
):
    # `a.code.errors` has no all-caps placeholder, so it is an ordinary name
    # the module does not register — the template rule must not swallow it.
    subgroup_tree(
        root,
        [("`rowsSent`", "counter"), ("`errorClass.code.errors`", "counter")],
    )
    assert exit_code(check_metric_docs) == 1
    err = capsys.readouterr().err
    assert "names `errorClass.code.errors`, which conn does not register" in err


def test_a_num_prefixed_subgroup_leaf_fails_naming_the_registrar(
    root, check_metric_docs, capsys
):
    # The #280 prefix rule reaches the base registrars too, attributed to the
    # registrar rather than to whichever module happens to use it — and it
    # fires even when no module does.
    write_source(
        root,
        "base",
        "ErrorCounters.java",
        subgroup_class("ErrorCounters", "GROUP", "errorClass", ["NUM_ERRORS"]),
    )
    clean_tree(root)
    write_config(
        root,
        connectors=[("conn", "docs/conn.md")],
        subgroups=["base/src/main/java/io/github/x/ErrorCounters.java"],
    )
    assert exit_code(check_metric_docs) == 1
    assert "ErrorCounters registers `numErrors`" in capsys.readouterr().err


# --- the allowlists, and entries that never fire ---


def test_exempt_forgives_a_registered_name_with_no_row(root, check_metric_docs):
    connector(root, counters=("ROWS_SENT", "ROWS_LOST"))
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)], exempt=["AMetricNames.rowsLost"])
    assert exit_code(check_metric_docs) == 0


def test_exempt_forgives_a_used_subgroup_leaf_with_no_row(root, check_metric_docs):
    source = subgroup_tree(root, [("`rowsSent`", "counter")])
    write_config(
        root,
        connectors=[("conn", "docs/conn.md")],
        subgroups=[source],
        exempt=["ErrorCounters.errors"],
    )
    assert exit_code(check_metric_docs) == 0


def test_extra_forgives_a_row_nothing_registers(root, check_metric_docs):
    clean_tree(
        root,
        rows=(("`rowsSent`", "counter"), ("`elsewhere`", "counter")),
        extra=["elsewhere"],
    )
    assert exit_code(check_metric_docs) == 0


@pytest.mark.parametrize(
    ("table", "entry"), [("exempt", "AMetricNames.unused"), ("extra", "unused")]
)
def test_an_allowlist_entry_that_never_fires_fails(
    root, check_metric_docs, capsys, table, entry
):
    clean_tree(root, **{table: [entry]})
    assert exit_code(check_metric_docs) == 1
    assert f'[{table}] entry "{entry}"' in capsys.readouterr().err


# --- unmapped modules ---


def test_a_module_with_an_inventory_and_no_mapping_fails(
    root, check_metric_docs, capsys
):
    clean_tree(root)
    write_source(
        root, "unmapped", "BMetricNames.java", inventory_class("BMetricNames", "X")
    )
    assert exit_code(check_metric_docs) == 1
    assert "unmapped registers metrics" in capsys.readouterr().err


def test_a_module_registering_without_an_inventory_and_no_mapping_fails(
    root, check_metric_docs, capsys
):
    # The other way a new connector arrives: bare registrations and no
    # *MetricNames class at all must still be caught.
    clean_tree(root)
    write_source(
        root,
        "unmapped",
        "Writer.java",
        metrics_class("AMetricNames", extra_lines=['metricGroup.counter("x");']),
    )
    assert exit_code(check_metric_docs) == 1
    assert "unmapped registers metrics" in capsys.readouterr().err


def test_an_unclaimed_registrar_shaped_source_is_stray(root, check_metric_docs, capsys):
    # The [[subgroups]] claim is what keeps the base registrars out of the
    # unmapped-module report; a registrar the config does not claim must
    # surface as stray rather than be quietly checked by nothing.
    subgroup_tree(
        root,
        [("`rowsSent`", "counter")],
        subgroup_in_config=False,
    )
    assert exit_code(check_metric_docs) == 1
    assert "base registers metrics" in capsys.readouterr().err


# --- infrastructure errors ---


def test_a_missing_config_file_is_an_infrastructure_error(root, check_metric_docs):
    assert exit_code(check_metric_docs) == 2


@pytest.mark.parametrize(
    "body",
    [
        # Not TOML at all.
        "[[connectors]\n",
        # No [[connectors]] mapping.
        '[exempt]\n[extra]\n"x" = "y"\n',
        # An entry missing its page.
        '[[connectors]]\nmodule = "conn"\n',
        # A [[subgroups]] entry missing its source.
        '[[connectors]]\nmodule = "conn"\npage = "docs/conn.md"\n[[subgroups]]\n',
    ],
)
def test_a_malformed_config_is_an_infrastructure_error(root, check_metric_docs, body):
    # The docstring promises exit 2 for malformed config, not a traceback.
    (root / "metric-docs.toml").write_text(body)
    assert exit_code(check_metric_docs) == 2


def test_a_missing_page_is_an_infrastructure_error(root, check_metric_docs):
    connector(root)
    write_config(root, connectors=[("conn", "docs/absent.md")])
    assert exit_code(check_metric_docs) == 2


def test_a_mapped_module_without_an_inventory_is_infrastructure(
    root, check_metric_docs
):
    write_source(root, "conn", "Writer.java", "package io.github.x;\nclass W {}\n")
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 2


def test_an_inventory_with_no_constants_is_infrastructure(root, check_metric_docs):
    write_source(
        root, "conn", "AMetricNames.java", "package io.github.x;\nclass A {}\n"
    )
    page = write_page(root, "conn.md", metric_page(("`rowsSent`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 2


def test_an_inventory_nothing_registers_through_is_infrastructure(
    root, check_metric_docs, capsys
):
    # An inventory with zero registrations found anywhere means the
    # registration shape changed, which would silently hollow out every
    # module's result — infrastructure, not a per-name policy failure.
    write_source(
        root, "conn", "AMetricNames.java", inventory_class("AMetricNames", "X")
    )
    page = write_page(root, "conn.md", metric_page(("`x`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 2
    # The message distinguishes this branch from the unresolvable-constant one
    # beside it, so pin it.
    assert "found no registration through it at all" in capsys.readouterr().err


def test_an_unresolvable_constant_reference_is_infrastructure(
    root, check_metric_docs, capsys
):
    write_source(
        root, "conn", "AMetricNames.java", inventory_class("AMetricNames", "X")
    )
    write_source(
        root,
        "conn",
        "WriterMetrics.java",
        metrics_class("AMetricNames", counters=("GHOST",)),
    )
    page = write_page(root, "conn.md", metric_page(("`x`", "counter")))
    write_config(root, connectors=[("conn", page)])
    assert exit_code(check_metric_docs) == 2
    assert "declares as a string constant" in capsys.readouterr().err


def test_an_unparseable_subgroup_source_is_infrastructure(root, check_metric_docs):
    source = write_source(
        root, "base", "ErrorCounters.java", "package io.github.x;\nclass E {}\n"
    )
    clean_tree(root)
    write_config(
        root,
        connectors=[("conn", "docs/conn.md")],
        subgroups=[source],
    )
    assert exit_code(check_metric_docs) == 2


def test_a_subgroup_leaf_with_two_kinds_is_infrastructure(root, check_metric_docs):
    # A registrar registering one leaf as counter here and gauge there makes
    # every page's Type check untrustworthy; the source is the thing to fix.
    source = write_source(
        root,
        "base",
        "ErrorCounters.java",
        "package io.github.x;\n\n"
        "public final class ErrorCounters {\n"
        '    public static final String GROUP = "errorClass";\n'
        '    public static final String ERRORS = "errors";\n'
        "    static void register(MetricGroup metricGroup, String key) {\n"
        "        MetricGroup group = metricGroup.addGroup(GROUP, key);\n"
        "        group.counter(ERRORS); group.gauge(ERRORS, () -> 0);\n"
        "    }\n"
        "}\n",
    )
    clean_tree(root)
    write_config(
        root,
        connectors=[("conn", "docs/conn.md")],
        subgroups=[source],
    )
    assert exit_code(check_metric_docs) == 2
