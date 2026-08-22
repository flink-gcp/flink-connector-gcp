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
"""Tests for scripts/check-option-message-names.py (ADR-0127, issue #1028).

Synthetic fixtures only — a module tree built in tmp_path with ROOT and CONFIG
monkeypatched onto it. The real tree's verdict is CI's own job; what has no
other cover is the parsing, and the direction that matters is a checker quietly
finding *fewer* call sites than exist, which reads exactly like a clean tree.

Two habits here are deliberate and worth keeping.

**Every exit-1 test asserts the problem count.** The checker accumulates
problems and exits 1 if any exist, so an assertion on the exit code and a
substring can be satisfied by a *different* problem than the one the test names.
That already happened once in review — a test meant to prove an unqualified call
is not a call site passed on a dead allowlist entry instead.

**The fixtures carry the shapes the real tree has**, not the simplest shape that
parses: a generic builder return type, an annotation above a declaration, a
`ConfigOption<List<String>>`. Dropping the generic group from `METHOD` leaves a
plain `public D topic(String)` matching while every real
`public BigQuerySourceBuilder<T> parentProject(String)` stops — so a fixture
without generics cannot see the parser go blind.

Exit codes are the assertion surface: 0 clean, 1 policy violation, 2 an
infrastructure or config-authoring error.
"""

import re

import pytest

MODULE = "flink-connector-gcp-demo"


def write(root, module, *parts, body):
    """Write a Java source at `<module>/src/main/java/io/github/<parts>`."""
    path = root.joinpath(module, "src", "main", "java", "io", "github", *parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return str(path.relative_to(root))


def connector_options(*keys, name="DemoConnectorOptions", listed=()):
    """A `*ConnectorOptions.java` declaring one `ConfigOption` per key.

    `listed` keys are declared as `ConfigOption<List<String>>`, the nested
    generic the real BigQuery and Pub/Sub options classes use, so CONFIG_OPTION's
    inner alternation is exercised rather than assumed.
    """
    lines = [f"package io.github.table;\n\npublic class {name} {{"]
    for key in (*keys, *listed):
        constant = key.upper().replace(".", "_").replace("-", "_")
        parameter = "List<String>" if key in listed else "String"
        lines.append(
            f"    public static final ConfigOption<{parameter}> {constant} =\n"
            f'            ConfigOptions.key("{key}").stringType().noDefaultValue();'
        )
    lines.append("}\n")
    return "\n".join(lines)


def builder(
    member, literal, klass="DemoBuilder", validator="ResourceNames.checkComponent"
):
    """A DataStream builder setter that rejects under a literal name.

    Generic return type and an annotation above the declaration, because that is
    what every real classified setter looks like.
    """
    return (
        f"package io.github.sink;\n\n"
        f"public class {klass}<T> {{\n"
        f"    @Nullable\n"
        f"    public {klass}<T> {member}(String value) {{\n"
        f'        this.value = {validator}(value, "{literal}");\n'
        f"        return this;\n"
        f"    }}\n"
        f"}}\n"
    )


def factory(body, klass="DemoDynamicTableFactory", package="table"):
    """A class holding whatever restatement a test needs."""
    return f"package io.github.{package};\n\npublic class {klass} {{\n{body}\n}}\n"


def restating(constant, validator="ResourceNames.checkComponent"):
    """The ordinary restatement: one method naming one option constant."""
    return (
        f"    private static String read(ReadableConfig config) {{\n"
        f"        return config.getOptional(DemoConnectorOptions.{constant})\n"
        f"                .map(value -> {validator}(value, "
        f"DemoConnectorOptions.{constant}.key()))\n"
        f"                .orElse(null);\n"
        f"    }}"
    )


def site(
    module=MODULE,
    klass="DemoBuilder",
    member="setting",
    literal="setting",
    verdict="same",
    keys=None,
    reason=None,
):
    entry = {
        "module": module,
        "class": klass,
        "member": member,
        "literal": literal,
        "verdict": verdict,
    }
    if keys is not None:
        entry["keys"] = keys
    if reason is not None:
        entry["reason"] = reason
    return entry


def write_config(root, *entries, raw=None):
    if raw is not None:
        (root / "option-message-names.toml").write_text(raw)
        return
    lines = []
    for entry in entries:
        lines.append("[[sites]]")
        for field, value in entry.items():
            if isinstance(value, list):
                rendered = ", ".join(f'"{item}"' for item in value)
                lines.append(f"{field} = [{rendered}]")
            else:
                lines.append(f'{field} = "{value}"')
        lines.append("")
    (root / "option-message-names.toml").write_text("\n".join(lines))


@pytest.fixture()
def root(tmp_path, check_option_message_names, monkeypatch):
    monkeypatch.setattr(check_option_message_names, "ROOT", tmp_path)
    monkeypatch.setattr(
        check_option_message_names,
        "CONFIG",
        tmp_path / "option-message-names.toml",
    )
    return tmp_path


def exit_code(module) -> int:
    try:
        return module.main()
    except SystemExit as error:
        return error.code


def problem_count(capsys) -> int:
    """How many problems the run reported, from its own footer.

    The reason every exit-1 test asserts this: the exit code alone cannot tell
    "the problem I meant" from "that problem plus two I did not".
    """
    err = capsys.readouterr().err
    match = re.search(r"(\d+) problem\(s\)", err)
    assert match, f"no problem count in stderr: {err!r}"
    return int(match.group(1))


# --- the three verdicts ---


def test_a_literal_equal_to_its_key_is_same(root, check_option_message_names):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_a_literal_that_is_no_key_cannot_be_same(
    root, check_option_message_names, capsys
):
    """The verdict is the comparison, not the entry's say-so."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("topicName", "topicName")
    )
    write_config(root, site(member="topicName", literal="topicName"))
    assert exit_code(check_option_message_names) == 1
    assert problem_count(capsys) == 1


def test_same_compares_against_the_whole_key_set_not_the_reaching_key(
    root, check_option_message_names
):
    """A documented limit, pinned so it cannot widen without someone deciding to.

    `judge` asks whether the literal is *a* key the module declares, not whether
    it is the key that reaches this setter — that is the reachability the script
    does not compute. A module with a second, differently-scoped project key
    therefore satisfies `same` on `project` even though a caller who wrote
    `scan.project` is answered `project`.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("project", "scan.project"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("of", "project"))
    write_config(root, site(member="of", literal="project"))
    assert exit_code(check_option_message_names) == 0


def test_a_restated_key_the_table_layer_checks_passes(root, check_option_message_names):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(restating("SINK_LOCATION")),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_restated_key_nothing_checks_fails(root, check_option_message_names, capsys):
    """The defect #1019 and #1027 were: the setter names itself, nothing else does."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    assert "nothing in flink-connector-gcp-demo's table package checks" in err
    assert 'would be answered "location …" instead' in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


def test_a_restated_key_no_options_class_declares_fails(
    root, check_option_message_names, capsys
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.locaiton"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    assert "does not declare" in capsys.readouterr().err


def test_a_restatement_outside_the_table_layer_does_not_count(
    root, check_option_message_names, capsys
):
    """A check on the DataStream side answers a DataStream caller, not a SQL one."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "sink",
        "Elsewhere.java",
        body=factory(restating("SINK_LOCATION"), klass="Elsewhere", package="sink"),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    # Exactly one: the DataStream-side keyed call must contribute no second
    # problem of its own, which is what makes this test about the table-layer
    # rule rather than about anything else the fixture happens to contain.
    assert problem_count(capsys) == 1


def test_unreachable_passes_on_its_reason_alone(root, check_option_message_names):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("jsonFieldPath", "path")
    )
    write_config(
        root,
        site(
            member="jsonFieldPath",
            literal="path",
            verdict="unreachable",
            reason="measured on #1027: a blank element reaches no check.",
        ),
    )
    assert exit_code(check_option_message_names) == 0


# --- the two directions of the allowlist ---


def test_an_unclassified_call_site_fails(root, check_option_message_names, capsys):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write(
        root,
        MODULE,
        "sink",
        "Other.java",
        body=builder("region", "region", klass="Other"),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    assert "a check that names a setting, with no verdict" in err
    assert "Other.region" in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


def test_an_entry_that_never_fires_fails(root, check_option_message_names, capsys):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write_config(
        root, site(member="topic", literal="topic"), site(member="gone", literal="gone")
    )
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    assert "never fires" in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


def test_two_entries_for_one_call_site_are_infrastructure(
    root, check_option_message_names, capsys
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write_config(
        root,
        site(member="topic", literal="topic"),
        site(member="topic", literal="topic"),
    )
    assert exit_code(check_option_message_names) == 2
    assert "classifies DemoBuilder.topic" in capsys.readouterr().err


# --- resolving which key a `.key()` names ---


def test_a_helper_takes_its_keys_from_its_call_sites_arguments(
    root, check_option_message_names
):
    """`notBlankUnderItsKey(config, SINK_LOCATION)` — the key is the argument.

    Read from the call's own arguments and not from the calling method's body,
    which on the real factory mentions forty other options.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location", "topic"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static String sinkLocation(ReadableConfig config) {\n"
            "        return underItsKey(config, DemoConnectorOptions.SINK_LOCATION);\n"
            "    }\n"
            "    private static String underItsKey(ReadableConfig config, "
            "ConfigOption<String> option) {\n"
            "        return config.getOptional(option)\n"
            "                .map(value -> ResourceNames.checkComponent(value, option.key()))\n"
            "                .orElse(null);\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_helper_does_not_cover_an_option_no_caller_hands_it(
    root, check_option_message_names, capsys
):
    """The mirror of the test above, and the one that would catch it going loose."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location", "topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topicName"))
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static String sinkLocation(ReadableConfig config) {\n"
            "        String unrelated = DemoConnectorOptions.TOPIC.key();\n"
            "        return underItsKey(config, DemoConnectorOptions.SINK_LOCATION);\n"
            "    }\n"
            "    private static String underItsKey(ReadableConfig config, "
            "ConfigOption<String> option) {\n"
            "        return config.getOptional(option)\n"
            "                .map(value -> ResourceNames.checkComponent(value, option.key()))\n"
            "                .orElse(null);\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(member="topic", literal="topicName", verdict="restated", keys=["topic"]),
    )
    assert exit_code(check_option_message_names) == 1
    assert problem_count(capsys) == 1


def test_a_same_named_method_in_another_class_donates_no_keys(
    root, check_option_message_names, capsys
):
    """`check`, `map` and `settings` recur across these table packages.

    Without file scoping, an unrelated one-argument `check(SINK_LOCATION)` in a
    sibling class made `sink.location` read as restated — measured on this
    branch, and it credited a class that never mentions the option.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location", "project"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "Helper.java",
        body=factory(
            "    private static String check(ReadableConfig config, "
            "ConfigOption<String> option) {\n"
            "        return ResourceNames.checkComponent(config.get(option), option.key());\n"
            "    }",
            klass="Helper",
        ),
    )
    write(
        root,
        MODULE,
        "table",
        "Unrelated.java",
        body=factory(
            "    private static void wire() {\n"
            "        register(check(config, DemoConnectorOptions.SINK_LOCATION));\n"
            "    }",
            klass="Unrelated",
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    assert problem_count(capsys) == 1


def test_a_helper_call_of_the_wrong_arity_donates_no_keys(
    root, check_option_message_names, capsys
):
    """Same-file, same name, different method — the arity is what separates them."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static String check(ReadableConfig config, "
            "ConfigOption<String> option) {\n"
            "        return ResourceNames.checkComponent(config.get(option), option.key());\n"
            "    }\n"
            "    private static void other() {\n"
            "        check(DemoConnectorOptions.SINK_LOCATION);\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    assert problem_count(capsys) == 1


def test_a_helper_reads_only_the_argument_it_names_keys_by(
    root, check_option_message_names, capsys
):
    """`check(config.get(SINK_LOCATION), TOPIC)` reports `topic`, not both.

    Reading every argument credited a `sink.location` verdict to a check whose
    failure says `topic`. Found by independent review.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location", "topic"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static String check(ConfigOption<String> option, "
            "Map<String, String> seen, String value) {\n"
            "        return ResourceNames.checkNotBlank(value, option.key());\n"
            "    }\n"
            "    private static void wire(ReadableConfig config) {\n"
            "        check(DemoConnectorOptions.TOPIC, seen, "
            "config.get(DemoConnectorOptions.SINK_LOCATION));\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    # Names the key, so "the helper credited the wrong argument" cannot be
    # satisfied by the entry simply never firing.
    assert "classified `restated` under `sink.location`" in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


def test_a_helper_with_a_generic_parameter_is_still_matched(
    root, check_option_message_names
):
    """`Map<String, String>` is one parameter, not two.

    Splitting the list on every comma put a phantom `String` in it, so the arity
    guard never matched a real call and every key that helper checked went
    uncredited — a verdict failing for a restatement that was there. The
    positive direction is what pins it: the negative one fails either way.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static String check(ConfigOption<String> option, "
            "Map<String, String> seen, String value) {\n"
            "        return ResourceNames.checkNotBlank(value, option.key());\n"
            "    }\n"
            "    private static void wire(ReadableConfig config) {\n"
            "        check(DemoConnectorOptions.SINK_LOCATION, seen, "
            "config.get(DemoConnectorOptions.SINK_LOCATION));\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_comparison_in_an_argument_does_not_hide_the_site(
    root, check_option_message_names, capsys
):
    """`length<1 ? a : b` — `<` after an identifier reads as a generic.

    The comma is then not split, the name stops looking like a literal, and the
    call site *disappears* rather than being reported. Formatting normally puts
    spaces around the operator; the checker must not depend on that.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            '        ResourceNames.checkNotBlank(length<1 ? a : b, "topic");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    # No entry: the site must still be found, and demand one.
    write_config(root, site())
    assert exit_code(check_option_message_names) == 1
    assert "DemoBuilder.topic" in capsys.readouterr().err


def test_a_text_block_does_not_swallow_the_call_after_it(
    root, check_option_message_names, capsys
):
    """A text block holding an odd number of quotes desynchronises the scan.

    Blanking text blocks is what stops that. Left live, the stray `"` pairs with
    the next one in the file and the *real* call below it lands inside the span
    — so the site disappears and the run reads clean.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            '        String doc = """\n'
            '            he said "hi\n'
            '            """;\n'
            '        ResourceNames.checkComponent(value, "topic");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    # No entry, so the site must be found and demand one; if the text block
    # swallowed it the run would be clean instead.
    write_config(root, site())
    assert exit_code(check_option_message_names) == 1
    assert "DemoBuilder.topic" in capsys.readouterr().err


def test_a_name_held_in_a_constant_is_reported_wherever_it_sits(
    root, check_option_message_names, capsys
):
    """`parse(value, NAME)` with a private `NAME = "emulatorEndpoint"`.

    Outside the table layer this was skipped rather than resolved, so the call
    left the population with nothing to report and no verdict to write — the
    setter could name itself while its factory restated nothing, and the run
    stayed green. Resolution now happens everywhere; only what a name
    *restates* is confined to the table layer.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("emulator-endpoint"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            '    private static final String NAME = "emulatorEndpoint";\n'
            "    public DemoBuilder<T> emulatorEndpoint(String value) {\n"
            "        EmulatorEndpoint.parse(value, NAME);\n"
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "neither a string literal nor an expression" in capsys.readouterr().err


def test_a_validator_named_inside_a_string_is_not_a_call(
    root, check_option_message_names
):
    """blank_comments keeps literals on purpose, so CALL can see into them.

    `String d = "ResourceNames.checkNotBlank(";` has no closing parenthesis to
    find, and failed the whole run on a call that does not exist.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write(
        root,
        MODULE,
        "sink",
        "Diagnostics.java",
        body=(
            "package io.github.sink;\n\n"
            "public class Diagnostics {\n"
            "    void f() {\n"
            '        String d = "ResourceNames.checkNotBlank(";\n'
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_a_control_block_is_not_a_member(root, check_option_message_names):
    """`METHOD`'s return type is optional for constructors, so `if (e) {` matches.

    A call inside one was attributed to a member named `if`, which no entry can
    name, and two such blocks looked like two declarations of it.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            "        if (enabled) {\n"
            '            ResourceNames.checkComponent(value, "topic");\n'
            "        }\n"
            "        if (other) {\n"
            '            ResourceNames.checkNotBlank(value, "topic");\n'
            "        }\n"
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    # One member, one literal, one verdict — and no `DemoBuilder.if`.
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_an_identifying_field_that_is_not_text_is_infrastructure(
    root, check_option_message_names, capsys
):
    """`literal = 1` stringified to "1" and matched nothing, forever."""
    write_config(
        root,
        raw='[[sites]]\nmodule = "m"\nclass = "C"\nmember = "x"\nliteral = 1\n'
        'verdict = "same"\n',
    )
    assert exit_code(check_option_message_names) == 2
    assert "lacks literal" in capsys.readouterr().err


def test_a_reason_that_is_not_text_is_infrastructure(
    root, check_option_message_names, capsys
):
    """`reason = 1` stringified to "1", which is not an argument."""
    write_config(
        root,
        raw='[[sites]]\nmodule = "m"\nclass = "C"\nmember = "x"\nliteral = "x"\n'
        'verdict = "unreachable"\nreason = 1\n',
    )
    assert exit_code(check_option_message_names) == 2
    assert "gives no reason" in capsys.readouterr().err


def test_an_overloaded_helper_name_is_reported_not_guessed(
    root, check_option_message_names, capsys
):
    """Calls are matched by name and arity, and two overloads share both.

    Correcting the parameter split gave the helper its real arity, which made a
    call to the *other* overload match — donating its argument as a key the
    helper never receives. Found by re-reviewing that fix.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location", "topic"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void check(ReadableConfig config, "
            "Map<String, String> hints, ConfigOption<String> option) {\n"
            "        ResourceNames.checkNotBlank(config.get(option), option.key());\n"
            "    }\n"
            "    private static void check(String a, String b, ConfigOption<String> other) {\n"
            "        log(a);\n"
            "    }\n"
            "    private void validate(ReadableConfig config) {\n"
            "        check(x, y, DemoConnectorOptions.TOPIC);\n"
            "    }"
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "declares 2 methods with that name" in capsys.readouterr().err


def test_a_generic_argument_is_one_argument(root, check_option_message_names):
    """`new HashMap<String, String>()` is one argument, not two.

    The declaration side was made generic-aware first; until the call side
    followed, the arity guard never matched and the key went uncredited — a
    verdict failing for a restatement that was there.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void check(ReadableConfig config, "
            "Map<String, String> seen, ConfigOption<String> option) {\n"
            "        ResourceNames.checkNotBlank(config.get(option), option.key());\n"
            "    }\n"
            "    private void wire(ReadableConfig config) {\n"
            "        check(config, new HashMap<String, String>(), "
            "DemoConnectorOptions.SINK_LOCATION);\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_brace_less_loop_body_is_lexed_not_scanned_for_a_semicolon(
    root, check_option_message_names
):
    """Two ways a `;` precedes the call without ending the body.

    `LOG.warn("a;b", …)` hides one in a string literal, which blank_comments
    keeps on purpose; `run(() -> { touch(); … })` puts one at brace depth 1.
    A raw `find(";")` ends the body at either, and the run then says the call
    sits inside no loop — a false statement about correct Java.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private void f(ReadableConfig config) {\n"
            "        for (ConfigOption<String> option : "
            "Arrays.asList(DemoConnectorOptions.SINK_LOCATION))\n"
            '            LOG.warn("a;b", ResourceNames.checkNotBlank('
            "config.get(option), option.key()));\n"
            "    }\n"
            "    private void g(ReadableConfig config) {\n"
            "        for (ConfigOption<String> option : "
            "Arrays.asList(DemoConnectorOptions.SINK_LOCATION))\n"
            "            run(() -> { touch(); ResourceNames.checkNotBlank("
            "config.get(option), option.key()); });\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_wildcard_for_header_is_read(root, check_option_message_names):
    """`ConfigOption<?>` — the header class admitted neither `?` nor `@`."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.location"),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("location", "location")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private void f(ReadableConfig config) {\n"
            "        for (ConfigOption<?> option : "
            "Arrays.asList(DemoConnectorOptions.SINK_LOCATION)) {\n"
            "            ResourceNames.checkNotBlank(config.get(option), option.key());\n"
            "        }\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_two_checks_in_one_body_share_one_verdict(root, check_option_message_names):
    """`checkNotBlank` then `checkComponent` on one value is one setting.

    The collision guard must fire on two *declarations*, not on two calls: the
    repair it names — rename one of them — does not exist for a single method
    that checks its argument twice.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("dataset"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    @Nullable\n"
            "    public DemoBuilder<T> dataset(String dataset) {\n"
            '        ResourceNames.checkNotBlank(dataset, "dataset");\n'
            '        ResourceNames.checkComponent(dataset, "dataset");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="dataset", literal="dataset"))
    assert exit_code(check_option_message_names) == 0


def test_two_enclosing_loops_binding_one_name_are_reported(
    root, check_option_message_names, capsys
):
    """Deliberately not valid Java — Java forbids shadowing a local.

    Which is the point: the guard exists for a *parser* that believes it saw
    two bindings, not for a source that has them. It fails closed rather than
    merging the headers, and this is what says so.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("emulator-endpoint", "topic"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void validate(ReadableConfig config) {\n"
            "        for (ConfigOption<String> option :\n"
            "                Arrays.asList(DemoConnectorOptions.EMULATOR_ENDPOINT)) {\n"
            "            for (ConfigOption<String> option :\n"
            "                    Arrays.asList(DemoConnectorOptions.TOPIC)) {\n"
            "                config.getOptional(option)\n"
            "                        .ifPresent(v -> EmulatorEndpoint.parse(v, option.key()));\n"
            "            }\n"
            "        }\n"
            "    }"
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    # The reason, not merely that resolution failed: deleting loop support
    # altogether produces the same exit code and the same opening sentence.
    assert "sits inside 2 for-each loops" in capsys.readouterr().err


def test_a_second_loop_binding_the_same_name_donates_no_keys(
    root, check_option_message_names, capsys
):
    """One `option` loop validates; a later `option` loop only logs.

    Aggregating every header in the method credited the second loop's option as
    restated. Found by independent review.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("emulator-endpoint", "topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=builder(
            "emulatorEndpoint", "emulatorEndpoint", validator="EmulatorEndpoint.parse"
        ),
    )
    write(
        root,
        MODULE,
        "sink",
        "Other.java",
        body=builder("topic", "topicName", klass="Other"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void validate(ReadableConfig config) {\n"
            "        for (ConfigOption<String> option :\n"
            "                Arrays.asList(DemoConnectorOptions.EMULATOR_ENDPOINT)) {\n"
            "            config.getOptional(option)\n"
            "                    .ifPresent(value -> EmulatorEndpoint.parse(value, option.key()));\n"
            "        }\n"
            "        for (ConfigOption<String> option :\n"
            "                Arrays.asList(DemoConnectorOptions.TOPIC)) {\n"
            "            log(option);\n"
            "        }\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="emulatorEndpoint",
            literal="emulatorEndpoint",
            verdict="restated",
            keys=["emulator-endpoint"],
        ),
        site(
            klass="Other",
            member="topic",
            literal="topicName",
            verdict="restated",
            keys=["topic"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    # Which entry failed, not just how many: a count of one is equally satisfied
    # by the opposite pair failing, which is what a last-header-wins bug does.
    assert "classified `restated` under `topic`" in err
    assert "emulator-endpoint`, but nothing" not in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


def test_two_checks_with_one_identity_are_infrastructure(
    root, check_option_message_names, capsys
):
    """Two overloads rejecting under the same literal cannot share one verdict.

    A new SQL-reachable overload would otherwise inherit the existing entry —
    an `unreachable` verdict answering a check that is now reachable. Found by
    independent review.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "Opts.java",
        body=(
            "package io.github.sink;\n\n"
            "public class Opts<T> {\n"
            "    @Nullable\n"
            "    public Opts<T> jsonFieldPath(String path) {\n"
            '        ResourceNames.checkNotBlank(path, "path");\n'
            "        return this;\n"
            "    }\n"
            "    @Nullable\n"
            "    public Opts<T> jsonFieldPath(Collection<String> paths) {\n"
            '        ResourceNames.checkNotBlank(first(paths), "path");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(
        root,
        site(
            klass="Opts",
            member="jsonFieldPath",
            literal="path",
            verdict="unreachable",
            reason="measured.",
        ),
    )
    assert exit_code(check_option_message_names) == 2
    assert "two checks with one identity" in capsys.readouterr().err


def test_a_loop_variable_takes_its_keys_from_the_header(
    root, check_option_message_names
):
    """BigQuery's `for (ConfigOption<String> option : asList(A, B))` shape."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("emulator-endpoint", "emulator-rest-endpoint"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=builder(
            "emulatorEndpoint", "emulatorEndpoint", validator="EmulatorEndpoint.parse"
        ),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void validate(ReadableConfig config) {\n"
            "        for (ConfigOption<String> option :\n"
            "                Arrays.asList(\n"
            "                        DemoConnectorOptions.EMULATOR_ENDPOINT,\n"
            "                        DemoConnectorOptions.EMULATOR_REST_ENDPOINT)) {\n"
            "            config.getOptional(option)\n"
            "                    .ifPresent(value -> EmulatorEndpoint.parse(value, option.key()));\n"
            "        }\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="emulatorEndpoint",
            literal="emulatorEndpoint",
            verdict="restated",
            keys=["emulator-endpoint", "emulator-rest-endpoint"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_loop_does_not_cover_an_option_its_body_merely_mentions(
    root, check_option_message_names, capsys
):
    """The finding that made this branch exact rather than loose.

    An earlier draft credited every constant named anywhere in the enclosing
    method. Adding one unrelated `SINK_LOCATION` line to the real
    `validateEmulatorEndpoints` then made `sink.location` read as restated, so
    deleting the factory's actual check left the run clean. Measured.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("emulator-endpoint", "sink.location"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=builder(
            "emulatorEndpoint", "emulatorEndpoint", validator="EmulatorEndpoint.parse"
        ),
    )
    write(
        root,
        MODULE,
        "sink",
        "Other.java",
        body=builder("location", "location", klass="Other"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void validate(ReadableConfig config) {\n"
            "        config.getOptional(DemoConnectorOptions.SINK_LOCATION)"
            ".ifPresent(v -> log(v));\n"
            "        for (ConfigOption<String> option :\n"
            "                Arrays.asList(DemoConnectorOptions.EMULATOR_ENDPOINT)) {\n"
            "            config.getOptional(option)\n"
            "                    .ifPresent(value -> EmulatorEndpoint.parse(value, option.key()));\n"
            "        }\n"
            "    }"
        ),
    )
    write_config(
        root,
        site(
            member="emulatorEndpoint",
            literal="emulatorEndpoint",
            verdict="restated",
            keys=["emulator-endpoint"],
        ),
        site(
            klass="Other",
            member="location",
            literal="location",
            verdict="restated",
            keys=["sink.location"],
        ),
    )
    assert exit_code(check_option_message_names) == 1
    assert problem_count(capsys) == 1


def test_a_key_wrapped_across_lines_is_not_read_as_a_literal(
    root, check_option_message_names
):
    """Spotless wraps a long qualified constant onto three lines."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("sink.file-loads.temp-dataset"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=builder("tempDataset", "tempDataset"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void map(ReadableConfig config) {\n"
            "        config.getOptional(DemoConnectorOptions.SINK_FILE_LOADS_TEMP_DATASET)\n"
            "                .ifPresent(\n"
            "                        value ->\n"
            "                                ResourceNames.checkComponent(\n"
            "                                        value,\n"
            "                                        DemoConnectorOptions\n"
            "                                                .SINK_FILE_LOADS_TEMP_DATASET\n"
            "                                                .key()));\n"
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(
        root,
        site(
            member="tempDataset",
            literal="tempDataset",
            verdict="restated",
            keys=["sink.file-loads.temp-dataset"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_key_composed_with_a_literal_is_not_read_as_a_literal(
    root, check_option_message_names
):
    """`"an entry of " + …ALLOWED_REGIONS.key()` names a key, not a setter."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options(listed=("regions",)),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("regions", "regionsList")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void map(ReadableConfig config) {\n"
            "        ResourceNames.checkNotBlank(\n"
            '                value, "an entry of " + DemoConnectorOptions.REGIONS.key());\n'
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(
        root,
        site(
            member="regions",
            literal="regionsList",
            verdict="restated",
            keys=["regions"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_a_local_standing_in_for_a_key_resolves_to_it(root, check_option_message_names):
    """`String key = …REGIONS.key();` then `"an entry of " + key`."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options(listed=("regions",)),
    )
    write(
        root, MODULE, "sink", "DemoBuilder.java", body=builder("regions", "regionsList")
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void map(ReadableConfig config) {\n"
            "        String key =\n"
            "                DemoConnectorOptions\n"
            "                        .REGIONS\n"
            "                        .key();\n"
            "        Preconditions.checkArgument(\n"
            '                !regions.isEmpty(), "%s must not be empty", key);\n'
            "        regions.forEach(\n"
            "                region ->\n"
            '                        ResourceNames.checkNotBlank(region, "an entry of " + key));\n'
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(
        root,
        site(
            member="regions",
            literal="regionsList",
            verdict="restated",
            keys=["regions"],
        ),
    )
    assert exit_code(check_option_message_names) == 0


def test_an_alias_does_not_leak_out_of_its_own_method(
    root, check_option_message_names, capsys
):
    """The mirror: a `key` declared in one method says nothing about another's."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("regions"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void declaring(ReadableConfig config) {\n"
            "        String key = DemoConnectorOptions.REGIONS.key();\n"
            "        use(key);\n"
            "    }\n"
            "    private static void borrowing(String key) {\n"
            '        ResourceNames.checkNotBlank(value, "an entry of " + key);\n'
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "neither a string literal nor an expression" in capsys.readouterr().err


def test_the_words_of_a_message_are_not_read_as_locals(
    root, check_option_message_names, capsys
):
    """An argument's prose is not code.

    The message says "the key of", and the method happens to hold a local named
    `key` — but the name actually comes from `label`, which names nothing this
    script can trace. Reading identifiers out of the literal resolves it to the
    local instead, and reports a restatement that is not there.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("regions"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void map(ReadableConfig config) {\n"
            "        String key = DemoConnectorOptions.REGIONS.key();\n"
            "        use(key);\n"
            '        ResourceNames.checkNotBlank(value, "the key of " + label);\n'
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "neither a string literal nor an expression" in capsys.readouterr().err


def test_an_alias_reassigned_in_its_method_is_reported_not_guessed(
    root, check_option_message_names, capsys
):
    """Two writes to one local: which key it holds depends on where the call sits.

    A dict of the last write answers every use with the same key — wrong in both
    directions from one edit, so this is reported rather than resolved.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("regions", "kms-key-name"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoOptionsMapper.java",
        body=factory(
            "    private static void map(ReadableConfig config) {\n"
            "        String key = DemoConnectorOptions.REGIONS.key();\n"
            '        ResourceNames.checkNotBlank(first, "an entry of " + key);\n'
            "        key = DemoConnectorOptions.KMS_KEY_NAME.key();\n"
            '        ResourceNames.checkNotBlank(second, "the " + key);\n'
            "    }",
            klass="DemoOptionsMapper",
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "neither a string literal nor an expression" in capsys.readouterr().err


def test_a_name_that_is_neither_shape_is_infrastructure(
    root, check_option_message_names, capsys
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void check(String value, String name) {\n"
            "        ResourceNames.checkComponent(value, name);\n"
            "    }"
        ),
    )
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "neither a string literal nor an expression" in capsys.readouterr().err


def test_a_keyed_call_outside_the_table_layer_needs_no_verdict(
    root, check_option_message_names
):
    """It restates nothing a SQL caller reads, and it is not a literal either."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write(
        root,
        MODULE,
        "sink",
        "Runtime.java",
        body=factory(restating("TOPIC"), klass="Runtime", package="sink"),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


# --- the parser: what it must not see, and what it must not lose ---


@pytest.mark.parametrize(
    "hidden",
    [
        '    // ResourceNames.checkComponent(value, "commented");',
        '    /* ResourceNames.checkComponent(value, "commented"); */',
        '    /** {@code ResourceNames.checkComponent(value, "commented")} */',
        '    String doc = """\n        ResourceNames.checkComponent(value, "quoted")\n        """;',
        '    String tricky = """\n        a \\""" b ResourceNames.checkComponent(v, "quoted")\n        """;',
        "    char quote = '\"';",
    ],
)
def test_a_call_that_is_not_code_is_not_a_call_site(
    root, check_option_message_names, hidden
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            f"{hidden}\n"
            '        ResourceNames.checkComponent(value, "topic");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_a_message_holding_a_bracket_does_not_end_the_argument_scan(
    root, check_option_message_names
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            '        ResourceNames.checkComponent(prefix(")", "a,b"), "topic");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_an_unbalanced_call_is_infrastructure(root, check_option_message_names, capsys):
    """Fail closed: a call that cannot be parsed is not a call that is absent."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            '        ResourceNames.checkComponent(value, "topic";\n'
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 2
    assert "no closing parenthesis" in capsys.readouterr().err


def test_an_unqualified_same_named_helper_is_not_this_validator(
    root, check_option_message_names
):
    """`BigtableDynamicTableFactory.checkNotBlank` is a different method.

    Exit 0 only if the unqualified call produced no call site of its own: it has
    no entry, and an unclassified site is exit 1.
    """
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write(
        root,
        MODULE,
        "table",
        "DemoDynamicTableFactory.java",
        body=factory(
            "    private static void validate(String value) {\n"
            '        checkNotBlank(value, "sink.other");\n'
            "    }"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 0


def test_a_static_import_of_a_validator_is_infrastructure(
    root, check_option_message_names, capsys
):
    """It would hide call sites with nothing to report — no site, no dead entry."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "import static io.github.base.options.ResourceNames.checkNotBlank;\n\n"
            "public class DemoBuilder<T> {\n"
            "    public DemoBuilder<T> topic(String value) {\n"
            '        checkNotBlank(value, "topic");\n'
            "        return this;\n"
            "    }\n"
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 2
    assert "statically imported" in capsys.readouterr().err


def test_a_constructor_is_a_member(root, check_option_message_names):
    """`TableDestination.of` delegates to one; a check moved there is classified."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("project"),
    )
    write(
        root,
        MODULE,
        "sink",
        "Destination.java",
        body=(
            "package io.github.sink;\n\n"
            "public class Destination {\n"
            "    Destination(String project) {\n"
            '        this.project = ResourceNames.checkComponent(project, "project");\n'
            "    }\n"
            "}\n"
        ),
    )
    write_config(
        root, site(klass="Destination", member="Destination", literal="project")
    )
    assert exit_code(check_option_message_names) == 0


def test_a_call_outside_any_member_is_infrastructure(
    root, check_option_message_names, capsys
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "sink",
        "DemoBuilder.java",
        body=(
            "package io.github.sink;\n\n"
            "public class DemoBuilder {\n"
            '    private static final String TOPIC = ResourceNames.checkComponent(raw, "topic");\n'
            "}\n"
        ),
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 2
    assert "sits in no method or constructor body" in capsys.readouterr().err


def test_an_options_class_declaring_nothing_is_infrastructure(
    root, check_option_message_names, capsys
):
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body="package io.github.table;\n\npublic class DemoConnectorOptions {}\n",
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 2
    assert "declares no ConfigOption this script recognises" in capsys.readouterr().err


def test_two_options_classes_are_merged(root, check_option_message_names):
    """The docstring advertises this; nothing else would notice it stopping."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "table",
        "SinkConnectorOptions.java",
        body=connector_options("sink.location", name="SinkConnectorOptions"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("of", "sink.location"))
    write_config(root, site(member="of", literal="sink.location"))
    assert exit_code(check_option_message_names) == 0


def test_one_constant_in_two_options_classes_is_infrastructure(
    root, check_option_message_names, capsys
):
    """Filename order must not decide which key a constant means."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(
        root,
        MODULE,
        "table",
        "SinkConnectorOptions.java",
        body=connector_options("sink.topic", name="SinkConnectorOptions").replace(
            "SINK_TOPIC", "TOPIC"
        ),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 2
    assert "is also declared in" in capsys.readouterr().err


def test_a_compat_source_root_is_read(root, check_option_message_names, capsys):
    """`java-flink1` / `java-flink2` are main sources too (ADR-0054)."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    compat = root.joinpath(MODULE, "src", "main", "java-flink2", "io", "github", "sink")
    compat.mkdir(parents=True)
    (compat / "CrossVersionSink.java").write_text(
        builder("region", "region", klass="CrossVersionSink")
    )
    write_config(root, site(member="topic", literal="topic"))
    assert exit_code(check_option_message_names) == 1
    err = capsys.readouterr().err
    assert "CrossVersionSink.region" in err
    assert re.search(r"(\d+) problem", err).group(1) == "1"


# --- config authoring: every one of these is exit 2 ---


def test_a_missing_config_is_infrastructure(root, check_option_message_names, capsys):
    assert exit_code(check_option_message_names) == 2
    # Named, because an empty tmp_path also has no source root and that guard
    # would otherwise satisfy this test from the wrong end.
    assert "is missing" in capsys.readouterr().err


def test_an_unknown_top_level_table_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, raw="[sights]\nx = 1\n")
    assert exit_code(check_option_message_names) == 2
    assert "unknown top-level entries" in capsys.readouterr().err


def test_a_config_with_no_sites_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, raw="# nothing here\n")
    assert exit_code(check_option_message_names) == 2
    assert "names no [[sites]] entry" in capsys.readouterr().err


def test_sites_written_as_a_table_is_infrastructure(
    root, check_option_message_names, capsys
):
    """`[sites]` rather than `[[sites]]` used to die on a traceback, exiting 1."""
    write_config(root, raw='[sites]\nmodule = "m"\n')
    assert exit_code(check_option_message_names) == 2
    assert "must be [[sites]] entries" in capsys.readouterr().err


def test_a_malformed_config_is_infrastructure(root, check_option_message_names, capsys):
    write_config(root, raw="[[sites]\n")
    assert exit_code(check_option_message_names) == 2
    assert "is not valid TOML" in capsys.readouterr().err


def test_an_entry_missing_a_field_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, raw='[[sites]]\nmodule = "m"\nclass = "C"\nverdict = "same"\n')
    assert exit_code(check_option_message_names) == 2
    assert "lacks member, literal" in capsys.readouterr().err


def test_an_entry_with_a_field_nothing_reads_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(
        root,
        raw='[[sites]]\nmodule = "m"\nclass = "C"\nmember = "x"\nliteral = "x"\n'
        'verdict = "same"\nnote = "why"\n',
    )
    assert exit_code(check_option_message_names) == 2
    assert "fields nothing reads" in capsys.readouterr().err


def test_an_unknown_verdict_is_infrastructure(root, check_option_message_names, capsys):
    write_config(root, site(verdict="probably"))
    assert exit_code(check_option_message_names) == 2
    assert "it must be one of same, restated, unreachable" in capsys.readouterr().err


def test_a_restated_entry_with_no_keys_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, site(verdict="restated"))
    assert exit_code(check_option_message_names) == 2
    assert "is restated but names no keys" in capsys.readouterr().err


def test_keys_on_a_verdict_that_ignores_them_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, site(verdict="same", keys=["topic"]))
    assert exit_code(check_option_message_names) == 2
    assert (
        "carries keys, which only a restated verdict is read for"
        in capsys.readouterr().err
    )


def test_an_unreachable_entry_with_no_reason_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, site(verdict="unreachable"))
    assert exit_code(check_option_message_names) == 2
    assert "gives no reason" in capsys.readouterr().err


def test_a_reason_on_a_verdict_that_ignores_it_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, site(verdict="same", reason="because."))
    assert exit_code(check_option_message_names) == 2
    assert "carries a reason" in capsys.readouterr().err


def test_no_source_root_at_all_is_infrastructure(
    root, check_option_message_names, capsys
):
    write_config(root, site())
    assert exit_code(check_option_message_names) == 2
    assert "the layout changed" in capsys.readouterr().err


# --- the success report ---


def test_the_tally_counts_the_sites_and_the_verdicts(
    root, check_option_message_names, capsys
):
    """Nothing else reads stdout, so a wrong tally would be invisible."""
    write(
        root,
        MODULE,
        "table",
        "DemoConnectorOptions.java",
        body=connector_options("topic"),
    )
    write(root, MODULE, "sink", "DemoBuilder.java", body=builder("topic", "topic"))
    write(
        root, MODULE, "sink", "Other.java", body=builder("path", "path", klass="Other")
    )
    write_config(
        root,
        site(member="topic", literal="topic"),
        site(
            klass="Other",
            member="path",
            literal="path",
            verdict="unreachable",
            reason="measured: no SQL caller reaches it.",
        ),
    )
    assert exit_code(check_option_message_names) == 0
    out = capsys.readouterr().out
    assert "2 check(s) that name a setting:" in out
    assert re.search(r"^\s+1\s+same$", out, re.MULTILINE)
    assert re.search(r"^\s+0\s+restated$", out, re.MULTILINE)
    assert re.search(r"^\s+1\s+unreachable$", out, re.MULTILINE)
