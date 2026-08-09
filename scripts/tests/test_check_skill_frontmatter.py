#
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
"""Tests for scripts/check-skill-frontmatter.py (ADR-0069).

Synthetic trees in tmp_path, per this repository's rule for checker tests — a
real-tree assertion would put .claude/skills/ under every path filter that
matters, and `just lint` already runs the checker over the real tree.

The direction that matters is the checker quietly finding *less* than it
should, because a skill that fails to load looks exactly like a skill Claude
chose not to use. So each rule is pinned by a case that fails when the rule is
removed, and the first of them is the real defect this checker was written for:
an unquoted description carrying a `: `.
"""

import textwrap


def _skill(root, name, frontmatter, body="# Body\n"):
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(
        f"---\n{frontmatter}\n---\n\n{body}", encoding="utf-8"
    )
    return directory / "SKILL.md"


def _good(root, name="alpha"):
    return _skill(
        root, name, f"name: {name}\ndescription: Does a thing. Use when a thing is due."
    )


def test_a_loadable_skill_passes(check_skill_frontmatter, tmp_path):
    _good(tmp_path)

    assert check_skill_frontmatter.check(tmp_path) == []


def test_an_unquoted_description_carrying_a_colon_fails(
    check_skill_frontmatter, tmp_path
):
    # The real defect: `: ` ends a YAML plain scalar, so the frontmatter stops
    # parsing and the skill silently never loads. This happened to a mandatory
    # review skill and nothing reported it.
    _skill(
        tmp_path,
        "alpha",
        "name: alpha\ndescription: Lenses point outward rather than at the diff: the user, the"
        " operator.",
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not valid YAML" in problems[0]
    # The failure names the fix, since this check has no allowlist and one cure.
    assert "double quotes" in problems[0]


def test_the_same_description_passes_once_quoted(check_skill_frontmatter, tmp_path):
    _skill(
        tmp_path,
        "alpha",
        'name: alpha\ndescription: "Lenses point outward rather than at the diff: the user, the'
        ' operator."',
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_a_description_that_is_only_a_comment_marker_fails(
    check_skill_frontmatter, tmp_path
):
    # ` #` starts a YAML comment, so the value is silently truncated rather than
    # rejected. Where nothing survives the truncation the non-empty rule catches
    # it; where something does, the skill loads advertising half of itself and
    # only review catches that — which is why the guidance is to quote, not to
    # lean on this case.
    _skill(tmp_path, "alpha", "name: alpha\ndescription: #376 is the only trigger.")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "description" in problems[0]


def test_a_name_that_does_not_match_its_directory_fails(
    check_skill_frontmatter, tmp_path
):
    # The directory is how the skill is invoked, so a mismatch means the name in
    # the file is a lie about how to reach it.
    _skill(tmp_path, "alpha", "name: beta\ndescription: Does a thing.")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "'beta'" in problems[0] and "'alpha'" in problems[0]


def test_a_missing_description_fails(check_skill_frontmatter, tmp_path):
    _skill(tmp_path, "alpha", "name: alpha")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "description" in problems[0]


def test_an_empty_description_fails(check_skill_frontmatter, tmp_path):
    _skill(tmp_path, "alpha", 'name: alpha\ndescription: "   "')

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "description" in problems[0]


def test_a_file_without_frontmatter_fails(check_skill_frontmatter, tmp_path):
    directory = tmp_path / "alpha"
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text("# No frontmatter here\n", encoding="utf-8")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    # The exact rule, not just the word: several of the messages here say
    # "frontmatter", so a looser assertion passes against the wrong one — and a
    # count of them in this comment would be one more thing to keep in step.
    assert "no `---` delimited frontmatter" in problems[0]


def test_frontmatter_that_is_not_a_mapping_fails(check_skill_frontmatter, tmp_path):
    _skill(tmp_path, "alpha", "- name: alpha\n- description: Does a thing.")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a mapping" in problems[0]


def test_an_empty_skills_directory_fails_rather_than_reporting_clean(
    check_skill_frontmatter, tmp_path
):
    # A checker that finds nothing must not report success: the layout moving is
    # exactly how this would stop covering anything.
    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "no SKILL.md found" in problems[0]


def test_every_skill_is_reported_not_only_the_first(check_skill_frontmatter, tmp_path):
    _good(tmp_path, "alpha")
    _skill(tmp_path, "beta", "name: wrong\ndescription: Does a thing.")
    _skill(tmp_path, "gamma", "name: gamma")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 2


def test_the_body_is_not_parsed(check_skill_frontmatter, tmp_path):
    # Only the frontmatter is YAML; a `---` or a colon in the prose must not
    # trip the check, or every skill that documents YAML would fail.
    _skill(
        tmp_path,
        "alpha",
        "name: alpha\ndescription: Does a thing.",
        body=textwrap.dedent(
            """\
            # Body

            ---

            Note: this line has a colon, and the rule above is a horizontal rule.
            """
        ),
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_a_duplicated_key_fails(check_skill_frontmatter, tmp_path):
    # YAML allows it and keeps the last one, so a copied-rather-than-replaced
    # line takes effect with nothing said. This is the one rule a general YAML
    # linter would have added over a plain safe_load.
    _skill(tmp_path, "alpha", "name: alpha\ndescription: One.\ndescription: Two.")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "duplicate key" in problems[0]
    # ...and the cure named is the one for a duplicate, not the quoting advice.
    assert "Remove the repeat" in problems[0]
    assert "double quotes" not in problems[0]


def _raw_skill(root, name, text):
    """Writes a SKILL.md verbatim; the cases below are about the delimiters."""
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(text, encoding="utf-8")
    return directory / "SKILL.md"


def test_a_frontmatter_with_no_closing_delimiter_fails(
    check_skill_frontmatter, tmp_path
):
    # The opening-delimiter branch had a test; this one did not, and it is the
    # likelier typo — the closer is the line an edit runs past.
    _raw_skill(tmp_path, "alpha", "---\nname: alpha\ndescription: A thing.\n")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "no `---` delimited frontmatter" in problems[0]


def test_dashes_inside_a_value_are_reported(check_skill_frontmatter, tmp_path):
    # Measured against Claude Code 2.1.223 (2026-08-09): the loader ends the
    # frontmatter at the first `---` *anywhere*, so this file — both delimiters
    # present, nothing else wrong with it — loads advertising "INLINEDESC
    # before", a sentence its author never wrote. No other rule here would
    # notice: it parses, the name matches, the description is non-empty.
    _raw_skill(
        tmp_path,
        "alpha",
        "---\nname: alpha\ndescription: INLINEDESC before --- after.\n---\n\n# Body\n",
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a delimiter line" in problems[0]
    # Quoting is the advice every other message here gives, and the one thing
    # that cannot help: the block is cut out before any YAML is parsed.
    assert "Take the `---` out" in problems[0]
    assert "double quotes" not in problems[0]


def test_a_value_ending_in_dashes_is_reported(check_skill_frontmatter, tmp_path):
    # The dashes end the line, so what follows them *is* a line break and only
    # the "did the block end at a line start" half of the rule rejects this.
    _raw_skill(
        tmp_path, "alpha", "---\nname: alpha\ndescription: A thing ---\n---\n\n# Body\n"
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a delimiter line" in problems[0]


def test_a_body_rule_of_four_dashes_does_not_close_the_block(
    check_skill_frontmatter, tmp_path
):
    # #388's shape: the closing delimiter is gone and a body rule stands in for
    # it. The swallowed prose is a `#` line, which YAML reads as a comment, so
    # the block parses and carries the right name and description — which is why
    # this reported clean until the loader's rule was measured.
    _raw_skill(
        tmp_path,
        "alpha",
        "---\nname: alpha\ndescription: A thing.\n\n# Heading\n\n----\n\nBody.\n",
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a delimiter line" in problems[0]
    # No delimiter line exists anywhere in this file, so the cure is that one.
    assert "Add the closing" in problems[0]


def test_a_body_line_that_merely_starts_with_dashes_does_not_close_the_block(
    check_skill_frontmatter, tmp_path
):
    _raw_skill(
        tmp_path,
        "alpha",
        "---\nname: alpha\ndescription: A thing.\n\n# Heading\n\n--- not a delimiter\n",
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a delimiter line" in problems[0]


def test_a_body_rule_of_exactly_three_dashes_is_still_reported_clean(
    check_skill_frontmatter, tmp_path
):
    # The residue of #388, pinned so that closing it later is a deliberate act
    # rather than an accident. Claude Code stops at this line too (measured
    # 2.1.223, 2026-08-09: the skill loaded with the name and description below
    # and lost only the body above the rule), so the file is not misdescribed,
    # and telling a horizontal rule from a delimiter needs judgment this has
    # none of.
    _raw_skill(
        tmp_path,
        "alpha",
        "---\nname: alpha\ndescription: A thing.\n\n# Heading\n\n---\n\nBody.\n",
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_an_empty_block_is_reported_as_not_a_mapping(check_skill_frontmatter, tmp_path):
    # Broken either way, but by the right name: the delimiters here are both
    # real, and a message about the closing `---` "not being a delimiter line"
    # would send its reader looking at the one part of this file that is fine.
    _raw_skill(tmp_path, "alpha", "---\n---\n\n# Body\n")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not a mapping" in problems[0]


def test_delimiters_carrying_trailing_whitespace_are_accepted(
    check_skill_frontmatter, tmp_path
):
    # Claude Code tolerates them, and a checker stricter than the loader calls a
    # working skill broken.
    _raw_skill(
        tmp_path, "alpha", "--- \nname: alpha\ndescription: A thing.\n---\t\n\n# Body\n"
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_crlf_line_endings_are_accepted(check_skill_frontmatter, tmp_path):
    # A skill written on Windows loads, so it must not be reported broken. This
    # case only discriminates because check() reads bytes: through read_text the
    # CRLF is translated away before the delimiters are matched, and the mutation
    # batch is what caught it — with the translating read, dropping `\r?` from
    # both patterns changed nothing and this test passed either way.
    _raw_skill(
        tmp_path,
        "alpha",
        "---\r\nname: alpha\r\ndescription: A thing.\r\n---\r\n\r\n# Body\r\n",
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_the_cure_is_chosen_correctly_on_a_crlf_file(check_skill_frontmatter, tmp_path):
    # The delimiter line this looks for to pick between the two cures has to
    # allow the `\r` as well, or a Windows file gets told to add a closing
    # delimiter it already has — advice that would leave the description
    # truncated and the reader hunting for a delimiter that is right there.
    _raw_skill(
        tmp_path,
        "alpha",
        "---\r\nname: alpha\r\ndescription: A --- thing.\r\n---\r\n\r\n# Body\r\n",
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "Take the `---` out" in problems[0]


def test_a_closing_delimiter_at_end_of_file_is_accepted(
    check_skill_frontmatter, tmp_path
):
    # A skill with no body yet: the close is the last thing in the file, with no
    # newline after it.
    _raw_skill(tmp_path, "alpha", "---\nname: alpha\ndescription: A thing.\n---")

    assert check_skill_frontmatter.check(tmp_path) == []


def test_a_block_that_does_not_start_the_file_is_not_frontmatter(
    check_skill_frontmatter, tmp_path
):
    # Frontmatter is frontmatter because it is first, and a delimited block
    # further down is prose — reading it as metadata would invent a description
    # out of the body.
    _raw_skill(
        tmp_path, "alpha", "# Body\n\n---\nname: alpha\ndescription: A thing.\n---\n"
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "no `---` delimited frontmatter" in problems[0]


def test_a_missing_name_fails(check_skill_frontmatter, tmp_path):
    # Every other case supplies a name, so the "name is missing" branch was
    # reachable only through a file nothing tested.
    _skill(tmp_path, "alpha", "description: A thing.")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "name is missing" in problems[0]


def test_an_unhashable_key_is_reported_rather_than_crashing(
    check_skill_frontmatter, tmp_path
):
    # A markdown link reference definition parses as a flow sequence, so this is
    # reachable from prose. Scanning it for duplicates raised TypeError, which is
    # not a YAMLError and so escaped the handler: a traceback and no ::error::.
    _skill(tmp_path, "alpha", "name: alpha\ndescription: A thing.\n[a, b]: v")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "not valid YAML" in problems[0]


def test_a_merge_key_is_accepted_as_pyyaml_accepts_it(
    check_skill_frontmatter, tmp_path
):
    # The duplicate-key rule must not make this loader stricter than the parser
    # it wraps: safe_load resolves `<<`, so rejecting it would be this checker
    # inventing a rule of its own.
    _skill(
        tmp_path,
        "alpha",
        "name: alpha\ndescription: A thing.\nbase: &b\n  x: 1\nchild:\n  <<: *b\n  y: 2",
    )

    assert check_skill_frontmatter.check(tmp_path) == []


def test_every_problem_names_its_file(check_skill_frontmatter, tmp_path):
    # With seven skills, a message that does not name the file cannot be acted
    # on; asserting only the count would not notice the prefix going away.
    _skill(tmp_path, "beta", "name: wrong\ndescription: A thing.")
    _skill(tmp_path, "gamma", "name: gamma")

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 2
    assert any("beta/SKILL.md" in p for p in problems)
    assert any("gamma/SKILL.md" in p for p in problems)


def test_main_returns_one_and_annotates_when_a_skill_is_broken(
    check_skill_frontmatter, tmp_path, monkeypatch, capsys
):
    # main() had no test at all, so `return 1` could become `return 0` and
    # `just lint` would report success on a broken repository with the suite
    # fully green — the silent failure this checker exists to prevent.
    _skill(tmp_path, "alpha", "name: alpha\ndescription: Ends at the diff: here.")
    monkeypatch.setattr(check_skill_frontmatter, "SKILLS", tmp_path)

    assert check_skill_frontmatter.main() == 1
    assert "::error::" in capsys.readouterr().err


def test_main_returns_zero_on_a_clean_tree(
    check_skill_frontmatter, tmp_path, monkeypatch, capsys
):
    _good(tmp_path)
    monkeypatch.setattr(check_skill_frontmatter, "SKILLS", tmp_path)

    assert check_skill_frontmatter.main() == 0
    assert "::error::" not in capsys.readouterr().err


def test_a_skill_directory_with_no_skill_md_fails(check_skill_frontmatter, tmp_path):
    # A renamed or deleted SKILL.md removes the skill just as silently as a
    # frontmatter that will not parse, and the per-file loop cannot see it.
    _good(tmp_path, "alpha")
    (tmp_path / "beta").mkdir()
    (tmp_path / "beta" / "SKILLS.md").write_text(
        "---\nname: beta\n---\n", encoding="utf-8"
    )

    problems = check_skill_frontmatter.check(tmp_path)

    assert len(problems) == 1
    assert "no SKILL.md" in problems[0]
    assert "beta" in problems[0]
