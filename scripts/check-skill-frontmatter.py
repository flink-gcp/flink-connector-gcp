# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml>=6"]
# ///
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
"""Hold every `.claude/skills/*/SKILL.md` to frontmatter that is strict YAML.

**What this does and does not claim.** It enforces a house style, not Claude
Code's own requirement. `self-review-round-two` shipped with an unquoted
`description:` containing a `: `, which ends a YAML plain scalar — PyYAML
rejects that file outright, and yet Claude Code loaded the skill and kept the
whole description, verbatim to its last word. Measured by reading a live
session's skill listing built from that file (ADR-0069). So Claude Code's reader
is the *more tolerant* of the two, and nothing here can tell you what it would
reject; what it can tell you is that a file is ambiguous or unparseable by the
ordinary meaning of its own format, which is worth refusing on a directory whose
whole purpose is to be read by tools.

The reason to bother: a frontmatter that any reader disagrees about fails
silently. The file stays valid markdown, no build step reads it, markdownlint's
globs exclude it, and a skill that did not load looks exactly like a skill
Claude chose not to use.

Three rules, none of which needs an allowlist, because a frontmatter either
parses or it does not:

* it is a `---` delimited YAML mapping, it **parses**, and no key is
  duplicated — with PyYAML rather than a hand-rolled approximation, because a
  second, diverging parser in the tree is exactly the failure this repository
  has paid for elsewhere. PyYAML arrives through PEP 723 metadata in this
  file's own header, which is why this one script runs as
  `uv run --no-project scripts/…` and why the repository's uv project is
  untouched by it;
* `name` is present and equals the skill's directory, which is how the skill is
  invoked, so a mismatch is a lie about how to reach it;
* `description` is present and non-empty, since that is what Claude selects on.

A ` #` in an unquoted value is worth knowing about specifically: to YAML it
starts a comment, so the value is *truncated* rather than rejected. Whether
Claude Code truncates the same way is unmeasured — the point is that the two
readings differ, which is the ambiguity being refused.

Deliberately not checked: the description's wording or length. That is the part
with judgment in it, and judgment is what this check has none of.

**One known blind spot, tracked rather than papered over.** The block is the
text between the opening `---` and the next `---` line, which is what every
frontmatter reader does — so a file whose *closing* delimiter was deleted takes
the next `---` in the body as its close, and the prose in between is parsed as
frontmatter. Where that prose happens to parse and carry a `name` and a
`description`, this reports the file clean. It is latent while no skill body
contains a `---` line; the first one that documents frontmatter or uses a
horizontal rule arms it. Every tightening considered was unsound (an allowlist
of known keys false-positives on whatever Claude Code adds next; a line budget
is arbitrary), which is why it is recorded here and on #388 instead of
guessed at.
"""

from __future__ import annotations

import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKILLS = ROOT / ".claude" / "skills"


class _UniqueKeyLoader(yaml.SafeLoader):
    """A loader that refuses a duplicated key instead of letting the last one win.

    YAML permits it and PyYAML resolves it silently — under `safe_load` the last
    one wins — so a second `description:`, the shape an edit leaves behind when a
    line is copied rather than replaced, would take effect with nothing said.
    Which of the two *Claude Code* would keep is not something this has measured;
    what it can say is that the file is then ambiguous, and that is enough to
    reject it. A subclass rather than a second pinned linter and its
    configuration.
    """

    def construct_mapping(self, node, deep=False):
        # Two kinds of key are skipped rather than scanned, and both are about not
        # making this loader *stricter* than the parser it wraps. An unhashable
        # key — a flow sequence, which is how a markdown link reference definition
        # parses — is the base class's ConstructorError to raise, not a TypeError
        # to crash the checker on. A merge key is resolved by the flatten_mapping
        # that super() performs, so scanning it here would reject a document
        # PyYAML accepts.
        seen = set()
        for key_node, _ in node.value:
            if not isinstance(key_node, yaml.ScalarNode):
                continue
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate key {key!r}", key_node.start_mark
                )
            seen.add(key)
        return super().construct_mapping(node, deep)


def _frontmatter(text: str) -> str | None:
    """Returns the frontmatter block, or None when the file has no delimited one."""
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    return text[4:end]


def check(skills_dir: pathlib.Path) -> list[str]:
    """Returns one message per problem found, empty when every skill is loadable."""
    problems: list[str] = []
    paths = sorted(skills_dir.glob("*/SKILL.md"))
    if not paths:
        return [f"no SKILL.md found under {skills_dir}: the layout changed"]
    # A directory whose SKILL.md was renamed or deleted is the same silent
    # failure as one that will not parse — the skill simply stops existing, and
    # the per-file loop below cannot see a file that is not there.
    for directory in sorted(d for d in skills_dir.iterdir() if d.is_dir()):
        if not (directory / "SKILL.md").is_file():
            problems.append(f"{directory}: a skill directory with no SKILL.md")
    for path in paths:
        directory = path.parent.name
        raw = _frontmatter(path.read_text(encoding="utf-8"))
        if raw is None:
            problems.append(f"{path}: no `---` delimited frontmatter block")
            continue
        try:
            data = yaml.load(raw, Loader=_UniqueKeyLoader)
        except yaml.YAMLError as e:
            reason = str(e).splitlines()[0]
            # The cure differs, and naming the wrong one sends a reader looking
            # for a quoting problem in a file that has none.
            cure = (
                "Remove the repeat: YAML keeps the last one silently."
                if "duplicate key" in reason
                else "A `: ` or a ` #` in an unquoted value ends the scalar there — wrap the"
                " whole value in double quotes."
            )
            problems.append(
                f"{path}: the frontmatter is not valid YAML ({reason}). {cure}"
            )
            continue
        if not isinstance(data, dict):
            problems.append(f"{path}: the frontmatter is not a mapping")
            continue
        if "name" not in data:
            problems.append(f"{path}: name is missing")
        elif data["name"] != directory:
            problems.append(
                f"{path}: name is {data['name']!r} but the skill is invoked as {directory!r};"
                " they must match"
            )
        if not str(data.get("description") or "").strip():
            problems.append(f"{path}: description is missing or empty")
    return problems


def main() -> int:
    problems = check(SKILLS)
    for problem in problems:
        print(f"::error::{problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"skills with a loadable frontmatter: {len(list(SKILLS.glob('*/SKILL.md')))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
