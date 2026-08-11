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
"""Hold every `.agents/skills/*/SKILL.md` to frontmatter that is strict YAML.

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

**Where the block ends is Claude Code's answer, not this script's** (#388).
Measured against 2.1.223 on 2026-08-09, by loading deliberately malformed skills
through `--plugin-dir` beside a well-formed control and reading back the
descriptions a session was given: the loader ends the frontmatter at the **first
`---` anywhere** after the opening line — not at the first `---` *line*. A body
rule of `----`, a line reading `--- not a delimiter`, and a `---` sitting
mid-sentence inside a comment all closed it, and a `---` inside a `description:`
value truncated that description at the dashes while the skill still loaded,
advertising a sentence its author never wrote. The control arm loaded intact in
the same run, which is what lets the rest of the column mean anything.

So this delimits the block the way that reader does, and then asks one question
of its own: **is the `---` it stopped at a delimiter line?** A close that is not
alone on its line means the loader ends the block somewhere the author did not
write one — the truncated description above, or a body rule standing in for a
deleted closing delimiter — and both are rejected here, with the cure that fits
whichever it is. There is nothing arbitrary in that rule and no list to keep:
the delimiter either is one or is not.

That question is **deliberately stricter than the loader**, and it is the only
place this file is: everything else here tracks the loader's tolerance, because
a check stricter than the loader calls a working skill broken. A `-----` typed
into the closing line loads with its description intact — measured in the same
run — and is reported here anyway. What the strictness buys is the case above
it, where the loader loads something quietly other than what the file says; a
rule that fired only where the skill was already unloadable would not have
caught that one at all.

**What remains, stated rather than papered over.** A file whose closing
delimiter was deleted and whose body contains a line that is *exactly* `---`
still reports clean, because the loader stops there too, and the skill really
does load with the name and description its author wrote — what it loses is the
part of its *body* above that line, which invoking one such skill and reading
back its content confirmed rather than inferred. Distinguishing a horizontal rule from a
closing delimiter needs judgment this check does not have (an allowlist of known
keys false-positives on whatever Claude Code adds next; a line budget is
arbitrary), and the file is not, by the measurement, misdescribed. That residue
is the whole of #388 that survives it.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys
import tomllib

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKILLS = ROOT / ".agents" / "skills"
AGENT_GUIDANCE_LIMIT = 32 * 1024
PROJECT_MEMORY_ROUTE = (
    "Before planning non-trivial repository work, use `$project-memory`"
)


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


# The opening line, what may follow a close, and a delimiter line anywhere
# later. The `\A` is belt and braces with the `.match()` below, which anchors on
# its own: frontmatter is frontmatter because it comes first, and a delimited
# block further down a file is prose. `\r?` throughout because the file is read
# as bytes — see the read in check(), which is what makes these reachable.
_OPENING = re.compile(r"\A---[ \t]*\r?\n")
_AFTER_CLOSE = re.compile(r"[ \t]*(?:\r?\n|\Z)")
_DELIMITER_LINE = re.compile(r"^---[ \t]*\r?$", re.MULTILINE)


def _frontmatter(text: str) -> tuple[str | None, str | None]:
    """Returns the block Claude Code reads, or the reason it cannot be trusted.

    Exactly one half of the pair is ever set. The `find("---")` is the loader's
    rule as measured, dashes anywhere and not only at a line start (see the
    module docstring); the delimiter-line test after it is this script's own,
    and is what makes a stray `---` reportable rather than silently obeyed.
    """
    opening = _OPENING.match(text)
    if opening is None:
        return None, "no `---` delimited frontmatter block"
    close = text.find("---", opening.end())
    if close == -1:
        return None, "no `---` delimited frontmatter block"
    block = text[opening.end() : close]
    if (not block or block.endswith("\n")) and _AFTER_CLOSE.match(text, close + 3):
        return block, None
    # The cure differs by which half is wrong, and quoting — the advice for
    # every other message here — is the one thing that cannot help: the block is
    # cut out of the file before any YAML is parsed.
    cure = (
        "Take the `---` out of the frontmatter; quotes will not hold it."
        if _DELIMITER_LINE.search(text, close + 3)
        else "Add the closing `---` delimiter."
    )
    line = text.count("\n", 0, close) + 1
    return None, (
        f"Claude Code ends the frontmatter at the first `---` in the file, on line {line},"
        f" which is not a delimiter line — so the block it loads is not the one written here."
        f" {cure}"
    )


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
        # Bytes, not read_text: that translates CRLF to LF before this sees the
        # file, so the checker would be answering about a normalised copy the
        # loader never reads — and every `\r` branch above would be dead code
        # with a test that cannot tell whether it is there.
        raw, problem = _frontmatter(path.read_bytes().decode("utf-8"))
        if problem is not None:
            problems.append(f"{path}: {problem}")
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


def check_openai_metadata(skills_dir: pathlib.Path) -> list[str]:
    """Return problems with Codex's UI metadata for the canonical skills."""
    problems: list[str] = []
    for skill_dir in sorted(path.parent for path in skills_dir.glob("*/SKILL.md")):
        path = skill_dir / "agents" / "openai.yaml"
        if not path.is_file():
            problems.append(f"{path}: Codex skill metadata is missing")
            continue
        try:
            data = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
        except yaml.YAMLError as e:
            problems.append(
                f"{path}: metadata is not valid YAML ({str(e).splitlines()[0]})"
            )
            continue
        interface = data.get("interface") if isinstance(data, dict) else None
        if not isinstance(interface, dict):
            problems.append(f"{path}: interface mapping is missing")
            continue
        for key in ("display_name", "short_description", "default_prompt"):
            if not str(interface.get(key) or "").strip():
                problems.append(f"{path}: interface.{key} is missing or empty")
        prompt = str(interface.get("default_prompt") or "")
        invocation = re.compile(rf"(?<![\w-])\${re.escape(skill_dir.name)}(?![\w-])")
        if invocation.search(prompt) is None:
            problems.append(
                f"{path}: interface.default_prompt must mention ${skill_dir.name}"
            )
    return problems


def check_repository_layout(root: pathlib.Path) -> list[str]:
    """Return problems with the shared Codex/Claude repository layout."""
    problems: list[str] = []
    canonical = root / ".agents" / "skills"
    compatibility = root / ".claude" / "skills"
    if not compatibility.is_symlink():
        problems.append(f"{compatibility}: must be a symlink to ../.agents/skills")
    elif compatibility.readlink() != pathlib.Path("../.agents/skills"):
        problems.append(
            f"{compatibility}: points to {compatibility.readlink()}, expected ../.agents/skills"
        )
    elif compatibility.resolve() != canonical.resolve():
        problems.append(f"{compatibility}: does not resolve to {canonical}")

    memory_skill = canonical / "project-memory" / "SKILL.md"
    if not memory_skill.is_file():
        problems.append(
            f"{memory_skill}: the shared local-memory bridge skill is missing"
        )

    root_guidance = root / "AGENTS.md"
    if not root_guidance.is_file():
        problems.append(f"{root_guidance}: repository guidance is missing")
        return problems
    guidance_lines = root_guidance.read_text(encoding="utf-8").splitlines()
    if not any(line.startswith(PROJECT_MEMORY_ROUTE) for line in guidance_lines):
        problems.append(f"{root_guidance}: must route agents to $project-memory")
    guidance_files = sorted(
        path
        for path in root.rglob("AGENTS.md")
        if not any(
            part.startswith(".") or part == "target"
            for part in path.relative_to(root).parts
        )
    )
    for guidance in guidance_files:
        scoped = [root_guidance]
        if guidance != root_guidance:
            scoped.extend(
                parent / "AGENTS.md"
                for parent in guidance.parents
                if parent != root and (parent / "AGENTS.md").is_file()
            )
        combined = sum(path.stat().st_size for path in dict.fromkeys(scoped))
        if combined > AGENT_GUIDANCE_LIMIT:
            problems.append(
                f"{guidance}: root plus scoped guidance is {combined} bytes, over the"
                f" {AGENT_GUIDANCE_LIMIT}-byte Codex default"
            )
        wrapper = guidance.with_name("CLAUDE.md")
        imports = (
            []
            if not wrapper.is_file()
            else [
                line.strip()
                for line in wrapper.read_text(encoding="utf-8").splitlines()
            ]
        )
        if "@AGENTS.md" not in imports:
            problems.append(f"{wrapper}: must import @AGENTS.md for Claude Code")
    return problems


def check_mcp_config(root: pathlib.Path) -> list[str]:
    """Return syntax and cross-client problems in the project MCP configuration."""
    problems: list[str] = []
    codex_path = root / ".codex" / "config.toml"
    claude_path = root / ".mcp.json"
    try:
        codex = tomllib.loads(codex_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as e:
        return [f"{codex_path}: cannot load Codex MCP configuration ({e})"]
    try:
        claude = json.loads(claude_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return [f"{claude_path}: cannot load Claude MCP configuration ({e})"]

    codex_servers = codex.get("mcp_servers", {})
    claude_servers = claude.get("mcpServers", {})
    if set(codex_servers) != {"context7", "serena"}:
        problems.append(
            f"{codex_path}: MCP servers must be exactly context7 and serena"
        )
    if set(claude_servers) != {"context7", "serena"}:
        problems.append(
            f"{claude_path}: MCP servers must be exactly context7 and serena"
        )
    for servers, path in ((codex_servers, codex_path), (claude_servers, claude_path)):
        context7 = servers.get("context7", {})
        if context7.get("url") != "https://mcp.context7.com/mcp":
            problems.append(
                f"{path}: context7 must use the anonymous remote MCP endpoint"
            )
        serena = servers.get("serena", {})
        args = serena.get("args", [])
        required = (
            "serena-agent==1.7.0",
            "--project-from-cwd",
            "--add-mode",
            "no-memories",
        )
        if serena.get("command") != "mise" or any(
            value not in args for value in required
        ):
            problems.append(
                f"{path}: serena must use pinned 1.7.0 from the cwd in no-memories mode"
            )
    codex_args = codex_servers.get("serena", {}).get("args", [])
    claude_args = claude_servers.get("serena", {}).get("args", [])
    if "--context=codex" not in codex_args:
        problems.append(f"{codex_path}: serena must use the codex context")
    if "--context=claude-code" not in claude_args:
        problems.append(f"{claude_path}: serena must use the claude-code context")
    return problems


def main() -> int:
    problems = [
        *check(SKILLS),
        *check_openai_metadata(SKILLS),
        *check_repository_layout(ROOT),
        *check_mcp_config(ROOT),
    ]
    for problem in problems:
        print(f"::error::{problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"skills with a loadable frontmatter: {len(list(SKILLS.glob('*/SKILL.md')))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
