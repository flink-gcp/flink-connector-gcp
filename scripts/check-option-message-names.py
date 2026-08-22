#!/usr/bin/env python3
#
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
"""Hold a rejection to naming what the caller typed (ADR-0127, issue #1028).

ADR-0127 says a configured value is checked where it is configured, and that
where the check runs decides what the failure can name. Nothing enforced it, so
the same defect was found six times by reading — #235, #895, #984/#920/#976,
#1009/#1013, #1019, #1027 — three of them after the ADR was written.

The detector that worked, twice, was a grep: find the name-taking validators'
call sites whose *name argument is a string literal*, then ask whether a SQL
caller can reach that check. #1027 says so in as many words. This is that grep,
with the second half made mechanical.

Every such call site is classified by one `[[sites]]` entry in
scripts/config/option-message-names.toml, under one of three verdicts:

* `same` — the literal is one of the DDL keys that module declares, so a SQL
  caller reading the message reads a word they could have typed.
* `restated` — the table layer checks the value again under each key the entry
  names. Held by finding that key at a `.key()`-named validator call under the
  module's `table` package.
* `unreachable` — no SQL caller reaches this check. The only verdict this script
  cannot verify, so it is the only one that must argue for itself in prose.

A call site with no entry fails: a new check that names a setting is a decision,
and this is where it gets made. An entry matching no call site fails too, the
rule check-flink-api-tiers.toml applies to its allowlist.

Its boundary — which calls it reads, and what a verdict does and does not prove
— is recorded in ADR-0127 along with the measurements behind it. Three limits
are worth repeating where someone editing this file will meet them:

* The population is the three shared validators in VALIDATORS, not every
  rejection that names something. A check written as a bare
  `Preconditions.checkArgument(…, "kmsKeyName must not be blank")` is invisible
  here. `assert_population_is_reachable` below is what keeps that boundary from
  moving without anyone noticing.
* `same` compares the literal against the *set* of keys the module declares, not
  against the key that reaches this particular setter. Which key reaches which
  setter is the reachability this script deliberately does not compute, so a
  module that grows a second key spelled like an existing one can satisfy `same`
  wrongly. Only a test holds that.
* `restated` finds the check, not the path to it. A restatement that is present
  but no longer reached — a private helper left behind when its caller went back
  to reading the option raw — still counts. Measured: reverting the call alone
  reads clean, and deleting the orphaned helper with it is what fails.
* The population is call sites, not options. A *new* `ConfigOption` routed into
  an already-classified setter does not fail here.

Exit codes: 0 clean, 1 policy violation (an unclassified call site, a verdict the
sources contradict, a dead entry), 2 infrastructure or config authoring error
(missing file, unparsable call, malformed config).

Standard library only, like its siblings in this directory. The Java lexing
primitives come from java_example_regions, as check-javadoc-links.py takes them.
"""

import re
import sys
from pathlib import Path

from java_example_regions import line_at, skip_quoted, skip_text_block

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-option-message-names`, or any python3 "
        ">= 3.11. CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
CONFIG = Path(__file__).resolve().parent / "config" / "option-message-names.toml"

SKILL = ".agents/skills/curate-option-message-names/"

# The validators that take the caller-facing name as an argument. ADR-0127
# consolidated six private copies into the first two; the third is its shape 2.
# Each takes the name last, which is what the argument walk below relies on.
VALIDATORS = (
    "ResourceNames.checkComponent",
    "ResourceNames.checkNotBlank",
    "EmulatorEndpoint.parse",
)

# Broad, so every call site is found; the argument walk is the narrow half, and a
# call it cannot parse is reported rather than skipped. The qualifier is required
# because a private same-named helper (BigtableDynamicTableFactory.checkNotBlank,
# HttpTargetSerializationSchema.checkNotBlank) is a different method with a
# different contract. Requiring it means a *static import* would hide a call
# site, which is why assert_population_is_reachable refuses one.
CALL = re.compile(r"\b(" + "|".join(re.escape(name) for name in VALIDATORS) + r")\s*\(")

STATIC_IMPORT = re.compile(
    r"^import\s+static\s+[\w.]*\b(?:"
    + "|".join(sorted({re.escape(name.split(".")[0]) for name in VALIDATORS}))
    + r")\.[\w*]+\s*;",
    re.MULTILINE,
)

# A name argument is either a string literal *entire* — the shape this check
# exists to classify — or an expression that reaches a `ConfigOption`'s key.
#
# The literal must be the whole argument, because a key can be composed with one:
# `"an entry of " + …ALLOWED_REGIONS.key()` names a key and would otherwise be
# read as naming a literal. The key form is matched with whitespace removed
# outright, because Spotless wraps a long qualified constant across three lines
# (`BigQueryConnectorOptions\n    .SINK_FILE_LOADS_TEMP_DATASET\n    .key()`) and
# no whitespace inside such an expression carries meaning.
STRING_ARGUMENT = re.compile(r'^"((?:\\.|[^"\\])*)"$')
KEY_ARGUMENT = re.compile(r"([\w$]+)\.key\(\)")

# A local that holds a key, so a name composed from it several times reads once:
# `String key = …ALLOWED_REGIONS.key();` then `"an entry of " + key`. Matched over
# the enclosing method with its whitespace collapsed to single spaces, which is
# what lets a Spotless-wrapped qualified constant still be one expression while
# `String key` stays two tokens.
ALIAS = re.compile(r"(\w+)\s*=\s*(?:[\w$]+\s*\.\s*)*([\w$]+)\s*\.\s*key\(\s*\)")

# `for (ConfigOption<String> option : …)`, whose header names the options the
# loop runs over. The header is where the binding is; the body is not.
FOR_EACH = re.compile(r"for\s*\(\s*(?:final\s+)?[\w.$<>?@,\s\[\]]+?\s+(\w+)\s*:")

JAVA_IDENTIFIER = re.compile(r"[A-Za-z_$][\w$]*")
CONSTANT_NAME = re.compile(r"\b([A-Z][A-Z0-9_]*)\b")

# `public static final ConfigOption<List<String>> SINK_X = ConfigOptions.key("x")`,
# wrapped anywhere Spotless likes. The generic argument may itself be generic.
CONFIG_OPTION = re.compile(
    r"ConfigOption<(?:[^<>]|<[^<>]*>)*>\s+(\w+)\s*=\s*"
    r"ConfigOptions\s*\.\s*key\(\s*\"([^\"]+)\"",
    re.DOTALL,
)

# A method or constructor declaration, up to its opening brace. Anchored at a
# line start with leading whitespace, so a call expression cannot match. The body
# is then found by brace matching, which is what gives a call site its enclosing
# member.
#
# The return type is optional so a constructor — `TableDestination(String p) {`,
# with or without a modifier — is a member too. Without that, a check moved into
# a package-private constructor became "sits in no method body", which is a wrong
# diagnosis of a natural place to put one.
METHOD = re.compile(
    r"^[ \t]+(?:(?:@[\w.]+(?:\([^\n]*\))?|public|private|protected|static|final"
    r"|abstract|synchronized|native|default|strictfp)\s+)*"
    r"(?:<[^<>]*(?:<[^<>]*>)?[^<>]*>\s+)?"
    r"(?:[\w.$]+(?:<(?:[^<>]|<[^<>]*>)*>)?(?:\[\])*\s+)?"
    r"(\w+)\s*\(([^;{)]*)\)\s*(?:throws\s+[\w.,\s]+?)?\{",
    re.MULTILINE,
)

# The declared name of one parameter — the last identifier of `ConfigOption<String>
# option` or `final ReadableConfig config`. Which of a method's parameters a
# `.key()` names is what separates a helper from an ordinary check, and *which*
# parameter it is decides which argument helper_keys reads — so the list has to
# be split on generic nesting rather than on every comma. See split_parameters.
PARAMETER = re.compile(r"(\w+)\s*$")

# A `<` opens a generic only after a type name — `Map<`, `HashMap<`, `List<List<`.
# After anything else it is a comparison, and counting it would merge arguments.
TYPE_TAIL = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$>")

# `METHOD`'s return type is optional so a constructor is a member, which also
# lets `if (enabled) {` match as a declaration named `if`. A call inside one
# would then be attributed to `if` rather than to its setter, and two such
# blocks would look like two declarations of it.
CONTROL_KEYWORDS = frozenset(
    {
        "if",
        "for",
        "while",
        "switch",
        "catch",
        "synchronized",
        "try",
        "do",
        "else",
        "finally",
        "return",
        "new",
        "case",
        "record",
        "yield",
    }
)

VERDICTS = ("same", "restated", "unreachable")


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def blank_comments(source: str) -> tuple[str, list[tuple[int, int]]]:
    """Blank every comment and text block, preserving newlines and columns.

    String literals are kept intact, unlike in check-flink-api-tiers.py: the name
    argument this script reads *is* a string literal, so blanking them would
    leave every call site looking unparsable. They are matched first and kept, so
    a `//` inside one cannot be read as a comment opener — the same mechanism
    check-option-docs.py and check-metric-docs.py use.

    A text block is blanked rather than kept, because its content is prose that
    may hold anything, including a `"` that would desynchronise the scan that
    follows. Nothing in these trees calls a validator from inside one.

    Returns the blanked text *and* the spans of the literals it kept, because
    both are needed and deriving the second by a second scan means two scanners
    that must agree about text blocks. `String d = "ResourceNames.checkNotBlank(";`
    is why the spans are needed at all: without them that literal was read as a
    call whose parenthesis never closes.
    """
    out: list[str] = []
    kept: list[tuple[int, int]] = []
    index, length = 0, len(source)
    while index < length:
        character = source[index]
        if source.startswith('"""', index):
            end = skip_text_block(source, index)
            out.append(re.sub(r"[^\n]", " ", source[index:end]))
            index = end
        elif character in "\"'":
            end = skip_quoted(source, index, character)
            kept.append((index, end))
            out.append(source[index:end])
            index = end
        elif source.startswith("//", index):
            end = source.find("\n", index)
            end = length if end < 0 else end
            out.append(" " * (end - index))
            index = end
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            end = length if end < 0 else end + 2
            out.append(re.sub(r"[^\n]", " ", source[index:end]))
            index = end
        else:
            out.append(character)
            index += 1
    return "".join(out), kept


def inside_string(spans: list[tuple[int, int]], offset: int) -> bool:
    return any(start <= offset < end for start, end in spans)


def strip_literals(text: str) -> str:
    """The text with every string and character literal removed.

    An argument's *prose* is not code: `"an entry of the key list" + label`
    holds the word `key`, and reading identifiers out of it made a message that
    mentions a local's name resolve to that local rather than to what it names.
    """
    out: list[str] = []
    index, length = 0, len(text)
    while index < length:
        if text[index] in "\"'":
            index = skip_quoted(text, index, text[index])
            continue
        out.append(text[index])
        index += 1
    return "".join(out)


def arguments(
    text: str, opening: int, generics: bool = True
) -> tuple[list[str], int] | None:
    """Split one call's top-level arguments; None if its parentheses never close.

    `opening` is the index of the `(`. String and character literals are skipped
    while walking, so a `,` or `)` inside a message cannot end an argument early.

    Brackets are matched against their own kind rather than counted together: a
    `}` must not close a `(`. Counting one depth let a call missing its closing
    parenthesis be closed by the method's own brace, so the argument list parsed
    "successfully" as `["value", "\\"topic\\";"]` and the site vanished from the
    population instead of being reported.
    """
    pairs = {")": "(", "]": "[", "}": "{"}
    stack: list[str] = []
    # Generic depth is counted separately from the bracket stack and never ends
    # the call: `<` is only sometimes a bracket in Java, so letting it terminate
    # anything would be unsound. It decides one thing — whether a comma is top
    # level — so `new HashMap<String, String>()` is one argument.
    #
    # It is a heuristic, and `checkNotBlank(length<1 ? a : b, "topic")` is where
    # it is wrong: `<` after an identifier reads as generic, the comma is not
    # split, and the name argument stops looking like a literal — so the call
    # site would *disappear* rather than be reported. Which is why the caller
    # asks for `generics=False` when testing for a literal: that question needs
    # no generic awareness, and answering it without one cannot be fooled.
    generic = 0
    index, length = opening, len(text)
    start = opening + 1
    parts: list[str] = []
    while index < length:
        character = text[index]
        if character in "\"'":
            index = skip_quoted(text, index, character)
            continue
        if character in "([{":
            stack.append(character)
        elif character in pairs:
            if not stack or stack[-1] != pairs[character]:
                return None
            stack.pop()
            if not stack:
                parts.append(text[start:index])
                return parts, index + 1
        elif generics and character == "<" and index and text[index - 1] in TYPE_TAIL:
            generic += 1
        elif character == ">" and generic:
            generic -= 1
        elif character == "," and len(stack) == 1 and not generic:
            parts.append(text[start:index])
            start = index + 1
        index += 1
    return None


class Member:
    """One method or constructor body: its name, parameters and extent."""

    def __init__(
        self, name: str, parameters: list[str], paren: int, start: int, end: int
    ):
        self.name = name
        self.parameters = parameters
        # The offset of the declaration's own `(`, so a scan for calls to this
        # member can skip the declaration by an equality rather than by a range.
        # A range is only meaningful inside the declaring file, and comparing one
        # against offsets in a sibling file skips whichever call happens to land
        # in it — which is how a test meant to pin the file scope stopped being
        # able to fail.
        self.paren = paren
        self.start = start
        self.end = end

    def body(self, text: str) -> str:
        return text[self.start : self.end]


def members(text: str) -> list[Member]:
    """Every method body in one source, over `text`.

    Nested declarations are included, and a call belongs to the innermost body
    that contains it — which for a lambda inside a method is still the method,
    since a lambda carries no declaration of its own.
    """
    found: list[Member] = []
    for match in METHOD.finditer(text):
        depth, index, length = 0, match.end() - 1, len(text)
        while index < length:
            character = text[index]
            if character in "\"'":
                index = skip_quoted(text, index, character)
                continue
            if character == "{":
                depth += 1
            elif character == "}":
                depth -= 1
                if depth == 0:
                    if match.group(1) in CONTROL_KEYWORDS:
                        break
                    parameters = [
                        name.group(1)
                        for part in split_parameters(match.group(2))
                        if (name := PARAMETER.search(part.strip()))
                    ]
                    found.append(
                        Member(
                            match.group(1),
                            parameters,
                            match.start(2) - 1,
                            match.end(),
                            index,
                        )
                    )
                    break
            index += 1
    return found


def split_parameters(declared: str) -> list[str]:
    """Split a parameter list on its top-level commas, not on every comma.

    `Map<String, ConfigOption<String>> byKey` is one parameter. Splitting naively
    made it two, which put a phantom `String` in the list: the arity guard then
    never matched a real call, so every key that helper checked went uncredited
    and its verdict failed for a restatement that was there. Angle brackets are
    generics here — a parameter list holds no comparison operator.
    """
    parts: list[str] = []
    depth, start = 0, 0
    for index, character in enumerate(declared):
        if character in "<([":
            depth += 1
        elif character in ">)]":
            depth -= 1
        elif character == "," and depth == 0:
            parts.append(declared[start:index])
            start = index + 1
    parts.append(declared[start:])
    return [part for part in parts if part.strip()]


def enclosing(spans: list[Member], offset: int) -> Member | None:
    """The innermost method body containing `offset`."""
    containing = [span for span in spans if span.start <= offset < span.end]
    return (
        min(containing, key=lambda span: span.end - span.start) if containing else None
    )


def main_source_trees() -> list[Path]:
    """Every module's main source roots — the one place this location is spelled.

    `java*` rather than `java`, because `java-flink1` / `java-flink2` hold the
    per-major seam (ADR-0054). Nothing there calls a validator today; reading
    them anyway is what keeps one from being invisible if something ever does.
    """
    return sorted(ROOT.glob("*/src/main/java*"))


class Site:
    """One literal-named validator call site, and where it was found."""

    def __init__(
        self,
        module: str,
        path: str,
        klass: str,
        member: str,
        literal: str,
        line: int,
        declared_at: int,
    ):
        self.module = module
        self.path = path
        self.klass = klass
        self.member = member
        self.literal = literal
        self.line = line
        # Which declaration the call sits in, so two calls in *one* body are not
        # mistaken for two overloads. One verdict answers both of those, and the
        # repair the collision message gives — rename one of them — does not
        # exist for a single method that checks a value twice.
        self.declared_at = declared_at

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (self.module, self.klass, self.member, self.literal)

    def __str__(self) -> str:
        return f'{self.path}:{self.line}: {self.klass}.{self.member}("{self.literal}")'


def is_table_layer(relative: str) -> bool:
    """Whether a source sits in a module's Table API package.

    That is where a SQL caller's value is configured, so it is the only place a
    restatement can answer one — ADR-0127's "a check runs where the value is
    configured". A factory, an options mapper and a lookup runtime all qualify.
    """
    return "/table/" in relative


def assert_population_is_reachable(relative: str, text: str) -> None:
    """Refuse a static import of a validator, which would hide its call sites.

    CALL requires the qualifier, so `import static …ResourceNames.checkNotBlank;`
    plus a bare `checkNotBlank(value, "tempDataset")` would leave that site out
    of the population with nothing to report: no unclassified site, no dead
    entry, a clean tally. The one guard against the population silently
    shrinking is that this shape cannot appear.
    """
    match = STATIC_IMPORT.search(text)
    if match:
        infra(
            f"{relative}:{line_at(text, match.start())}: a validator is statically "
            f"imported, so its calls carry no qualifier and this script cannot "
            f"find them — the population would shrink with nothing to report. "
            f"Qualify the calls, or teach CALL the unqualified form and give it a "
            f"way to tell them from the private helpers of the same name."
        )


def resolve(
    argument: str, member: Member, text: str, offset: int, constants: dict[str, str]
) -> tuple[str | None, object]:
    """How a non-literal name argument reaches a key, and which key(s).

    Returns `(resolution, payload)`, or `(None, reason)` when nothing named can
    be found — which is reported rather than guessed at, with the reason in the
    message so "two loops bind this name" does not read the same as "loops are
    not supported at all". For `helper` the payload is the *index* of the
    parameter the name comes from, so only the argument in that position is
    read; for the rest it is the option constants named.
    """
    tokens = KEY_ARGUMENT.findall("".join(argument.split()))
    named = [token for token in tokens if token in constants]
    if named:
        return "constant", named
    if tokens:
        token = tokens[-1]
        if token in member.parameters:
            return "helper", member.parameters.index(token)
        bound, why = loop_constants(text, member, token, offset, constants)
        if bound is not None:
            return "loop", bound
        return None, why
    # A local standing in for a key, which is how a name built from one more than
    # once is written. Read per method, so two methods may each hold a `key`.
    aliases: dict[str, set[str]] = {}
    for alias, token in ALIAS.findall(" ".join(member.body(text).split())):
        aliases.setdefault(alias, set()).add(token)
    borrowed = [
        name
        for name in JAVA_IDENTIFIER.findall(strip_literals(argument))
        if name in aliases
    ]
    if not borrowed:
        return None, (
            "it names no option constant, no parameter, no loop variable and no "
            "local this method assigns from a key"
        )
    resolved: list[str] = []
    for name in borrowed:
        if len(aliases[name]) != 1:
            # Reassigned in the same method, so which key it holds depends on
            # where the call sits. A dict of the last write would answer every
            # use with the same key, wrong in both directions from one edit.
            return None, (
                f"the local `{name}` it names is assigned from more than one "
                f"key in this method, so which one it holds depends on where "
                f"the call sits"
            )
        resolved.append(next(iter(aliases[name])))
    if not all(token in constants for token in resolved):
        return (
            None,
            "the local it names is assigned from something that is not an option",
        )
    return "constant", resolved


def loop_constants(
    text: str, member: Member, token: str, offset: int, constants: dict[str, str]
) -> tuple[list[str] | None, str]:
    """The option constants bound by the for-each loop this call sits *inside*.

    Not every loop in the method that happens to bind the same name. A method
    may hold `for (ConfigOption<String> option : asList(A))` that validates and a
    second `for (ConfigOption<String> option : asList(B))` that only logs;
    aggregating the headers credited B as restated, so deleting B's real check
    elsewhere would have stayed green. Found by independent review.

    `(None, reason)` when the call sits inside no such loop, or inside more than
    one — both are reported rather than guessed at, and the reason separates
    them, because "loops are unsupported" and "this call is in two of them" want
    different repairs.
    """
    enclosing_headers: list[str] = []
    for match in re.finditer(
        rf"for\s*\(\s*(?:final\s+)?[\w.$<>?@,\[\]\s]+?\s+{re.escape(token)}\s*:",
        text[member.start : member.end],
    ):
        start = member.start + match.start()
        parsed = arguments(text, text.index("(", start))
        if parsed is None:
            continue
        header, after = parsed
        body_end = loop_body_end(text, after)
        if body_end is None or not after <= offset < body_end:
            continue
        enclosing_headers.append(" ".join(header))
    if len(enclosing_headers) > 1:
        return None, (
            f"the call sits inside {len(enclosing_headers)} for-each loops that "
            f"each bind `{token}`, so which one supplies its key is ambiguous"
        )
    if not enclosing_headers:
        return None, f"`{token}` is bound by no for-each loop this call sits inside"
    found = [
        name
        for name in CONSTANT_NAME.findall(strip_literals(enclosing_headers[0]))
        if name in constants
    ]
    if not found:
        return None, f"the for-each loop binding `{token}` runs over no option constant"
    return found, ""


def loop_body_end(text: str, after: int) -> int | None:
    """Where the loop body starting at `after` ends: its `}`, or its `;`."""
    index = after
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text):
        return None
    if text[index] != "{":
        # Lexed, not `find(";")`: blank_comments deliberately keeps string
        # literals, so `LOG.warn("a;b", check(...))` ended the body before the
        # call and the run then said the call sat inside no loop — a false
        # statement about correct Java, in a message a reader would act on.
        depth = 0
        while index < len(text):
            character = text[index]
            if character in "\"'":
                index = skip_quoted(text, index, character)
                continue
            if character in "([{":
                depth += 1
            elif character in ")]}":
                depth -= 1
            elif character == ";" and depth == 0:
                return index + 1
            index += 1
        return None
    depth = 0
    while index < len(text):
        character = text[index]
        if character in "\"'":
            index = skip_quoted(text, index, character)
            continue
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return None


def restated_keys(
    calls: list[tuple[str, str, object, Member]],
    layer: dict[str, tuple[str, list[Member]]],
    constants: dict[str, str],
) -> set[str]:
    """Which DDL keys a module's table layer checks under their own name.

    The token before `.key()` decides how the key is found, and every shape in
    these trees resolves to a *named* option rather than to a guess:

    * an option constant — `…SINK_LOCATION.key()`, or a local assigned from one —
      names exactly the options it spells.
    * a local bound by a for-each header — `for (ConfigOption<String> option :
      Arrays.asList(A, B))` — names the options the loop runs over, read from the
      header. The body is not the binding and is not read.
    * a *parameter* of the enclosing method — `option.key()` inside
      `notBlankUnderItsKey(ReadableConfig, ConfigOption<String> option)` — makes
      that method a helper, so the keys are whatever its callers hand it.

    A token that fits none of them is reported rather than guessed at. An earlier
    draft ended in "otherwise, every constant named anywhere in the method", and
    that was a false pass with teeth: adding one unrelated `SINK_LOCATION`
    mention to `validateEmulatorEndpoints` made `sink.location` read as restated,
    so deleting the factory's actual `sink.location` check — the thing this
    record exists to hold — left the run clean. Measured on this branch.
    """
    found: set[str] = set()
    for path, resolution, payload, member in calls:
        if resolution == "helper":
            text, spans = layer[path]
            found |= helper_keys(path, text, spans, member, payload, constants)
        else:
            found |= {constants[token] for token in payload if token in constants}
    return found


def helper_keys(
    path: str,
    text: str,
    spans: list[Member],
    helper: Member,
    position: int,
    constants: dict[str, str],
) -> set[str]:
    """The keys a helper's callers hand it, from the argument it names them by.

    Scoped four ways, and every one of them was a false pass before it was added:

    * to the file that declares the helper — `check`, `map` and `settings` recur
      across these table packages, and without it an unrelated `check(...)` in a
      sibling class donated its argument as a key this helper had checked;
    * to a name declared **once** in that file. Calls are matched by name and
      arity, and two overloads share both, so a call to the *other* one donated
      its argument — and the other one's own declaration was read as a call, with
      any capitalised type in its parameter list becoming a key. A name this
      script cannot tell apart is reported rather than resolved;
    * to calls of the helper's declared arity;
    * to the argument in the *position* of the parameter the `.key()` names.
      Reading every argument credited `check(config.get(SINK_LOCATION), TOPIC)`
      with both keys, so a `sink.location` verdict passed on a check whose
      failure says `topic`.
    """
    declarations = [span for span in spans if span.name == helper.name]
    if len(declarations) != 1:
        infra(
            f"{path}:{line_at(text, helper.paren)}: `{helper.name}` names the "
            f"setting of a check under one of its own parameters, and this file "
            f"declares {len(declarations)} methods with that name. Calls are "
            f"matched by name and arity, which cannot tell overloads apart, so "
            f"the keys it checks would be a guess. Rename one, or see {SKILL}."
        )
    found: set[str] = set()
    for call in re.finditer(rf"(?<![\w$.]){re.escape(helper.name)}\s*\(", text):
        if call.end() - 1 == helper.paren:
            continue
        parsed = arguments(text, call.end() - 1)
        if parsed is None or len(parsed[0]) != len(helper.parameters):
            continue
        for constant in CONSTANT_NAME.findall(strip_literals(parsed[0][position])):
            if constant in constants:
                found.add(constants[constant])
    return found


def option_constants(module: str) -> dict[str, str]:
    """`ConfigOption` constant name -> DDL key, for one module's table layer.

    Read from every `*ConnectorOptions.java` under the module rather than from a
    configured path, so a second options class is picked up without an edit here.
    Two classes declaring the same constant name is refused rather than resolved
    by filename order, which would credit the wrong key.
    """
    found: dict[str, str] = {}
    origin: dict[str, str] = {}
    for tree in main_source_trees():
        if tree.relative_to(ROOT).parts[0] != module:
            continue
        for source in sorted(tree.rglob("*ConnectorOptions.java")):
            relative = str(source.relative_to(ROOT))
            text, _ = blank_comments(source.read_text(encoding="utf-8"))
            declared = CONFIG_OPTION.findall(text)
            if not declared:
                infra(
                    f"{relative} matches *ConnectorOptions.java but declares no "
                    f"ConfigOption this script recognises. Either it is not an "
                    f"options class, or CONFIG_OPTION no longer matches the shape, "
                    f"which would make every key lookup wrong."
                )
            for constant, key in declared:
                if constant in found and found[constant] != key:
                    infra(
                        f"{relative}: {constant} is also declared in "
                        f"{origin[constant]}, under a different key. This script "
                        f"resolves a constant name to one key per module, so the "
                        f"two would silently take whichever file sorts last."
                    )
                found[constant] = key
                origin[constant] = relative
    return found


def scan(
    constants_by_module: dict[str, dict[str, str]],
) -> tuple[list[Site], dict[str, set[str]]]:
    """Every literal-named call site, and every key restated in a table layer."""
    sites: list[Site] = []
    # module -> relative path -> (blanked text, its members), table layer only.
    # Retained because resolving a helper means looking at the calls in the file
    # that declares it, and at whether its name is declared there more than once.
    layers: dict[str, dict[str, tuple[str, list[Member]]]] = {}
    # module -> (path, resolution, payload, enclosing member)
    keyed: dict[str, list[tuple[str, str, list[str], Member]]] = {}
    trees = main_source_trees()
    if not trees:
        infra("no */src/main/java* source root exists; the layout changed.")
    for tree in trees:
        module = tree.relative_to(ROOT).parts[0]
        constants = constants_by_module.get(module, {})
        for source in sorted(tree.rglob("*.java")):
            relative = str(source.relative_to(ROOT).as_posix())
            text, literals = blank_comments(source.read_text(encoding="utf-8"))
            assert_population_is_reachable(relative, text)
            spans = members(text)
            if is_table_layer(relative):
                layers.setdefault(module, {})[relative] = (text, spans)
            for match in CALL.finditer(text):
                if inside_string(literals, match.start()):
                    continue
                parsed = arguments(text, match.end() - 1)
                if parsed is None:
                    infra(
                        f"{relative}:{line_at(text, match.start())}: the call to "
                        f"{match.group(1)} has no closing parenthesis this script "
                        f"can find, so its name argument cannot be read. Every "
                        f"other call's verdict is untrustworthy while one cannot "
                        f"be parsed."
                    )
                argument = " ".join(parsed[0][-1].split())
                # The literal test asks a question generic awareness cannot help
                # with, and a wrong `<` guess would hide the site entirely.
                plain = arguments(text, match.end() - 1, generics=False)
                plain_argument = (
                    " ".join(plain[0][-1].split()) if plain is not None else argument
                )
                where = enclosing(spans, match.start())
                if where is None:
                    infra(
                        f"{relative}:{line_at(text, match.start())}: the call to "
                        f"{match.group(1)} sits in no method or constructor body "
                        f"this script recognises, so it cannot be classified. "
                        f"Teach METHOD the declaration shape it is in; leaving it "
                        f"out would drop this call from the population silently."
                    )
                literal = STRING_ARGUMENT.match(
                    plain_argument
                ) or STRING_ARGUMENT.match(argument)
                if literal:
                    sites.append(
                        Site(
                            module,
                            relative,
                            source.stem,
                            where.name,
                            literal.group(1),
                            line_at(text, match.start()),
                            where.start,
                        )
                    )
                    continue
                # Resolved everywhere, including outside the table layer. A name
                # that is neither a literal nor traceable to an option was once
                # skipped there — so `parse(value, NAME)`, with a private
                # constant holding "emulatorEndpoint", left the population with
                # nothing to report and no verdict to write. Only what the name
                # *restates* is confined to the table layer, below.
                resolution, payload = resolve(
                    argument, where, text, match.start(), constants
                )
                if resolution is None:
                    infra(
                        f"{relative}:{line_at(text, match.start())}: the call to "
                        f"{match.group(1)} names its setting with `{argument}`, "
                        f"which is neither a string literal nor an expression this "
                        f"script can trace to a `ConfigOption`: {payload}. Reading "
                        f"it as a guess is how a deleted check once still read "
                        f"clean, so it is reported instead: see {SKILL}."
                    )
                if not is_table_layer(relative):
                    # It resolves, but a check outside the table layer restates
                    # nothing a SQL caller reads.
                    continue
                keyed.setdefault(module, []).append(
                    (relative, resolution, payload, where)
                )

    restated: dict[str, set[str]] = {}
    for module, calls in keyed.items():
        restated[module] = restated_keys(
            calls, layers.get(module, {}), constants_by_module.get(module, {})
        )
    return sites, restated


def load_config() -> list[dict]:
    """The parsed `[[sites]]` entries, their fields checked so a typo is exit 2."""
    if not CONFIG.is_file():
        infra(f"{CONFIG} is missing.")
    try:
        config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        infra(f"{CONFIG.name} is not valid TOML: {error}")
    unknown = set(config) - {"sites"}
    if unknown:
        # A typo'd table name would otherwise sit ignored while its call sites
        # get reported as unclassified — fail on the typo, which is the fixable
        # end.
        infra(f"{CONFIG.name} has unknown top-level entries: {sorted(unknown)}.")
    entries = config.get("sites")
    if not entries:
        infra(f"{CONFIG.name} names no [[sites]] entry.")
    if not isinstance(entries, list) or not all(
        isinstance(entry, dict) for entry in entries
    ):
        # `[sites]` rather than `[[sites]]`. Without this the loop below walks a
        # dict's keys and dies on a traceback, which exits 1 — a policy verdict
        # for what is an authoring mistake.
        infra(f"{CONFIG.name} writes sites as a table; it must be [[sites]] entries.")
    fields = {"module", "class", "member", "literal", "verdict", "keys", "reason"}
    for entry in entries:
        missing = [
            field
            for field in ("module", "class", "member", "literal", "verdict")
            if not isinstance(entry.get(field), str) or not entry[field].strip()
        ]
        if missing:
            infra(f"a [[sites]] entry in {CONFIG.name} lacks {', '.join(missing)}.")
        name = f"{entry['module']}:{entry['class']}.{entry['member']}"
        stray = sorted(set(entry) - fields)
        if stray:
            # An entry is a claim somebody checked; a field nothing reads is a
            # claim nothing carries.
            infra(
                f"the [[sites]] entry for {name} in {CONFIG.name} has fields "
                f"nothing reads: {stray}."
            )
        if entry["verdict"] not in VERDICTS:
            infra(
                f"the [[sites]] entry for {name} in {CONFIG.name} has verdict "
                f"{entry['verdict']!r}; it must be one of {', '.join(VERDICTS)}."
            )
        if entry["verdict"] == "restated":
            keys = entry.get("keys")
            if (
                not isinstance(keys, list)
                or not keys
                or not all(isinstance(key, str) and key.strip() for key in keys)
            ):
                infra(
                    f"the [[sites]] entry for {name} in {CONFIG.name} is restated "
                    f"but names no keys. A restated verdict is a claim about which "
                    f"DDL keys reach this check; list them."
                )
        elif entry.get("keys") is not None:
            infra(
                f"the [[sites]] entry for {name} in {CONFIG.name} is "
                f"{entry['verdict']} but carries keys, which only a restated "
                f"verdict is read for."
            )
        if entry["verdict"] == "unreachable" and (
            not isinstance(entry.get("reason"), str) or not entry["reason"].strip()
        ):
            infra(
                f"the [[sites]] entry for {name} in {CONFIG.name} is unreachable "
                f"but gives no reason. It is the one verdict this script cannot "
                f"check, so the reason is the whole of it."
            )
        elif entry["verdict"] != "unreachable" and entry.get("reason") is not None:
            infra(
                f"the [[sites]] entry for {name} in {CONFIG.name} is "
                f"{entry['verdict']} but carries a reason, which only an "
                f"unreachable verdict is read for. The sources argue the other two."
            )
    return entries


def judge(
    site: Site, entry: dict, constants: dict[str, str], restated: set[str]
) -> list[str]:
    """What the sources say about one classified call site."""
    keys = set(constants.values())
    if entry["verdict"] == "unreachable":
        return []
    if entry["verdict"] == "same":
        if site.literal in keys:
            return []
        return [
            (
                f"{site}: classified `same`, but `{site.literal}` is not a key "
                f"{site.module}'s *ConnectorOptions.java declares, so no SQL "
                f"caller types it. Correct the verdict — `restated` if the "
                f"table layer checks it under its own key, `unreachable` with "
                f"a reason if no SQL caller reaches this check at all."
            )
        ]
    problems = []
    for key in entry["keys"]:
        if key not in keys:
            problems.append(
                f"{site}: classified `restated` under `{key}`, which "
                f"{site.module}'s *ConnectorOptions.java does not declare. "
                f"Correct the key, or the verdict."
            )
        elif key not in restated:
            problems.append(
                f"{site}: classified `restated` under `{key}`, but nothing in "
                f"{site.module}'s table package checks a value with "
                f"`{key}`'s ConfigOption. A SQL caller who wrote `{key}` would "
                f'be answered "{site.literal} …" instead. Restate the check in '
                f"the table layer, or correct the verdict; see {SKILL}."
            )
    return problems


def main() -> int:
    entries = load_config()
    modules = sorted({tree.relative_to(ROOT).parts[0] for tree in main_source_trees()})
    # Read once per module rather than once per call site: every lookup below
    # wants the same dict, and building it here is also what lets scan() resolve
    # a token to a constant while it walks.
    constants_by_module = {module: option_constants(module) for module in modules}
    sites, restated = scan(constants_by_module)
    problems: list[str] = []
    # Which entries actually did something. An entry that never fires is a claim
    # nobody can check, and it accumulates silently — the rule
    # check-flink-api-tiers.toml applies to its allowlist, and check-option-docs
    # shipped four dead entries before it did the same.
    used: set[tuple[str, str, str, str]] = set()

    by_identity: dict[tuple[str, str, str, str], dict] = {}
    for entry in entries:
        identity = (entry["module"], entry["class"], entry["member"], entry["literal"])
        if identity in by_identity:
            infra(
                f"{CONFIG.name} classifies {entry['class']}.{entry['member']}"
                f'("{entry["literal"]}") in {entry["module"]} twice.'
            )
        by_identity[identity] = entry

    # One verdict cannot answer two *declarations*. Nothing collapses today (37
    # sites, 37 identities), but two overloads of one setter both rejecting under
    # the same literal would silently share an entry — so a new SQL-reachable
    # overload would inherit an `unreachable` verdict instead of demanding its
    # own. Found by independent review.
    #
    # Two calls in one body are deliberately not a collision: `checkNotBlank`
    # then `checkComponent` on the same value, or one per branch of an if, are
    # one setting and one verdict answers both.
    seen: dict[tuple[str, str, str, str], Site] = {}
    for site in sites:
        if (
            site.identity in seen
            and seen[site.identity].declared_at != site.declared_at
        ):
            infra(
                f"{site}: this and {seen[site.identity]} are two checks with one "
                f"identity, so one [[sites]] verdict would answer both. Give the "
                f"members distinct names, or teach the identity what separates "
                f"them; see {SKILL}."
            )
        seen[site.identity] = site

    for site in sites:
        entry = by_identity.get(site.identity)
        if entry is None:
            problems.append(
                f"{site}: a check that names a setting, with no verdict in "
                f'{CONFIG.name}. Add a [[sites]] entry saying whether "'
                f'{site.literal}" is what a SQL caller types (`same`), is '
                f"restated under a DDL key in the table layer (`restated`), or "
                f"cannot be reached from SQL at all (`unreachable`). "
                f"See {SKILL}."
            )
            continue
        used.add(site.identity)
        problems += judge(
            site,
            entry,
            constants_by_module.get(site.module, {}),
            restated.get(site.module, set()),
        )

    for identity in sorted(set(by_identity) - used):
        module, klass, member, literal = identity
        problems.append(
            f'[[sites]] entry {klass}.{member}("{literal}") in {module} never '
            f"fires: {CONFIG.name} classifies a call site that is not there. "
            f"Delete it — an allowlist entry that forgives nothing is a claim "
            f"nobody can check."
        )

    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        fail(f"\n{len(problems)} problem(s) between the checks and their names.")

    tally = {verdict: 0 for verdict in VERDICTS}
    for entry in entries:
        tally[entry["verdict"]] += 1
    print(f"{len(sites)} check(s) that name a setting:")
    for verdict in VERDICTS:
        print(f"  {tally[verdict]:>3}  {verdict}")
    print(
        f"  every entry fires, or this would have failed; only `unreachable` is "
        f"unverified here (see {CONFIG.name})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
