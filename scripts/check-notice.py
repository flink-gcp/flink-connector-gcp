#!/usr/bin/env python3
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
"""Generate and verify a shaded module's META-INF/NOTICE and META-INF/licenses/.

The prose of a NOTICE is human-written and lives in the module's NOTICE.template;
everything mechanical is generated from it:

  - each `{{Licence Name}}` placeholder becomes the sorted bullet list of the
    bundled artifacts license-maven-plugin resolved to that licence (names as
    normalised by the licenseMerges in the root POM);
  - every artifact whose licence is not Apache-2.0 must have an entry in
    scripts/licence-sources.toml, which pins where its licence text comes from
    (the artifact's own jar where it ships one, an https URL otherwise) and the
    sha256 of that text. --update materialises those files; the check verifies
    the checked-in files still hash to the recorded values.

Modes:
    check-notice.py <module>            offline check (CI): regenerate the NOTICE
                                        in memory and fail on any difference from
                                        the checked-in one, or on any licence file
                                        missing, unpinned, tampered or orphaned
    check-notice.py --update <module>   rewrite META-INF/NOTICE and
                                        META-INF/licenses/ (fetches url sources)

Both read target/generated-sources/license/THIRD-PARTY.txt, which the
`just check-notice` / `just update-notice` recipes regenerate first.

A fetched text must hash to the pinned sha256 — an upstream edit is a failure a
human reviews, never something silently shipped. HTML responses are rejected
outright: several POM-declared licence URLs serve web pages, which is how
unreviewed content would otherwise sneak in. GITHUB_TOKEN (or `gh auth token`)
is sent when available; raw.githubusercontent.com needs neither.

Standard library only, deliberately: nothing here justifies a package manager.
"""

import argparse
import hashlib
import re
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-notice <module>`, or any python3 "
        ">= 3.11. CI installs one with actions/setup-python."
    )

SOURCES = Path(__file__).parent / "licence-sources.toml"

# `    (Licence Name) Artifact Description (groupId:artifactId:version - url)`
THIRD_PARTY_LINE = re.compile(
    r"^\s+\((?P<licence>.+?)\)\s+.*\((?P<ga>[\w.\-]+:[\w.\-]+):(?P<version>[\w.+\-]+)\s"
)
# The report's own count. Compared against what parsed, because a *partial*
# parse is the dangerous direction: an artifact the regex cannot read is simply
# absent, and nothing then demands the NOTICE mention it.
THIRD_PARTY_COUNT = re.compile(r"^Lists of (?P<count>\d+) third-party dependencies")

PLACEHOLDER = re.compile(r"^\{\{(?P<group>.+)\}\}$")

# Every group except this one must carry licence texts in META-INF/licenses/.
TEXT_EXEMPT_GROUP = "Apache-2.0"

# This project is Apache-2.0 with no usage restrictions of its own, so a
# dependency under a restrictive licence — the GPL family, or the newer
# source-available and non-commercial ones — is normally not adopted at all
# rather than recorded in a NOTICE. This gate exists to force that discussion,
# not to encode its outcome: it fails before anyone can write a template
# paragraph, and the message says to decide adoption first. Matched against the
# resolved licence names (post-merge). A licence name this misses is still
# caught structurally — no template paragraph, hard failure — this is the
# sharper message for the families known to be a problem. The one exemption is
# dual-licensed *with the classpath exception*, taken under CDDL: the
# combination deliberately in the bundle today.
# The token alternation deliberately does not end at a word boundary: Maven's
# most common spellings are `GPLv2`/`LGPLv3`, where a trailing \b never matches
# (the next character is a word character), and the spelled-out names carry no
# GPL token at all — both misses were measured against real licence strings.
RESTRICTED = re.compile(
    r"General Public License"  # GNU GPL/LGPL/AGPL spelled out
    r"|\b(GPL|AGPL|LGPL|SSPL|BUSL|RSAL)"  # tokens, incl. GPLv2 / GPL-2.0 / GPL 2
    r"|Business Source|Commons Clause|Elastic License|Server Side Public"
    r"|Non-?Commercial",
    re.IGNORECASE,
)
# Keyed by the merged licence *name*, which means any artifact resolving to this
# exact dual-licence string rides the exemption — acceptable because the string
# itself names the terms (classpath exception, CDDL alternative), and a second
# artifact under the same terms would get the same answer.
RESTRICTED_EXEMPT = {"CDDL + GPLv2 with classpath exception"}


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def read_resolved(module: Path) -> dict[str, str]:
    """Return {groupId:artifactId:version -> merged licence name} from the report."""
    report = module / "target" / "generated-sources" / "license" / "THIRD-PARTY.txt"
    if not report.is_file():
        fail(
            f"{report} is missing. Run `just check-notice {module.name}` (or "
            f"`just update-notice`), which regenerates it first."
        )
    resolved: dict[str, str] = {}
    declared = None
    for line in report.read_text(encoding="utf-8").splitlines():
        header = THIRD_PARTY_COUNT.match(line)
        if header:
            declared = int(header["count"])
        match = THIRD_PARTY_LINE.match(line)
        if match:
            resolved[f"{match['ga']}:{match['version']}"] = match["licence"]
    if declared is None:
        fail(f"Found no dependency count in {report}; its format has changed.")
    if declared != len(resolved):
        fail(
            f"{report} says {declared} dependencies but {len(resolved)} could be "
            f"parsed. Every unparsed one is an artifact nothing would require "
            f"META-INF/NOTICE to list, so this is a hard failure rather than a "
            f"partial check. Widen THIRD_PARTY_LINE in {Path(__file__).name}."
        )
    return resolved


def load_sources() -> dict[str, dict]:
    with SOURCES.open("rb") as handle:
        files = tomllib.load(handle)["files"]
    owners: dict[str, str] = {}
    for name, entry in files.items():
        for ga in entry["artifacts"]:
            if ga in owners:
                fail(f"{SOURCES}: {ga} appears under both {owners[ga]} and {name}.")
            owners[ga] = name
    return files


def render_notice(
    template: Path, resolved: dict[str, str], files: dict[str, dict]
) -> str:
    """Fill each {{Licence}} placeholder with its sorted, pointered bullet list."""
    owners = {ga: name for name, entry in files.items() for ga in entry["artifacts"]}
    by_group: dict[str, list[str]] = {}
    for gav, licence in resolved.items():
        by_group.setdefault(licence, []).append(gav)

    lines: list[str] = []
    rendered_groups: set[str] = set()
    for line in template.read_text(encoding="utf-8").splitlines():
        match = PLACEHOLDER.match(line.strip())
        if not match:
            lines.append(line)
            continue
        group = match["group"]
        if group not in by_group:
            fail(
                f"{template} has a paragraph for '{group}' but no bundled artifact "
                f"resolves to it. Remove the paragraph, or fix licenseMerges."
            )
        if group in rendered_groups:
            fail(f"{template} has two paragraphs for '{group}'; merge them.")
        rendered_groups.add(group)
        for gav in sorted(by_group[group]):
            ga = gav.rsplit(":", 1)[0]
            owner = owners.get(ga)
            if group == TEXT_EXEMPT_GROUP:
                lines.append(f"- {gav}")
            elif owner is None:
                fail(
                    f"{gav} is bundled under '{group}', which requires its licence "
                    f"text in META-INF/licenses/ — no entry in {SOURCES} covers "
                    f"{ga}. Curate one, in this order:\n"
                    f"  1. a licence file inside the artifact's own jar (jar:)\n"
                    f"  2. the publisher's repository at the tag matching the "
                    f"bundled version\n"
                    f"  3. the publisher's repository head, only if it is frozen "
                    f"(archived) or no version tag exists — record why in the note\n"
                    f"  4. there is no rung 4. A generic template is not this "
                    f"project's text (the copyright line is part of a BSD or MIT "
                    f"licence), so if no publisher-provided text can be pinned, "
                    f"question the dependency itself rather than substitute one."
                )
            else:
                lines.append(f"- {gav} (META-INF/licenses/{owner})")

    missing = set(by_group) - rendered_groups
    if missing:
        fail(
            f"Bundled artifacts resolve to licences the template has no paragraph "
            f"for: {sorted(missing)}. Add a paragraph with a "
            f"{{{{placeholder}}}} to {template}."
        )
    return "\n".join(lines) + "\n"


def github_token() -> str | None:
    import os

    if os.environ.get("GITHUB_TOKEN"):
        return os.environ["GITHUB_TOKEN"]
    try:
        out = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return out.stdout.strip() or None
    except (OSError, subprocess.TimeoutExpired):
        return None


def obtain_text(name: str, entry: dict, module: Path) -> bytes:
    """Fetch or extract one licence text and verify it against its pinned sha256."""
    if "jar" in entry:
        classpath = module / "target" / "runtime-classpath.txt"
        if not classpath.is_file():
            fail(f"{classpath} is missing; run the build first.")
        ga = entry["artifacts"][0]
        artifact_id = ga.split(":")[1]
        # Matched on the repository layout (…/<artifactId>/<version>/<artifactId>-….jar),
        # not on the file-name prefix alone: `gax-` as a prefix also matches
        # gax-grpc-*.jar and gax-httpjson-*.jar (measured), so a prefix match would
        # trip its own uniqueness guard the day a gax-family entry uses jar:.
        jars = [
            p
            for p in map(Path, classpath.read_text(encoding="utf-8").strip().split(":"))
            if p.suffix == ".jar"
            and p.parent.parent.name == artifact_id
            and p.name.startswith(artifact_id + "-")
        ]
        if len(jars) != 1:
            fail(
                f"{name}: expected one jar for {ga} on the runtime classpath, found {jars}"
            )
        body = zipfile.ZipFile(jars[0]).read(entry["jar"])
    else:
        request = urllib.request.Request(
            entry["url"], headers={"User-Agent": "flink-connector-gcp-notice"}
        )
        token = github_token()
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        body = urllib.request.urlopen(request, timeout=30).read()
        if b"<html" in body[:400].lower() or b"<!doctype" in body[:400].lower():
            fail(
                f"{name}: {entry['url']} served an HTML page, not a licence text. "
                f"Several POM-declared licence URLs do this; pin a raw text URL."
            )
    digest = hashlib.sha256(body).hexdigest()
    if digest != entry["sha256"]:
        fail(
            f"{name}: content hash {digest} does not match the pin in {SOURCES}. "
            f"The source changed. Review the new text, and update the pin only "
            f"if the change is legitimate."
        )
    return body


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("module", type=Path)
    parser.add_argument(
        "-u",
        "--update",
        action="store_true",
        help="rewrite META-INF/NOTICE and META-INF/licenses/ instead of checking",
    )
    args = parser.parse_args()

    module: Path = args.module
    template = module / "NOTICE.template"
    notice = module / "src" / "main" / "resources" / "META-INF" / "NOTICE"
    licence_dir = notice.parent / "licenses"
    if not template.is_file():
        fail(f"{template} does not exist.")

    resolved = read_resolved(module)
    for licence in sorted(set(resolved.values()) - RESTRICTED_EXEMPT):
        if RESTRICTED.search(licence):
            offenders = sorted(g for g, lic in resolved.items() if lic == licence)
            fail(
                f"'{licence}' resolved for {offenders}. This project is Apache-2.0 "
                f"with no usage restrictions, so a restrictively-licensed dependency "
                f"is normally rejected outright rather than recorded — discuss "
                f"adoption first, and only then teach this script about it."
            )
    files = load_sources()
    for name, entry in files.items():
        for ga in entry["artifacts"]:
            for gav, licence in resolved.items():
                if gav.rsplit(":", 1)[0] == ga and licence == TEXT_EXEMPT_GROUP:
                    fail(
                        f"{SOURCES}: {ga} resolves to {TEXT_EXEMPT_GROUP}, whose "
                        f"artifacts carry no licence file — this entry would be "
                        f"materialised but referenced by nothing. Remove it."
                    )
    # Only the entries whose artifacts this module actually bundles.
    bundled_ga = {gav.rsplit(":", 1)[0] for gav in resolved}
    relevant = {
        name: entry
        for name, entry in files.items()
        if any(ga in bundled_ga for ga in entry["artifacts"])
    }
    expected_notice = render_notice(template, resolved, files)

    if args.update:
        notice.write_text(expected_notice, encoding="utf-8")
        licence_dir.mkdir(parents=True, exist_ok=True)
        for name, entry in relevant.items():
            (licence_dir / name).write_bytes(obtain_text(name, entry, module))
        for stray in licence_dir.iterdir():
            if (
                stray.is_file()
                and not stray.name.startswith(".")
                and stray.name not in relevant
            ):
                stray.unlink()
                print(f"removed {stray} (no longer in {SOURCES.name})")
        print(
            f"{module.name}: wrote META-INF/NOTICE ({len(resolved)} artifacts) "
            f"and {len(relevant)} licence files."
        )
        return 0

    # ---- offline check ----
    problems: list[str] = []
    actual = notice.read_text(encoding="utf-8") if notice.is_file() else ""
    if actual != expected_notice:
        problems.append(
            "META-INF/NOTICE differs from what NOTICE.template + the resolved "
            "bundle generate. Run `just update-notice` and commit the result."
        )
    for name, entry in relevant.items():
        path = licence_dir / name
        if not path.is_file():
            problems.append(
                f"META-INF/licenses/{name} is missing; run `just update-notice`."
            )
        elif hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            problems.append(
                f"META-INF/licenses/{name} does not hash to the pin in "
                f"{SOURCES.name} — edited by hand, or the pin moved without "
                f"regenerating. Run `just update-notice`."
            )
    if licence_dir.is_dir():
        for stray in licence_dir.iterdir():
            if (
                stray.is_file()
                and not stray.name.startswith(".")
                and stray.name not in relevant
            ):
                problems.append(
                    f"META-INF/licenses/{stray.name} is referenced by nothing in "
                    f"{SOURCES.name}; run `just update-notice` to remove it."
                )

    if problems:
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        fail(f"\n{module.name}: NOTICE/licences are stale relative to the bundle.")
    print(
        f"{module.name}: NOTICE matches the bundle ({len(resolved)} artifacts, "
        f"{len(relevant)} licence files, all hashes pinned)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
