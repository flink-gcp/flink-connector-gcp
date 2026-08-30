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
"""Guard `just stage-release`'s version/toolchain pairing (issue #724, ADR-0147).

Every release stages two version lines: bare ``X.Y.Z`` is the Flink 2.x line
built at the pom's pinned floor, and ``X.Y.Z-1.20`` is the same coordinates
compiled for the 1.x LTS. A mislabelled pairing — a 2.x-built jar staged under
a ``-1.20`` version, or the reverse — would be discovered only after an
irreversible Maven Central publish, so the recipe refuses to start one.

Rejecting bad *arguments* is a losing game: ``-D`` has a ``--define`` long
form, both take separated values, and ``MAVEN_OPTS`` and ``.mvn/maven.config``
inject properties no argument scan sees. This guard asks Maven instead:
``help:evaluate`` runs with the deploy line's own fixed properties plus the
caller's extra arguments and reports the *effective* ``flink.version`` and
``flink.compat``, which are compared against the version line being staged.
The bare line's expected floor is read as the literal ``<flink.version>`` in
the root pom rather than probed, so an environment channel (``MAVEN_OPTS``, a
settings profile) that shifts every probe equally cannot move the expectation
along with the measurement.

Even that measurement cannot be the deploy itself — the probe necessarily
carries ``-Dexpression``/``-DforceStdout``, which a sufficiently perverse
profile activation could key on — so the airtight half of the guard is the
``enforce-version-line-toolchain`` enforcer rule in the root pom's
``central-release`` profile, which re-checks the same invariant inside the
release build against its final effective model. This script is the early,
readable half: it fails before ``versions:set`` touches the working tree.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

VERSION_PATTERN = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-1\.20)?\Z"
)
LTS_SUFFIX = "-1.20"
FLOOR_PATTERN = re.compile(r"<flink\.version>([^<]+)</flink\.version>")

# The deploy line's fixed properties, mirrored so a profile keyed on any of
# them (e.g. `release` in settings.xml) is active for the measurement too.
FIXED_FLAGS = ("-Drelease", "-DskipTests", "-Djapicmp.skip=true")


def classify(version: str) -> str | None:
    """Return "lts" or "bare" for a well-formed staging version, else None."""
    if not VERSION_PATTERN.match(version):
        return None
    return "lts" if version.endswith(LTS_SUFFIX) else "bare"


def toolchain_error(
    kind: str, effective_version: str, effective_compat: str, floor: str | None
) -> str | None:
    """The refusal message for a mismatched pairing, or None when it matches."""
    if kind == "lts":
        if effective_version.startswith("1.20.") and effective_compat == "flink1":
            return None
        return (
            f"a {LTS_SUFFIX} version must stage the 1.20/flink1 build, but the "
            f"effective model resolves flink.version='{effective_version}', "
            f"flink.compat='{effective_compat}' — pass -Dflink.version=1.20.<patch> "
            "and nothing that overrides either property"
        )
    if effective_version == floor and effective_compat == "flink2":
        return None
    return (
        "a bare X.Y.Z version stages the Flink 2.x line at the pom's pinned "
        f"floor ({floor}, flink2), but the effective model resolves "
        f"flink.version='{effective_version}', flink.compat='{effective_compat}' "
        "— do not override either property"
    )


def pinned_floor(pom_text: str) -> str:
    """The literal <flink.version> the root pom pins (the 2.x floor)."""
    match = FLOOR_PATTERN.search(pom_text)
    if match is None:
        raise SystemExit(
            "stage-release: the root pom declares no <flink.version> literal"
        )
    return match.group(1)


def evaluate(expression: str, extra_args: list[str]) -> str:
    """Resolve one property from Maven's effective model, deploy-flagged."""
    command = [
        "./mvnw",
        "-ntp",
        "-q",
        "help:evaluate",
        f"-Dexpression={expression}",
        "-DforceStdout",
        *FIXED_FLAGS,
        *extra_args,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise SystemExit(
            f"stage-release: probing {expression} failed"
            f" ({' '.join(command)}):\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def read_root_pom() -> str:
    """The root pom's text; a seam for the tests."""
    return Path("pom.xml").read_text(encoding="utf-8")


def main(argv: list[str], evaluate_fn=evaluate, pom_reader=read_root_pom) -> str | None:
    """Return the refusal message, or None when staging may proceed."""
    if not argv:
        return "usage: stage-release-guard.py <version> [maven args...]"
    version, extra = argv[0], list(argv[1:])
    kind = classify(version)
    if kind is None:
        return (
            f"stage-release: version must be X.Y.Z or X.Y.Z{LTS_SUFFIX}, got "
            f"'{version}' (no leading v, no leading zeros)"
        )
    probe_args = (["-Dflink.compat=flink1"] if kind == "lts" else []) + extra
    effective_version = evaluate_fn("flink.version", probe_args)
    effective_compat = evaluate_fn("flink.compat", probe_args)
    floor = pinned_floor(pom_reader()) if kind == "bare" else None
    error = toolchain_error(kind, effective_version, effective_compat, floor)
    return f"stage-release: {error}" if error else None


if __name__ == "__main__":
    message = main(sys.argv[1:])
    if message:
        print(message, file=sys.stderr)
        sys.exit(1)
