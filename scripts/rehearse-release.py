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
"""Rehearse a release without uploading anything to the Central Portal (#1818).

Both version lines go through ``just stage-release`` with ``-DskipUpload=true``:
central-publishing-maven-plugin 0.12.0 writes the real bundle to
``target/central-publishing/central-bundle.zip``, returns before the upload and
reads no Central credentials. Each bundle is copied to a temporary directory
before the next build's ``clean`` and then inspected for what Central's
validation would otherwise reject only after the tag: the ``v1.2.0`` tag run
failed because Maven 3.10.0 wrote local-repository files into the plugin's
staging directory and plugin 0.11.0 bundled them ("Bundle has content that does
NOT have a .pom file", #1812).

Each bundle must hold, for every directory that contains a file, the
``<artifactId>-<version>.pom`` that directory's path names, at the version line
being rehearsed; no local-repository file; and a ``.asc`` beside every file
that is not itself a signature or a checksum. A local-repository file is
reported once, as itself, rather than again by the other checks.

The report gives each line's file count and bytes and their totals. Central's
Usage Center counts the uncompressed bytes of every published file in decimal
megabytes, so the report sums the entries' uncompressed sizes rather than the
zip's size.

Signing needs MAVEN_GPG_KEY and MAVEN_GPG_PASSPHRASE. ``--unsigned`` adds
``-Dgpg.skip=true`` to both builds and waives the signature check, for a
rehearsal without the release key; its counts then lack the signatures a
release adds. The deploy phase still installs the release version into the
local repository, so a local rehearsal should also pass
``-Dmaven.repo.local=<throwaway copy>``. stage-release re-versions the POMs, so
this refuses to start on uncommitted POM edits and restores the POMs on exit.
A SIGTERM is passed on to the running ``just``, which passes it to Maven, and
the build is waited for before the restore, so nothing that writes the POMs
outlives it. (stage-release's guard does not pass it on, but its Maven probe
only reads the model, before versions:set.)

Usage: rehearse-release.py <version> [--unsigned] [maven args...]
"""

from __future__ import annotations

import os
import posixpath
import shutil
import signal
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

BUNDLE = Path("target/central-publishing/central-bundle.zip")
LTS_SUFFIX = "-1.20"
POMS = "*pom.xml"

# Maven 3.10.0 writes these into a file:// repository it deploys to; Central
# rejects a bundle carrying them. The staging metadata is the plugin's own,
# which every version removes before bundling. A checksum of any of them is
# the same defect.
LOCAL_REPOSITORY_FILES = frozenset(
    {
        "maven-metadata-local.xml",
        "_remote.repositories",
        "maven-metadata-central-staging.xml",
    }
)
LOCKS_DIRECTORY = ".locks"
CHECKSUM_SUFFIXES = (".md5", ".sha1", ".sha256", ".sha512")
SIGNATURE_SUFFIX = ".asc"


@dataclass
class Inspection:
    """One bundle's defects and totals."""

    version: str
    files: int = 0
    bytes: int = 0
    errors: list[str] = field(default_factory=list)


def is_local_repository_file(name: str) -> bool:
    """Whether an entry is local-repository state rather than a published file."""
    parts = name.split("/")
    if LOCKS_DIRECTORY in parts[:-1]:
        return True
    base = parts[-1]
    stem = next(
        (
            base.removesuffix(suffix)
            for suffix in CHECKSUM_SUFFIXES
            if base.endswith(suffix)
        ),
        base,
    )
    return stem in LOCAL_REPOSITORY_FILES


def needs_signature(name: str) -> bool:
    """Central requires a signature beside every file but signatures and checksums."""
    return not name.endswith((SIGNATURE_SUFFIX,) + CHECKSUM_SUFFIXES)


def inspect(version: str, entries: list[tuple[str, int]], unsigned: bool) -> Inspection:
    """Check one bundle's (name, uncompressed size) entries for the version line."""
    result = Inspection(version)
    files = [(name, size) for name, size in entries if not name.endswith("/")]
    result.files = len(files)
    result.bytes = sum(size for _, size in files)
    if not files:
        result.errors.append("the bundle holds no files")
        return result

    local = sorted(name for name, _ in files if is_local_repository_file(name))
    if local:
        result.errors.append(
            f"local-repository files that Central rejects: {', '.join(local)}"
        )
    names = {name for name, _ in files} - set(local)

    directories = sorted({posixpath.dirname(name) for name in names})
    for directory in directories:
        parts = directory.split("/")
        if len(parts) < 3:
            result.errors.append(
                f"'{directory or '/'}' is not a <group>/<artifactId>/<version> directory"
            )
            continue
        artifact_id, directory_version = parts[-2], parts[-1]
        if directory_version != version:
            result.errors.append(
                f"'{directory}' holds files but is not a <artifactId>/{version} directory"
            )
            continue
        pom = f"{directory}/{artifact_id}-{version}.pom"
        if pom not in names:
            result.errors.append(f"'{directory}' has no {posixpath.basename(pom)}")

    if not unsigned:
        unsigned = sorted(
            name
            for name in names
            if needs_signature(name) and name + SIGNATURE_SUFFIX not in names
        )
        if unsigned:
            result.errors.append(
                f"{len(unsigned)} files without a {SIGNATURE_SUFFIX} signature, "
                f"first {unsigned[0]}; a rehearsal without the release key "
                "passes --unsigned"
            )
    return result


def read_entries(path: Path) -> list[tuple[str, int]]:
    """A zip's entry names and uncompressed sizes."""
    with zipfile.ZipFile(path) as bundle:
        return [(info.filename, info.file_size) for info in bundle.infolist()]


def megabytes(size: int) -> str:
    return f"{size / 1_000_000:,.2f} MB"


def report(inspections: list[Inspection], unsigned: bool) -> str:
    """The Markdown table of each line's files and bytes, with a total row."""
    lines = [
        "| Version line | Files | Size | Bytes |",
        "|---|---:|---:|---:|",
    ]
    for item in inspections:
        lines.append(
            f"| `{item.version}` | {item.files:,} | {megabytes(item.bytes)} | {item.bytes:,} |"
        )
    files = sum(item.files for item in inspections)
    size = sum(item.bytes for item in inspections)
    lines.append(f"| Total | {files:,} | {megabytes(size)} | {size:,} |")
    if unsigned:
        lines += [
            "",
            "Built without signatures: a release adds one `.asc` per signed file.",
        ]
    return "\n".join(lines) + "\n"


def run(command: list[str]) -> None:
    """Run a command, passing an interruption on to it and waiting for it."""
    with subprocess.Popen(command) as process:
        try:
            status = process.wait()
        except BaseException:
            # subprocess.run would SIGKILL the child here; `just` forwards a
            # SIGTERM to the Maven build it runs, but not a SIGKILL.
            process.terminate()
            process.wait()
            raise
    if status:
        raise subprocess.CalledProcessError(status, command)


def stage(version: str, maven_args: list[str], dest: Path) -> Path:
    """Build one line's bundle without uploading it and copy it to dest."""
    # Removed first, so a build that writes no bundle cannot leave an earlier
    # run's in place for the copy.
    BUNDLE.unlink(missing_ok=True)
    # -DskipUpload=true comes last: Maven keeps the last value a property is
    # given, so no forwarded argument can turn the upload back on.
    run(["just", "stage-release", version, *maven_args, "-DskipUpload=true"])
    if not BUNDLE.is_file():
        raise SystemExit(f"rehearse-release: the {version} build wrote no {BUNDLE}")
    copy = dest / f"central-bundle-{version}.zip"
    shutil.copyfile(BUNDLE, copy)
    return copy


def lts_version() -> str:
    """weekly.yaml's FLINK_LTS, through the flag verify.yaml also reads it with."""
    reader = Path(__file__).with_name("check-flink-api-tiers.py")
    return subprocess.run(
        [sys.executable, str(reader), "--print-lts-version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def build(version: str, maven_args: list[str]) -> list[tuple[str, Path]]:
    """Stage both lines, the 1.20 one at weekly.yaml's LTS patch."""
    lts = lts_version()
    dest = Path(tempfile.mkdtemp(prefix="rehearse-release-"))
    bundles = [
        (version, stage(version, maven_args, dest)),
        (
            version + LTS_SUFFIX,
            stage(version + LTS_SUFFIX, [f"-Dflink.version={lts}", *maven_args], dest),
        ),
    ]
    print(f"rehearse-release: bundles are in {dest}")
    return bundles


def restore_poms() -> bool:
    """Undo versions:set, uninterrupted; report a failure rather than raise it."""
    previous = {
        signum: signal.signal(signum, signal.SIG_IGN)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        result = subprocess.run(["git", "checkout", "--", POMS], check=False)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if result.returncode:
        print(
            "rehearse-release: the POMs were NOT restored; "
            f"run git checkout -- '{POMS}'",
            file=sys.stderr,
        )
    return not result.returncode


def exit_status(returncode: int) -> int:
    """A child's status as a shell reports it: 128+N for death by signal N."""
    return 128 - returncode if returncode < 0 else returncode


def main(argv: list[str]) -> int:
    if not argv or argv[0].startswith("-"):
        print(
            "usage: rehearse-release.py <version> [--unsigned] [maven args...]",
            file=sys.stderr,
        )
        return 2
    version, maven_args = argv[0], argv[1:]
    unsigned = maven_args[:1] == ["--unsigned"]
    if unsigned:
        maven_args = [*maven_args[1:], "-Dgpg.skip=true"]

    if subprocess.run(["git", "diff", "--quiet", "--", POMS], check=False).returncode:
        print(
            "rehearse-release: uncommitted POM edits would be overwritten by "
            "versions:set and then discarded; commit or stash them first",
            file=sys.stderr,
        )
        return 1
    # SIGTERM unwinds like an interrupt: run() passes it on, then the POMs are
    # restored before the process exits.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(128 + signal.SIGTERM))
    failed = None
    try:
        bundles = build(version, maven_args)
    except subprocess.CalledProcessError as error:
        failed = error
    finally:
        # Unconditional, and before anything else can fail: any exit from
        # here on leaves the POMs as they were.
        restored = restore_poms()
    if failed:
        print(f"rehearse-release: {' '.join(failed.cmd)} failed", file=sys.stderr)
        return exit_status(failed.returncode)
    if not restored:
        return 1

    inspections = [
        inspect(line, read_entries(path), unsigned) for line, path in bundles
    ]
    errors = [
        f"rehearse-release: {item.version}: {error}"
        for item in inspections
        for error in item.errors
    ]
    table = report(inspections, unsigned)
    print(table, end="")
    for error in errors:
        print(error, file=sys.stderr)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        # The defects go to the summary too, so a failed run names them there.
        listed = "".join(f"- {error}\n" for error in errors)
        with Path(destination).open("a", encoding="utf-8") as output:
            output.write("### Central bundle rehearsal\n\n" + table)
            if listed:
                output.write("\n" + listed)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
