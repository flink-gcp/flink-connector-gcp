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
"""Build a release's GitHub Release from Maven Central's files (ADR-0147, #1980).

`download VERSION DEST` fetches every SQL uber-jar of both version lines and its
`.asc` from Maven Central into a new DEST. Each jar must match its `.sha1` and
carry a valid signature by the pinned release key. Central serves no checksum for
a `.asc` (its `.asc.sha1` is a 404), so what is checked of a signature file is
that gpg verifies its jar with it; gpg ignores bytes after the armor. DEST receives
the files only once all of them pass. The checkout of the release tag fixes the
set through its `flink-sql-connector-gcp-*` modules: six per line and 24 files
from 1.2.0, five per line and 20 files for 1.0.0 and 1.1.0.

`complete VERSION DIR` creates the Release for tag vVERSION as a draft if none
exists, removes attachments whose upload never finished, attaches the files it
lacks, and publishes the draft only once its attachments are exactly those
files with matching digests. A finished attachment it cannot vouch for stops it.
Both subcommands are safe to repeat, which is what a re-run of the release
workflow's failed job does.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPOSITORY = "https://repo1.maven.org/maven2/io/github/flink-gcp"
# The dedicated project release key (ADR-0147), published on keyserver.ubuntu.com.
# A signature counts only when gpg reports this primary-key fingerprint, so where
# the key is fetched from does not matter to what is accepted.
RELEASE_KEY = "679AB13942F51CAF09F06E73608AC6CD5F89628E"
KEYSERVER = "hkps://keyserver.ubuntu.com"
VERSION_PATTERN = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
REQUEST_TIMEOUT = 60
# A deployment can be PUBLISHED a little before repo1 serves every file. One
# deadline covers the whole download, not each of its 36 requests.
FETCH_WAIT = 30 * 60
FETCH_INTERVAL = 30
KEY_ATTEMPTS = 3


class ReleaseError(Exception):
    """An actionable failure."""


def summary(message: str) -> None:
    print(message, flush=True)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a", encoding="utf-8") as output:
            output.write(f"- {message}\n")


def modules(root: Path) -> list[str]:
    found = sorted(
        path.parent.name for path in root.glob("flink-sql-connector-gcp-*/pom.xml")
    )
    # The tagged tree fixes the set, which differs between releases (1.2.0 added
    # Firestore). A tag run's staging job has already asserted the count of the
    # same commit through scripts/collect-release-jars.sh.
    if not found:
        raise ReleaseError(
            "found no SQL uber-jar module; run from the repository root of the release tag"
        )
    return found


def expected_files(root: Path, version: str) -> list[tuple[str, str, str]]:
    """Each (artifact, version line, file name) to publish, jars before signatures."""
    files = []
    for line in (version, f"{version}-1.20"):
        for artifact in modules(root):
            jar = f"{artifact}-{line}.jar"
            files += [(artifact, line, jar), (artifact, line, jar + ".asc")]
    return files


def fetch(url: str, deadline: float, destination: Path | None = None) -> bytes | None:
    """Return the body, or stream it to destination; retry until repo1 serves it."""
    while True:
        try:
            with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT) as response:
                if destination is None:
                    return response.read()
                with destination.open("wb") as output:
                    while chunk := response.read(1 << 20):
                        output.write(chunk)
                return None
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            if status != 404 and status != 429 and not 500 <= status < 600:
                raise ReleaseError(f"{url}: HTTP {status}") from None
            failure = f"HTTP {status}"
        except (OSError, http.client.HTTPException) as error:
            failure = type(error).__name__
        if time.monotonic() >= deadline:
            raise ReleaseError(
                f"{url}: still {failure} at the {FETCH_WAIT} s deadline; "
                "re-run the failed job"
            )
        print(f"{url}: {failure}; retrying", flush=True)
        time.sleep(FETCH_INTERVAL)


def digest(path: Path, algorithm: str) -> str:
    hasher = hashlib.new(algorithm)
    with path.open("rb") as source:
        while chunk := source.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def gpg(home: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gpg", "--batch", "--no-tty", "--homedir", str(home), *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )


def import_release_key(home: Path) -> None:
    for attempt in range(1, KEY_ATTEMPTS + 1):
        result = gpg(home, "--keyserver", KEYSERVER, "--recv-keys", RELEASE_KEY)
        if result.returncode == 0:
            return
        print(f"gpg --recv-keys attempt {attempt} failed:\n{result.stderr}", flush=True)
        if attempt < KEY_ATTEMPTS:
            time.sleep(FETCH_INTERVAL)
    raise ReleaseError(f"could not fetch release key {RELEASE_KEY} from {KEYSERVER}")


def verify_signature(home: Path, jar: Path) -> None:
    result = gpg(home, "--status-fd", "1", "--verify", f"{jar}.asc", str(jar))
    lines = result.stdout.splitlines()
    # VALIDSIG's last field is the primary key's fingerprint, whichever subkey signed.
    signers = {
        line.split()[-1] for line in lines if line.startswith("[GNUPG:] VALIDSIG ")
    }
    # gpg reports EXPKEYSIG or REVKEYSIG instead of GOODSIG for a signature by an
    # expired or revoked key, while still printing VALIDSIG.
    good = any(line.startswith("[GNUPG:] GOODSIG ") for line in lines)
    if result.returncode != 0 or not good or signers != {RELEASE_KEY}:
        raise ReleaseError(
            f"{jar.name}: no valid signature by release key {RELEASE_KEY}"
        )


def download(root: Path, version: str, destination: Path) -> None:
    if destination.exists():
        raise ReleaseError(
            f"{destination} already exists; download into a new directory"
        )
    files = expected_files(root, version)
    with tempfile.TemporaryDirectory(prefix="release-assets-") as scratch:
        staging = Path(scratch) / "files"
        staging.mkdir()
        deadline = time.monotonic() + FETCH_WAIT
        for artifact, line, name in files:
            url = f"{REPOSITORY}/{artifact}/{line}/{name}"
            path = staging / name
            fetch(url, deadline, path)
            if name.endswith(".asc"):
                continue
            published = (
                fetch(url + ".sha1", deadline).decode("ascii", "replace").split()
            )
            if not published or published[0].lower() != digest(path, "sha1"):
                raise ReleaseError(
                    f"{name}: SHA-1 does not match Maven Central's .sha1"
                )
        home = Path(scratch) / "gnupg"
        home.mkdir(mode=0o700)
        import_release_key(home)
        for _, _, name in files:
            if name.endswith(".jar"):
                verify_signature(home, staging / name)
        # Only verified files reach the destination.
        shutil.copytree(staging, destination)
    summary(
        f"Downloaded and verified {len(files)} files of {version} and {version}-1.20 "
        "from Maven Central."
    )


def gh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gh", *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )


def checked(result: subprocess.CompletedProcess, action: str) -> str:
    if result.returncode != 0:
        raise ReleaseError(
            f"{action} failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout


def view(tag: str) -> dict | None:
    result = gh("release", "view", tag, "--json", "tagName,isDraft,assets")
    if result.returncode != 0 and "release not found" in result.stderr:
        return None
    release = json.loads(checked(result, f"gh release view {tag}"))
    if release.get("tagName") != tag:
        raise ReleaseError(f"gh release view {tag} returned {release.get('tagName')!r}")
    return release


def check_assets(release: dict, local: dict[str, Path]) -> None:
    """Every finished attachment must be one of the local files, byte for byte."""
    for asset in release["assets"]:
        name = asset["name"]
        if name not in local:
            raise ReleaseError(
                f"the Release carries {name}, outside the published set; "
                "remove it by hand"
            )
        if asset.get("state") != "uploaded":
            continue
        if asset.get("digest") != f"sha256:{digest(local[name], 'sha256')}":
            raise ReleaseError(
                f"attachment {name} differs from Maven Central's file; replace it by hand"
            )


def complete(root: Path, version: str, directory: Path) -> None:
    tag = f"v{version}"
    expected = sorted(name for _, _, name in expected_files(root, version))
    local = {path.name: path for path in directory.iterdir()}
    if sorted(local) != expected:
        raise ReleaseError(
            f"{directory} does not hold exactly the {len(expected)} files of {version}"
        )
    release = view(tag)
    if release is None:
        # --verify-tag refuses to create the tag: the Release must name the pushed one.
        checked(
            gh("release", "create", tag, "--draft", "--verify-tag", "--generate-notes"),
            f"gh release create {tag}",
        )
        summary(f"Created draft GitHub Release {tag}.")
        release = view(tag)
        if release is None:
            raise ReleaseError(f"the draft Release {tag} is not visible after creation")
    check_assets(release, local)
    # An upload that never finished leaves an asset GitHub has no content for; it
    # is safe to delete and send again.
    for asset in release["assets"]:
        if asset.get("state") != "uploaded":
            checked(
                gh("release", "delete-asset", tag, asset["name"], "--yes"),
                f"gh release delete-asset {tag} {asset['name']}",
            )
            summary(f"Deleted unfinished attachment {asset['name']} from {tag}.")
    finished = {a["name"] for a in release["assets"] if a.get("state") == "uploaded"}
    if missing := [name for name in expected if name not in finished]:
        checked(
            gh("release", "upload", tag, *(str(local[name]) for name in missing)),
            f"gh release upload {tag}",
        )
        summary(f"Attached {len(missing)} files to GitHub Release {tag}.")
    release = view(tag)
    if release is None:
        raise ReleaseError(f"the Release {tag} disappeared during upload")
    check_assets(release, local)
    finished = sorted(
        a["name"] for a in release["assets"] if a.get("state") == "uploaded"
    )
    if finished != expected or len(release["assets"]) != len(expected):
        raise ReleaseError(f"the Release {tag} still lacks finished attachments")
    if release["isDraft"]:
        checked(gh("release", "edit", tag, "--draft=false"), f"gh release edit {tag}")
        release = view(tag)
        if release is None or release["isDraft"]:
            raise ReleaseError(f"the Release {tag} is still a draft")
        summary(f"Published GitHub Release {tag} with all {len(expected)} attachments.")
    else:
        summary(
            f"GitHub Release {tag} is published with all {len(expected)} attachments."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    fetching = commands.add_parser("download", help="fetch and verify both lines")
    fetching.add_argument("version")
    fetching.add_argument("destination", type=Path)
    completing = commands.add_parser("complete", help="create or finish the Release")
    completing.add_argument("version")
    completing.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    try:
        if not re.fullmatch(VERSION_PATTERN, args.version):
            raise ReleaseError("expected bare X.Y.Z, without v prefix or -1.20 suffix")
        if args.action == "download":
            download(Path.cwd(), args.version, args.destination)
        else:
            complete(Path.cwd(), args.version, args.directory)
    except (ReleaseError, OSError, ValueError) as error:
        summary(f"release-assets: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
