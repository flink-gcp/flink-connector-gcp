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
"""Synthetic module trees, Central responses, gpg results and Release states."""

import hashlib
import io
import json
import re
import subprocess
import urllib.error
from pathlib import Path

import pytest
from conftest import load_script

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = [
    f"flink-sql-connector-gcp-{name}"
    for name in ("bigquery", "bigtable", "cloudtasks", "firestore", "pubsub", "spanner")
]


@pytest.fixture
def helper(monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    module = load_script("release-assets.py")
    # A fake clock that sleeping advances: a retry loop reaches its deadline at
    # once instead of spinning for FETCH_WAIT real seconds.
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    return module


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "tree"
    for artifact in ARTIFACTS:
        (root / artifact).mkdir(parents=True)
        (root / artifact / "pom.xml").write_text("<project/>")
    # Neither a directory without a POM nor another module counts.
    (root / "flink-sql-connector-gcp-stale").mkdir()
    (root / "flink-connector-gcp-base").mkdir()
    (root / "flink-connector-gcp-base" / "pom.xml").write_text("<project/>")
    return root


def names(version="1.3.0"):
    return sorted(
        f"{artifact}-{line}.jar{suffix}"
        for line in (version, f"{version}-1.20")
        for artifact in ARTIFACTS
        for suffix in ("", ".asc")
    )


def test_expected_files_are_six_jars_and_signatures_per_line(helper, tree):
    files = helper.expected_files(tree, "1.3.0")
    assert sorted(name for _, _, name in files) == names()
    assert len(files) == 24
    assert (
        "flink-sql-connector-gcp-pubsub",
        "1.3.0-1.20",
        "flink-sql-connector-gcp-pubsub-1.3.0-1.20.jar.asc",
    ) in files


def test_the_tagged_tree_fixes_the_set(helper, tree):
    # 1.0.0 and 1.1.0 shipped five SQL uber-jars per line; Firestore came in 1.2.0.
    (tree / "flink-sql-connector-gcp-firestore" / "pom.xml").unlink()
    files = helper.expected_files(tree, "1.1.0")
    assert len(files) == 20
    assert not any("firestore" in name for _, _, name in files)


def test_a_tree_without_modules_fails(helper, tmp_path):
    with pytest.raises(helper.ReleaseError, match="found no SQL uber-jar module"):
        helper.expected_files(tmp_path, "1.3.0")


def test_this_tree_matches_the_staging_collector(helper):
    # The tag run's staging job asserts this tree's count with
    # collect-release-jars.sh before Publish; the two must agree.
    collector = (ROOT / "scripts/collect-release-jars.sh").read_text()
    count = str(len(helper.modules(ROOT)))
    assert re.findall(r"-ne (\d+) \]", collector) == [count] * 2


class Central:
    """repo1 as a dict of URL to body; a missing URL is a 404."""

    def __init__(self, files, late=None):
        self.bodies = {}
        for name, body in files.items():
            artifact, line = owner(name)
            url = f"https://repo1.maven.org/maven2/io/github/flink-gcp/{artifact}/{line}/{name}"
            self.bodies[url] = body
            # As on repo1: a jar has a .sha1, a signature has none.
            if not name.endswith(".asc"):
                self.bodies[url + ".sha1"] = hashlib.sha1(body).hexdigest().encode()
        # name -> number of 404s to serve before the file appears
        self.late = dict(late or {})
        self.requests = []

    def urlopen(self, url, timeout):
        self.requests.append(url)
        assert timeout == 60
        name = url.rsplit("/", 1)[-1]
        if self.late.get(name):
            self.late[name] -= 1
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO())
        if url not in self.bodies:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO())
        return io.BytesIO(self.bodies[url])


def owner(name):
    for artifact in ARTIFACTS:
        if name.startswith(artifact + "-"):
            line = name.removeprefix(artifact + "-").split(".jar")[0]
            return artifact, line
    raise AssertionError(name)


def published(version="1.3.0"):
    return {name: f"bytes of {name}".encode() for name in names(version)}


@pytest.fixture
def signatures(helper, monkeypatch):
    """gpg as a function. It accepts only the argument shapes the real gpg is
    called with, and a jar's VALIDSIG names the signer recorded for it (the
    release key unless overridden; None is a bad signature)."""
    state = {"signers": {}, "calls": [], "exit": {}, "verdicts": {}}

    def gpg(home, *args):
        state["calls"].append(args)
        assert Path(home).is_dir()
        if args[0] == "--keyserver":
            assert args == (
                "--keyserver",
                helper.KEYSERVER,
                "--recv-keys",
                helper.RELEASE_KEY,
            )
            return subprocess.CompletedProcess(args, 0, "", "")
        assert args[:3] == ("--status-fd", "1", "--verify"), args
        signature, jar = args[3:]
        assert signature == jar + ".asc"
        name = Path(jar).name
        signer = state["signers"].get(name, helper.RELEASE_KEY)
        if signer is None:
            return subprocess.CompletedProcess(args, 1, "[GNUPG:] BADSIG x\n", "")
        verdict = state["verdicts"].get(name, "GOODSIG")
        lines = (
            f"[GNUPG:] {verdict} 608AC6CD5F89628E flink-gcp\n"
            f"[GNUPG:] VALIDSIG AAAA 2026-11-01 0 0 4 0 1 10 00 {signer}\n"
        )
        return subprocess.CompletedProcess(args, state["exit"].get(name, 0), lines, "")

    monkeypatch.setattr(helper, "gpg", gpg)
    return state


def test_download_fetches_checks_and_verifies_all_24(
    helper, tree, tmp_path, monkeypatch, signatures
):
    central = Central(published())
    monkeypatch.setattr(helper.urllib.request, "urlopen", central.urlopen)
    destination = tmp_path / "dist"
    helper.download(tree, "1.3.0", destination)
    assert sorted(path.name for path in destination.iterdir()) == names()
    assert (destination / names()[0]).read_bytes() == published()[names()[0]]
    verified = [
        Path(args[-1]).name for args in signatures["calls"] if "--verify" in args
    ]
    assert sorted(verified) == [name for name in names() if name.endswith(".jar")]
    # Twelve jars, twelve signatures and the jars' twelve .sha1 files.
    assert len(central.requests) == 36


def test_download_waits_for_repo1_to_serve_a_file(
    helper, tree, tmp_path, monkeypatch, signatures
):
    late = names()[3]
    central = Central(published(), late={late: 2})
    monkeypatch.setattr(helper.urllib.request, "urlopen", central.urlopen)
    helper.download(tree, "1.3.0", tmp_path / "dist")
    assert sum(url.endswith("/" + late) for url in central.requests) == 3


def test_one_deadline_covers_the_whole_download(
    helper, tree, tmp_path, monkeypatch, signatures
):
    # The first file is late for all but one interval of the deadline; the last
    # one never arrives. A deadline that restarted per file would allow the last
    # file the whole FETCH_WAIT again; one deadline leaves it a single retry.
    files = helper.expected_files(tree, "1.3.0")
    first, last = files[0][2], files[-1][2]
    contents = published()
    del contents[last]
    tries = helper.FETCH_WAIT // helper.FETCH_INTERVAL
    central = Central(contents, late={first: tries - 1})
    monkeypatch.setattr(helper.urllib.request, "urlopen", central.urlopen)
    with pytest.raises(helper.ReleaseError, match=f"{last}: still HTTP 404"):
        helper.download(tree, "1.3.0", tmp_path / "dist")
    assert sum(url.endswith("/" + last) for url in central.requests) == 2
    assert not (tmp_path / "dist").exists()


class Flaky:
    """Fails each URL's first request with `failure`, then serves `body`."""

    def __init__(self, failure, body=b"payload"):
        self.failure = failure
        self.body = body
        self.requests = []

    def urlopen(self, url, timeout):
        self.requests.append(url)
        if len(self.requests) == 1:
            raise self.failure(url)
        return io.BytesIO(self.body)


def http_error(status):
    return lambda url: urllib.error.HTTPError(url, status, "x", {}, io.BytesIO())


class BrokenStream(io.BytesIO):
    def read(self, *args):
        raise ConnectionResetError("reset mid-stream")


@pytest.mark.parametrize(
    "failure",
    [
        http_error(429),
        http_error(500),
        http_error(503),
        lambda url: urllib.error.URLError("unreachable"),
        lambda url: TimeoutError("timed out"),
        lambda url: ConnectionResetError("reset"),
    ],
)
def test_download_retries_transient_failures(helper, monkeypatch, tmp_path, failure):
    flaky = Flaky(failure)
    monkeypatch.setattr(helper.urllib.request, "urlopen", flaky.urlopen)
    target = tmp_path / "file"
    helper.fetch("https://repo1.maven.org/x", helper.FETCH_WAIT, target)
    assert target.read_bytes() == b"payload"
    assert len(flaky.requests) == 2


def test_download_rewrites_a_file_whose_stream_broke(helper, monkeypatch, tmp_path):
    responses = [BrokenStream(b"partial"), io.BytesIO(b"whole")]
    monkeypatch.setattr(
        helper.urllib.request, "urlopen", lambda url, timeout: responses.pop(0)
    )
    target = tmp_path / "file"
    helper.fetch("https://repo1.maven.org/x", helper.FETCH_WAIT, target)
    assert target.read_bytes() == b"whole"


def test_download_does_not_retry_a_refusal(helper, monkeypatch):
    requests = []

    def refuse(url, timeout):
        requests.append(url)
        raise urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO())

    monkeypatch.setattr(helper.urllib.request, "urlopen", refuse)
    with pytest.raises(helper.ReleaseError, match="HTTP 403"):
        helper.fetch("https://repo1.maven.org/x", helper.FETCH_WAIT)
    assert len(requests) == 1


@pytest.mark.parametrize("line", ["1.3.0", "1.3.0-1.20"])
def test_download_rejects_a_jar_that_does_not_match_its_sha1(
    helper, tree, tmp_path, monkeypatch, signatures, line
):
    central = Central(published())
    name = f"{ARTIFACTS[2]}-{line}.jar"
    url = next(u for u in central.bodies if u.endswith("/" + name))
    central.bodies[url] = b"tampered"
    monkeypatch.setattr(helper.urllib.request, "urlopen", central.urlopen)
    with pytest.raises(helper.ReleaseError, match=f"{name}: SHA-1 does not match"):
        helper.download(tree, "1.3.0", tmp_path / "dist")
    assert not (tmp_path / "dist").exists()


@pytest.mark.parametrize("signer,code", [(None, 1), ("0" * 40, 0), ("release", 2)])
def test_download_rejects_a_jar_not_signed_by_the_release_key(
    helper, tree, tmp_path, monkeypatch, signatures, signer, code
):
    jar = next(name for name in names() if name.endswith(".jar"))
    signatures["signers"][jar] = helper.RELEASE_KEY if signer == "release" else signer
    signatures["exit"][jar] = code
    monkeypatch.setattr(helper.urllib.request, "urlopen", Central(published()).urlopen)
    with pytest.raises(helper.ReleaseError, match=f"{jar}: no valid signature"):
        helper.download(tree, "1.3.0", tmp_path / "dist")
    # Nothing unverified reaches the destination.
    assert not (tmp_path / "dist").exists()


@pytest.mark.parametrize("verdict", ["EXPKEYSIG", "REVKEYSIG"])
def test_download_rejects_a_signature_by_an_expired_or_revoked_key(
    helper, tree, tmp_path, monkeypatch, signatures, verdict
):
    # gpg still prints VALIDSIG, and may exit 0, for such a signature.
    jar = next(name for name in names() if name.endswith(".jar"))
    signatures["verdicts"][jar] = verdict
    monkeypatch.setattr(helper.urllib.request, "urlopen", Central(published()).urlopen)
    with pytest.raises(helper.ReleaseError, match=f"{jar}: no valid signature"):
        helper.download(tree, "1.3.0", tmp_path / "dist")


def test_download_refuses_an_existing_destination(helper, tree, tmp_path):
    (tmp_path / "dist").mkdir()
    with pytest.raises(helper.ReleaseError, match="already exists"):
        helper.download(tree, "1.3.0", tmp_path / "dist")


def test_release_key_import_failure_is_fatal(helper, tmp_path, monkeypatch):
    calls = []

    def gpg(home, *args):
        calls.append(args)
        return subprocess.CompletedProcess(args, 2, "", "keyserver unreachable")

    monkeypatch.setattr(helper, "gpg", gpg)
    with pytest.raises(helper.ReleaseError, match="could not fetch release key"):
        helper.import_release_key(tmp_path)
    assert len(calls) == helper.KEY_ATTEMPTS


def test_signature_check_uses_the_real_gpg_status_format(helper, tmp_path, monkeypatch):
    # VALIDSIG's last field is the primary key, even when a subkey signed.
    status = (
        "[GNUPG:] NEWSIG\n[GNUPG:] GOODSIG 608AC6CD5F89628E flink-gcp\n"
        "[GNUPG:] VALIDSIG 1111111111111111111111111111111111111111 2026-11-01 "
        f"1793504485 0 4 0 1 10 00 {helper.RELEASE_KEY}\n"
    )
    monkeypatch.setattr(
        helper,
        "gpg",
        lambda home, *args: subprocess.CompletedProcess(args, 0, status, ""),
    )
    helper.verify_signature(tmp_path, tmp_path / "a.jar")


class Releases:
    """`gh release` over one in-memory Release; None means it does not exist."""

    def __init__(self, tag, release=None, upload_state="uploaded", stays_draft=False):
        self.tag = tag
        self.release = release
        self.upload_state = upload_state
        self.stays_draft = stays_draft
        self.calls = []

    def gh(self, *args):
        self.calls.append(args)
        command, tag = args[1], args[2]
        assert args[0] == "release" and tag == self.tag
        if command == "view":
            assert args[3:] == ("--json", "tagName,isDraft,assets")
            if self.release is None:
                return subprocess.CompletedProcess(args, 1, "", "release not found\n")
            return subprocess.CompletedProcess(
                args, 0, json.dumps({"tagName": tag, **self.release}), ""
            )
        if command == "create":
            assert args[3:] == ("--draft", "--verify-tag", "--generate-notes")
            self.release = {"isDraft": True, "assets": []}
        elif command == "upload":
            for path in args[3:]:
                self.release["assets"].append(asset(path, state=self.upload_state))
        elif command == "delete-asset":
            assert args[4:] == ("--yes",)
            self.release["assets"] = [
                a for a in self.release["assets"] if a["name"] != args[3]
            ]
        elif command == "edit":
            assert args[3:] == ("--draft=false",)
            self.release["isDraft"] = self.stays_draft
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    def mutations(self):
        return [args[1] for args in self.calls if args[1] != "view"]


def asset(path, **overrides):
    path = Path(path)
    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    return {"name": path.name, "state": "uploaded", "digest": digest, **overrides}


@pytest.fixture
def dist(tmp_path):
    directory = tmp_path / "dist"
    directory.mkdir()
    for name, body in published().items():
        (directory / name).write_bytes(body)
    return directory


def complete(helper, tree, dist, monkeypatch, releases):
    monkeypatch.setattr(helper, "gh", releases.gh)
    helper.complete(tree, "1.3.0", dist)


def test_complete_creates_a_draft_attaches_all_then_publishes(
    helper, tree, dist, monkeypatch
):
    releases = Releases("v1.3.0")
    complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == ["create", "upload", "edit"]
    assert releases.release["isDraft"] is False
    assert sorted(a["name"] for a in releases.release["assets"]) == names()


def test_complete_finishes_a_partial_draft(helper, tree, dist, monkeypatch):
    present = [asset(dist / name) for name in names()[:5]]
    releases = Releases("v1.3.0", {"isDraft": True, "assets": present})
    complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == ["upload", "edit"]
    upload = next(args for args in releases.calls if args[1] == "upload")
    assert sorted(Path(p).name for p in upload[3:]) == names()[5:]


def test_complete_replaces_an_unfinished_upload(helper, tree, dist, monkeypatch):
    # An interrupted upload leaves a starter asset with no content; a re-run
    # deletes it and sends the verified file again.
    assets = [asset(dist / name) for name in names()]
    assets[0] = {"name": names()[0], "state": "starter", "digest": None}
    releases = Releases("v1.3.0", {"isDraft": True, "assets": assets})
    complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == ["delete-asset", "upload", "edit"]
    upload = next(args for args in releases.calls if args[1] == "upload")
    assert [Path(p).name for p in upload[3:]] == [names()[0]]
    assert all(a["state"] == "uploaded" for a in releases.release["assets"])


def test_complete_leaves_a_finished_release_untouched(helper, tree, dist, monkeypatch):
    every = [asset(dist / name) for name in names()]
    releases = Releases("v1.3.0", {"isDraft": False, "assets": every})
    complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == []


def test_complete_attaches_to_a_published_release_without_republishing(
    helper, tree, dist, monkeypatch
):
    present = [asset(dist / name) for name in names()[1:]]
    releases = Releases("v1.3.0", {"isDraft": False, "assets": present})
    complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == ["upload"]


@pytest.mark.parametrize("is_draft", [True, False])
@pytest.mark.parametrize(
    "change,message",
    [
        (
            lambda a: a.append({**a[0], "name": "notes.txt"}),
            "outside the published set",
        ),
        (
            lambda a: a.append({"name": "notes.txt", "state": "starter"}),
            "outside the published set",
        ),
        (
            lambda a: a[0].update(digest="sha256:" + "0" * 64),
            "differs from Maven Central",
        ),
        (lambda a: a[0].update(digest=None), "differs from Maven Central"),
    ],
)
def test_complete_refuses_a_release_it_cannot_vouch_for(
    helper, tree, dist, monkeypatch, is_draft, change, message
):
    assets = [asset(dist / name) for name in names()]
    change(assets)
    releases = Releases("v1.3.0", {"isDraft": is_draft, "assets": assets})
    with pytest.raises(helper.ReleaseError, match=message):
        complete(helper, tree, dist, monkeypatch, releases)
    assert releases.mutations() == []


def test_complete_does_not_publish_an_upload_github_did_not_finish(
    helper, tree, dist, monkeypatch
):
    # gh exits 0 but the assets stay unfinished.
    releases = Releases("v1.3.0", upload_state="starter")
    with pytest.raises(helper.ReleaseError, match="still lacks finished"):
        complete(helper, tree, dist, monkeypatch, releases)
    assert "edit" not in releases.mutations()
    assert releases.release["isDraft"] is True


def test_complete_does_not_publish_an_upload_github_stored_wrongly(
    helper, tree, dist, monkeypatch
):
    # gh exits 0 but one stored asset does not match the file sent.
    releases = Releases("v1.3.0")
    original = releases.gh

    def gh(*args):
        result = original(*args)
        if args[1] == "upload":
            releases.release["assets"][0]["digest"] = "sha256:" + "0" * 64
        return result

    monkeypatch.setattr(helper, "gh", gh)
    with pytest.raises(helper.ReleaseError, match="differs from Maven Central"):
        helper.complete(tree, "1.3.0", dist)
    assert "edit" not in releases.mutations()
    assert releases.release["isDraft"] is True


def test_complete_fails_when_the_draft_does_not_publish(
    helper, tree, dist, monkeypatch
):
    releases = Releases("v1.3.0", stays_draft=True)
    with pytest.raises(helper.ReleaseError, match="still a draft"):
        complete(helper, tree, dist, monkeypatch, releases)


def test_complete_refuses_a_view_of_another_tag(helper, tree, dist, monkeypatch):
    def gh(*args):
        body = json.dumps({"tagName": "v1.2.0", "isDraft": False, "assets": []})
        return subprocess.CompletedProcess(args, 0, body, "")

    monkeypatch.setattr(helper, "gh", gh)
    with pytest.raises(helper.ReleaseError, match="returned 'v1.2.0'"):
        helper.complete(tree, "1.3.0", dist)


@pytest.mark.parametrize(
    "stderr", ["HTTP 401: Bad credentials", "HTTP 502: Bad Gateway", "connection reset"]
)
def test_complete_stops_on_a_view_failure(helper, tree, dist, monkeypatch, stderr):
    # Only "release not found" means there is no Release; any other failure of
    # view must not be read as absence, or a re-run could create a second draft.
    calls = []

    def gh(*args):
        calls.append(args)
        if args[1] == "view":
            return subprocess.CompletedProcess(args, 1, "", stderr)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(helper, "gh", gh)
    with pytest.raises(helper.ReleaseError, match=stderr):
        helper.complete(tree, "1.3.0", dist)
    assert [args[1] for args in calls] == ["view"]


def test_complete_does_not_publish_after_a_failed_upload(
    helper, tree, dist, monkeypatch
):
    releases = Releases("v1.3.0")

    def gh(*args):
        if args[1] == "upload":
            releases.calls.append(args)
            return subprocess.CompletedProcess(args, 1, "", "upload interrupted")
        return releases.gh(*args)

    monkeypatch.setattr(helper, "gh", gh)
    with pytest.raises(helper.ReleaseError, match="upload interrupted"):
        helper.complete(tree, "1.3.0", dist)
    assert releases.release["isDraft"] is True
    assert "edit" not in releases.mutations()


@pytest.mark.parametrize("change", ["extra", "missing"])
def test_complete_refuses_a_directory_that_is_not_the_published_set(
    helper, tree, dist, monkeypatch, change
):
    if change == "extra":
        (dist / "notes.txt").write_text("x")
    else:
        (dist / names()[0]).unlink()
    releases = Releases("v1.3.0")
    with pytest.raises(helper.ReleaseError, match="does not hold exactly the 24 files"):
        complete(helper, tree, dist, monkeypatch, releases)
    assert releases.calls == []


@pytest.mark.parametrize("action", ["download", "complete"])
@pytest.mark.parametrize("version", ["v1.3.0", "1.3.0-1.20", "01.3.0", ""])
def test_cli_rejects_malformed_versions_before_any_work(
    helper, monkeypatch, action, version
):
    monkeypatch.setattr(helper, "download", lambda *a: pytest.fail("download ran"))
    monkeypatch.setattr(helper, "complete", lambda *a: pytest.fail("complete ran"))
    assert helper.main([action, version, "dist"]) == 1


@pytest.mark.parametrize("action", ["download", "complete"])
def test_cli_passes_a_bare_version_and_the_checkout(helper, monkeypatch, action):
    calls = []
    monkeypatch.setattr(helper, action, lambda *args: calls.append(args))
    assert helper.main([action, "1.3.0", "dist"]) == 0
    assert calls == [(Path.cwd(), "1.3.0", Path("dist"))]
