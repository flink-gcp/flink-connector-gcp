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
"""Tests for scripts/check-flink-api-tiers.py (issues #103, #249).

Offline and synthetic: the sources jars are built with zipfile into the cache
directory the script would otherwise download into, and urlopen is monkeypatched
to raise, so a test that reaches the network fails rather than merely being
slow. ROOT, CACHE and CONFIG are patched separately on purpose — CACHE is
derived from ROOT at import time, so moving ROOT alone would leave it pointing
at the real target/.

The parser is what these cover. It degrades fail-closed by design (a missed
class-level annotation demotes a type to "unannotated", which demands an
allowlist entry), and that only holds while the annotation shapes it does read
keep being read.

Exit codes: 0 clean, 1 policy violation, 2 infrastructure or config authoring.
"""

import http.client
import io
import urllib.error
import zipfile

import pytest

POM = "<project><properties>{property}</properties></project>"
FLINK_VERSION_PROPERTY = "<flink.version>{version}</flink.version>"
VERSION = "9.9.9"


def java(name, annotations="", kind="class", package="org.apache.flink.demo"):
    return (
        f"package {package};\n\n"
        f"/** Javadoc for {name}. */\n"
        f"{annotations}public {kind} {name} {{}}\n"
    )


def no_network(*args, **kwargs):
    raise AssertionError("the test suite must not download sources jars")


@pytest.fixture()
def root(tmp_path, check_flink_api_tiers, monkeypatch):
    (tmp_path / "pom.xml").write_text(
        POM.format(property=FLINK_VERSION_PROPERTY.format(version=VERSION))
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setattr(check_flink_api_tiers, "ROOT", tmp_path)
    monkeypatch.setattr(check_flink_api_tiers, "CACHE", cache)
    monkeypatch.setattr(check_flink_api_tiers, "CONFIG", tmp_path / "tiers.toml")
    monkeypatch.setattr(check_flink_api_tiers.urllib.request, "urlopen", no_network)
    monkeypatch.setattr("sys.argv", ["check-flink-api-tiers.py"])
    return tmp_path


def write_import(root, fqcn, module="conn", tree="java", name="User.java"):
    path = root / module / "src" / "main" / tree / "io" / "github" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"package io.github;\n\nimport {fqcn};\n\nclass X {{}}\n")


def write_jar(root, artifact, entries, version=VERSION):
    """Seed the cache with a sources jar, which is what keeps this offline."""
    jar = root / "cache" / f"{artifact}-{version}-sources.jar"
    with zipfile.ZipFile(jar, "w") as archive:
        for name, body in entries.items():
            archive.writestr(name, body)
    return jar


def write_config(root, artifacts=("flink-core",), **tables):
    lines = ["artifacts = [" + ", ".join(f'"{a}"' for a in artifacts) + "]", ""]
    for table, entries in tables.items():
        for fqcn in entries:
            lines += [f'[{table}."{fqcn}"]', 'reason = "Because, at length."', ""]
    (root / "tiers.toml").write_text("\n".join(lines))


def exit_code(module) -> int:
    try:
        return module.main()
    except SystemExit as error:
        return error.code


# --- classification ---


def classify(module, source, entry="org/apache/flink/demo/Demo.java", nested=()):
    return module.classify(source, entry, list(nested))


@pytest.mark.parametrize(
    ("annotations", "expected"),
    [
        ("@Public\n", "Public"),
        ("@PublicEvolving\n", "PublicEvolving"),
        ("@Experimental\n", "Experimental"),
        ("@Internal\n", "Internal"),
        ("", "unannotated"),
        ("@Deprecated\n", "unannotated"),
        # Flink does dual-annotate (ExternallyInducedSourceReader in 2.2.1):
        # the weaker guarantee governs, whichever order they are written in.
        ("@Experimental\n@PublicEvolving\n", "Experimental"),
        ("@PublicEvolving\n@Experimental\n", "Experimental"),
        # One level of nesting inside annotation arguments, as TaskManagerOptions
        # has: anchoring at the first `)` would demote this to unannotated.
        ('@ConfigGroups(groups = @ConfigGroup(name = "a"))\n@Internal\n', "Internal"),
        ("@Internal\npublic abstract\n", "Internal"),
        # ConfigGroup.java's own shape: empty argument parens, and the whole
        # block on the declaration's line (surveyed 2026-08-02 over the 4,864
        # sources-jar entries at 2.2.1 — 55 carry annotation arguments).
        ("@Target({}) @Internal public ", "Internal"),
    ],
)
def test_class_level_annotations_decide_the_tier(
    check_flink_api_tiers, annotations, expected
):
    source = f"package org.apache.flink.demo;\n\n{annotations}class Demo {{}}\n"
    assert classify(check_flink_api_tiers, source) == expected


def test_a_fully_qualified_annotation_is_read(
    check_flink_api_tiers,
):
    source = "@org.apache.flink.annotation.Internal\npublic class Demo {}\n"
    assert classify(check_flink_api_tiers, source) == "Internal"


def test_invalid_java_is_an_infrastructure_error(check_flink_api_tiers):
    with pytest.raises(SystemExit) as error:
        classify(check_flink_api_tiers, "public class Demo {\n")
    assert error.value.code == 2


@pytest.mark.parametrize("kind", ["class", "interface", "enum", "record", "@interface"])
def test_every_declaration_kind_is_read(check_flink_api_tiers, kind):
    parameters = "()" if kind == "record" else ""
    source = f"@Internal\npublic {kind} Demo{parameters} {{}}\n"
    assert classify(check_flink_api_tiers, source) == "Internal"


def test_a_method_level_annotation_does_not_reach_the_class(check_flink_api_tiers):
    # The whole reason this script reads sources rather than class files: a
    # class file's constant pool lists method annotations too (issue #103).
    source = "@Public\npublic class Demo {\n  @Internal\n  void hidden() {}\n}\n"
    assert classify(check_flink_api_tiers, source) == "Public"


def test_a_nested_type_is_classified_by_its_own_declaration(check_flink_api_tiers):
    source = (
        "@Public\npublic class Demo {\n  @Internal\n  public static class Inner {}\n}\n"
    )
    assert classify(check_flink_api_tiers, source, nested=["Inner"]) == "Internal"


def test_an_unannotated_nested_type_falls_back_to_the_file(check_flink_api_tiers):
    source = "@Internal\npublic class Demo {\n  public static class Inner {}\n}\n"
    assert classify(check_flink_api_tiers, source, nested=["Inner"]) == "Internal"


def test_a_source_with_no_declaration_is_an_infrastructure_error(
    check_flink_api_tiers,
):
    with pytest.raises(SystemExit) as error:
        classify(check_flink_api_tiers, "package org.apache.flink.demo;\n")
    assert error.value.code == 2


# --- resolution of an import to a jar entry ---


def test_an_import_resolves_to_its_own_entry(check_flink_api_tiers):
    index = {"org/apache/flink/demo/Demo.java": ("flink-core", None)}
    assert check_flink_api_tiers.resolve("org.apache.flink.demo.Demo", index) == (
        "org/apache/flink/demo/Demo.java",
        [],
    )


def test_a_nested_import_drops_segments_until_a_file_matches(check_flink_api_tiers):
    index = {"org/apache/flink/demo/Demo.java": ("flink-core", None)}
    assert check_flink_api_tiers.resolve(
        "org.apache.flink.demo.Demo.Inner.Leaf", index
    ) == ("org/apache/flink/demo/Demo.java", ["Inner", "Leaf"])


def test_an_unresolvable_import_is_an_infrastructure_error(check_flink_api_tiers):
    with pytest.raises(SystemExit) as error:
        check_flink_api_tiers.resolve("org.apache.flink.gone.Type", {})
    assert error.value.code == 2


# --- reading the tree ---


def test_imports_are_collected_from_every_per_major_source_root(
    root, check_flink_api_tiers
):
    write_import(root, "org.apache.flink.demo.Two", tree="java-flink2", name="A.java")
    write_import(root, "org.apache.flink.demo.One", tree="java-flink1", name="B.java")
    write_import(root, "static org.apache.flink.demo.Util.check", name="C.java")
    assert check_flink_api_tiers.collect_imports() == {
        "org.apache.flink.demo.One",
        "org.apache.flink.demo.Two",
        "org.apache.flink.demo.Util.check",
    }


def test_import_whitespace_does_not_hide_a_flink_import(root, check_flink_api_tiers):
    source = root / "conn/src/main/java/io/github/User.java"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        "package io.github;\n\nimport\torg.apache.flink.demo.One;\n\nclass X {}\n"
    )
    assert check_flink_api_tiers.collect_imports() == {"org.apache.flink.demo.One"}


def test_a_flink_wildcard_import_names_the_fixable_source_error(
    root, check_flink_api_tiers, capsys
):
    write_import(root, "org.apache.flink.demo.*")

    with pytest.raises(SystemExit) as error:
        check_flink_api_tiers.collect_imports()

    stderr = capsys.readouterr().err
    assert error.value.code == 2
    assert "User.java:3: wildcard import org.apache.flink.demo.*" in stderr
    assert "replace it with explicit type imports" in stderr


def test_a_tree_with_no_flink_imports_is_an_infrastructure_error(
    root, check_flink_api_tiers
):
    write_import(root, "java.util.List")
    with pytest.raises(SystemExit) as error:
        check_flink_api_tiers.collect_imports()
    assert error.value.code == 2


def test_the_flink_version_comes_from_the_pom(root, check_flink_api_tiers):
    assert check_flink_api_tiers.flink_version() == VERSION


def test_the_flink_version_is_stripped_of_surrounding_whitespace(
    root, check_flink_api_tiers
):
    # The regex tolerates whitespace inside the tags; the jar URLs, the cache
    # filenames and the CI cache key (a $GITHUB_OUTPUT `key=value` line, which
    # a newline would malform) all want the bare version.
    (root / "pom.xml").write_text(
        POM.format(property=f"<flink.version>\n  {VERSION}\n</flink.version>")
    )
    assert check_flink_api_tiers.flink_version() == VERSION


def test_a_pom_without_a_flink_version_is_an_infrastructure_error(
    root, check_flink_api_tiers
):
    (root / "pom.xml").write_text("<project/>")
    with pytest.raises(SystemExit) as error:
        check_flink_api_tiers.flink_version()
    assert error.value.code == 2


def test_the_version_flag_prints_it_and_audits_nothing(
    root, check_flink_api_tiers, monkeypatch, capsys
):
    # CI keys the sources-jar cache on flink.version through this flag, so
    # the workflow and the audit cannot disagree about how the pom is read.
    # No config, imports or jars exist under `root`, so anything beyond
    # printing the version would fail loudly here.
    monkeypatch.setattr(
        "sys.argv", ["check-flink-api-tiers.py", "--print-flink-version"]
    )
    assert exit_code(check_flink_api_tiers) == 0
    assert capsys.readouterr().out.strip() == VERSION


def test_the_first_jar_owning_an_entry_wins(root, check_flink_api_tiers):
    write_jar(root, "flink-core", {"org/apache/flink/demo/Demo.java": java("Demo")})
    write_jar(root, "flink-runtime", {"org/apache/flink/demo/Demo.java": java("Demo")})
    index = check_flink_api_tiers.build_index(["flink-core", "flink-runtime"], VERSION)
    assert index["org/apache/flink/demo/Demo.java"][0] == "flink-core"


def test_a_cached_file_that_is_not_a_zip_is_removed_and_reported(
    root, check_flink_api_tiers
):
    # An HTTP 200 that was an outage page: without the unlink it would satisfy
    # the cache check on every later run.
    jar = root / "cache" / f"flink-core-{VERSION}-sources.jar"
    jar.write_text("<html>maintenance</html>")
    with pytest.raises(SystemExit) as error:
        check_flink_api_tiers.build_index(["flink-core"], VERSION)
    assert error.value.code == 2
    assert not jar.exists()


# --- the download the cache exists to avoid ---


def test_a_cache_hit_needs_no_download(root, check_flink_api_tiers):
    # urlopen raises for the whole file, so this asserts the short circuit.
    jar = write_jar(root, "flink-core", {})
    assert check_flink_api_tiers.sources_jar("flink-core", VERSION) == jar


def test_a_download_lands_in_the_cache_leaving_no_partial_file(
    root, check_flink_api_tiers, monkeypatch
):
    body = b"PK\x03\x04 pretend jar"
    monkeypatch.setattr(
        check_flink_api_tiers.urllib.request,
        "urlopen",
        lambda *a, **k: io.BytesIO(body),
    )
    jar = check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert jar.read_bytes() == body
    # The cache ends up holding the finished jar and nothing under the
    # temporary name the download writes through.
    assert [path.name for path in sorted((root / "cache").iterdir())] == [jar.name]


def http_error(code, body=None):
    """A urllib HTTPError carrying an open response body, as urlopen raises."""
    return urllib.error.HTTPError(
        url="https://repo1.example/sources.jar",
        code=code,
        msg="from Maven Central",
        hdrs=None,
        fp=body if body is not None else io.BytesIO(b"response body"),
    )


def stub_download(check_flink_api_tiers, monkeypatch, responses):
    """Feed urlopen one scripted response per call, recording calls and sleeps.

    An exception in `responses` is raised, bytes are returned as the body. A
    call past the end of the script raises IndexError, so a retry the test
    did not budget for fails rather than looping.
    """
    calls, sleeps = [], []

    def urlopen(*args, **kwargs):
        calls.append(args)
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return io.BytesIO(response)

    monkeypatch.setattr(check_flink_api_tiers.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(check_flink_api_tiers.time, "sleep", sleeps.append)
    return calls, sleeps


@pytest.mark.parametrize(
    "error",
    [
        OSError("connection reset"),
        # Not an OSError, which is the whole reason the except clause names
        # http.client.HTTPException as well: a truncated body raises this.
        http.client.IncompleteRead(b"half a jar"),
    ],
)
def test_a_failed_download_is_an_infrastructure_error(
    root, check_flink_api_tiers, monkeypatch, error
):
    calls, sleeps = stub_download(check_flink_api_tiers, monkeypatch, [error])
    with pytest.raises(SystemExit) as raised:
        check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert raised.value.code == 2
    # A transport failure is not retried: only a status Maven Central chose to
    # send (429/5xx) says anything about trying again.
    assert len(calls) == 1
    assert sleeps == []
    # And nothing reaches the cache, so the next run retries rather than
    # reusing whatever the failure left.
    assert list((root / "cache").iterdir()) == []


def test_a_rate_limited_download_is_retried_with_backoff(
    root, check_flink_api_tiers, monkeypatch, capsys
):
    # The failure mode of issue #769: Maven Central rate-limits the shared
    # egress IPs of GitHub-hosted runners, so a 429 says nothing about the
    # change under test and is worth a second try.
    body = b"PK\x03\x04 pretend jar"
    error_bodies = [io.BytesIO(b"slow down"), io.BytesIO(b"slow down")]
    # The list holds the errors alive: were they reclaimed, the interpreter
    # would close their bodies itself and the closure assertion below would
    # pass without error.close() in the code under test.
    errors = [http_error(429, error_body) for error_body in error_bodies]
    calls, sleeps = stub_download(check_flink_api_tiers, monkeypatch, [*errors, body])
    jar = check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert jar.read_bytes() == body
    assert [path.name for path in sorted((root / "cache").iterdir())] == [jar.name]
    assert len(calls) == 3
    assert sleeps == [2.0, 4.0]
    # Each abandoned attempt closed its response body rather than leaking it.
    assert all(error_body.closed for error_body in error_bodies)
    # A slow CI run explains itself in the log, and the log tells the truth
    # about what was slept and which attempt it was.
    err = capsys.readouterr().err
    attempts = check_flink_api_tiers.RETRY_ATTEMPTS
    assert f"retrying in 2 s (attempt 1 of {attempts})" in err
    assert f"retrying in 4 s (attempt 2 of {attempts})" in err


@pytest.mark.parametrize("code", [429, 500, 503])
def test_persistent_rate_limiting_exhausts_the_retries(
    root, check_flink_api_tiers, monkeypatch, capsys, code
):
    attempts = check_flink_api_tiers.RETRY_ATTEMPTS
    calls, sleeps = stub_download(
        check_flink_api_tiers,
        monkeypatch,
        [http_error(code) for _ in range(attempts)],
    )
    with pytest.raises(SystemExit) as raised:
        check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert raised.value.code == 2
    assert len(calls) == attempts
    assert sleeps == [2.0, 4.0, 8.0]
    err = capsys.readouterr().err
    assert f"after {attempts} attempts" in err
    assert str(code) in err
    # The 404 hint would mislead here: the artifacts list is not the problem.
    assert "artifacts list" not in err
    assert list((root / "cache").iterdir()) == []


def test_a_missing_artifact_is_fatal_without_retry(
    root, check_flink_api_tiers, monkeypatch, capsys
):
    # A 404 means the artifacts list names something that does not exist at
    # this flink.version; retrying would hide exactly that.
    error_body = io.BytesIO(b"not found")
    error = http_error(404, error_body)  # held alive, as in the retry test
    calls, sleeps = stub_download(check_flink_api_tiers, monkeypatch, [error])
    with pytest.raises(SystemExit) as raised:
        check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert raised.value.code == 2
    assert len(calls) == 1
    assert sleeps == []
    assert error_body.closed
    assert "A 404 usually means" in capsys.readouterr().err


def test_a_client_error_other_than_404_is_fatal_without_retry(
    root, check_flink_api_tiers, monkeypatch, capsys
):
    # A 403 is neither the repository's fault (404's hint would mislead) nor
    # transient (retrying would just wait through three backoffs to fail).
    calls, sleeps = stub_download(check_flink_api_tiers, monkeypatch, [http_error(403)])
    with pytest.raises(SystemExit) as raised:
        check_flink_api_tiers.sources_jar("flink-core", VERSION)
    assert raised.value.code == 2
    assert len(calls) == 1
    assert sleeps == []
    err = capsys.readouterr().err
    assert "403" in err
    assert "artifacts list" not in err


# --- config authoring ---


def test_an_unknown_top_level_table_is_an_infrastructure_error(
    root, check_flink_api_tiers
):
    (root / "tiers.toml").write_text('artifacts = ["flink-core"]\n\n[intrenal]\n')
    assert exit_code(check_flink_api_tiers) == 2


def test_an_empty_artifacts_list_is_an_infrastructure_error(
    root, check_flink_api_tiers
):
    (root / "tiers.toml").write_text("artifacts = []\n")
    assert exit_code(check_flink_api_tiers) == 2


def test_malformed_toml_is_an_infrastructure_error(root, check_flink_api_tiers):
    (root / "tiers.toml").write_text("artifacts = [\n")
    assert exit_code(check_flink_api_tiers) == 2


@pytest.mark.parametrize("entry", ['reason = "  "', 'note = "typo\'d key"', ""])
def test_an_entry_without_a_reason_is_an_infrastructure_error(
    root, check_flink_api_tiers, entry
):
    (root / "tiers.toml").write_text(
        f'artifacts = ["flink-core"]\n\n[internal."org.apache.flink.demo.Demo"]\n{entry}\n'
    )
    assert exit_code(check_flink_api_tiers) == 2


# --- the audit, end to end ---


def audit_tree(root, tier, allowlist=None, artifacts=("flink-core",)):
    annotations = "" if tier is None else f"@{tier}\n"
    write_import(root, "org.apache.flink.demo.Demo")
    write_jar(
        root,
        "flink-core",
        {"org/apache/flink/demo/Demo.java": java("Demo", annotations)},
    )
    write_config(root, artifacts=artifacts, **(allowlist or {}))


def test_a_public_import_needs_no_entry(root, check_flink_api_tiers):
    audit_tree(root, "Public")
    assert exit_code(check_flink_api_tiers) == 0


@pytest.mark.parametrize(
    ("tier", "table"),
    [("Internal", "internal"), ("Experimental", "experimental"), (None, "unannotated")],
)
def test_an_unstable_import_needs_an_entry(
    root, check_flink_api_tiers, capsys, tier, table
):
    audit_tree(root, tier)
    assert exit_code(check_flink_api_tiers) == 1
    assert f"[{table}] entry" in capsys.readouterr().err

    audit_tree(root, tier, allowlist={table: ["org.apache.flink.demo.Demo"]})
    assert exit_code(check_flink_api_tiers) == 0


def test_an_entry_the_sources_no_longer_import_is_stale(
    root, check_flink_api_tiers, capsys
):
    audit_tree(
        root, "Public", allowlist={"internal": ["org.apache.flink.demo.Vanished"]}
    )
    assert exit_code(check_flink_api_tiers) == 1
    assert "is stale" in capsys.readouterr().err


def test_an_entry_filed_under_the_wrong_tier_fails_from_both_ends(
    root, check_flink_api_tiers, capsys
):
    # A type that moved tier reads as one unlisted import plus one stale entry,
    # which is what tells the reader to re-file rather than to add.
    audit_tree(
        root, "Experimental", allowlist={"internal": ["org.apache.flink.demo.Demo"]}
    )
    assert exit_code(check_flink_api_tiers) == 1
    err = capsys.readouterr().err
    assert "has no [experimental] entry" in err
    assert "[internal] entry org.apache.flink.demo.Demo is stale" in err


def test_an_artifact_owning_no_imported_type_fails(root, check_flink_api_tiers, capsys):
    audit_tree(root, "Public", artifacts=("flink-core", "flink-metrics-core"))
    write_jar(root, "flink-metrics-core", {"org/apache/flink/metrics/Counter.java": ""})
    assert exit_code(check_flink_api_tiers) == 1
    assert "flink-metrics-core owns no imported type" in capsys.readouterr().err


# --- the wiring verify.yaml's cache steps share with this script ---


def test_the_ci_cache_wiring_matches_this_scripts_constants(check_flink_api_tiers):
    # verify.yaml's api_tiers job caches the directory this script downloads
    # into, keys it on the config file this script reads, and derives the
    # version through this script's own flag. None of that is visible to the
    # audit itself — a drifted path or key still passes, it just re-downloads
    # on every run, which is the silent failure issue #769 exists to close —
    # so the coupling is pinned here, in the style of test_ci_gate.py's
    # wiring tests. Deliberately not the `root` fixture: this reads the real
    # tree the workflow runs against.
    module_root = check_flink_api_tiers.ROOT
    text = (module_root / ".github" / "workflows" / "verify.yaml").read_text()
    cache_dir = check_flink_api_tiers.CACHE.relative_to(module_root).as_posix()
    config = check_flink_api_tiers.CONFIG.relative_to(module_root).as_posix()
    version = "${{ steps.flink_version.outputs.version }}"
    key = "api-tier-jars-" + version + "-${{ hashFiles('" + config + "') }}"
    # Counted, not just present: restore and save each repeat the path and
    # the key, so a substring match would hold while one of the pair drifts.
    assert text.count(f"path: {cache_dir}") == 2
    assert text.count(f"key: {key}") == 2
    assert f"restore-keys: api-tier-jars-{version}-" in text
    assert "--print-flink-version" in text
