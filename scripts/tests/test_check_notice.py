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
"""Tests for scripts/check-notice.py (issues #138, #249).

Synthetic and offline: a module directory in tmp_path carrying its own
THIRD-PARTY report, template and META-INF tree, with SOURCES monkeypatched onto
a licence-sources.toml written beside it. The jar-mode tests build real zips on
a fake runtime classpath; the url-mode tests monkeypatch urlopen, since
fetching is exactly what must not happen here.

What this file is protecting is a licensing obligation, so the assertions are
about the failure directions rather than the happy path: an artifact that
parses out of the report is one nothing then requires the NOTICE to mention, a
missing licence text ships a jar without it, and a restricted licence is a
decision to make before any of this runs.

Exit codes: 0 clean, 1 everything else — this script has no infra/policy split.
"""

import hashlib
import io
import shutil
import subprocess
import sys
import zipfile

import pytest

TEMPLATE = """\
flink-sql-connector-gcp-demo
Copyright 2026 The flink-gcp authors

{paragraphs}"""


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def write_report(module, *entries, declared=None):
    """The license-maven-plugin report the recipes regenerate before this runs."""
    count = len(entries) if declared is None else declared
    lines = [f"Lists of {count} third-party dependencies."]
    lines += [
        f"     ({licence}) Some Artifact ({ga}:{version} - https://example.invalid)"
        for licence, ga, version in entries
    ]
    report = module / "target" / "generated-sources" / "license" / "THIRD-PARTY.txt"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n")
    return report


def write_template(module, *groups):
    paragraphs = "\n".join(f"{group}\n{{{{{group}}}}}\n" for group in groups)
    (module / "NOTICE.template").write_text(TEMPLATE.format(paragraphs=paragraphs))


def write_sources(path, entries):
    # The `[files]` header is unconditional: the real licence-sources.toml always
    # carries the table, and a file without it makes load_sources raise KeyError
    # rather than the failure under test — which turns a clean assertion into an
    # opaque traceback pointing at the fixture.
    lines = ["[files]", ""]
    for name, entry in entries.items():
        artifacts = ", ".join(f'"{artifact}"' for artifact in entry["artifacts"])
        pointer = (
            f'jar = "{entry["jar"]}"' if "jar" in entry else f'url = "{entry["url"]}"'
        )
        lines += [f'[files."{name}"]', f"artifacts = [{artifacts}]", pointer]
        if "version_strip_prefix" in entry:
            lines.append(f'version_strip_prefix = "{entry["version_strip_prefix"]}"')
        if entry.get("version_independent"):
            lines.append("version_independent = true")
        lines += [f'sha256 = "{entry["sha256"]}"', ""]
    path.write_text("\n".join(lines))


def write_jar(root, group, artifact_id, version, entries):
    """A jar at the repository layout obtain_text matches against."""
    directory = root / "m2" / group.replace(".", "/") / artifact_id / version
    directory.mkdir(parents=True, exist_ok=True)
    jar = directory / f"{artifact_id}-{version}.jar"
    with zipfile.ZipFile(jar, "w") as archive:
        for name, body in entries.items():
            archive.writestr(name, body)
    return jar


def write_classpath(module, *jars):
    path = module / "target" / "runtime-classpath.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(":".join(str(jar) for jar in jars))
    return path


@pytest.fixture()
def module(tmp_path, check_notice, monkeypatch):
    directory = tmp_path / "flink-sql-connector-gcp-demo"
    (directory / "src" / "main" / "resources" / "META-INF" / "licenses").mkdir(
        parents=True
    )
    monkeypatch.setattr(check_notice, "SOURCES", tmp_path / "licence-sources.toml")
    # ROOT is what the dead-entry check discovers shaded modules under. Pointed at
    # the synthetic tree, never the real repository.
    monkeypatch.setattr(check_notice, "ROOT", tmp_path)
    return directory


def notice_path(module):
    return module / "src" / "main" / "resources" / "META-INF" / "NOTICE"


@pytest.fixture()
def run(check_notice, monkeypatch):
    """Drive main() the way the CLI does, returning its exit code."""

    def invoke(module, *flags) -> int:
        monkeypatch.setattr(sys, "argv", ["check-notice.py", *flags, str(module)])
        try:
            return check_notice.main()
        except SystemExit as error:
            return error.code

    return invoke


# --- reading the resolved bundle ---


def test_the_report_is_parsed_into_gav_to_licence(module, check_notice):
    write_report(
        module,
        ("Apache License, Version 2.0", "com.google.guava:guava", "33.0.0-jre"),
        ("BSD-3-Clause", "com.google.api:gax", "2.0.0"),
    )
    assert check_notice.read_resolved(module) == {
        "com.google.guava:guava:33.0.0-jre": "Apache License, Version 2.0",
        "com.google.api:gax:2.0.0": "BSD-3-Clause",
    }


def test_a_partial_parse_fails_rather_than_checking_what_it_could(
    module, check_notice, capsys
):
    # The dangerous direction: an artifact the regex cannot read is simply
    # absent, and nothing then demands the NOTICE mention it.
    write_report(module, ("MIT", "com.example:one", "1.0"), declared=2)
    with pytest.raises(SystemExit) as error:
        check_notice.read_resolved(module)
    assert error.value.code == 1
    assert "1 could be parsed" in capsys.readouterr().err


def test_a_report_without_its_count_header_fails(module, check_notice):
    report = write_report(module, ("MIT", "com.example:one", "1.0"))
    report.write_text(report.read_text().replace("Lists of", "Listing"))
    with pytest.raises(SystemExit) as error:
        check_notice.read_resolved(module)
    assert error.value.code == 1


def test_a_missing_report_fails(module, check_notice):
    with pytest.raises(SystemExit) as error:
        check_notice.read_resolved(module)
    assert error.value.code == 1


# --- the restricted-licence gate ---


@pytest.mark.parametrize(
    "licence",
    [
        "GPLv2",
        "GPL-2.0",
        "GPL 2",
        "LGPLv3",
        "AGPL-3.0",
        "GNU General Public License, version 2",
        "GNU Lesser General Public License",
        "SSPL",
        "Server Side Public License",
        "Business Source License 1.1",
        "BUSL-1.1",
        "Apache 2.0 with Commons Clause",
        "Elastic License 2.0",
        "Creative Commons Non-Commercial",
        "Noncommercial licence",
        # A dual licence whose other arm is permissive is still restricted: the
        # gate makes electing an arm a decision, not a default. This exact
        # spelling is javax.annotation-api's, excluded from both bundles.
        "CDDL + GPLv2 with classpath exception",
    ],
)
def test_restricted_licences_are_recognised(check_notice, licence):
    assert check_notice.RESTRICTED.search(licence)


@pytest.mark.parametrize(
    "licence",
    [
        "Apache License, Version 2.0",
        "Apache-2.0",
        "MIT",
        "BSD-3-Clause",
        "EPL 2.0",
        "CDDL 1.1",
        "The Go license",
    ],
)
def test_permissive_licences_pass_the_gate(check_notice, licence):
    assert not check_notice.RESTRICTED.search(licence)


@pytest.mark.parametrize(
    ("licence", "ga"),
    [
        ("GPLv2", "com.example:copyleft"),
        # The dual-licensed one, whose licence *text* and template paragraph the
        # module could satisfy: the gate must still answer, because satisfying
        # them means electing an arm on this project's behalf. javax.annotation-api
        # resolved to exactly this and is excluded from both bundles, so this case
        # is what reports it arriving again.
        (
            "CDDL + GPLv2 with classpath exception",
            "javax.annotation:javax.annotation-api",
        ),
    ],
)
def test_a_restricted_dependency_stops_before_any_notice_work(
    module, run, check_notice, capsys, licence, ga
):
    # The template deliberately has a paragraph for neither, so rendering would
    # fail too: the gate is what must answer, and it must answer first. The
    # message is "decide adoption", not "add a paragraph".
    write_report(module, (licence, ga, "1.0"))
    write_template(module, "Apache-2.0")
    write_sources(check_notice.SOURCES, {})
    assert run(module) == 1
    assert "discuss adoption first" in capsys.readouterr().err
    # What "stops before any notice work" means, asserted rather than inferred
    # from the message: moving the gate below render_notice fails here.
    assert not notice_path(module).exists()


# --- the licence-source index ---


def test_one_artifact_under_two_entries_fails(module, check_notice, capsys):
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.a": {
                "artifacts": ["com.example:shared"],
                "url": "https://example.invalid/a",
                "version_independent": True,
                "sha256": "0" * 64,
            },
            "LICENSE.b": {
                "artifacts": ["com.example:shared"],
                "url": "https://example.invalid/b",
                "version_independent": True,
                "sha256": "0" * 64,
            },
        },
    )
    with pytest.raises(SystemExit) as error:
        check_notice.load_sources()
    assert error.value.code == 1
    assert "appears under both" in capsys.readouterr().err


def url_entry(url, **keys):
    return {"artifacts": ["com.example:lib"], "url": url, "sha256": "0" * 64, **keys}


def load_failure(check_notice, capsys, entries) -> str:
    write_sources(check_notice.SOURCES, entries)
    with pytest.raises(SystemExit) as error:
        check_notice.load_sources()
    assert error.value.code == 1
    return capsys.readouterr().err


def test_a_literal_url_that_declares_nothing_fails(module, check_notice, capsys):
    # The issue #343 shape: a tag-pinned url that --update happily re-fetches
    # after the artifact it describes has moved on. The entry has to say which
    # kind it is.
    err = load_failure(
        check_notice,
        capsys,
        {"LICENSE.lib": url_entry("https://example.invalid/v1.0/LICENSE")},
    )
    assert "neither templates {version} nor declares version_independent" in err


def test_a_templated_url_that_also_declares_independence_fails(
    module, check_notice, capsys
):
    err = load_failure(
        check_notice,
        capsys,
        {
            "LICENSE.lib": url_entry(
                "https://example.invalid/v{version}/LICENSE",
                version_independent=True,
            )
        },
    )
    assert "version-dependent by construction" in err


@pytest.mark.parametrize(
    "entry",
    [
        # On a version-independent url, and on a jar source: neither has a
        # {version} template for the prefix to act on.
        url_entry(
            "https://example.invalid/LICENSE",
            version_independent=True,
            version_strip_prefix="4.",
        ),
        {
            "artifacts": ["com.example:lib"],
            "jar": "META-INF/LICENSE",
            "version_strip_prefix": "4.",
            "sha256": "0" * 64,
        },
    ],
)
def test_a_strip_prefix_without_a_template_fails(module, check_notice, capsys, entry):
    err = load_failure(check_notice, capsys, {"LICENSE.lib": entry})
    assert "no {version} url template" in err


def test_the_two_valid_url_shapes_load(module, check_notice):
    # The success side, pinned so the validation cannot drift into rejecting
    # the real file's entries: a strip-prefixed template (protobuf's shape)
    # and a declared-independent literal (gax's shape) both load.
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.template": {
                "artifacts": ["com.example:templated"],
                "url": "https://example.invalid/v{version}/LICENSE",
                "version_strip_prefix": "4.",
                "sha256": "0" * 64,
            },
            "LICENSE.frozen": {
                "artifacts": ["com.example:frozen"],
                "url": "https://example.invalid/master/LICENSE",
                "version_independent": True,
                "sha256": "0" * 64,
            },
        },
    )
    assert set(check_notice.load_sources()) == {"LICENSE.template", "LICENSE.frozen"}


def test_an_independence_declaration_on_a_jar_source_fails(
    module, check_notice, capsys
):
    # The flag describes a url's relationship to the artifact version; left on
    # an entry converted to jar: it would sit dormant and lie about the shape.
    err = load_failure(
        check_notice,
        capsys,
        {
            "LICENSE.lib": {
                "artifacts": ["com.example:lib"],
                "jar": "META-INF/LICENSE",
                "version_independent": True,
                "sha256": "0" * 64,
            }
        },
    )
    assert "version_independent on a jar source" in err


def test_an_unknown_key_fails(module, check_notice, capsys):
    # version_independent misspelled would otherwise be silently dropped, and
    # the failure that then fires argues about the wrong thing.
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.lib": url_entry(
                "https://example.invalid/LICENSE", version_independent=True
            )
        },
    )
    with check_notice.SOURCES.open("a") as handle:
        handle.write("version_independant = true\n")
    with pytest.raises(SystemExit) as error:
        check_notice.load_sources()
    assert error.value.code == 1
    assert "unknown keys ['version_independant']" in capsys.readouterr().err


# --- resolving the version a url template fetches at ---


def test_a_strip_prefix_drops_the_java_major_offset(check_notice):
    # protobuf: the Java artifact 4.33.2 releases from tag v33.2.
    entry = {
        "artifacts": ["com.google.protobuf:protobuf-java"],
        "version_strip_prefix": "4.",
    }
    resolved = {"com.google.protobuf:protobuf-java:4.33.2": "BSD-3-Clause"}
    assert check_notice.resolve_url_version("LICENSE.p", entry, resolved) == "33.2"


def test_the_version_comes_from_whichever_entry_artifact_is_bundled(check_notice):
    # A module may bundle only some of an entry's artifacts; the version must
    # come from the ones it does bundle, not blindly from the first listed.
    entry = {
        "artifacts": [
            "com.google.protobuf:protobuf-java",
            "com.google.protobuf:protobuf-java-util",
        ]
    }
    resolved = {"com.google.protobuf:protobuf-java-util:4.33.2": "BSD-3-Clause"}
    assert check_notice.resolve_url_version("LICENSE.p", entry, resolved) == "4.33.2"


def test_a_strip_prefix_the_version_does_not_carry_fails(check_notice, capsys):
    # The day protobuf's Java line moves to 5.x the encoded offset is wrong,
    # and the answer is a human re-deriving the tag scheme, not v.1.0 fetched.
    entry = {
        "artifacts": ["com.google.protobuf:protobuf-java"],
        "version_strip_prefix": "4.",
    }
    resolved = {"com.google.protobuf:protobuf-java:5.1.0": "BSD-3-Clause"}
    with pytest.raises(SystemExit) as error:
        check_notice.resolve_url_version("LICENSE.p", entry, resolved)
    assert error.value.code == 1
    assert "does not start with" in capsys.readouterr().err


def test_two_resolved_versions_under_one_entry_fail(check_notice, capsys):
    # One shared licence text cannot correspond to two versions at once.
    entry = {"artifacts": ["com.example:a", "com.example:b"]}
    resolved = {"com.example:a:1.0": "MIT", "com.example:b:2.0": "MIT"}
    with pytest.raises(SystemExit) as error:
        check_notice.resolve_url_version("LICENSE.ab", entry, resolved)
    assert error.value.code == 1
    assert "['1.0', '2.0']" in capsys.readouterr().err


def test_an_entry_whose_artifacts_are_not_bundled_fails(check_notice, capsys):
    entry = {"artifacts": ["com.example:lib"]}
    with pytest.raises(SystemExit) as error:
        check_notice.resolve_url_version("LICENSE.lib", entry, {"c:other:1.0": "MIT"})
    assert error.value.code == 1
    assert "exactly one resolved version" in capsys.readouterr().err


# --- rendering the NOTICE ---


def render(check_notice, module, resolved, files=None):
    return check_notice.render_notice(module / "NOTICE.template", resolved, files or {})


def test_apache_bullets_carry_no_pointer_and_others_do(module, check_notice):
    write_template(module, "Apache-2.0", "BSD-3-Clause")
    rendered = render(
        check_notice,
        module,
        {
            "com.example:b:2.0": "Apache-2.0",
            "com.example:a:1.0": "Apache-2.0",
            "com.google.api:gax:2.0.0": "BSD-3-Clause",
        },
        {"LICENSE.gax": {"artifacts": ["com.google.api:gax"]}},
    )
    assert rendered.splitlines()[3:] == [
        "Apache-2.0",
        "- com.example:a:1.0",
        "- com.example:b:2.0",
        "",
        "BSD-3-Clause",
        "- com.google.api:gax:2.0.0 (META-INF/licenses/LICENSE.gax)",
    ]


def test_a_paragraph_no_artifact_resolves_to_fails(module, check_notice, capsys):
    write_template(module, "Apache-2.0", "BSD-3-Clause")
    with pytest.raises(SystemExit) as error:
        render(check_notice, module, {"com.example:a:1.0": "Apache-2.0"})
    assert error.value.code == 1
    assert "no bundled artifact" in capsys.readouterr().err


def test_two_paragraphs_for_one_group_fail(module, check_notice, capsys):
    write_template(module, "Apache-2.0", "Apache-2.0")
    with pytest.raises(SystemExit) as error:
        render(check_notice, module, {"com.example:a:1.0": "Apache-2.0"})
    assert error.value.code == 1
    assert "two paragraphs" in capsys.readouterr().err


def test_a_bundled_licence_the_template_ignores_fails(module, check_notice, capsys):
    write_template(module, "Apache-2.0")
    with pytest.raises(SystemExit) as error:
        render(
            check_notice,
            module,
            {"com.example:a:1.0": "Apache-2.0", "com.example:b:1.0": "MIT"},
        )
    assert error.value.code == 1
    assert "no paragraph for: ['MIT']" in capsys.readouterr().err


def test_a_non_apache_artifact_with_no_source_entry_fails(module, check_notice, capsys):
    write_template(module, "BSD-3-Clause")
    with pytest.raises(SystemExit) as error:
        render(check_notice, module, {"com.google.api:gax:2.0.0": "BSD-3-Clause"})
    assert error.value.code == 1
    # The curation ladder is the actionable half of this message.
    assert "1. a licence file inside the artifact's own jar" in capsys.readouterr().err


# --- obtaining a licence text ---


def test_a_jar_entry_is_read_from_the_bundled_artifact(tmp_path, module, check_notice):
    text = b"BSD-3-Clause text\n"
    jar = write_jar(
        tmp_path, "com.google.api", "gax", "2.0.0", {"META-INF/LICENSE": text}
    )
    # gax-grpc and gax-httpjson are on the classpath too: matching on the
    # file-name prefix alone would find three jars and fail its own uniqueness
    # guard, which is why the match is on the repository layout.
    siblings = [
        write_jar(tmp_path, "com.google.api", name, "2.0.0", {"META-INF/LICENSE": b"x"})
        for name in ("gax-grpc", "gax-httpjson")
    ]
    write_classpath(module, jar, *siblings)
    entry = {
        "artifacts": ["com.google.api:gax"],
        "jar": "META-INF/LICENSE",
        "sha256": sha256(text),
    }
    assert check_notice.obtain_text("LICENSE.gax", entry, module, {}) == text


def test_a_jar_entry_whose_hash_moved_fails(tmp_path, module, check_notice, capsys):
    jar = write_jar(
        tmp_path, "com.example", "lib", "1.0", {"META-INF/LICENSE": b"edited\n"}
    )
    write_classpath(module, jar)
    entry = {
        "artifacts": ["com.example:lib"],
        "jar": "META-INF/LICENSE",
        "sha256": "0" * 64,
    }
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.lib", entry, module, {})
    assert error.value.code == 1
    assert "does not match the pin" in capsys.readouterr().err


def test_a_jar_entry_with_no_classpath_file_fails(module, check_notice):
    entry = {"artifacts": ["com.example:lib"], "jar": "L", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.lib", entry, module, {})
    assert error.value.code == 1


def test_a_jar_entry_not_on_the_classpath_fails(tmp_path, module, check_notice):
    write_classpath(module, write_jar(tmp_path, "com.example", "other", "1.0", {}))
    entry = {"artifacts": ["com.example:lib"], "jar": "L", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.lib", entry, module, {})
    assert error.value.code == 1


def serve(check_notice, monkeypatch, body: bytes):
    class Response:
        def read(self):
            return body

    monkeypatch.setattr(check_notice, "github_token", lambda: None)
    monkeypatch.setattr(
        check_notice.urllib.request, "urlopen", lambda *a, **k: Response()
    )


def test_a_url_entry_is_verified_against_its_pin(module, check_notice, monkeypatch):
    text = b"Licence text\n"
    serve(check_notice, monkeypatch, text)
    entry = {
        "artifacts": ["c:l"],
        "url": "https://example.invalid",
        "sha256": sha256(text),
    }
    assert check_notice.obtain_text("LICENSE.l", entry, module, {}) == text


def test_an_html_response_is_rejected(module, check_notice, monkeypatch, capsys):
    body = b"<!DOCTYPE html>\n<html><body>Licence</body></html>\n"
    serve(check_notice, monkeypatch, body)
    entry = {
        "artifacts": ["c:l"],
        "url": "https://example.invalid",
        "sha256": sha256(body),
    }
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.l", entry, module, {})
    assert error.value.code == 1
    assert "served an HTML page" in capsys.readouterr().err


def test_a_url_whose_content_changed_upstream_is_rejected(
    module, check_notice, monkeypatch
):
    serve(check_notice, monkeypatch, b"rewritten upstream\n")
    entry = {"artifacts": ["c:l"], "url": "https://example.invalid", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.l", entry, module, {})
    assert error.value.code == 1


def test_a_template_fetches_at_the_resolved_version(module, check_notice, monkeypatch):
    # The issue #343 fix: a dependency bump changes what the resolved report
    # says, and the very next --update fetches the new tag with no edit to
    # licence-sources.toml.
    text = b"Licence text\n"
    seen = []

    def urlopen(request, **kwargs):
        seen.append(request.full_url)
        return io.BytesIO(text)

    monkeypatch.setattr(check_notice, "github_token", lambda: None)
    monkeypatch.setattr(check_notice.urllib.request, "urlopen", urlopen)
    entry = {
        "artifacts": ["com.google.re2j:re2j"],
        "url": "https://example.invalid/re2j-{version}/LICENSE",
        "sha256": sha256(text),
    }
    resolved = {"com.google.re2j:re2j:1.9": "Go License"}
    assert check_notice.obtain_text("LICENSE.re2j", entry, module, resolved) == text
    assert seen == ["https://example.invalid/re2j-1.9/LICENSE"]


# --- the token the fetch sends when it has one ---


def test_the_token_comes_from_the_environment_first(check_notice, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "from-the-environment")
    assert check_notice.github_token() == "from-the-environment"


@pytest.mark.parametrize(
    "outcome",
    [
        # `gh` absent, and `gh` present but not logged in: neither is an error
        # here, since raw.githubusercontent.com needs no token at all.
        OSError("no gh on PATH"),
        subprocess.CompletedProcess([], 1, stdout="\n", stderr="not logged in"),
    ],
)
def test_no_token_is_not_an_error(check_notice, monkeypatch, outcome):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    def run_gh(*args, **kwargs):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(check_notice.subprocess, "run", run_gh)
    assert check_notice.github_token() is None


def test_a_token_is_sent_as_a_bearer_header(module, check_notice, monkeypatch):
    text = b"Licence text\n"
    seen = []

    def urlopen(request, **kwargs):
        seen.append(request)
        return io.BytesIO(text)

    monkeypatch.setattr(check_notice, "github_token", lambda: "a-token")
    monkeypatch.setattr(check_notice.urllib.request, "urlopen", urlopen)
    entry = {
        "artifacts": ["c:l"],
        "url": "https://example.invalid",
        "sha256": sha256(text),
    }
    assert check_notice.obtain_text("LICENSE.l", entry, module, {}) == text
    assert seen[0].get_header("Authorization") == "Bearer a-token"


# --- the offline check, end to end ---


LICENCE_TEXT = b"BSD-3-Clause text\n"


@pytest.fixture()
def bundle(tmp_path, module, check_notice):
    """A module whose checked-in NOTICE and licences match its bundle."""
    write_report(
        module,
        ("Apache-2.0", "com.example:lib", "1.0"),
        ("BSD-3-Clause", "com.google.api:gax", "2.0.0"),
    )
    write_template(module, "Apache-2.0", "BSD-3-Clause")
    jar = write_jar(
        tmp_path, "com.google.api", "gax", "2.0.0", {"META-INF/LICENSE": LICENCE_TEXT}
    )
    write_classpath(module, jar)
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.gax": {
                "artifacts": ["com.google.api:gax"],
                "jar": "META-INF/LICENSE",
                "sha256": sha256(LICENCE_TEXT),
            }
        },
    )
    licences = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.gax").write_bytes(LICENCE_TEXT)
    notice_path(module).write_text(
        check_notice.render_notice(
            module / "NOTICE.template",
            check_notice.read_resolved(module),
            check_notice.load_sources(),
        )
    )
    return module


def test_a_matching_bundle_passes(bundle, run, check_notice):
    assert run(bundle) == 0


def test_an_entry_no_module_bundles_is_dead(bundle, run, check_notice, capsys):
    # licence-sources.toml is shared across modules, so `relevant` — which is per
    # module — never looks at an entry nothing bundles. Without this the pin for a
    # dropped dependency sits in the file indefinitely, checked by nothing.
    write_sources(
        check_notice.SOURCES,
        {
            # The `bundle` fixture's own entry, kept live so only the second one
            # can be what fails.
            "LICENSE.gax": {
                "artifacts": ["com.google.api:gax"],
                "jar": "META-INF/LICENSE",
                "sha256": sha256(LICENCE_TEXT),
            },
            "LICENSE.gone": {
                "artifacts": ["com.example:dropped"],
                "url": "https://example.invalid/LICENSE",
                "version_independent": True,
                "sha256": "0" * 64,
            },
        },
    )
    assert run(bundle) == 1
    err = capsys.readouterr().err
    assert "LICENSE.gone" in err
    # And only that one. gax's bullet carries a ` (META-INF/licenses/…)` pointer,
    # so this is also what holds NOTICE_BULLET to the pointered form: parse it
    # wrong and every entry looks dead, which the line above cannot tell apart.
    assert "LICENSE.gax" not in err


def test_the_offline_check_does_not_fetch_a_url_entry(
    tmp_path, module, run, check_notice, monkeypatch
):
    # The whole point of the check half is that CI runs it offline. Everything
    # else here uses a jar: pointer, whose text is read from disk either way — so
    # only a url: entry can catch a check path that starts fetching.
    def refuse(*args, **kwargs):
        raise AssertionError("the offline check fetched a licence text")

    monkeypatch.setattr(check_notice.urllib.request, "urlopen", refuse)
    write_report(module, ("BSD-3-Clause", "com.google.api:gax", "2.0.0"))
    write_template(module, "BSD-3-Clause")
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.gax": {
                "artifacts": ["com.google.api:gax"],
                "url": "https://example.invalid/LICENSE",
                "version_independent": True,
                "sha256": sha256(LICENCE_TEXT),
            }
        },
    )
    licences = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.gax").write_bytes(LICENCE_TEXT)
    notice_path(module).write_text(
        check_notice.render_notice(
            module / "NOTICE.template",
            check_notice.read_resolved(module),
            check_notice.load_sources(),
        )
    )
    assert run(module) == 0


def test_a_placeholder_group_may_contain_spaces(module, run, check_notice):
    # Two shipping templates use them ({{Go License}}, {{Public Domain}}) and no
    # other test does, so tightening PLACEHOLDER would pass the suite and emit the
    # placeholder line verbatim into a released NOTICE.
    write_report(module, ("Go License", "com.google.re2j:re2j", "1.8"))
    write_template(module, "Go License")
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.re2j": {
                "artifacts": ["com.google.re2j:re2j"],
                "url": "https://example.invalid/LICENSE",
                "version_independent": True,
                "sha256": sha256(LICENCE_TEXT),
            }
        },
    )
    licences = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.re2j").write_bytes(LICENCE_TEXT)
    rendered = check_notice.render_notice(
        module / "NOTICE.template",
        check_notice.read_resolved(module),
        check_notice.load_sources(),
    )
    assert "{{Go License}}" not in rendered
    assert "- com.google.re2j:re2j:1.8" in rendered
    notice_path(module).write_text(rendered)
    assert run(module) == 0


def test_a_hand_edited_notice_fails(bundle, run, capsys):
    notice_path(bundle).write_text("hand-written\n")
    assert run(bundle) == 1
    assert "META-INF/NOTICE differs" in capsys.readouterr().err


def test_a_missing_licence_text_fails(bundle, run, capsys):
    (bundle / "src/main/resources/META-INF/licenses/LICENSE.gax").unlink()
    assert run(bundle) == 1
    assert "is missing" in capsys.readouterr().err


def test_a_hand_edited_licence_text_fails(bundle, run, capsys):
    (bundle / "src/main/resources/META-INF/licenses/LICENSE.gax").write_bytes(b"edited")
    assert run(bundle) == 1
    assert "does not hash to the pin" in capsys.readouterr().err


def test_a_licence_text_nothing_references_fails(bundle, run, capsys):
    (bundle / "src/main/resources/META-INF/licenses/LICENSE.gone").write_bytes(b"stale")
    assert run(bundle) == 1
    assert "referenced by nothing" in capsys.readouterr().err


def test_an_entry_for_an_apache_artifact_fails(bundle, run, check_notice, capsys):
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.gax": {
                "artifacts": ["com.google.api:gax"],
                "jar": "META-INF/LICENSE",
                "sha256": sha256(LICENCE_TEXT),
            },
            "LICENSE.lib": {
                "artifacts": ["com.example:lib"],
                "jar": "META-INF/LICENSE",
                "sha256": "0" * 64,
            },
        },
    )
    assert run(bundle) == 1
    assert "would be materialised but referenced by nothing" in capsys.readouterr().err


def test_a_module_without_a_template_fails(module, run, capsys):
    # Named, so that the missing report this module also has cannot be what
    # produced the exit code.
    assert run(module) == 1
    assert "NOTICE.template does not exist" in capsys.readouterr().err


# --- --update ---


def test_update_writes_the_notice_and_the_licence_texts(bundle, run, check_notice):
    notice_path(bundle).unlink()
    licences = bundle / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.gax").unlink()
    assert run(bundle, "--update") == 0
    assert (licences / "LICENSE.gax").read_bytes() == LICENCE_TEXT
    assert "- com.google.api:gax:2.0.0 (META-INF/licenses/LICENSE.gax)" in (
        notice_path(bundle).read_text()
    )
    assert run(bundle) == 0


def test_update_fetches_a_templated_url_through_the_resolved_bundle(
    module, run, check_notice, monkeypatch
):
    # The wiring, not just the unit: main() hands obtain_text the resolved
    # bundle, which is what a {version} template resolves against on --update.
    text = b"Go licence text\n"
    seen = []

    def urlopen(request, **kwargs):
        seen.append(request.full_url)
        return io.BytesIO(text)

    monkeypatch.setattr(check_notice, "github_token", lambda: None)
    monkeypatch.setattr(check_notice.urllib.request, "urlopen", urlopen)
    write_report(module, ("Go License", "com.google.re2j:re2j", "1.9"))
    write_template(module, "Go License")
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.re2j": {
                "artifacts": ["com.google.re2j:re2j"],
                "url": "https://example.invalid/re2j-{version}/LICENSE",
                "sha256": sha256(text),
            }
        },
    )
    assert run(module, "--update") == 0
    assert seen == ["https://example.invalid/re2j-1.9/LICENSE"]
    licences = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    assert (licences / "LICENSE.re2j").read_bytes() == text


def test_update_removes_a_licence_text_nothing_references(bundle, run, check_notice):
    licences = bundle / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.gone").write_bytes(b"stale")
    assert run(bundle, "--update") == 0
    assert not (licences / "LICENSE.gone").exists()


def test_update_creates_the_resources_tree_a_new_module_does_not_have(
    bundle, run, check_notice
):
    # The state a shaded module is in the first time it is generated: no
    # META-INF directory at all. Before #290 this exited with a bare
    # FileNotFoundError from write_text, which is what the second one met.
    shutil.rmtree(bundle / "src" / "main" / "resources")
    assert run(bundle, "--update") == 0
    assert run(bundle) == 0
