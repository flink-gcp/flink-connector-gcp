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
"""Tests for scripts/check-notice.py (issues #138, #249).

Synthetic and offline: a module directory in tmp_path carrying its own
THIRD-PARTY report, template and META-INF tree, with SOURCES monkeypatched onto
a licence-sources.toml written beside it. The jar-mode tests build real zips on
a fake runtime classpath; the two url-mode tests monkeypatch urlopen, since
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
Copyright 2026 laughingman7743

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
    lines = []
    for name, entry in entries.items():
        artifacts = ", ".join(f'"{artifact}"' for artifact in entry["artifacts"])
        pointer = (
            f'jar = "{entry["jar"]}"' if "jar" in entry else f'url = "{entry["url"]}"'
        )
        lines += [
            f'[files."{name}"]',
            f"artifacts = [{artifacts}]",
            pointer,
            f'sha256 = "{entry["sha256"]}"',
            "",
        ]
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


def test_a_restricted_dependency_stops_before_any_notice_work(
    module, run, check_notice, capsys
):
    # The template deliberately has no paragraph for GPLv2, so rendering would
    # fail too: the gate is what must answer, and it must answer first. The
    # message is "decide adoption", not "add a paragraph".
    write_report(module, ("GPLv2", "com.example:copyleft", "1.0"))
    write_template(module, "Apache-2.0")
    write_sources(check_notice.SOURCES, {})
    assert run(module) == 1
    assert "discuss adoption first" in capsys.readouterr().err


def test_the_classpath_exception_dual_licence_is_exempt(module, run, check_notice):
    # javax.annotation-api: dual-licensed, taken under CDDL, and the only
    # combination deliberately in the bundle today.
    licence = "CDDL + GPLv2 with classpath exception"
    write_report(module, (licence, "javax.annotation:javax.annotation-api", "1.3.2"))
    write_template(module, licence)
    text = b"CDDL text\n"
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.javax-annotation": {
                "artifacts": ["javax.annotation:javax.annotation-api"],
                "url": "https://example.invalid/LICENSE",
                "sha256": sha256(text),
            }
        },
    )
    licences = module / "src" / "main" / "resources" / "META-INF" / "licenses"
    (licences / "LICENSE.javax-annotation").write_bytes(text)
    notice_path(module).write_text(
        check_notice.render_notice(
            module / "NOTICE.template",
            check_notice.read_resolved(module),
            check_notice.load_sources(),
        )
    )
    assert run(module) == 0


# --- the licence-source index ---


def test_one_artifact_under_two_entries_fails(module, check_notice):
    write_sources(
        check_notice.SOURCES,
        {
            "LICENSE.a": {
                "artifacts": ["com.example:shared"],
                "url": "https://example.invalid/a",
                "sha256": "0" * 64,
            },
            "LICENSE.b": {
                "artifacts": ["com.example:shared"],
                "url": "https://example.invalid/b",
                "sha256": "0" * 64,
            },
        },
    )
    with pytest.raises(SystemExit) as error:
        check_notice.load_sources()
    assert error.value.code == 1


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
    assert check_notice.obtain_text("LICENSE.gax", entry, module) == text


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
        check_notice.obtain_text("LICENSE.lib", entry, module)
    assert error.value.code == 1
    assert "does not match the pin" in capsys.readouterr().err


def test_a_jar_entry_with_no_classpath_file_fails(module, check_notice):
    entry = {"artifacts": ["com.example:lib"], "jar": "L", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.lib", entry, module)
    assert error.value.code == 1


def test_a_jar_entry_not_on_the_classpath_fails(tmp_path, module, check_notice):
    write_classpath(module, write_jar(tmp_path, "com.example", "other", "1.0", {}))
    entry = {"artifacts": ["com.example:lib"], "jar": "L", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.lib", entry, module)
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
    assert check_notice.obtain_text("LICENSE.l", entry, module) == text


def test_an_html_response_is_rejected(module, check_notice, monkeypatch, capsys):
    body = b"<!DOCTYPE html>\n<html><body>Licence</body></html>\n"
    serve(check_notice, monkeypatch, body)
    entry = {
        "artifacts": ["c:l"],
        "url": "https://example.invalid",
        "sha256": sha256(body),
    }
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.l", entry, module)
    assert error.value.code == 1
    assert "served an HTML page" in capsys.readouterr().err


def test_a_url_whose_content_changed_upstream_is_rejected(
    module, check_notice, monkeypatch
):
    serve(check_notice, monkeypatch, b"rewritten upstream\n")
    entry = {"artifacts": ["c:l"], "url": "https://example.invalid", "sha256": "0" * 64}
    with pytest.raises(SystemExit) as error:
        check_notice.obtain_text("LICENSE.l", entry, module)
    assert error.value.code == 1


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
    assert check_notice.obtain_text("LICENSE.l", entry, module) == text
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
