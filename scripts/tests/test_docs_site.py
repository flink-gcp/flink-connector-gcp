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
"""Synthetic release inventories, source trees and published HTML."""

import json
import subprocess
from html.parser import HTMLParser
from pathlib import Path
from urllib.robotparser import RobotFileParser

import pytest
from conftest import load_script


@pytest.fixture
def helper():
    return load_script("docs-site.py")


def release(tag, **changes):
    return {
        "tag_name": tag,
        "draft": False,
        "prerelease": False,
        "published_at": "2026-01-01T00:00:00Z",
    } | changes


def test_latest_patch_per_minor_ignores_publication_order(helper):
    releases = [
        release(tag)
        for tag in (
            "v1.2.1",
            "v1.1.10",
            "v1.10.0",
            "v1.2.0",
            "v1.1.11",
            "v1.9.0",
            "v1.9.3",
        )
    ]
    assert helper.select_releases(releases, 2) == [(1, 10, 0), (1, 9, 3)]
    assert helper.select_releases(list(reversed(releases)), 2) == [
        (1, 10, 0),
        (1, 9, 3),
    ]


def test_window_crosses_major_boundary_and_counts_existing_minors(helper):
    releases = [release("v1.9.4"), release("v1.7.8"), release("v2.0.0")]
    assert helper.select_releases(releases, 2) == [(2, 0, 0), (1, 9, 4)]
    assert helper.select_releases(releases, 1) == [(2, 0, 0)]


def test_one_release_needs_no_placeholder_for_a_previous_minor(helper):
    assert helper.select_releases([release("v1.0.0")], 2) == [(1, 0, 0)]


def test_nonfinal_releases_and_artifact_suffixes_never_enter_window(helper):
    releases = [
        release("v1.0.0"),
        release("v9.0.0", draft=True),
        release("v8.0.0", prerelease=True),
        release("v7.0.0", published_at=None),
    ]
    releases += [
        release(tag)
        for tag in (
            "v6.0.0-1.20",
            "v6.0.0-rc1",
            "v6.0.0+build",
            "1.1.0",
            "v01.1.0",
            "v1.01.0",
            "v1.1.00",
            "v1.1.0\n",
            "v1.1.0/../../development",
            "v1.1.0-SNAPSHOT",
        )
    ]
    assert helper.select_releases(releases, 2) == [(1, 0, 0)]


@pytest.mark.parametrize("count", [0, -1, True, "2", 2.5])
def test_invalid_retention_is_rejected(helper, count):
    with pytest.raises(ValueError, match="positive integer"):
        helper.select_releases([release("v1.0.0")], count)


def test_no_stable_release_fails_instead_of_publishing_development_as_current(helper):
    with pytest.raises(ValueError, match="no published stable"):
        helper.select_releases([release("v1.0.0", draft=True)], 2)


@pytest.mark.parametrize(
    "base",
    [
        "/project/",
        "https://example.org/project",
        "https://example.org/project/?q=1",
        "https://example.org/#x",
    ],
)
def test_invalid_site_base(helper, base):
    with pytest.raises(ValueError, match="baseURL"):
        helper.site_base(base)


@pytest.fixture
def manifest():
    return {
        "base_url": "https://example.org/project/",
        "repository": "example/connector",
        "controller_sha": "c" * 40,
        "current": "1.2",
        "versions": [
            {"id": "1.2", "label": "1.2.3", "ref": "v1.2.3", "sha": "a" * 40},
            {"id": "1.1", "label": "1.1.9", "ref": "v1.1.9", "sha": "b" * 40},
            {
                "id": "development",
                "label": "Development",
                "ref": "main",
                "sha": "c" * 40,
            },
        ],
    }


class Links(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.links = []
        self.options = []
        self.meta = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.links.append(dict(attrs))
        if tag == "option":
            self.options.append(dict(attrs))
        if tag == "meta":
            self.meta.append(dict(attrs))


def html_page(body="<p>Example</p>"):
    return f"<!doctype html><html><head><title>Test</title></head><body>{body}</body></html>"


@pytest.fixture
def lines(tmp_path, manifest, helper, monkeypatch):
    controller = tmp_path / "controller"
    assets = controller / "docs/versioning"
    assets.mkdir(parents=True)
    for name in ("docs-version.css", "docs-version.js"):
        (assets / name).write_text("/* fixture */")
    monkeypatch.setattr(helper, "ROOT", controller)
    inputs = tmp_path / "lines"
    for version in manifest["versions"]:
        line = inputs / version["id"]
        for page in (
            "index.html",
            "docs/shared/index.html",
            "api/java/index.html",
            "api/java/example/Type.html",
            "404.html",
        ):
            path = line / page
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(html_page(f"<p>{version['label']}: {page}</p>"))
        (line / "sitemap.xml").write_text("<urlset/>")
        (line / "en.search-data.min.fixture.json").write_text(
            json.dumps(
                [
                    {
                        "href": f"/project/{version['id']}/docs/shared/",
                        "title": version["label"],
                    }
                ]
            )
        )
        (line / "docs-build.json").write_text(json.dumps(version))
    return inputs


def test_assembly_contains_exact_lines_sources_and_searches(
    helper, manifest, lines, tmp_path
):
    retired = lines / "0.9"
    retired.mkdir()
    (retired / "index.html").write_text("retired")
    site = tmp_path / "site"
    helper.assemble(manifest, lines, site)
    assert not (site / "0.9").exists()
    assert json.loads((site / "versions.json").read_text()) == manifest
    for version in manifest["versions"]:
        page = (site / version["id"] / "docs/shared/index.html").read_text()
        assert f"{version['label']}: docs/shared/index.html" in page
        assert f"/tree/{version['sha']}" in page
        assert json.loads(
            (site / version["id"] / "en.search-data.min.fixture.json").read_text()
        ) == [
            {
                "href": f"/project/{version['id']}/docs/shared/",
                "title": version["label"],
            }
        ]
        assert ('name="robots" content="noindex, nofollow"' in page) == (
            version["id"] == "development"
        )
        assert f'href="/project/{version["id"]}/api/java/"' in page
        assert "API reference (Javadoc)" in page
        api = (site / version["id"] / "api/java/example/Type.html").read_text()
        assert f'href="/project/{version["id"]}/">Documentation</a>' in api
        assert f"<strong>{version['label']}</strong>" in api
        assert not Links(api).options
        assert "<select" not in api
    assert "development" not in (site / "sitemap.xml").read_text()
    robots = RobotFileParser()
    robots.parse((site / "robots.txt").read_text().splitlines())
    assert robots.can_fetch(
        "Googlebot", manifest["base_url"] + "development/docs/shared/"
    )


def test_selector_preserves_existing_page_and_falls_back_for_missing_page(
    helper, manifest, lines, tmp_path
):
    (lines / "1.1/docs/shared/index.html").unlink()
    site = tmp_path / "site"
    helper.assemble(manifest, lines, site)
    parsed = Links((site / "1.2/docs/shared/index.html").read_text())
    links = {link["href"]: link for link in parsed.links}
    assert links["/project/1.2/docs/shared/"]["aria-current"] == "true"
    assert links["/project/development/docs/shared/"]["data-same-page"] == "true"
    assert "data-same-page" not in links["/project/1.1/"]
    options = {option["value"]: option for option in parsed.options}
    assert "selected" in options["/project/1.2/docs/shared/"]
    assert "selected" not in options["/project/development/docs/shared/"]
    assert options["/project/development/docs/shared/"]["data-same-page"] == "true"
    assert "data-same-page" not in options["/project/1.1/"]


def test_legacy_html_urls_redirect_to_current_including_javadoc(
    helper, manifest, lines, tmp_path
):
    site = tmp_path / "site"
    helper.assemble(manifest, lines, site)
    expected = {
        "index.html": "/project/1.2/",
        "docs/shared/index.html": "/project/1.2/docs/shared/",
        "api/java/example/Type.html": "/project/1.2/api/java/example/Type.html",
    }
    for path, target in expected.items():
        text = (site / path).read_text()
        assert Links(text).links[0]["href"] == target
        assert "+location.search+location.hash" in text
    missing = (site / "404.html").read_text()
    assert "no longer hosted" in missing
    assert "example/connector/releases" in missing
    assert "http-equiv" not in missing


@pytest.mark.parametrize("damage", ["provenance", "api", "html", "symlink"])
def test_incomplete_or_mismatched_build_never_creates_site(
    helper, manifest, lines, tmp_path, damage
):
    if damage == "provenance":
        (lines / "1.1/docs-build.json").write_text(
            json.dumps(dict(manifest["versions"][1], sha="wrong"))
        )
    elif damage == "api":
        (lines / "1.1/api/java/index.html").unlink()
    elif damage == "html":
        (lines / "1.1/docs/shared/index.html").write_text("not HTML")
    else:
        # Exercise direct assembler input; build() normally resolves links.
        (lines / "1.1/linked").symlink_to(lines / "1.2")
    site = tmp_path / "site"
    with pytest.raises(ValueError):
        helper.assemble(manifest, lines, site)
    assert not site.exists()


def test_assembly_refuses_to_overwrite_an_existing_site(
    helper, manifest, lines, tmp_path
):
    site = tmp_path / "site"
    site.mkdir()
    (site / "sentinel").write_text("previous publication")
    with pytest.raises(ValueError, match="must not exist"):
        helper.assemble(manifest, lines, site)
    assert (site / "sentinel").read_text() == "previous publication"


def test_index_suffix_is_not_a_directory_index(helper):
    assert helper.page_url("/project/", Path("api/java/search-index.html")) == (
        "/project/api/java/search-index.html"
    )


def test_assembly_cannot_recursively_copy_into_its_inputs(helper, manifest, lines):
    with pytest.raises(ValueError, match="outside its input"):
        helper.assemble(manifest, lines, lines / "1.2/nested")


@pytest.mark.parametrize(
    "body,anchor",
    [
        (
            (
                "<aside class=book-menu><div class=book-menu-content>"
                "<h2 class=book-brand><a href=/><span>Site</span></a></h2>"
                "<div class=book-search>Search</div><nav>Menu</nav></div></aside>"
                "<main class=book-page>Content</main>"
            ),
            "</nav>",
        ),
        (
            (
                '<aside class="book-menu"><div class="book-menu-content">'
                '<h2 class="book-brand">\n<a href="/">Site</a>\n</h2>'
                '<div class="book-search">Search</div><nav>Menu</nav></div></aside>'
                '<main class="book-page">Content</main>'
            ),
            "</nav>",
        ),
        (
            '<header role="banner" class="flex-header"><div>menu</div></header>',
            'class="flex-header">',
        ),
        ("<p>content</p>", "<body>"),
    ],
)
def test_navigation_joins_the_host_layout(helper, manifest, body, anchor):
    inventory = {v["id"]: {Path("index.html")} for v in manifest["versions"]}
    text = helper.decorate(
        html_page(body),
        manifest,
        manifest["versions"][0],
        Path("index.html"),
        inventory,
    )
    assert anchor + '<nav class="docs-version"' in text
    assert text.count('class="docs-version"') == 1


def test_released_source_links_are_pinned_without_rewriting_vendor_links(
    helper, manifest
):
    body = (
        '<a href="https://github.com/example/connector/blob/main/LICENSE">license</a>'
    )
    body += '<a href="https://github.com/vendor/library/blob/main/LICENSE">vendor</a>'
    body += '<a href="https://github.com/example/connector/tree/main/docs">docs</a>'
    inventory = {v["id"]: {Path("index.html")} for v in manifest["versions"]}
    text = helper.decorate(
        html_page(body),
        manifest,
        manifest["versions"][0],
        Path("index.html"),
        inventory,
    )
    assert "/connector/blob/" + "a" * 40 + "/LICENSE" in text
    assert "vendor/library/blob/main/LICENSE" in text
    assert "/connector/tree/" + "a" * 40 + "/docs" in text


def test_plan_reads_every_release_page_and_pins_git_objects(
    helper, tmp_path, monkeypatch
):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/versions.toml").write_text("retained_minor_lines = 2\n")
    (tmp_path / "docs/hugo.toml").write_text(
        'baseURL = "https://example.org/project/"\n'
    )
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output"))
    calls = []

    def fake_run(*args, **kwargs):
        calls.append(args)
        if args[:2] == ("gh", "repo"):
            return "example/connector"
        if args[:2] == ("gh", "api"):
            assert args[-2:] == ("--paginate", "--slurp")
            return json.dumps([[release("v1.0.0")], [release("v1.1.0")]])
        return {
            "refs/tags/v1.0.0^{commit}": "a" * 40,
            "refs/tags/v1.1.0^{commit}": "b" * 40,
            "HEAD": "c" * 40,
        }[args[-1]]

    monkeypatch.setattr(helper, "run", fake_run)
    helper.plan(tmp_path / "plan.json")
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["current"] == "1.1"
    assert [v["sha"] for v in plan["versions"]] == ["b" * 40, "a" * 40, "c" * 40]
    assert "controller_sha=" + "c" * 40 in (tmp_path / "output").read_text()


def test_bootstrap_adapter_checks_old_text_and_reversions_validation_reactor(
    helper, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    (source / "docs/content").mkdir(parents=True)
    page = source / "docs/content/_index.md"
    page.write_text("prefix\n" + helper.LEGACY_PUBLICATION_TEXT + "\nsuffix")
    calls = []

    def fake_run(*args, **kwargs):
        calls.append(args)
        return "a" * 40 if args[:2] == ("git", "rev-parse") else ""

    monkeypatch.setattr(helper, "run", fake_run)
    version = {"id": "1.0", "label": "1.0.0", "ref": "v1.0.0", "sha": "a" * 40}
    helper.prepare_source(source, version)
    assert page.read_text() == "prefix\n" + helper.PUBLICATION_TEXT + "\nsuffix"
    maven = calls[-1]
    assert "-Pdocs-snippets" in maven and "-DnewVersion=1.0.0" in maven
    with pytest.raises(ValueError, match="bootstrap adapter"):
        helper.prepare_source(source, version)


def test_build_failure_does_not_produce_a_success_marker(
    helper, tmp_path, manifest, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(helper, "prepare_source", lambda *args: None)

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(helper, "run", fail)
    output = tmp_path / "output"
    with pytest.raises(subprocess.CalledProcessError):
        helper.build(manifest, "development", source, output)
    assert not output.exists()
