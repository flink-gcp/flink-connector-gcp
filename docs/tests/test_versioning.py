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
"""Render synthetic Hugo sites and execute the published navigation JavaScript."""

import importlib.util
import json
import os
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


class ScriptLinks(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.sources = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        if tag == "script" and "src" in dict(attrs):
            self.sources.append(dict(attrs)["src"])


@pytest.mark.parametrize("line", ["1.1", "1.2"])
def test_build_scopes_the_real_theme_search_to_its_version(tmp_path, monkeypatch, line):
    spec = importlib.util.spec_from_file_location(
        "docs_site", ROOT / "scripts/docs-site.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    source = tmp_path / "source"
    docs = source / "docs"
    (docs / "content/docs").mkdir(parents=True)
    (docs / "content/_index.md").write_text("---\ntitle: Home\n---\nHome\n")
    (docs / "content/docs/page.md").write_text(
        f"---\ntitle: Version {line}\n---\nSearch content for {line}.\n"
        "[API]({{< param ApiDocsURL >}})\n"
    )
    for name in ("go.mod", "go.sum"):
        shutil.copyfile(ROOT / "docs" / name, docs / name)
    (docs / "hugo.toml").write_text(
        'baseURL = "https://example.org/unused/"\n'
        'disableKinds = ["taxonomy", "term"]\n'
        '[params]\nBookRepo = "https://github.com/example/connector"\n'
        'BookEditLink = "https://github.com/example/connector/edit/main/docs/{{ .Path }}"\n'
        "[module]\n[[module.imports]]\n"
        'path = "github.com/flink-gcp/flink-gcp-dev-tools/hugo"\n'
    )
    # Stub Maven and the separately tested snippet checkers. The production
    # build() hands its actual environment to the real pinned Hugo/theme here.
    monkeypatch.setattr(helper, "prepare_source", lambda *args: None)

    def run(*args, cwd, env):
        if args[-1] == "docs-javadoc":
            api = docs / "static/api/java"
            api.mkdir(parents=True)
            (api / "index.html").write_text(
                "<!doctype html><html><body>API</body></html>"
            )
        elif args[-1] == "check-doc-sql-snippets":
            result = subprocess.run(
                ["hugo", "--source", str(docs), "--minify", "--panicOnWarning"],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )
            assert result.returncode == 0, result.stdout + result.stderr

    monkeypatch.setattr(helper, "run", run)
    version = {
        "id": line,
        "label": line + ".0",
        "ref": "v" + line + ".0",
        "sha": "a" * 40,
    }
    manifest = {"base_url": "https://example.org/project/", "versions": [version]}
    helper.build(manifest, line, source, tmp_path / "output")
    public = tmp_path / "output" / line
    prefix = "/project/" + line + "/"
    text = (public / "docs/page/index.html").read_text()
    assert "/edit/main/" not in text
    assert prefix + "api/java/" in text
    scripts = [url for url in ScriptLinks(text).sources if ".search." in url]
    assert len(scripts) == 1 and scripts[0].startswith(prefix)
    search = (public / scripts[0].removeprefix(prefix)).read_text()
    indexes = list(public.glob("*.search-data.*.json"))
    assert len(indexes) == 1
    assert prefix + indexes[0].name in search
    records = json.loads(indexes[0].read_text())
    assert records and all(record["href"].startswith(prefix) for record in records)
    assert any(
        record["href"] == prefix + "docs/page/"
        and f"content for {line}" in record["content"]
        for record in records
    )
    assert "/unused/" not in text + search
    # Check the insertion against the real pinned theme, including its minified
    # sidebar markup, rather than relying only on synthetic HTML shapes.
    manifest.update(repository="example/connector", current=line)
    decorated = helper.decorate(
        text,
        manifest,
        version,
        Path("docs/page/index.html"),
        {line: {Path("docs/page/index.html")}},
    )
    assert decorated.index("</nav>") < decorated.index('class="docs-version"')
    assert decorated.index('class="docs-version"') < decorated.index("</aside>")
    assert f'href="{prefix}api/java/">API reference (Javadoc)</a>' in decorated


@pytest.mark.parametrize(
    "path", ["/", "/project/", "/project/1.0/", "/project/development/"]
)
def test_hugo_links_follow_the_selected_base_url(tmp_path, path):
    for directory in ("content", "layouts"):
        (tmp_path / directory).mkdir()
    (tmp_path / "content/_index.md").write_text(
        '---\ntitle: Home\n---\n[API]({{< api-docs-url >}})\n[Other]({{< relref "other" >}})\n'
    )
    (tmp_path / "content/other.md").write_text("---\ntitle: Other\n---\nExample\n")
    (tmp_path / "layouts/home.html").write_text(
        "<!doctype html><html><body>{{ .Content }}</body></html>"
    )
    (tmp_path / "layouts/single.html").write_text(
        "<!doctype html><html><body>{{ .Content }}</body></html>"
    )
    shortcode = ROOT / "docs/layouts/_shortcodes/api-docs-url.html"
    (tmp_path / "hugo.toml").write_text(
        'baseURL = "https://example.org/unused/"\n'
        'disableKinds = ["taxonomy", "term"]\n'
        '[module]\n[[module.mounts]]\nsource = "content"\ntarget = "content"\n'
        '[[module.mounts]]\nsource = "layouts"\ntarget = "layouts"\n'
        f"[[module.mounts]]\nsource = {json.dumps(str(shortcode))}\n"
        'target = "layouts/_shortcodes/api-docs-url.html"\n'
    )
    result = subprocess.run(
        ["hugo", "--source", str(tmp_path), "--panicOnWarning"],
        check=False,
        env=dict(os.environ, HUGO_BASEURL="https://example.org" + path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    text = (tmp_path / "public/index.html").read_text()
    assert f'href="{path}api/java/"' in text
    assert f'href="{path}other/"' in text
    assert "/unused/" not in text


def test_legacy_redirect_preserves_encoded_query_and_api_fragment():
    spec = importlib.util.spec_from_file_location(
        "docs_site", ROOT / "scripts/docs-site.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    page = helper.redirect("/project/1.0/api/java/Type.html")
    script = page.split("<script>", 1)[1].split("</script>", 1)[0]
    program = """
const vm = require('node:vm');
const assert = require('node:assert/strict');
let actual;
const location = {search: '?q=A%26B', hash: '#set(java.lang.String)', replace: x => actual = x};
vm.runInNewContext(SCRIPT, {location});
assert.equal(actual, '/project/1.0/api/java/Type.html?q=A%26B#set(java.lang.String)');
""".replace("SCRIPT", json.dumps(script))
    subprocess.run(["node", "-e", program], check=True)


@pytest.mark.parametrize("same_page", [True, False])
def test_version_dropdown_preserves_context_only_for_matching_pages(same_page):
    script = (ROOT / "docs/versioning/docs-version.js").read_text()
    program = """
const vm = require('node:vm');
const assert = require('node:assert/strict');
let change, pageShow, actual;
const option = {value: SAME_PAGE ? '/project/1.1/docs/shared/' : '/project/1.1/',
  hasAttribute: name => { assert.equal(name, 'data-same-page'); return SAME_PAGE; }};
const currentOption = {value: '/project/1.2/docs/shared/',
  hasAttribute: name => { assert.equal(name, 'data-same-page'); return true; }};
const select = {hidden: true, selectedOptions: [currentOption],
  querySelector: selector => {
    assert.equal(selector, 'option[selected]');
    return currentOption;
  },
  addEventListener: (type, callback) => { assert.equal(type, 'change'); change = callback; }};
const window = {addEventListener: (type, callback) => {
  assert.equal(type, 'pageshow'); pageShow = callback;
}};
const document = {querySelectorAll: selector => {
  assert.equal(selector, '.docs-version select'); return [select]; }};
const location = {href: 'https://example.org/project/1.2/docs/shared/',
  search: '', hash: '', assign: url => actual = url};
vm.runInNewContext(SCRIPT, {document, window, location, URL});
assert.equal(select.hidden, false);
location.search = '?q=A%26B';
location.hash = '#delivery';
select.selectedOptions = [option];
change();
assert.equal(actual, SAME_PAGE
  ? 'https://example.org/project/1.1/docs/shared/?q=A%26B#delivery'
  : 'https://example.org/project/1.1/');
select.value = option.value;
pageShow();
assert.equal(select.value, '/project/1.2/docs/shared/');
""".replace("SCRIPT", json.dumps(script)).replace("SAME_PAGE", json.dumps(same_page))
    subprocess.run(["node", "-e", program], check=True)
