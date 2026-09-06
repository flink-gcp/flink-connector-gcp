#!/usr/bin/env python3
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
"""Plan, build and assemble release-tag documentation and Development for Pages."""

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from urllib.parse import quote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
RELEASE = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
LEGACY_PUBLICATION_TEXT = (
    "It is generated from `main`, so it describes the current source rather than any release. References\n"
    "per released version arrive with artifact publishing\n"
    "([#39]({{< param BookRepo >}}/issues/39))."
)
PUBLICATION_TEXT = (
    "The API reference describes the same connector version as the selected documentation.\n"
    "Development follows the unreleased source on `main`."
)


def run(*args, cwd=ROOT, env=None, capture=False):
    result = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else None


def select_releases(releases, retained):
    if type(retained) is not int or retained < 1:
        raise ValueError("retained_minor_lines must be a positive integer")
    latest = {}
    for release in releases:
        match = RELEASE.fullmatch(release["tag_name"])
        if (
            not match
            or release["draft"]
            or release["prerelease"]
            or not release["published_at"]
        ):
            continue
        version = tuple(map(int, match.groups()))
        line = version[:2]
        if line not in latest or version > latest[line]:
            latest[line] = version
    if not latest:
        raise ValueError(
            "no published stable vX.Y.Z release; refusing to replace the site"
        )
    return [latest[line] for line in sorted(latest, reverse=True)[:retained]]


def site_base(value):
    url = urlsplit(value)
    if (
        url.scheme not in {"http", "https"}
        or not url.netloc
        or url.query
        or url.fragment
        or not value.endswith("/")
    ):
        raise ValueError(
            "baseURL must be an absolute HTTP(S) URL ending in / without query or fragment"
        )
    return url.path


def plan(destination):
    config = tomllib.loads((ROOT / "docs/versions.toml").read_text())
    hugo = tomllib.loads((ROOT / "docs/hugo.toml").read_text())
    base = hugo["baseURL"]
    site_base(base)
    repository = run(
        "gh",
        "repo",
        "view",
        "--json",
        "nameWithOwner",
        "--jq",
        ".nameWithOwner",
        capture=True,
    )
    pages = json.loads(
        run(
            "gh",
            "api",
            f"repos/{repository}/releases",
            "--paginate",
            "--slurp",
            capture=True,
        )
    )
    releases = [release for page in pages for release in page]
    versions = []
    for version in select_releases(releases, config["retained_minor_lines"]):
        label = ".".join(map(str, version))
        ref = f"v{label}"
        sha = run("git", "rev-parse", f"refs/tags/{ref}^{{commit}}", capture=True)
        versions.append(
            {
                "id": ".".join(map(str, version[:2])),
                "label": label,
                "ref": ref,
                "sha": sha,
            }
        )
    controller = run("git", "rev-parse", "HEAD", capture=True)
    versions.append(
        {"id": "development", "label": "Development", "ref": "main", "sha": controller}
    )
    manifest = {
        "base_url": base,
        "repository": repository,
        "controller_sha": controller,
        "current": versions[0]["id"],
        "versions": versions,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(manifest, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(
                f"matrix={json.dumps({'include': versions})}\ncontroller_sha={controller}\n"
            )
    print(json.dumps(manifest, indent=2))


def version_named(manifest, name):
    return next(version for version in manifest["versions"] if version["id"] == name)


def prepare_source(source, version):
    """Adapt publication metadata only, inside a disposable source checkout."""
    if source.resolve() == ROOT:
        raise ValueError(
            "build requires a disposable source checkout, separate from the controller"
        )
    if run("git", "rev-parse", "HEAD", cwd=source, capture=True) != version["sha"]:
        raise ValueError("source HEAD does not match the planned SHA")
    if run("git", "status", "--porcelain", cwd=source, capture=True):
        raise ValueError("source checkout must be clean before building")
    if version["id"] == "development":
        return
    if version["ref"] == "v1.0.0":
        homepage = source / "docs/content/_index.md"
        text = homepage.read_text()
        if text.count(LEGACY_PUBLICATION_TEXT) != 1:
            raise ValueError(
                "v1.0.0 publication paragraph changed; review the bootstrap adapter"
            )
        homepage.write_text(text.replace(LEGACY_PUBLICATION_TEXT, PUBLICATION_TEXT))
    # Release tags contain the pre-release SNAPSHOT POM. Include the opt-in
    # validation module so its parent and connector dependencies move together.
    run(
        "mise",
        "x",
        "java",
        "--",
        "./mvnw",
        "-ntp",
        "-Pdocs-snippets",
        "versions:set",
        f"-DnewVersion={version['label']}",
        "-DgenerateBackupPoms=false",
        cwd=source,
    )


def build(manifest, name, source, destination):
    version = version_named(manifest, name)
    if (destination / name).exists() or destination.is_relative_to(source):
        raise ValueError(
            "build destination must be fresh and outside the source checkout"
        )
    prepare_source(source, version)
    base = manifest["base_url"] + name + "/"
    # Environment configuration reaches the historical `just docs` invoked by
    # snippet validation too. ApiDocsURL supports the pre-versioning tag.
    env = dict(
        os.environ,
        HUGO_BASEURL=base,
        HUGO_PARAMS_APIDOCSURL=site_base(base) + "api/java/",
    )
    if name != "development":
        env["HUGO_PARAMS_BOOKEDITLINK"] = ""
    # Maven needs Java; each recipe activates its other tools from this source.
    # A bare mise entrypoint would also install unrelated linters and OpenTofu.
    run("mise", "x", "java", "--", "just", "docs-javadoc", cwd=source, env=env)
    run(
        "mise", "x", "java", "--", "just", "check-doc-sql-snippets", cwd=source, env=env
    )
    run(
        "mise",
        "x",
        "java",
        "--",
        "just",
        "test-java-snippet-shortcode",
        cwd=source,
        env=env,
    )
    run(
        "mise",
        "x",
        "java",
        "--",
        "just",
        "test-sql-snippet-shortcode",
        cwd=source,
        env=env,
    )
    public = source / "docs/public"
    if (
        not (public / "index.html").is_file()
        or not (public / "api/java/index.html").is_file()
    ):
        raise ValueError(f"{name}: documentation or API reference missing")
    shutil.copytree(public, destination / name)
    (destination / name / "docs-build.json").write_text(json.dumps(version) + "\n")


def page_url(prefix, relative):
    path = relative.as_posix()
    if relative.name == "index.html":
        path = path.removesuffix("index.html")
    return prefix + quote(path, safe="/")


def navigation(manifest, version, relative, inventories):
    base = site_base(manifest["base_url"])
    name = version["id"]
    api = relative.parts[:2] == ("api", "java")
    options = []
    fallback = []
    for other in manifest["versions"]:
        target = (
            relative if relative in inventories[other["id"]] else Path("index.html")
        )
        href = page_url(base + other["id"] + "/", target)
        active = ' aria-current="true"' if other["id"] == name else ""
        selected = " selected" if other["id"] == name else ""
        same = ' data-same-page="true"' if target == relative else ""
        suffix = " (latest)" if other["id"] == manifest["current"] else ""
        label = html.escape(other["label"] + suffix)
        options.append(
            f'<option value="{html.escape(href, quote=True)}"{selected}{same}>'
            f"{label}</option>"
        )
        fallback.append(
            f'<li><a href="{html.escape(href, quote=True)}"{active}{same}>'
            f"{label}</a></li>"
        )
    source = f"https://github.com/{manifest['repository']}/tree/{version['sha']}"
    prefix = base + name + "/"
    reference = (
        f'<a class="docs-version-reference" href="{prefix}">Documentation</a>'
        if api
        else f'<a class="docs-version-reference" href="{prefix}api/java/">API reference (Javadoc)</a>'
    )
    control = (
        f"<strong>{html.escape(version['label'])}</strong>"
        if api
        else (
            "<label><span>Version</span><select hidden>"
            f"{''.join(options)}</select></label>"
            f"<noscript><ul>{''.join(fallback)}</ul></noscript>"
        )
    )
    return (
        '<nav class="docs-version" aria-label="Documentation version">'
        f"{'' if api else reference}"
        f"{control}"
        '<div class="docs-version-links">'
        f"{reference if api else ''}"
        f'<a href="{html.escape(source, quote=True)}">Source</a></div></nav>'
    )


def decorate(text, manifest, version, relative, inventories):
    base = site_base(manifest["base_url"])
    extra = f'<link rel="stylesheet" href="{base}docs-version.css">'
    extra += f'<script defer src="{base}docs-version.js"></script>'
    if version["id"] == "development":
        extra += '<meta name="robots" content="noindex, nofollow">'
    text, count = re.subn(
        r"</head>", lambda _: extra + "</head>", text, count=1, flags=re.IGNORECASE
    )
    if count != 1:
        raise ValueError(f"{version['id']}/{relative}: missing HTML head")
    nav = navigation(manifest, version, relative, inventories)
    # Each tag renders its own theme before this shared control is attached
    # after the sidebar menu; on mobile the same sidebar is the menu drawer.
    if 'class="flex-header"' in text or "class=flex-header" in text:
        anchor = r"<header\b[^>]*>"
    elif 'class="book-menu"' in text or "class=book-menu" in text:
        anchor = r'<aside\b[^>]*\bclass=(?:"book-menu"|book-menu)[^>]*>[\s\S]*?</nav>'
    else:
        anchor = r"<body\b[^>]*>"
    text, count = re.subn(
        anchor, lambda match: match[0] + nav, text, count=1, flags=re.IGNORECASE
    )
    if count != 1:
        raise ValueError(
            f"{version['id']}/{relative}: missing navigation insertion point"
        )
    if version["id"] != "development":
        repository = f"https://github.com/{manifest['repository']}"
        for kind in ("blob", "tree"):
            text = text.replace(
                f"{repository}/{kind}/main/", f"{repository}/{kind}/{version['sha']}/"
            )
    return text


def redirect(target):
    escaped = html.escape(target, quote=True)
    # A relative same-origin target comes only from the selected output tree.
    script_target = json.dumps(target).replace("<", "\\u003c")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f'<meta http-equiv="refresh" content="0;url={escaped}">'
        f'<link rel="canonical" href="{escaped}"><title>Documentation moved</title>'
        f"<script>location.replace({script_target}+location.search+location.hash)</script>"
        f'</head><body><a href="{escaped}">Continue to the latest release documentation</a></body></html>'
    )


def assemble(manifest, inputs, destination):
    site_base(manifest["base_url"])
    if destination.is_relative_to(inputs):
        raise ValueError("assembly destination must be outside its input directory")
    if destination.exists():
        raise ValueError("assembly destination must not exist; use a fresh directory")
    inventories = {}
    for version in manifest["versions"]:
        source = inputs / version["id"]
        if json.loads((source / "docs-build.json").read_text()) != version:
            raise ValueError(
                f"{version['id']}: build provenance does not match the plan"
            )
        inventory = {path.relative_to(source) for path in source.rglob("*.html")}
        if not {Path("index.html"), Path("api/java/index.html")} <= inventory:
            raise ValueError(f"{version['id']}: documentation or API reference missing")
        # Also validate directly supplied local inputs. build() normally
        # dereferences symlinks when it copies the generated site.
        if any(path.is_symlink() for path in source.rglob("*")):
            raise ValueError(f"{version['id']}: Pages output must not contain symlinks")
        inventories[version["id"]] = inventory
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="docs-assemble-", dir=destination.parent
    ) as temporary:
        staged = Path(temporary) / "site"
        staged.mkdir()
        for version in manifest["versions"]:
            name = version["id"]
            shutil.copytree(inputs / name, staged / name)
            for relative in inventories[name]:
                page = staged / name / relative
                page.write_text(
                    decorate(page.read_text(), manifest, version, relative, inventories)
                )
        current = manifest["current"]
        for relative in inventories[current] - {Path("404.html")}:
            page = staged / relative
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text(
                redirect(
                    page_url(site_base(manifest["base_url"]) + current + "/", relative)
                )
            )
        for name in ("docs-version.css", "docs-version.js"):
            shutil.copyfile(ROOT / "docs/versioning" / name, staged / name)
        base = html.escape(site_base(manifest["base_url"]), quote=True)
        repository = html.escape(manifest["repository"], quote=True)
        (staged / "404.html").write_text(
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="robots" content="noindex"><title>Documentation not found</title></head>'
            "<body><h1>Documentation not found</h1><p>This page may belong to a version "
            "that is no longer hosted. The site keeps the latest patch of each retained minor.</p>"
            f'<p><a href="{base}">Latest documentation</a> · '
            f'<a href="{base}development/">Development</a> · '
            f'<a href="https://github.com/{repository}/releases">Release archives</a></p></body></html>'
        )
        (staged / "versions.json").write_text(json.dumps(manifest, indent=2) + "\n")
        # Crawlers must fetch Development pages to see their noindex metadata.
        (staged / "robots.txt").write_text(
            f"User-agent: *\nAllow: /\nSitemap: {manifest['base_url']}sitemap.xml\n"
        )
        maps = "".join(
            f"<sitemap><loc>{html.escape(manifest['base_url'] + v['id'] + '/sitemap.xml')}</loc></sitemap>"
            for v in manifest["versions"]
            if v["id"] != "development"
        )
        (staged / "sitemap.xml").write_text(
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{maps}</sitemapindex>'
        )
        shutil.move(staged, destination)
    print(f"Assembled {len(manifest['versions'])} documentation lines in {destination}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser(
        "plan", help="resolve published releases and immutable source SHAs"
    )
    planner.add_argument("destination", type=Path)
    builder = commands.add_parser(
        "build", help="build one line in a disposable clean checkout"
    )
    builder.add_argument("manifest", type=Path)
    builder.add_argument("version")
    builder.add_argument("source", type=Path)
    builder.add_argument("destination", type=Path)
    assembler = commands.add_parser(
        "assemble", help="combine verified lines into a fresh Pages directory"
    )
    assembler.add_argument("manifest", type=Path)
    assembler.add_argument("inputs", type=Path)
    assembler.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "plan":
            plan(args.destination.resolve())
        else:
            manifest = json.loads(args.manifest.read_text())
            if args.command == "build":
                build(
                    manifest,
                    args.version,
                    args.source.resolve(),
                    args.destination.resolve(),
                )
            else:
                assemble(manifest, args.inputs.resolve(), args.destination.resolve())
    except (ValueError, OSError, subprocess.CalledProcessError, StopIteration) as error:
        print(f"docs-site: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
