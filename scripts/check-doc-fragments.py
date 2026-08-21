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
"""Hold every documentation link fragment to an anchor the site really emits (issue #867).

Two tools each cover part of this and neither covers a *cross-page* fragment.
markdownlint's MD051 judges in-page ``[](#anchor)`` fragments only, and judges
them against the Markdown. Hugo's ``relref`` resolves and validates the *page*
and never the fragment — measured: ``{{< relref "page#not-a-heading" >}}`` builds
clean under ``--panicOnWarning`` and emits the fragment verbatim. Renaming a
heading therefore breaks every inbound link to it silently, which is what
happened on PR #836: ``docs/content/docs/examples/_index.md`` went on linking to
``docs/examples/bigquery#no-emulator-path`` after that page renamed the heading,
and nothing in ``just lint``, ``just docs`` or CI had anything to say.

This reads the **rendered** site rather than the Markdown. Hugo's heading ids are
a configured algorithm — ``markup.goldmark.parser.autoidtype``, ``github`` here —
so a checker with those rules baked in would agree today and go silently wrong
the day the setting moved. Reading what was emitted cannot, and it covers an
anchor a shortcode or raw HTML defines without knowing anything about either.

Ids are collected from the whole page; links only from inside ``<article>``,
which is the prose this repository writes. The sidebar menu and the two copies
of the table of contents sit outside it and are the theme's own output.

What it does not do: it says nothing about external links, whose liveness is a
different failure mode with a network dependency; nothing about whether a
fragment points at the *right* heading, only that its target exists; and nothing
about a page absent from the build, since it only reads what Hugo emitted.

Exit codes: 0 clean, 1 a fragment that does not resolve, 2 infrastructure (no
built site, or a site whose shape says this check would have judged nothing).

Standard library only, like the other repository checkers.
"""

import difflib
import html.parser
import re
import sys
import urllib.parse
from pathlib import Path

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-doc-fragments`, or any python3 >= 3.11. "
        "CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
HUGO_CONFIG = ROOT / "docs" / "hugo.toml"
SITE = ROOT / "docs" / "public"
CONTENT = ROOT / "docs" / "content"

# How many near-miss ids a failure offers as the repair.
SUGGESTIONS = 3


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


# Elements with no end tag, which must not be pushed onto the open-element
# stack below or everything after the first one nests inside it.
VOID_ELEMENTS = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)


class Page(html.parser.HTMLParser):
    """The anchors a rendered page offers, and the links its prose writes.

    A real parser rather than a regex because `just docs` minifies: Hugo emits
    `id=file-loads` and `href=/base/page/#anchor` without quotes, so a pattern
    written against `id="..."` matches nothing at all and the check passes
    having seen no anchors.

    Two links are deliberately not collected. One outside `<article>` is the
    theme's — the sidebar menu and both copies of the table of contents, all
    derived from the headings they point at. One *inside* the element whose id
    it names is the theme's too: hugo-book renders
    `<h2 id=x>Title<a class=anchor href=#x>#</a></h2>` beside every heading.
    Both are satisfied by construction, so checking them would say nothing —
    and, worse, 430 of them would hold the "no fragment links" floor up while
    every prose link had stopped being seen.

    The enclosing-id rule is written that way rather than against the theme's
    `class=anchor` so that it holds whatever the theme calls it.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: set[str] = set()
        self.links: list[str] = []
        self.has_article = False
        self._open: list[tuple[str, str | None]] = []

    def _names_an_enclosing_id(self, href: str) -> bool:
        if not href.startswith("#"):
            return False
        fragment = urllib.parse.unquote(href[1:])
        return any(fragment == identifier for _, identifier in self._open)

    def _in_article(self) -> bool:
        return any(tag == "article" for tag, _ in self._open)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        identifier = values.get("id")
        if identifier:
            self.ids.add(identifier)
        # The pre-HTML5 form, still what some hand-written anchors use.
        if tag == "a" and values.get("name"):
            self.ids.add(str(values["name"]))

        if tag == "a" and self._in_article():
            href = values.get("href")
            if href and not self._names_an_enclosing_id(href):
                self.links.append(href)

        if tag == "article":
            self.has_article = True
        if tag not in VOID_ELEMENTS:
            self._open.append((tag, identifier))

    def handle_endtag(self, tag: str) -> None:
        # Pop through anything left unclosed, so one stray tag cannot leave the
        # rest of the document reading as nested inside it.
        for index in range(len(self._open) - 1, -1, -1):
            if self._open[index][0] == tag:
                del self._open[index:]
                return


def base_path() -> str:
    """The site's own path prefix, read from hugo.toml rather than hardcoded.

    Every rendered internal link starts with it. Hardcoding the value would
    classify all of them as external the day baseURL moves — and a check that
    judges nothing reports clean.
    """
    try:
        with HUGO_CONFIG.open("rb") as stream:
            config = tomllib.load(stream)
    except OSError as error:
        infra(f"Could not read the Hugo configuration at {HUGO_CONFIG}: {error}")
    except tomllib.TOMLDecodeError as error:
        infra(f"{HUGO_CONFIG} does not parse: {error}")

    declared = config.get("baseURL")
    if not isinstance(declared, str) or not declared.strip():
        infra(
            f"{HUGO_CONFIG} declares no baseURL; this check resolves links against it."
        )

    path = urllib.parse.urlsplit(declared.strip()).path or "/"
    return path if path.endswith("/") else path + "/"


def read_pages() -> dict[Path, Page]:
    paths = sorted(SITE.rglob("*.html"))
    if not paths:
        infra(
            f"No built site under {SITE}. Run `just docs` first — `just "
            f"check-doc-fragments` does that for you."
        )

    pages: dict[Path, Page] = {}
    for path in paths:
        page = Page()
        page.feed(path.read_text(encoding="utf-8", errors="replace"))
        page.close()
        pages[path] = page
    return pages


def page_url(page: Path, base: str) -> str:
    """The site path a built page is served at, for resolving a relative link."""
    served = page.relative_to(SITE).as_posix().removesuffix("index.html")
    return base + served


def resolve_page(link_path: str, base: str, pages: dict[Path, Page]) -> Path | None:
    """The built page a site-internal link path names, if the build has one."""
    # Percent-decoded because the filesystem holds the decoded name, and Hugo
    # encodes a path with a non-ASCII slug in the href it emits.
    relative = urllib.parse.unquote(link_path[len(base) :])
    if not relative or relative.endswith("/"):
        candidates = [SITE / relative / "index.html"]
    else:
        # A link to an emitted file — what a hand-written `.html` path or
        # uglyURLs produces — before the directory form Hugo defaults to.
        candidates = [SITE / relative, SITE / relative / "index.html"]
    for candidate in candidates:
        if candidate in pages:
            return candidate
    return None


def source_of(page: Path) -> Path | None:
    """The Markdown a rendered page came from, where one exists.

    Hugo also emits pages nothing in docs/content wrote — the taxonomy lists,
    the section index, `404.html` — and those fall back to the rendered path.
    The filename is part of the question: attributing every non-`index.html`
    page to its section's `_index.md` would name a file that never wrote the
    link.
    """
    relative = page.relative_to(SITE)
    if relative.name == "index.html":
        directory = relative.parent
        candidates = [CONTENT / directory / "_index.md"]
        if directory != Path("."):
            candidates.insert(0, CONTENT / f"{directory}.md")
    else:
        candidates = [CONTENT / relative.with_suffix(".md")]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def lines_of(source: Path, fragment: str) -> list[int]:
    """Every line writing this fragment, so the failure names what to edit.

    All of them, because one dead anchor is usually written in several places
    and reporting only the first would name one line for links on four others.
    """
    text = source.read_text(encoding="utf-8", errors="replace").splitlines()
    # The closing parenthesis is what makes this the link rather than a heading
    # or a sentence mentioning the same word; the bare form is the fallback for
    # a fragment a shortcode assembled. Both stop at an identifier character, or
    # a broken `#file-load` would report the line of a working `#file-loads`.
    quoted = re.escape(fragment)
    for pattern in (rf"#{quoted}\)", rf"#{quoted}(?![\w-])"):
        matcher = re.compile(pattern)
        found = [
            number
            for number, line in enumerate(text, start=1)
            if matcher.search(line) is not None
        ]
        if found:
            return found
    return []


def location(page: Path, fragment: str) -> tuple[str, str]:
    """Where the link is written, and any further lines writing the same one."""
    source = source_of(page)
    if source is None:
        return str(page.relative_to(ROOT)), ""
    shown = str(source.relative_to(ROOT))
    lines = lines_of(source, fragment)
    if not lines:
        return shown, ""
    rest = ", ".join(str(line) for line in lines[1:])
    return f"{shown}:{lines[0]}", f" Also written at line {rest}." if rest else ""


def nearest(fragment: str, ids: set[str]) -> str:
    matches = difflib.get_close_matches(
        fragment, sorted(ids), n=SUGGESTIONS, cutoff=0.6
    )
    if matches:
        listed = ", ".join(f"`{match}`" for match in matches)
        return f" Nearest ids there: {listed}."
    return f" It emits {len(ids)} ids, none of them close to this one."


def page_label(page: Path) -> str:
    """A page named by the Markdown that wrote it, where one did."""
    source = source_of(page)
    return str((source or page).relative_to(ROOT))


def check() -> tuple[int, int, int, list[str]]:
    base = base_path()
    pages = read_pages()

    problems: list[str] = []
    # One dead anchor written five times on a page is one problem with five
    # lines against it, not five problems.
    reported: set[tuple[Path, str, str]] = set()
    same_page = 0
    cross_page = 0
    for path, page in sorted(pages.items()):
        for href in page.links:
            parts = urllib.parse.urlsplit(href)
            if parts.scheme or parts.netloc or not parts.fragment:
                continue
            fragment = urllib.parse.unquote(parts.fragment)
            if parts.path:
                cross_page += 1
            else:
                same_page += 1

            if (path, parts.path, fragment) in reported:
                continue
            reported.add((path, parts.path, fragment))
            where, also = location(path, fragment)

            if parts.path:
                # Relative against the page's own URL first: `../target/#deep`
                # and `other/#deep` both resolve in a browser, and reading them
                # literally would report a working link as leaving the site.
                wanted = urllib.parse.urljoin(page_url(path, base), parts.path)
                if wanted + "/" == base:
                    wanted = base
                if not wanted.startswith(base):
                    problems.append(
                        f"{where}: links to `{parts.path}#{fragment}`, which is "
                        f"outside the site's base path `{base}`. Write a cross-page "
                        f'link as `{{{{< relref "page" >}}}}#{fragment}` so Hugo '
                        f"resolves it.{also}"
                    )
                    continue
                target = resolve_page(wanted, base, pages)
                if target is None:
                    problems.append(
                        f"{where}: links to `{parts.path}#{fragment}`, but the built "
                        f"site has no page at `{wanted}`. Correct the path, or remove "
                        f"the link.{also}"
                    )
                    continue
                named = page_label(target)
            else:
                target = path
                named = "its own page"

            if fragment not in pages[target].ids:
                problems.append(
                    f"{where}: links to `#{fragment}` on {named}, which emits no such "
                    f"anchor. Correct the fragment, or restore the heading it "
                    f"named.{nearest(fragment, pages[target].ids)}{also}"
                )

    if not any(page.has_article for page in pages.values()):
        infra(
            f"No page under {SITE} has an <article> element, so this check read no "
            f"links at all. The theme's markup moved; update the element this "
            f"script reads prose links from."
        )
    if not same_page and not cross_page:
        infra(
            f"The {len(pages)} built pages carry no internal fragment links at all, "
            f"so this check judged nothing. Either every such link was removed, or "
            f"the base path read from {HUGO_CONFIG.name} ({base}) no longer matches "
            f"what the site emits."
        )

    return len(pages), same_page, cross_page, problems


def main() -> int:
    try:
        pages, same_page, cross_page, problems = check()
    except OSError as error:
        # The built site or the Markdown a failure is being attributed to; both
        # are read here, so the message names neither exclusively.
        infra(f"Could not read the documentation: {error}")

    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        fail(f"\n{len(problems)} documentation fragment(s) resolve to no anchor.")

    print(
        f"{cross_page} cross-page and {same_page} same-page documentation fragments "
        f"resolve, across {pages} built pages."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
