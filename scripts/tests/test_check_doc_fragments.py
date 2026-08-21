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
"""Synthetic coverage for scripts/check-doc-fragments.py.

A rendered site and its Markdown sources built in tmp_path, with the script's
four path constants redirected onto them. The live site's verdict is CI's own
documentation job; what has no other cover is the parsing, and the direction
that matters is this check finding less than it claims — which reads exactly
like a clean site.

So the cases that carry the most weight here are the ones where a plausible
implementation reports clean: attributes without quotes (what `--minify`
emits), a base path that stops matching, and prose the `<article>` scoping no
longer reaches. Each has a floor that fails instead, and a test that the floor
fires.
"""

from pathlib import Path

import pytest

BASE_URL = "https://example.test/site/"
BASE = "/site/"


def exit_code(module) -> int:
    try:
        return module.main()
    except SystemExit as error:
        return error.code


def render(ids=(), links=(), *, article=True, quoted=True, outside=()) -> str:
    """A rendered page: `ids` become headings, `links` go inside <article>.

    Each heading carries the self-anchor hugo-book renders beside it, so every
    case here also exercises the rule that skips them. `quoted=False` is what
    Hugo's `--minify` really emits, and `outside` puts a link where the theme's
    menu and table of contents put theirs.
    """

    def attribute(name: str, value: str) -> str:
        return f'{name}="{value}"' if quoted else f"{name}={value}"

    headings = "\n".join(
        f"<h2 {attribute('id', name)}>{name}"
        f"<a {attribute('class', 'anchor')} {attribute('href', '#' + name)}>#</a>"
        f"</h2>"
        for name in ids
    )
    inside = "\n".join(f"<a {attribute('href', href)}>text</a>" for href in links)
    prose = f"{headings}\n{inside}"
    if article:
        prose = f"<article {attribute('class', 'markdown')}>\n{prose}\n</article>"
    menu = "\n".join(f"<a {attribute('href', href)}>menu</a>" for href in outside)
    # Every real page carries these, and they are the reason anchors are read
    # from `name` on <a> alone rather than on any element.
    head = (
        f"<head><meta {attribute('name', 'viewport')} {attribute('content', 'x')}>"
        f"<meta {attribute('name', 'description')} {attribute('content', 'x')}>"
        f"</head>"
    )
    return f"<!doctype html><html>{head}<body>\n{menu}\n{prose}\n</body></html>\n"


def build(root: Path, relative: str, body: str) -> Path:
    path = root / "docs" / "public" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def source(root: Path, relative: str, body: str) -> Path:
    path = root / "docs" / "content" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def config(root: Path, base_url: str = BASE_URL) -> None:
    path = root / "docs" / "hugo.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'baseURL = "{base_url}"\n', encoding="utf-8")


def clean_site(
    root: Path,
    *,
    base_url: str = BASE_URL,
    links=("#intro", f"{BASE}target/#deep"),
    quoted: bool = True,
    outside=(),
) -> None:
    """A site that passes, so a single perturbation is the test.

    Spelled as keyword parameters rather than **overrides so that a mistyped
    knob is a TypeError rather than a test quietly exercising the default site.
    """
    config(root, base_url)
    build(
        root,
        "hub/index.html",
        render(ids=("intro",), links=links, quoted=quoted, outside=outside),
    )
    build(root, "target/index.html", render(ids=("deep",), quoted=quoted))
    source(
        root,
        "hub.md",
        '# Hub\n\nSee [intro](#intro).\nThen [deep]({{< relref "target" >}}#deep).\n',
    )
    source(root, "target.md", "# Target\n\n## Deep\n")


@pytest.fixture()
def root(tmp_path, check_doc_fragments, monkeypatch):
    # SITE, CONTENT and HUGO_CONFIG are derived from ROOT at import time, so
    # moving ROOT alone would leave all three pointing at the real repository.
    monkeypatch.setattr(check_doc_fragments, "ROOT", tmp_path)
    monkeypatch.setattr(
        check_doc_fragments, "HUGO_CONFIG", tmp_path / "docs" / "hugo.toml"
    )
    monkeypatch.setattr(check_doc_fragments, "SITE", tmp_path / "docs" / "public")
    monkeypatch.setattr(check_doc_fragments, "CONTENT", tmp_path / "docs" / "content")
    return tmp_path


def test_a_site_whose_fragments_all_resolve_passes(root, check_doc_fragments, capsys):
    clean_site(root)
    assert exit_code(check_doc_fragments) == 0
    out = capsys.readouterr().out
    assert "1 cross-page and 1 same-page documentation fragments resolve" in out
    assert "across 2 built pages" in out


def test_a_cross_page_fragment_with_no_anchor_fails(root, check_doc_fragments, capsys):
    # The #867 shape: the page resolves, the fragment does not, and Hugo's
    # relref validation has nothing to say about it.
    clean_site(root, links=("#intro", f"{BASE}target/#renamed"))
    assert exit_code(check_doc_fragments) == 1
    err = capsys.readouterr().err
    assert "links to `#renamed` on docs/content/target.md" in err
    assert "emits no such anchor" in err


def test_a_same_page_fragment_with_no_anchor_fails(root, check_doc_fragments, capsys):
    clean_site(root, links=("#gone", f"{BASE}target/#deep"))
    assert exit_code(check_doc_fragments) == 1
    assert "links to `#gone` on its own page" in capsys.readouterr().err


def test_the_failure_names_the_markdown_line_that_wrote_the_link(
    root, check_doc_fragments, capsys
):
    # The built HTML is not what anyone edits, so a failure that named it would
    # leave the reader to find the source themselves.
    clean_site(root, links=("#intro", f"{BASE}target/#renamed"))
    source(
        root,
        "hub.md",
        '# Hub\n\nfiller\nfiller\n[deep]({{< relref "target" >}}#renamed).\n',
    )
    assert exit_code(check_doc_fragments) == 1
    assert "docs/content/hub.md:5:" in capsys.readouterr().err


def test_every_line_writing_a_dead_fragment_is_named(root, check_doc_fragments, capsys):
    # One dead anchor is usually written several times. Reporting only the first
    # line would name one line for links written on three others.
    clean_site(root, links=("#intro", f"{BASE}target/#renamed"))
    source(
        root,
        "hub.md",
        "# Hub\n\n[a](#renamed)\nfiller\n[b](#renamed)\n[c](#renamed)\n",
    )
    assert exit_code(check_doc_fragments) == 1
    err = capsys.readouterr().err
    assert "docs/content/hub.md:3:" in err
    assert "Also written at line 5, 6." in err


def test_the_line_lookup_stops_at_an_identifier_character(
    root, check_doc_fragments, capsys
):
    # A working `#file-loads` contains a broken `#file-load`. Matched as a bare
    # substring, the headline names the innocent line and demotes the defect to
    # the "also" clause.
    clean_site(root, links=("#file-load", f"{BASE}target/#deep"))
    source(
        root,
        "hub.md",
        '# Hub\n\nSee [loads](#file-loads).\n<a href="#file-load">bad</a>\n',
    )
    assert exit_code(check_doc_fragments) == 1
    assert "docs/content/hub.md:4:" in capsys.readouterr().err


def test_a_prose_mention_does_not_outrank_the_link_itself(
    root, check_doc_fragments, capsys
):
    # A page that discusses an anchor before linking it. Matching the link form
    # first is what keeps the headline on the line there is something to fix on.
    clean_site(root, links=("#gone", f"{BASE}target/#deep"))
    source(root, "hub.md", "# Hub\n\nThe `#gone` anchor moved.\n[x](#gone)\n")
    assert exit_code(check_doc_fragments) == 1
    err = capsys.readouterr().err
    assert "docs/content/hub.md:4:" in err
    assert "Also written" not in err


def test_a_fragment_written_without_a_closing_paren_still_finds_its_line(
    root, check_doc_fragments, capsys
):
    # The bare fallback exists for a fragment a shortcode assembled, which never
    # reaches the `](#anchor)` form. Without it the failure carries no line at
    # all and the reader has to grep for it.
    clean_site(root, links=("#gone", f"{BASE}target/#deep"))
    source(root, "hub.md", '# Hub\n\nfiller\n{{< card anchor="#gone" >}}\n')
    assert exit_code(check_doc_fragments) == 1
    assert "docs/content/hub.md:4:" in capsys.readouterr().err


def test_a_page_that_is_not_an_index_is_not_blamed_on_its_section(
    root, check_doc_fragments, capsys
):
    # Deriving the source from the directory alone attributes 404.html — and any
    # page under uglyURLs — to a `_index.md` that never wrote the link.
    config(root)
    build(root, "404.html", render(links=("#gone",)))
    source(root, "_index.md", "# Home\n\n[x](#gone)\n")
    assert exit_code(check_doc_fragments) == 1
    assert "docs/public/404.html:" in capsys.readouterr().err


def test_at_most_three_near_misses_are_offered(root, check_doc_fragments, capsys):
    clean_site(root, links=("#intro", f"{BASE}target/#deep-x"))
    build(
        root,
        "target/index.html",
        render(ids=("deep-a", "deep-b", "deep-c", "deep-d", "deep-e")),
    )
    assert exit_code(check_doc_fragments) == 1
    assert capsys.readouterr().err.count("`deep-") == 3


def test_an_unreadable_hugo_config_is_infrastructure(root, check_doc_fragments, capsys):
    # The message has to name the configuration rather than the site: main()'s
    # own OSError handler would otherwise catch this too, and the reader would
    # be sent looking in docs/public for a file that is not there.
    clean_site(root)
    (root / "docs" / "hugo.toml").unlink()
    assert exit_code(check_doc_fragments) == 2
    assert "Could not read the Hugo configuration" in capsys.readouterr().err


def test_an_unreadable_built_page_is_infrastructure(root, check_doc_fragments, capsys):
    # Without the handler this is a traceback and exit 1 — "a fragment does not
    # resolve" — for something that is not a documentation defect at all.
    clean_site(root)
    (root / "docs" / "public" / "broken.html").mkdir()
    assert exit_code(check_doc_fragments) == 2
    assert "Could not read the documentation" in capsys.readouterr().err


def test_one_dead_anchor_linked_repeatedly_is_one_problem(
    root, check_doc_fragments, capsys
):
    clean_site(root, links=(f"{BASE}target/#renamed",) * 4 + ("#intro",))
    assert exit_code(check_doc_fragments) == 1
    assert "1 documentation fragment(s)" in capsys.readouterr().err


def test_a_near_miss_id_is_offered_as_the_repair(root, check_doc_fragments, capsys):
    clean_site(root, links=("#intro", f"{BASE}target/#deeps"))
    assert exit_code(check_doc_fragments) == 1
    assert "Nearest ids there: `deep`." in capsys.readouterr().err


def test_no_near_miss_says_so_rather_than_offering_nothing(
    root, check_doc_fragments, capsys
):
    clean_site(root, links=("#intro", f"{BASE}target/#utterly-unrelated"))
    assert exit_code(check_doc_fragments) == 1
    assert "It emits 1 ids, none of them close to this one." in capsys.readouterr().err


def test_minified_attributes_without_quotes_are_read(root, check_doc_fragments, capsys):
    # `just docs` runs Hugo with --minify, which emits `id=deep` and
    # `href=/site/target/#deep`. A regex written against `id="..."` matches
    # nothing at all, and a checker that saw no anchors would report clean.
    clean_site(root, quoted=False, links=("#intro", f"{BASE}target/#renamed"))
    assert exit_code(check_doc_fragments) == 1
    assert "links to `#renamed`" in capsys.readouterr().err


def test_minified_anchors_still_satisfy_a_link(root, check_doc_fragments, capsys):
    # The other direction of the same risk: unquoted ids must *count*, or every
    # link would fail rather than every link passing.
    clean_site(root, quoted=False)
    assert exit_code(check_doc_fragments) == 0
    assert (
        "1 cross-page and 1 same-page documentation fragments resolve"
        in capsys.readouterr().out
    )


def test_a_name_attribute_counts_as_an_anchor(root, check_doc_fragments):
    clean_site(root)
    build(root, "target/index.html", '<article><a name="deep">x</a></article>')
    assert exit_code(check_doc_fragments) == 0


def test_a_percent_encoded_fragment_resolves(root, check_doc_fragments):
    clean_site(root, links=("#intro", f"{BASE}target/#deep%20end"))
    build(root, "target/index.html", render(ids=("deep end",)))
    assert exit_code(check_doc_fragments) == 0


def test_a_name_attribute_on_anything_but_an_anchor_is_not_one(
    root, check_doc_fragments, capsys
):
    # Every real page carries `<meta name=description>` and three siblings, so
    # dropping the `tag == "a"` guard as redundant would silently make
    # `#description` and `#viewport` resolve on every page in the site.
    clean_site(root, links=("#description", f"{BASE}target/#deep"))
    assert exit_code(check_doc_fragments) == 1
    assert "links to `#description` on its own page" in capsys.readouterr().err


def test_an_external_link_with_a_fragment_is_left_alone(root, check_doc_fragments):
    # External liveness is a different failure mode with a network dependency,
    # and is deliberately out of scope.
    clean_site(
        root,
        links=("#intro", f"{BASE}target/#deep", "https://example.com/page#whatever"),
    )
    assert exit_code(check_doc_fragments) == 0


def test_a_protocol_relative_link_is_external_too(root, check_doc_fragments):
    # `//example.com/page#x` has no scheme but is not this site; read as a path
    # it would be reported as leaving the base, telling the author to wrap a
    # genuinely external URL in relref.
    clean_site(
        root, links=("#intro", f"{BASE}target/#deep", "//example.com/page#whatever")
    )
    assert exit_code(check_doc_fragments) == 0


def test_a_relative_cross_page_link_resolves(root, check_doc_fragments):
    # `../target/#deep` works in a browser and is inside the site; read
    # literally it does not start with the base path, and the author would be
    # told a working link leaves the site.
    config(root)
    build(root, "docs/hub/index.html", render(links=("../target/#deep",)))
    build(root, "docs/target/index.html", render(ids=("deep",)))
    assert exit_code(check_doc_fragments) == 0


def test_a_link_to_the_site_root_without_its_trailing_slash_resolves(
    root, check_doc_fragments
):
    clean_site(root, links=("#intro", "/site#home"))
    build(root, "index.html", render(ids=("home",)))
    assert exit_code(check_doc_fragments) == 0


def test_a_link_to_an_emitted_html_path_resolves(root, check_doc_fragments):
    # What a hand-written `.html` path or uglyURLs produces. Only the second
    # resolution candidate is exercised by the directory form.
    config(root)
    build(root, "hub/index.html", render(links=(f"{BASE}target.html#deep",)))
    build(root, "target.html", render(ids=("deep",)))
    assert exit_code(check_doc_fragments) == 0


def test_a_percent_encoded_page_path_resolves(root, check_doc_fragments):
    # The fragment is decoded, so the path has to be too, or a page with a
    # non-ASCII slug is reported as missing from its own build.
    config(root)
    build(root, "hub/index.html", render(links=(f"{BASE}caf%C3%A9/#deep",)))
    build(root, "café/index.html", render(ids=("deep",)))
    assert exit_code(check_doc_fragments) == 0


def test_a_base_url_without_a_trailing_slash_is_normalised(
    root, check_doc_fragments, capsys
):
    # Without the normalisation the site-relative remainder keeps its leading
    # slash, every candidate becomes an absolute path outside SITE, and every
    # cross-page link in the site is reported as having no page.
    clean_site(root, base_url="https://example.test/site")
    assert exit_code(check_doc_fragments) == 0
    assert "1 cross-page and 1 same-page documentation fragments resolve" in (
        capsys.readouterr().out
    )


def test_a_link_without_a_fragment_is_not_counted(root, check_doc_fragments, capsys):
    clean_site(root, links=("#intro", f"{BASE}target/#deep", f"{BASE}target/"))
    assert exit_code(check_doc_fragments) == 0
    assert (
        "1 cross-page and 1 same-page documentation fragments resolve"
        in capsys.readouterr().out
    )


def test_a_headings_own_self_anchor_is_not_counted(root, check_doc_fragments, capsys):
    # hugo-book renders `<h2 id=x>Title<a class=anchor href=#x>#</a></h2>` beside
    # every heading. Those are satisfied by construction, so counting them would
    # say nothing about the prose — on this repository there are 430 of them
    # against 172 prose links. The rule is written against the enclosing id
    # rather than the theme's class name, so it holds whatever the theme calls
    # it.
    clean_site(root)
    assert exit_code(check_doc_fragments) == 0
    assert (
        "1 cross-page and 1 same-page documentation fragments resolve"
        in capsys.readouterr().out
    )


def test_a_link_naming_an_id_that_does_not_enclose_it_is_still_counted(
    root, check_doc_fragments, capsys
):
    # The other direction: a heading linking to a *different* heading is prose.
    config(root)
    build(
        root,
        "hub/index.html",
        "<article><h2 id=here>x<a href=#gone>#</a></h2></article>",
    )
    assert exit_code(check_doc_fragments) == 1
    assert "links to `#gone` on its own page" in capsys.readouterr().err


def test_a_void_element_does_not_enclose_what_follows_it(root, check_doc_fragments):
    # <img> has no end tag, so pushing it would make the rest of the document
    # read as nested inside it and silently swallow every later self-named link.
    config(root)
    build(
        root,
        "hub/index.html",
        "<article><img id=gone><a href=#gone>x</a></article>",
    )
    assert exit_code(check_doc_fragments) == 0


def test_an_unclosed_tag_does_not_leave_the_footer_reading_as_prose(
    root, check_doc_fragments
):
    # HTML lets <p> close implicitly, and Hugo's output uses that. Popping only
    # the top of the open-element stack would leave <article> on it for the rest
    # of the document, so every theme link after </article> would be judged as
    # prose. Popping through to the matching tag is what keeps that honest.
    config(root)
    build(
        root,
        "hub/index.html",
        "<article><h2 id=here>x</h2><p>text<a href=#here>link</a></article>"
        "<footer><a href=#not-there>theme</a></footer>",
    )
    assert exit_code(check_doc_fragments) == 0


def test_a_site_whose_only_fragments_are_self_anchors_is_infrastructure(
    root, check_doc_fragments, capsys
):
    # Why the skip matters beyond a tidy count: left in, several hundred
    # self-anchors would hold this floor up while every prose link had stopped
    # being seen.
    clean_site(root, links=())
    assert exit_code(check_doc_fragments) == 2
    assert "carry no internal fragment links at all" in capsys.readouterr().err


def test_a_fragment_link_outside_the_base_path_is_reported(
    root, check_doc_fragments, capsys
):
    clean_site(root, links=("#intro", "/elsewhere/page/#deep"))
    assert exit_code(check_doc_fragments) == 1
    assert "outside the site's base path `/site/`" in capsys.readouterr().err


def test_a_fragment_link_to_a_page_the_build_lacks_is_reported(
    root, check_doc_fragments, capsys
):
    clean_site(root, links=("#intro", f"{BASE}missing/#deep"))
    assert exit_code(check_doc_fragments) == 1
    assert "the built site has no page at `/site/missing/`" in capsys.readouterr().err


def test_a_page_path_without_a_trailing_slash_still_resolves(root, check_doc_fragments):
    clean_site(root, links=("#intro", f"{BASE}target#deep"))
    assert exit_code(check_doc_fragments) == 0


def test_links_outside_the_article_are_the_themes_and_are_ignored(
    root, check_doc_fragments, capsys
):
    # The sidebar menu and both copies of the table of contents sit outside
    # <article>; they are the theme's output, not this repository's prose.
    clean_site(root, outside=(f"{BASE}target/#not-there",))
    assert exit_code(check_doc_fragments) == 0
    assert (
        "1 cross-page and 1 same-page documentation fragments resolve"
        in capsys.readouterr().out
    )


def test_a_section_index_maps_to_its_underscore_index_source(
    root, check_doc_fragments, capsys
):
    config(root)
    build(root, "guide/index.html", render(links=(f"{BASE}guide/#gone",)))
    source(root, "guide/_index.md", "# Guide\n\n[x](#gone)\n")
    assert exit_code(check_doc_fragments) == 1
    assert "docs/content/guide/_index.md:3:" in capsys.readouterr().err


def test_the_home_page_maps_to_the_root_index_source(root, check_doc_fragments, capsys):
    config(root)
    build(root, "index.html", render(links=("#gone",)))
    source(root, "_index.md", "# Home\n\n[x](#gone)\n")
    assert exit_code(check_doc_fragments) == 1
    assert "docs/content/_index.md:3:" in capsys.readouterr().err


def test_a_generated_page_falls_back_to_the_rendered_path(
    root, check_doc_fragments, capsys
):
    # Hugo emits taxonomy lists and section indexes that nothing under
    # docs/content wrote, so there is no Markdown line to name.
    config(root)
    build(root, "tags/index.html", render(links=("#gone",)))
    assert exit_code(check_doc_fragments) == 1
    assert "docs/public/tags/index.html:" in capsys.readouterr().err


def test_a_different_base_path_is_followed_rather_than_assumed(
    root, check_doc_fragments, capsys
):
    # Hardcoding this repository's own base path would classify every internal
    # link as external the day baseURL moves, and the check would pass having
    # judged nothing.
    clean_site(
        root,
        base_url="https://example.test/other/",
        links=("#intro", "/other/target/#renamed"),
    )
    assert exit_code(check_doc_fragments) == 1
    assert "links to `#renamed` on docs/content/target.md" in capsys.readouterr().err


def test_no_built_site_is_infrastructure_and_names_the_build(
    root, check_doc_fragments, capsys
):
    config(root)
    (root / "docs" / "public").mkdir(parents=True)
    assert exit_code(check_doc_fragments) == 2
    assert "Run `just docs` first" in capsys.readouterr().err


def test_a_site_with_no_article_anywhere_is_infrastructure(
    root, check_doc_fragments, capsys
):
    # If the theme's markup moves, this check reads no links at all — which is
    # indistinguishable from a clean site unless it says so. The floor is
    # site-wide on purpose: one page legitimately carrying no <article> says
    # nothing, while none of them carrying one says the element was renamed.
    config(root)
    build(root, "hub/index.html", render(links=("#gone",), article=False))
    build(root, "target/index.html", render(ids=("deep",), article=False))
    assert exit_code(check_doc_fragments) == 2
    assert "has an <article> element" in capsys.readouterr().err


def test_a_site_with_no_internal_fragment_links_is_infrastructure(
    root, check_doc_fragments, capsys
):
    # The floor under the base-path classification: every link external, or the
    # prefix no longer matching, both land here rather than reporting clean.
    clean_site(root, links=("https://example.com/#x",))
    assert exit_code(check_doc_fragments) == 2
    assert "carry no internal fragment links at all" in capsys.readouterr().err


def test_a_config_without_a_base_url_is_infrastructure(
    root, check_doc_fragments, capsys
):
    clean_site(root)
    (root / "docs" / "hugo.toml").write_text('title = "x"\n', encoding="utf-8")
    assert exit_code(check_doc_fragments) == 2
    assert "declares no baseURL" in capsys.readouterr().err


def test_an_unparseable_config_is_infrastructure(root, check_doc_fragments, capsys):
    clean_site(root)
    (root / "docs" / "hugo.toml").write_text("baseURL = \n", encoding="utf-8")
    assert exit_code(check_doc_fragments) == 2
    assert "does not parse" in capsys.readouterr().err
