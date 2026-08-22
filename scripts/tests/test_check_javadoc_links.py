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
"""Synthetic coverage for scripts/check-javadoc-links.py.

The checker owns a small Java index — types, their nesting, and which of their
members are fields and which are methods — and every verdict it reaches depends
on that index being right. These fixtures are temporary trees, so a repository
source growing a new reference changes the real check rather than this suite.

Several cases here are the shapes that fooled the hand scan this checker
replaced: a call site read as a declaration, a method of the same name on a
nested type, and a reference wrapped across two Javadoc lines.
"""

from pathlib import Path

import pytest


def write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture()
def tree(tmp_path, check_javadoc_links, monkeypatch):
    monkeypatch.setattr(check_javadoc_links, "ROOT", tmp_path)
    monkeypatch.setattr(
        check_javadoc_links,
        "MAIN_SOURCE_PATTERN",
        "flink-*/src/main/java*/**/*.java",
    )
    return tmp_path / "flink-demo/src/main/java/demo"


def audit(check_javadoc_links) -> list[str]:
    _, problems = check_javadoc_links.check()
    return problems


def test_a_class_javadoc_is_read_through_the_types_annotations(
    tree, check_javadoc_links
):
    """The comment documents the type below it, annotations and all.

    Every public type here carries a Flink API-tier annotation, so treating
    `@Public` as something other than whitespace left the class javadoc of the
    whole repository without a context, and every bare reference in one
    unchecked (issue #930).
    """
    write(
        tree / "Builder.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Entry point, configured through {@link #handler}. */
@Public
public class Builder {
    private Handler handler;

    public Builder handler(Handler handler) {
        return this;
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Builder.java:5" in problems[0]
    assert "`#handler(Handler)`" in problems[0]


def test_a_class_javadoc_is_read_through_an_annotation_with_arguments(
    tree, check_javadoc_links
):
    """Annotation arguments are no more a declaration than the annotation name.

    Removing only ``@TypeInfo`` leaves ``(Factory.class)`` between the comment
    and the declaration, so the class javadoc loses its context and a bare
    reference in it is skipped.
    """
    write(
        tree / "Builder.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Entry point, configured through {@link #handler}. */
@Public
@TypeInfo(Factory.class)
public class Builder {
    private Handler handler;

    public Builder handler(Handler handler) {
        return this;
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Builder.java:5" in problems[0]
    assert "`#handler(Handler)`" in problems[0]


def test_a_reference_to_a_private_field_offers_a_code_span(tree, check_javadoc_links):
    """No method of the name means no parameter list to suggest.

    The reference is dead all the same — a private field is not in the
    generated documentation — so the message offers the other repair, which is
    what the prose almost always meant (issue #931).
    """
    write(
        tree / "Writer.java",
        """package demo;

public class Writer {
    private Throwable asyncError;

    /** Set by the callback thread; {@link #asyncError} is read on the next write. */
    public void write(String record) {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`{@code asyncError}`" in problems[0]
    assert "parameter list" in problems[0]


def test_a_package_private_field_is_no_more_documented_than_a_private_one(
    tree, check_javadoc_links
):
    write(
        tree / "State.java",
        """package demo;

public class State {
    boolean topicMissing;

    /** Cleared once the topic exists; see {@link #topicMissing}. */
    public void repair() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "package field `topicMissing`" in problems[0]


def test_a_method_shadowed_by_a_private_field_fails(tree, check_javadoc_links):
    write(
        tree / "Builder.java",
        """package demo;

public class Builder {
    private Handler handler = Handler.fail();

    /** Sets the handler. */
    public Builder handler(Handler handler) {
        this.handler = handler;
        return this;
    }
}
""",
    )
    write(
        tree / "Sink.java",
        """package demo;

/** Entry point, configured through {@link Builder#handler}. */
public final class Sink {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Sink.java:3" in problems[0]
    assert "private field `handler`" in problems[0]
    assert "`Builder#handler(Handler)`" in problems[0]


def test_an_overloaded_method_fails_and_lists_its_signatures(tree, check_javadoc_links):
    write(
        tree / "Closers.java",
        """package demo;

public final class Closers {
    /** Closes a collection. */
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) {}

    /** Closes an array, through {@link #closeAll(Iterable)}. */
    public static void closeAll(AutoCloseable... closeables) {}
}
""",
    )
    write(
        tree / "Caller.java",
        """package demo;

/** Releases through {@link Closers#closeAll}. */
final class Caller {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Caller.java:3" in problems[0]
    assert "`Closers#closeAll(Iterable)`" in problems[0]
    assert "`Closers#closeAll(AutoCloseable...)`" in problems[0]


def test_a_reference_naming_only_a_field_passes(tree, check_javadoc_links):
    write(
        tree / "Bounds.java",
        """package demo;

public final class Bounds {
    /** No bound. */
    public static final long UNBOUNDED = -1L;

    /** Defaults to {@link #UNBOUNDED}. */
    public long bound() {
        return UNBOUNDED;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_reference_carrying_its_parameter_list_passes(tree, check_javadoc_links):
    write(
        tree / "Builder.java",
        """package demo;

public class Builder {
    private Handler handler;

    /** Sets it; see {@link #handler(Handler)}. */
    public Builder handler(Handler handler) {
        this.handler = handler;
        return this;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_same_named_method_on_a_nested_type_is_not_the_enclosing_one(
    tree, check_javadoc_links
):
    write(
        tree / "Admin.java",
        """package demo;

public class Admin {
    /** The creation call, taken as a seam. */
    interface Creator {
        Subscription create(Subscription subscription);
    }

    /** The body of {@link #create}. */
    static Info createWith(Creator creator) {
        return null;
    }

    /** Creates it. */
    public Info create(Destination destination) {
        return createWith(null);
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_bracketed_field_initializer_does_not_hide_the_members_below_it(
    tree, check_javadoc_links
):
    """The scan has to walk an initializer, not resume just after its `=`.

    Resuming inside the expression puts the next `)` against a brace depth of
    zero, and every member declared below it is then read at the wrong depth
    and dropped — silently, since a checker that indexes nothing reports
    nothing. Measured on `RowRanges.java`, where it cost 17 methods.
    """
    write(
        tree / "Ranges.java",
        """package demo;

public final class Ranges {
    private static final byte[] SMALLEST = copyFrom(new byte[] {0});

    private Handler handler;

    /** Sets it, unlike {@link #handler}. */
    public Ranges handler(Handler handler) {
        this.handler = handler;
        return this;
    }

    private static byte[] copyFrom(byte[] bytes) {
        return bytes;
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`#handler(Handler)`" in problems[0]


def test_a_call_site_is_not_a_declaration(tree, check_javadoc_links):
    write(
        tree / "Admin.java",
        """package demo;

public class Admin {
    private final Info info = load();

    /** Wrapped by {@link #createWith}. */
    public Info create() {
        return createWith(info);
    }

    static Info createWith(Info info) {
        return info;
    }

    private static Info load() {
        return null;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_reference_wrapped_across_javadoc_lines_is_still_read(
    tree, check_javadoc_links
):
    write(
        tree / "Builder.java",
        """package demo;

public class Builder {
    private Handler handler;

    /** Sets it. */
    public Builder handler(Handler handler) {
        this.handler = handler;
        return this;
    }
}
""",
    )
    write(
        tree / "Sink.java",
        """package demo;

/**
 * Entry point.
 *
 * <p>Under a dropping policy configured through {@link
 * Builder#handler}, a completed checkpoint means every record was published.
 */
public final class Sink {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Sink.java:6" in problems[0]


def test_a_type_outside_the_repository_is_left_alone(tree, check_javadoc_links):
    write(
        tree / "Options.java",
        """package demo;

import java.time.Duration;

public final class Options {
    /** Defaults to {@link Duration#ofSeconds} of the vendor default. */
    public Duration timeout() {
        return Duration.ofSeconds(10);
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_an_unimported_simple_name_is_not_guessed_from_another_package(
    tree, check_javadoc_links
):
    write(
        tree / "Options.java",
        """package demo;

/** Bounded like {@link TimestampBound#strong}. */
public final class Options {}
""",
    )
    write(
        tree.parent.parent.parent.parent.parent
        / "flink-other/src/main/java/other/TimestampBound.java",
        """package other;

public final class TimestampBound {
    private final Mode strong = null;

    public static TimestampBound strong(Mode mode) {
        return null;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_bare_reference_resolves_in_the_enclosing_type(tree, check_javadoc_links):
    write(
        tree / "Reader.java",
        """package demo;

public class Reader {
    private final Wakeup wakeUp = new Wakeup();

    public void wakeUp(Wakeup signal) {}

    /** One stream being read. */
    final class Active {
        /** Interrupted by {@link #wakeUp}. */
        void read() {}
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`#wakeUp(Wakeup)`" in problems[0]


def test_a_documented_public_field_renders_an_anchor_and_passes(
    tree, check_javadoc_links
):
    write(
        tree / "Registry.java",
        """package demo;

public final class Registry {
    /** The shared instance. */
    public static final Registry instance = new Registry();

    /** Reads {@link #instance}. */
    public static Registry instance(String name) {
        return instance;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_an_enum_constant_is_not_read_as_a_method(tree, check_javadoc_links):
    """A constant with arguments looks exactly like a method call.

    Java lets a constant and a method share a name, which is the shape that
    makes the difference visible: read as a method, `retry` would have two
    signatures and the reference below would be reported as ambiguous. It
    resolves to the constant, and renders an anchor.
    """
    write(
        tree / "Mode.java",
        """package demo;

public enum Mode {
    /** Retried three times. */
    retry(3),
    /** Never retried. */
    never(0);

    private final int attempts;

    Mode(int attempts) {
        this.attempts = attempts;
    }

    /** Falls back to {@link #retry}. */
    public int retry() {
        return attempts;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_constant_wins_over_the_overloads_that_share_its_name(
    tree, check_javadoc_links
):
    """Java lets a constant and an overloaded method share a name.

    The reference binds the constant, which is documented, so there is nothing
    to disambiguate — reporting the overloads would be a finding for a link
    that renders an anchor. It also pins which constant the declaration is
    named after: a list carries several, and only the first opens the
    parameter list.
    """
    write(
        tree / "Mode.java",
        """package demo;

public enum Mode {
    /** Retried. */
    RETRY(3),
    /** Never retried. */
    never(0);

    private final int attempts;

    Mode(int attempts) {
        this.attempts = attempts;
    }

    /** Falls back to {@link #never}. */
    public int never() {
        return attempts;
    }

    public int never(int scale) {
        return attempts * scale;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_qualified_reference_does_not_search_the_enclosing_type(
    tree, check_javadoc_links
):
    """`Type#member` says which type to look in, and Javadoc looks there.

    It searches that type and what it inherits, not whatever encloses it, so a
    shadowed member on the *outer* class is not this reference's problem.
    """
    write(
        tree / "Reader.java",
        """package demo;

public class Reader {
    private final Wakeup wakeUp = new Wakeup();

    public void wakeUp(Wakeup signal) {}

    /** One stream being read, interrupted by {@link Active#wakeUp}. */
    public final class Active {
        void read() {}
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_see_tag_is_held_to_the_same_rule(tree, check_javadoc_links):
    write(
        tree / "Closers.java",
        """package demo;

public final class Closers {
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) {}

    public static void closeAll(AutoCloseable... closeables) {}
}
""",
    )
    write(
        tree / "Caller.java",
        """package demo;

/**
 * Releases everything.
 *
 * @see Closers#closeAll
 */
final class Caller {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Caller.java:6" in problems[0]


def test_a_comment_shaped_string_is_not_read_as_javadoc(tree, check_javadoc_links):
    """The string below would be a finding if it were read as a comment.

    It names a method this class shadows with a private field of the same
    name, so a scan that does not skip string literals reports it — the same
    verdict as the first test here, on prose that is data.
    """
    write(
        tree / "Message.java",
        """package demo;

public final class Message {
    private final String handler = "/** {@link Message#handler} */";

    public String handler(String value) {
        return handler + value;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_no_sources_is_an_infrastructure_failure(tree, check_javadoc_links):
    with pytest.raises(FileNotFoundError):
        check_javadoc_links.check()
