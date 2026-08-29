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

The presence rules of issue #1093 — Javadoc on the tier-annotated surface, and
ConfigOption Javadoc equal to the withDescription text — are covered at the
bottom, one discriminating fixture per rule: each fails when its rule is
removed, not merely executes it.
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

    /** Sets the handler. */
    public Builder handler(Handler handler) {
        return this;
    }

    /** Creates a builder. */
    public Builder() {}
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

    /** Sets the handler. */
    public Builder handler(Handler handler) {
        return this;
    }

    /** Creates a builder. */
    public Builder() {}
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
    """An initializer must not hide the declarations that follow it.

    The former delimiter scan resumed inside the expression, read every member
    below it at the wrong depth, and dropped them silently. Measured on
    `RowRanges.java`, where it cost 17 methods.
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


def test_package_and_import_whitespace_do_not_hide_a_reference(
    tree, check_javadoc_links
):
    write(
        tree / "target/Builder.java",
        """package\tdemo.target;

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
        tree / "source/Sink.java",
        """package demo.source;

import\tdemo.target.Builder;

/** Configured through {@link Builder#handler}. */
public final class Sink {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`Builder#handler(Handler)`" in problems[0]


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


def test_a_comment_between_type_javadoc_and_annotations_keeps_its_context(
    tree, check_javadoc_links
):
    write(
        tree / "Sink.java",
        """package demo;

/** Configured through {@link #handler}. */
// The rationale is recorded next to the declaration.
public final class Sink {
    private final String handler = "default";

    /** Creates a sink. */
    public Sink() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`#handler` resolves to the private field" in problems[0]


def test_a_type_javadoc_after_its_annotation_keeps_its_context(
    tree, check_javadoc_links
):
    write(
        tree / "Sink.java",
        """package demo;

import org.apache.flink.annotation.Internal;

@Internal
/** Configured through {@link #handler}. */
final class Sink {
    private final String handler = "default";
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "`#handler` resolves to the private field" in problems[0]


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
    name, so treating the literal as a comment reports it — the same verdict
    as the first test here, on prose that is data.
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


def test_an_undocumented_public_method_of_a_tier_annotated_type_fails(
    tree, check_javadoc_links
):
    write(
        tree / "FooSource.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** A source. */
@PublicEvolving
public class FooSource {
    public FooSource builder() {
        return this;
    }

    /** Creates a source. */
    public FooSource() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "FooSource.java:8" in problems[0]
    assert (
        "public method 'builder()' of @PublicEvolving type 'FooSource'" in problems[0]
    )
    assert "has no Javadoc; add one" in problems[0]


def test_an_undocumented_public_field_is_reported_like_a_method(
    tree, check_javadoc_links
):
    write(
        tree / "Bounds.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Limits. */
@Public
public final class Bounds {
    public static final long LIMIT = 10L;

    private long used;

    /** Creates bounds. */
    public Bounds() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public field 'LIMIT' of @Public type 'Bounds'" in problems[0]


def test_an_interface_constant_is_an_implicitly_public_field(tree, check_javadoc_links):
    write(
        tree / "Clock.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A clock. */
@Public
public interface Clock {
    Clock SYSTEM = () -> 0L;

    /** Reads time. */
    long millis();
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public field 'SYSTEM' of @Public type 'Clock'" in problems[0]


def test_an_override_member_inherits_its_docs_and_is_exempt(tree, check_javadoc_links):
    write(
        tree / "Sink.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A sink. */
@Public
public class Sink implements AutoCloseable {
    /** Builds a sink. */
    public Sink() {}

    @Override
    public void close() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # The type and the constructor were counted, so the fixture was indexed:
    # an empty tree would also report no problems.
    assert counts.documented == 2


def test_an_internal_nested_type_is_off_the_documented_surface(
    tree, check_javadoc_links
):
    """Its own tier annotation answers for its members, and @Internal says no.

    Neither the nested declaration nor its bare public method is a finding:
    the generated API reference does not show @Internal types.
    """
    write(
        tree / "Source.java",
        """package demo;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;

/** Entry point. */
@Public
public class Source {
    @Internal
    public static class Plumbing {
        public void run() {}
    }

    /** Creates a source. */
    public Source() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # Only the enclosing type and its constructor were counted; the fixture
    # was indexed and the nested type really was excluded rather than unseen.
    assert counts.documented == 2


def test_an_unannotated_nested_type_inherits_the_enclosing_tier(
    tree, check_javadoc_links
):
    write(
        tree / "Source.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Entry point. */
@Public
public class Source {
    /** Configures a source. */
    public static final class Builder {
        public Source build() {
            return null;
        }

        /** Creates a builder. */
        public Builder() {}
    }

    /** Creates a source. */
    public Source() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public method 'build()' of @Public type 'Builder'" in problems[0]


def test_an_enum_constant_of_an_in_scope_enum_needs_its_own_javadoc(
    tree, check_javadoc_links
):
    """Each entry in a comma-separated constant list is its own syntax node.

    Each constant is documented on its own all the same, so the comment above
    the first cannot stand in for the second.
    """
    write(
        tree / "TargetType.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** Where the task goes. */
@PublicEvolving
public enum TargetType {
    /** Delivered over HTTP. */
    HTTP("http"),
    APP_ENGINE("app-engine");

    private final String value;

    TargetType(String value) {
        this.value = value;
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "TargetType.java:10" in problems[0]
    assert (
        "enum constant 'APP_ENGINE' of @PublicEvolving type 'TargetType'" in problems[0]
    )


def test_a_bare_constant_before_an_argument_bearing_one_stays_in_the_list(
    tree, check_javadoc_links
):
    """The first top-level `(` need not belong to the first enum constant."""
    write(
        tree / "Mode.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Selection modes. */
@Public
public enum Mode {
    /** Uses defaults. */
    DEFAULT,
    CUSTOM(1);

    private final int value;

    Mode() {
        this(0);
    }

    Mode(int value) {
        this.value = value;
    }

    /** Returns the encoded value. */
    public int value() {
        return value;
    }
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert len(problems) == 1
    assert "enum constant 'CUSTOM' of @Public type 'Mode'" in problems[0]
    assert counts.documented == 3


def test_a_bare_constant_list_without_a_semicolon_is_still_read(
    tree, check_javadoc_links
):
    """The `;` after the constants is optional when the enum has no members."""
    write(
        tree / "Kind.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Rule kinds. */
@Public
public enum Kind {
    /** A union. */
    UNION,
    INTERSECTION
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "enum constant 'INTERSECTION'" in problems[0]


def test_an_implicitly_public_interface_member_needs_javadoc(tree, check_javadoc_links):
    write(
        tree / "Listener.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Callbacks. */
@Public
public interface Listener {
    default void onOpen() {}

    /** Called once closed. */
    void onClose();
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public method 'onOpen()' of @Public type 'Listener'" in problems[0]


def test_a_tier_annotated_type_needs_type_level_javadoc(tree, check_javadoc_links):
    write(
        tree / "Probe.java",
        """package demo;

import org.apache.flink.annotation.Experimental;

@Experimental
public class Probe {
    /** Runs the probe. */
    public void run() {}

    /** Creates a probe. */
    public Probe() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "@Experimental class 'Probe' has no Javadoc" in problems[0]
    assert "type-level comment above its annotations" in problems[0]


def test_annotations_between_the_javadoc_and_the_declaration_still_count(
    tree, check_javadoc_links
):
    """Javadoc sits above the annotations, exactly as Javadoc itself reads it.

    Measuring presence from the post-annotation declaration instead would put
    `@Deprecated` inside the gap and report a documented member as bare.
    """
    write(
        tree / "FooSource.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** A source. */
@PublicEvolving
public class FooSource {
    /** Builds a source. */
    @Deprecated
    public FooSource builder() {
        return this;
    }

    /** Creates a source. */
    public FooSource() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.documented == 3


def test_a_config_option_javadoc_differing_from_its_description_fails(
    tree, check_javadoc_links
):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The host to call. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint to call.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert (
        "make the Javadoc of 'ENDPOINT' equal to its withDescription text"
        in problems[0]
    )
    assert "The host to call." in problems[0]
    assert "The endpoint to call." in problems[0]


def test_a_comment_between_field_modifiers_does_not_hide_a_config_option(
    tree, check_javadoc_links
):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The host to call. */
    public /* retained rationale */ static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint to call.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "make the Javadoc of 'ENDPOINT' equal" in problems[0]


def test_a_visibility_word_inside_a_modifier_comment_does_not_hide_a_member(
    tree, check_javadoc_links
):
    write(
        tree / "Sink.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A sink. */
@Public
public final class Sink {
    /** Creates a sink. */
    public Sink() {}

    public /* not private */ void send() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public method 'send()'" in problems[0]


def test_multiple_config_options_in_one_declaration_must_be_split(
    tree, check_javadoc_links
):
    """One shared comment cannot hold two different runtime descriptions."""
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The first option. */
    public static final ConfigOption<String> FIRST =
                    ConfigOptions.key("first")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("The first option."),
            SECOND =
                    ConfigOptions.key("second")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("The second option.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "ConfigOption declaration starting at 'FIRST'" in problems[0]
    assert "split each constant into its own declaration" in problems[0]


def test_the_last_with_description_call_is_the_runtime_description(
    tree, check_javadoc_links
):
    """A later ConfigOption copy replaces the earlier description."""
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The first description. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The first description.")
                    .withDescription("The runtime description.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "The runtime description." in problems[0]


def test_a_type_annotated_config_option_still_has_its_description_checked(
    tree, check_javadoc_links
):
    """A legal type-use annotation does not take the constant off the rule."""
    write(
        tree / "DemoOptions.java",
        """package demo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@Target(ElementType.TYPE_USE)
@interface TypeUse {
    String value();
}

public final class DemoOptions {
    /** The stale description. */
    public static final @TypeUse("checked") ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The runtime description.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "The runtime description." in problems[0]


def test_a_config_option_javadoc_equal_to_its_concatenated_description_passes(
    tree, check_javadoc_links
):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The endpoint to call. Given as host and port. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The endpoint to call."
                                    + " Given as host and port.");
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.options == 1


def test_a_config_option_javadoc_equal_to_its_text_block_description_passes(
    tree, check_javadoc_links
):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The endpoint to call. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(\"\"\"
                            The endpoint to call.
                            \"\"\");
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.options == 1


def test_comments_do_not_hide_a_literal_description(tree, check_javadoc_links):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The endpoint to call. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            /* kept literal */
                            ("The endpoint" /* joined */ + " to call."));
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.options == 1


def test_an_uninitialized_second_config_option_declarator_must_be_split(
    tree, check_javadoc_links
):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The first option. */
    public static final ConfigOption<String> FIRST =
                    ConfigOptions.key("first")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("The first option."),
            SECOND;

    static {
        SECOND =
                ConfigOptions.key("second")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("The second option.");
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "ConfigOption declaration starting at 'FIRST'" in problems[0]
    assert "split each constant into its own declaration" in problems[0]


def test_a_parenthesized_literal_description_is_still_compared(
    tree, check_javadoc_links
):
    """Parentheses do not turn a literal-only expression into a Description object."""
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** The old endpoint contract. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription((("The new endpoint" + (" contract."))));
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "make the Javadoc of 'ENDPOINT' equal" in problems[0]
    assert "The new endpoint contract." in problems[0]


def test_a_content_leading_star_in_a_config_option_javadoc_is_not_margin(
    tree, check_javadoc_links
):
    """Only the comment margin comes off: a `*` the prose owns stays.

    Two shapes, because they fail differently: a wrapped line starting with
    `*.googleapis.com` sits behind a margin star, and a one-line comment
    starting with it has no margin at all. Stripping stars greedily mangles
    either into `.googleapis.com` and reports an equal pair as drifted.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /**
     * Accepts
     * *.googleapis.com endpoints.
     */
    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Accepts *.googleapis.com endpoints.");

    /** *.googleapis.com endpoints are trusted. */
    public static final ConfigOption<String> TRUSTED =
            ConfigOptions.key("trusted")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("*.googleapis.com endpoints are trusted.");
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.options == 2


def test_a_description_containing_a_semicolon_is_read_to_its_end(
    tree, check_javadoc_links
):
    """A `;` inside the description is text, not the statement's end.

    The syntax tree supplies each field boundary. The fixture keeps that
    load-bearing twice over: `OPEN` carries an unbalanced `(` in its literal,
    and the description carries a lone `)` before its `;`. The escaped quotes
    are unescaped the way Java reads them, and the constants below only count
    if each field remains independent.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    private static final String OPEN = "(";

    /** A "strict" mode: ')' ends a range; the rest is rejected. */
    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A \\"strict\\" mode: ')' ends a range; the rest is rejected.");

    /** The endpoint to call. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint to call.");
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # counts.options == 2 is the load-bearing assertion: a wrong field boundary
    # loses a constant without ever reporting a problem.
    assert counts.options == 2


def test_a_config_option_without_a_string_description_is_not_compared(
    tree, check_javadoc_links
):
    """A Description object has no one flat text to hold the Javadoc to.

    The literals inside it are not the description; comparing against them
    would report this documented constant as drifted.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

public final class DemoOptions {
    /** Something the description renders with markup. */
    public static final ConfigOption<String> FANCY =
            ConfigOptions.key("fancy")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(Description.builder().text("Not this.").build());

    /** The endpoint to call. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint to call.");
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # Exactly the literal-described sibling counts: the fixture was indexed,
    # and FANCY was skipped rather than the whole file never seen.
    assert counts.options == 1


def test_an_undocumented_config_option_off_the_surface_is_still_asked_for_javadoc(
    tree, check_javadoc_links
):
    """@Internal takes the presence rule away, not the ConfigOption rule."""
    write(
        tree / "HiddenOptions.java",
        """package demo;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Plumbing options. */
@Internal
public final class HiddenOptions {
    public static final ConfigOption<String> KEY =
            ConfigOptions.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The key.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "ConfigOption 'KEY' has no Javadoc" in problems[0]
    assert "The key." in problems[0]


def test_an_undocumented_config_option_on_the_surface_is_reported_once(
    tree, check_javadoc_links
):
    """One member, one message: the presence rule owns the missing comment."""
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** The options. */
@PublicEvolving
public final class DemoOptions {
    public static final ConfigOption<String> KEY =
            ConfigOptions.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The key.");

    /** Creates options. */
    public DemoOptions() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public field 'KEY' of @PublicEvolving type 'DemoOptions'" in problems[0]
    assert 'add one equal to its withDescription text "The key."' in problems[0]


def test_an_empty_javadoc_does_not_count_as_present(tree, check_javadoc_links):
    """A comment with nothing in it is not documentation.

    Two empty shapes, because they fail differently: `/** */` has a blank raw
    body, while the conventional multiline one carries a `*` margin line and
    nothing else, so emptiness has to be judged on the rendered text — on the
    type-level comment as much as on a member's. The message differs from the
    missing-comment one only in what the repair asks for: the comment exists
    and is empty.
    """
    write(
        tree / "BarSource.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/**
 *
 */
@PublicEvolving
public class BarSource {
    /** Builds a source. */
    public BarSource builder() {
        return this;
    }

    /** Creates a source. */
    public BarSource() {}
}
""",
    )
    write(
        tree / "FooSource.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** A source. */
@PublicEvolving
public class FooSource {
    /** */
    public FooSource builder() {
        return this;
    }

    /**
     *
     */
    public FooSource other() {
        return this;
    }

    /** Creates a source. */
    public FooSource() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 3
    assert (
        "@PublicEvolving class 'BarSource' has an empty Javadoc; write" in problems[0]
    )
    assert (
        "public method 'builder()' of @PublicEvolving type 'FooSource'" in problems[1]
    )
    assert "has an empty Javadoc; write one" in problems[1]
    assert "public method 'other()' of @PublicEvolving type 'FooSource'" in problems[2]
    assert "has an empty Javadoc; write one" in problems[2]


def test_an_annotation_type_is_on_the_surface_like_any_other(tree, check_javadoc_links):
    """`@interface` opens a declaration, not an annotation use.

    The discriminating shape is a nested annotation type with no modifier —
    implicitly public in an interface — where nothing shields `@interface`
    from the annotation walk: read as an annotation use, the whole type
    vanishes from the index and its undocumented element with it. A top-level
    `public @interface` never shows this, because the modifier stops the walk
    first.
    """
    write(
        tree / "Listener.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Callbacks. */
@Public
public interface Listener {
    @interface Handler {
        String value();
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 2
    assert "@Public @interface 'Handler' has no Javadoc" in problems[0]
    assert "public method 'value()' of @Public type 'Handler'" in problems[1]


def test_malformed_java_fails_before_inventorying(tree, check_javadoc_links):
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** A raw \\uZZZZ sequence. */
    public static final ConfigOption<String> RAW =
            ConfigOptions.key("raw")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A raw \\uZZZZ sequence.");
}
""",
    )

    with pytest.raises(check_javadoc_links.JavaSyntaxError):
        check_javadoc_links.check()


def test_a_generic_comma_does_not_mint_a_phantom_member(tree, check_javadoc_links):
    """The comma in `ConfigOption<Map<String, String>>` separates type args.

    A comma-naive declarator split would record a second public member named
    'String' sharing the constant's Javadoc position — documented by accident,
    compared by accident, and counted twice.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** The options. */
@PublicEvolving
public final class DemoOptions {
    /** The headers to send. */
    public static final ConfigOption<Map<String, String>> HEADERS =
            ConfigOptions.key("headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("The headers to send.");

    /** Creates options. */
    public DemoOptions() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # The type, constructor, and exactly one option field were counted, and
    # exactly one option compared: a phantom 'String' member raises both counts.
    assert counts.documented == 3
    assert counts.options == 1


def test_a_difference_past_the_clip_is_still_shown(tree, check_javadoc_links):
    """The mismatch message windows around the first differing character.

    Clipping both strings from the head would show two identical-looking
    prefixes here, telling the reader nothing about what to change.
    """
    prefix = (
        "The endpoint to call for every request the sink issues, given as a"
        " host and port pair without a scheme, resolved once at startup."
    )
    write(
        tree / "DemoOptions.java",
        f"""package demo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {{
    /** {prefix} Cached forever. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("{prefix} Re-resolved on failure.");
}}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "Cached forever." in problems[0]
    assert "Re-resolved on failure." in problems[0]


def test_the_first_enum_constants_own_annotation_does_not_hide_its_javadoc(
    tree, check_javadoc_links
):
    """The first constant's syntax node includes its annotations.

    Its Javadoc position is therefore the declaration's own; measuring from
    the constant list instead would put `@Deprecated` inside the gap and
    report a documented constant as bare.
    """
    write(
        tree / "Mode.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** The modes. */
@PublicEvolving
public enum Mode {
    /** The old spelling. */
    @Deprecated
    LEGACY("l"),
    /** The current spelling. */
    CURRENT("c");

    private final String value;

    Mode(String value) {
        this.value = value;
    }
}
""",
    )

    assert audit(check_javadoc_links) == []


def test_a_protected_member_is_on_the_surface(tree, check_javadoc_links):
    write(
        tree / "Base.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A base type. */
@Public
public abstract class Base {
    protected abstract void flush();

    /** Creates a base instance. */
    protected Base() {}
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "protected method 'flush()' of @Public type 'Base'" in problems[0]


def test_a_type_behind_a_package_private_enclosure_is_not_on_the_surface(
    tree, check_javadoc_links
):
    """A tier reaches a nested type only along a chain the reader can see.

    `Inner` inherits @Public through `Plumbing`, but the generated reference
    never shows a package-private type or anything inside it.
    """
    write(
        tree / "Outer.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** Entry point. */
@Public
public class Outer {
    static final class Plumbing {
        public static final class Inner {
            public void run() {}
        }
    }

    /** Creates an outer instance. */
    public Outer() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.documented == 2


def test_a_body_opening_enum_constant_is_still_a_constant(tree, check_javadoc_links):
    """A strategy-pattern constant opens a class body and stays a constant.

    Treating the body's `{` as the list's end would drop every constant, and an
    undocumented one would pass silently — ADR-0143 says every enum constant.
    """
    write(
        tree / "Rounding.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** How values round. */
@Public
public enum Rounding {
    /** Rounds up. */
    UP("u") {
        @Override
        int apply(int value) {
            return value + 1;
        }
    },
    DOWN("d") {
        @Override
        int apply(int value) {
            return value - 1;
        }
    };

    private final String label;

    Rounding(String label) {
        this.label = label;
    }

    abstract int apply(int value);
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "enum constant 'DOWN' of @Public type 'Rounding'" in problems[0]


def test_a_constant_body_does_not_leak_members_onto_the_enum(tree, check_javadoc_links):
    """What a constant's body declares belongs to its anonymous class.

    The public method and the nested type inside the body are not the enum's
    members, so a documented constant list passes with nothing else counted or
    reported.
    """
    write(
        tree / "Strategy.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;

/** The strategies. */
@PublicEvolving
public enum Strategy {
    /** The only strategy. */
    DIRECT {
        /** Helper of the body, not of the enum. */
        class Helper {
            public void run() {}
        }

        public int extra() {
            return 1;
        }
    };
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # The enum and its one constant, nothing from inside the body.
    assert counts.documented == 2


def test_a_container_of_options_is_not_compared_by_family_2(tree, check_javadoc_links):
    """A `List<ConfigOption<?>>` field is not itself a ConfigOption.

    The withDescription in its initializer belongs to the nested option;
    comparing the field's own Javadoc against it would fail CI on a correct
    source. The field is still on the presence surface like any public field.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/** The options. */
@PublicEvolving
public final class DemoOptions {
    /** Every option this connector takes. */
    public static final List<ConfigOption<?>> ALL =
            List.of(
                    ConfigOptions.key("endpoint")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("The endpoint to call."));

    /** Creates options. */
    public DemoOptions() {}
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    # Family 1 counted the type, constructor, and field; family 2 compared nothing.
    assert counts.documented == 3
    assert counts.options == 0


def test_a_compact_record_constructor_is_a_constructor(tree, check_javadoc_links):
    """`public Endpoint {` inside a record is the canonical constructor.

    No main source declares a record today, but ADR-0143 says every public
    constructor, and the compact shape — no parameter list, header ending at
    the brace — must not slip past the presence rule the way an initializer
    block does.
    """
    write(
        tree / "Endpoint.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** An endpoint. */
@Public
public record Endpoint(String host, int port) {
    public Endpoint {
        host = host.trim();
    }
}
""",
    )
    write(
        tree / "Range.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A range. */
@Public
public record Range(int low, int high) {
    /** Swaps the bounds when reversed. */
    public Range {
        low = Math.min(low, high);
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "public constructor 'Endpoint()' of @Public type 'Endpoint'" in problems[0]


def test_an_implicit_constructor_must_be_declared_and_documented(
    tree, check_javadoc_links
):
    """A public class or record otherwise exposes an undocumented constructor."""
    write(
        tree / "Factories.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A factory. */
@Public
public class Factories {}
""",
    )
    write(
        tree / "Endpoint.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** An endpoint. */
@Public
public record Endpoint(String host) {}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 2
    assert all("exposes an implicit constructor" in problem for problem in problems)
    assert "class 'Factories'" in problems[1]
    assert "record 'Endpoint'" in problems[0]


def test_an_auxiliary_record_constructor_does_not_document_the_canonical_one(
    tree, check_javadoc_links
):
    """A same-arity overload leaves the record's canonical constructor implicit."""
    write(
        tree / "Range.java",
        """package demo;

import org.apache.flink.annotation.Public;

/** A range. */
@Public
public record Range(int low, int high) {
    /** Creates a range from wider inputs. */
    public Range(long low, long high) {
        this((int) low, (int) high);
    }
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert "record 'Range' exposes an implicit constructor" in problems[0]


def test_a_fully_qualified_config_option_is_still_compared(tree, check_javadoc_links):
    """The fully qualified spelling declares the same type.

    `org.apache.flink.configuration.ConfigOption<String>` is legal Java for
    the same constant; letting it bypass the equality rule would make the
    import style decide whether the copy is held.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

public final class DemoOptions {
    /** The host to call. */
    public static final org.apache.flink.configuration.ConfigOption<String> ENDPOINT =
            org.apache.flink.configuration.ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint to call.");
}
""",
    )

    problems = audit(check_javadoc_links)

    assert len(problems) == 1
    assert (
        "make the Javadoc of 'ENDPOINT' equal to its withDescription text"
        in problems[0]
    )


def test_a_type_merely_containing_the_name_is_not_an_option(tree, check_javadoc_links):
    """`MyConfigOptionHolder` contains the name and is not the type.

    Its initializer's withDescription belongs to whatever it wraps; comparing
    the holder's Javadoc against it would be a false failure.
    """
    write(
        tree / "DemoOptions.java",
        """package demo;

import org.apache.flink.configuration.ConfigOptions;

public final class DemoOptions {
    /** A holder, not an option. */
    public static final MyConfigOptionHolder HOLDER =
            MyConfigOptionHolder.of(
                    ConfigOptions.key("endpoint")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("Not compared."));
}
""",
    )

    counts, problems = check_javadoc_links.check()

    assert problems == []
    assert counts.options == 0
