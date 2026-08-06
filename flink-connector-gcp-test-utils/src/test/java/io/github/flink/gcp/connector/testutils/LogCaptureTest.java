/*
 * Copyright 2026 laughingman7743
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.testutils;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LogCapture}.
 *
 * <p>The failure this class is aimed at is the capture quietly seeing <em>less</em> than it should,
 * which reads exactly like a passing test — so each rule the helper rests on has a case that fails
 * when that rule is removed, rather than a case that merely exercises it.
 *
 * <p>Two properties of this module make the cases below discriminating, and both are asserted
 * rather than assumed, so a change to either fails here instead of silently disarming the suite:
 *
 * <ul>
 *   <li>{@code DEBUG} is not enabled ambiently, so a {@code DEBUG} capture that works proves the
 *       helper widened the level. Picking {@code WARN} instead would prove nothing — every module's
 *       {@code log4j2-test.properties} sets {@code rootLogger.level = WARN}, so a WARN passes with
 *       no widening at all.
 *   <li>{@code INFO} <em>is</em> enabled ambiently, so a {@code WARN} capture that excludes an
 *       {@code INFO} proves the appender-side threshold filter is doing it.
 * </ul>
 *
 * <p>The fixture logger belongs to a nested class on purpose: its slf4j name ends {@code
 * LogCaptureTest$Fixture} while log4j2 would derive {@code LogCaptureTest.Fixture} from the same
 * {@code Class}, so every case here also pins the name derivation.
 */
class LogCaptureTest {

    /** Owns a logger whose slf4j and log4j2 names differ. */
    private static final class Fixture {

        private static final Logger LOG = LoggerFactory.getLogger(Fixture.class);

        private Fixture() {}
    }

    @Test
    void theAmbientConfigurationIsWhatTheseTestsAssume() {
        Logger logger = LoggerFactory.getLogger(Fixture.class);

        // Asserted, not assumed: -Dorg.apache.logging.log4j.level or a changed
        // log4j2-test.properties would otherwise leave the tests below passing for no reason.
        assertThat(logger.isDebugEnabled()).isFalse();
        assertThat(logger.isInfoEnabled()).isTrue();
    }

    @Test
    void theSlf4jAndLog4j2NamesForANestedClassDiffer() {
        // The divergence the helper's name derivation exists for. Pinned so that a log4j2 or slf4j
        // change unifying them shows up as a visible failure rather than as a silent coincidence.
        assertThat(LoggerFactory.getLogger(Fixture.class).getName())
                .isEqualTo(Fixture.class.getName())
                .isNotEqualTo(LogManager.getLogger(Fixture.class).getName());
    }

    @Test
    void capturesAWarning() {
        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Fixture.LOG.warn("a warning naming {}", "something");

            assertThat(capture.getMessages()).containsExactly("a warning naming something");
        }
    }

    @Test
    void capturesFromALoggerResolvedBeforeTheCaptureOpened() {
        // Kills the mutant that drops updateLoggers(). A Logger caches the LoggerConfig it
        // resolved to, and the classes this helper is aimed at hold a static one that a reused
        // fork resolved long before any capture opened - so the resolution is forced here rather
        // than left to the order JUnit happens to run these methods in.
        Logger resolvedFirst = LoggerFactory.getLogger(Fixture.class);
        resolvedFirst.warn("resolving the logger");

        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            resolvedFirst.warn("after the capture opened");

            assertThat(capture.getMessages()).containsExactly("after the capture opened");
        }
    }

    @Test
    void capturesTheAttachedThrowable() {
        RuntimeException cause = new RuntimeException("the cause");

        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Fixture.LOG.warn("failed to do {}", "the thing", cause);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).isEqualTo("failed to do the thing");
                                assertThat(event.getThrowable()).isSameAs(cause);
                            });
        }
    }

    @Test
    void capturesBelowTheAmbientLevelByWideningIt() {
        // Kills the mutant that drops the widening, and the one that widens the Logger rather than
        // the LoggerConfig: DEBUG is ambiently disabled, so an unwidened capture sees nothing.
        try (LogCapture capture = LogCapture.of(Fixture.class, LogCapture.Level.DEBUG)) {
            assertThat(LoggerFactory.getLogger(Fixture.class).isDebugEnabled()).isTrue();
            Fixture.LOG.debug("a debug line");

            assertThat(capture.getMessages()).containsExactly("a debug line");
        }
    }

    @Test
    void theWidenedLevelIsRestoredOnClose() {
        try (LogCapture capture = LogCapture.of(Fixture.class, LogCapture.Level.DEBUG)) {
            assertThat(capture.getMessages()).isEmpty();
        }

        // Kills the mutant that leaves the widened level behind. A leak here is the #316 shape:
        // every later class in this reused fork would log at DEBUG.
        assertThat(LoggerFactory.getLogger(Fixture.class).isDebugEnabled()).isFalse();
    }

    @Test
    void nothingIsCapturedAfterClose() {
        LogCapture capture = LogCapture.of(Fixture.class);
        Fixture.LOG.warn("before");
        capture.close();

        Fixture.LOG.warn("after");

        // Kills the mutant that drops removeAppender, and the one that leaves the created
        // LoggerConfig in the configuration with the appender still on it.
        assertThat(capture.getMessages()).containsExactly("before");
    }

    @Test
    void aWarnCaptureIgnoresAnInfoOnTheSameLogger() {
        // Kills the mutant that drops the appender-side threshold filter. INFO is ambiently
        // enabled here, so it reaches the appender and only the filter can exclude it.
        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Fixture.LOG.info("an info line");
            Fixture.LOG.warn("a warning");

            assertThat(capture.getMessages()).containsExactly("a warning");
        }
    }

    @Test
    void anErrorIsCapturedByAWarnCapture() {
        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Fixture.LOG.error("an error");

            // "WARN and above", so a capture taken at WARN must not miss an ERROR.
            assertThat(capture.getMessages()).containsExactly("an error");
        }
    }

    @Test
    void aLevelOutsideTheUsualFiveIsCollectedRatherThanDropped() {
        // log4j2's levels are an open set: Level.forName registers one at any severity. Anything
        // that mapped them onto a fixed set would throw here, and AbstractAppender is built with
        // ignoreExceptions, so the throw would be swallowed and the event would simply vanish -
        // the failure this class exists to rule out. 250 sits between ERROR (200) and WARN (300),
        // so a WARN capture must collect it.
        org.apache.logging.log4j.Level custom =
                org.apache.logging.log4j.Level.forName("LOGCAPTURE_TEST_NOTICE", 250);

        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            LogManager.getLogger(Fixture.class.getName()).log(custom, "a custom level");

            assertThat(capture.getMessages()).containsExactly("a custom level");
        }
    }

    @Test
    void closingTheInnerCaptureLeavesTheOuterOneAttached() {
        // Kills the mutant that hardcodes one appender name: LoggerConfig.removeAppender(name)
        // removes every control carrying that name, so the inner close would detach both.
        // Also the only case that drives the branch for a LoggerConfig that already exists.
        try (LogCapture outer = LogCapture.of(Fixture.class)) {
            LogCapture inner = LogCapture.of(Fixture.class);
            try {
                Fixture.LOG.warn("seen by both");
                assertThat(inner.getMessages()).containsExactly("seen by both");
            } finally {
                inner.close();
            }

            Fixture.LOG.warn("seen by the outer one only");

            assertThat(outer.getMessages())
                    .containsExactly("seen by both", "seen by the outer one only");
            // Kills the mutant that drops removeAppender. On this branch close() restores a level
            // rather than removing the LoggerConfig, so a skipped detach leaves the closed
            // capture's appender collecting - which only the closed capture can reveal.
            assertThat(inner.getMessages()).containsExactly("seen by both");
        }
    }

    @Test
    void eachEventKeepsItsOwnMessage() {
        // log4j2 reuses the LogEvent instance by default outside a web application, so an
        // implementation holding the event rather than reading it out reports the last message
        // twice.
        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Fixture.LOG.warn("first");
            Fixture.LOG.warn("second");

            assertThat(capture.getMessages()).containsExactly("first", "second");
        }
    }

    @Test
    void capturesFromABackgroundThread() throws Exception {
        // BoundedShutdown emits its warnings from the thread it hands the wait to, so the
        // collection has to be safe to publish across one. join() makes the handover explicit -
        // a plain ArrayList would usually survive this, so treat it as pinning the requirement
        // rather than as a reliable killer of a non-concurrent list.
        try (LogCapture capture = LogCapture.of(Fixture.class)) {
            Thread thread = new Thread(() -> Fixture.LOG.warn("from a background thread"));
            thread.start();
            thread.join();

            assertThat(capture.getMessages()).containsExactly("from a background thread");
        }
    }

    @Test
    void closingTwiceIsHarmless() {
        LogCapture capture = LogCapture.of(Fixture.class, LogCapture.Level.DEBUG);
        capture.close();
        capture.close();

        assertThat(LoggerFactory.getLogger(Fixture.class).isDebugEnabled()).isFalse();
        Fixture.LOG.warn("after both closes");
        assertThat(capture.getMessages()).isEmpty();
    }
}
