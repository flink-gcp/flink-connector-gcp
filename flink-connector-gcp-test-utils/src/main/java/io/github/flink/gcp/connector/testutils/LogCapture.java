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

import org.apache.flink.annotation.Internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures what a class logs, so a test can assert the line rather than only its side effects.
 *
 * <p>Deliberately narrow (#323). It is for the call sites where the log <em>is</em> the report: a
 * handler whose whole behaviour is to log and drop, a failure absorbed during a shutdown the
 * mailbox can no longer observe, a quota warning the job builds straight through, an abandoned
 * teardown whose counter says one happened but not which client. Where a test already asserts a
 * counter, a returned value or an absorbed exception that identifies the event, the log is a second
 * report of something already covered — and asserting it there buys little while coupling the test
 * to the message's wording, which is the cost that keeps this class from spreading.
 *
 * <pre>{@code
 * try (LogCapture capture = LogCapture.of(FailureHandlers.LogAndDrop.class)) {
 *     FailureHandler.logAndDrop().handle(element);
 *     assertThat(capture.getMessages()).singleElement().satisfies(m -> ...);
 * }
 * }</pre>
 *
 * <p>The backend is log4j2, which is what the test classpath actually carries — {@code
 * log4j-slf4j-impl} reaches every module transitively through {@code flink-test-utils}. Logback is
 * absent, so the {@code ListAppender} shape does not apply, and log4j's own {@code ListAppender}
 * ships only in a {@code log4j-core} test-jar this build does not resolve. No log4j2 type appears
 * in this class's signature, so replacing the backend is a change to this file alone.
 *
 * <p>Everything below exists to rule out one failure: a capture that collects nothing while looking
 * exactly like a log that was never emitted.
 *
 * <ul>
 *   <li><b>The logger name is derived as slf4j derives it</b>, {@code Class#getName}, because the
 *       code under test logs through slf4j. {@code LogManager.getLogger(Class)} does <em>not</em>
 *       agree: it prefers {@code Class#getCanonicalName}, so for a nested type such as {@code
 *       FailureHandlers.LogAndDrop} the two names differ by {@code $} versus {@code .}. They are
 *       not even in an ancestor relationship, since log4j2 splits a name on {@code .} alone — so
 *       taking log4j2's name would attach to a logger nothing ever writes to.
 *   <li><b>The level is forced on the {@code LoggerConfig}, not on the {@code Logger}.</b> Every
 *       module ships a {@code log4j2-test.properties} at {@code rootLogger.level = WARN}, so a WARN
 *       capture needs no help — but a capture below that does, and a module added without one falls
 *       back to log4j2's root {@code ERROR}, where even a WARN would be collected nowhere. {@code
 *       Logger#setLevel} would widen it, but it writes to the logger's cached config, which any
 *       later {@code updateLoggers()} — including one triggered by a second capture opening on an
 *       unrelated logger — silently discards.
 *   <li><b>The level is only ever widened, never narrowed</b>, and is restored to the explicit
 *       level found, which may be none. Saving the <em>effective</em> level and writing it back
 *       would turn an inherited level into an explicit one and outlive the capture.
 * </ul>
 *
 * <p>Restoration matters beyond tidiness: surefire reuses forks, so a {@code LoggerConfig} or a
 * widened level left behind reaches every later test class in that fork (the shape of #316).
 *
 * <p>No level guard ({@code if (LOG.isDebugEnabled())}) exists in this repository's main sources,
 * so widening cannot change what the code under test does. A guard added later would make that
 * untrue for a capture taken below the ambient level.
 *
 * <p>A test that asserts a log was <em>not</em> emitted should sit beside one that asserts a log
 * <em>was</em>, on the same logger: an empty capture is the expected result of both a working
 * capture and a broken one, and only the positive case can tell them apart.
 */
@Internal
public final class LogCapture implements AutoCloseable {

    /** Appender names must be distinct: {@code removeAppender} removes every control by name. */
    private static final AtomicLong NAMES = new AtomicLong();

    private final LoggerContext context;
    private final Configuration configuration;
    private final String loggerName;
    private final LoggerConfig owned;
    private final CollectingAppender appender;

    /** True when this capture created {@link #owned} and so must remove it again. */
    private final boolean created;

    /** The explicit level found on a pre-existing config, or null for none. Unused if created. */
    private final org.apache.logging.log4j.Level previousExplicitLevel;

    private boolean closed;

    /**
     * Captures {@code WARN} and above from the given class's logger, attaching immediately.
     *
     * @param owner the class whose logger to capture, as it was passed to {@code getLogger}
     */
    public static LogCapture of(Class<?> owner) {
        return of(owner, Level.WARN);
    }

    /**
     * Captures the given level and above from the given class's logger, attaching immediately.
     *
     * @param owner the class whose logger to capture, as it was passed to {@code getLogger}
     * @param level the least specific level to collect
     */
    public static LogCapture of(Class<?> owner, Level level) {
        return new LogCapture(owner.getName(), level);
    }

    private LogCapture(String loggerName, Level level) {
        this.loggerName = loggerName;
        org.apache.logging.log4j.Level threshold =
                org.apache.logging.log4j.Level.valueOf(level.name());

        org.apache.logging.log4j.spi.LoggerContext current = LogManager.getContext(false);
        if (!(current instanceof LoggerContext)) {
            throw new IllegalStateException(
                    "LogCapture needs log4j-core behind slf4j, but the context is a "
                            + current.getClass().getName()
                            + ". Nothing would be captured, which reads like a log that was never"
                            + " emitted.");
        }
        this.context = (LoggerContext) current;
        this.configuration = context.getConfiguration();
        this.appender = new CollectingAppender("LogCapture-" + NAMES.incrementAndGet(), threshold);
        this.appender.start();

        // getLoggerConfig falls back to the nearest ancestor, so an unequal name means this logger
        // has no config of its own and one has to be created for the appender to hang on.
        LoggerConfig existing = configuration.getLoggerConfig(loggerName);
        if (existing.getName().equals(loggerName)) {
            this.owned = existing;
            this.created = false;
            this.previousExplicitLevel = existing.getExplicitLevel();
            if (existing.getLevel().isMoreSpecificThan(threshold)) {
                existing.setLevel(threshold);
            }
        } else {
            // The more verbose of what this logger would have inherited and what the capture
            // needs. Creating it at the threshold alone would *narrow* a logger whose ambient
            // configuration is more verbose, silencing console output the capture was never asked
            // to touch - and it would leave the appender's own threshold check unreachable.
            org.apache.logging.log4j.Level inherited = existing.getLevel();
            // Additive, so the line still reaches the console appender a human reads.
            this.owned =
                    new LoggerConfig(
                            loggerName,
                            inherited.isMoreSpecificThan(threshold) ? threshold : inherited,
                            true);
            this.previousExplicitLevel = null;
            this.created = true;
            configuration.addLogger(loggerName, owned);
        }
        owned.addAppender(appender, null, null);

        // Loggers cache the config they resolved to, and the class under test holds a static one
        // that a reused fork has almost certainly resolved already.
        context.updateLoggers();
    }

    /** The formatted messages collected so far, in the order they were logged. */
    public List<String> getMessages() {
        List<String> messages = new ArrayList<>();
        for (Event event : appender.events) {
            messages.add(event.getMessage());
        }
        return List.copyOf(messages);
    }

    /**
     * The events collected so far, in the order they were logged.
     *
     * <p>For the sites that log a cause — an absorbed shutdown report, an abandoned stream — where
     * the attached throwable is part of what the line reports.
     */
    public List<Event> getEvents() {
        return List.copyOf(appender.events);
    }

    /** Detaches the capture and restores what it changed. Idempotent. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            owned.removeAppender(appender.getName());
            if (created) {
                configuration.removeLogger(loggerName);
            } else {
                owned.setLevel(previousExplicitLevel);
            }
        } finally {
            context.updateLoggers();
            appender.stop();
        }
    }

    /** The levels a capture can be taken at, so no log4j2 type reaches this class's signature. */
    public enum Level {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    /**
     * One collected log event, flattened out of log4j2's own type.
     *
     * <p>The level is carried for {@link #toString()} — which is what an AssertJ failure prints —
     * and is deliberately <b>not</b> exposed as a typed accessor. Doing so would mean mapping
     * log4j2's levels onto a fixed set, and they are an open set: {@code FATAL}, plus whatever
     * {@code Level.forName(name, intValue)} registers, at any severity. A mapping that missed one
     * would throw inside {@code append}, which {@code AbstractAppender} is built to ignore, so the
     * event would vanish — the one outcome this class exists to rule out. Holding the name verbatim
     * cannot fail, and the capture's own threshold already answers the only question a caller has
     * asked so far, which is which levels are collected at all.
     */
    public static final class Event {

        private final String level;
        private final String message;
        private final Throwable throwable;

        private Event(String level, String message, Throwable throwable) {
            this.level = level;
            this.message = message;
            this.throwable = throwable;
        }

        public String getMessage() {
            return message;
        }

        /** The throwable the statement passed, or null if it passed none. */
        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public String toString() {
            return level + " " + message + (throwable == null ? "" : " (" + throwable + ")");
        }
    }

    /**
     * Collects events at or above the capture's level.
     *
     * <p>The threshold is applied here as well as through the logger's level, so a capture sees the
     * same events in a module whose configuration already sets a more verbose level as it does in
     * one with no configuration at all.
     *
     * <p>The list is concurrent because the logging thread is not always the test's, and every
     * field is read off the event before this method returns: log4j2 reuses the {@code LogEvent}
     * instance by default outside a web application.
     */
    private static final class CollectingAppender extends AbstractAppender {

        private final List<Event> events = new CopyOnWriteArrayList<>();
        private final org.apache.logging.log4j.Level threshold;

        CollectingAppender(String name, org.apache.logging.log4j.Level threshold) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
            this.threshold = threshold;
        }

        @Override
        public void append(LogEvent event) {
            if (!event.getLevel().isMoreSpecificThan(threshold)) {
                return;
            }
            events.add(
                    new Event(
                            event.getLevel().name(),
                            event.getMessage().getFormattedMessage(),
                            event.getThrown()));
        }
    }
}
