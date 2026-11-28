/*
 * Copyright 2026 The flink-gcp authors
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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests how {@link SpannerChangeStreamSplitEnumerator} reports an initialization failure when its
 * close races initialization's outcome (#2358).
 *
 * <p>Flink runs the handler of {@code callAsync} on the coordinator's executor, closes the
 * enumerator on a closer thread, and then shuts that executor down with {@code shutdownNow()}
 * (Flink 1.20.5 and 2.2.1, {@code SourceCoordinator.close()} and {@code
 * SourceCoordinatorContext.close()}): the handler runs on a closed enumerator, or never.
 */
class SpannerChangeStreamSplitEnumeratorInitializationFailureTest {

    /** Where initialization fails while the enumerator is still open. */
    private enum OpenFailure {
        CREATION,
        INITIALIZATION,
        START_POSITION,
        RELEASE
    }

    /**
     * Builds an enumerator whose initialization fails at {@code where}: with {@code failure},
     * except at {@code START_POSITION}, where a "latest" start resolves after the bound.
     */
    private static SpannerChangeStreamSplitEnumerator failingEnumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            OpenFailure where,
            RuntimeException failure) {
        ScriptedClient client = new ScriptedClient();
        switch (where) {
            case CREATION:
                return enumerator(
                        context,
                        clientMetrics -> {
                            throw failure;
                        });
            case INITIALIZATION:
                client.initializationFailure = failure;
                return enumerator(context, client);
            case START_POSITION:
                return enumerator(context, clientMetrics -> client, Instant.EPOCH);
            case RELEASE:
                client.duringClose =
                        () -> {
                            throw failure;
                        };
                return enumerator(context, client);
            default:
                throw new IllegalArgumentException(where.name());
        }
    }

    /** Asserts that {@code actual} is the failure {@link #failingEnumerator} scripted. */
    private static void assertIsTheFailure(
            Throwable actual, OpenFailure where, RuntimeException failure) {
        if (where == OpenFailure.START_POSITION) {
            assertThat(actual)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("after the bounded timestamp");
        } else {
            assertThat(actual).isSameAs(failure);
        }
    }

    /** Closes the enumerator from a client callback, as Flink's closer thread can meanwhile. */
    private static Runnable closing(SpannerChangeStreamSplitEnumerator[] enumerator) {
        return () -> {
            try {
                enumerator[0].close();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
    }

    @ParameterizedTest
    @EnumSource(OpenFailure.class)
    void anInitializationFailureTheEnumeratorClosedBeforeReportingIsLoggedByItsClose(
            OpenFailure where) throws Exception {
        // Before #2358 the failure left no trace: Flink runs initialization's handler on a closed
        // enumerator, which drops its outcome, or never runs it once it has shut the coordinator's
        // executor down.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        SpannerChangeStreamSplitEnumerator enumerator = failingEnumerator(context, where, failure);
        enumerator.start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            List<Runnable> handlerCalls = context.runAsyncCallsDeferringHandlers();
            assertThat(handlerCalls).as("initialization's handler call").hasSize(1);
            assertThat(capture.getEvents()).as("logged before the close").isEmpty();

            enumerator.close();
            assertThat(capture.getEvents())
                    .as("the failure is logged by the close, without the handler")
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(event -> assertIsTheFailure(event.getThrowable(), where, failure));

            handlerCalls.forEach(Runnable::run);
            enumerator.close();
            assertThat(capture.getEvents())
                    .as("a handler that runs after the close, or a second close, logs nothing more")
                    .hasSize(1);
        }
    }

    @ParameterizedTest
    @EnumSource(OpenFailure.class)
    void anInitializationFailureTheOpenEnumeratorReportedFailsInitializationAndIsNotLogged(
            OpenFailure where) throws Exception {
        // A client close that fails while the enumerator is open (RELEASE) fails initialization
        // too, rather than being logged.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        SpannerChangeStreamSplitEnumerator enumerator = failingEnumerator(context, where, failure);
        enumerator.start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            assertThat(catchThrowable(context::runAsyncCalls))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .satisfies(thrown -> assertIsTheFailure(thrown.getCause(), where, failure));
            enumerator.close();
            assertThat(capture.getEvents()).as("events after the close").isEmpty();
        }
    }

    @Test
    void theFirstFailureIsTheOneLoggedAndCarriesALaterReleaseFailure() throws Exception {
        // The client's initialize() fails and then its release fails too, while the enumerator is
        // open: the close logs the first, which carries the second.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        Exception releaseFailure = new IllegalStateException("release failed");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    throw releaseFailure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            context.runAsyncCallsDeferringHandlers();
            enumerator.close();
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(
                            event -> {
                                assertThat(event.getThrowable()).isSameAs(failure);
                                assertThat(failure.getSuppressed()).containsExactly(releaseFailure);
                            });
        }
    }

    @Test
    void aFailedCallOfTheClientTheEnumeratorsCloseTookOverIsLoggedAtInfo() throws Exception {
        // The close took the installed client over and closed it: the call may have failed because
        // of that close, or for a reason of its own the close overtook, so it is logged without
        // being called an initialization failure.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException callFailure = new IllegalStateException("the client was closed");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = callFailure;
        SpannerChangeStreamSplitEnumerator[] enumerator = {enumerator(context, client)};
        client.duringInitialize = closing(enumerator);
        enumerator[0].start();

        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            assertThat(catchThrowable(context::runAsyncCalls)).isNull();
            enumerator[0].close();
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.toString())
                                            .as("the event's level")
                                            .startsWith("INFO "))
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(callFailure));
        }
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aStartPositionFailureAfterTheEnumeratorsCloseTookTheClientOverIsLogged() throws Exception {
        // The client answered before the close took it over, so resolving the start position
        // fails on its own; the closed enumerator's handler drops that failure.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator[] enumerator = {
            enumerator(context, clientMetrics -> client, Instant.EPOCH)
        };
        client.duringInitialize = closing(enumerator);
        enumerator[0].start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            assertThat(catchThrowable(context::runAsyncCalls)).isNull();
            assertThat(capture.getEvents())
                    .as("the failure is logged where it is observed")
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(
                            event ->
                                    assertIsTheFailure(
                                            event.getThrowable(),
                                            OpenFailure.START_POSITION,
                                            null));
        }
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aFatalReleaseFailureTakesTheFirstFailuresPlaceInTheLog() throws Exception {
        // As Closers.closeAllSuppressing has it, a JVM-fatal release failure is what initialization
        // throws, carrying the first; the close must log that one, not the first without it.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        OutOfMemoryError releaseFailure = new OutOfMemoryError("release failed");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    throw releaseFailure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            context.runAsyncCallsDeferringHandlers();
            enumerator.close();
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(
                            event -> {
                                assertThat(event.getThrowable()).isSameAs(releaseFailure);
                                assertThat(releaseFailure.getSuppressed()).containsExactly(failure);
                            });
        }
    }

    @Test
    void anOpenEnumeratorFailsInitializationWithAFatalReleaseFailure() {
        // The record and what initialization throws must agree: the fatal release failure, carrying
        // the first.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        OutOfMemoryError releaseFailure = new OutOfMemoryError("release failed");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    throw releaseFailure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        assertThat(catchThrowable(context::runAsyncCalls))
                .isInstanceOf(FlinkRuntimeException.class)
                .cause()
                .isSameAs(releaseFailure);
        assertThat(releaseFailure.getSuppressed()).containsExactly(failure);
    }

    @Test
    void aReleaseThatFailsWithInitializationsOwnFailureIsLoggedOnce() throws Exception {
        // A client may throw one instance from both its call and its close; it cannot be
        // suppressed onto itself, and the attempt would replace the failure.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("the client is broken");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    throw failure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            context.runAsyncCallsDeferringHandlers();
            enumerator.close();
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(failure));
        }
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void anOpenEnumeratorFailsInitializationWithTheFailureAReleaseRethrows() {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("the client is broken");
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    throw failure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        assertThat(catchThrowable(context::runAsyncCalls))
                .isInstanceOf(FlinkRuntimeException.class)
                .cause()
                .as("not an IllegalArgumentException from suppressing it onto itself")
                .isSameAs(failure);
    }

    @Test
    void aCloseDuringTheReleaseLogsTheFailureOnlyOnceTheReleaseHasAddedItsOwn() throws Exception {
        // The close takes the recorded failure while the release after it is still closing the
        // client; it waits for that release, so the release's failure is attached when it logs.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        Exception releaseFailure = new IllegalStateException("release failed");
        CountDownLatch releaseEntered = new CountDownLatch(1);
        CountDownLatch releaseGate = new CountDownLatch(1);
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure = failure;
        client.duringClose =
                () -> {
                    releaseEntered.countDown();
                    releaseGate.await();
                    throw releaseFailure;
                };
        // Taken where the release is about to let the waiting close go: the count-down after the
        // creation is the first, the one after the release the second.
        AtomicInteger countDowns = new AtomicInteger();
        AtomicReference<Long> countAtReleaseEnd = new AtomicReference<>();
        AtomicReference<List<Throwable>> suppressedAtReleaseEnd = new AtomicReference<>();
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> client,
                        count -> {
                            if (countDowns.incrementAndGet() == 2) {
                                countAtReleaseEnd.set(count);
                                suppressedAtReleaseEnd.set(List.of(failure.getSuppressed()));
                            }
                        });
        enumerator.start();
        Thread initializing = new Thread(() -> context.runAsyncCallsDeferringHandlers(), "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        AtomicReference<List<LogCapture.Event>> loggedByClose = new AtomicReference<>();

        // At INFO, so that a failure reported at INFO instead is captured, and fails the level
        // assertion rather than going unseen.
        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            Thread closing =
                    new Thread(
                            () -> {
                                closeResult.set(catchThrowable(enumerator::close));
                                // Taken as close() returns: Flink may never run the handler after
                                // it.
                                loggedByClose.set(new ArrayList<>(capture.getEvents()));
                            },
                            "c");
            try {
                initializing.start();
                assertThat(releaseEntered.await(10, TimeUnit.SECONDS))
                        .as("the release after the failure began closing the client")
                        .isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for the release")
                        .isEqualTo(Thread.State.WAITING);
                assertThat(capture.getEvents())
                        .as("logged while the release is still closing the client")
                        .isEmpty();
                releaseGate.countDown();
            } finally {
                releaseGate.countDown();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
            assertThat(countAtReleaseEnd.get())
                    .as(
                            "the release's latch as it was about to let the close go, which nothing"
                                    + " may count down earlier")
                    .isEqualTo(1L);
            assertThat(suppressedAtReleaseEnd.get())
                    .as("what the initialization failure carried by then")
                    .containsExactly(releaseFailure);
            // The release's own failure is logged as well, since the enumerator had closed by the
            // time it failed; this test asserts only the initialization failure.
            assertThat(loggedByClose.get())
                    .as("the initialization failure, logged at WARN by the time close() returns")
                    .anySatisfy(
                            event -> {
                                isAWarning(event);
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
        assertThat(closeResult.get()).isNull();
    }

    @Test
    void aStartPositionFailureIsRecordedBeforeTheReleaseSoACloseDuringItLogsIt() throws Exception {
        // The start position resolves after the bound while the enumerator is open, and the close
        // lands while the release after it is still closing the client. Recorded only after the
        // release, the failure would be left to initialization's thread, which would log it only
        // once the release has ended, racing the close's return, when Flink may already be shutting
        // the coordinator's executor down.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        CountDownLatch releaseEntered = new CountDownLatch(1);
        CountDownLatch releaseGate = new CountDownLatch(1);
        ScriptedClient client = new ScriptedClient();
        client.duringClose =
                () -> {
                    releaseEntered.countDown();
                    releaseGate.await();
                };
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, clientMetrics -> client, Instant.EPOCH);
        enumerator.start();
        Thread initializing = new Thread(() -> context.runAsyncCallsDeferringHandlers(), "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        AtomicReference<List<LogCapture.Event>> loggedByClose = new AtomicReference<>();

        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            Thread closing =
                    new Thread(
                            () -> {
                                closeResult.set(catchThrowable(enumerator::close));
                                // Taken as close() returns: Flink may never run the handler after
                                // it.
                                loggedByClose.set(new ArrayList<>(capture.getEvents()));
                            },
                            "c");
            try {
                initializing.start();
                assertThat(releaseEntered.await(10, TimeUnit.SECONDS))
                        .as("the release after the failure began closing the client")
                        .isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for the release")
                        .isEqualTo(Thread.State.WAITING);
                assertHoldsNoMonitorOf(closing, enumerator);
                // Holding the enumerator's monitor once the release ends keeps initialization's
                // thread from recording or logging the failure until the close has returned, so
                // what the close logged cannot be confused with what that thread logs after it. The
                // close needs the monitor only before it waits for the release.
                synchronized (enumerator) {
                    releaseGate.countDown();
                    closing.join(10_000);
                    assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
                }
            } finally {
                releaseGate.countDown();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(loggedByClose.get())
                    .as("the start-position failure, logged by the time close() returns")
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(
                            event ->
                                    assertIsTheFailure(
                                            event.getThrowable(),
                                            OpenFailure.START_POSITION,
                                            null));
            assertThat(capture.getEvents()).as("events once initialization ended").hasSize(1);
        }
        assertThat(closeResult.get()).isNull();
        assertThat(client.closeCalls).isOne();
    }

    /** The step whose failure initialization records before it lets a waiting close go. */
    private enum RecordedStep {
        /** The client's creation fails. */
        CREATION,
        /** Initialization succeeds and the client's release fails. */
        RELEASE,
        /** The client's call fails, and its release fails with a JVM-fatal error carrying it. */
        FATAL_RELEASE
    }

    @ParameterizedTest
    @EnumSource(RecordedStep.class)
    void aCloseLandingJustBeforeInitializationLetsItGoLogsTheFailureItRecorded(RecordedStep step)
            throws Exception {
        // Initialization records the failure a step observes before it counts down the latch a
        // close() waits on. A close that lands between the two finds the failure recorded and logs
        // it; recorded only after the latch, it would be left to initialization's thread, which
        // would log it racing the close's return.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException failure = new IllegalStateException("initialization failed");
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamCoordinatorClientFactory clientFactory = clientMetrics -> client;
        OutOfMemoryError fatal = new OutOfMemoryError("release failed");
        Throwable logged;
        switch (step) {
            case CREATION:
                clientFactory =
                        clientMetrics -> {
                            throw failure;
                        };
                logged = failure;
                break;
            case RELEASE:
                client.duringClose =
                        () -> {
                            throw failure;
                        };
                logged = failure;
                break;
            case FATAL_RELEASE:
                client.initializationFailure = failure;
                client.duringClose =
                        () -> {
                            throw fatal;
                        };
                logged = fatal;
                break;
            default:
                throw new IllegalArgumentException(step.name());
        }
        // Initialization lets a waiting close go once after creating the client and once after
        // releasing it; the step that failed is the first for a creation, the second otherwise.
        GatedCountDown countDown = new GatedCountDown(step == RecordedStep.CREATION ? 1 : 2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, clientFactory, countDown);
        enumerator.start();
        Thread initializing = new Thread(() -> context.runAsyncCallsDeferringHandlers(), "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        AtomicReference<List<LogCapture.Event>> loggedByClose = new AtomicReference<>();

        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            Thread closing =
                    new Thread(
                            () -> {
                                closeResult.set(catchThrowable(enumerator::close));
                                // Taken as close() returns: Flink may never run the handler after
                                // it.
                                loggedByClose.set(new ArrayList<>(capture.getEvents()));
                            },
                            "c");
            try {
                initializing.start();
                assertThat(countDown.awaitReached())
                        .as("initialization reached the end of the step that failed")
                        .isTrue();
                assertThat(countDown.countAtGate())
                        .as(
                                "the step's latch as initialization was about to let a close go, which"
                                        + " nothing may count down earlier")
                        .isEqualTo(1L);
                if (step == RecordedStep.FATAL_RELEASE) {
                    assertThat(fatal.getSuppressed())
                            .as("what the fatal release failure carried by then")
                            .containsExactly(failure);
                }
                assertThat(capture.getEvents()).as("logged before the close").isEmpty();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for that step")
                        .isEqualTo(Thread.State.WAITING);
                assertHoldsNoMonitorOf(closing, enumerator);
                // Holding the enumerator's monitor once the close is let go keeps initialization's
                // thread from recording or logging the failure until the close has returned, so
                // what the close logged cannot be confused with what that thread logs after it.
                synchronized (enumerator) {
                    countDown.open();
                    closing.join(10_000);
                    assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
                }
            } finally {
                countDown.open();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(loggedByClose.get())
                    .as("the failure initialization recorded, logged by the time close() returns")
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(
                            event ->
                                    assertThat(event.getThrowable())
                                            .as("the failure recorded when the close was let go")
                                            .isSameAs(logged));
            assertThat(capture.getEvents()).as("events once initialization ended").hasSize(1);
        }
        assertThat(closeResult.get()).isNull();
    }

    /** The step at which initialization, holding a client, finds the enumerator closed. */
    private enum ClosedStep {
        /** The client is created after the close landed, so it is closed at once. */
        CREATION,
        /** The client is released after the close landed. */
        RELEASE
    }

    @ParameterizedTest
    @EnumSource(ClosedStep.class)
    void aClientCloseFailureAfterTheEnumeratorClosedIsLoggedBeforeTheCloseIsLetGo(ClosedStep step)
            throws Exception {
        // The enumerator's close lands while initialization is creating or releasing a client, and
        // closing that client then fails. Initialization logs the failure before it lets the
        // waiting close go, so it is logged by the time close() returns: Flink may never run the
        // handler after it.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        RuntimeException closeFailure = new IllegalStateException("close failed");
        CountDownLatch stepEntered = new CountDownLatch(1);
        CountDownLatch stepGate = new CountDownLatch(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamCoordinatorClientFactory clientFactory;
        if (step == ClosedStep.CREATION) {
            client.duringClose =
                    () -> {
                        throw closeFailure;
                    };
            clientFactory =
                    clientMetrics -> {
                        stepEntered.countDown();
                        stepGate.await();
                        return client;
                    };
        } else {
            client.duringClose =
                    () -> {
                        stepEntered.countDown();
                        stepGate.await();
                        throw closeFailure;
                    };
            clientFactory = clientMetrics -> client;
        }
        // The first count-down follows the creation, the second the release.
        GatedCountDown countDown = new GatedCountDown(step == ClosedStep.CREATION ? 1 : 2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, clientFactory, countDown);
        enumerator.start();
        Thread initializing = new Thread(() -> context.runAsyncCallsDeferringHandlers(), "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        Thread closing = new Thread(() -> closeResult.set(catchThrowable(enumerator::close)), "c");

        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            try {
                initializing.start();
                assertThat(stepEntered.await(10, TimeUnit.SECONDS))
                        .as("initialization began the step")
                        .isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for that step")
                        .isEqualTo(Thread.State.WAITING);
                stepGate.countDown();
                assertThat(countDown.awaitReached())
                        .as("initialization reached the count-down after the step")
                        .isTrue();
                assertThat(countDown.countAtGate())
                        .as(
                                "the step's latch as initialization was about to let the close go,"
                                        + " which nothing may count down earlier")
                        .isEqualTo(1L);
                assertThat(capture.getEvents())
                        .as("logged before initialization lets the close go")
                        .singleElement()
                        .satisfies(
                                SpannerChangeStreamSplitEnumeratorInitializationFailureTest
                                        ::isAWarning)
                        .satisfies(
                                event ->
                                        assertThat(event.getThrowable())
                                                .as("the client's close failure")
                                                .isSameAs(closeFailure));
            } finally {
                stepGate.countDown();
                countDown.open();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
            assertThat(capture.getEvents()).as("events once both ended").hasSize(1);
        }
        assertThat(closeResult.get()).isNull();
    }

    @ParameterizedTest
    @MethodSource("closeFailures")
    void aCloseFailureOfTheClientTheEnumeratorsCloseTookOverIsLoggedBeforeItIsThrown(
            Throwable closeFailure) throws Exception {
        // Flink closes an enumerator through IOUtils.closeQuietly, which drops what close()
        // throws: before #2366 this failure left no trace.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        client.duringClose =
                () -> {
                    if (closeFailure instanceof Error) {
                        throw (Error) closeFailure;
                    }
                    throw (Exception) closeFailure;
                };
        SpannerChangeStreamSplitEnumerator[] enumerator = {enumerator(context, client)};
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        AtomicReference<List<LogCapture.Event>> loggedByClose = new AtomicReference<>();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            client.duringInitialize =
                    () -> {
                        closeResult.set(catchThrowable(enumerator[0]::close));
                        // Taken as close() returns: Flink may never run the handler after it.
                        loggedByClose.set(new ArrayList<>(capture.getEvents()));
                    };
            enumerator[0].start();
            assertThat(catchThrowable(context::runAsyncCalls)).isNull();
            assertThat(loggedByClose.get())
                    .as("the close failure, logged by the time close() returns")
                    .singleElement()
                    .satisfies(
                            SpannerChangeStreamSplitEnumeratorInitializationFailureTest::isAWarning)
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(closeFailure));
            assertThat(capture.getEvents()).as("events after the handler ran").hasSize(1);
        }
        if (closeFailure instanceof Error) {
            assertThat(closeResult.get()).as("what close() threw").isSameAs(closeFailure);
        } else {
            assertThat(closeResult.get())
                    .as("what close() threw")
                    .isInstanceOf(java.io.IOException.class)
                    .cause()
                    .isSameAs(closeFailure);
        }
    }

    private static Stream<Throwable> closeFailures() {
        return Stream.of(
                new IllegalStateException("close failed"),
                new NoClassDefFoundError("closed loader"));
    }

    @Test
    void theRefusalOfAnEnumeratorClosedBeforeInitializationIsNotLogged() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, new ScriptedClient());
        enumerator.start();

        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            enumerator.close();
            assertThat(catchThrowable(context::runAsyncCalls)).isNull();
            assertThat(capture.getEvents()).as("events of the refused initialization").isEmpty();
        }
    }

    /**
     * A {@code beforeClientReleasedCountDown} hook that records the latch's count at its {@code
     * gated}-th call and holds initialization there until {@link #open()}.
     */
    private static final class GatedCountDown implements LongConsumer {

        private final int gated;
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch gate = new CountDownLatch(1);
        private volatile long countAtGate = -1;

        GatedCountDown(int gated) {
            this.gated = gated;
        }

        @Override
        public void accept(long count) {
            if (calls.incrementAndGet() == gated) {
                countAtGate = count;
                reached.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /** Waits for the gated call; false if it did not come in time. */
        boolean awaitReached() throws InterruptedException {
            return reached.await(10, TimeUnit.SECONDS);
        }

        /** The latch's count at the gated call, or -1 before it. */
        long countAtGate() {
            return countAtGate;
        }

        /** Lets the gated call, and any later one, go on. */
        void open() {
            gate.countDown();
        }
    }

    private static FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context(int parallelism) {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i);
        }
        return context;
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, ScriptedClient client) {
        return enumerator(context, clientMetrics -> client);
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory) {
        return enumerator(context, clientFactory, count -> {});
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            LongConsumer beforeClientReleasedCountDown) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                clientFactory,
                StartPosition.latest(),
                null,
                null,
                2_000,
                true,
                Duration.ofMinutes(10),
                null,
                null,
                System::currentTimeMillis,
                System::nanoTime,
                beforeClientReleasedCountDown);
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            @Nullable Instant endTimestamp) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                clientFactory,
                StartPosition.latest(),
                null,
                endTimestamp,
                2_000,
                true,
                Duration.ofMinutes(10),
                null,
                null);
    }

    /** Waits until a thread parks waiting, is blocked on a monitor, or has ended. */
    private static Thread.State awaitParkedOrEnded(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING
                    || state == Thread.State.BLOCKED
                    || state == Thread.State.TERMINATED) {
                return state;
            }
            assertThat(System.nanoTime())
                    .as("%s parked or ended in time", thread.getName())
                    .isLessThan(deadline);
            Thread.onSpinWait();
        }
    }

    /**
     * Asserts that {@code thread} holds no monitor of {@code object}, before the test takes that
     * monitor itself: should it hold one, taking it would block the test instead of failing it.
     */
    private static void assertHoldsNoMonitorOf(Thread thread, Object object) {
        ThreadInfo info =
                ManagementFactory.getThreadMXBean()
                        .getThreadInfo(new long[] {thread.getId()}, true, false)[0];
        assertThat(info).as("%s is alive", thread.getName()).isNotNull();
        assertThat(info.getLockedMonitors())
                .as("the monitors %s holds while it waits", thread.getName())
                .noneMatch(
                        monitor ->
                                monitor.getIdentityHashCode() == System.identityHashCode(object)
                                        && monitor.getClassName()
                                                .equals(object.getClass().getName()));
    }

    /** Asserts that a captured event was logged at WARN, which its rendering starts with. */
    private static void isAWarning(LogCapture.Event event) {
        assertThat(event.toString()).as("the event's level").startsWith("WARN ");
    }
}
