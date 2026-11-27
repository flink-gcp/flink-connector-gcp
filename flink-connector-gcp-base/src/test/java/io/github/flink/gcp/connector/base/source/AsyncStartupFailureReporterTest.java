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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Who reports a start-up failure: the handler that claims it, the close that finds it unclaimed, or
 * the work itself once the enumerator has closed. Each case asserts the log line, because the line
 * is the only report a closed enumerator leaves.
 */
class AsyncStartupFailureReporterTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncStartupFailureReporterTest.class);

    private static final String FAILURE_MESSAGE = "Failed to start the test source.";

    private static final String UNREPORTED_LINE =
            FAILURE_MESSAGE
                    + " The enumerator closed before it reported the failure, so this is its only"
                    + " trace.";

    private static final String AFTER_CLOSE_LINE =
            "The enumerator's start-up work failed after the enumerator closed, and that close may"
                    + " have caused the failure, so the job is not failed with it: "
                    + FAILURE_MESSAGE;

    @Test
    void theWrappedWorkAnswersAsTheWorkDoes() throws Exception {
        AsyncStartupFailureReporter reporter = reporter();

        assertThat(reporter.observing(() -> "planned").call()).isEqualTo("planned");
    }

    @Test
    void theWrappedWorkThrowsTheWorksOwnFailure() {
        AsyncStartupFailureReporter reporter = reporter();
        StackOverflowError failure = new StackOverflowError("deep");

        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);
    }

    @Test
    void aClaimAnswersTheExceptionTheJobFailsWith() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("permission denied");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        assertThat(reporter.claim(failure))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(FAILURE_MESSAGE)
                .hasCause(failure);
    }

    @Test
    void closeLogsAFailureTheHandlerHasNotClaimed() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("permission denied");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        try (LogCapture capture = LogCapture.of(AsyncStartupFailureReporterTest.class)) {
            reporter.close();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("WARN ");
                                assertThat(event.getMessage()).isEqualTo(UNREPORTED_LINE);
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void closeLogsAnErrorTheHandlerHasNotClaimed() {
        // A NoClassDefFoundError from a released class loader is the likeliest Error here.
        AsyncStartupFailureReporter reporter = reporter();
        NoClassDefFoundError failure = new NoClassDefFoundError("com/example/Released");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        try (LogCapture capture = LogCapture.of(AsyncStartupFailureReporterTest.class)) {
            reporter.close();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).isEqualTo(UNREPORTED_LINE);
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void closeLogsNothingForAFailureTheHandlerClaimed() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("permission denied");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        try (LogCapture capture =
                LogCapture.of(AsyncStartupFailureReporterTest.class, LogCapture.Level.DEBUG)) {
            assertThat(reporter.claim(failure)).isNotNull();
            reporter.close();

            assertThat(capture.getEvents()).isEmpty();
        }
    }

    @Test
    void aClosedReporterRefusesTheClaimOfTheFailureItLogged() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("permission denied");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        try (LogCapture capture = LogCapture.of(AsyncStartupFailureReporterTest.class)) {
            reporter.close();

            assertThat(reporter.claim(failure)).isNull();
            assertThat(capture.getMessages()).containsExactly(UNREPORTED_LINE);
        }
    }

    @Test
    void aFailureAfterTheCloseIsLoggedAtInfoWhereItIsThrown() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("channel closed");

        try (LogCapture capture =
                LogCapture.of(AsyncStartupFailureReporterTest.class, LogCapture.Level.INFO)) {
            reporter.close();
            assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

            assertThat(reporter.claim(failure)).isNull();
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("INFO ");
                                assertThat(event.getMessage()).isEqualTo(AFTER_CLOSE_LINE);
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void workThatStartedBeforeTheCloseAndFailsAfterItIsLoggedAtInfo() {
        // The reporter's state is read when the work fails, not when it starts: a call in flight
        // when the enumerator closes, and failing because of that close, is not a lost failure.
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("cancelled by the close");
        Callable<String> inFlight =
                reporter.observing(
                        () -> {
                            reporter.close();
                            throw failure;
                        });

        try (LogCapture capture =
                LogCapture.of(AsyncStartupFailureReporterTest.class, LogCapture.Level.INFO)) {
            assertThatThrownBy(inFlight::call).isSameAs(failure);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("INFO ");
                                assertThat(event.getMessage()).isEqualTo(AFTER_CLOSE_LINE);
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void anErrorAfterTheCloseIsLoggedToo() {
        AsyncStartupFailureReporter reporter = reporter();
        NoClassDefFoundError failure = new NoClassDefFoundError("com/example/Released");

        try (LogCapture capture =
                LogCapture.of(AsyncStartupFailureReporterTest.class, LogCapture.Level.INFO)) {
            reporter.close();
            assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(failure));
        }
    }

    @Test
    void closeLogsNothingAfterTheWorkSucceeded() throws Exception {
        AsyncStartupFailureReporter reporter = reporter();
        reporter.observing(() -> "planned").call();

        try (LogCapture capture =
                LogCapture.of(AsyncStartupFailureReporterTest.class, LogCapture.Level.DEBUG)) {
            reporter.close();

            assertThat(capture.getEvents()).isEmpty();
        }
    }

    @Test
    void aSecondCloseLogsTheFailureNoMore() {
        AsyncStartupFailureReporter reporter = reporter();
        IllegalStateException failure = new IllegalStateException("permission denied");
        assertThatThrownBy(reporter.observing(failingWith(failure))::call).isSameAs(failure);

        try (LogCapture capture = LogCapture.of(AsyncStartupFailureReporterTest.class)) {
            reporter.close();
            reporter.close();

            assertThat(capture.getMessages()).containsExactly(UNREPORTED_LINE);
        }
    }

    private static AsyncStartupFailureReporter reporter() {
        return new AsyncStartupFailureReporter(LOG, FAILURE_MESSAGE);
    }

    private static Callable<String> failingWith(Throwable failure) {
        return () -> {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (Exception) failure;
        };
    }
}
