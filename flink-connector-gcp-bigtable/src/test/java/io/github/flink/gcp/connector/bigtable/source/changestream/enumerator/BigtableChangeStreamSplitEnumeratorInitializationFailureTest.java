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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An initialization failure that races the enumerator's close is logged rather than dropped
 * (#2369): Flink runs {@code onInitialized} on a closed enumerator, or never once it has shut the
 * coordinator's executor down.
 */
class BigtableChangeStreamSplitEnumeratorInitializationFailureTest {

    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();

    private static final String FAILURE_MESSAGE =
            "Failed to initialize Bigtable Change Streams partitions.";

    private static final String UNREPORTED_LINE =
            FAILURE_MESSAGE
                    + " The enumerator closed before it reported the failure, so this is its only"
                    + " trace.";

    private static final String AFTER_CLOSE_LINE =
            "The enumerator's start-up work failed after the enumerator closed, and that close may"
                    + " have caused the failure, so the job is not failed with it: "
                    + FAILURE_MESSAGE;

    @Test
    void logsAnInitializationFailureWhoseHandlerRunsOnlyAfterTheClose() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("permission denied");
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, failingWith(failure));
        enumerator.start();
        List<Runnable> handlers = context.runAsyncCallsDeferringHandlers();

        try (LogCapture capture =
                LogCapture.of(BigtableChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            enumerator.close();
            handlers.forEach(Runnable::run);

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
    void logsAnInitializationFailureWhoseHandlerNeverRuns() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("permission denied");
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, failingWith(failure));
        enumerator.start();
        context.runAsyncCallsDeferringHandlers();

        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            enumerator.close();

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
    void doesNotLogAnInitializationFailureItReported() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("permission denied");
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, failingWith(failure));
        enumerator.start();
        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(FAILURE_MESSAGE)
                .hasCause(failure);

        try (LogCapture capture =
                LogCapture.of(BigtableChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            enumerator.close();

            assertThat(capture.getEvents()).isEmpty();
        }
    }

    @Test
    void logsAnInitializationFailureAfterTheCloseAtInfo() throws Exception {
        // The close of the coordinator client may have caused it.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("client closed");
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, failingWith(failure));
        enumerator.start();

        try (LogCapture capture =
                LogCapture.of(BigtableChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            enumerator.close();
            context.runAsyncCalls();

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
    void logsAnInitializationFailureTheCloseOfTheClientCausesAtInfo() throws Exception {
        // Initialization is in flight when close() closes the client, and fails because of it.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("client closed");
        ScriptedChangeStreamCoordinatorClient client = failingWith(failure);
        List<Runnable> handlers = new ArrayList<>();
        client.onClose(() -> handlers.addAll(context.runAsyncCallsDeferringHandlers()));
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client);
        enumerator.start();

        try (LogCapture capture =
                LogCapture.of(BigtableChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            enumerator.close();
            handlers.forEach(Runnable::run);

            assertThat(handlers).hasSize(1);
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

    private static ScriptedChangeStreamCoordinatorClient failingWith(RuntimeException failure) {
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        client.failNextGenerationWith(failure);
        return client;
    }

    private static BigtableChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client) {
        return new BigtableChangeStreamSplitEnumerator(
                context,
                client,
                StartPosition.latest(),
                null,
                null,
                false,
                false,
                Clock.systemUTC(),
                Duration.ZERO,
                true);
    }
}
