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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.FailedTask;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the per-task failure policy of {@link CloudTasksWriter}.
 *
 * <p>Kept apart from {@link CloudTasksWriterTest}, which stays the regression guard for the default
 * policy: {@code failJob()} is today's capture-and-rethrow, so that class keeps passing on the
 * routed failures too.
 *
 * <p>Timed out as a class because the fake mailbox blocks on an empty mailbox exactly as the real
 * one does, so a broken drain predicate hangs rather than fails.
 */
@Timeout(30)
class CloudTasksWriterFailureHandlerTest {

    private final FakeTaskCreator creator = new FakeTaskCreator();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final ManualTimeSource time = new ManualTimeSource();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final RecordingHandler handler = new RecordingHandler();

    /** Records what it is handed, and optionally fails. */
    private static final class RecordingHandler implements FailureHandler<FailedTask> {

        private static final long serialVersionUID = 1L;

        private final transient List<FailedTask> handled = new ArrayList<>();
        private final transient List<String> events = new ArrayList<>();
        private transient Exception failure;
        private transient int openCalls;
        private transient int flushCalls;
        private transient int closeCalls;

        @Override
        public void open(FailureHandlerContext context) {
            openCalls++;
        }

        @Override
        public void handle(FailedTask task) throws IOException {
            handled.add(task);
            events.add("handle");
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
        }

        @Override
        public void flush() {
            flushCalls++;
            events.add("flush");
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    // ---------------------------------------------------------------- write path

    @Test
    void routesASerializationFailureToTheHandler() throws Exception {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .serializer(
                                        element -> {
                                            throw new IllegalStateException("broken");
                                        }));

        writer.write("order-1", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        FailedTask failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(TestSinkConfigs.QUEUE);
        assertThat(failed.getTask()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.getErrorMessage()).isEqualTo("The record could not be serialized.");
        assertThat(failed.getCause()).hasMessage("broken");
        // Dropped, so nothing was sent and the record does not hold the writer.
        assertThat(creator.requests).isEmpty();
        assertThat(writer.getInFlightTasks()).isZero();
    }

    @Test
    void routesATaskIdExtractorFailureToTheHandler() throws Exception {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .taskIdExtractor(
                                        element -> {
                                            throw new IllegalStateException("no key here");
                                        }));

        writer.write("order-1", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        FailedTask failed = handler.handled.get(0);
        assertThat(failed.getErrorMessage())
                .isEqualTo("The task id extractor failed for the record.");
        assertThat(failed.getCause()).hasMessage("no key here");
        // The record did serialize, so the payload survives for the dead letter — unnamed, since
        // the name is exactly what could not be composed.
        assertThat(failed.getTask()).isNotNull();
        assertThat(failed.getTask().getHttpRequest().getBody().toStringUtf8()).isEqualTo("order-1");
        assertThat(failed.getTask().getName()).isEmpty();
        assertThat(creator.requests).isEmpty();
    }

    @Test
    void failsTheJobOnAnEmptyExtractedTaskId() {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> ""));

        // Configuration-shaped: an extractor with no key to return has none for any record, so
        // dropping would leave an empty queue under a green job.
        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty key");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void failsTheJobOnATaskTheSerializerAlreadyNamed() {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .serializer(
                                        element ->
                                                Task.newBuilder()
                                                        .setName(
                                                                TestSinkConfigs.QUEUE_PATH
                                                                        + "/tasks/mine")
                                                        .build()));

        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already named");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void failsTheJobWhenTheDestinationResolverReturnsNull() {
        CloudTasksWriter<String> writer =
                writer(
                        CloudTasksSink.<String>builder()
                                .serializer(TestSinkConfigs.serializer())
                                .destinationResolver((element, context) -> null));

        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("returned null");
        assertThat(handler.handled).isEmpty();
    }

    // ---------------------------------------------------------------- create path

    @Test
    void routesAnInvalidArgumentCreationToTheHandler() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        FailedTask failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(TestSinkConfigs.QUEUE);
        assertThat(failed.describeDestination()).isEqualTo(TestSinkConfigs.QUEUE_PATH);
        assertThat(failed.getTask()).isNotNull();
        assertThat(failed.getTask().getHttpRequest().getBody().toStringUtf8()).isEqualTo("order-1");
        assertThat(failed.getErrorMessage())
                .isEqualTo("Cloud Tasks rejected the task with INVALID_ARGUMENT.");
        // Dropped: the creation is not retried and the checkpoint completes.
        assertThat(creator.requests).hasSize(1);
        assertThat(writer.getParkedTasks()).isZero();
    }

    @Test
    void treatsAChainCarryingBothStatusesAsTransient() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(1));
        // INVALID_ARGUMENT outermost, UNAVAILABLE underneath: the first classifiable status is the
        // data-shaped one, and it must still not be dropped. Classification is a precedence over
        // the whole chain, not a first-match, so an unstable service can never produce a dead
        // letter. No gax failure carries both today — the point is that the guarantee does not
        // rest on that.
        creator.enqueueFailure(
                FakeTaskCreator.apiException(
                        StatusCode.Code.INVALID_ARGUMENT,
                        FakeTaskCreator.apiException(StatusCode.Code.UNAVAILABLE)));

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("UNAVAILABLE");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void retriesATransientStatusBuriedUnderAServerError() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(2));
        // The transient lookup scans the whole chain whatever sits in front of it, so this is
        // retried rather than treated as the terminal INTERNAL the first classifiable status
        // names. Safe in the one direction that matters — an unstable service is never dropped —
        // and it costs only a retry when the chain really was terminal.
        creator.enqueueFailure(
                FakeTaskCreator.apiException(
                        StatusCode.Code.INTERNAL,
                        FakeTaskCreator.apiException(StatusCode.Code.UNAVAILABLE)));

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests).hasSize(2);
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void failsTheJobOnAnInvalidArgumentBuriedUnderAServerError() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        // INTERNAL outermost: whatever the inner INVALID_ARGUMENT describes, this call failed
        // server-side, and dropping the record over that would be the mirror image of dropping it
        // over an outage. Only a chain whose first classifiable status *is* the data-shaped one is
        // routed.
        creator.enqueueFailure(
                FakeTaskCreator.apiException(
                        StatusCode.Code.INTERNAL,
                        FakeTaskCreator.apiException(StatusCode.Code.INVALID_ARGUMENT)));

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void routesAnInvalidArgumentNestedBehindAnUnclassifiableWrapper() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(
                new IllegalStateException(
                        "wrapper", FakeTaskCreator.apiException(StatusCode.Code.INVALID_ARGUMENT)));

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // The other half of the precedence: with no transient status anywhere in the chain, the
        // data-shaped one is still found however deep it sits.
        assertThat(handler.handled).hasSize(1);
    }

    @Test
    void keepsFailingTheJobOnAnExhaustedTransientBudget() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(2));
        creator.enqueueFailures(2, StatusCode.Code.UNAVAILABLE);

        writer.write("order-1", TestContexts.NO_OP);

        // An outage must never reach a dropping handler, or an incident bleeds the stream one
        // record at a time instead of backpressuring.
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("UNAVAILABLE");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void keepsFailingTheJobOnAnExhaustedNotFoundBudget() throws Exception {
        CloudTasksWriter<String> writer = writer(notFoundRetrying(2));
        creator.enqueueFailures(2, StatusCode.Code.NOT_FOUND);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void keepsFailingTheJobOnPermissionDenied() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.PERMISSION_DENIED);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void keepsTreatingAlreadyExistsOnANamedTaskAsSuccess() throws Exception {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> element));
        creator.enqueueFailure(StatusCode.Code.ALREADY_EXISTS);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // The deduplication that naming asked for: a success, so it never reaches the handler.
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void routesEvenAfterAnEarlierFailureAlreadyFailedTheJob() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.PERMISSION_DENIED);
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);
        writer.write("order-2", TestContexts.NO_OP);
        mailbox.drain();

        // The job is going to fail either way, but this task really did fail terminally: a
        // dead-letter destination missing it is worse than one holding a duplicate.
        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getTask().getHttpRequest().getBody().toStringUtf8())
                .isEqualTo("order-2");
        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
    }

    // ---------------------------------------------------------------- lifecycle

    @Test
    void flushesTheHandlerAfterTheWritePathHasDrained() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        // The failure mail is queued by write() and only runs inside the flush drain, so the
        // handler is handed the task *during* the flush: flushing the handler before the drain
        // would checkpoint past a dead letter still to come.
        writer.write("order-1", TestContexts.NO_OP);
        assertThat(handler.events).isEmpty();
        writer.flush(false);

        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void flushesTheHandlerOnceParkedRetriesHaveDrained() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(3));
        creator.enqueueFailure(StatusCode.Code.UNAVAILABLE);
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // The first attempt parks, the re-dispatch inside flush is rejected INVALID_ARGUMENT, and
        // only then does the handler flush.
        assertThat(creator.requests).hasSize(2);
        assertThat(handler.events).containsExactly("handle", "flush");
        assertThat(writer.getParkedTasks()).isZero();
    }

    @Test
    void capturesAHandlerFailureFromTheCompletionCallback() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        handler.failure = new IOException("dead-letter queue is down");
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);

        // A mailbox mail cannot throw a checked exception at its caller, so the failure is
        // captured and rethrown from the next write or flush.
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("dead-letter queue is down");
    }

    @Test
    void wrapsAnUncheckedHandlerFailureNamingTheQueue() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        handler.failure = new IllegalStateException("handler bug");
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed-task handler failed")
                .hasMessageContaining(TestSinkConfigs.QUEUE.toString())
                .hasRootCauseMessage("handler bug");
    }

    @Test
    void propagatesAHandlerFailureFromTheWritePath() {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .serializer(
                                        element -> {
                                            throw new IllegalStateException("broken");
                                        }));
        handler.failure = new IOException("no");

        // Not captured: write() is on the task thread already, so the handler's decision to fail
        // the job reaches the caller directly.
        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage("no");
    }

    @Test
    void closesTheHandlerWithTheWriter() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.close();

        assertThat(handler.closeCalls).isEqualTo(1);
        assertThat(creator.closeCalls).isEqualTo(1);
    }

    @Test
    void closesTheHandlerEvenWhenTheCreatorFails() {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.closeFailure = new IllegalStateException("client shutdown failed");

        // The lifecycle contract promises close on the failure path too.
        assertThatThrownBy(writer::close).hasMessage("client shutdown failed");
        assertThat(handler.closeCalls).isEqualTo(1);
    }

    @Test
    void neverOpensTheHandlerItself() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // Opening belongs to the sink's production createWriter (pinned by
        // CloudTasksSinkFailureHandlerOpenTest), so a writer built directly opens nothing.
        assertThat(handler.openCalls).isZero();
    }

    private CloudTasksWriter<String> writer(CloudTasksSinkBuilder<String> builder) {
        return new CloudTasksWriter<>(
                TestSinkConfigs.config(builder.failedTaskHandler(handler)),
                creator,
                mailbox,
                metrics,
                time);
    }

    /** A builder whose transient retry budget is the given number of attempts. */
    private static CloudTasksSinkBuilder<String> retrying(int maxAttempts) {
        return TestSinkConfigs.builder()
                .writerOptions(
                        CloudTasksWriterOptions.builder()
                                .retryMaxAttempts(maxAttempts)
                                .retryInitialBackoff(Duration.ofMillis(1))
                                .retryMaxBackoff(Duration.ofMillis(1))
                                .build());
    }

    /** A builder whose {@code NOT_FOUND} budget is the given number of attempts. */
    private static CloudTasksSinkBuilder<String> notFoundRetrying(int maxAttempts) {
        return TestSinkConfigs.builder()
                .writerOptions(
                        CloudTasksWriterOptions.builder()
                                .notFoundMaxAttempts(maxAttempts)
                                .notFoundInitialBackoff(Duration.ofMillis(1))
                                .notFoundMaxBackoff(Duration.ofMillis(1))
                                .build());
    }
}
