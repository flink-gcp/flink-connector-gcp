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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloudTasksWriter}. */
class CloudTasksWriterTest {

    private final FakeTaskCreator creator = new FakeTaskCreator();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final ManualTimeSource time = new ManualTimeSource();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void writesOneTaskPerRecordIntoTheResolvedQueue() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("first", TestContexts.NO_OP);
        writer.write("second", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests).hasSize(2);
        assertThat(creator.requests)
                .allSatisfy(
                        request ->
                                assertThat(request.getParent())
                                        .isEqualTo(TestSinkConfigs.QUEUE_PATH));
        assertThat(creator.requests.get(0).getTask().getHttpRequest().getBody().toStringUtf8())
                .isEqualTo("first");
        assertThat(writer.getInFlightTasks()).isZero();
    }

    @Test
    void resolvesTheQueuePerRecord() throws Exception {
        QueueDestination other = QueueDestination.of("my-project", "europe-west1", "slow");
        CloudTasksWriter<String> writer =
                writer(
                        builderWithoutDestination()
                                .destinationResolver(
                                        (element, context) ->
                                                element.startsWith("slow")
                                                        ? other
                                                        : TestSinkConfigs.QUEUE));

        writer.write("fast-one", TestContexts.NO_OP);
        writer.write("slow-one", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests.get(0).getParent()).isEqualTo(TestSinkConfigs.QUEUE_PATH);
        assertThat(creator.requests.get(1).getParent())
                .isEqualTo("projects/my-project/locations/europe-west1/queues/slow");
    }

    @Test
    void namesTasksWithTheHashedExtractedKey() throws Exception {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> element));

        writer.write("order-1", TestContexts.NO_OP);
        writer.write("order-1", TestContexts.NO_OP);
        writer.write("order-2", TestContexts.NO_OP);
        writer.flush(false);

        // The key itself never reaches Cloud Tasks: sequential ids raise latency and error rates,
        // so the sink writes the digest, which is stable per key and 64 hex characters long.
        assertThat(creator.requests.get(0).getTask().getName())
                .isEqualTo(TestSinkConfigs.QUEUE_PATH + "/tasks/" + TestSinkConfigs.ORDER_1_DIGEST);
        assertThat(creator.requests.get(1).getTask().getName())
                .isEqualTo(creator.requests.get(0).getTask().getName());
        assertThat(creator.requests.get(2).getTask().getName())
                .isNotEqualTo(creator.requests.get(0).getTask().getName())
                .matches(".*/tasks/[0-9a-f]{64}");
    }

    @Test
    void leavesTasksUnnamedWithoutAnExtractor() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests.get(0).getTask().getName()).isEmpty();
    }

    @Test
    void treatsAlreadyExistsOnANamedTaskAsSuccess() throws Exception {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> element));
        creator.enqueueFailure(StatusCode.Code.ALREADY_EXISTS);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // Cloud Tasks still remembers the id, which is exactly the deduplication that was asked
        // for — the task exists, so there is nothing to retry and nothing to fail.
        assertThat(creator.requests).hasSize(1);
    }

    @Test
    void failsOnAlreadyExistsWithoutANameBecauseItShouldBeUnreachable() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.ALREADY_EXISTS);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("carries no name");
    }

    @Test
    void retriesTransientFailuresWithinTheBudget() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(3, Duration.ofMillis(100)));
        creator.enqueueFailure(StatusCode.Code.UNAVAILABLE);
        creator.enqueueFailure(StatusCode.Code.RESOURCE_EXHAUSTED);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        // The same request is re-sent unchanged, and the backoff is waited out rather than spun on.
        assertThat(creator.requests).hasSize(3);
        assertThat(creator.requests.get(1)).isEqualTo(creator.requests.get(0));
        assertThat(creator.requests.get(2)).isEqualTo(creator.requests.get(0));
        assertThat(time.getSleptMillis()).isPositive();
    }

    @Test
    void aRetryOfANamedTaskTheServiceAlreadyCreatedSucceeds() throws Exception {
        // The ambiguity naming exists to remove: a DEADLINE_EXCEEDED may mean the task was created
        // anyway, and the retry then comes back ALREADY_EXISTS — which is the task being there,
        // not a failure. Under unnamed tasks the same retry would silently duplicate.
        CloudTasksWriter<String> writer =
                writer(retrying(3, Duration.ofMillis(100)).taskIdExtractor(element -> element));
        creator.enqueueFailure(StatusCode.Code.DEADLINE_EXCEEDED);
        creator.enqueueFailure(StatusCode.Code.ALREADY_EXISTS);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests).hasSize(2);
        assertThat(creator.requests.get(1).getTask().getName())
                .isEqualTo(creator.requests.get(0).getTask().getName());
    }

    @Test
    void failsOnceTheTransientBudgetIsSpent() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(2, Duration.ofMillis(100)));
        creator.enqueueFailures(2, StatusCode.Code.DEADLINE_EXCEEDED);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED")
                .hasMessageContaining("2 attempt(s)");
        assertThat(creator.requests).hasSize(2);
    }

    @Test
    void retriesNotFoundOnItsOwnShorterBudget() throws Exception {
        // A NOT_FOUND spends only the NOT_FOUND budget, never the transient one, so a mistyped
        // queue name fails after a few attempts instead of the full budget on every record.
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .writerOptions(
                                        CloudTasksWriterOptions.builder()
                                                .retryMaxAttempts(20)
                                                .notFoundMaxAttempts(2)
                                                .notFoundInitialBackoff(Duration.ofMillis(500))
                                                .build()));
        creator.enqueueFailures(2, StatusCode.Code.NOT_FOUND);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("2 attempt(s)");
        assertThat(creator.requests).hasSize(2);
        // One backoff of the NOT_FOUND budget's initial 500 ms, jittered by ±25%. The point is
        // that it spent the NOT_FOUND budget rather than the transient one (whose initial backoff
        // is 100 ms), which the range still distinguishes.
        assertThat(time.getSleptMillis()).isBetween(375L, 625L);
    }

    @Test
    void failsImmediatelyOnATerminalStatus() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("order-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(creator.requests).hasSize(1);
    }

    @Test
    void surfacesAnAsynchronousFailureFromTheNextWrite() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());
        creator.enqueueFailure(StatusCode.Code.PERMISSION_DENIED);

        writer.write("order-1", TestContexts.NO_OP);
        mailbox.drain();

        assertThatThrownBy(() -> writer.write("order-2", TestContexts.NO_OP))
                .isInstanceOf(IOException.class);
    }

    @Test
    void yieldsToTheMailboxWhenTheInFlightCapIsReached() throws Exception {
        CloudTasksWriter<String> writer = writer(capped(2));
        SettableApiFuture<Task> first = creator.enqueuePending();
        creator.enqueuePending();

        writer.write("order-1", TestContexts.NO_OP);
        writer.write("order-2", TestContexts.NO_OP);
        assertThat(writer.getInFlightTasks()).isEqualTo(2);

        // The third write must wait for a completion; without one it would block the task thread.
        first.set(Task.getDefaultInstance());
        writer.write("order-3", TestContexts.NO_OP);

        assertThat(creator.requests).hasSize(3);
        assertThat(writer.getInFlightTasks()).isEqualTo(2);
    }

    @Test
    void countsCreationsWaitingOutABackoffAgainstTheInFlightCap() throws Exception {
        // Parked creations are records Cloud Tasks has not accepted yet, so they bound memory the
        // same way in-flight ones do: at a cap of one, the next write cannot start until the
        // parked creation has been re-sent and completed.
        CloudTasksWriter<String> writer = writer(capped(1));
        creator.enqueueFailure(StatusCode.Code.UNAVAILABLE);

        writer.write("order-1", TestContexts.NO_OP);
        mailbox.drain();
        assertThat(writer.getInFlightTasks()).isZero();
        assertThat(writer.getParkedTasks()).isEqualTo(1);

        writer.write("order-2", TestContexts.NO_OP);

        assertThat(creator.requests).hasSize(3);
        assertThat(writer.getParkedTasks()).isZero();
        assertThat(time.getSleptMillis()).isPositive();
    }

    @Test
    void flushWaitsForCreationsWaitingOutABackoff() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(3, Duration.ofMillis(250)));
        creator.enqueueFailure(StatusCode.Code.UNAVAILABLE);

        writer.write("order-1", TestContexts.NO_OP);
        mailbox.drain();
        assertThat(writer.getParkedTasks()).isEqualTo(1);

        writer.flush(true);

        assertThat(writer.getParkedTasks()).isZero();
        assertThat(writer.getInFlightTasks()).isZero();
        assertThat(creator.requests).hasSize(2);
    }

    @Test
    void rejectsARecordWhoseDestinationResolvesToNull() {
        CloudTasksWriter<String> writer =
                writer(builderWithoutDestination().destinationResolver((element, context) -> null));

        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("returned null");
    }

    @Test
    void rejectsATaskTheSerializerAlreadyNamed() {
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

        // Letting a serializer-chosen name through would be a second path around the hashing.
        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already named");
    }

    @Test
    void rejectsAnEmptyExtractedTaskId() {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> ""));

        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty key");
    }

    @Test
    void failsTheJobOnASerializationFailureUnderTheDefaultPolicy() {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .serializer(
                                        element -> {
                                            throw new IllegalStateException("broken");
                                        }));

        assertThatThrownBy(() -> writer.write("order-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("could not be serialized")
                .hasRootCauseMessage("broken");
    }

    @Test
    void closesTheCreator() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.close();

        assertThat(creator.closeCalls).isEqualTo(1);
    }

    private CloudTasksWriter<String> writer(CloudTasksSinkBuilder<String> builder) {
        return new CloudTasksWriter<>(
                TestSinkConfigs.config(builder), creator, mailbox, metrics, time);
    }

    /** A builder retrying transient failures with the given budget. */
    private static CloudTasksSinkBuilder<String> retrying(int maxAttempts, Duration backoff) {
        return TestSinkConfigs.builder()
                .writerOptions(
                        CloudTasksWriterOptions.builder()
                                .retryMaxAttempts(maxAttempts)
                                .retryInitialBackoff(backoff)
                                .retryMaxBackoff(backoff)
                                .build());
    }

    /** A builder capping the outstanding creations, keeping the retry backoff short. */
    private static CloudTasksSinkBuilder<String> capped(int maxInFlightTasks) {
        return TestSinkConfigs.builder()
                .writerOptions(
                        CloudTasksWriterOptions.builder()
                                .maxInFlightTasks(maxInFlightTasks)
                                .retryInitialBackoff(Duration.ofMillis(100))
                                .build());
    }

    /** A builder carrying the shared serializer but no destination yet. */
    private static CloudTasksSinkBuilder<String> builderWithoutDestination() {
        return CloudTasksSink.<String>builder().serializer(TestSinkConfigs.serializer());
    }
}
