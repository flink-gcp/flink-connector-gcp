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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the metrics {@link CloudTasksWriter} registers, against the same fake task creator and
 * manual clock its behavioural tests use.
 *
 * <p>Every assertion goes through the name a metric registered under, so renaming one — or failing
 * to register it — fails here.
 */
class CloudTasksWriterMetricsTest {

    private final FakeTaskCreator creator = new FakeTaskCreator();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final ManualTimeSource time = new ManualTimeSource();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    private CloudTasksWriter<String> writer(CloudTasksSinkBuilder<String> builder) {
        return new CloudTasksWriter<>(
                TestSinkConfigs.config(builder), creator, mailbox, metrics, time);
    }

    /** A builder retrying transient failures with a one-millisecond budget. */
    private static CloudTasksSinkBuilder<String> retrying(int maxAttempts) {
        return TestSinkConfigs.builder()
                .writerOptions(
                        CloudTasksWriterOptions.builder()
                                .retryMaxAttempts(maxAttempts)
                                .retryInitialBackoff(Duration.ofMillis(1))
                                .retryMaxBackoff(Duration.ofMillis(1))
                                .build());
    }

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private long errors(String errorClass) {
        return counter("errorClass", errorClass, "errors");
    }

    /** Serialized size of the task the fixture's serializer produces for this record. */
    private int sizeOf(String record) {
        return creator.requests.stream()
                .map(request -> request.getTask())
                .filter(task -> task.getHttpRequest().getBody().toStringUtf8().equals(record))
                .findFirst()
                .map(Task::getSerializedSize)
                .orElseThrow(() -> new AssertionError("No task was created for " + record + "."));
    }

    @Test
    void countsEveryRecordHandedToTheClientWithItsSerializedSize() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("first", TestContexts.NO_OP);
        writer.write("second", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend")).isEqualTo(sizeOf("first") + sizeOf("second"));
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void countsARetriedRecordOnlyOnce() throws Exception {
        // This sink owns its retries — a failed creation is parked and re-dispatched — so the
        // increment sits in write(), not in dispatch(), which dispatchDueRetries re-enters. The
        // attempts are visible as error-class counters instead. A mutant moving the increment into
        // dispatch dies here.
        CloudTasksWriter<String> writer = writer(retrying(5));
        creator.enqueueFailures(2, StatusCode.Code.UNAVAILABLE);

        writer.write("first", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(creator.requests).hasSize(3);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numBytesSend")).isEqualTo(sizeOf("first"));
        assertThat(errors("UNAVAILABLE")).isEqualTo(2);
    }

    @Test
    void gaugesReportTheWritersInFlightAndParkedCounts() throws Exception {
        CloudTasksWriter<String> writer = writer(retrying(5));
        SettableApiFuture<Task> pending = creator.enqueuePending();

        writer.write("first", TestContexts.NO_OP);

        assertThat(metrics.<Integer>gaugeValue("inFlightTasks")).isEqualTo(1);
        assertThat(metrics.<Integer>gaugeValue("parkedTasks")).isZero();

        pending.setException(FakeTaskCreator.apiException(StatusCode.Code.UNAVAILABLE));
        mailbox.drain();

        // The failure mail moved it out of flight and into the parked queue, so between them the
        // two gauges always account for every outstanding creation.
        assertThat(metrics.<Integer>gaugeValue("inFlightTasks")).isZero();
        assertThat(metrics.<Integer>gaugeValue("parkedTasks")).isEqualTo(1);

        writer.flush(false);

        assertThat(metrics.<Integer>gaugeValue("parkedTasks")).isZero();
    }

    @Test
    void countsADeduplicatedNamedTaskRatherThanAnError() throws Exception {
        // ALREADY_EXISTS on a named task is the deduplication naming asked for, so it is a success
        // with a counter of its own — not an error class, and not a send error.
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().taskIdExtractor(element -> element));
        creator.enqueueFailure(StatusCode.Code.ALREADY_EXISTS);

        writer.write("order-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(counter("tasksDeduplicated")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isZero();
        assertThat(metrics.hasMetric("errorClass", "ALREADY_EXISTS", "errors")).isFalse();
    }

    @Test
    void countsEveryRetryableAttemptUnderItsStatusCode() throws Exception {
        // Deliberately every attempt, retryable ones included: the sum over the retryable codes is
        // what a separate retries counter would have reported, which is why there is not one.
        CloudTasksWriter<String> writer = writer(retrying(3));
        creator.enqueueFailures(2, StatusCode.Code.DEADLINE_EXCEEDED);

        writer.write("first", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(errors("DEADLINE_EXCEEDED")).isEqualTo(2);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
    }

    @Test
    void countsARecordTheSerializerRejectedAsASendError() throws Exception {
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .serializer(
                                        element -> {
                                            throw new IllegalStateException("broken");
                                        })
                                .failedTaskHandler(FailureHandler.logAndDrop()));

        writer.write("first", TestContexts.NO_OP);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // Never handed to the client, so it is not a send.
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void countsATaskTheServiceRejectedAsASendError() throws Exception {
        CloudTasksWriter<String> writer =
                writer(TestSinkConfigs.builder().failedTaskHandler(FailureHandler.logAndDrop()));
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("first", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
    }

    @Test
    void registersNoPerQueueCountersByDefault() throws Exception {
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("first", TestContexts.NO_OP);
        writer.flush(false);

        // Off means nothing registered, not a counter at zero: Flink cannot unregister a metric.
        assertThat(metrics.hasMetric("destination", TestSinkConfigs.QUEUE_PATH, "recordsSend"))
                .isFalse();
        assertThat(counter("numRecordsSend")).isEqualTo(1);
    }

    @Test
    void countsPerQueueWhenPerDestinationMetricsAreOn() throws Exception {
        QueueDestination other = QueueDestination.of("my-project", "europe-west1", "slow");
        CloudTasksWriter<String> writer =
                writer(
                        TestSinkConfigs.builder()
                                .destinationResolver(
                                        (element, context) ->
                                                element.startsWith("slow")
                                                        ? other
                                                        : TestSinkConfigs.QUEUE)
                                .failedTaskHandler(FailureHandler.logAndDrop())
                                .writerOptions(
                                        CloudTasksWriterOptions.builder()
                                                .perDestinationMetrics(true)
                                                .build()));
        creator.enqueueFailure(StatusCode.Code.INVALID_ARGUMENT);

        writer.write("fast-one", TestContexts.NO_OP);
        writer.write("slow-one", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(counter("destination", TestSinkConfigs.QUEUE_PATH, "recordsSend")).isEqualTo(1);
        assertThat(counter("destination", TestSinkConfigs.QUEUE_PATH, "sendErrors")).isEqualTo(1);
        assertThat(counter("destination", other.toQueuePath(), "recordsSend")).isEqualTo(1);
        assertThat(counter("destination", other.toQueuePath(), "sendErrors")).isZero();
    }

    @Test
    void theProductionPathRegistersOnTheContextsOwnMetricGroup() throws Exception {
        // Everything else here injects the group directly, so this is what pins the one line
        // carrying it from Flink: a writer registering on a group of its own would report nothing
        // any reporter sees, and every other test in this class would still pass. The Pub/Sub sink
        // has the twin of this test, added when its absence was found in review (#208).
        StubWriterInitContext context = new StubWriterInitContext(0);

        SinkWriter<String> writer = TestSinkConfigs.builder().build().createWriter(context);

        assertThat(context.getSinkWriterMetricGroup().hasMetric("inFlightTasks")).isTrue();
        assertThat(context.getSinkWriterMetricGroup().hasMetric("tasksDeduplicated")).isTrue();
        writer.close();
    }

    @Test
    void leavesCurrentSendTimeUnset() throws Exception {
        // Deliberate (#37): a creation may sit parked through several backoffs, so the interval
        // this writer could measure would describe its own retry budget, not the service.
        CloudTasksWriter<String> writer = writer(TestSinkConfigs.builder());

        writer.write("first", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(metrics.getCurrentSendTimeGauge()).isNull();
    }
}
