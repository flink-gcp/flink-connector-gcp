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

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for the tasks the sink creates, against the Cloud Tasks emulator. Unlike the
 * unit tests, which script an in-memory {@code TaskCreator}, these run against a real v2 server:
 * the tasks are read back from the service, and its status codes are classified by the gax client
 * the sink ships with.
 *
 * <p>The queues here are paused, so they hold the tasks instead of dispatching them — a running
 * queue drops a task as soon as it completes, which would race every assertion about the task
 * itself. What the target receives when a queue does dispatch is {@link CloudTasksDispatchITCase}.
 */
class CloudTasksTaskCreationITCase extends AbstractCloudTasksEmulatorITCase {

    @Test
    void unnamedTasksAreCreatedPerRecordWithoutDeduplication() throws Exception {
        QueueDestination queue = createPausedQueue("create-unnamed");

        // The same record twice is the replay a restart would produce.
        write(TestSinkConfigs.builder(queue), "first", "first");

        // Two tasks, so the endpoint will be called twice: without a taskIdExtractor there is no
        // id for Cloud Tasks to recognise a replay by.
        assertThat(bodies(listTasks(queue))).containsExactlyInAnyOrder("first", "first");
    }

    @Test
    void namedTasksCarryTheHashedKeyAndDeduplicateReplaysAcrossFlushes() throws Exception {
        QueueDestination queue = createPausedQueue("create-named");

        try (CloudTasksWriter<String> writer =
                newWriter(TestSinkConfigs.builder(queue).taskIdExtractor(element -> element))) {
            write(writer, "order-1");
            // The replay goes through a second write/flush cycle rather than a second record in the
            // same one, so its create provably starts after the first task is stored: the emulator
            // checks a task name and stores it under separate locks, so two concurrent creates of
            // one name can both succeed and the test would never reach ALREADY_EXISTS. Reusing the
            // writer also covers the cycles a streaming job puts it through.
            write(writer, "order-1");
        }

        // ALREADY_EXISTS is the deduplication naming asked for, so the sink counts it as success
        // and the job survives the replay.
        assertThat(listTasks(queue))
                .singleElement()
                .satisfies(
                        task -> {
                            assertThat(task.getName())
                                    .isEqualTo(
                                            queue.toQueuePath()
                                                    + "/tasks/"
                                                    + TestSinkConfigs.ORDER_1_DIGEST);
                            assertThat(task.getHttpRequest().getBody().toStringUtf8())
                                    .isEqualTo("order-1");
                        });
    }

    @Test
    void taskNamesAreScopedToTheirQueue() throws Exception {
        QueueDestination first = createPausedQueue("create-split-a");
        QueueDestination second = createPausedQueue("create-split-b");
        CloudTasksSinkBuilder<String> builder =
                CloudTasksSink.<String>builder()
                        .serializer(TestSinkConfigs.serializer())
                        // One key for both records, so only the queue tells their tasks apart.
                        .taskIdExtractor(element -> "shared-key")
                        .destinationResolver(
                                (element, context) -> element.equals("a") ? first : second);

        write(builder, "a", "b");

        // The same key in two queues is not a duplicate: the name a task is deduplicated by is
        // composed from the resolved queue as well as the hashed key, so both records survive.
        assertThat(bodies(listTasks(first))).containsExactly("a");
        assertThat(bodies(listTasks(second))).containsExactly("b");
        String firstName = listTasks(first).get(0).getName();
        String secondName = listTasks(second).get(0).getName();
        assertThat(firstName).startsWith(first.toQueuePath() + "/tasks/");
        assertThat(secondName).startsWith(second.toQueuePath() + "/tasks/");
        assertThat(taskId(firstName)).isEqualTo(taskId(secondName));
    }

    private static String taskId(String taskName) {
        return taskName.substring(taskName.lastIndexOf('/') + 1);
    }

    @Test
    void flushWaitsForEveryOutstandingCreation() throws Exception {
        QueueDestination queue = createPausedQueue("create-flush");
        String[] records = new String[50];
        for (int index = 0; index < records.length; index++) {
            records[index] = "record-" + index;
        }
        // A cap well below the record count forces the writer to yield for completions mid-write,
        // so the flush has to cope with creations both in flight and already done.
        try (CloudTasksWriter<String> writer =
                newWriter(
                        TestSinkConfigs.builder(queue)
                                .writerOptions(
                                        CloudTasksWriterOptions.builder()
                                                .maxInFlightTasks(5)
                                                .build()))) {
            for (String record : records) {
                writer.write(record, TestContexts.NO_OP);
                assertThat(writer.getInFlightTasks() + writer.getParkedTasks())
                        .isLessThanOrEqualTo(5);
            }
            writer.flush(false);

            assertThat(writer.getInFlightTasks()).isZero();
            assertThat(writer.getParkedTasks()).isZero();
            // No polling: a returned flush is the sink's promise that Cloud Tasks holds every
            // record written before it, which is what makes the checkpoint meaningful.
            assertThat(bodies(listTasks(queue))).containsExactlyInAnyOrder(records);
        }
    }

    @Test
    void aMissingQueueFailsTheJobAfterTheShortNotFoundBudget() throws Exception {
        // The sink never creates queues, so a queue that does not exist is a configuration error;
        // it gets a budget of its own so a mistyped name cannot burn the full retry budget per
        // record. This is also the only test that runs the park-and-re-dispatch loop on the real
        // clock — the unit tests drive it through an injected time source.
        QueueDestination missing = QueueDestination.of(PROJECT, LOCATION, "create-missing");
        CloudTasksSinkBuilder<String> builder =
                TestSinkConfigs.builder(missing)
                        .writerOptions(
                                CloudTasksWriterOptions.builder()
                                        // Distinguishable budgets: were NOT_FOUND classified onto
                                        // the transient one, the attempt count below would differ.
                                        .recoveryMaxAttempts(20)
                                        .notFoundRecoveryMaxAttempts(2)
                                        .notFoundRecoveryInitialBackoff(Duration.ofMillis(500))
                                        .build());

        long startedAt = System.nanoTime();
        assertThatThrownBy(() -> write(builder, "order-1"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND after 2 attempt(s)")
                .hasMessageContaining("the queue does not exist");
        // One backoff was waited out between the two attempts, so the creation really was parked
        // and re-dispatched rather than failed on the spot. The floor is the 500 ms initial
        // backoff less the ±25% jitter, not the 500 ms itself.
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .isGreaterThanOrEqualTo(Duration.ofMillis(375));
    }

    private static List<String> bodies(List<Task> tasks) {
        return tasks.stream()
                .map(task -> task.getHttpRequest().getBody().toStringUtf8())
                .collect(Collectors.toList());
    }
}
