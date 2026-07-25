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

package io.github.flink.gcp.connector.cloudtasks.sink.createtask.writer;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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

    /** SHA-256 of {@code order-1}, the id the writer must derive rather than use the key itself. */
    private static final String ORDER_1_DIGEST =
            "0bafe22156d2698c143b86040446d366ead863ba600d5c924f3d15c786ef4057";

    @Test
    void unnamedTasksAreCreatedPerRecordWithoutDeduplication() throws Exception {
        QueueDestination queue = createPausedQueue("create-unnamed");

        // The same record twice is the replay a restart would produce.
        write(builder(queue), "first", "first");

        List<Task> tasks = listTasks(queue);
        assertThat(bodies(tasks)).containsExactlyInAnyOrder("first", "first");
        // Cloud Tasks names an unnamed task itself, and those names are unique per task — which is
        // exactly why a replayed record calls the endpoint twice without a taskIdExtractor.
        assertThat(tasks.stream().map(Task::getName).distinct()).hasSize(2);
    }

    @Test
    void namedTasksCarryTheHashedKeyAndDeduplicateReplays() throws Exception {
        QueueDestination queue = createPausedQueue("create-named");

        // The second write is the replay a restart would produce: Cloud Tasks answers
        // ALREADY_EXISTS, which is the deduplication naming asked for and therefore not a failure.
        write(builder(queue).taskIdExtractor(element -> element), "order-1", "order-1");

        assertThat(listTasks(queue))
                .singleElement()
                .satisfies(
                        task ->
                                assertThat(task.getName())
                                        .isEqualTo(
                                                queue.toQueuePath() + "/tasks/" + ORDER_1_DIGEST));
    }

    @Test
    void perRecordDestinationsSplitTasksAcrossQueues() throws Exception {
        QueueDestination first = createPausedQueue("create-split-a");
        QueueDestination second = createPausedQueue("create-split-b");
        CloudTasksSinkBuilder<String> builder =
                CloudTasksSink.<String>builder()
                        .serializer(TestSinkConfigs.serializer())
                        .destinationResolver(
                                (element, context) -> element.startsWith("a") ? first : second);

        write(builder, "a-1", "b-1", "b-2");

        assertThat(bodies(listTasks(first))).containsExactly("a-1");
        assertThat(bodies(listTasks(second))).containsExactlyInAnyOrder("b-1", "b-2");
    }

    @Test
    void flushWaitsForEveryOutstandingCreation() throws Exception {
        QueueDestination queue = createPausedQueue("create-flush");
        String[] records = new String[50];
        for (int i = 0; i < records.length; i++) {
            records[i] = "record-" + i;
        }
        // A cap well below the record count forces the writer to yield for completions mid-write,
        // so the flush has to cope with creations both in flight and already done.
        CloudTasksSinkBuilder<String> builder =
                builder(queue)
                        .writerOptions(
                                CloudTasksWriterOptions.builder().maxInFlightTasks(5).build());

        write(builder, records);

        // No polling: a returned flush is the sink's promise that Cloud Tasks holds every record
        // written before it, which is what makes the checkpoint meaningful.
        assertThat(bodies(listTasks(queue))).containsExactlyInAnyOrder(records);
    }

    @Test
    void aMissingQueueFailsTheJobAfterTheShortNotFoundBudget() throws Exception {
        // The sink never creates queues, so a queue that does not exist is a configuration error;
        // it gets a budget of its own so a mistyped name cannot burn the full retry budget per
        // record.
        QueueDestination missing = QueueDestination.of(PROJECT, LOCATION, "create-missing");

        assertThatThrownBy(() -> write(builder(missing), "order-1"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND");
    }

    private static CloudTasksSinkBuilder<String> builder(QueueDestination queue) {
        return CloudTasksSink.<String>builder()
                .queue(queue)
                .serializer(TestSinkConfigs.serializer());
    }

    private static List<String> bodies(List<Task> tasks) {
        return tasks.stream()
                .map(task -> task.getHttpRequest().getBody().toStringUtf8())
                .collect(Collectors.toList());
    }

    /** Writes the records through a writer of its own and flushes, as a checkpoint would. */
    private static void write(CloudTasksSinkBuilder<String> builder, String... elements)
            throws Exception {
        CloudTasksSinkConfig<String> config = TestSinkConfigs.config(builder);
        CloudTasksWriter<String> writer = newWriter(config, new FakeMailboxExecutor());
        try {
            for (String element : elements) {
                writer.write(element, CONTEXT);
            }
            writer.flush(false);
        } finally {
            writer.close();
        }
    }
}
