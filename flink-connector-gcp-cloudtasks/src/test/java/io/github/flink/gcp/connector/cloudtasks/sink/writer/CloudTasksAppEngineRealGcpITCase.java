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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.GetTaskRequest;
import com.google.cloud.tasks.v2.ListTasksRequest;
import com.google.cloud.tasks.v2.LocationName;
import com.google.cloud.tasks.v2.Queue;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.RateLimits;
import com.google.cloud.tasks.v2.RetryConfig;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Duration;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-GCP acceptance for Cloud Tasks {@code AppEngineHttpRequest} targets.
 *
 * <p>The emulator cannot dispatch App Engine tasks or apply queue-level App Engine routing. The
 * surrounding lifecycle wrapper starts one manually scaled instance only for this class and exports
 * its exact service, version and instance identifiers. Every test owns a unique queue and deletes
 * it even when an assertion fails.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "CLOUDTASKS_IT_PROJECT", matches = ".+")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class CloudTasksAppEngineRealGcpITCase {

    private static final String LOCATION = "us-central1";
    private static final String QUEUE_PREFIX = "flink-appengine-e2e-";
    private static final java.time.Duration DISPATCH_TIMEOUT = java.time.Duration.ofMinutes(2);
    private static final long POLL_MILLIS = 500;

    private static final Set<QueueName> CREATED_QUEUES = new LinkedHashSet<>();

    private static CloudTasksClient client;
    private static String project;
    private static String service;
    private static String version;
    private static String instance;

    @BeforeAll
    static void createClient() throws IOException {
        project = requiredEnvironment("CLOUDTASKS_IT_PROJECT");
        service = requiredEnvironment("CLOUDTASKS_IT_APPENGINE_SERVICE");
        version = requiredEnvironment("CLOUDTASKS_IT_APPENGINE_VERSION");
        instance = requiredEnvironment("CLOUDTASKS_IT_APPENGINE_INSTANCE");
        client = CloudTasksClient.create();
    }

    @AfterEach
    void deleteTestQueues() {
        deleteTrackedQueues();
    }

    @AfterAll
    static void closeClient() {
        try {
            deleteTrackedQueues();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void fixedRoutingAndRequestFieldsAreStoredOnAPausedQueue() throws Exception {
        QueueDestination queue = createPausedQueue("fixed", null);
        CloudTasksSerializationSchema<String> serializer =
                CloudTasksSerializationSchema.appEngineTarget("/fixed")
                        .withBody(new SimpleStringSchema())
                        .withHeaders(element -> Map.of("X-Test-Record", element))
                        .withRouting(fixtureRouting())
                        .build();

        write(queue, serializer, "fixed-body");

        assertThat(listTasks(queue))
                .singleElement()
                .satisfies(
                        task -> {
                            assertThat(task.getAppEngineHttpRequest().getRelativeUri())
                                    .isEqualTo("/fixed");
                            assertThat(task.getAppEngineHttpRequest().getBody().toStringUtf8())
                                    .isEqualTo("fixed-body");
                            assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                                    .containsEntry("X-Test-Record", "fixed-body");
                            assertRouting(
                                    task.getAppEngineHttpRequest().getAppEngineRouting(),
                                    service,
                                    version,
                                    instance);
                        });
    }

    @Test
    void routingAndRelativeUrisCanBeResolvedPerRecord() throws Exception {
        QueueDestination queue = createPausedQueue("per-record", null);
        CloudTasksSerializationSchema<String> serializer =
                CloudTasksSerializationSchema.appEngineTarget("/unused")
                        .withBody(new SimpleStringSchema())
                        .withRelativeUri(element -> "/per-record/" + element)
                        .withHeaders(element -> Map.of("X-Test-Record", element))
                        .withRouting(element -> element.equals("routed") ? fixtureRouting() : null)
                        .build();

        write(queue, serializer, "routed", "default");

        Map<String, Task> tasksByUri = new LinkedHashMap<>();
        for (Task task : listTasks(queue)) {
            tasksByUri.put(task.getAppEngineHttpRequest().getRelativeUri(), task);
        }
        assertThat(tasksByUri).containsOnlyKeys("/per-record/routed", "/per-record/default");
        assertRouting(
                tasksByUri
                        .get("/per-record/routed")
                        .getAppEngineHttpRequest()
                        .getAppEngineRouting(),
                service,
                version,
                instance);
        AppEngineRouting defaultRouting =
                tasksByUri
                        .get("/per-record/default")
                        .getAppEngineHttpRequest()
                        .getAppEngineRouting();
        // The service populates the output-only host even when the task did not
        // select a service, version or instance. The three input selectors are
        // therefore the stable evidence that the extractor returned null.
        assertRouting(defaultRouting, "", "", "");
        assertThat(defaultRouting.getHost()).isNotBlank();
    }

    @Test
    void queueLevelRoutingOverrideDispatchesAStoredTask() throws Exception {
        QueueDestination queue = createPausedQueue("override", fixtureRouting());
        AppEngineRouting taskRouting =
                AppEngineRouting.newBuilder()
                        .setService("must-not-route")
                        .setVersion("missing-version")
                        .setInstance("missing-instance")
                        .build();
        CloudTasksSerializationSchema<String> serializer =
                CloudTasksSerializationSchema.appEngineTarget("/accepted")
                        .withBody(new SimpleStringSchema())
                        .withRouting(taskRouting)
                        .build();

        write(queue, serializer, "override-body");

        QueueName queueName = QueueName.of(project, LOCATION, queue.getQueue());
        Queue storedQueue = client.getQueue(queueName);
        assertRouting(storedQueue.getAppEngineRoutingOverride(), service, version, instance);
        Task storedTask = onlyTask(queue);
        assertRouting(
                storedTask.getAppEngineHttpRequest().getAppEngineRouting(),
                "must-not-route",
                "missing-version",
                "missing-instance");

        client.resumeQueue(queueName);
        awaitTaskDeletion(storedTask.getName());
    }

    @Test
    void unavailableResponseProvesTheHandlerWasAttempted() throws Exception {
        QueueDestination queue = createPausedQueue("unavailable", null);
        write(queue, serializer("/unavailable"), "unavailable-body");
        Task storedTask = onlyTask(queue);

        QueueName queueName = QueueName.of(project, LOCATION, queue.getQueue());
        client.resumeQueue(queueName);
        Task attempted = awaitCompletedAttempt(storedTask.getName());
        client.pauseQueue(queueName);

        assertThat(attempted.getLastAttempt().getResponseStatus().getCode()).isNotZero();
    }

    @Test
    void redirectResponseIsNotFollowed() throws Exception {
        QueueDestination queue = createPausedQueue("redirect", null);
        write(queue, serializer("/redirect"), "redirect-body");
        Task storedTask = onlyTask(queue);

        QueueName queueName = QueueName.of(project, LOCATION, queue.getQueue());
        client.resumeQueue(queueName);
        Task attempted = awaitCompletedAttempt(storedTask.getName());
        client.pauseQueue(queueName);

        assertThat(attempted.getLastAttempt().getResponseStatus().getCode()).isNotZero();
        assertThat(attempted.getName()).isEqualTo(storedTask.getName());
    }

    private static CloudTasksSerializationSchema<String> serializer(String relativeUri) {
        return CloudTasksSerializationSchema.appEngineTarget(relativeUri)
                .withBody(new SimpleStringSchema())
                .withRouting(fixtureRouting())
                .build();
    }

    private static AppEngineRouting fixtureRouting() {
        return AppEngineRouting.newBuilder()
                .setService(service)
                .setVersion(version)
                .setInstance(instance)
                .build();
    }

    private static QueueDestination createPausedQueue(String purpose, AppEngineRouting override) {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String queueId = QUEUE_PREFIX + purpose + "-" + suffix;
        QueueDestination destination = QueueDestination.of(project, LOCATION, queueId);
        QueueName name = QueueName.of(project, LOCATION, queueId);
        CREATED_QUEUES.add(name);

        Queue.Builder queue =
                Queue.newBuilder()
                        .setName(destination.toQueuePath())
                        .setRateLimits(RateLimits.newBuilder().setMaxConcurrentDispatches(1))
                        .setRetryConfig(
                                RetryConfig.newBuilder()
                                        .setMaxAttempts(5)
                                        .setMinBackoff(Duration.newBuilder().setSeconds(60))
                                        .setMaxBackoff(Duration.newBuilder().setSeconds(60)));
        if (override != null) {
            queue.setAppEngineRoutingOverride(override);
        }
        client.createQueue(LocationName.of(project, LOCATION), queue.build());
        client.pauseQueue(name);
        return destination;
    }

    private static void write(
            QueueDestination queue,
            CloudTasksSerializationSchema<String> serializer,
            String... records)
            throws Exception {
        CloudTasksSinkBuilder<String> builder =
                CloudTasksSink.<String>builder().queue(queue).serializer(serializer);
        try (CloudTasksWriter<String> writer =
                new CloudTasksWriter<>(
                        TestSinkConfigs.config(builder),
                        new DefaultTaskCreatorFactory().create(),
                        new FakeMailboxExecutor(),
                        TestSinkWriterMetricGroup.create())) {
            for (String record : records) {
                writer.write(record, TestContexts.NO_OP);
            }
            writer.flush(false);
        }
    }

    private static Task onlyTask(QueueDestination queue) {
        List<Task> tasks = listTasks(queue);
        assertThat(tasks).singleElement();
        return tasks.get(0);
    }

    private static List<Task> listTasks(QueueDestination queue) {
        List<Task> tasks = new ArrayList<>();
        client.listTasks(
                        ListTasksRequest.newBuilder()
                                .setParent(queue.toQueuePath())
                                .setResponseView(Task.View.FULL)
                                .build())
                .iterateAll()
                .forEach(tasks::add);
        return tasks;
    }

    private static Task awaitCompletedAttempt(String taskName) throws InterruptedException {
        long deadline = System.nanoTime() + DISPATCH_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                Task task = getTask(taskName);
                if (task.hasLastAttempt() && task.getLastAttempt().hasResponseTime()) {
                    return task;
                }
            } catch (ApiException e) {
                if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
                    throw new AssertionError(
                            "The task completed before a failed attempt could be inspected; the"
                                    + " handler response may have been treated as success.",
                            e);
                }
                throw e;
            }
            Thread.sleep(POLL_MILLIS);
        }
        throw new AssertionError("Timed out waiting for a completed App Engine task attempt.");
    }

    private static void awaitTaskDeletion(String taskName) throws InterruptedException {
        long deadline = System.nanoTime() + DISPATCH_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                getTask(taskName);
            } catch (ApiException e) {
                if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
                    return;
                }
                throw e;
            }
            Thread.sleep(POLL_MILLIS);
        }
        throw new AssertionError("Timed out waiting for a successful App Engine task dispatch.");
    }

    private static Task getTask(String taskName) {
        return client.getTask(
                GetTaskRequest.newBuilder()
                        .setName(taskName)
                        .setResponseView(Task.View.FULL)
                        .build());
    }

    private static void assertRouting(
            AppEngineRouting routing,
            String expectedService,
            String expectedVersion,
            String expectedInstance) {
        assertThat(routing.getService()).isEqualTo(expectedService);
        assertThat(routing.getVersion()).isEqualTo(expectedVersion);
        assertThat(routing.getInstance()).isEqualTo(expectedInstance);
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    name + " is not set; run this class through the App Engine E2E lifecycle.");
        }
        return value;
    }

    private static void deleteTrackedQueues() {
        if (client == null) {
            return;
        }
        RuntimeException failure = null;
        for (QueueName queue : List.copyOf(CREATED_QUEUES)) {
            try {
                client.deleteQueue(queue);
                CREATED_QUEUES.remove(queue);
            } catch (ApiException e) {
                if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
                    CREATED_QUEUES.remove(queue);
                } else if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
