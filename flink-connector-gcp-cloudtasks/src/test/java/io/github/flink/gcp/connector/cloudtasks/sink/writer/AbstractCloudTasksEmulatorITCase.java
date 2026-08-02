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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.ListTasksRequest;
import com.google.cloud.tasks.v2.LocationName;
import com.google.cloud.tasks.v2.Queue;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.RateLimits;
import com.google.cloud.tasks.v2.Task;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.testcontainers.Testcontainers.exposeHostPorts;

/**
 * Shared harness for integration tests against the Cloud Tasks emulator
 * (aertje/cloud-tasks-emulator — Google publishes no official emulator and testcontainers' GCloud
 * module has no Cloud Tasks support, so it runs as a plain {@link GenericContainer}): the
 * container, a harness-owned {@link CloudTasksClient} for queue administration and task inspection,
 * and an HTTP server the emulator dispatches tasks to.
 *
 * <p>Writers under test are wired through the production {@code DefaultTaskCreatorFactory} in its
 * emulator-endpoint mode, so these tests exercise the client construction that ships. They
 * construct {@link CloudTasksWriter} directly rather than through {@code CloudTasksCreateTaskSink},
 * so the serializer's {@code open(...)} is only covered by the tests that run a job.
 *
 * <p>Queues are never created by the sink, so every test creates its own — which also isolates the
 * tests from one another: the emulator keys task names by their full path, so a queue of one's own
 * is a namespace of one's own.
 */
@Testcontainers
@Timeout(180)
abstract class AbstractCloudTasksEmulatorITCase {

    static final String PROJECT = "it-project";

    /** Emulator locations are opaque path segments; no region has to exist. */
    static final String LOCATION = "us-central1";

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static final int EMULATOR_PORT = 8123;

    /** How long a dispatch is waited for; generous, since a passing test does not spend it. */
    private static final Duration DISPATCH_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Dispatches recorded by {@link #RECEIVER}, written from its handler threads and read from test
     * threads. It is never cleared, so tests keep themselves apart by giving every one of them a
     * target path of its own.
     */
    private static final List<RecordedRequest> REQUESTS = new CopyOnWriteArrayList<>();

    /**
     * The target the emulator dispatches tasks to; see {@link #startReceiver()}. It is deliberately
     * never stopped: an {@link HttpServer} cannot be restarted, so stopping it when the first
     * subclass finishes would silently starve the rest wherever they share a JVM — a surefire fork
     * (the #243 root-pom override reuses them) or an IDE run. Surefire exits the fork regardless of
     * the server's non-daemon dispatcher thread, so nothing is left running behind the build.
     */
    private static final HttpServer RECEIVER = startReceiver();

    @Container
    private static final GenericContainer<?> EMULATOR =
            new GenericContainer<>("ghcr.io/aertje/cloud-tasks-emulator:1.2.0")
                    // The emulator binds to localhost by default, which nothing outside the
                    // container could reach.
                    .withCommand("-host", "0.0.0.0", "-port", String.valueOf(EMULATOR_PORT))
                    .withExposedPorts(EMULATOR_PORT)
                    .waitingFor(Wait.forListeningPorts(EMULATOR_PORT));

    private static ManagedChannel channel;
    private static CloudTasksClient client;

    @BeforeAll
    static void createClient() throws IOException {
        channel = ManagedChannelBuilder.forTarget(emulatorEndpoint()).usePlaintext().build();
        client =
                CloudTasksClient.create(
                        CloudTasksSettings.newBuilder()
                                .setTransportChannelProvider(
                                        FixedTransportChannelProvider.create(
                                                GrpcTransportChannel.create(channel)))
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
    }

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    static String emulatorEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getMappedPort(EMULATOR_PORT);
    }

    /**
     * Returns a writer under test wired to the emulator through the production task creator
     * factory. The writer closes its creator, so every writer gets one of its own rather than a
     * shared client.
     */
    static CloudTasksWriter<String> newWriter(CloudTasksSinkBuilder<String> builder)
            throws IOException {
        return new CloudTasksWriter<>(
                TestSinkConfigs.config(builder),
                new DefaultTaskCreatorFactory(emulatorEndpoint()).create(),
                new FakeMailboxExecutor(),
                TestSinkWriterMetricGroup.create());
    }

    /**
     * Writes the records through a writer of its own and flushes, as a checkpoint would. Closing in
     * try-with-resources keeps a failing close from replacing the failure under test.
     */
    static void write(CloudTasksSinkBuilder<String> builder, String... elements) throws Exception {
        try (CloudTasksWriter<String> writer = newWriter(builder)) {
            write(writer, elements);
        }
    }

    /** Writes the records through the given writer and flushes, as a checkpoint would. */
    static void write(CloudTasksWriter<String> writer, String... elements) throws Exception {
        for (String element : elements) {
            writer.write(element, CONTEXT);
        }
        writer.flush(false);
    }

    /** Creates a queue that dispatches its tasks, and returns the destination naming it. */
    static QueueDestination createQueue(String queueId) {
        return createQueue(queueId, false);
    }

    /**
     * Creates a paused queue: it accepts tasks but never dispatches them, so the tasks the sink
     * created stay inspectable through {@link #listTasks(QueueDestination)}. A running queue
     * dispatches immediately and drops completed tasks, which would race every assertion about the
     * task itself.
     */
    static QueueDestination createPausedQueue(String queueId) {
        return createQueue(queueId, true);
    }

    private static QueueDestination createQueue(String queueId, boolean paused) {
        QueueDestination destination = QueueDestination.of(PROJECT, LOCATION, queueId);
        client.createQueue(
                LocationName.of(PROJECT, LOCATION),
                Queue.newBuilder()
                        .setName(destination.toQueuePath())
                        // The emulator runs one dispatch worker per allowed concurrent dispatch and
                        // defaults to 1000 of them; a small cap keeps the container cheap.
                        .setRateLimits(RateLimits.newBuilder().setMaxConcurrentDispatches(10))
                        .build());
        if (paused) {
            client.pauseQueue(QueueName.of(PROJECT, LOCATION, queueId));
        }
        return destination;
    }

    /** Returns the tasks currently held by the queue. */
    static List<Task> listTasks(QueueDestination destination) {
        List<Task> tasks = new ArrayList<>();
        client.listTasks(
                        ListTasksRequest.newBuilder()
                                .setParent(destination.toQueuePath())
                                // The emulator ignores response_view, but Cloud Tasks omits bodies
                                // and headers under the default BASIC view; asking for FULL keeps
                                // these assertions true of the service as well.
                                .setResponseView(Task.View.FULL)
                                .build())
                .iterateAll()
                .forEach(tasks::add);
        return tasks;
    }

    /** Returns the URL of the harness receiver as seen from inside the container. */
    static String targetUrl(String path) {
        return "http://host.testcontainers.internal:" + RECEIVER.getAddress().getPort() + path;
    }

    /**
     * Polls until {@code expected} dispatches to {@code path} have been recorded or the deadline
     * expires, and returns them. Dispatch is asynchronous — a successful {@code CreateTask} only
     * means the queue holds the task — so there is nothing to wait on but the arrival itself.
     */
    static List<RecordedRequest> awaitRequests(String path, int expected)
            throws InterruptedException {
        return await(path, expected, matches -> matches.size() >= expected);
    }

    /**
     * Polls until {@code expected} distinct bodies have been dispatched to {@code path} or the
     * deadline expires, and returns them. Counting distinct bodies is what an at-least-once sink
     * allows: waiting for a raw count would return early on a duplicate and leave a record still in
     * flight.
     */
    static Set<String> awaitDistinctBodies(String path, int expected) throws InterruptedException {
        return distinctBodies(
                await(path, expected, matches -> distinctBodies(matches).size() >= expected));
    }

    private static List<RecordedRequest> await(
            String path, int expected, Predicate<List<RecordedRequest>> done)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + DISPATCH_TIMEOUT.toNanos();
        List<RecordedRequest> matches = requestsTo(path);
        while (!done.test(matches) && System.nanoTime() < deadlineNanos) {
            Thread.sleep(100);
            matches = requestsTo(path);
        }
        return matches;
    }

    private static Set<String> distinctBodies(List<RecordedRequest> requests) {
        return requests.stream().map(request -> request.body).collect(Collectors.toSet());
    }

    private static List<RecordedRequest> requestsTo(String path) {
        return REQUESTS.stream()
                .filter(request -> request.path.equals(path))
                .collect(Collectors.toList());
    }

    /**
     * Starts the HTTP server the emulator dispatches tasks to and publishes its port to the Docker
     * network as {@code host.testcontainers.internal}. Both happen in static initialization because
     * containers join the port-forwarding network when they are created: a {@code @BeforeAll} would
     * run after the testcontainers extension has already started {@link #EMULATOR}.
     */
    private static HttpServer startReceiver() {
        HttpServer server;
        try {
            server =
                    HttpServer.create(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start the task target receiver.", e);
        }
        server.createContext("/", AbstractCloudTasksEmulatorITCase::recordRequest);
        server.start();
        exposeHostPorts(server.getAddress().getPort());
        return server;
    }

    private static void recordRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequestHeaders()
                    .forEach(
                            (name, values) ->
                                    headers.put(name.toLowerCase(Locale.ROOT), values.get(0)));
            REQUESTS.add(
                    new RecordedRequest(
                            exchange.getRequestMethod(),
                            exchange.getRequestURI().getPath(),
                            new String(body, StandardCharsets.UTF_8),
                            headers));
            // Anything but a 2xx makes the queue retry the task, which these tests have no use for.
            exchange.sendResponseHeaders(200, -1);
        }
    }

    /** One task dispatch the emulator delivered to the harness receiver. */
    static final class RecordedRequest {

        final String method;
        final String body;

        private final String path;

        /** Request headers, keyed by lowercase name. */
        private final Map<String, String> headers;

        private RecordedRequest(
                String method, String path, String body, Map<String, String> headers) {
            this.method = method;
            this.path = path;
            this.body = body;
            this.headers = headers;
        }

        /** Returns the header value, or {@code null} when the request carried no such header. */
        String header(String name) {
            return headers.get(name.toLowerCase(Locale.ROOT));
        }
    }
}
