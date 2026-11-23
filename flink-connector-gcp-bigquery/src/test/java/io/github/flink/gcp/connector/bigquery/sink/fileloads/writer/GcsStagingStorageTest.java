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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Lifecycle and deletion tests for {@link GcsStagingStorage}. */
class GcsStagingStorageTest {

    private static final String BUCKET = "staging-bucket";

    private FakeGcsBatchEndpoint endpoint;
    private GcsStagingStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        endpoint = new FakeGcsBatchEndpoint();
        storage =
                GcsStagingStorage.withClient(
                        StorageOptions.http()
                                .setHost(endpoint.url())
                                .setProjectId("test-project")
                                .setCredentials(NoCredentials.getInstance())
                                .build()
                                .getService());
    }

    @AfterEach
    void tearDown() {
        endpoint.close();
    }

    @Test
    void closingAnInstanceThatNeverBuiltAClientDoesNothing() {
        // Every subtask deserializes an instance whose client is built on first use, and a
        // committer that commits nothing never asks for one. Closing must therefore tolerate the
        // client being absent — Flink calls close() on the writer and the committer either way.
        //
        // This is the whole of what a unit test can say about this close(): on the HTTP transport,
        // closing a real client is the interface's inherited no-op, so there is nothing to observe.
        assertThatCode(new GcsStagingStorage()::close).doesNotThrowAnyException();
    }

    @Test
    void objectsAnEarlierAttemptAlreadyDeletedRaiseNoWarning() {
        // A commit that re-attaches to a load job an earlier attempt completed deletes the staging
        // objects that attempt most likely deleted already, and the service answers 404 for each.
        endpoint.answer("a.avro", 404);
        endpoint.answer("b.avro", 404);
        endpoint.answer("c.avro", 204);

        try (LogCapture logs = LogCapture.of(GcsStagingStorage.class, LogCapture.Level.DEBUG)) {
            storage.deleteObjects(uris("a.avro", "b.avro", "c.avro"));

            assertThat(logs.getMessages())
                    .containsExactly("2 of 3 staging objects were already deleted");
        }
        assertThat(endpoint.deleted()).containsExactly("a.avro", "b.avro", "c.avro");
    }

    @Test
    void aRefusedDeletionStillWarnsAndCarriesItsCause() {
        endpoint.answer("deleted.avro", 204);
        endpoint.answer("missing.avro", 404);
        endpoint.answer("refused.avro", 403);
        endpoint.answer("throttled.avro", 429);

        try (LogCapture warnings = LogCapture.of(GcsStagingStorage.class)) {
            storage.deleteObjects(
                    uris("deleted.avro", "missing.avro", "refused.avro", "throttled.avro"));

            assertThat(warnings.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage())
                                        .isEqualTo(
                                                "Failed to delete 2 of 4 staging objects; a bucket"
                                                        + " lifecycle rule is recommended to"
                                                        + " expire leftovers");
                                assertThat(event.getThrowable())
                                        .isInstanceOfSatisfying(
                                                StorageException.class,
                                                failure ->
                                                        assertThat(failure.getCode())
                                                                .isEqualTo(403));
                            });
        }
    }

    @Test
    void aFailedRequestCountsEveryObjectItCarriedAsFailed() {
        endpoint.failRequest(1, 400);

        try (LogCapture warnings = LogCapture.of(GcsStagingStorage.class)) {
            storage.deleteObjects(uris("a.avro", "b.avro"));

            assertThat(warnings.getMessages())
                    .containsExactly(
                            "Could not complete a batched delete of 2 staging objects",
                            "Failed to delete 2 of 2 staging objects; a bucket lifecycle rule is"
                                    + " recommended to expire leftovers");
            // The request reached the fake and was refused there, not lost on the way to it.
            assertThat(warnings.getEvents().get(0).getThrowable())
                    .isInstanceOfSatisfying(
                            StorageException.class,
                            failure -> assertThat(failure.getCode()).isEqualTo(400));
        }
        assertThat(endpoint.requestSizes()).containsExactly(2);
    }

    @Test
    void anObjectThatCannotBeNamedFailsItsWholeBatchWithoutThrowing() {
        // Nothing is submitted once one URI of a batch cannot be parsed, so every object in it
        // counts as failed: those before the bad URI and the bad URI itself alike.
        List<String> gcsUris = uris("a.avro");
        gcsUris.add("not-a-gcs-uri");

        try (LogCapture warnings = LogCapture.of(GcsStagingStorage.class)) {
            storage.deleteObjects(gcsUris);

            assertThat(warnings.getMessages())
                    .containsExactly(
                            "Could not complete a batched delete of 2 staging objects",
                            "Failed to delete 2 of 2 staging objects; a bucket lifecycle rule is"
                                    + " recommended to expire leftovers");
        }
        assertThat(endpoint.requestSizes()).isEmpty();
    }

    @Test
    void aResponseCutShortAfterEveryOutcomeReportsNoFailedDeletion() {
        // Every part arrives and says its object was not found, then the response ends before
        // its closing boundary. The request fails, but no object failed to be deleted.
        endpoint.answer("a.avro", 404);
        endpoint.answer("b.avro", 404);
        endpoint.truncateResponses();

        try (LogCapture warnings = LogCapture.of(GcsStagingStorage.class)) {
            storage.deleteObjects(uris("a.avro", "b.avro"));

            assertThat(warnings.getMessages())
                    .containsExactly("Could not complete a batched delete of 2 staging objects");
        }
        assertThat(endpoint.deleted()).containsExactly("a.avro", "b.avro");
    }

    @Test
    void aFailedRequestDoesNotStopTheNextOne() {
        // The client splits one batch into requests of 100 itself, but stops at the first request
        // that fails as a whole; the objects in the requests after it must still be deleted.
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            String name = "part-" + i + ".avro";
            names.add(name);
            endpoint.answer(name, 204);
        }
        endpoint.failRequest(1, 400);

        try (LogCapture warnings = LogCapture.of(GcsStagingStorage.class)) {
            storage.deleteObjects(uris(names.toArray(new String[0])));

            assertThat(warnings.getMessages())
                    .containsExactly(
                            "Could not complete a batched delete of 100 staging objects",
                            "Failed to delete 100 of 150 staging objects; a bucket lifecycle rule"
                                    + " is recommended to expire leftovers");
        }
        assertThat(endpoint.requestSizes()).containsExactly(100, 50);
        assertThat(endpoint.deleted()).containsExactlyElementsOf(names.subList(100, 150));
    }

    private static List<String> uris(String... names) {
        List<String> uris = new ArrayList<>(names.length);
        for (String name : names) {
            uris.add("gs://" + BUCKET + "/" + name);
        }
        return uris;
    }

    /**
     * The JSON API's batch endpoint, answering each delete with the status configured for its
     * object. Parts are answered in request order, which is how the client pairs them.
     */
    private static final class FakeGcsBatchEndpoint implements AutoCloseable {

        private static final Pattern DELETE_LINE =
                Pattern.compile("^DELETE \\S*/b/" + BUCKET + "/o/([^?\\s]+)", Pattern.MULTILINE);
        private static final String BOUNDARY = "fake_batch_response";

        private final Map<String, Integer> statuses = new ConcurrentHashMap<>();
        private final List<String> deleted = new CopyOnWriteArrayList<>();
        private final List<Integer> requestSizes = new CopyOnWriteArrayList<>();
        private final Map<Integer, Integer> failedRequests = new ConcurrentHashMap<>();
        private final HttpServer server;
        private volatile boolean truncateResponses;

        FakeGcsBatchEndpoint() throws IOException {
            server =
                    HttpServer.create(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        String url() {
            // The bound address, not "localhost", which may resolve to another loopback address.
            try {
                // URI brackets an IPv6 literal, which a JVM preferring IPv6 binds to.
                return new URI(
                                "http",
                                null,
                                server.getAddress().getAddress().getHostAddress(),
                                server.getAddress().getPort(),
                                null,
                                null,
                                null)
                        .toString();
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        }

        void answer(String objectName, int status) {
            statuses.put(objectName, status);
        }

        /** Ends every response after its last part, without the closing boundary. */
        void truncateResponses() {
            truncateResponses = true;
        }

        /** Refuses the given request as a whole, counting requests from 1. */
        void failRequest(int ordinal, int status) {
            failedRequests.put(ordinal, status);
        }

        List<String> deleted() {
            return deleted;
        }

        List<Integer> requestSizes() {
            return requestSizes;
        }

        private void handle(HttpExchange exchange) throws IOException {
            String request =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            List<String> names = new ArrayList<>();
            Matcher matcher = DELETE_LINE.matcher(request);
            while (matcher.find()) {
                names.add(URLDecoder.decode(matcher.group(1), StandardCharsets.UTF_8));
            }
            requestSizes.add(names.size());
            Integer refusal = failedRequests.get(requestSizes.size());
            if (refusal != null) {
                respond(exchange, refusal, "application/json", error(refusal));
                return;
            }
            StringBuilder response = new StringBuilder();
            for (String name : names) {
                deleted.add(name);
                response.append(part(statuses.getOrDefault(name, 404)));
            }
            if (!truncateResponses) {
                response.append("--").append(BOUNDARY).append("--\r\n");
            }
            respond(exchange, 200, "multipart/mixed; boundary=" + BOUNDARY, response.toString());
        }

        private static String part(int status) {
            String json = status == 204 ? "" : error(status);
            return "--"
                    + BOUNDARY
                    + "\r\nContent-Type: application/http\r\n\r\nHTTP/1.1 "
                    + status
                    + " Fake\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: "
                    + json.getBytes(StandardCharsets.UTF_8).length
                    + "\r\n\r\n"
                    + json
                    + "\r\n";
        }

        private static String error(int status) {
            return "{\"error\":{\"code\":" + status + ",\"message\":\"fake\"}}";
        }

        private static void respond(
                HttpExchange exchange, int status, String contentType, String body)
                throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
