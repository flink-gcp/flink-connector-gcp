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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for what the target endpoint actually receives, against the Cloud Tasks
 * emulator. The emulator dispatches tasks over real HTTP, so these tests are what distinguishes a
 * task the service accepted from a task that arrives as intended: method, body, headers and OIDC
 * authorization all come back from the wire rather than from the request the sink built.
 *
 * <p>Every test uses a queue and a target path of its own, so the recorded dispatches of one test
 * cannot be mistaken for another's.
 */
class CloudTasksDispatchITCase extends AbstractCloudTasksEmulatorITCase {

    @Test
    void postDispatchesTheRecordAsTheBodyWithItsHeaders() throws Exception {
        QueueDestination queue = createQueue("dispatch-post");

        write(
                TestSinkConfigs.builder(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/post"))
                                .withBody(new SimpleStringSchema())
                                .withHeaders(
                                        element -> {
                                            Map<String, String> headers = new HashMap<>();
                                            headers.put("Content-Type", "application/json");
                                            headers.put("X-Order", element);
                                            return headers;
                                        })),
                "order-1");

        List<RecordedRequest> requests = awaitRequests("/post", 1);
        assertThat(requests).hasSize(1);
        RecordedRequest request = requests.get(0);
        assertThat(request.method).isEqualTo("POST");
        assertThat(request.body).isEqualTo("order-1");
        assertThat(request.header("Content-Type")).isEqualTo("application/json");
        assertThat(request.header("X-Order")).isEqualTo("order-1");
        // Not a sink behaviour, but a cheap check that the request really came through a queue
        // rather than straight from the harness: Cloud Tasks stamps its own headers onto dispatch.
        assertThat(request.header("X-CloudTasks-QueueName")).isEqualTo("dispatch-post");
    }

    @Test
    void theBodyIsSentOnlyUnderMethodsThatAllowOne() throws Exception {
        QueueDestination queue = createQueue("dispatch-methods");

        write(methodTarget(queue, HttpMethod.PUT, "/put"), "order-2");
        write(methodTarget(queue, HttpMethod.GET, "/get"), "order-3");

        // The body schema is what binds the record type, so it cannot be left out; Cloud Tasks
        // rejects a task carrying a body under a method that forbids one, so the sink drops it
        // there and keeps it everywhere it is allowed.
        assertThat(awaitRequests("/put", 1))
                .singleElement()
                .satisfies(
                        request -> {
                            assertThat(request.method).isEqualTo("PUT");
                            assertThat(request.body).isEqualTo("order-2");
                        });
        assertThat(awaitRequests("/get", 1))
                .singleElement()
                .satisfies(
                        request -> {
                            assertThat(request.method).isEqualTo("GET");
                            assertThat(request.body).isEmpty();
                        });
    }

    @Test
    void perRecordUrlsFanOutToDifferentPaths() throws Exception {
        QueueDestination queue = createQueue("dispatch-fanout");
        String prefix = targetUrl("/fanout/");

        write(
                TestSinkConfigs.builder(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(prefix)
                                .withBody(new SimpleStringSchema())
                                .withUrl(element -> prefix + element)),
                "a",
                "b");

        assertThat(awaitRequests("/fanout/a", 1))
                .singleElement()
                .satisfies(request -> assertThat(request.body).isEqualTo("a"));
        assertThat(awaitRequests("/fanout/b", 1))
                .singleElement()
                .satisfies(request -> assertThat(request.body).isEqualTo("b"));
    }

    @Test
    void oidcTokensArriveAsABearerJwtForTheConfiguredServiceAccount() throws Exception {
        QueueDestination queue = createQueue("dispatch-oidc");
        String serviceAccount = "dispatcher@it-project.iam.gserviceaccount.com";

        write(
                TestSinkConfigs.builder(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/oidc"))
                                .withBody(new SimpleStringSchema())
                                .withOidcToken(serviceAccount, "https://api.example.com")),
                "order-4");
        write(
                TestSinkConfigs.builder(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/oidc-default"))
                                .withBody(new SimpleStringSchema())
                                .withOidcToken(serviceAccount)),
                "order-5");

        // The emulator signs its own tokens with a throwaway key, so the signature says nothing;
        // the claims are what prove the service account and audience reached the token minter.
        assertClaims(awaitRequests("/oidc", 1), serviceAccount, "https://api.example.com");
        // Left unset, the audience defaults to the target URL — a service behaviour, not the
        // sink's, and the reason the builder can leave it out.
        assertClaims(awaitRequests("/oidc-default", 1), serviceAccount, targetUrl("/oidc-default"));
    }

    private static CloudTasksSinkBuilder<String> methodTarget(
            QueueDestination queue, HttpMethod method, String path) {
        return TestSinkConfigs.builder(
                queue,
                CloudTasksSerializationSchema.httpTarget(targetUrl(path))
                        .withBody(new SimpleStringSchema())
                        .withMethod(method));
    }

    private static void assertClaims(
            List<RecordedRequest> requests, String serviceAccount, String audience)
            throws IOException {
        assertThat(requests).hasSize(1);
        String authorization = requests.get(0).header("Authorization");
        assertThat(authorization).startsWith("Bearer ");
        String payload = authorization.substring("Bearer ".length()).split("\\.")[1];
        ObjectMapper mapper = new ObjectMapper();
        JsonNode claims = mapper.readTree(Base64.getUrlDecoder().decode(payload));
        assertThat(claims.path("email")).isEqualTo(mapper.valueToTree(serviceAccount));
        // RFC 7519 section 4.1.3 permits a single audience as a string or an array.
        assertThat(claims.path("aud"))
                .isIn(mapper.valueToTree(audience), mapper.valueToTree(List.of(audience)));
    }
}
