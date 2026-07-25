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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

    private static final Duration DISPATCH_TIMEOUT = Duration.ofSeconds(60);

    @Test
    void postDispatchesTheRecordAsTheBodyWithItsHeaders() throws Exception {
        QueueDestination queue = createQueue("dispatch-post");
        CloudTasksSinkConfig<String> config =
                config(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/post"))
                                .withBody(new SimpleStringSchema())
                                .withHeaders(
                                        element -> {
                                            Map<String, String> headers = new HashMap<>();
                                            headers.put("Content-Type", "application/json");
                                            headers.put("X-Order", element);
                                            return headers;
                                        }));

        write(config, "order-1");

        List<RecordedRequest> requests = awaitRequests("/post", 1, DISPATCH_TIMEOUT);
        assertThat(requests).hasSize(1);
        RecordedRequest request = requests.get(0);
        assertThat(request.method).isEqualTo("POST");
        assertThat(request.body).isEqualTo("order-1");
        assertThat(request.header("Content-Type")).isEqualTo("application/json");
        assertThat(request.header("X-Order")).isEqualTo("order-1");
        // Cloud Tasks stamps its own headers onto every dispatch; their presence is what tells the
        // handler the request came from a queue rather than from a client.
        assertThat(request.header("X-CloudTasks-QueueName")).isEqualTo("dispatch-post");
    }

    @Test
    void getDispatchesWithoutABody() throws Exception {
        QueueDestination queue = createQueue("dispatch-get");
        CloudTasksSinkConfig<String> config =
                config(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/get"))
                                .withBody(new SimpleStringSchema())
                                .withMethod(HttpMethod.GET));

        write(config, "order-2");

        List<RecordedRequest> requests = awaitRequests("/get", 1, DISPATCH_TIMEOUT);
        assertThat(requests).hasSize(1);
        // The body schema is what binds the record type, so it cannot be left out; Cloud Tasks
        // rejects a task carrying a body under a method that forbids one, so the sink drops it.
        assertThat(requests.get(0).method).isEqualTo("GET");
        assertThat(requests.get(0).body).isEmpty();
    }

    @Test
    void perRecordUrlsFanOutToDifferentPaths() throws Exception {
        QueueDestination queue = createQueue("dispatch-fanout");
        String prefix = targetUrl("/fanout/");
        CloudTasksSinkConfig<String> config =
                config(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(prefix)
                                .withBody(new SimpleStringSchema())
                                .withUrl(element -> prefix + element));

        write(config, "a", "b");

        assertThat(awaitRequests("/fanout/a", 1, DISPATCH_TIMEOUT))
                .singleElement()
                .satisfies(request -> assertThat(request.body).isEqualTo("a"));
        assertThat(awaitRequests("/fanout/b", 1, DISPATCH_TIMEOUT))
                .singleElement()
                .satisfies(request -> assertThat(request.body).isEqualTo("b"));
    }

    @Test
    void oidcTokensArriveAsABearerJwtForTheConfiguredServiceAccount() throws Exception {
        QueueDestination queue = createQueue("dispatch-oidc");
        CloudTasksSinkConfig<String> config =
                config(
                        queue,
                        CloudTasksSerializationSchema.httpTarget(targetUrl("/oidc"))
                                .withBody(new SimpleStringSchema())
                                .withOidcToken(
                                        "dispatcher@it-project.iam.gserviceaccount.com",
                                        "https://api.example.com"));

        write(config, "order-3");

        List<RecordedRequest> requests = awaitRequests("/oidc", 1, DISPATCH_TIMEOUT);
        assertThat(requests).hasSize(1);
        String authorization = requests.get(0).header("Authorization");
        assertThat(authorization).startsWith("Bearer ");
        // The emulator signs its own tokens with a throwaway key, so the signature says nothing;
        // the claims are what prove the service account and audience reached the token minter.
        assertThat(claims(authorization))
                .contains("\"email\":\"dispatcher@it-project.iam.gserviceaccount.com\"")
                .contains("\"aud\":\"https://api.example.com\"");
    }

    /** Returns the decoded payload of the {@code Bearer <jwt>} authorization header. */
    private static String claims(String authorization) {
        String jwt = authorization.substring("Bearer ".length());
        String payload = jwt.split("\\.")[1];
        return new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
    }

    private static CloudTasksSinkConfig<String> config(
            QueueDestination queue, CloudTasksSerializationSchema<String> serializer) {
        return TestSinkConfigs.config(
                CloudTasksSink.<String>builder().queue(queue).serializer(serializer));
    }

    /** Writes the records through a writer of its own and flushes, as a checkpoint would. */
    private static void write(CloudTasksSinkConfig<String> config, String... elements)
            throws Exception {
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
