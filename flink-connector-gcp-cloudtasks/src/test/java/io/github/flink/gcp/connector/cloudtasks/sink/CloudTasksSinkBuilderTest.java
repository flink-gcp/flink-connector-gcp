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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloudTasksSinkBuilder}. */
class CloudTasksSinkBuilderTest {

    private static final String SERVICE_ACCOUNT_KEY_FILE =
            "/var/run/secrets/gcp/service-account.json";

    private static final QueueDestination QUEUE =
            QueueDestination.of("my-project", "asia-northeast1", "webhooks");

    private static final CloudTasksSerializationSchema<String> SERIALIZER =
            CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                    .withBody(new SimpleStringSchema());

    @Test
    void buildsASinkWithTheDefaultsWiredThrough() {
        Sink<String> sink =
                CloudTasksSink.<String>builder().queue(QUEUE).serializer(SERIALIZER).build();

        CloudTasksSinkConfig<String> config = config(sink);
        assertThat(config.getDestinationResolver()).isInstanceOf(FixedDestinationResolver.class);
        assertThat(((FixedDestinationResolver) config.getDestinationResolver()).getDestination())
                .isEqualTo(QUEUE);
        assertThat(config.getSerializer()).isSameAs(SERIALIZER);
        assertThat(config.getWriterOptions()).isEqualTo(CloudTasksWriterOptions.defaults());
        // Naming is opt-in: the default is unnamed tasks at full create speed.
        assertThat(config.getTaskIdExtractor()).isNull();
        assertThat(config.getServiceAccountKeyFile()).isNull();
        assertThat(config.getEmulatorEndpoint()).isNull();
    }

    @Test
    void carriesTheOptionalSettings() {
        CloudTasksWriterOptions options =
                CloudTasksWriterOptions.builder().maxInFlightTasks(10).build();
        TaskIdExtractor<String> extractor = element -> element;

        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QUEUE)
                        .serializer(SERIALIZER)
                        .taskIdExtractor(extractor)
                        .writerOptions(options)
                        .emulatorEndpoint("localhost:8123")
                        .build();

        CloudTasksSinkConfig<String> config = config(sink);
        assertThat(config.getTaskIdExtractor()).isSameAs(extractor);
        assertThat(config.getWriterOptions()).isSameAs(options);
        assertThat(config.getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8123"));
    }

    @Test
    void carriesTheServiceAccountKeyFileThroughJobGraphSerialization() throws Exception {
        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QUEUE)
                        .serializer(SERIALIZER)
                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                        .build();

        Sink<String> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink), getClass().getClassLoader());

        assertThat(config(restored).getServiceAccountKeyFile()).isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
    }

    @Test
    void theLastDestinationCallWins() {
        DestinationResolver<String> resolver = (element, context) -> QUEUE;

        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QUEUE)
                        .destinationResolver(resolver)
                        .serializer(SERIALIZER)
                        .build();

        assertThat(config(sink).getDestinationResolver()).isSameAs(resolver);
    }

    @Test
    void requiresASerializer() {
        assertThatThrownBy(() -> CloudTasksSink.<String>builder().queue(QUEUE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void requiresADestination() {
        assertThatThrownBy(() -> CloudTasksSink.<String>builder().serializer(SERIALIZER).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("destination");
    }

    @Test
    void defaultsToFailingTheJobOnAFailedTask() {
        Sink<String> sink =
                CloudTasksSink.<String>builder().queue(QUEUE).serializer(SERIALIZER).build();

        assertThat(config(sink).getFailedTaskHandler()).isSameAs(FailureHandler.failJob());
    }

    @Test
    void carriesTheFailedTaskHandler() {
        FailureHandler<FailedTask> handler = FailureHandler.logAndDrop();

        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QUEUE)
                        .serializer(SERIALIZER)
                        .failedTaskHandler(handler)
                        .build();

        assertThat(config(sink).getFailedTaskHandler()).isSameAs(handler);
    }

    @Test
    void acceptsACrossConnectorHandlerWithoutACast() {
        // The contravariant parameter is the point: one handler written against the shared contract
        // serves every connector in this repository.
        FailureHandler<FailedElement> shared = FailureHandler.logAndDrop();

        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QUEUE)
                        .serializer(SERIALIZER)
                        .failedTaskHandler(shared)
                        .build();

        assertThat(config(sink).getFailedTaskHandler()).isSameAs(shared);
    }

    @Test
    void rejectsNullAndBlankSettings() {
        CloudTasksSinkBuilder<String> builder = CloudTasksSink.builder();

        assertThatThrownBy(() -> builder.queue(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serializer(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.destinationResolver(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.taskIdExtractor(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.writerOptions(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.failedTaskHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("failedTaskHandler must not be null");
        assertThatThrownBy(() -> builder.serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> builder.serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
        assertThatThrownBy(() -> builder.emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class);
        // Parsed at the setter, so a typo fails on the client rather than at connect time; the
        // full parse table is EmulatorEndpointTest's.
        assertThatThrownBy(() -> builder.emulatorEndpoint("localhost8123"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost8123'");
    }

    @Test
    void rejectsAServiceAccountKeyFileFollowedByAnEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                CloudTasksSink.<String>builder()
                                        .queue(QUEUE)
                                        .serializer(SERIALIZER)
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .emulatorEndpoint("localhost:8123")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void rejectsAnEmulatorEndpointFollowedByAServiceAccountKeyFile() {
        assertThatThrownBy(
                        () ->
                                CloudTasksSink.<String>builder()
                                        .queue(QUEUE)
                                        .serializer(SERIALIZER)
                                        .emulatorEndpoint("localhost:8123")
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @SuppressWarnings("unchecked")
    private static CloudTasksSinkConfig<String> config(Sink<String> sink) {
        return ((CloudTasksCreateTaskSink<String>) sink).getConfig();
    }
}
