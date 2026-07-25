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

import io.github.flink.gcp.connector.cloudtasks.sink.createtask.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloudTasksSinkBuilder}. */
class CloudTasksSinkBuilderTest {

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
        assertThat(config.getEmulatorEndpoint()).isEqualTo("localhost:8123");
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
        assertThatThrownBy(() -> builder.emulatorEndpoint("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    private static CloudTasksSinkConfig<String> config(Sink<String> sink) {
        return ((CloudTasksCreateTaskSink<String>) sink).getConfig();
    }
}
