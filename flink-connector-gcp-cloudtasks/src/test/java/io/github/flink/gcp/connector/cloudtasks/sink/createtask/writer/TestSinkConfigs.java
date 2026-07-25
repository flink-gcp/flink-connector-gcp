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

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.createtask.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

/**
 * Builds writer test configs through the public sink builder (keeping the build path covered), so
 * builder/config signature changes touch one place instead of every writer test class.
 */
final class TestSinkConfigs {

    static final QueueDestination QUEUE =
            QueueDestination.of("my-project", "asia-northeast1", "webhooks");

    static final String QUEUE_PATH =
            "projects/my-project/locations/asia-northeast1/queues/webhooks";

    /** SHA-256 of {@code order-1}, the id the writer must derive rather than use the key itself. */
    static final String ORDER_1_DIGEST =
            "0bafe22156d2698c143b86040446d366ead863ba600d5c924f3d15c786ef4057";

    /** A serializer posting the record itself as the body of a fixed HTTP target. */
    static CloudTasksSerializationSchema<String> serializer() {
        return CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                .withBody(new SimpleStringSchema());
    }

    /** A builder writing to {@link #QUEUE} with {@link #serializer()}, ready to be extended. */
    static CloudTasksSinkBuilder<String> builder() {
        return builder(QUEUE);
    }

    /** A builder writing to the given queue with {@link #serializer()}. */
    static CloudTasksSinkBuilder<String> builder(QueueDestination queue) {
        return builder(queue, serializer());
    }

    /** A builder writing to the given queue with the given serializer. */
    static CloudTasksSinkBuilder<String> builder(
            QueueDestination queue, CloudTasksSerializationSchema<String> serializer) {
        return CloudTasksSink.<String>builder().queue(queue).serializer(serializer);
    }

    static CloudTasksSinkConfig<String> config(CloudTasksSinkBuilder<String> builder) {
        return ((CloudTasksCreateTaskSink<String>) builder.build()).getConfig();
    }

    private TestSinkConfigs() {}
}
