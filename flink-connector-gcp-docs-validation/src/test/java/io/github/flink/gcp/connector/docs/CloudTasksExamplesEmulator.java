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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

final class CloudTasksExamplesEmulator {

    private CloudTasksExamplesEmulator() {}

    static void build() {
        // tag::cloud-tasks-examples-emulator[]
        CloudTasksSink.<String>builder()
                .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                .serializer(
                        // Not localhost: the emulator dispatches from inside the container, where
                        // that would be the container itself. --add-host above is what makes this
                        // name resolve to the host on Linux; Docker Desktop provides it already.
                        CloudTasksSerializationSchema.httpTarget(
                                        "http://host.docker.internal:9000/orders")
                                .withBody(new SimpleStringSchema()))
                .emulatorEndpoint("localhost:8123")
                .build();
        // end::cloud-tasks-examples-emulator[]
    }
}
