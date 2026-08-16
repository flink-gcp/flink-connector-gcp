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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.connector.sink2.Sink;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.MyEventJsonSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.OrderEvent;

import java.util.Map;

final class CloudTasksConnectorOverview {

    private CloudTasksConnectorOverview() {}

    static Sink<OrderEvent> build() {
        // tag::cloud-tasks-connector-overview[]
        Sink<OrderEvent> sink =
                CloudTasksSink.<OrderEvent>builder()
                        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                        .serializer(
                                CloudTasksSerializationSchema.httpTarget(
                                                "https://api.example.com/v1/orders")
                                        .withBody(new MyEventJsonSerializationSchema())
                                        .withHeaders(
                                                e -> Map.of("Content-Type", "application/json"))
                                        .withOidcToken(
                                                "dispatcher@my-project.iam.gserviceaccount.com"))
                        .build();
        // end::cloud-tasks-connector-overview[]
        return sink;
    }
}
