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

import com.google.cloud.tasks.v2.AppEngineRouting;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.MyEventJsonSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.OrderEvent;

import java.util.Map;

final class CloudTasksConnectorAppEngineTargets {

    private CloudTasksConnectorAppEngineTargets() {}

    static CloudTasksSerializationSchema<OrderEvent> build() {
        // tag::cloud-tasks-connector-app-engine-targets[]
        CloudTasksSerializationSchema<OrderEvent> serializer =
                CloudTasksSerializationSchema.appEngineTarget("/tasks/orders")
                        .withBody(new MyEventJsonSerializationSchema())
                        .withRelativeUri(event -> "/tasks/orders/" + event.orderId())
                        .withHeaders(event -> Map.of("Content-Type", "application/json"))
                        .withRouting(
                                event ->
                                        AppEngineRouting.newBuilder()
                                                .setService("worker")
                                                .setVersion("v2")
                                                .build())
                        .build();
        // end::cloud-tasks-connector-app-engine-targets[]
        return serializer;
    }
}
