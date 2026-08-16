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

import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.MyEventJsonSerializationSchema;
import io.github.flink.gcp.connector.docs.CloudTasksDocumentationTypes.OrderEvent;

import java.util.Map;

final class JavadocCloudTasksExamples {

    private JavadocCloudTasksExamples() {}

    static Sink<OrderEvent> sink() {
        // tag::sink[]
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
        // end::sink[]
        return sink;
    }

    static void httpTarget() {
        // tag::http-target[]
        CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                .withBody(new MyEventJsonSerializationSchema())
                .withHeaders(e -> Map.of("Content-Type", "application/json"))
                .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com");
        // end::http-target[]
    }

    static void appEngineTarget() {
        // tag::app-engine-target[]
        CloudTasksSerializationSchema.appEngineTarget("/tasks/orders")
                .withBody(new MyEventJsonSerializationSchema())
                .withRouting(AppEngineRouting.newBuilder().setService("worker").build())
                .build();
        // end::app-engine-target[]
    }

    static void detailedAppEngineTarget() {
        // tag::detailed-app-engine-target[]
        CloudTasksSerializationSchema.appEngineTarget("/tasks/orders")
                .withBody(new MyEventJsonSerializationSchema())
                .withMethod(HttpMethod.POST)
                .withRelativeUri(e -> "/tasks/orders/" + e.orderId())
                .withRouting(
                        AppEngineRouting.newBuilder().setService("worker").setVersion("v2").build())
                .build();
        // end::detailed-app-engine-target[]
    }

    static void detailedHttpTarget() {
        // tag::detailed-http-target[]
        CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                .withBody(new MyEventJsonSerializationSchema())
                .withMethod(HttpMethod.POST)
                .withUrl(e -> "https://api.example.com/v1/orders/" + e.orderId())
                .withHeaders(e -> Map.of("Content-Type", "application/json"))
                .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com");
        // end::detailed-http-target[]
    }
}
