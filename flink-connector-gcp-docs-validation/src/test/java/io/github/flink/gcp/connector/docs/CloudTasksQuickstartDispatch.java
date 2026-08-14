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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import java.util.Map;

final class CloudTasksQuickstartDispatch {

    private CloudTasksQuickstartDispatch() {}

    static void run() throws Exception {
        // tag::cloud-tasks-quickstart-dispatch[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Not optional: the sink is at-least-once only with checkpointing, which is what makes
        // Flink wait for every outstanding task creation before the barrier passes.
        env.enableCheckpointing(60_000);

        env.fromData("{\"order_id\":\"a-1\"}")
                .sinkTo(
                        CloudTasksSink.<String>builder()
                                .queue(
                                        QueueDestination.of(
                                                "my-project", "asia-northeast1", "webhooks"))
                                .serializer(
                                        CloudTasksSerializationSchema.httpTarget(
                                                        "https://api.example.com/v1/orders")
                                                .withBody(new SimpleStringSchema())
                                                .withHeaders(
                                                        element ->
                                                                Map.of(
                                                                        "Content-Type",
                                                                        "application/json")))
                                .build());

        env.execute("cloudtasks-quickstart");
        // end::cloud-tasks-quickstart-dispatch[]
    }
}
