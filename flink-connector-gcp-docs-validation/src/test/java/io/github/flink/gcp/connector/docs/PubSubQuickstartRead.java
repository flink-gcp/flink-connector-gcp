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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

final class PubSubQuickstartRead {

    private PubSubQuickstartRead() {}

    static void run() throws Exception {
        // tag::pubsub-quickstart-read[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Also not optional here, and for a sharper reason: the source acknowledges on checkpoint
        // completion, so without checkpointing nothing is ever acknowledged and it stalls once
        // the client library's flow control fills. It fails the job itself after 10 minutes of
        // that rather than hanging quietly.
        env.enableCheckpointing(60_000);

        Source<String, ?, ?> source =
                PubSubSource.<String>builder()
                        .subscription(SubscriptionDestination.of("my-project", "orders-sub"))
                        .deserializer(PubSubDeserializationSchema.payload(new SimpleStringSchema()))
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub").print();

        env.execute("pubsub-source-quickstart");
        // end::pubsub-quickstart-read[]
    }
}
