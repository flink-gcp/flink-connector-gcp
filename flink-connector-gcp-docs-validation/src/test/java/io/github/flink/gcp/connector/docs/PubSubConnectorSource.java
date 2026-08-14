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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import java.time.Duration;

final class PubSubConnectorSource {

    private PubSubConnectorSource() {}

    static Source<String, ?, ?> build(StreamExecutionEnvironment env) {
        // tag::pubsub-connector-source[]
        Source<String, ?, ?> source =
                PubSubSource.<String>builder()
                        .subscriptions(
                                SubscriptionDestination.of("my-project", "orders"),
                                SubscriptionDestination.of("my-project", "returns"))
                        .deserializationSchema(
                                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                        .subscriberOptions(
                                PubSubSubscriberOptions.builder()
                                        .flowControlMaxOutstandingElementCount(5_000)
                                        .maxAckExtensionPeriod(Duration.ofMinutes(30))
                                        .build())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub");
        // end::pubsub-connector-source[]
        return source;
    }
}
