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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

final class DynamicDestinationsPubSubTopics {

    private DynamicDestinationsPubSubTopics() {}

    static void build(StreamExecutionEnvironment env, Source<OrderEvent, ?, ?> source) {
        // tag::pubsub-topics[]
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
                .keyBy(OrderEvent::customerId)
                .sinkTo(
                        PubSubSink.<OrderEvent>builder()
                                .destinationResolver(
                                        (event, context) ->
                                                TopicDestination.of("my-project", event.region()))
                                .serializer(
                                        PubSubSerializationSchema.dataOnly(new OrderEventSchema())
                                                .withOrderingKey(OrderEvent::customerId))
                                .publisherOptions(
                                        PubSubPublisherOptions.builder()
                                                .enableMessageOrdering(true)
                                                .build())
                                .build());
        // end::pubsub-topics[]
    }

    private static final class OrderEvent {

        private final String customerId;
        private final String region;

        private OrderEvent(String customerId, String region) {
            this.customerId = customerId;
            this.region = region;
        }

        String customerId() {
            return customerId;
        }

        String region() {
            return region;
        }
    }

    private static final class OrderEventSchema implements SerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(OrderEvent element) {
            return new byte[0];
        }
    }
}
