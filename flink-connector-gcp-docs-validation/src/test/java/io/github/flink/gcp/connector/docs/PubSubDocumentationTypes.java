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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class PubSubDocumentationTypes {

    private PubSubDocumentationTypes() {}

    static final class MyEvent {

        private final String topicName;
        private final String source;
        private final String deviceId;

        private MyEvent(String topicName, String source, String deviceId) {
            this.topicName = topicName;
            this.source = source;
            this.deviceId = deviceId;
        }

        String topicName() {
            return topicName;
        }

        String source() {
            return source;
        }

        String deviceId() {
            return deviceId;
        }
    }

    static final class MyEventSerializationSchema implements SerializationSchema<MyEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(MyEvent element) {
            return element.source().getBytes(StandardCharsets.UTF_8);
        }
    }

    static final class OrderEvent {

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

    static final class OrderEventSchema implements SerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(OrderEvent element) {
            return element.customerId().getBytes(StandardCharsets.UTF_8);
        }
    }

    static final class OrderEventDeserializationSchema
            implements PubSubDeserializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                PubsubMessage message,
                SubscriptionDestination subscription,
                Collector<OrderEvent> out)
                throws IOException {
            out.collect(new OrderEvent(message.getData().toStringUtf8(), "unknown"));
        }

        @Override
        public TypeInformation<OrderEvent> getProducedType() {
            return TypeInformation.of(OrderEvent.class);
        }
    }
}
