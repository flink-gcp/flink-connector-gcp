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

import java.nio.charset.StandardCharsets;

final class CloudTasksDocumentationTypes {

    private CloudTasksDocumentationTypes() {}

    static final class OrderEvent {

        private final String orderId;
        private final String customerId;

        private OrderEvent(String orderId, String customerId) {
            this.orderId = orderId;
            this.customerId = customerId;
        }

        String orderId() {
            return orderId;
        }

        String customerId() {
            return customerId;
        }
    }

    static final class MyEventJsonSerializationSchema implements SerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(OrderEvent element) {
            return element.orderId().getBytes(StandardCharsets.UTF_8);
        }
    }

    static final class OrderEventSchema implements SerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(OrderEvent element) {
            return element.orderId().getBytes(StandardCharsets.UTF_8);
        }
    }
}
