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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;
import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

final class FirestoreDocumentationTypes {

    private FirestoreDocumentationTypes() {}

    static final class OrderEvent {

        String getId() {
            return "order-1";
        }

        long getTotal() {
            return 1L;
        }

        String getStatus() {
            return "shipped";
        }

        boolean isCancelled() {
            return false;
        }

        boolean isHeartbeat() {
            return false;
        }
    }

    static final class Order {

        Order(String id, long total) {}
    }

    static final class OrderDeserializer implements FirestoreDocumentDeserializationSchema<Order> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DocumentSnapshot document, Collector<Order> out) {
            out.collect(new Order(document.getId(), document.getLong("total")));
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }

    static final class OrderEntityDeserializer
            implements DatastoreEntityDeserializationSchema<Order> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<Order> out) {
            out.collect(new Order(entity.getKey().getName(), entity.getLong("total")));
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
}
