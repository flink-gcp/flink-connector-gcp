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
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.Order;

final class DatastoreConnectorSourceDeserializer {

    private DatastoreConnectorSourceDeserializer() {}

    // tag::datastore-connector-source-deserializer[]
    static final class ShippedOrders implements DatastoreEntityDeserializationSchema<Order> {

        @Override
        public void deserialize(Entity entity, Collector<Order> out) {
            // Emitting nothing skips the entity and counts it in recordsSkipped. A getter throws
            // for a property the entity does not hold, so check for it first.
            if (entity.contains("status") && "shipped".equals(entity.getString("status"))) {
                // A key holds a name or a numeric id.
                String id = String.valueOf(entity.getKey().getNameOrId());
                out.collect(new Order(id, entity.getLong("total")));
            }
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
    // end::datastore-connector-source-deserializer[]
}
