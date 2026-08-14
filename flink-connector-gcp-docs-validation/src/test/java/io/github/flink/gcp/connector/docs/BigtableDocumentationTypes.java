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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import java.io.IOException;
import java.time.LocalDate;

final class BigtableDocumentationTypes {

    private BigtableDocumentationTypes() {}

    static final class Order {}

    static final class OrderEvent {

        private final LocalDate day;
        private final String id;
        private final long timestampMicros;
        private final long updatedAtMillis;
        private final String body;
        private final String status;
        private final String totalCents;
        private final boolean cancelled;

        private OrderEvent(
                LocalDate day,
                String id,
                long timestampMicros,
                long updatedAtMillis,
                String body,
                String status,
                String totalCents,
                boolean cancelled) {
            this.day = day;
            this.id = id;
            this.timestampMicros = timestampMicros;
            this.updatedAtMillis = updatedAtMillis;
            this.body = body;
            this.status = status;
            this.totalCents = totalCents;
            this.cancelled = cancelled;
        }

        LocalDate day() {
            return day;
        }

        String id() {
            return id;
        }

        long timestampMicros() {
            return timestampMicros;
        }

        long updatedAtMillis() {
            return updatedAtMillis;
        }

        String body() {
            return body;
        }

        String status() {
            return status;
        }

        String totalCents() {
            return totalCents;
        }

        boolean isCancelled() {
            return cancelled;
        }
    }

    static final class OrderEventMutations implements BigtableSerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public RowMutationEntry serialize(OrderEvent event, SinkWriter.Context context) {
            return RowMutationEntry.create(event.id())
                    .setCell("cf", "payload", event.timestampMicros(), event.body());
        }
    }

    static final class OrderRows implements BigtableRowDeserializationSchema<Order> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Row row, Collector<Order> out) throws IOException {
            out.collect(new Order());
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
}
