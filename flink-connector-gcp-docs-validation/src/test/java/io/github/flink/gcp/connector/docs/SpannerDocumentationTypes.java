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
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import java.io.IOException;

final class SpannerDocumentationTypes {

    private SpannerDocumentationTypes() {}

    static final class OrderEvent {

        String getId() {
            return "order-1";
        }

        long getTotal() {
            return 1L;
        }
    }

    static final class Event {

        String getId() {
            return "event-1";
        }

        String getBody() {
            return "body";
        }

        boolean isAudit() {
            return false;
        }

        boolean isHeartbeat() {
            return false;
        }
    }

    static final class Singer {}

    static final class Order {

        private final String id;
        private final long total;

        Order(String id, long total) {
            this.id = id;
            this.total = total;
        }

        String getId() {
            return id;
        }

        long getTotal() {
            return total;
        }
    }

    static final class OrderChange {}

    static final class OrderEventSerializer
            implements SpannerMutationSerializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public Mutation serialize(OrderEvent event, SinkWriter.Context context) {
            return Mutation.newInsertOrUpdateBuilder("Orders")
                    .set("OrderId")
                    .to(event.getId())
                    .set("Total")
                    .to(event.getTotal())
                    .build();
        }
    }

    static final class SingerDeserializer implements SpannerStructDeserializationSchema<Singer> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Struct row, Collector<Singer> out) throws IOException {
            out.collect(new Singer());
        }

        @Override
        public TypeInformation<Singer> getProducedType() {
            return TypeInformation.of(Singer.class);
        }
    }

    static final class OrderDeserializer implements SpannerStructDeserializationSchema<Order> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Struct row, Collector<Order> out) throws IOException {
            out.collect(new Order(row.getString("OrderId"), 0L));
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }

    static final class OrderChangeDeserializer
            implements SpannerChangeStreamDeserializationSchema<OrderChange> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<OrderChange> out)
                throws IOException {
            out.collect(new OrderChange());
        }

        @Override
        public TypeInformation<OrderChange> getProducedType() {
            return TypeInformation.of(OrderChange.class);
        }
    }
}
