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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

final class JavadocBigtableExamples {

    private JavadocBigtableExamples() {}

    static Sink<OrderEvent> sink() {
        // tag::sink[]
        Sink<OrderEvent> sink =
                BigtableSink.<OrderEvent>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        .serializer(
                                (event, context) ->
                                        RowMutationEntry.create(event.getId())
                                                .setCell(
                                                        "cf",
                                                        "payload",
                                                        event.getTimestampMicros(),
                                                        event.getBody()))
                        .build();
        // end::sink[]
        return sink;
    }

    static BigtableSerializationSchema<OrderEvent> serializationSchema() {
        BigtableSerializationSchema<OrderEvent> serializer =
                // tag::serialization-schema[]
                (record, context) ->
                        RowMutationEntry.create(record.getId())
                                .setCell(
                                        "cf",
                                        "payload",
                                        record.getTimestampMicros(),
                                        record.getBody());
        // end::serialization-schema[]
        return serializer;
    }

    static Source<Order, ?, ?> source(BigtableRowDeserializationSchema<Order> myDeserializer) {
        // tag::source[]
        Source<Order, ?, ?> source =
                BigtableSource.<Order>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        .deserializer(myDeserializer)
                        .prefix("2026-08-")
                        .build();
        // end::source[]
        return source;
    }

    private static final class Order {}

    private static final class OrderEvent {

        String getId() {
            return "order-1";
        }

        long getTimestampMicros() {
            return 1L;
        }

        String getBody() {
            return "body";
        }
    }
}
