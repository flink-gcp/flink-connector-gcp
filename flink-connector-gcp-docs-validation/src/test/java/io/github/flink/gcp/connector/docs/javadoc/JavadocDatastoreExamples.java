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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEntityDeserializer;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEvent;

final class JavadocDatastoreExamples {

    private JavadocDatastoreExamples() {}

    static Sink<OrderEvent> sink() {
        // tag::sink[]
        Sink<OrderEvent> sink =
                DatastoreSink.<OrderEvent>builder()
                        .database(DatabaseDestination.of("my-project"))
                        .serializer(
                                (event, context) ->
                                        DatastoreMutation.upsert(
                                                Entity.newBuilder(
                                                                Key.newBuilder(
                                                                                "my-project",
                                                                                "Order",
                                                                                event.getId())
                                                                        .build())
                                                        .set("total", event.getTotal())
                                                        .build()))
                        .build();
        // end::sink[]
        return sink;
    }

    static Source<Order, ?, ?> source() {
        // tag::source[]
        Source<Order, ?, ?> source =
                DatastoreSource.<Order>builder()
                        .database(DatabaseDestination.of("my-project"))
                        .kind("Order")
                        .deserializer(new OrderEntityDeserializer())
                        .build();
        // end::source[]
        return source;
    }
}
