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

import org.apache.flink.api.connector.source.Source;

import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEntityDeserializer;

final class DatastoreConnectorSource {

    private DatastoreConnectorSource() {}

    static Source<Order, ?, ?> kind() {
        // tag::datastore-connector-source[]
        Source<Order, ?, ?> source =
                DatastoreSource.<Order>builder()
                        .database(DatabaseDestination.of("my-project", "orders-db"))
                        .namespace("tenant-a")
                        .kind("Order")
                        .splitCount(64)
                        .deserializer(new OrderEntityDeserializer())
                        .build();
        // end::datastore-connector-source[]
        return source;
    }

    static Source<Order, ?, ?> query() {
        // tag::datastore-connector-source-query[]
        Query shipped =
                Query.newBuilder()
                        .addKind(KindExpression.newBuilder().setName("Order"))
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                                "status",
                                                PropertyFilter.Operator.EQUAL,
                                                DatastoreHelper.makeValue("shipped"))
                                        .build())
                        .build();
        Source<Order, ?, ?> source =
                DatastoreSource.<Order>builder()
                        .database(DatabaseDestination.of("my-project"))
                        .query(shipped)
                        .deserializer(new OrderEntityDeserializer())
                        .build();
        // end::datastore-connector-source-query[]
        return source;
    }

    static Source<Order, ?, ?> gql() {
        // tag::datastore-connector-source-gql[]
        Source<Order, ?, ?> source =
                DatastoreSource.<Order>builder()
                        .database(DatabaseDestination.of("my-project"))
                        .gqlQuery("SELECT * FROM `Order` WHERE total > 100 ORDER BY total DESC")
                        .deserializer(new OrderEntityDeserializer())
                        .build();
        // end::datastore-connector-source-gql[]
        return source;
    }
}
