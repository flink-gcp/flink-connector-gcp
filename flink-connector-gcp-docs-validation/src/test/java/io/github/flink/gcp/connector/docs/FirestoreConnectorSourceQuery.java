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

import com.google.cloud.firestore.Query;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderDeserializer;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;

import java.time.Instant;

final class FirestoreConnectorSourceQuery {

    private FirestoreConnectorSourceQuery() {}

    static Source<Order, ?, ?> query() {
        // tag::firestore-connector-source-query[]
        Source<Order, ?, ?> source =
                FirestoreSource.<Order>builder()
                        .database(DatabaseDestination.of("my-project"))
                        .query(
                                firestore ->
                                        firestore
                                                .collection("customers/c-1/orders")
                                                .whereEqualTo("status", "shipped")
                                                .orderBy("total", Query.Direction.DESCENDING)
                                                .limit(1000))
                        .readTime(Instant.parse("2026-09-30T09:00:00Z"))
                        .deserializer(new OrderDeserializer())
                        .build();
        // end::firestore-connector-source-query[]
        return source;
    }
}
