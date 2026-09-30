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

import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.Map;

final class FirestoreReadmeOverview {

    private FirestoreReadmeOverview() {}

    static Sink<OrderEvent> sink() {
        // tag::firestore-readme-overview[]
        Sink<OrderEvent> sink =
                FirestoreSink.<OrderEvent>builder()
                        .database(DatabaseDestination.of("my-project", "orders-db"))
                        .serializer(
                                (event, context) ->
                                        FirestoreWrite.set(
                                                "orders/" + event.getId(),
                                                Map.of("total", event.getTotal())))
                        .build();
        // end::firestore-readme-overview[]
        return sink;
    }
}
