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

import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.Map;

final class FirestoreExamplesMergingFields {

    private FirestoreExamplesMergingFields() {}

    static void build() {
        // tag::firestore-examples-merging-fields[]
        FirestoreSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project"))
                .serializer(
                        (event, context) ->
                                FirestoreWrite.setMerge(
                                        "orders/" + event.getId(),
                                        Map.of(
                                                "status", event.getStatus(),
                                                // A nested map is merged key by key, so other
                                                // keys of "audit" keep their values.
                                                "audit", Map.of("lastStatus", event.getStatus()))))
                .build();
        // end::firestore-examples-merging-fields[]
    }
}
