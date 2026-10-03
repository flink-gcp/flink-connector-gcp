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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEvent;

final class DatastoreConnectorSink {

    private DatastoreConnectorSink() {}

    static void build() {
        // tag::datastore-connector-sink[]
        DatastoreSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project", "orders-db"))
                .serializer(
                        (event, context) ->
                                event.isHeartbeat()
                                        ? null
                                        : DatastoreMutation.upsert(
                                                Entity.newBuilder(
                                                                // The key names the project and
                                                                // database the sink writes to.
                                                                Key.newBuilder(
                                                                                "my-project",
                                                                                "Order",
                                                                                event.getId(),
                                                                                "orders-db")
                                                                        .setNamespace("tenant-a")
                                                                        .build())
                                                        .set("total", event.getTotal())
                                                        // Indexed strings stop at 1,500 bytes.
                                                        .set(
                                                                "status",
                                                                StringValue.newBuilder(
                                                                                event.getStatus())
                                                                        .setExcludeFromIndexes(true)
                                                                        .build())
                                                        .build()))
                .build();
        // end::datastore-connector-sink[]
    }
}
