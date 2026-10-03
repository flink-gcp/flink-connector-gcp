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

import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema;
import io.github.flink.gcp.connector.docs.FirestoreDocumentationTypes.OrderEvent;

import java.time.Duration;

final class DatastoreConnectorTuning {

    private DatastoreConnectorTuning() {}

    static void build(DatastoreMutationSerializationSchema<OrderEvent> orderSerializer) {
        // tag::datastore-connector-tuning[]
        DatastoreSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project"))
                .serializer(orderSerializer)
                .writerOptions(
                        DatastoreWriterOptions.builder()
                                // Two sinks of parallelism 4 write to this database: with this set
                                // on both, the ramp-up's 500 operations per second are shared by
                                // all eight.
                                .throttlingParallelism(8)
                                .maxBatchMutations(200)
                                .recoveryMaxAttempts(15)
                                .requestTimeout(Duration.ofSeconds(30))
                                .build())
                .build();
        // end::datastore-connector-tuning[]
    }
}
