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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderEvent;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

final class BigtableExamplesTablePerDay {

    private BigtableExamplesTablePerDay() {}

    static void build() {
        // tag::bigtable-examples-table-per-day[]
        Map<LocalDate, TableDestination> byDay = new HashMap<>();

        BigtableSink.<OrderEvent>builder()
                .destinationResolver(
                        (event, context) ->
                                byDay.computeIfAbsent(
                                        event.day(),
                                        day ->
                                                TableDestination.of(
                                                        "my-project",
                                                        "my-instance",
                                                        "orders-" + day)))
                .serializer(
                        (event, context) ->
                                RowMutationEntry.create(event.id())
                                        .setCell(
                                                "cf",
                                                "payload",
                                                event.timestampMicros(),
                                                event.body()))
                // A day's table stops receiving records once the day rolls over, and its batcher
                // goes with
                // it after this long. One hour is the default; this job knows its tables turn over
                // faster.
                .writerOptions(
                        BigtableWriterOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(15))
                                .build())
                .build();
        // end::bigtable-examples-table-per-day[]
    }
}
