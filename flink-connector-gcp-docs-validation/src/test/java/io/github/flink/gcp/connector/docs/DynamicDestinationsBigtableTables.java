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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

final class DynamicDestinationsBigtableTables {

    private DynamicDestinationsBigtableTables() {}

    static void build() {
        // tag::bigtable-tables[]
        Map<LocalDate, TableDestination> tablesByDay = new HashMap<>();

        BigtableSink.<OrderEvent>builder()
                .destinationResolver(
                        (event, context) ->
                                tablesByDay.computeIfAbsent(
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
                .writerOptions(
                        BigtableWriterOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(15))
                                .build())
                .build();
        // end::bigtable-tables[]
    }

    private static final class OrderEvent {

        private final LocalDate day;
        private final String id;
        private final long timestampMicros;
        private final String body;

        private OrderEvent(LocalDate day, String id, long timestampMicros, String body) {
            this.day = day;
            this.id = id;
            this.timestampMicros = timestampMicros;
            this.body = body;
        }

        LocalDate day() {
            return day;
        }

        String id() {
            return id;
        }

        long timestampMicros() {
            return timestampMicros;
        }

        String body() {
            return body;
        }
    }
}
