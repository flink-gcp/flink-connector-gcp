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

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.DeadLetterQueue;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

final class DynamicDestinationsBigQueryTables {

    private DynamicDestinationsBigQueryTables() {}

    static void build(
            BigQueryProtoSerializer<OrderEvent> serializer, DeadLetterQueue deadLetterQueue) {
        // tag::bigquery-tables[]
        Map<LocalDate, TableDestination> tablesByDay = new HashMap<>();

        BigQuerySink.<OrderEvent>builder()
                .destinationResolver(
                        (event, context) -> {
                            if (!event.hasKnownTenant()) {
                                return UnroutableRecord.of(
                                        event.deadLetterPayload(), "Unknown tenant");
                            }
                            return tablesByDay.computeIfAbsent(
                                    event.day(),
                                    day ->
                                            TableDestination.of(
                                                    "my-project",
                                                    "my_dataset",
                                                    "orders_"
                                                            + day.format(
                                                                    DateTimeFormatter
                                                                            .BASIC_ISO_DATE)));
                        })
                .serializer(serializer)
                .failureHandler(FailureHandler.sendToDeadLetterQueue(deadLetterQueue))
                .build();
        // end::bigquery-tables[]
    }

    private static final class OrderEvent {

        private final LocalDate day;

        private OrderEvent(LocalDate day) {
            this.day = day;
        }

        LocalDate day() {
            return day;
        }

        boolean hasKnownTenant() {
            return true;
        }

        ByteString deadLetterPayload() {
            return ByteString.copyFromUtf8(day.toString());
        }
    }
}
