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

import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.OrderEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

interface BigQueryExamplesTablePerDay {

    // CHECKSTYLE.OFF: RedundantModifier
    // tag::bigquery-examples-table-per-day-resolver[]
    public class DailyTableResolver implements DestinationResolver<OrderEvent> {

        private static final DateTimeFormatter SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd");

        private final String project;
        private final String dataset;
        private final String prefix;

        // One entry per day rather than one TableDestination per record. A plain HashMap is enough:
        // the writer is single-threaded per subtask, and this resolver is never shared across them.
        private final Map<LocalDate, TableDestination> cache = new HashMap<>();

        public DailyTableResolver(String project, String dataset, String prefix) {
            this.project = project;
            this.dataset = dataset;
            this.prefix = prefix;
        }

        @Override
        public TableDestination resolve(OrderEvent element, SinkWriter.Context context) {
            Long eventTime = context.timestamp();
            // Null when nothing assigned the record a timestamp — a processing-time job, or a
            // source
            // with no timestamp assigner. Falling back to the record's own field keeps such a
            // record
            // routed rather than unroutable.
            Instant instant =
                    eventTime != null ? Instant.ofEpochMilli(eventTime) : element.createdAt();
            LocalDate day = instant.atZone(ZoneOffset.UTC).toLocalDate();
            return cache.computeIfAbsent(
                    day,
                    d -> TableDestination.of(project, dataset, prefix + "_" + d.format(SUFFIX)));
        }
    }

    // end::bigquery-examples-table-per-day-resolver[]
    // CHECKSTYLE.ON: RedundantModifier

    static void build(BigQueryProtoSerializationSchema<OrderEvent> serializer) {
        // tag::bigquery-examples-table-per-day-sink[]
        BigQuerySink.<OrderEvent>builder()
                .destinationResolver(new DailyTableResolver("my-project", "my_dataset", "orders"))
                .serializer(serializer)
                .build();
        // end::bigquery-examples-table-per-day-sink[]
    }
}
