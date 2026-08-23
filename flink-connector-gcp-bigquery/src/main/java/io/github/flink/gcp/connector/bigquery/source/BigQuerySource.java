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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Public;

/**
 * A bounded source reading a BigQuery table through the Storage Read API.
 *
 * <p>The source is bounded, which is not the same as batch-only: it runs inside a STREAMING
 * pipeline and finishes once the table has been read, which is what a dimension-table join needs.
 *
 * <p>Rows arrive as Avro and are handed to a {@link
 * io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema}, which
 * converts each one into zero or more non-null records through a collector. Collecting nothing
 * skips the row. Column projection and row filtering are applied by BigQuery when the read session
 * is created, so what they exclude is neither transferred nor billed.
 *
 * <p>A read through this API is charged for the bytes BigQuery scans to serve it, unlike the sink's
 * {@code FILE_LOADS} write path, which is free.
 * <!-- javadoc-example file="JavadocBigQueryExamples.java" tag="source" -->
 *
 * <pre>{@code
 * Source<GenericRecord, ?, ?> source =
 *         BigQuerySource.<GenericRecord>builder()
 *                 .table(TableDestination.of("my-project", "my_dataset", "my_table"))
 *                 .deserializer(BigQueryRowDeserializationSchema.genericRecord(schema))
 *                 .rowRestriction("state = 'CA'")
 *                 .build();
 *
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuery");
 * }</pre>
 */
@Public
public final class BigQuerySource {

    private BigQuerySource() {}

    /**
     * Returns a builder. A deserializer and a destination — either a table or a query, not both —
     * are required.
     *
     * @param <T> type of the records produced by the source
     * @return the builder
     */
    public static <T> BigQuerySourceBuilder<T> builder() {
        return new BigQuerySourceBuilder<>();
    }
}
