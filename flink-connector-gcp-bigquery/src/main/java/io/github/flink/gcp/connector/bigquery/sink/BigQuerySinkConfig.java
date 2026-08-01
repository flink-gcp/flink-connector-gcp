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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.io.Serializable;

/**
 * Immutable configuration shared by all {@link WriteMethod} implementations, assembled by {@link
 * BigQuerySinkBuilder}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class BigQuerySinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final BigQueryProtoSerializer<? super T> serializer;
    private final CreateDisposition createDisposition;
    private final TableCreateOptionsProvider tableCreateOptionsProvider;
    private final SchemaUpdateOptions schemaUpdateOptions;
    private final FailureHandler<FailedRow> failedRowHandler;
    private final String location;

    BigQuerySinkConfig(
            DestinationResolver<? super T> destinationResolver,
            BigQueryProtoSerializer<? super T> serializer,
            CreateDisposition createDisposition,
            TableCreateOptionsProvider tableCreateOptionsProvider,
            SchemaUpdateOptions schemaUpdateOptions,
            FailureHandler<FailedRow> failedRowHandler,
            String location) {
        this.destinationResolver = destinationResolver;
        this.serializer = serializer;
        this.createDisposition = createDisposition;
        this.tableCreateOptionsProvider = tableCreateOptionsProvider;
        this.schemaUpdateOptions = schemaUpdateOptions;
        this.failedRowHandler = failedRowHandler;
        this.location = location;
    }

    /** Returns the per-record destination resolver. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the record serializer. */
    public BigQueryProtoSerializer<? super T> getSerializer() {
        return serializer;
    }

    /** Returns the table create disposition. */
    public CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    /** Returns the per-destination creation options provider for auto-created tables. */
    public TableCreateOptionsProvider getTableCreateOptionsProvider() {
        return tableCreateOptionsProvider;
    }

    /** Returns the options gating connector-driven table schema updates. */
    public SchemaUpdateOptions getSchemaUpdateOptions() {
        return schemaUpdateOptions;
    }

    /** Returns the handler for rows that terminally fail to be written. */
    public FailureHandler<FailedRow> getFailedRowHandler() {
        return failedRowHandler;
    }

    /**
     * Returns the BigQuery location (for example {@code US} or {@code asia-northeast1}) of the
     * destination tables, or {@code null} when unset.
     */
    public String getLocation() {
        return location;
    }
}
