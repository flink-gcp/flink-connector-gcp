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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

/**
 * Builder for BigQuery sinks, obtained from {@link BigQuerySink#builder()}.
 *
 * <p>Required settings: a serializer and a destination. The destination is set through either
 * {@link #destination(TableDestination)} (fixed table) or {@link
 * #destinationResolver(DestinationResolver)} (per-record dynamic destinations); the two override
 * each other and the last call wins.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class BigQuerySinkBuilder<T> {

    private WriteMethod writeMethod = WriteMethod.STORAGE_API_AT_LEAST_ONCE;
    private DestinationResolver<? super T> destinationResolver;
    private BigQueryProtoSerializer<? super T> serializer;
    private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
    private TableCreateOptionsProvider tableCreateOptionsProvider =
            destination -> TableCreateOptions.defaults();
    private FailedRowHandler failedRowHandler = FailedRowHandler.failJob();
    private String location;

    BigQuerySinkBuilder() {}

    /**
     * Sets the write method. Defaults to {@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}.
     *
     * @param writeMethod the write method
     * @return this builder
     */
    public BigQuerySinkBuilder<T> writeMethod(WriteMethod writeMethod) {
        this.writeMethod = Preconditions.checkNotNull(writeMethod, "writeMethod must not be null");
        return this;
    }

    /**
     * Writes every record to the given fixed table. Overrides any previously set destination or
     * resolver.
     *
     * @param destination the destination table
     * @return this builder
     */
    public BigQuerySinkBuilder<T> destination(TableDestination destination) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(destination, "destination must not be null"));
        return this;
    }

    /**
     * Resolves the destination table per record (dynamic destinations). Overrides any previously
     * set destination or resolver.
     *
     * @param destinationResolver the resolver
     * @return this builder
     */
    public BigQuerySinkBuilder<T> destinationResolver(
            DestinationResolver<? super T> destinationResolver) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
        return this;
    }

    /**
     * Sets the record serializer.
     *
     * @param serializer the serializer
     * @return this builder
     */
    public BigQuerySinkBuilder<T> serializer(BigQueryProtoSerializer<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Sets the table create disposition. Defaults to {@link CreateDisposition#CREATE_IF_NEEDED}.
     *
     * @param createDisposition the create disposition
     * @return this builder
     */
    public BigQuerySinkBuilder<T> createDisposition(CreateDisposition createDisposition) {
        this.createDisposition =
                Preconditions.checkNotNull(createDisposition, "createDisposition must not be null");
        return this;
    }

    /**
     * Applies the same creation options (partitioning, clustering) to every table created under
     * {@link CreateDisposition#CREATE_IF_NEEDED}. Overrides any previously set options or provider.
     * Defaults to {@link TableCreateOptions#defaults()} (plain tables).
     *
     * @param tableCreateOptions the creation options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> tableCreateOptions(TableCreateOptions tableCreateOptions) {
        Preconditions.checkNotNull(tableCreateOptions, "tableCreateOptions must not be null");
        this.tableCreateOptionsProvider = destination -> tableCreateOptions;
        return this;
    }

    /**
     * Resolves creation options (partitioning, clustering) per destination for tables created under
     * {@link CreateDisposition#CREATE_IF_NEEDED}. Overrides any previously set options or provider.
     *
     * @param tableCreateOptionsProvider the provider
     * @return this builder
     */
    public BigQuerySinkBuilder<T> tableCreateOptionsProvider(
            TableCreateOptionsProvider tableCreateOptionsProvider) {
        this.tableCreateOptionsProvider =
                Preconditions.checkNotNull(
                        tableCreateOptionsProvider, "tableCreateOptionsProvider must not be null");
        return this;
    }

    /**
     * Sets the policy for rows that terminally fail to be written (rows rejected by the Storage
     * Write API with per-row error details, rows that fail serialization, and rows exceeding the
     * per-row size limit). Defaults to {@link FailedRowHandler#failJob()}.
     *
     * <p>The handler decides per row: returning normally drops the row, throwing fails the ongoing
     * write or checkpoint. Transient append failures are retried without involving the handler, and
     * terminal request failures such as {@code PERMISSION_DENIED} always fail the job.
     *
     * @param failedRowHandler the handler
     * @return this builder
     */
    public BigQuerySinkBuilder<T> failedRowHandler(FailedRowHandler failedRowHandler) {
        this.failedRowHandler =
                Preconditions.checkNotNull(failedRowHandler, "failedRowHandler must not be null");
        return this;
    }

    /**
     * Sets the BigQuery location (for example {@code US} or {@code asia-northeast1}) shared by the
     * destination tables. Optional; setting it avoids a per-table metadata lookup when opening
     * Storage Write API connections.
     *
     * @param location the BigQuery location
     * @return this builder
     */
    public BigQuerySinkBuilder<T> location(String location) {
        this.location = Preconditions.checkNotNull(location, "location must not be null");
        return this;
    }

    /**
     * Builds the sink for the configured {@link WriteMethod}.
     *
     * @return the sink
     */
    public Sink<T> build() {
        Preconditions.checkState(serializer != null, "A serializer is required.");
        Preconditions.checkState(
                destinationResolver != null,
                "A destination is required: set destination(...) or destinationResolver(...).");

        BigQuerySinkConfig<T> config =
                new BigQuerySinkConfig<>(
                        destinationResolver,
                        serializer,
                        createDisposition,
                        tableCreateOptionsProvider,
                        failedRowHandler,
                        location);
        switch (writeMethod) {
            case STORAGE_API_AT_LEAST_ONCE:
                return new BigQueryDefaultStreamSink<>(config);
            case STORAGE_API_EXACTLY_ONCE:
                throw new UnsupportedOperationException(
                        "WriteMethod.STORAGE_API_EXACTLY_ONCE is not implemented yet"
                                + " (tracked in issue #30).");
            case FILE_LOADS:
                throw new UnsupportedOperationException(
                        "WriteMethod.FILE_LOADS is not implemented yet (tracked in issue #14).");
            default:
                throw new IllegalStateException("Unknown write method: " + writeMethod);
        }
    }
}
