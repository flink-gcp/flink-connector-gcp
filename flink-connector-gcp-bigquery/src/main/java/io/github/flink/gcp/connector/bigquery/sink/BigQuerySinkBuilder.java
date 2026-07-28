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

import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRowHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

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
    private SchemaUpdateOptions schemaUpdateOptions = SchemaUpdateOptions.defaults();
    private FailedRowHandler failedRowHandler = FailedRowHandler.failJob();
    private String location;
    private FileLoadsOptions fileLoadsOptions;
    private BufferedStreamOptions bufferedStreamOptions;
    private DefaultStreamOptions defaultStreamOptions;

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
     * Sets the options gating connector-driven table schema updates. Defaults to {@link
     * SchemaUpdateOptions#defaults()} (updates disabled).
     *
     * <p>Schema changes made externally (for example via DDL) are always picked up without a job
     * restart; these options only control whether the sink may update destination table schemas
     * itself when the serializer's schema evolves past the table's.
     *
     * @param schemaUpdateOptions the schema update options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> schemaUpdateOptions(SchemaUpdateOptions schemaUpdateOptions) {
        this.schemaUpdateOptions =
                Preconditions.checkNotNull(
                        schemaUpdateOptions, "schemaUpdateOptions must not be null");
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
     * Sets the options specific to {@link WriteMethod#FILE_LOADS}. Required for that write method
     * and rejected for every other one.
     *
     * @param fileLoadsOptions the file-loads options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> fileLoadsOptions(FileLoadsOptions fileLoadsOptions) {
        this.fileLoadsOptions =
                Preconditions.checkNotNull(fileLoadsOptions, "fileLoadsOptions must not be null");
        return this;
    }

    /**
     * Sets the options specific to {@link WriteMethod#STORAGE_API_EXACTLY_ONCE}. Required for that
     * write method (all knobs are defaulted, so {@code BufferedStreamOptions.builder().build()} is
     * a valid value) and rejected for every other one.
     *
     * @param bufferedStreamOptions the buffered-stream options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> bufferedStreamOptions(
            BufferedStreamOptions bufferedStreamOptions) {
        this.bufferedStreamOptions =
                Preconditions.checkNotNull(
                        bufferedStreamOptions, "bufferedStreamOptions must not be null");
        return this;
    }

    /**
     * Sets the options specific to {@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}. Optional for that
     * write method — the default write method is chosen by not choosing, so unlike the other
     * write-method option objects nothing forces this one into view, and an unconfigured sink uses
     * {@code DefaultStreamOptions.builder().build()} — and rejected for every other one.
     *
     * @param defaultStreamOptions the default-stream options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> defaultStreamOptions(DefaultStreamOptions defaultStreamOptions) {
        this.defaultStreamOptions =
                Preconditions.checkNotNull(
                        defaultStreamOptions, "defaultStreamOptions must not be null");
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
                        schemaUpdateOptions,
                        failedRowHandler,
                        location);
        // The required/forbidden pairing for write-method-scoped options; future write-method
        // option objects follow the same two adjacent checks. defaultStreamOptions keeps only the
        // forbidden half: its write method is the default, chosen by not choosing, so there is
        // nothing to force into view and all knobs are defaulted.
        Preconditions.checkState(
                writeMethod == WriteMethod.FILE_LOADS || fileLoadsOptions == null,
                "fileLoadsOptions(...) is only valid for WriteMethod.FILE_LOADS"
                        + " (write method is %s).",
                writeMethod);
        Preconditions.checkState(
                writeMethod != WriteMethod.FILE_LOADS || fileLoadsOptions != null,
                "fileLoadsOptions(...) is required for WriteMethod.FILE_LOADS.");
        Preconditions.checkState(
                writeMethod == WriteMethod.STORAGE_API_EXACTLY_ONCE
                        || bufferedStreamOptions == null,
                "bufferedStreamOptions(...) is only valid for"
                        + " WriteMethod.STORAGE_API_EXACTLY_ONCE (write method is %s).",
                writeMethod);
        Preconditions.checkState(
                writeMethod != WriteMethod.STORAGE_API_EXACTLY_ONCE
                        || bufferedStreamOptions != null,
                "bufferedStreamOptions(...) is required for"
                        + " WriteMethod.STORAGE_API_EXACTLY_ONCE.");
        Preconditions.checkState(
                writeMethod == WriteMethod.STORAGE_API_AT_LEAST_ONCE
                        || defaultStreamOptions == null,
                "defaultStreamOptions(...) is only valid for"
                        + " WriteMethod.STORAGE_API_AT_LEAST_ONCE (write method is %s).",
                writeMethod);
        Preconditions.checkState(
                writeMethod != WriteMethod.STORAGE_API_EXACTLY_ONCE
                        || destinationResolver instanceof FixedDestinationResolver,
                "WriteMethod.STORAGE_API_EXACTLY_ONCE requires a fixed destination(...);"
                        + " destinationResolver(...) (dynamic destinations) is not supported for"
                        + " this write method yet.");
        Preconditions.checkState(
                writeMethod != WriteMethod.STORAGE_API_EXACTLY_ONCE
                        || !schemaUpdateOptions.isEnabled(),
                "schemaUpdateOptions(...) is not supported for"
                        + " WriteMethod.STORAGE_API_EXACTLY_ONCE: a buffered stream's schema is"
                        + " pinned when the stream is created, so the sink cannot evolve the"
                        + " table schema mid-run. Update the table schema out of band and"
                        + " restart the job, or use another write method.");
        switch (writeMethod) {
            case STORAGE_API_AT_LEAST_ONCE:
                return new BigQueryDefaultStreamSink<>(
                        config,
                        defaultStreamOptions != null
                                ? defaultStreamOptions
                                : DefaultStreamOptions.builder().build());
            case STORAGE_API_EXACTLY_ONCE:
                return new BigQueryBufferedStreamSink<>(config, bufferedStreamOptions);
            case FILE_LOADS:
                return new BigQueryFileLoadsSink<>(config, fileLoadsOptions);
            default:
                throw new IllegalStateException("Unknown write method: " + writeMethod);
        }
    }
}
