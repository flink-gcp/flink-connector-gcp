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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

import javax.annotation.Nullable;

/**
 * Builder for BigQuery sinks, obtained from {@link BigQuerySink#builder()}.
 *
 * <p>Required settings: a serializer and a destination. The destination is set through either
 * {@link #table(TableDestination)} (fixed table) or {@link
 * #destinationResolver(DestinationResolver)} (per-record dynamic destinations); the two override
 * each other and the last call wins.
 *
 * @param <T> type of the records written by the sink
 */
@Public
public class BigQuerySinkBuilder<T> {

    private WriteMethod writeMethod = WriteMethod.STORAGE_API_AT_LEAST_ONCE;
    private DestinationResolver<? super T> destinationResolver;
    private BigQueryProtoSerializationSchema<? super T> serializer;
    private AdditionalFields<? super T> additionalFields;
    private CdcOptions<? super T> cdcOptions;
    private CdcTableOptionsProvider cdcTableOptionsProvider =
            new FixedCdcTableOptionsProvider(CdcTableOptions.defaults());
    @Nullable private CdcTableOptions fixedCdcTableOptions = CdcTableOptions.defaults();
    private boolean cdcTableOptionsConfigured;
    private CdcTableReconciliationPolicy cdcTableReconciliationPolicy =
            CdcTableReconciliationPolicy.VERIFY_ONLY;
    private boolean cdcTableReconciliationPolicyConfigured;
    private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
    private TableCreateOptionsProvider tableCreateOptionsProvider =
            new FixedTableCreateOptionsProvider(TableCreateOptions.defaults());
    private SchemaUpdateOptions schemaUpdateOptions = SchemaUpdateOptions.defaults();
    private FailureHandler<? super BigQueryFailure> failureHandler = FailureHandler.failJob();
    private String location;
    private FileLoadsOptions fileLoadsOptions;
    private BufferedStreamOptions bufferedStreamOptions;
    private DefaultStreamOptions defaultStreamOptions;
    private String serviceAccountKeyFile;
    private EmulatorEndpoint emulatorEndpoint;
    private EmulatorEndpoint emulatorRestEndpoint;

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
     * Writes every record to the given fixed table. Overrides any previously set table or
     * destination resolver.
     *
     * @param table the destination table
     * @return this builder
     */
    public BigQuerySinkBuilder<T> table(TableDestination table) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(table, "table must not be null"));
        return this;
    }

    /**
     * Resolves the destination table per record (dynamic destinations). Overrides any previously
     * set table or destination resolver.
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
    public BigQuerySinkBuilder<T> serializer(
            BigQueryProtoSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Appends physical fields derived from each non-skipped input record.
     *
     * <p>The fields are added to protobuf rows and to the physical BigQuery schema used by every
     * write method for table creation and schema reconciliation. When this method is not called, it
     * adds no fields or provider calls. If no other row decorator such as {@link
     * #cdcOptions(CdcOptions)} is configured, the sink uses the serializer's schema, descriptor,
     * and row bytes unchanged.
     *
     * @param additionalFields ordered physical fields to append
     * @return this builder
     */
    public BigQuerySinkBuilder<T> additionalFields(AdditionalFields<? super T> additionalFields) {
        this.additionalFields =
                Preconditions.checkNotNull(additionalFields, "additionalFields must not be null");
        return this;
    }

    /**
     * Enables BigQuery change data capture for records appended through the Storage Write API
     * default stream.
     *
     * <p>The desired primary key and optional maximum-staleness policy are configured separately
     * through {@link #cdcTableOptions(CdcTableOptions)} or {@link
     * #cdcTableOptionsProvider(CdcTableOptionsProvider)}. The sink creates a missing physical
     * schema and primary key through the Tables API only when {@link
     * CreateDisposition#CREATE_IF_NEEDED} permits it. Existing-table handling is selected through
     * {@link #cdcTableReconciliationPolicy(CdcTableReconciliationPolicy)}. CDC pseudocolumns are
     * added to write rows only, never to the physical schema. Rejected for {@link
     * WriteMethod#STORAGE_API_EXACTLY_ONCE} and {@link WriteMethod#FILE_LOADS}.
     *
     * @param cdcOptions the per-record CDC providers
     * @return this builder
     */
    public BigQuerySinkBuilder<T> cdcOptions(CdcOptions<? super T> cdcOptions) {
        this.cdcOptions = Preconditions.checkNotNull(cdcOptions, "cdcOptions must not be null");
        return this;
    }

    /**
     * Applies the same desired CDC table contract to every destination.
     *
     * <p>Primary-key columns are required only when the sink must create a missing table or when
     * {@link CdcTableReconciliationPolicy#RECONCILE} needs an authoritative key. An existing table
     * under {@link CdcTableReconciliationPolicy#VERIFY_ONLY} may supply its key from BigQuery
     * metadata. Overrides any previously set options or provider.
     *
     * @param cdcTableOptions the desired CDC table contract
     * @return this builder
     */
    public BigQuerySinkBuilder<T> cdcTableOptions(CdcTableOptions cdcTableOptions) {
        Preconditions.checkNotNull(cdcTableOptions, "cdcTableOptions must not be null");
        this.cdcTableOptionsProvider = new FixedCdcTableOptionsProvider(cdcTableOptions);
        this.fixedCdcTableOptions = cdcTableOptions;
        this.cdcTableOptionsConfigured = true;
        return this;
    }

    /**
     * Resolves the desired CDC table contract per destination. Overrides fixed CDC table options.
     *
     * @param cdcTableOptionsProvider the provider
     * @return this builder
     */
    public BigQuerySinkBuilder<T> cdcTableOptionsProvider(
            CdcTableOptionsProvider cdcTableOptionsProvider) {
        this.cdcTableOptionsProvider =
                Preconditions.checkNotNull(
                        cdcTableOptionsProvider, "cdcTableOptionsProvider must not be null");
        this.fixedCdcTableOptions = null;
        this.cdcTableOptionsConfigured = true;
        return this;
    }

    /**
     * Sets how the sink handles a CDC destination table that already exists. Defaults to {@link
     * CdcTableReconciliationPolicy#VERIFY_ONLY}.
     *
     * <p>This policy is independent of {@link CreateDisposition}: reconciliation never authorizes
     * creation under {@link CreateDisposition#CREATE_NEVER}.
     *
     * @param policy the existing-table policy
     * @return this builder
     */
    public BigQuerySinkBuilder<T> cdcTableReconciliationPolicy(
            CdcTableReconciliationPolicy policy) {
        this.cdcTableReconciliationPolicy =
                Preconditions.checkNotNull(policy, "policy must not be null");
        this.cdcTableReconciliationPolicyConfigured = true;
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
     * Applies the same creation options to every table created under {@link
     * CreateDisposition#CREATE_IF_NEEDED}. Overrides any previously set options or provider.
     * Defaults to {@link TableCreateOptions#defaults()} (plain tables). CDC primary-key and
     * maximum-staleness properties belong to {@link CdcTableOptions} instead.
     *
     * @param tableCreateOptions the creation options
     * @return this builder
     */
    public BigQuerySinkBuilder<T> tableCreateOptions(TableCreateOptions tableCreateOptions) {
        Preconditions.checkNotNull(tableCreateOptions, "tableCreateOptions must not be null");
        this.tableCreateOptionsProvider = new FixedTableCreateOptionsProvider(tableCreateOptions);
        return this;
    }

    /**
     * Resolves creation options per destination for tables created under {@link
     * CreateDisposition#CREATE_IF_NEEDED}. Overrides any previously set options or provider.
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
     * Sets the policy for records that explicitly fail destination resolution or terminally fail
     * after routing (rows rejected by the Storage Write API with per-row error details, rows that
     * fail serialization, and rows exceeding the per-row size limit). Defaults to {@link
     * FailureHandler#failJob()}.
     *
     * <p>The handler decides per failure: returning normally drops the record, throwing fails the
     * write or checkpoint. Transient append failures are retried without involving the handler, and
     * terminal request failures such as {@code INVALID_ARGUMENT} always fail the job. The sink
     * drives the handler's lifecycle ({@code open}/{@code flush}/{@code close}) as documented on
     * {@link FailureHandler}. The parameter is contravariant, so a cross-connector {@code
     * FailureHandler<FailedElement>} is accepted as-is.
     *
     * @param failureHandler the handler
     * @return this builder
     */
    public BigQuerySinkBuilder<T> failureHandler(
            FailureHandler<? super BigQueryFailure> failureHandler) {
        this.failureHandler =
                Preconditions.checkNotNull(failureHandler, "failureHandler must not be null");
        return this;
    }

    /**
     * Sets the BigQuery location (for example {@code US} or {@code asia-northeast1}) shared by the
     * destination tables. Optional; setting it avoids a per-table metadata lookup when opening
     * Storage Write API connections, and under {@link WriteMethod#FILE_LOADS} it becomes the
     * location every load job runs in and is looked up under. When unset, {@code FILE_LOADS}
     * derives each job's location from its destination dataset's metadata instead — one {@code
     * datasets.get} per dataset per committer, which needs the {@code bigquery.datasets.get}
     * permission there — so a sink routing to datasets in several regions should leave it unset.
     *
     * @param location the BigQuery location
     * @return this builder
     */
    public BigQuerySinkBuilder<T> location(String location) {
        Preconditions.checkNotNull(location, "location must not be null");
        this.location = ResourceNames.checkNotBlank(location, "location");
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
     * Uses the service account in the given JSON key file for every BigQuery client the sink opens
     * and, under {@link WriteMethod#FILE_LOADS}, for Cloud Storage staging as well. Optional; when
     * unset, clients use application-default credentials.
     *
     * <p>The builder stores only the path in the job graph. Each runtime component reads the file
     * when it first creates a client, so the file must be available at the same path on every Task
     * Manager that runs a writer or committer.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public BigQuerySinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the sink's Storage Write API traffic at a BigQuery emulator instead of the production
     * service. The write stream opened for each destination connects to the given {@code host:port}
     * over a plaintext channel with no credentials, so this must only ever be used against an
     * emulator (for example a testcontainers {@code goccy/bigquery-emulator}). Optional; when unset
     * the sink connects to BigQuery with configured credentials or ADC.
     *
     * <p>BigQuery serves its two transports on <em>separate</em> ports — gRPC for the Storage Write
     * API, REST for table metadata — so this endpoint covers the gRPC half only, and a job that
     * also creates tables, evolves their schemas or manages a CDC table needs {@link
     * #emulatorRestEndpoint(String)} beside it. That is a deviation from the sibling connectors,
     * each of which needs one transport and so exposes one endpoint.
     *
     * <p>Rejected under {@link WriteMethod#FILE_LOADS}: that write method stages files to Cloud
     * Storage, which no emulator here stands in for, so an endpoint could only be half honored.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public BigQuerySinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Points the sink's table metadata traffic — table creation under {@link
     * CreateDisposition#CREATE_IF_NEEDED}, connector-driven schema updates and the CDC table
     * contract — at a BigQuery emulator instead of the production service. The REST client is built
     * against {@code http://host:port} with no credentials, so this must only ever be used against
     * an emulator. Optional; when unset the metadata client uses configured credentials or ADC — so
     * a sink doing any of those three with only {@link #emulatorEndpoint(String)} set still reaches
     * real BigQuery.
     *
     * <p>This is the REST half of {@link #emulatorEndpoint(String)}; see there for why the two are
     * separate and for the {@link WriteMethod#FILE_LOADS} rejection, which applies to both.
     *
     * @param emulatorRestEndpoint the emulator's REST endpoint as {@code host:port}, without a
     *     scheme
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public BigQuerySinkBuilder<T> emulatorRestEndpoint(String emulatorRestEndpoint) {
        this.emulatorRestEndpoint =
                EmulatorEndpoint.parse(emulatorRestEndpoint, "emulatorRestEndpoint");
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
                "A destination is required: set table(...) or destinationResolver(...).");

        BigQuerySinkConfig<T> config =
                new BigQuerySinkConfig<>(
                        destinationResolver,
                        serializer,
                        additionalFields,
                        cdcOptions,
                        createDisposition,
                        tableCreateOptionsProvider,
                        cdcTableOptionsProvider,
                        cdcTableReconciliationPolicy,
                        schemaUpdateOptions,
                        failureHandler,
                        location,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        emulatorRestEndpoint);
        // The required/forbidden pairing for write-method-scoped options; future write-method
        // option objects follow the same two adjacent checks. defaultStreamOptions keeps only the
        // forbidden half: its write method is the default, chosen by not choosing, so there is
        // nothing to force into view and all knobs are defaulted.
        Preconditions.checkState(
                writeMethod == WriteMethod.FILE_LOADS || fileLoadsOptions == null,
                "fileLoadsOptions(...) is only valid for WriteMethod.FILE_LOADS"
                        + " (write method is %s).",
                writeMethod.name());
        Preconditions.checkState(
                writeMethod != WriteMethod.FILE_LOADS || fileLoadsOptions != null,
                "fileLoadsOptions(...) is required for WriteMethod.FILE_LOADS.");
        Preconditions.checkState(
                writeMethod == WriteMethod.STORAGE_API_EXACTLY_ONCE
                        || bufferedStreamOptions == null,
                "bufferedStreamOptions(...) is only valid for"
                        + " WriteMethod.STORAGE_API_EXACTLY_ONCE (write method is %s).",
                writeMethod.name());
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
                writeMethod.name());
        Preconditions.checkState(
                writeMethod == WriteMethod.STORAGE_API_AT_LEAST_ONCE || cdcOptions == null,
                "cdcOptions(...) is only valid for WriteMethod.STORAGE_API_AT_LEAST_ONCE"
                        + " (write method is %s).",
                writeMethod.name());
        Preconditions.checkState(
                cdcOptions != null
                        || (!cdcTableOptionsConfigured && !cdcTableReconciliationPolicyConfigured),
                "cdcTableOptions(...), cdcTableOptionsProvider(...), and"
                        + " cdcTableReconciliationPolicy(...) are only valid with"
                        + " cdcOptions(...).");
        Preconditions.checkState(
                cdcOptions == null
                        || cdcTableReconciliationPolicy != CdcTableReconciliationPolicy.RECONCILE
                        || fixedCdcTableOptions == null
                        || !fixedCdcTableOptions.getPrimaryKeyColumns().isEmpty(),
                "CDC table reconciliation requires CdcTableOptions.primaryKeyColumns(...). A"
                        + " cdcTableOptionsProvider(...) must return primary-key columns for every"
                        + " destination.");
        // FILE_LOADS stages to Cloud Storage and submits load jobs; no emulator here stands in for
        // GCS, so an endpoint would be honored by the metadata half of that write method and
        // silently ignored by the half that actually moves the rows.
        Preconditions.checkState(
                writeMethod != WriteMethod.FILE_LOADS
                        || (emulatorEndpoint == null && emulatorRestEndpoint == null),
                "emulatorEndpoint(...) and emulatorRestEndpoint(...) are not supported for"
                        + " WriteMethod.FILE_LOADS: that write method stages files to Cloud"
                        + " Storage, which the BigQuery emulator does not provide.");
        Preconditions.checkState(
                serviceAccountKeyFile == null
                        || (emulatorEndpoint == null && emulatorRestEndpoint == null),
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...) or"
                        + " emulatorRestEndpoint(...): emulator connections are deliberately"
                        + " credential-free.");
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
