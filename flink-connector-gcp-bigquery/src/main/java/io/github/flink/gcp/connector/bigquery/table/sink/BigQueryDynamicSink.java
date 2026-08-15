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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcSequenceNumberProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.DebeziumMySqlCdcSequenceNumberEncoder;
import io.github.flink.gcp.connector.bigquery.sink.cdc.TiCdcSequenceNumberEncoder;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code bigquery} table sink.
 *
 * <p>Deliberately takes no {@code ReadableConfig}: turning a DDL option into a value happens in one
 * place, {@link io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory the
 * factory}, so this class has no configuration vocabulary at all.
 *
 * <p>Three abilities are <b>not</b> implemented, each for its own reason. {@code
 * SupportsPartitioning}: Flink's {@code PARTITIONED BY} models Hive-style value partitioning, which
 * BigQuery time partitioning is not, and ingestion-time partitioning has no column to name — so a
 * partition spec fails at plan time rather than being silently ignored, and everything goes through
 * {@code sink.table-create.*}. {@code SupportsOverwrite}: {@code INSERT OVERWRITE} has no meaning
 * for the Storage Write API, while {@code WRITE_TRUNCATE} stays reachable as {@code
 * sink.file-loads.write-disposition}. Writable metadata exists only when CDC is enabled: the
 * planner appends one selected sequence source after the physical row, while the physical DDL row
 * remains the BigQuery table schema.
 *
 * <p>Built through {@link #builder()} rather than a constructor: the resolved option families,
 * physical-key model and planner-selected metadata would otherwise form a positional list repeated
 * by construction, {@link #copy()} and {@link #equals(Object)}.
 */
@Internal
public final class BigQueryDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private final DataType physicalDataType;
    private final TableDestination destination;
    private final RowDataSchemaOptions schemaOptions;
    private final boolean cdcEnabled;
    private final List<String> debeziumMySqlSourceUuids;
    private final int[] primaryKeyIndexes;
    @Nullable private final String tiCdcClusterId;
    @Nullable private final WriteMethod writeMethod;
    @Nullable private final CreateDisposition createDisposition;
    @Nullable private final TableCreateOptions tableCreateOptions;
    @Nullable private final CdcTableOptions cdcTableOptions;
    @Nullable private final CdcTableReconciliationPolicy cdcTableReconciliationPolicy;
    @Nullable private final String location;
    @Nullable private final SchemaUpdateOptions schemaUpdateOptions;
    @Nullable private final DefaultStreamOptions defaultStreamOptions;
    @Nullable private final BufferedStreamOptions bufferedStreamOptions;
    @Nullable private final FileLoadsOptions fileLoadsOptions;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String emulatorRestEndpoint;
    @Nullable private final Integer parallelism;

    /** Metadata keys selected by the planner, in {@link WritableMetadata#listAll()} order. */
    private List<String> metadataKeys;

    private BigQueryDynamicSink(Builder builder) {
        this.physicalDataType = builder.physicalDataType;
        this.destination = builder.destination;
        this.schemaOptions = builder.schemaOptions;
        this.cdcEnabled = builder.cdcEnabled;
        this.debeziumMySqlSourceUuids =
                Collections.unmodifiableList(new ArrayList<>(builder.debeziumMySqlSourceUuids));
        if (!debeziumMySqlSourceUuids.isEmpty()) {
            new DebeziumMySqlCdcSequenceNumberEncoder(debeziumMySqlSourceUuids);
        }
        this.tiCdcClusterId = builder.tiCdcClusterId;
        if (tiCdcClusterId != null) {
            new TiCdcSequenceNumberEncoder(tiCdcClusterId);
        }
        this.primaryKeyIndexes = builder.primaryKeyIndexes.clone();
        this.writeMethod = builder.writeMethod;
        this.createDisposition = builder.createDisposition;
        this.tableCreateOptions = builder.tableCreateOptions;
        this.cdcTableOptions = builder.cdcTableOptions;
        this.cdcTableReconciliationPolicy = builder.cdcTableReconciliationPolicy;
        this.location = builder.location;
        this.schemaUpdateOptions = builder.schemaUpdateOptions;
        this.defaultStreamOptions = builder.defaultStreamOptions;
        this.bufferedStreamOptions = builder.bufferedStreamOptions;
        this.fileLoadsOptions = builder.fileLoadsOptions;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.emulatorRestEndpoint = builder.emulatorRestEndpoint;
        this.parallelism = builder.parallelism;
        this.metadataKeys = immutableMetadataKeys(builder.metadataKeys, cdcEnabled);
    }

    /**
     * Returns a builder for a sink made of fully resolved values.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return WritableMetadata.listAll();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = immutableMetadataKeys(metadataKeys, cdcEnabled);
    }

    private static List<String> immutableMetadataKeys(
            List<String> metadataKeys, boolean cdcEnabled) {
        List<String> selected = new ArrayList<>(metadataKeys);
        selected.forEach(WritableMetadata::of);
        if (!selected.isEmpty() && !cdcEnabled) {
            throw new ValidationException(
                    "BigQuery writable metadata is available only when 'sink.cdc.enabled' ="
                            + " 'true'.");
        }
        if (selected.size() > 1) {
            throw new ValidationException(
                    "Select exactly one BigQuery CDC sequence source: 'change-sequence-number'"
                            + " or 'debezium-source-properties', not both.");
        }
        return Collections.unmodifiableList(selected);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (cdcEnabled && !requestedMode.containsOnly(RowKind.INSERT)) {
            return CrossVersionChangelogMode.upsert();
        }
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        RowDataSerializer serializer =
                new RowDataSerializer(
                        rowType, schemaOptions, cdcEnabled ? primaryKeyIndexes : new int[0]);
        BigQuerySinkBuilder<RowData> builder =
                BigQuerySink.<RowData>builder().destination(destination).serializer(serializer);
        if (cdcEnabled) {
            CdcOptions.Builder<RowData> cdc =
                    CdcOptions.builder(RowDataCdcChangeTypeProvider.INSTANCE);
            if (!metadataKeys.isEmpty()) {
                WritableMetadata source = WritableMetadata.of(metadataKeys.get(0));
                int position = DataType.getFieldCount(physicalDataType);
                CdcSequenceNumberProvider<RowData> sequenceProvider =
                        new RowDataCdcSequenceNumberProvider(
                                source,
                                position,
                                new DebeziumCdcSequenceNumberResolver(
                                        debeziumMySqlSourceUuids, tiCdcClusterId));
                cdc.sequenceNumberProvider(sequenceProvider);
            }
            builder.cdcOptions(cdc.build());
        }
        if (writeMethod != null) {
            builder.writeMethod(writeMethod);
        }
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        if (tableCreateOptions != null) {
            // The single-options form, never the provider: a SQL INSERT INTO names one table, so
            // there is no second destination for a provider to answer differently for.
            builder.tableCreateOptions(tableCreateOptions);
        }
        if (cdcTableOptions != null) {
            builder.cdcTableOptions(cdcTableOptions);
        }
        if (cdcTableReconciliationPolicy != null) {
            builder.cdcTableReconciliationPolicy(cdcTableReconciliationPolicy);
        }
        if (location != null) {
            builder.location(location);
        }
        if (schemaUpdateOptions != null) {
            builder.schemaUpdateOptions(schemaUpdateOptions);
        }
        if (defaultStreamOptions != null) {
            builder.defaultStreamOptions(defaultStreamOptions);
        }
        if (bufferedStreamOptions != null) {
            builder.bufferedStreamOptions(bufferedStreamOptions);
        }
        if (fileLoadsOptions != null) {
            builder.fileLoadsOptions(fileLoadsOptions);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (emulatorRestEndpoint != null) {
            builder.emulatorRestEndpoint(emulatorRestEndpoint);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return builder()
                .physicalDataType(physicalDataType)
                .destination(destination)
                .schemaOptions(schemaOptions)
                .cdcEnabled(cdcEnabled)
                .debeziumMySqlSourceUuids(debeziumMySqlSourceUuids)
                .tiCdcClusterId(tiCdcClusterId)
                .primaryKeyIndexes(primaryKeyIndexes)
                .writeMethod(writeMethod)
                .createDisposition(createDisposition)
                .tableCreateOptions(tableCreateOptions)
                .cdcTableOptions(cdcTableOptions)
                .cdcTableReconciliationPolicy(cdcTableReconciliationPolicy)
                .location(location)
                .schemaUpdateOptions(schemaUpdateOptions)
                .defaultStreamOptions(defaultStreamOptions)
                .bufferedStreamOptions(bufferedStreamOptions)
                .fileLoadsOptions(fileLoadsOptions)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .emulatorEndpoint(emulatorEndpoint)
                .emulatorRestEndpoint(emulatorRestEndpoint)
                .parallelism(parallelism)
                .metadataKeys(metadataKeys)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "BigQuery table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigQueryDynamicSink that = (BigQueryDynamicSink) o;
        return physicalDataType.equals(that.physicalDataType)
                && destination.equals(that.destination)
                && schemaOptions.equals(that.schemaOptions)
                && cdcEnabled == that.cdcEnabled
                && debeziumMySqlSourceUuids.equals(that.debeziumMySqlSourceUuids)
                && Objects.equals(tiCdcClusterId, that.tiCdcClusterId)
                && Arrays.equals(primaryKeyIndexes, that.primaryKeyIndexes)
                && writeMethod == that.writeMethod
                && createDisposition == that.createDisposition
                && Objects.equals(tableCreateOptions, that.tableCreateOptions)
                && Objects.equals(cdcTableOptions, that.cdcTableOptions)
                && cdcTableReconciliationPolicy == that.cdcTableReconciliationPolicy
                && Objects.equals(location, that.location)
                && Objects.equals(schemaUpdateOptions, that.schemaUpdateOptions)
                && Objects.equals(defaultStreamOptions, that.defaultStreamOptions)
                && Objects.equals(bufferedStreamOptions, that.bufferedStreamOptions)
                && Objects.equals(fileLoadsOptions, that.fileLoadsOptions)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(emulatorRestEndpoint, that.emulatorRestEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && metadataKeys.equals(that.metadataKeys);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        physicalDataType,
                        destination,
                        schemaOptions,
                        cdcEnabled,
                        debeziumMySqlSourceUuids,
                        tiCdcClusterId,
                        writeMethod,
                        createDisposition,
                        tableCreateOptions,
                        cdcTableOptions,
                        cdcTableReconciliationPolicy,
                        location,
                        schemaUpdateOptions,
                        defaultStreamOptions,
                        bufferedStreamOptions,
                        fileLoadsOptions,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        emulatorRestEndpoint,
                        parallelism,
                        metadataKeys);
        result = 31 * result + Arrays.hashCode(primaryKeyIndexes);
        return result;
    }

    /**
     * Collects the sink's fully resolved values.
     *
     * <p>Every setter takes {@code null} to mean "leave the connector's own default alone", which
     * is exactly what {@code config.getOptional(...).orElse(null)} hands the factory, so an absent
     * DDL option needs no branch on the way here. The three values that have no default are checked
     * in {@link #build()}.
     */
    @Internal
    public static final class Builder {

        private DataType physicalDataType;
        private TableDestination destination;
        private RowDataSchemaOptions schemaOptions;
        private boolean cdcEnabled;
        private List<String> debeziumMySqlSourceUuids = Collections.emptyList();
        private int[] primaryKeyIndexes = new int[0];
        @Nullable private String tiCdcClusterId;
        @Nullable private WriteMethod writeMethod;
        @Nullable private CreateDisposition createDisposition;
        @Nullable private TableCreateOptions tableCreateOptions;
        @Nullable private CdcTableOptions cdcTableOptions;
        @Nullable private CdcTableReconciliationPolicy cdcTableReconciliationPolicy;
        @Nullable private String location;
        @Nullable private SchemaUpdateOptions schemaUpdateOptions;
        @Nullable private DefaultStreamOptions defaultStreamOptions;
        @Nullable private BufferedStreamOptions bufferedStreamOptions;
        @Nullable private FileLoadsOptions fileLoadsOptions;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private String emulatorEndpoint;
        @Nullable private String emulatorRestEndpoint;
        @Nullable private Integer parallelism;
        private List<String> metadataKeys = Collections.emptyList();

        private Builder() {}

        /**
         * Sets the physical columns of the table. Required.
         *
         * @param physicalDataType the physical row type
         * @return this builder
         */
        public Builder physicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        /**
         * Sets the destination table. Required.
         *
         * @param destination the destination table
         * @return this builder
         */
        public Builder destination(TableDestination destination) {
            this.destination = destination;
            return this;
        }

        /**
         * Sets how the columns derive a BigQuery schema. Required.
         *
         * @param schemaOptions the schema derivation options
         * @return this builder
         */
        public Builder schemaOptions(RowDataSchemaOptions schemaOptions) {
            this.schemaOptions = schemaOptions;
            return this;
        }

        /** Sets whether rows are written as BigQuery CDC mutations. */
        public Builder cdcEnabled(boolean cdcEnabled) {
            this.cdcEnabled = cdcEnabled;
            return this;
        }

        /** Sets the Debezium MySQL source UUIDs in causal epoch order. */
        public Builder debeziumMySqlSourceUuids(List<String> sourceUuids) {
            this.debeziumMySqlSourceUuids = new ArrayList<>(sourceUuids);
            return this;
        }

        /** Sets the TiDB cluster ID whose TiCDC commit TSOs this sink orders. */
        public Builder tiCdcClusterId(@Nullable String tiCdcClusterId) {
            this.tiCdcClusterId = tiCdcClusterId;
            return this;
        }

        /** Sets the physical column indexes of the declared primary key. */
        public Builder primaryKeyIndexes(int[] primaryKeyIndexes) {
            this.primaryKeyIndexes = primaryKeyIndexes.clone();
            return this;
        }

        /**
         * Sets the write method, or {@code null} to leave it at the connector's default.
         *
         * @param writeMethod the write method, or {@code null}
         * @return this builder
         */
        public Builder writeMethod(@Nullable WriteMethod writeMethod) {
            this.writeMethod = writeMethod;
            return this;
        }

        /**
         * Sets the create disposition, or {@code null} to leave it at the connector's default.
         *
         * @param createDisposition the create disposition, or {@code null}
         * @return this builder
         */
        public Builder createDisposition(@Nullable CreateDisposition createDisposition) {
            this.createDisposition = createDisposition;
            return this;
        }

        /**
         * Sets the settings a created table takes, or {@code null} to leave it unpartitioned and
         * unclustered.
         *
         * @param tableCreateOptions the creation settings, or {@code null}
         * @return this builder
         */
        public Builder tableCreateOptions(@Nullable TableCreateOptions tableCreateOptions) {
            this.tableCreateOptions = tableCreateOptions;
            return this;
        }

        /** Sets the desired CDC table contract, or {@code null} when CDC is disabled. */
        public Builder cdcTableOptions(@Nullable CdcTableOptions cdcTableOptions) {
            this.cdcTableOptions = cdcTableOptions;
            return this;
        }

        /** Sets the existing CDC table policy, or {@code null} when CDC is disabled. */
        public Builder cdcTableReconciliationPolicy(
                @Nullable CdcTableReconciliationPolicy cdcTableReconciliationPolicy) {
            this.cdcTableReconciliationPolicy = cdcTableReconciliationPolicy;
            return this;
        }

        /**
         * Sets the BigQuery location, or {@code null} to let the service resolve it.
         *
         * @param location the location, or {@code null}
         * @return this builder
         */
        public Builder location(@Nullable String location) {
            this.location = location;
            return this;
        }

        /**
         * Sets the schema update options, or {@code null} to leave them at the connector's default.
         *
         * @param schemaUpdateOptions the schema update options, or {@code null}
         * @return this builder
         */
        public Builder schemaUpdateOptions(@Nullable SchemaUpdateOptions schemaUpdateOptions) {
            this.schemaUpdateOptions = schemaUpdateOptions;
            return this;
        }

        /**
         * Sets the default-stream tuning, or {@code null} to leave every knob at its default.
         *
         * @param defaultStreamOptions the tuning, or {@code null}
         * @return this builder
         */
        public Builder defaultStreamOptions(@Nullable DefaultStreamOptions defaultStreamOptions) {
            this.defaultStreamOptions = defaultStreamOptions;
            return this;
        }

        /**
         * Sets the buffered-stream tuning, which the connector requires under {@code
         * STORAGE_API_EXACTLY_ONCE} and rejects under the other write methods.
         *
         * @param bufferedStreamOptions the tuning, or {@code null}
         * @return this builder
         */
        public Builder bufferedStreamOptions(
                @Nullable BufferedStreamOptions bufferedStreamOptions) {
            this.bufferedStreamOptions = bufferedStreamOptions;
            return this;
        }

        /**
         * Sets the FILE_LOADS options, which the connector requires under {@code FILE_LOADS} and
         * rejects under the other write methods.
         *
         * @param fileLoadsOptions the options, or {@code null}
         * @return this builder
         */
        public Builder fileLoadsOptions(@Nullable FileLoadsOptions fileLoadsOptions) {
            this.fileLoadsOptions = fileLoadsOptions;
            return this;
        }

        /** Sets the runtime service-account key-file path, or {@code null} for ADC. */
        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        /**
         * Sets the emulator's gRPC endpoint, or {@code null} for the real service.
         *
         * @param emulatorEndpoint the endpoint, or {@code null}
         * @return this builder
         */
        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        /**
         * Sets the emulator's REST endpoint, or {@code null} for the real service.
         *
         * @param emulatorRestEndpoint the endpoint, or {@code null}
         * @return this builder
         */
        public Builder emulatorRestEndpoint(@Nullable String emulatorRestEndpoint) {
            this.emulatorRestEndpoint = emulatorRestEndpoint;
            return this;
        }

        /**
         * Sets the sink parallelism, or {@code null} for the planner's own.
         *
         * @param parallelism the parallelism, or {@code null}
         * @return this builder
         */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /** Restores the writable metadata selection when a sink is copied. */
        Builder metadataKeys(List<String> metadataKeys) {
            this.metadataKeys = new ArrayList<>(metadataKeys);
            return this;
        }

        /**
         * Builds the sink.
         *
         * @return the sink
         */
        public BigQueryDynamicSink build() {
            Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
            Preconditions.checkNotNull(destination, "destination must not be null");
            Preconditions.checkNotNull(schemaOptions, "schemaOptions must not be null");
            Preconditions.checkNotNull(primaryKeyIndexes, "primaryKeyIndexes must not be null");
            Preconditions.checkState(
                    !cdcEnabled || primaryKeyIndexes.length > 0,
                    "cdcEnabled requires at least one primaryKeyIndexes entry");
            return new BigQueryDynamicSink(this);
        }
    }
}
