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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.conditional.EmptyBranchPolicy;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteSink;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.InsertOnlyInputMode;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code bigtable} table sink.
 *
 * <p>Deliberately takes no {@code ReadableConfig}: turning a DDL option into a value happens in one
 * place, {@link io.github.flink.gcp.connector.bigtable.table.BigtableDynamicTableFactory the
 * factory}, so this class has no configuration vocabulary at all.
 *
 * <p>{@code SupportsPartitioning} is not implemented: a Bigtable table is partitioned by row-key
 * range, which the service chooses and moves on its own, so there is nothing a {@code PARTITIONED
 * BY} clause could name. {@link SupportsWritingMetadata} exposes the timestamp shared by every cell
 * the row writes, except in append and increment modes, whose timestamps are service-assigned.
 *
 * <p>Built through {@link #builder()} rather than a constructor, for the reason {@code
 * BigQueryDynamicSink} records: a positional list is repeated four times over — the constructor,
 * {@link #copy()}, {@link #equals(Object)} and {@link #hashCode()} — with no compiler check that
 * the repetitions agree, and this one carries several nullable and policy values.
 */
@Internal
public final class BigtableDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private final BigtableTableSchema schema;
    private final TableDestination destination;
    private final String nullStringLiteral;
    @Nullable private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    private final BigtableWriterOptions writerOptions;
    private final WriteMode writeMode;
    private final BigtableRequestOptions requestOptions;
    @Nullable private final EmptyBranchPolicy emptyBranchPolicy;
    @Nullable private final CreateDisposition createDisposition;
    @Nullable private final TableCreateOptions tableCreateOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;
    private final boolean keyOnlyDeletesAreSafe;
    private final InsertOnlyInputMode insertOnlyInputMode;
    private final boolean truncateCellTimestampToMillis;

    /** Metadata keys the planner selected, in {@link WritableMetadata#listAll()} order. */
    private List<String> metadataKeys;

    private BigtableDynamicSink(Builder builder) {
        this.schema = Preconditions.checkNotNull(builder.schema, "schema must not be null");
        this.destination =
                Preconditions.checkNotNull(builder.destination, "destination must not be null");
        this.nullStringLiteral =
                Preconditions.checkNotNull(
                        builder.nullStringLiteral, "nullStringLiteral must not be null");
        this.appProfileId = builder.appProfileId;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.writerOptions =
                Preconditions.checkNotNull(builder.writerOptions, "writerOptions must not be null");
        this.writeMode = builder.writeMode;
        this.requestOptions = builder.requestOptions;
        this.emptyBranchPolicy = builder.emptyBranchPolicy;
        this.createDisposition = builder.createDisposition;
        this.tableCreateOptions = builder.tableCreateOptions;
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.parallelism = builder.parallelism;
        this.keyOnlyDeletesAreSafe = builder.keyOnlyDeletesAreSafe;
        this.insertOnlyInputMode =
                Preconditions.checkNotNull(
                        builder.insertOnlyInputMode, "insertOnlyInputMode must not be null");
        this.truncateCellTimestampToMillis = builder.truncateCellTimestampToMillis;
        this.metadataKeys =
                Preconditions.checkNotNull(builder.metadataKeys, "metadataKeys must not be null");
    }

    /**
     * Returns a builder for this sink.
     *
     * @return a builder with nothing set
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return writeMode == WriteMode.APPEND || writeMode == WriteMode.INCREMENT
                ? Collections.emptyMap()
                : WritableMetadata.listAll();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        // consumedDataType is discarded: a position follows from the selection order and the
        // physical column count the DDL model already carries, so keeping it would add a field to
        // equals and hashCode that says nothing the schema does not.
        if (!metadataKeys.isEmpty()
                && (writeMode == WriteMode.APPEND || writeMode == WriteMode.INCREMENT)) {
            throw new ValidationException(
                    "Writable metadata cannot be used with 'sink.write-mode' = '"
                            + writeMode
                            + "'.");
        }
        metadataKeys.forEach(WritableMetadata::of);
        this.metadataKeys = Collections.unmodifiableList(new ArrayList<>(metadataKeys));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (writeMode != WriteMode.UPSERT) {
            if (!requestedMode.containsOnly(RowKind.INSERT)) {
                throw new ValidationException(
                        "Bigtable 'sink.write-mode' = '"
                                + writeMode
                                + "' requires INSERT-only input.");
            }
            return ChangelogMode.insertOnly();
        }
        // The compatibility mode restores #488's append answer for a statement that cannot carry
        // Flink 2.3's ON CONFLICT syntax. It is deliberately narrow: an updating query must still
        // expose Bigtable's physical upsert behavior so the planner can complete and order it.
        if (requestedMode.containsOnly(RowKind.INSERT)
                && insertOnlyInputMode == InsertOnlyInputMode.INSERT_ONLY) {
            return ChangelogMode.insertOnly();
        }
        // For an updating query, a Bigtable write is an upsert on the row key by construction:
        // setCell overwrites, and there is no retract path to offer instead.
        //
        // Whether a delete may carry the upsert key *alone* is a different question, and the
        // answer is whether that key is the row key. A declared PRIMARY KEY guarantees it, the
        // factory having already refused one naming anything else; with none declared the planner
        // keys its upserts on whatever the query happens to be unique by, and a key-only delete
        // then reaches RowDataSerializationSchema with the row-key column null (#470).
        //
        // Measured on Flink 2.2.1 against an upsert source keyed on a non-row-key column: with a
        // key declared the plan already carries ChangelogNormalize and upsertMaterialize, so the
        // row key is filled in either way; with none, answering false is what puts
        // ChangelogNormalize there.
        return CrossVersionChangelogMode.upsert(keyOnlyDeletesAreSafe);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        WritableMetadata[] selected =
                metadataKeys.stream().map(WritableMetadata::of).toArray(WritableMetadata[]::new);
        if (writeMode == WriteMode.APPEND || writeMode == WriteMode.INCREMENT) {
            BigtableReadModifyWriteSinkBuilder<RowData> requests =
                    BigtableReadModifyWriteSink.<RowData>builder()
                            .table(destination)
                            .serializer(
                                    new RowDataReadModifyWriteSerializationSchema(
                                            schema, writeMode))
                            .requestOptions(requestOptions);
            if (appProfileId != null) {
                requests.appProfileId(appProfileId);
            }
            if (serviceAccountKeyFile != null) {
                requests.serviceAccountKeyFile(serviceAccountKeyFile);
            }
            if (emulatorEndpoint != null) {
                requests.emulatorEndpoint(
                        EmulatorEndpoint.parse(emulatorEndpoint, "emulator-endpoint"));
            }
            return SinkV2Provider.of(requests.build(), parallelism);
        }
        if (writeMode == WriteMode.INSERT_IF_ABSENT) {
            BigtableConditionalSinkBuilder<RowData> conditional =
                    BigtableConditionalSink.<RowData>builder()
                            .table(destination)
                            .serializer(
                                    new RowDataConditionalSerializationSchema(
                                            schema,
                                            nullStringLiteral,
                                            selected,
                                            truncateCellTimestampToMillis))
                            .requestOptions(requestOptions);
            if (appProfileId != null) {
                conditional.appProfileId(appProfileId);
            }
            if (serviceAccountKeyFile != null) {
                conditional.serviceAccountKeyFile(serviceAccountKeyFile);
            }
            if (emulatorEndpoint != null) {
                conditional.emulatorEndpoint(
                        EmulatorEndpoint.parse(emulatorEndpoint, "emulator-endpoint"));
            }
            if (emptyBranchPolicy != null) {
                conditional.emptyBranchPolicy(emptyBranchPolicy);
            }
            return SinkV2Provider.of(conditional.build(), parallelism);
        }
        BigtableSinkBuilder<RowData> builder =
                BigtableSink.<RowData>builder()
                        .table(destination)
                        .serializer(
                                new RowDataSerializationSchema(
                                        schema,
                                        nullStringLiteral,
                                        selected,
                                        truncateCellTimestampToMillis))
                        .writerOptions(writerOptions);
        if (appProfileId != null) {
            builder.appProfileId(appProfileId);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        if (tableCreateOptions != null) {
            builder.tableCreateOptions(tableCreateOptions);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return builder()
                .schema(schema)
                .destination(destination)
                .nullStringLiteral(nullStringLiteral)
                .appProfileId(appProfileId)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .writerOptions(writerOptions)
                .writeMode(writeMode)
                .requestOptions(requestOptions)
                .emptyBranchPolicy(emptyBranchPolicy)
                .createDisposition(createDisposition)
                .tableCreateOptions(tableCreateOptions)
                .emulatorEndpoint(emulatorEndpoint)
                .parallelism(parallelism)
                .keyOnlyDeletesAreSafe(keyOnlyDeletesAreSafe)
                .insertOnlyInputMode(insertOnlyInputMode)
                .truncateCellTimestampToMillis(truncateCellTimestampToMillis)
                .metadataKeys(metadataKeys)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "Bigtable table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableDynamicSink that = (BigtableDynamicSink) o;
        return schema.equals(that.schema)
                && destination.equals(that.destination)
                && nullStringLiteral.equals(that.nullStringLiteral)
                && Objects.equals(appProfileId, that.appProfileId)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && writerOptions.equals(that.writerOptions)
                && writeMode == that.writeMode
                && requestOptions.equals(that.requestOptions)
                && emptyBranchPolicy == that.emptyBranchPolicy
                && createDisposition == that.createDisposition
                && Objects.equals(tableCreateOptions, that.tableCreateOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && keyOnlyDeletesAreSafe == that.keyOnlyDeletesAreSafe
                && insertOnlyInputMode == that.insertOnlyInputMode
                && truncateCellTimestampToMillis == that.truncateCellTimestampToMillis
                && metadataKeys.equals(that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                destination,
                nullStringLiteral,
                appProfileId,
                serviceAccountKeyFile,
                writerOptions,
                writeMode,
                requestOptions,
                emptyBranchPolicy,
                createDisposition,
                tableCreateOptions,
                emulatorEndpoint,
                parallelism,
                keyOnlyDeletesAreSafe,
                insertOnlyInputMode,
                truncateCellTimestampToMillis,
                metadataKeys);
    }

    /** Collects the sink's values, so no caller has to keep a positional list in order. */
    public static final class Builder {

        private BigtableTableSchema schema;
        private TableDestination destination;
        private String nullStringLiteral;
        @Nullable private String appProfileId;
        @Nullable private String serviceAccountKeyFile;
        private BigtableWriterOptions writerOptions;
        private WriteMode writeMode = WriteMode.UPSERT;
        private BigtableRequestOptions requestOptions = BigtableRequestOptions.builder().build();
        @Nullable private EmptyBranchPolicy emptyBranchPolicy;
        @Nullable private CreateDisposition createDisposition;
        @Nullable private TableCreateOptions tableCreateOptions;
        @Nullable private String emulatorEndpoint;
        @Nullable private Integer parallelism;
        private boolean keyOnlyDeletesAreSafe;
        private InsertOnlyInputMode insertOnlyInputMode;
        private boolean truncateCellTimestampToMillis;
        private List<String> metadataKeys = Collections.emptyList();

        private Builder() {}

        /**
         * @param value the write operation
         * @return this builder
         */
        public Builder writeMode(WriteMode value) {
            this.writeMode = value;
            return this;
        }

        /**
         * @param value the request runtime options
         * @return this builder
         */
        public Builder requestOptions(BigtableRequestOptions value) {
            this.requestOptions = value;
            return this;
        }

        /**
         * @param value the explicit empty-branch policy, or null
         * @return this builder
         */
        public Builder emptyBranchPolicy(@Nullable EmptyBranchPolicy value) {
            this.emptyBranchPolicy = value;
            return this;
        }

        /**
         * @param schema the table's parsed DDL model
         * @return this builder
         */
        public Builder schema(BigtableTableSchema schema) {
            this.schema = schema;
            return this;
        }

        /**
         * @param destination the destination table
         * @return this builder
         */
        public Builder destination(TableDestination destination) {
            this.destination = destination;
            return this;
        }

        /**
         * @param nullStringLiteral the cell value standing for a null character string
         * @return this builder
         */
        public Builder nullStringLiteral(String nullStringLiteral) {
            this.nullStringLiteral = nullStringLiteral;
            return this;
        }

        /**
         * @param appProfileId the app profile the writes are attributed to, or {@code null}
         * @return this builder
         */
        public Builder appProfileId(@Nullable String appProfileId) {
            this.appProfileId = appProfileId;
            return this;
        }

        /**
         * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
         * @return this builder
         */
        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        /**
         * @param writerOptions the writer tuning
         * @return this builder
         */
        public Builder writerOptions(BigtableWriterOptions writerOptions) {
            this.writerOptions = writerOptions;
            return this;
        }

        /**
         * @param createDisposition the create disposition, or {@code null} for the connector's own
         * @return this builder
         */
        public Builder createDisposition(@Nullable CreateDisposition createDisposition) {
            this.createDisposition = createDisposition;
            return this;
        }

        /**
         * @param tableCreateOptions the creation settings, or {@code null} when nothing is created
         * @return this builder
         */
        public Builder tableCreateOptions(@Nullable TableCreateOptions tableCreateOptions) {
            this.tableCreateOptions = tableCreateOptions;
            return this;
        }

        /**
         * @param emulatorEndpoint the emulator's endpoint, or {@code null} for the real service
         * @return this builder
         */
        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        /**
         * @param parallelism the sink parallelism, or {@code null} for the planner's own
         * @return this builder
         */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Says whether the planner may send a delete carrying the upsert key alone, which is safe
         * exactly when that key is the row key. The factory sets it from the DDL's {@code PRIMARY
         * KEY}, having already refused one naming any other column.
         *
         * @param keyOnlyDeletesAreSafe whether a delete may carry the upsert key alone
         * @return this builder
         */
        public Builder keyOnlyDeletesAreSafe(boolean keyOnlyDeletesAreSafe) {
            this.keyOnlyDeletesAreSafe = keyOnlyDeletesAreSafe;
            return this;
        }

        /**
         * @param insertOnlyInputMode the mode advertised for an insert-only requested changelog
         * @return this builder
         */
        public Builder insertOnlyInputMode(InsertOnlyInputMode insertOnlyInputMode) {
            this.insertOnlyInputMode = insertOnlyInputMode;
            return this;
        }

        /**
         * @param truncateCellTimestampToMillis whether explicit cell timestamps lose their
         *     sub-millisecond part before being sent
         * @return this builder
         */
        public Builder truncateCellTimestampToMillis(boolean truncateCellTimestampToMillis) {
            this.truncateCellTimestampToMillis = truncateCellTimestampToMillis;
            return this;
        }

        /**
         * @param metadataKeys the writable metadata the planner selected, in the order it laid the
         *     consumed row out in
         * @return this builder
         */
        Builder metadataKeys(List<String> metadataKeys) {
            this.metadataKeys = Collections.unmodifiableList(new ArrayList<>(metadataKeys));
            return this;
        }

        /**
         * @return the sink
         */
        public BigtableDynamicSink build() {
            return new BigtableDynamicSink(this);
        }
    }
}
