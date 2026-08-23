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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceBuilder;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.ChangeStreamChangelogMode;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;
import io.github.flink.gcp.connector.bigtable.table.SelectedCellTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Table changelog source backed by the DataStream Change Streams source.
 *
 * <p>Built through {@link #builder()} rather than a constructor, for the reason {@link
 * io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink} records and this class
 * carries further: the positional list was repeated across three constructors, {@link #copy()},
 * {@link #equals(Object)} and {@link #hashCode()} with no compiler check that the repetitions
 * agree, and four of the private constructor's fifteen parameters were {@code String}s — {@code
 * appProfileId} and {@code serviceAccountKeyFile} side by side, then {@code selectedCellFamily} and
 * {@code selectedCellSourceClusterId} two apart — so transposing any two of them compiled.
 *
 * <p>The changelog mode is derived rather than passed: setting {@code selectedCellSchema} selects
 * it, and the builder then requires the rest of the selected-cell group and rejects a {@code
 * physicalDataType} the schema already supplies.
 */
@Internal
public final class BigtableChangeStreamDynamicSource
        implements ScanTableSource, SupportsReadingMetadata {

    private final TableDestination destination;
    private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    @Nullable private final Instant boundedTimestamp;
    @Nullable private final Integer maxConcurrentStreamsPerSubtask;
    @Nullable private final Integer parallelism;
    private final DataType physicalDataType;
    private final ChangeStreamChangelogMode changelogMode;
    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    @Nullable private final SelectedCellTableSchema selectedCellSchema;
    @Nullable private final String selectedCellFamily;
    @Nullable private final ByteString selectedCellQualifier;
    @Nullable private final String selectedCellSourceClusterId;
    @Nullable private final TrailingBytes trailingBytes;

    /** Metadata keys selected by the planner, in the order appended to produced rows. */
    private List<String> metadataKeys;

    /** The physical envelope plus the metadata selected by the planner. */
    private DataType producedDataType;

    private BigtableChangeStreamDynamicSource(Builder builder) {
        this.destination =
                Preconditions.checkNotNull(builder.destination, "destination must not be null");
        this.appProfileId =
                Preconditions.checkNotNull(builder.appProfileId, "appProfileId must not be null");
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.startPosition = builder.startPosition;
        this.resumeFallback = builder.resumeFallback;
        this.boundedTimestamp = builder.boundedTimestamp;
        this.maxConcurrentStreamsPerSubtask = builder.maxConcurrentStreamsPerSubtask;
        this.parallelism = builder.parallelism;
        this.changelogMode =
                builder.selectedCellSchema == null
                        ? ChangeStreamChangelogMode.ENVELOPE
                        : ChangeStreamChangelogMode.SELECTED_CELL;
        if (changelogMode == ChangeStreamChangelogMode.ENVELOPE) {
            checkEnvelopeCarriesNoSelectedCell(builder);
            this.physicalDataType =
                    Preconditions.checkNotNull(
                            builder.physicalDataType, "physicalDataType must not be null");
            this.decodingFormat = null;
            this.selectedCellSchema = null;
            this.selectedCellFamily = null;
            this.selectedCellQualifier = null;
            this.selectedCellSourceClusterId = null;
            this.trailingBytes = null;
        } else {
            Preconditions.checkArgument(
                    builder.physicalDataType == null,
                    "physicalDataType must not be set beside selectedCellSchema: the selected-cell"
                            + " physical type is the schema's.");
            this.physicalDataType = builder.selectedCellSchema.getPhysicalDataType();
            this.decodingFormat =
                    Preconditions.checkNotNull(
                            builder.decodingFormat, "decodingFormat must not be null");
            this.selectedCellSchema = builder.selectedCellSchema;
            this.selectedCellFamily =
                    Preconditions.checkNotNull(
                            builder.selectedCellFamily, "selectedCellFamily must not be null");
            this.selectedCellQualifier =
                    Preconditions.checkNotNull(
                            builder.selectedCellQualifier,
                            "selectedCellQualifier must not be null");
            this.selectedCellSourceClusterId =
                    Preconditions.checkNotNull(
                            builder.selectedCellSourceClusterId,
                            "selectedCellSourceClusterId must not be null");
            this.trailingBytes =
                    Preconditions.checkNotNull(
                            builder.trailingBytes, "trailingBytes must not be null");
        }
        this.metadataKeys = Collections.emptyList();
        this.producedDataType = physicalDataType;
    }

    /**
     * Returns a builder for this source.
     *
     * @return a builder with nothing set
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Rejects a selected-cell value left on an envelope source.
     *
     * <p>Two positional constructors could not express this, which is exactly why it has to be
     * checked now: with one builder, a caller that sets {@code selectedCellFamily} and forgets
     * {@code selectedCellSchema} would otherwise build an envelope source carrying selected-cell
     * configuration that nothing reads.
     */
    private static void checkEnvelopeCarriesNoSelectedCell(Builder builder) {
        Preconditions.checkArgument(
                builder.decodingFormat == null
                        && builder.selectedCellFamily == null
                        && builder.selectedCellQualifier == null
                        && builder.selectedCellSourceClusterId == null
                        && builder.trailingBytes == null,
                "decodingFormat, selectedCellFamily, selectedCellQualifier,"
                        + " selectedCellSourceClusterId and trailingBytes belong to the"
                        + " selected-cell mode and must not be set without selectedCellSchema.");
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return ChangeStreamReadableMetadata.listAll();
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        for (String key : metadataKeys) {
            ChangeStreamReadableMetadata.of(key);
        }
        this.metadataKeys = Collections.unmodifiableList(new ArrayList<>(metadataKeys));
        this.producedDataType =
                Preconditions.checkNotNull(producedDataType, "producedDataType must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode == ChangeStreamChangelogMode.ENVELOPE
                ? ChangelogMode.insertOnly()
                : ChangelogMode.upsert();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> typeInformation = context.createTypeInformation(producedDataType);
        ChangeStreamReadableMetadata[] selectedMetadata =
                metadataKeys.stream()
                        .map(ChangeStreamReadableMetadata::of)
                        .toArray(ChangeStreamReadableMetadata[]::new);
        BigtableChangeStreamSourceBuilder<RowData> builder =
                BigtableChangeStreamSource.<RowData>builder()
                        .table(destination)
                        .appProfileId(appProfileId);
        if (changelogMode == ChangeStreamChangelogMode.ENVELOPE) {
            builder.deserializer(
                    new BigtableChangeStreamMutationRowDataDeserializationSchema(
                            selectedMetadata, typeInformation));
        } else {
            DeserializationSchema<RowData> payloadDeserializer =
                    decodingFormat.createRuntimeDecoder(
                            context, selectedCellSchema.getPayloadDataType());
            builder.deserializer(
                    new SelectedCellRowDataDeserializationSchema(
                            payloadDeserializer,
                            new SelectedCellMutationClassifier(
                                    selectedCellFamily,
                                    selectedCellQualifier,
                                    selectedCellSourceClusterId),
                            selectedCellSchema,
                            trailingBytes,
                            selectedMetadata,
                            typeInformation));
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (startPosition != null) {
            builder.startPosition(startPosition);
        }
        if (resumeFallback != null) {
            builder.resumeFallback(resumeFallback);
        }
        if (boundedTimestamp != null) {
            builder.boundedTimestamp(boundedTimestamp);
        }
        OptionSetters.accept(
                BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK.key(),
                maxConcurrentStreamsPerSubtask,
                builder::maxConcurrentStreamsPerSubtask);
        return SourceProvider.of(builder.build(), parallelism);
    }

    @Override
    public DynamicTableSource copy() {
        Builder builder =
                builder()
                        .destination(destination)
                        .appProfileId(appProfileId)
                        .serviceAccountKeyFile(serviceAccountKeyFile)
                        .startPosition(startPosition)
                        .resumeFallback(resumeFallback)
                        .boundedTimestamp(boundedTimestamp)
                        .maxConcurrentStreamsPerSubtask(maxConcurrentStreamsPerSubtask)
                        .parallelism(parallelism);
        // One branch, over the two fields that decide the mode rather than over two argument
        // lists: everything above is shared, and the constructor re-derives the mode from what
        // this sets.
        if (changelogMode == ChangeStreamChangelogMode.ENVELOPE) {
            builder.physicalDataType(physicalDataType);
        } else {
            builder.selectedCellSchema(selectedCellSchema)
                    .decodingFormat(decodingFormat)
                    .selectedCellFamily(selectedCellFamily)
                    .selectedCellQualifier(selectedCellQualifier)
                    .selectedCellSourceClusterId(selectedCellSourceClusterId)
                    .trailingBytes(trailingBytes);
        }
        BigtableChangeStreamDynamicSource copy = builder.build();
        copy.applyReadableMetadata(metadataKeys, producedDataType);
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Bigtable Change Streams";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BigtableChangeStreamDynamicSource)) {
            return false;
        }
        BigtableChangeStreamDynamicSource that = (BigtableChangeStreamDynamicSource) other;
        return destination.equals(that.destination)
                && appProfileId.equals(that.appProfileId)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(startPosition, that.startPosition)
                && Objects.equals(resumeFallback, that.resumeFallback)
                && Objects.equals(boundedTimestamp, that.boundedTimestamp)
                && Objects.equals(
                        maxConcurrentStreamsPerSubtask, that.maxConcurrentStreamsPerSubtask)
                && Objects.equals(parallelism, that.parallelism)
                && physicalDataType.equals(that.physicalDataType)
                && changelogMode == that.changelogMode
                && Objects.equals(decodingFormat, that.decodingFormat)
                && Objects.equals(selectedCellSchema, that.selectedCellSchema)
                && Objects.equals(selectedCellFamily, that.selectedCellFamily)
                && Objects.equals(selectedCellQualifier, that.selectedCellQualifier)
                && Objects.equals(selectedCellSourceClusterId, that.selectedCellSourceClusterId)
                && trailingBytes == that.trailingBytes
                && metadataKeys.equals(that.metadataKeys)
                && producedDataType.equals(that.producedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                destination,
                appProfileId,
                serviceAccountKeyFile,
                startPosition,
                resumeFallback,
                boundedTimestamp,
                maxConcurrentStreamsPerSubtask,
                parallelism,
                physicalDataType,
                changelogMode,
                decodingFormat,
                selectedCellSchema,
                selectedCellFamily,
                selectedCellQualifier,
                selectedCellSourceClusterId,
                trailingBytes,
                metadataKeys,
                producedDataType);
    }

    /**
     * Builds a {@link BigtableChangeStreamDynamicSource}.
     *
     * <p>Setting {@link #selectedCellSchema(SelectedCellTableSchema)} selects the selected-cell
     * changelog mode and makes the rest of that group required; leaving it unset selects the
     * envelope mode and makes {@link #physicalDataType(DataType)} required.
     */
    public static final class Builder {

        @Nullable private TableDestination destination;
        @Nullable private String appProfileId;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private StartPosition startPosition;
        @Nullable private StartPosition resumeFallback;
        @Nullable private Instant boundedTimestamp;
        @Nullable private Integer maxConcurrentStreamsPerSubtask;
        @Nullable private Integer parallelism;
        @Nullable private DataType physicalDataType;
        @Nullable private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
        @Nullable private SelectedCellTableSchema selectedCellSchema;
        @Nullable private String selectedCellFamily;
        @Nullable private ByteString selectedCellQualifier;
        @Nullable private String selectedCellSourceClusterId;
        @Nullable private TrailingBytes trailingBytes;

        private Builder() {}

        /**
         * Sets the table to read. Required.
         *
         * @param destination the table
         * @return this builder
         */
        public Builder destination(TableDestination destination) {
            this.destination = destination;
            return this;
        }

        /**
         * Sets the single-cluster application profile to route through. Required.
         *
         * @param appProfileId the application profile
         * @return this builder
         */
        public Builder appProfileId(String appProfileId) {
            this.appProfileId = appProfileId;
            return this;
        }

        /**
         * Sets the service-account key-file path, or {@code null} to keep ADC.
         *
         * @param serviceAccountKeyFile the path
         * @return this builder
         */
        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        /**
         * Sets where a fresh run starts, or {@code null} for the source's default.
         *
         * @param startPosition the start position
         * @return this builder
         */
        public Builder startPosition(@Nullable StartPosition startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        /**
         * Sets where a restored position that has expired restarts, or {@code null} to fail
         * instead.
         *
         * @param resumeFallback the fallback position
         * @return this builder
         */
        public Builder resumeFallback(@Nullable StartPosition resumeFallback) {
            this.resumeFallback = resumeFallback;
            return this;
        }

        /**
         * Sets the timestamp that bounds the run, or {@code null} for an unbounded one.
         *
         * @param boundedTimestamp the bounded timestamp
         * @return this builder
         */
        public Builder boundedTimestamp(@Nullable Instant boundedTimestamp) {
            this.boundedTimestamp = boundedTimestamp;
            return this;
        }

        /**
         * Sets how many partition streams one subtask opens at once, or {@code null} for the
         * source's default.
         *
         * @param maxConcurrentStreamsPerSubtask the maximum
         * @return this builder
         */
        public Builder maxConcurrentStreamsPerSubtask(
                @Nullable Integer maxConcurrentStreamsPerSubtask) {
            this.maxConcurrentStreamsPerSubtask = maxConcurrentStreamsPerSubtask;
            return this;
        }

        /**
         * Sets the source parallelism, or {@code null} to leave it to the planner.
         *
         * @param parallelism the parallelism
         * @return this builder
         */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Sets the physical envelope type. Required in the envelope mode, and rejected in the
         * selected-cell mode, where {@link #selectedCellSchema(SelectedCellTableSchema)} supplies
         * it.
         *
         * @param physicalDataType the physical envelope type
         * @return this builder
         */
        public Builder physicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        /**
         * Sets the selected-cell schema, which selects the selected-cell changelog mode.
         *
         * @param selectedCellSchema the schema
         * @return this builder
         */
        public Builder selectedCellSchema(SelectedCellTableSchema selectedCellSchema) {
            this.selectedCellSchema = selectedCellSchema;
            return this;
        }

        /**
         * Sets the format decoding the selected cell's payload. Required in the selected-cell mode.
         *
         * @param decodingFormat the format
         * @return this builder
         */
        public Builder decodingFormat(
                DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
            this.decodingFormat = decodingFormat;
            return this;
        }

        /**
         * Sets the column family holding the selected cell. Required in the selected-cell mode.
         *
         * @param selectedCellFamily the family
         * @return this builder
         */
        public Builder selectedCellFamily(String selectedCellFamily) {
            this.selectedCellFamily = selectedCellFamily;
            return this;
        }

        /**
         * Sets the qualifier of the selected cell. Required in the selected-cell mode.
         *
         * @param selectedCellQualifier the qualifier
         * @return this builder
         */
        public Builder selectedCellQualifier(ByteString selectedCellQualifier) {
            this.selectedCellQualifier = selectedCellQualifier;
            return this;
        }

        /**
         * Sets the cluster whose writes are read as source-of-truth. Required in the selected-cell
         * mode.
         *
         * @param selectedCellSourceClusterId the cluster id
         * @return this builder
         */
        public Builder selectedCellSourceClusterId(String selectedCellSourceClusterId) {
            this.selectedCellSourceClusterId = selectedCellSourceClusterId;
            return this;
        }

        /**
         * Sets what the primary-key decode does with bytes past the declared layout. Required in
         * the selected-cell mode, which is the only mode that decodes through the cell codec.
         *
         * @param trailingBytes the fixed-width decode policy
         * @return this builder
         */
        public Builder trailingBytes(TrailingBytes trailingBytes) {
            this.trailingBytes = trailingBytes;
            return this;
        }

        /**
         * Builds the source.
         *
         * @return the source
         */
        public BigtableChangeStreamDynamicSource build() {
            return new BigtableChangeStreamDynamicSource(this);
        }
    }
}
