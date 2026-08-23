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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceBuilder;
import io.github.flink.gcp.connector.spanner.table.ChangeStreamChangelogMode;
import io.github.flink.gcp.connector.spanner.table.ChangeStreamStartPositionMapper;
import io.github.flink.gcp.connector.spanner.table.OptionSetters;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** An unbounded Table API source for one table observed through a Spanner change stream. */
@Internal
public final class SpannerChangeStreamDynamicSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsSourceWatermark {
    private final SpannerTableSchemaConverter schema;
    private final DatabaseDestination database;
    private final SpannerTableName table;
    private final String changeStreamName;
    private final ChangeStreamChangelogMode changelogMode;
    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    private final Duration absentRetentionFallback;
    private final Duration heartbeatInterval;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final int maxConcurrentQueriesPerSubtask;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;

    private DataType producedDataType;
    private List<String> metadataKeys;

    public static SpannerChangeStreamDynamicSource from(
            SpannerTableSchemaConverter schema,
            DataType producedDataType,
            ReadableConfig config,
            SpannerTableName table) {
        return new SpannerChangeStreamDynamicSource(
                schema,
                DatabaseDestination.of(
                        config.get(SpannerConnectorOptions.PROJECT),
                        config.get(SpannerConnectorOptions.INSTANCE),
                        config.get(SpannerConnectorOptions.DATABASE)),
                table,
                producedDataType,
                Collections.emptyList(),
                config.get(SpannerConnectorOptions.SCAN_CHANGE_STREAM_NAME),
                config.get(SpannerConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE),
                ChangeStreamStartPositionMapper.startup(config),
                ChangeStreamStartPositionMapper.resumeFallback(config),
                config.get(SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK),
                config.get(SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL),
                config.getOptional(SpannerConnectorOptions.SCAN_RPC_PRIORITY).orElse(null),
                config.get(SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK),
                config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
    }

    private SpannerChangeStreamDynamicSource(
            SpannerTableSchemaConverter schema,
            DatabaseDestination database,
            SpannerTableName table,
            DataType producedDataType,
            List<String> metadataKeys,
            String changeStreamName,
            ChangeStreamChangelogMode changelogMode,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            Duration absentRetentionFallback,
            Duration heartbeatInterval,
            @Nullable SpannerRpcPriority rpcPriority,
            int maxConcurrentQueriesPerSubtask,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            @Nullable Integer parallelism) {
        this.schema = schema;
        this.database = database;
        this.table = table;
        this.producedDataType = producedDataType;
        this.metadataKeys =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        metadataKeys, "metadataKeys must not be null")));
        this.changeStreamName = changeStreamName;
        this.changelogMode = changelogMode;
        this.startPosition = startPosition;
        this.resumeFallback = resumeFallback;
        this.absentRetentionFallback = absentRetentionFallback;
        this.heartbeatInterval = heartbeatInterval;
        this.rpcPriority = rpcPriority;
        this.maxConcurrentQueriesPerSubtask = maxConcurrentQueriesPerSubtask;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.parallelism = parallelism;
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return ReadableMetadata.listAll();
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        List<String> selected =
                new ArrayList<>(
                        Preconditions.checkNotNull(metadataKeys, "metadataKeys must not be null"));
        DataType selectedProducedType =
                Preconditions.checkNotNull(producedDataType, "producedDataType must not be null");
        for (String key : selected) {
            ReadableMetadata.of(key);
        }
        this.metadataKeys = Collections.unmodifiableList(selected);
        this.producedDataType = selectedProducedType;
    }

    @Override
    public void applySourceWatermark() {
        // The FLIP-27 source already emits commit timestamps and heartbeat watermarks.
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (changelogMode == ChangeStreamChangelogMode.UPSERT) {
            return CrossVersionChangelogMode.upsert();
        }
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
        ReadableMetadata[] selectedMetadata =
                metadataKeys.stream().map(ReadableMetadata::of).toArray(ReadableMetadata[]::new);
        SpannerChangeStreamSourceBuilder<RowData> builder =
                SpannerChangeStreamSource.<RowData>builder()
                        .database(database)
                        .changeStreamName(changeStreamName)
                        .deserializer(
                                new SpannerChangeStreamRowDataDeserializationSchema(
                                        schema,
                                        table,
                                        changelogMode,
                                        selectedMetadata,
                                        producedType))
                        .startPosition(startPosition);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK.key(),
                absentRetentionFallback,
                builder::absentRetentionFallback);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL.key(),
                heartbeatInterval,
                builder::heartbeatInterval);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK.key(),
                maxConcurrentQueriesPerSubtask,
                builder::maxConcurrentQueriesPerSubtask);
        if (resumeFallback != null) {
            builder.resumeFallback(resumeFallback);
        }
        if (rpcPriority != null) {
            builder.rpcPriority(rpcPriority);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        return SourceProvider.of(builder.build(), parallelism);
    }

    @Override
    public DynamicTableSource copy() {
        return new SpannerChangeStreamDynamicSource(
                schema,
                database,
                table,
                producedDataType,
                metadataKeys,
                changeStreamName,
                changelogMode,
                startPosition,
                resumeFallback,
                absentRetentionFallback,
                heartbeatInterval,
                rpcPriority,
                maxConcurrentQueriesPerSubtask,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism);
    }

    @Override
    public String asSummaryString() {
        return "Spanner change-stream table source";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerChangeStreamDynamicSource)) {
            return false;
        }
        SpannerChangeStreamDynamicSource that = (SpannerChangeStreamDynamicSource) other;
        return maxConcurrentQueriesPerSubtask == that.maxConcurrentQueriesPerSubtask
                && schema.equals(that.schema)
                && database.equals(that.database)
                && table.equals(that.table)
                && producedDataType.equals(that.producedDataType)
                && metadataKeys.equals(that.metadataKeys)
                && changeStreamName.equals(that.changeStreamName)
                && changelogMode == that.changelogMode
                && startPosition.equals(that.startPosition)
                && Objects.equals(resumeFallback, that.resumeFallback)
                && absentRetentionFallback.equals(that.absentRetentionFallback)
                && heartbeatInterval.equals(that.heartbeatInterval)
                && rpcPriority == that.rpcPriority
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                table,
                producedDataType,
                metadataKeys,
                changeStreamName,
                changelogMode,
                startPosition,
                resumeFallback,
                absentRetentionFallback,
                heartbeatInterval,
                rpcPriority,
                maxConcurrentQueriesPerSubtask,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism);
    }
}
