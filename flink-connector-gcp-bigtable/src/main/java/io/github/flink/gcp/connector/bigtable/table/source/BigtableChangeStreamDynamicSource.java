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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceBuilder;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Insert-only generic mutation-envelope source backed by the DataStream Change Streams source. */
@Internal
public final class BigtableChangeStreamDynamicSource
        implements ScanTableSource, SupportsReadingMetadata {

    private final TableDestination destination;
    private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    @Nullable private final Instant endTime;
    @Nullable private final Integer maxConcurrentStreamsPerSubtask;
    @Nullable private final Integer parallelism;
    private final DataType physicalDataType;

    /** Metadata keys selected by the planner, in the order appended to produced rows. */
    private List<String> metadataKeys;

    /** The physical envelope plus the metadata selected by the planner. */
    private DataType producedDataType;

    public BigtableChangeStreamDynamicSource(
            TableDestination destination,
            String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant endTime,
            @Nullable Integer maxConcurrentStreamsPerSubtask,
            @Nullable Integer parallelism,
            DataType producedDataType) {
        this.destination = Objects.requireNonNull(destination);
        this.appProfileId = Objects.requireNonNull(appProfileId);
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.startPosition = startPosition;
        this.resumeFallback = resumeFallback;
        this.endTime = endTime;
        this.maxConcurrentStreamsPerSubtask = maxConcurrentStreamsPerSubtask;
        this.parallelism = parallelism;
        this.physicalDataType = Objects.requireNonNull(producedDataType);
        this.metadataKeys = Collections.emptyList();
        this.producedDataType = producedDataType;
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
        this.producedDataType = Objects.requireNonNull(producedDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
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
                        .appProfileId(appProfileId)
                        .deserializer(
                                new ChangeStreamMutationRowDataDeserializationSchema(
                                        selectedMetadata, typeInformation));
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (startPosition != null) {
            builder.startPosition(startPosition);
        }
        if (resumeFallback != null) {
            builder.resumeFallback(resumeFallback);
        }
        if (endTime != null) {
            builder.endTime(endTime);
        }
        if (maxConcurrentStreamsPerSubtask != null) {
            builder.maxConcurrentStreamsPerSubtask(maxConcurrentStreamsPerSubtask);
        }
        return SourceProvider.of(builder.build(), parallelism);
    }

    @Override
    public DynamicTableSource copy() {
        BigtableChangeStreamDynamicSource copy =
                new BigtableChangeStreamDynamicSource(
                        destination,
                        appProfileId,
                        serviceAccountKeyFile,
                        startPosition,
                        resumeFallback,
                        endTime,
                        maxConcurrentStreamsPerSubtask,
                        parallelism,
                        physicalDataType);
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
                && Objects.equals(endTime, that.endTime)
                && Objects.equals(
                        maxConcurrentStreamsPerSubtask, that.maxConcurrentStreamsPerSubtask)
                && Objects.equals(parallelism, that.parallelism)
                && physicalDataType.equals(that.physicalDataType)
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
                endTime,
                maxConcurrentStreamsPerSubtask,
                parallelism,
                physicalDataType,
                metadataKeys,
                producedDataType);
    }
}
