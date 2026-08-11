/*
 * Copyright 2026 laughingman7743
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** A bounded table scan backed by the DataStream Spanner source. */
@Internal
public final class SpannerDynamicSource implements ScanTableSource, SupportsProjectionPushDown {
    private final SpannerTableSchemaConverter schema;
    private final SpannerDatabase database;
    private final String table;
    @Nullable private final Long maxPartitions;
    @Nullable private final Long partitionSizeBytes;
    @Nullable private final Boolean dataBoostEnabled;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final TimestampBound timestampBound;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;
    private DataType producedDataType;
    @Nullable private int[] projectedFields;

    public SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            SpannerDatabase database,
            String table,
            DataType producedDataType,
            ReadableConfig config) {
        this(
                schema,
                database,
                table,
                producedDataType,
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS)
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_SIZE)
                        .map(size -> size.getBytes())
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_DATA_BOOST_ENABLED).orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_RPC_PRIORITY).orElse(null),
                timestampBound(config),
                config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
    }

    private SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            SpannerDatabase database,
            String table,
            DataType producedDataType,
            @Nullable Long maxPartitions,
            @Nullable Long partitionSizeBytes,
            @Nullable Boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority,
            TimestampBound timestampBound,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism) {
        this.schema = schema;
        this.database = database;
        this.table = table;
        this.producedDataType = producedDataType;
        this.maxPartitions = maxPartitions;
        this.partitionSizeBytes = partitionSizeBytes;
        this.dataBoostEnabled = dataBoostEnabled;
        this.rpcPriority = rpcPriority;
        this.timestampBound = timestampBound;
        this.emulatorEndpoint = emulatorEndpoint;
        this.parallelism = parallelism;
    }

    private static TimestampBound timestampBound(ReadableConfig config) {
        String readTimestamp =
                config.getOptional(SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP)
                        .orElse(null);
        Duration exactStaleness =
                config.getOptional(SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_EXACT_STALENESS)
                        .orElse(null);
        if (readTimestamp != null && exactStaleness != null) {
            throw new ValidationException(
                    "scan.timestamp-bound.read-timestamp and scan.timestamp-bound.exact-staleness are mutually exclusive.");
        }
        if (readTimestamp != null) {
            try {
                return TimestampBound.ofReadTimestamp(Timestamp.parseTimestamp(readTimestamp));
            } catch (RuntimeException e) {
                throw new ValidationException("Invalid scan timestamp: " + readTimestamp, e);
            }
        }
        if (exactStaleness != null) {
            if (exactStaleness.isZero() || exactStaleness.isNegative()) {
                throw new ValidationException(
                        "scan.timestamp-bound.exact-staleness must be positive.");
            }
            return TimestampBound.ofExactStaleness(exactStaleness.toNanos(), TimeUnit.NANOSECONDS);
        }
        return TimestampBound.strong();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            this.projectedFields[i] = projectedFields[i][0];
        }
        this.producedDataType = producedDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
        SpannerSourceBuilder<RowData> builder =
                SpannerSource.<RowData>builder()
                        .database(database)
                        .readOperation(
                                SpannerReadOperation.read(table, KeySet.all(), retainedColumns()))
                        .deserializer(
                                new RowDataDeserializationSchema(
                                        new StructToRowDataConverter(schema, projectedFields),
                                        producedType))
                        .timestampBound(timestampBound);
        if (maxPartitions != null) {
            builder.maxPartitions(maxPartitions);
        }
        if (partitionSizeBytes != null) {
            builder.partitionSizeBytes(partitionSizeBytes);
        }
        if (rpcPriority != null) {
            builder.rpcPriority(rpcPriority);
        }
        if (dataBoostEnabled != null) {
            builder.dataBoostEnabled(dataBoostEnabled);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(source, parallelism);
    }

    private List<String> retainedColumns() {
        List<String> names = new ArrayList<>();
        if (projectedFields == null) {
            for (SpannerTableSchemaConverter.Column column : schema.getColumns()) {
                names.add(column.getName());
            }
        } else {
            for (int index : projectedFields) {
                names.add(schema.getColumns().get(index).getName());
            }
        }
        if (names.isEmpty()) {
            names.add(schema.getColumns().get(0).getName());
        }
        return names;
    }

    @Override
    public DynamicTableSource copy() {
        SpannerDynamicSource copy =
                new SpannerDynamicSource(
                        schema,
                        database,
                        table,
                        producedDataType,
                        maxPartitions,
                        partitionSizeBytes,
                        dataBoostEnabled,
                        rpcPriority,
                        timestampBound,
                        emulatorEndpoint,
                        parallelism);
        copy.projectedFields = projectedFields == null ? null : projectedFields.clone();
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Spanner table source";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerDynamicSource)) {
            return false;
        }
        SpannerDynamicSource that = (SpannerDynamicSource) other;
        return Objects.equals(dataBoostEnabled, that.dataBoostEnabled)
                && schema.equals(that.schema)
                && database.equals(that.database)
                && table.equals(that.table)
                && producedDataType.equals(that.producedDataType)
                && Objects.equals(maxPartitions, that.maxPartitions)
                && Objects.equals(partitionSizeBytes, that.partitionSizeBytes)
                && rpcPriority == that.rpcPriority
                && timestampBound.equals(that.timestampBound)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && Arrays.equals(projectedFields, that.projectedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                table,
                producedDataType,
                maxPartitions,
                partitionSizeBytes,
                dataBoostEnabled,
                rpcPriority,
                timestampBound,
                emulatorEndpoint,
                parallelism,
                Arrays.hashCode(projectedFields));
    }
}
