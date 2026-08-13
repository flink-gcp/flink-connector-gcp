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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolution;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerLookupConfig;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** A bounded table scan and primary-key point-lookup source for Spanner tables. */
@Internal
public final class SpannerDynamicSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsFilterPushDown {
    private final SpannerTableSchemaConverter schema;
    private final SpannerDatabase database;
    private final SpannerTableName table;
    private final Dialect dialect;
    @Nullable private final SpannerTableName.AccessPathName scanIndex;
    @Nullable private final Long maxPartitions;
    @Nullable private final Long partitionSizeBytes;
    @Nullable private final Boolean dataBoostEnabled;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final TimestampBound timestampBound;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    private final SpannerLookupConfig lookupConfig;
    private DataType producedDataType;
    @Nullable private int[] projectedFields;
    private SpannerFilterPushDown.State filterState = SpannerFilterPushDown.State.empty();

    public SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            SpannerDatabase database,
            String table,
            DataType producedDataType,
            ReadableConfig config) {
        this(
                schema,
                database,
                tableName(table, config),
                producedDataType,
                config.get(SpannerConnectorOptions.DIALECT),
                scanIndex(table, config),
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS)
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_SIZE)
                        .map(size -> size.getBytes())
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_DATA_BOOST_ENABLED).orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_RPC_PRIORITY).orElse(null),
                timestampBound(config),
                config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null),
                SpannerLookupConfig.from(config));
    }

    private static SpannerTableName tableName(String table, ReadableConfig config) {
        return SpannerTableName.of(
                config.getOptional(SpannerConnectorOptions.SCHEMA).orElse(null),
                table,
                config.get(SpannerConnectorOptions.DIALECT));
    }

    @Nullable
    private static SpannerTableName.AccessPathName scanIndex(String table, ReadableConfig config) {
        String index = config.getOptional(SpannerConnectorOptions.SCAN_INDEX).orElse(null);
        return index == null ? null : tableName(table, config).accessPath(index, "scan.index");
    }

    private SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            SpannerDatabase database,
            SpannerTableName table,
            DataType producedDataType,
            Dialect dialect,
            @Nullable SpannerTableName.AccessPathName scanIndex,
            @Nullable Long maxPartitions,
            @Nullable Long partitionSizeBytes,
            @Nullable Boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority,
            TimestampBound timestampBound,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            @Nullable Integer parallelism,
            SpannerLookupConfig lookupConfig) {
        this.schema = schema;
        this.database = database;
        this.table = table;
        this.dialect = dialect;
        this.scanIndex = scanIndex;
        this.producedDataType = producedDataType;
        this.maxPartitions = maxPartitions;
        this.partitionSizeBytes = partitionSizeBytes;
        this.dataBoostEnabled = dataBoostEnabled;
        this.rpcPriority = rpcPriority;
        this.timestampBound = timestampBound;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.parallelism = parallelism;
        this.lookupConfig = lookupConfig;
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
    public SupportsFilterPushDown.Result applyFilters(List<ResolvedExpression> filters) {
        filterState = SpannerFilterPushDown.translate(schema, filters, scanIndex != null);
        return filterState.result();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
        SpannerSourceBuilder<RowData> builder =
                SpannerSource.<RowData>builder()
                        .database(database)
                        .readOperation(readOperation())
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
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(source, parallelism);
    }

    private SpannerReadOperation readOperation() {
        List<String> columns = projectedColumns();
        boolean zeroColumnProjection = columns.isEmpty();
        if (scanIndex == null && !filterState.hasPrimaryKeyConstraint()) {
            return SpannerReadOperation.read(table.apiName(), KeySet.all(), retainedColumns());
        }
        if (scanIndex == null) {
            KeySet keys = filterState.directionIndependentPrimaryKeySet(declaredPrimaryKey());
            if (keys != null) {
                return SpannerReadOperation.read(table.apiName(), keys, retainedColumns());
            }
        }
        return SpannerReadOperationResolution.deferred(
                new SpannerTableReadResolver(
                        schema,
                        table,
                        scanIndex,
                        columns,
                        zeroColumnProjection,
                        dialect,
                        filterState.runtime()));
    }

    private List<SpannerFilterPushDown.KeyPart> declaredPrimaryKey() {
        List<SpannerFilterPushDown.KeyPart> key = new ArrayList<>();
        for (int index : schema.getPrimaryKeyIndexes()) {
            key.add(
                    new SpannerFilterPushDown.KeyPart(
                            schema.getColumns().get(index).getName(), index, false, false));
        }
        return key;
    }

    private List<String> projectedColumns() {
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
        return names;
    }

    private List<String> retainedColumns() {
        List<String> names = projectedColumns();
        if (names.isEmpty()) {
            names.add(schema.getColumns().get(0).getName());
        }
        return names;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[] keyPositions = lookupKeyPositions(context);
        if (lookupConfig.isAsync()) {
            SpannerRowDataAsyncLookupFunction function =
                    new SpannerRowDataAsyncLookupFunction(
                            database,
                            table.apiName(),
                            retainedColumns(),
                            schema,
                            projectedFields,
                            keyPositions,
                            emulatorEndpoint,
                            serviceAccountKeyFile,
                            lookupConfig.getMaxRetries(),
                            filterState.runtime());
            return lookupConfig.getCacheType() == LookupCacheType.PARTIAL
                    ? PartialCachingAsyncLookupProvider.of(
                            function, lookupConfig.createPartialCache())
                    : AsyncLookupFunctionProvider.of(function);
        }
        SpannerRowDataLookupFunction function =
                new SpannerRowDataLookupFunction(
                        database,
                        table.apiName(),
                        retainedColumns(),
                        schema,
                        projectedFields,
                        keyPositions,
                        emulatorEndpoint,
                        serviceAccountKeyFile,
                        lookupConfig.getMaxRetries(),
                        filterState.runtime());
        return lookupConfig.getCacheType() == LookupCacheType.PARTIAL
                ? PartialCachingLookupProvider.of(function, lookupConfig.createPartialCache())
                : LookupFunctionProvider.of(function);
    }

    private int[] lookupKeyPositions(LookupContext context) {
        int[][] keys = context.getKeys();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length == 0 || keys.length != primaryKeyIndexes.length) {
            throw lookupKeyException();
        }
        int[] positions = new int[primaryKeyIndexes.length];
        Arrays.fill(positions, -1);
        for (int keyPosition = 0; keyPosition < keys.length; keyPosition++) {
            if (keys[keyPosition].length != 1) {
                throw lookupKeyException();
            }
            int producedIndex = keys[keyPosition][0];
            int producedArity = producedDataType.getChildren().size();
            if (producedIndex < 0 || producedIndex >= producedArity) {
                throw lookupKeyException();
            }
            int physicalIndex =
                    projectedFields == null ? producedIndex : projectedFields[producedIndex];
            int primaryPosition = indexOf(primaryKeyIndexes, physicalIndex);
            if (primaryPosition < 0 || positions[primaryPosition] >= 0) {
                throw lookupKeyException();
            }
            positions[primaryPosition] = keyPosition;
        }
        return positions;
    }

    private static int indexOf(int[] values, int wanted) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == wanted) {
                return i;
            }
        }
        return -1;
    }

    private static ValidationException lookupKeyException() {
        return new ValidationException(
                "A Spanner lookup requires equality predicates for every declared PRIMARY KEY column.");
    }

    @Override
    public DynamicTableSource copy() {
        SpannerDynamicSource copy =
                new SpannerDynamicSource(
                        schema,
                        database,
                        table,
                        producedDataType,
                        dialect,
                        scanIndex,
                        maxPartitions,
                        partitionSizeBytes,
                        dataBoostEnabled,
                        rpcPriority,
                        timestampBound,
                        emulatorEndpoint,
                        serviceAccountKeyFile,
                        parallelism,
                        lookupConfig);
        copy.projectedFields = projectedFields == null ? null : projectedFields.clone();
        copy.filterState = filterState;
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
                && dialect == that.dialect
                && Objects.equals(scanIndex, that.scanIndex)
                && producedDataType.equals(that.producedDataType)
                && Objects.equals(maxPartitions, that.maxPartitions)
                && Objects.equals(partitionSizeBytes, that.partitionSizeBytes)
                && rpcPriority == that.rpcPriority
                && timestampBound.equals(that.timestampBound)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && lookupConfig.equals(that.lookupConfig)
                && Arrays.equals(projectedFields, that.projectedFields)
                && filterState.equals(that.filterState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                table,
                dialect,
                scanIndex,
                producedDataType,
                maxPartitions,
                partitionSizeBytes,
                dataBoostEnabled,
                rpcPriority,
                timestampBound,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism,
                lookupConfig,
                filterState,
                Arrays.hashCode(projectedFields));
    }
}
