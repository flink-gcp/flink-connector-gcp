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
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.table.LookupKeyFilter;
import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolution;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.table.SpannerClientMetricsConfig;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerLookupConfig;
import io.github.flink.gcp.connector.spanner.table.SpannerTableLineage;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A bounded table scan and lookup source for Spanner tables: primary-key point reads, or Flink's
 * full cache loaded through the bounded scan.
 */
@Internal
public final class SpannerDynamicSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsFilterPushDown {
    private final SpannerTableSchemaConverter schema;
    private final DatabaseDestination database;
    private final SpannerTableName table;
    private final Dialect dialect;
    @Nullable private final SpannerTableName.AccessPathName scanIndex;
    @Nullable private final Long maxPartitions;
    @Nullable private final Long partitionSizeBytes;
    @Nullable private final Integer maxRowsPerFetch;
    @Nullable private final Long maxBytesPerFetch;
    @Nullable private final Boolean dataBoostEnabled;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final TimestampBound timestampBound;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    private final SpannerLookupConfig lookupConfig;
    private final SpannerClientMetricsConfig clientMetrics;
    @Nullable private final SpannerTableLineage lineage;
    private DataType producedDataType;
    @Nullable private int[] projectedFields;
    private SpannerFilterPushDown.State filterState = SpannerFilterPushDown.State.empty();

    public SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            DatabaseDestination database,
            String table,
            DataType producedDataType,
            ReadableConfig config) {
        this(schema, database, table, producedDataType, config, null);
    }

    public SpannerDynamicSource(
            SpannerTableSchemaConverter schema,
            DatabaseDestination database,
            String table,
            DataType producedDataType,
            ReadableConfig config,
            @Nullable SpannerTableLineage lineage) {
        this(
                schema,
                database,
                tableName(table, config),
                producedDataType,
                config.get(SpannerConnectorOptions.DIALECT),
                scanIndex(table, config),
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS)
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_PARTITION_SIZE_BYTES)
                        .map(size -> size.getBytes())
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_MAX_ROWS_PER_FETCH).orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_MAX_BYTES_PER_FETCH)
                        .map(size -> size.getBytes())
                        .orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_DATA_BOOST_ENABLED).orElse(null),
                config.getOptional(SpannerConnectorOptions.SCAN_RPC_PRIORITY).orElse(null),
                timestampBound(config),
                config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null),
                SpannerLookupConfig.from(config),
                SpannerClientMetricsConfig.from(config),
                lineage);
    }

    private static SpannerTableName tableName(String table, ReadableConfig config) {
        return SpannerTableName.of(
                config.getOptional(SpannerConnectorOptions.NAMED_SCHEMA).orElse(null),
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
            DatabaseDestination database,
            SpannerTableName table,
            DataType producedDataType,
            Dialect dialect,
            @Nullable SpannerTableName.AccessPathName scanIndex,
            @Nullable Long maxPartitions,
            @Nullable Long partitionSizeBytes,
            @Nullable Integer maxRowsPerFetch,
            @Nullable Long maxBytesPerFetch,
            @Nullable Boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority,
            TimestampBound timestampBound,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            @Nullable Integer parallelism,
            SpannerLookupConfig lookupConfig,
            SpannerClientMetricsConfig clientMetrics,
            @Nullable SpannerTableLineage lineage) {
        this.schema = schema;
        this.database = database;
        this.table = table;
        this.dialect = dialect;
        this.scanIndex = scanIndex;
        this.producedDataType = producedDataType;
        this.maxPartitions = maxPartitions;
        this.partitionSizeBytes = partitionSizeBytes;
        this.maxRowsPerFetch = maxRowsPerFetch;
        this.maxBytesPerFetch = maxBytesPerFetch;
        this.dataBoostEnabled = dataBoostEnabled;
        this.rpcPriority = rpcPriority;
        this.timestampBound = timestampBound;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.parallelism = parallelism;
        this.lookupConfig = lookupConfig;
        this.clientMetrics = clientMetrics;
        this.lineage = lineage;
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
        Source<RowData, ?, ?> source = scanBuilder(producedType).build();
        return SourceProvider.of(
                lineage == null ? source : lineage.source(source, producedType), parallelism);
    }

    /**
     * The bounded scan's builder, shared by the scan and the full lookup cache's loader so that a
     * full-cache load reads exactly what a scan of the same table would.
     */
    private SpannerSourceBuilder<RowData> scanBuilder(TypeInformation<RowData> producedType) {
        SpannerSourceBuilder<RowData> builder =
                SpannerSource.<RowData>builder()
                        .database(database)
                        .readOperation(readOperation())
                        .deserializer(
                                new RowDataDeserializationSchema(
                                        new StructToRowDataConverter(schema, projectedFields),
                                        producedType))
                        .timestampBound(timestampBound);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS.key(),
                maxPartitions,
                builder::maxPartitions);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_PARTITION_SIZE_BYTES.key(),
                partitionSizeBytes,
                builder::partitionSizeBytes);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_MAX_ROWS_PER_FETCH.key(),
                maxRowsPerFetch,
                builder::maxRowsPerFetch);
        OptionSetters.accept(
                SpannerConnectorOptions.SCAN_MAX_BYTES_PER_FETCH.key(),
                maxBytesPerFetch,
                builder::maxBytesPerFetch);
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
        clientMetrics.applyTo(
                builder::exportClientMetricsToFlink, builder::exportClientMetricsToCloudMonitoring);
        return builder;
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

    private List<KeyColumn> declaredPrimaryKey() {
        List<KeyColumn> key = new ArrayList<>();
        for (int index : schema.getPrimaryKeyIndexes()) {
            key.add(new KeyColumn(schema.getColumns().get(index).getName(), index, false, false));
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
        if (lookupConfig.getCacheType() == LookupCacheType.FULL) {
            return fullCachingProvider(context);
        }
        LookupKeyFilter equalityKeys = lookupKeys(context);
        int[] keyPositions = equalityKeys.addressingPositions();
        if (equalityKeys.hasAdditionalKeys()) {
            for (int i = 0; i < keyPositions.length; i++) {
                keyPositions[i] = i;
            }
        }
        if (lookupConfig.isAsync()) {
            AsyncLookupFunction function =
                    equalityKeys.wrap(
                            new SpannerRowDataAsyncLookupFunction(
                                    database,
                                    table.apiName(),
                                    retainedColumns(),
                                    schema,
                                    projectedFields,
                                    keyPositions,
                                    emulatorEndpoint,
                                    serviceAccountKeyFile,
                                    clientMetrics,
                                    lookupConfig.getMaxRetries(),
                                    filterState.runtime()));
            return lookupConfig.getCacheType() == LookupCacheType.PARTIAL
                    ? PartialCachingAsyncLookupProvider.of(
                            function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                    : AsyncLookupFunctionProvider.of(function);
        }
        LookupFunction function =
                equalityKeys.wrap(
                        new SpannerRowDataLookupFunction(
                                database,
                                table.apiName(),
                                retainedColumns(),
                                schema,
                                projectedFields,
                                keyPositions,
                                emulatorEndpoint,
                                serviceAccountKeyFile,
                                clientMetrics,
                                lookupConfig.getMaxRetries(),
                                filterState.runtime()));
        return lookupConfig.getCacheType() == LookupCacheType.PARTIAL
                ? PartialCachingLookupProvider.of(
                        function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                : LookupFunctionProvider.of(function);
    }

    /**
     * Flink's full cache over the bounded scan.
     *
     * <p>Flink indexes the loaded rows by the planner's lookup keys and keeps every row of a key,
     * so the keys need not include the primary key. They must be top-level columns whose Flink
     * values compare under {@code equals} as SQL equality does, because that is how the cache index
     * matches them.
     */
    private LookupRuntimeProvider fullCachingProvider(LookupContext context) {
        if (timestampBound.getMode() == TimestampBound.Mode.READ_TIMESTAMP) {
            throw new ValidationException(
                    String.format(
                            "'%s' cannot be set when '%s' is FULL: every full-cache load reads a"
                                    + " fresh snapshot, and a fixed read timestamp would reload the"
                                    + " same rows until the timestamp leaves the database's version"
                                    + " retention period and the load fails. Use '%s' to read"
                                    + " older data.",
                            SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP.key(),
                            LookupOptions.CACHE_TYPE.key(),
                            SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_EXACT_STALENESS.key()));
        }
        RowType producedType = (RowType) producedDataType.getLogicalType();
        int[][] keys = context.getKeys();
        if (keys.length == 0) {
            throw new ValidationException(FULL_CACHE_KEY_REQUIREMENT);
        }
        for (int[] key : keys) {
            if (key.length != 1) {
                throw new ValidationException(
                        FULL_CACHE_KEY_REQUIREMENT + " A nested key path is not supported.");
            }
            LogicalType type = producedType.getTypeAt(key[0]);
            if (!fullCacheKeyType(type)) {
                throw new ValidationException(
                        FULL_CACHE_KEY_REQUIREMENT
                                + " Lookup column '"
                                + producedType.getFieldNames().get(key[0])
                                + "' has unsupported equality type "
                                + type.asSummaryString()
                                + ".");
            }
        }
        SpannerBatchReadSource<RowData> scan =
                (SpannerBatchReadSource<RowData>)
                        scanBuilder(context.createTypeInformation(producedDataType)).build();
        return FullCachingLookupProvider.of(
                InputFormatProvider.of(new SpannerFullCacheInputFormat(scan.getConfig())),
                lookupConfig.createFullReloadTrigger());
    }

    private static final String FULL_CACHE_KEY_REQUIREMENT =
            "A Spanner lookup with '"
                    + LookupOptions.CACHE_TYPE.key()
                    + "' = 'FULL' requires equality predicates on top-level columns of a scalar"
                    + " type other than FLOAT and DOUBLE, whose Java equality differs from SQL's.";

    /**
     * Whether a key type's Flink values match in the full cache's index exactly when SQL equality
     * holds, among the types {@code SpannerTableSchemaConverter} maps. {@code FLOAT} and {@code
     * DOUBLE} do not: their boxed {@code equals} tells {@code -0.0} from {@code 0.0} and matches
     * {@code NaN} to itself. {@code ARRAY} does not either, because its binary and generic forms
     * are not equal to each other.
     */
    private static boolean fullCacheKeyType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case BIGINT:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    private LookupKeyFilter lookupKeys(LookupContext context) {
        return LookupKeyFilter.of(
                (RowType) producedDataType.getLogicalType(),
                context.getKeys(),
                projectedFields,
                schema.getPrimaryKeyIndexes(),
                false,
                lookupKeyException().getMessage());
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
                        maxRowsPerFetch,
                        maxBytesPerFetch,
                        dataBoostEnabled,
                        rpcPriority,
                        timestampBound,
                        emulatorEndpoint,
                        serviceAccountKeyFile,
                        parallelism,
                        lookupConfig,
                        clientMetrics,
                        lineage);
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
                && Objects.equals(lineage, that.lineage)
                && schema.equals(that.schema)
                && database.equals(that.database)
                && table.equals(that.table)
                && dialect == that.dialect
                && Objects.equals(scanIndex, that.scanIndex)
                && producedDataType.equals(that.producedDataType)
                && Objects.equals(maxPartitions, that.maxPartitions)
                && Objects.equals(partitionSizeBytes, that.partitionSizeBytes)
                && Objects.equals(maxRowsPerFetch, that.maxRowsPerFetch)
                && Objects.equals(maxBytesPerFetch, that.maxBytesPerFetch)
                && rpcPriority == that.rpcPriority
                && timestampBound.equals(that.timestampBound)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && lookupConfig.equals(that.lookupConfig)
                && clientMetrics.equals(that.clientMetrics)
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
                maxRowsPerFetch,
                maxBytesPerFetch,
                dataBoostEnabled,
                rpcPriority,
                timestampBound,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism,
                lineage,
                lookupConfig,
                clientMetrics,
                filterState,
                Arrays.hashCode(projectedFields));
    }
}
