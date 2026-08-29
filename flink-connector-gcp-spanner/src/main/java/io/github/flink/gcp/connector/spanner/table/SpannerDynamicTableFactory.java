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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import io.github.flink.gcp.connector.spanner.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.spanner.table.source.SpannerChangeStreamDynamicSource;
import io.github.flink.gcp.connector.spanner.table.source.SpannerDynamicSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Creates the {@code spanner} table source and sink from a SQL DDL.
 *
 * <p>A source rejects the options the other scan mode owns rather than ignoring them, and {@code
 * scan.mode = change-stream} selects a source-only table: an {@code INSERT INTO} over one is
 * refused instead of silently writing into the table being watched, as are the change-stream
 * options on any table being written to. The bounded-scan and lookup options of a table that is
 * also read stay accepted on a sink.
 */
@Internal
public final class SpannerDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "spanner";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        SpannerConnectorOptions.PROJECT,
                        SpannerConnectorOptions.INSTANCE,
                        SpannerConnectorOptions.DATABASE,
                        SpannerConnectorOptions.TABLE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        SpannerConnectorOptions.EMULATOR_ENDPOINT,
                        SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        SpannerConnectorOptions.DIALECT,
                        SpannerConnectorOptions.SCHEMA,
                        SpannerConnectorOptions.SCHEMA_JSON_FIELD_PATHS,
                        SpannerConnectorOptions.SCHEMA_UUID_FIELD_PATHS,
                        SpannerConnectorOptions.SCHEMA_PROTO_TYPE_NAMES,
                        SpannerConnectorOptions.SCHEMA_ENUM_TYPE_NAMES,
                        SpannerConnectorOptions.SCAN_MODE,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_NAME,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE,
                        SpannerConnectorOptions.SCAN_STARTUP_MODE,
                        SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE,
                        SpannerConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL,
                        SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK,
                        SpannerConnectorOptions.SCAN_INDEX,
                        SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS,
                        SpannerConnectorOptions.SCAN_PARTITION_SIZE_BYTES,
                        SpannerConnectorOptions.SCAN_DATA_BOOST_ENABLED,
                        SpannerConnectorOptions.SCAN_RPC_PRIORITY,
                        SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP,
                        SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_EXACT_STALENESS,
                        SpannerConnectorOptions.LOOKUP_ASYNC,
                        LookupOptions.CACHE_TYPE,
                        LookupOptions.MAX_RETRIES,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                        LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                        LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_CELLS,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_MUTATIONS,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_COMMIT_DELAY,
                        SpannerConnectorOptions.SINK_RPC_PRIORITY,
                        SpannerConnectorOptions.SINK_BATCH_WRITE_TIMEOUT,
                        SpannerConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        SpannerConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        SpannerConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                        FactoryUtil.SINK_PARALLELISM,
                        FactoryUtil.SOURCE_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Before validating, as Bigtable's sink does: a table that cannot be written to at all is
        // the reason to report, not whichever secondary complaint validate() reaches first.
        checkNotAChangeStreamTable(helper.getOptions());
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        rejectChangeStreamOptions(context.getCatalogTable().getOptions(), "a sink");
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        DataType physicalType = context.getPhysicalRowDataType();
        SpannerTableSchemaConverter schema = createSchema(context, config, physicalType);
        SpannerTableName table = tableName(config);

        return SpannerDynamicSink.builder()
                .schema(schema)
                .database(
                        DatabaseDestination.of(
                                config.get(SpannerConnectorOptions.PROJECT),
                                config.get(SpannerConnectorOptions.INSTANCE),
                                config.get(SpannerConnectorOptions.DATABASE)))
                .table(table.apiName())
                .writerOptions(WriterOptionsMapper.map(config))
                .emulatorEndpoint(
                        config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null))
                .serviceAccountKeyFile(
                        config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                .build();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        DataType physicalType = context.getPhysicalRowDataType();
        SpannerTableSchemaConverter schema = createSchema(context, config, physicalType);
        validateSourceMode(context.getCatalogTable().getOptions(), config, schema);
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        if (config.get(SpannerConnectorOptions.SCAN_MODE) == ScanMode.CHANGE_STREAM) {
            return SpannerChangeStreamDynamicSource.from(
                    schema, physicalType, config, tableName(config));
        }
        return new SpannerDynamicSource(
                schema,
                DatabaseDestination.of(
                        config.get(SpannerConnectorOptions.PROJECT),
                        config.get(SpannerConnectorOptions.INSTANCE),
                        config.get(SpannerConnectorOptions.DATABASE)),
                config.get(SpannerConnectorOptions.TABLE),
                physicalType,
                config);
    }

    private static void validateSourceMode(
            Map<String, String> supplied,
            ReadableConfig config,
            SpannerTableSchemaConverter schema) {
        ScanMode mode = config.get(SpannerConnectorOptions.SCAN_MODE);
        if (mode == ScanMode.BOUNDED) {
            rejectChangeStreamOptions(supplied, "scan.mode=bounded");
            return;
        }
        rejectSupplied(
                supplied,
                "scan.mode=change-stream",
                SpannerConnectorOptions.SCAN_INDEX,
                SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS,
                SpannerConnectorOptions.SCAN_PARTITION_SIZE_BYTES,
                SpannerConnectorOptions.SCAN_DATA_BOOST_ENABLED,
                SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP,
                SpannerConnectorOptions.SCAN_TIMESTAMP_BOUND_EXACT_STALENESS,
                SpannerConnectorOptions.LOOKUP_ASYNC,
                LookupOptions.CACHE_TYPE,
                LookupOptions.MAX_RETRIES,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        requireNonBlank(config, SpannerConnectorOptions.SCAN_CHANGE_STREAM_NAME);
        ChangeStreamChangelogMode changelog =
                config.getOptional(SpannerConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "scan.change-stream.changelog-mode is required when scan.mode=change-stream."));
        if (changelog == ChangeStreamChangelogMode.UPSERT && !schema.hasPrimaryKey()) {
            throw new ValidationException(
                    "scan.change-stream.changelog-mode=upsert requires a PRIMARY KEY declared NOT ENFORCED in the table DDL.");
        }
        ChangeStreamStartPositionMapper.validate(config, supplied);
    }

    /**
     * Rejects a change-stream table written to. Nothing downstream catches it: a change-stream DDL
     * declares the watched table's own columns and {@link #createSchema} builds it identically for
     * both directions, so the write lands in the table being watched.
     */
    private static void checkNotAChangeStreamTable(ReadableConfig config) {
        if (config.get(SpannerConnectorOptions.SCAN_MODE) == ScanMode.CHANGE_STREAM) {
            throw new ValidationException(
                    "scan.mode=change-stream selects a source-only table and cannot be written to;"
                            + " write to a table declared without it.");
        }
    }

    private static void rejectChangeStreamOptions(
            Map<String, String> supplied, String incompatibleWith) {
        rejectSupplied(
                supplied,
                incompatibleWith,
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_NAME,
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE,
                SpannerConnectorOptions.SCAN_STARTUP_MODE,
                SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE,
                SpannerConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS,
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK,
                SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL,
                SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK);
    }

    @SafeVarargs
    private static void rejectSupplied(
            Map<String, String> supplied, String incompatibleWith, ConfigOption<?>... options) {
        for (ConfigOption<?> option : options) {
            if (supplied.containsKey(option.key())) {
                throw new ValidationException(
                        option.key() + " is incompatible with " + incompatibleWith + ".");
            }
        }
    }

    private static void requireNonBlank(ReadableConfig config, ConfigOption<String> option) {
        String value =
                config.getOptional(option)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                option.key()
                                                        + " is required when scan.mode=change-stream."));
        if (value.isBlank()) {
            throw new ValidationException(option.key() + " must not be blank.");
        }
    }

    private static void validateCredentialsMode(ReadableConfig config) {
        String keyFile =
                config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        if (keyFile != null && keyFile.isBlank()) {
            throw new ValidationException("service-account-key-file must not be blank.");
        }
        if (keyFile != null
                && config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    "service-account-key-file cannot be combined with emulator-endpoint.");
        }
    }

    /**
     * Reaches the lookup path, which {@code SpannerDatabaseRowLookup} left until it opened on a
     * TaskManager (issue #1013, {@code docs/adr/0127}).
     *
     * <p>Call it after every check that refuses an option outright, never before one. An emulator
     * endpoint is itself legal in every Spanner mode, but {@code validateSourceMode} and {@code
     * rejectChangeStreamOptions} refuse <em>other</em> options, and a DDL told to remove one of
     * those is not helped by an answer about this one's shape.
     */
    private static void validateEmulatorEndpoint(ReadableConfig config) {
        config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        value ->
                                EmulatorEndpoint.parse(
                                        value, SpannerConnectorOptions.EMULATOR_ENDPOINT.key()));
    }

    private static SpannerTableName tableName(ReadableConfig config) {
        return SpannerTableName.of(
                config.getOptional(SpannerConnectorOptions.SCHEMA).orElse(null),
                config.get(SpannerConnectorOptions.TABLE),
                config.get(SpannerConnectorOptions.DIALECT));
    }

    private static SpannerTableSchemaConverter createSchema(
            Context context, ReadableConfig config, DataType physicalType) {
        return SpannerTableSchemaConverter.of(
                (RowType) physicalType.getLogicalType(),
                context.getPrimaryKeyIndexes(),
                config.get(SpannerConnectorOptions.DIALECT),
                config.getOptional(SpannerConnectorOptions.SCHEMA_JSON_FIELD_PATHS)
                        .orElse(Collections.emptyList()),
                config.getOptional(SpannerConnectorOptions.SCHEMA_UUID_FIELD_PATHS)
                        .orElse(Collections.emptyList()),
                config.getOptional(SpannerConnectorOptions.SCHEMA_PROTO_TYPE_NAMES)
                        .orElse(Collections.emptyMap()),
                config.getOptional(SpannerConnectorOptions.SCHEMA_ENUM_TYPE_NAMES)
                        .orElse(Collections.emptyMap()));
    }
}
