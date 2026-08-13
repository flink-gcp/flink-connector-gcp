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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import io.github.flink.gcp.connector.spanner.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.spanner.table.source.SpannerDynamicSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Creates the {@code spanner} table source and sink from a SQL DDL. */
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
                        SpannerConnectorOptions.SCAN_INDEX,
                        SpannerConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS,
                        SpannerConnectorOptions.SCAN_PARTITION_SIZE,
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
                        SpannerConnectorOptions.SINK_RETRY_INITIAL_BACKOFF,
                        SpannerConnectorOptions.SINK_RETRY_MAX_BACKOFF,
                        SpannerConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        FactoryUtil.SINK_PARALLELISM,
                        FactoryUtil.SOURCE_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        DataType physicalType = context.getPhysicalRowDataType();
        SpannerTableSchemaConverter schema = createSchema(context, config, physicalType);
        SpannerTableName table = tableName(config);

        return SpannerDynamicSink.builder()
                .schema(schema)
                .database(
                        SpannerDatabase.of(
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
        return new SpannerDynamicSource(
                schema,
                SpannerDatabase.of(
                        config.get(SpannerConnectorOptions.PROJECT),
                        config.get(SpannerConnectorOptions.INSTANCE),
                        config.get(SpannerConnectorOptions.DATABASE)),
                config.get(SpannerConnectorOptions.TABLE),
                physicalType,
                config);
    }

    private static void validateCredentialsMode(ReadableConfig config) {
        String keyFile =
                config.getOptional(SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        if (keyFile != null && keyFile.trim().isEmpty()) {
            throw new org.apache.flink.table.api.ValidationException(
                    "service-account-key-file must not be blank.");
        }
        if (keyFile != null
                && config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new org.apache.flink.table.api.ValidationException(
                    "service-account-key-file cannot be combined with emulator-endpoint.");
        }
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
