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
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import io.github.flink.gcp.connector.spanner.table.sink.WriterOptionsMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Creates the {@code spanner} table sink from a SQL DDL. */
@Internal
public final class SpannerDynamicTableFactory implements DynamicTableSinkFactory {

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
                        SpannerConnectorOptions.DIALECT,
                        SpannerConnectorOptions.SCHEMA_JSON_FIELD_PATHS,
                        SpannerConnectorOptions.SCHEMA_PROTO_TYPE_NAMES,
                        SpannerConnectorOptions.SCHEMA_ENUM_TYPE_NAMES,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_CELLS,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_MUTATIONS,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE,
                        SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_COMMIT_DELAY,
                        SpannerConnectorOptions.SINK_RPC_PRIORITY,
                        SpannerConnectorOptions.SINK_RETRY_INITIAL_BACKOFF,
                        SpannerConnectorOptions.SINK_RETRY_MAX_BACKOFF,
                        SpannerConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        DataType physicalType = context.getPhysicalRowDataType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        (RowType) physicalType.getLogicalType(),
                        context.getPrimaryKeyIndexes(),
                        config.get(SpannerConnectorOptions.DIALECT),
                        config.getOptional(SpannerConnectorOptions.SCHEMA_JSON_FIELD_PATHS)
                                .orElse(Collections.emptyList()),
                        config.getOptional(SpannerConnectorOptions.SCHEMA_PROTO_TYPE_NAMES)
                                .orElse(Collections.emptyMap()),
                        config.getOptional(SpannerConnectorOptions.SCHEMA_ENUM_TYPE_NAMES)
                                .orElse(Collections.emptyMap()));

        return SpannerDynamicSink.builder()
                .schema(schema)
                .database(
                        SpannerDatabase.of(
                                config.get(SpannerConnectorOptions.PROJECT),
                                config.get(SpannerConnectorOptions.INSTANCE),
                                config.get(SpannerConnectorOptions.DATABASE)))
                .table(config.get(SpannerConnectorOptions.TABLE))
                .writerOptions(WriterOptionsMapper.map(config))
                .emulatorEndpoint(
                        config.getOptional(SpannerConnectorOptions.EMULATOR_ENDPOINT).orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                .build();
    }
}
