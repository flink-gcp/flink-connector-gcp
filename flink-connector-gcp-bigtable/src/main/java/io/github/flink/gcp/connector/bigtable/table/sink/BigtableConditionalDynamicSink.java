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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSinkBuilder;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/** An INSERT-only command input whose DDL fixes one complete conditional request structure. */
@Internal
public final class BigtableConditionalDynamicSink implements DynamicTableSink {
    private final RowType rowType;
    private final Map<String, String> options;
    private final String logicalName;
    private final BigtableConditionalSink<RowData> sink;
    @Nullable private final Integer parallelism;

    private BigtableConditionalDynamicSink(
            RowType rowType,
            Map<String, String> options,
            String logicalName,
            BigtableConditionalSink<RowData> sink,
            @Nullable Integer parallelism) {
        this.rowType = rowType;
        this.options = Map.copyOf(options);
        this.logicalName = logicalName;
        this.sink = sink;
        this.parallelism = parallelism;
    }

    /** Validates the physical command schema and compiles its DDL before runtime translation. */
    public static BigtableConditionalDynamicSink create(
            DynamicTableFactory.Context context, ReadableConfig config) {
        if (context.getPrimaryKeyIndexes().length > 0) {
            throw new ValidationException(
                    "Bigtable 'sink.write-mode' = 'conditional' must not declare a PRIMARY KEY.");
        }
        if (context.getCatalogTable().getResolvedSchema().getColumns().stream()
                .anyMatch(column -> column instanceof Column.MetadataColumn)) {
            throw new ValidationException(
                    "Bigtable 'sink.write-mode' = 'conditional' must not declare metadata columns.");
        }
        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        Map<String, String> options = context.getCatalogTable().getOptions();
        ConditionalTableTemplate template = ConditionalTableTemplate.compile(rowType, options);
        BigtableConditionalSinkBuilder<RowData> builder =
                BigtableConditionalSink.<RowData>builder()
                        .table(
                                TableDestination.of(
                                        config.get(BigtableConnectorOptions.PROJECT),
                                        config.get(BigtableConnectorOptions.INSTANCE),
                                        config.get(BigtableConnectorOptions.TABLE)))
                        .serializer(new RowDataConditionalCommandSerializationSchema(template))
                        .requestOptions(RequestOptionsMapper.map(config));
        OptionSetters.apply(
                config, BigtableConnectorOptions.SINK_APP_PROFILE_ID, builder::appProfileId);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                builder::serviceAccountKeyFile);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_CONDITIONAL_EMPTY_BRANCH_POLICY,
                builder::emptyBranchPolicy);
        config.getOptional(BigtableConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        endpoint ->
                                builder.emulatorEndpoint(
                                        EmulatorEndpoint.parse(endpoint, "emulator-endpoint")));
        String logicalName = context.getObjectIdentifier().asSummaryString();
        return new BigtableConditionalDynamicSink(
                rowType,
                options,
                logicalName,
                builder.build().withTableLineage(logicalName),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    /** Rejects request-schema DDL before a source or lookup can interpret it as stored cells. */
    public static void rejectRead(Map<String, String> options) {
        if ("conditional"
                        .equalsIgnoreCase(
                                options.get(BigtableConnectorOptions.SINK_WRITE_MODE.key()))
                || options.keySet().stream()
                        .anyMatch(WriteModeOptionChecks::isConditionalCommandOption)) {
            throw new ValidationException(
                    "Bigtable 'sink.write-mode' = 'conditional' command tables are write-only; declare a separate read table.");
        }
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!requestedMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    "Bigtable 'sink.write-mode' = 'conditional' requires INSERT-only input.");
        }
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigtableConditionalDynamicSink(rowType, options, logicalName, sink, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "Bigtable conditional command sink";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BigtableConditionalDynamicSink)) {
            return false;
        }
        BigtableConditionalDynamicSink that = (BigtableConditionalDynamicSink) other;
        return rowType.equals(that.rowType)
                && options.equals(that.options)
                && logicalName.equals(that.logicalName)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, options, logicalName, parallelism);
    }
}
