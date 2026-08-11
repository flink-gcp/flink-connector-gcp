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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import io.github.flink.gcp.connector.bigtable.table.BigtableLookupConfig;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the source hands the DataStream builder, read back through {@code
 * BigtableReadRowsSource.getConfig()}.
 */
class BigtableDynamicSourceTest {

    /** A row key {@code rowkey}, a family {@code cf1} of one string, a family {@code cf2}. */
    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("rowkey", DataTypes.STRING()),
                    DataTypes.FIELD("cf1", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()))),
                    DataTypes.FIELD(
                            "cf2", DataTypes.ROW(DataTypes.FIELD("m", DataTypes.DOUBLE()))));

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of((RowType) PHYSICAL.getLogicalType());

    private static BigtableDynamicSource.Builder minimal() {
        return BigtableDynamicSource.builder()
                .schema(SCHEMA)
                .destination(TableDestination.of("p", "i", "t"))
                .nullStringLiteral("null")
                // The provider builds the source's real clients, which would demand
                // application-default credentials on a machine that has them and fail in CI on one
                // that does not. The endpoint is never connected to.
                .emulatorEndpoint("localhost:1")
                .lookupOptions(BigtableLookupConfig.from(new Configuration()))
                .producedDataType(PHYSICAL);
    }

    private static BigtableLookupConfig lookupOptions(String... entries) {
        Configuration config = new Configuration();
        for (int i = 0; i < entries.length; i += 2) {
            config.setString(entries[i], entries[i + 1]);
        }
        return BigtableLookupConfig.from(config);
    }

    private static LookupRuntimeProvider lookupProvider(BigtableDynamicSource source, int key) {
        return source.getLookupRuntimeProvider(lookupContext(new int[][] {{key}}));
    }

    private static LookupContext lookupContext(int[][] keys) {
        return (LookupContext)
                Proxy.newProxyInstance(
                        LookupContext.class.getClassLoader(),
                        new Class<?>[] {LookupContext.class},
                        (proxy, method, arguments) -> {
                            switch (method.getName()) {
                                case "getKeys":
                                    return keys;
                                case "createTypeInformation":
                                    if (arguments[0] instanceof DataType) {
                                        return ScanRuntimeProviderContext.INSTANCE
                                                .createTypeInformation((DataType) arguments[0]);
                                    }
                                    return ScanRuntimeProviderContext.INSTANCE
                                            .createTypeInformation(
                                                    (org.apache.flink.table.types.logical
                                                                    .LogicalType)
                                                            arguments[0]);
                                case "createDataStructureConverter":
                                    return ScanRuntimeProviderContext.INSTANCE
                                            .createDataStructureConverter((DataType) arguments[0]);
                                case "preferCustomShuffle":
                                    return false;
                                default:
                                    throw new UnsupportedOperationException(method.toString());
                            }
                        });
    }

    private static BigtableSourceConfig<?> configOf(BigtableDynamicSource source) {
        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((BigtableReadRowsSource<?>) provider.createSource()).getConfig();
    }

    private static List<String> rangesOf(BigtableSourceConfig<?> config) {
        return config.getRanges().stream().map(RowRanges::format).collect(Collectors.toList());
    }

    private static DataType projectedType(String... fieldNames) {
        List<String> physicalNames = ((RowType) PHYSICAL.getLogicalType()).getFieldNames();
        DataTypes.Field[] fields =
                Arrays.stream(fieldNames)
                        .map(
                                name ->
                                        DataTypes.FIELD(
                                                name,
                                                PHYSICAL.getChildren()
                                                        .get(physicalNames.indexOf(name))))
                        .toArray(DataTypes.Field[]::new);
        return DataTypes.ROW(fields);
    }

    @Test
    void anUnprojectedScanRetainsEveryDeclaredFamily() {
        // The filter is applied even with no projection: families the physical table has but the
        // DDL does not declare never leave the server.
        BigtableSourceConfig<?> config = configOf(minimal().build());

        assertThat(config.getFilter().toProto())
                .isEqualTo(
                        FILTERS.interleave()
                                .filter(FILTERS.family().exactMatch("cf1"))
                                .filter(FILTERS.family().exactMatch("cf2"))
                                .toProto());
    }

    @Test
    void aProjectionPrunesToItsFamilies() {
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}, {2}}, projectedType("rowkey", "cf2"));

        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(FILTERS.family().exactMatch("cf2").toProto());
    }

    @Test
    void aRowKeyOnlyProjectionScansKeysOnly() {
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}}, projectedType("rowkey"));

        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(
                        FILTERS.chain()
                                .filter(FILTERS.limit().cellsPerRow(1))
                                .filter(FILTERS.value().strip())
                                .toProto());
    }

    @Test
    void prefixesAndTheRangeAreAdditive() {
        BigtableSourceConfig<?> config =
                configOf(
                        minimal()
                                .prefixes(Arrays.asList("user", "web"))
                                .rangeStartClosed("a")
                                .rangeEndOpen("m")
                                .build());

        assertThat(rangesOf(config)).containsExactly("[a, m)", "[user, uses)", "[web, wec)");
    }

    @Test
    void aOneSidedRangeLeavesTheOtherEndOpen() {
        assertThat(rangesOf(configOf(minimal().rangeStartClosed("b").build())))
                .containsExactly("[b, *)");
        assertThat(rangesOf(configOf(minimal().rangeEndOpen("b").build())))
                .containsExactly("(*, b)");
    }

    @Test
    void theAppProfileReachesTheSourceConfig() {
        assertThat(configOf(minimal().appProfileId("boost").build()).getAppProfileId())
                .isEqualTo("boost");
        assertThat(configOf(minimal().build()).getAppProfileId()).isNull();
    }

    @Test
    void theParallelismReachesTheProvider() {
        SourceProvider provider =
                (SourceProvider)
                        minimal()
                                .parallelism(5)
                                .build()
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.getParallelism()).contains(5);
        assertThat(provider.isBounded()).isTrue();
    }

    @Test
    void theProducedTypeFollowsTheProjection() {
        // The spec's own sentence: the source reports the producedDataType it was passed, never
        // toPhysicalRowDataType(). A source that kept the physical type here still answers every
        // in-JVM test — operators chain, so the mismatched serializer is never exercised — and
        // fails only at a real network exchange, which is why the reported type is pinned
        // directly.
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}}, projectedType("rowkey"));

        assertThat(configOf(source).getDeserializer().getProducedType())
                .isEqualTo(
                        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(
                                projectedType("rowkey")));
    }

    @Test
    void copyCarriesTheProjectionState() {
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}}, projectedType("cf2"));

        DynamicTableSource copy = projected.copy();

        assertThat(copy).isEqualTo(projected);
        assertThat(((BigtableDynamicSource) copy).copy()).isEqualTo(projected);
    }

    @Test
    void theProjectionStateDistinguishesSources() {
        BigtableDynamicSource plain = minimal().build();
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}}, projectedType("cf2"));

        assertThat(plain).isEqualTo(minimal().build());
        assertThat(plain.hashCode()).isEqualTo(minimal().build().hashCode());
        assertThat(projected).isNotEqualTo(plain);
    }

    @Test
    void selectsExactlyOneProviderShapeForEveryPointLookupMode() {
        assertThat(lookupProvider(minimal().build(), 0))
                .isInstanceOf(LookupFunctionProvider.class)
                .isNotInstanceOf(AsyncLookupFunctionProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(lookupOptions("lookup.async", "true"))
                                        .build(),
                                0))
                .isInstanceOf(AsyncLookupFunctionProvider.class)
                .isNotInstanceOf(LookupFunctionProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.cache",
                                                        "partial",
                                                        "lookup.partial-cache.max-rows",
                                                        "10"))
                                        .build(),
                                0))
                .isInstanceOf(PartialCachingLookupProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.async",
                                                        "true",
                                                        "lookup.cache",
                                                        "partial",
                                                        "lookup.partial-cache.max-rows",
                                                        "10"))
                                        .build(),
                                0))
                .isInstanceOf(PartialCachingAsyncLookupProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.cache",
                                                        "full",
                                                        "lookup.full-cache.periodic-reload.interval",
                                                        "1 min"))
                                        .build(),
                                0))
                .isInstanceOf(FullCachingLookupProvider.class);
    }

    @Test
    void lookupKeysIndexThePostProjectionRow() {
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}, {0}}, projectedType("cf2", "rowkey"));

        assertThat(lookupProvider(projected, 1)).isInstanceOf(LookupFunctionProvider.class);
        assertThatThrownBy(() -> lookupProvider(projected, 0))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
    }

    @Test
    void rejectsACompositeOrNestedLookupKey() {
        BigtableDynamicSource source = minimal().build();

        assertThatThrownBy(
                        () ->
                                source.getLookupRuntimeProvider(
                                        lookupContext(new int[][] {{0}, {1}})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
        assertThatThrownBy(
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[][] {{1, 0}})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
    }
}
