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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.BigQueryStorageReadSource;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.table.BigQueryLookupConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code bigquery} table source as a lookup source: {@code FULL} only, over the scan's read.
 */
class BigQueryLookupSourceTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.DOUBLE()),
                    DataTypes.FIELD("code", DataTypes.INT()));

    /** A {@code FULL} cache reloaded hourly. */
    static BigQueryLookupConfig fullCache() {
        Configuration config = new Configuration();
        config.set(LookupOptions.CACHE_TYPE, LookupCacheType.FULL);
        config.set(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL, Duration.ofHours(1));
        return BigQueryLookupConfig.from(config);
    }

    private static BigQueryDynamicSource.Builder base() {
        return BigQueryDynamicSource.builder()
                .physicalDataType(PHYSICAL)
                .table(TableDestination.of("p", "d", "t"))
                .parentProject("billing")
                .emulatorEndpoint("localhost:1")
                .lookupConfig(fullCache());
    }

    private static BigQueryDynamicSource source(UnaryOperator<BigQueryDynamicSource.Builder> vary) {
        return vary.apply(base()).build();
    }

    @Test
    void aFullCacheLoadReadsTheScansColumnsAndFiltersFromOneStream() {
        BigQueryDynamicSource source =
                source(
                        builder ->
                                builder.rowRestriction("score > 0")
                                        .maxStreamCount(7)
                                        .preferredMinStreamCount(3)
                                        .maxRecordsPerFetch(200)
                                        .maxBytesPerFetch(4096L)
                                        .retryMaxAttempts(9));
        source.applyProjection(
                new int[][] {{1}, {0}},
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.BIGINT())));
        source.applyFilters(
                Collections.singletonList(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(
                                        new FieldReferenceExpression(
                                                "id", DataTypes.BIGINT(), 0, 0),
                                        new ValueLiteralExpression(5L)),
                                DataTypes.BOOLEAN())));

        LookupTableSource.LookupRuntimeProvider provider =
                source.getLookupRuntimeProvider(context(new int[] {0}));

        assertThat(provider).isInstanceOf(FullCachingLookupProvider.class);
        assertThat(((FullCachingLookupProvider) provider).getCacheReloadTrigger())
                .isInstanceOf(PeriodicCacheReloadTrigger.class);
        BigQuerySourceConfig<RowData> load = loadConfig(provider);
        assertThat(load.getTable()).isEqualTo(TableDestination.of("p", "d", "t"));
        assertThat(load.getParentProject()).isEqualTo("billing");
        assertThat(load.getSelectedFields()).containsExactly("name", "id");
        // The scan's own restriction: the explicit one and the pushed filter, combined once.
        assertThat(load.getRowRestriction())
                .isEqualTo(scanConfig(source).getRowRestriction())
                .contains("score > 0")
                .contains("`id` > 5");
        assertThat(load.getMaxStreamCount()).as("one stream per load").isEqualTo(1);
        assertThat(load.getPreferredMinStreamCount()).isZero();
        assertThat(load.getMaxRecordsPerFetch()).isEqualTo(200);
        assertThat(load.getMaxBytesPerFetch()).isEqualTo(4096L);
        assertThat(((ReadClientRowStreamOpener) load.getRowStreamOpener()).retryMaxAttempts())
                .isEqualTo(9);
        assertThat(load.getDeserializer().getProducedType())
                .isEqualTo(
                        InternalTypeInfo.of(
                                DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("id", DataTypes.BIGINT()))
                                        .getLogicalType()));
    }

    @Test
    void theScanOfTheSameTableKeepsItsOwnStreamCounts() {
        // The load's one stream must not leak into the scan, which shares the builder.
        BigQueryDynamicSource source =
                source(builder -> builder.maxStreamCount(7).preferredMinStreamCount(3));
        source.getLookupRuntimeProvider(context(new int[] {0}));

        BigQuerySourceConfig<?> scan = scanConfig(source);
        assertThat(scan.getMaxStreamCount()).isEqualTo(7);
        assertThat(scan.getPreferredMinStreamCount()).isEqualTo(3);
    }

    @Test
    void everyCacheTypeButFullIsRefused() {
        for (LookupCacheType type :
                new LookupCacheType[] {LookupCacheType.NONE, LookupCacheType.PARTIAL}) {
            Configuration config = new Configuration();
            config.set(LookupOptions.CACHE_TYPE, type);
            BigQueryDynamicSource source =
                    source(builder -> builder.lookupConfig(BigQueryLookupConfig.from(config)));

            assertThatThrownBy(() -> source.getLookupRuntimeProvider(context(new int[] {0})))
                    .as("%s", type)
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("only through Flink's full cache")
                    .hasMessageContaining("'lookup.cache' is " + type);
        }
        // A source built without lookup options takes Flink's default, NONE.
        assertThatThrownBy(
                        () ->
                                source(builder -> builder.lookupConfig(null))
                                        .getLookupRuntimeProvider(context(new int[] {0})))
                .hasMessageContaining("'lookup.cache' is NONE");
    }

    @Test
    void aQueryAViewMaterializationOrASnapshotTimeIsRefused() {
        assertThatThrownBy(
                        () ->
                                source(builder -> builder.table(null).query("SELECT 1 AS id"))
                                        .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.query' cannot be set");
        assertThatThrownBy(
                        () ->
                                source(builder -> builder.materializeViews(true))
                                        .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.materialize-views' cannot be true");
        assertThatThrownBy(
                        () ->
                                source(builder -> builder.snapshotTime(Instant.EPOCH))
                                        .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.snapshot-time' cannot be set");
    }

    @Test
    void aKeyMustBeATopLevelColumnWhoseFlinkEqualityIsSqls() {
        BigQueryDynamicSource source = source(UnaryOperator.identity());

        assertThat(source.getLookupRuntimeProvider(context(new int[] {3}, new int[] {1})))
                .isInstanceOf(FullCachingLookupProvider.class);
        assertThatThrownBy(() -> source.getLookupRuntimeProvider(context(new int[] {2})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Lookup column 'score' has unsupported equality type DOUBLE");
        assertThatThrownBy(() -> source.getLookupRuntimeProvider(context()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No equality reached the source as a key");
    }

    private static BigQuerySourceConfig<?> scanConfig(BigQueryDynamicSource source) {
        return TestSources.configOf(
                (BigQueryStorageReadSource<?>)
                        ((SourceProvider)
                                        source.getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                                .createSource());
    }

    @SuppressWarnings("unchecked")
    private static BigQuerySourceConfig<RowData> loadConfig(
            LookupTableSource.LookupRuntimeProvider provider) {
        return ((BigQueryFullCacheInputFormat)
                        ((InputFormatProvider)
                                        ((FullCachingLookupProvider) provider)
                                                .getScanRuntimeProvider())
                                .createInputFormat())
                .getConfig();
    }

    /** A lookup context with the given keys; it compiles against both Flink lines. */
    private static LookupTableSource.LookupContext context(int[]... keys) {
        return new LookupTableSource.LookupContext() {
            @Override
            public int[][] getKeys() {
                return keys;
            }

            // Flink 2.x only; an extra method on 1.20.
            public boolean preferCustomShuffle() {
                return false;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                return createTypeInformation(producedDataType.getLogicalType());
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                return (TypeInformation<T>) InternalTypeInfo.of(producedLogicalType);
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                    DataType producedDataType) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
