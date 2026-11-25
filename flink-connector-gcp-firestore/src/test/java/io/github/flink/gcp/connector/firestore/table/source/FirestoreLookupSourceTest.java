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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.table.FirestoreDynamicTableFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the source's lookup path without a planner. Refusals are asserted on phrases only the
 * connector's sentence carries: {@code FactoryUtil} echoes every option into its own message.
 */
class FirestoreLookupSourceTest {

    private static final ResolvedSchema KEYED =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("name", DataTypes.STRING()),
                            Column.physical("a.b", DataTypes.STRING())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")));

    private static final ResolvedSchema UNKEYED =
            ResolvedSchema.of(
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("a.b", DataTypes.STRING()));

    private static Map<String, String> options(String... pairs) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", FirestoreDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("collection", "users/alice/orders");
        for (int i = 0; i < pairs.length; i += 2) {
            options.put(pairs[i], pairs[i + 1]);
        }
        return options;
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

    private static FirestoreDynamicSource source(ResolvedSchema schema, String... pairs) {
        return (FirestoreDynamicSource) FactoryMocks.createTableSource(schema, options(pairs));
    }

    private static int maxRetries(LookupTableSource.LookupRuntimeProvider provider) {
        return provider instanceof LookupFunctionProvider
                ? ((FirestoreRowDataLookupFunction)
                                ((LookupFunctionProvider) provider).createLookupFunction())
                        .maxRetries()
                : ((FirestoreRowDataAsyncLookupFunction)
                                ((AsyncLookupFunctionProvider) provider)
                                        .createAsyncLookupFunction())
                        .maxRetries();
    }

    private static FirestoreCollectionDocumentLookup documentLookup(
            LookupTableSource.LookupRuntimeProvider provider) {
        if (provider instanceof LookupFunctionProvider) {
            return (FirestoreCollectionDocumentLookup)
                    ((FirestoreRowDataLookupFunction)
                                    ((LookupFunctionProvider) provider).createLookupFunction())
                            .documentLookup();
        }
        return (FirestoreCollectionDocumentLookup)
                ((FirestoreRowDataAsyncLookupFunction)
                                ((AsyncLookupFunctionProvider) provider)
                                        .createAsyncLookupFunction())
                        .documentLookup();
    }

    @Test
    void aLookupIsSynchronousAndUncachedByDefaultAndReadsTheDeclaredFields() {
        LookupTableSource.LookupRuntimeProvider provider =
                source(KEYED, "service-account-key-file", "/keys/sa.json")
                        .getLookupRuntimeProvider(context(new int[] {0}));

        assertThat(provider)
                .isInstanceOf(LookupFunctionProvider.class)
                .isNotInstanceOf(PartialCachingLookupProvider.class);
        FirestoreCollectionDocumentLookup lookup = documentLookup(provider);
        assertThat(lookup.collection()).isEqualTo("users/alice/orders");
        assertThat(lookup.fields()).containsExactly("name", "`a.b`");
        assertThat(lookup.serviceAccountKeyFile()).isEqualTo("/keys/sa.json");
    }

    @Test
    void theRetryBudgetReachesBothFunctions() {
        assertThat(maxRetries(source(KEYED).getLookupRuntimeProvider(context(new int[] {0}))))
                .isEqualTo(3);
        assertThat(
                        maxRetries(
                                source(KEYED, "lookup.max-retries", "7")
                                        .getLookupRuntimeProvider(context(new int[] {0}))))
                .isEqualTo(7);
        assertThat(
                        maxRetries(
                                source(KEYED, "lookup.async", "true", "lookup.max-retries", "7")
                                        .getLookupRuntimeProvider(context(new int[] {0}))))
                .isEqualTo(7);
    }

    @Test
    void everyPartialCacheOptionReachesFlinksCacheInBothModes() throws Exception {
        String[] cache = {
            "lookup.cache", "PARTIAL",
            "lookup.partial-cache.expire-after-access", "1 min",
            "lookup.partial-cache.expire-after-write", "2 min",
            "lookup.partial-cache.cache-missing-key", "false",
            "lookup.partial-cache.max-rows", "10"
        };
        DefaultLookupCache expected =
                DefaultLookupCache.newBuilder()
                        .expireAfterAccess(Duration.ofMinutes(1))
                        .expireAfterWrite(Duration.ofMinutes(2))
                        .cacheMissingKey(false)
                        .maximumSize(10)
                        .build();
        String[] async = Arrays.copyOf(cache, cache.length + 2);
        async[cache.length] = "lookup.async";
        async[cache.length + 1] = "true";

        assertThat(
                        ((PartialCachingLookupProvider)
                                        source(KEYED, cache)
                                                .getLookupRuntimeProvider(context(new int[] {0})))
                                .getCache())
                .isEqualTo(expected);
        assertThat(
                        ((PartialCachingAsyncLookupProvider)
                                        source(KEYED, async)
                                                .getLookupRuntimeProvider(context(new int[] {0})))
                                .getCache())
                .isEqualTo(expected);
    }

    @Test
    void theNamedDatabaseAndProjectReachTheClient() throws Exception {
        FirestoreCollectionDocumentLookup lookup =
                documentLookup(
                        source(KEYED, "database", "db", "emulator-endpoint", "localhost:8080")
                                .getLookupRuntimeProvider(context(new int[] {0})));

        assertThat(lookup.settings().build().getProjectId()).isEqualTo("my-project");
        assertThat(lookup.settings().build().getDatabaseId()).isEqualTo("db");
    }

    @Test
    void theOptionsChooseAsyncAndThePartialCache() {
        assertThat(
                        source(KEYED, "lookup.async", "true")
                                .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(AsyncLookupFunctionProvider.class)
                .isNotInstanceOf(PartialCachingAsyncLookupProvider.class);
        assertThat(
                        source(
                                        KEYED,
                                        "lookup.cache",
                                        "PARTIAL",
                                        "lookup.partial-cache.max-rows",
                                        "10")
                                .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(PartialCachingLookupProvider.class);
        assertThat(
                        source(
                                        KEYED,
                                        "lookup.async",
                                        "true",
                                        "lookup.cache",
                                        "PARTIAL",
                                        "lookup.partial-cache.max-rows",
                                        "10")
                                .getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(PartialCachingAsyncLookupProvider.class);
    }

    @Test
    void aProjectedLookupReadsOnlyTheProjectedFieldsAndFindsTheKeyByItsNewPosition() {
        FirestoreDynamicSource source = source(KEYED);
        source.applyProjection(
                new int[][] {{2}, {0}},
                DataTypes.ROW(
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.STRING().notNull())));

        assertThat(documentLookup(source.getLookupRuntimeProvider(context(new int[] {1}))).fields())
                .containsExactly("`a.b`");
        assertThatThrownBy(() -> source.getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires an equality predicate on the PRIMARY KEY column");
    }

    @Test
    void acceptsAdditionalPhysicalKeysInEitherOrder() {
        assertThat(source(KEYED).getLookupRuntimeProvider(context(new int[] {0}, new int[] {1})))
                .isInstanceOf(LookupFunctionProvider.class);
        assertThat(source(KEYED).getLookupRuntimeProvider(context(new int[] {1}, new int[] {0})))
                .isInstanceOf(LookupFunctionProvider.class);
    }

    @Test
    void aLookupWithoutTheDocumentIdOrWithMalformedKeysIsRefused() {
        for (int[][] keys : new int[][][] {{{1}}, {{0}, {0}}, {{0, 0}}, {}, {{3}}, {{-1}}}) {
            assertThatThrownBy(() -> source(KEYED).getLookupRuntimeProvider(context(keys)))
                    .as("%s", (Object) keys)
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "requires an equality predicate on the PRIMARY KEY column");
        }
        assertThatThrownBy(() -> source(UNKEYED).getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("A table without a PRIMARY KEY");
    }

    @Test
    void anAdditionalMetadataKeyExplainsTheUnsupportedKeyEvenWhenTheIdIsPresent() {
        FirestoreDynamicSource source = source(KEYED);
        source.applyReadableMetadata(
                List.of("update-time"),
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("update_time", DataTypes.TIMESTAMP_LTZ(9).notNull())));

        assertThatThrownBy(
                        () ->
                                source.getLookupRuntimeProvider(
                                        context(new int[] {0}, new int[] {3})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("PRIMARY KEY column 'id'")
                .hasMessageContaining("metadata and nested key paths are unsupported");
    }

    private static FirestoreFullCacheInputFormat fullCacheLoader(
            LookupTableSource.LookupRuntimeProvider provider) {
        assertThat(provider).isInstanceOf(FullCachingLookupProvider.class);
        return (FirestoreFullCacheInputFormat)
                ((InputFormatProvider)
                                ((FullCachingLookupProvider) provider).getScanRuntimeProvider())
                        .createInputFormat();
    }

    @Test
    void aFullCacheLoadsTheScansReadOfTheProducedFieldsWithTheReloadTrigger() {
        LookupTableSource.LookupRuntimeProvider provider =
                source(
                                KEYED,
                                "lookup.cache",
                                "FULL",
                                "lookup.full-cache.periodic-reload.interval",
                                "10 min",
                                "scan.max-rows-per-fetch",
                                "7",
                                "service-account-key-file",
                                "/keys/sa.json")
                        .getLookupRuntimeProvider(context(new int[] {0}));

        FirestoreSourceConfig<RowData> config = fullCacheLoader(provider).getConfig();
        assertThat(config.getCollectionGroup()).isNull();
        assertThat(config.getPageSize()).isEqualTo(7);
        assertThat(config.getServiceAccountKeyFile()).isEqualTo("/keys/sa.json");
        assertThat(((FullCachingLookupProvider) provider).getCacheReloadTrigger())
                .isInstanceOf(PeriodicCacheReloadTrigger.class);
    }

    @Test
    void aFullCacheLoadReadsAtItsOwnTimeWhateverScanReadTimeSays() {
        FirestoreDynamicSource source =
                source(
                        KEYED,
                        "lookup.cache",
                        "FULL",
                        "lookup.full-cache.periodic-reload.interval",
                        "1 h",
                        "scan.read-time",
                        "2026-10-04T00:00:00Z");

        assertThat(
                        fullCacheLoader(source.getLookupRuntimeProvider(context(new int[] {0})))
                                .getConfig()
                                .getReadTime())
                .isNull();
    }

    @Test
    void aFullCacheServesAnyTopLevelKeyIncludingOnATableWithoutAPrimaryKey() {
        assertThat(
                        source(
                                        KEYED,
                                        "lookup.cache",
                                        "FULL",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 h")
                                .getLookupRuntimeProvider(context(new int[] {1})))
                .isInstanceOf(FullCachingLookupProvider.class);
        FirestoreDynamicSource group =
                source(
                        UNKEYED,
                        "lookup.cache",
                        "FULL",
                        "lookup.full-cache.periodic-reload.interval",
                        "1 h",
                        "scan.collection-group",
                        "true",
                        "scan.partition.max-partitions",
                        "4");
        group.applyReadableMetadata(
                List.of("document-path"),
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("document_path", DataTypes.STRING().notNull())));

        FirestoreSourceConfig<RowData> config =
                fullCacheLoader(group.getLookupRuntimeProvider(context(new int[] {2}))).getConfig();
        assertThat(config.getCollectionGroup()).isEqualTo("orders");
        // A load is one split read serially, so the group is one partition whatever the scan asks.
        assertThat(config.getPartitionCount()).isNull();
    }

    @Test
    void aFullCacheRefusesAKeyWhoseJavaEqualityIsNotSqls() {
        ResolvedSchema typed =
                ResolvedSchema.of(
                        Column.physical("ratio", DataTypes.DOUBLE()),
                        Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        Column.physical(
                                "r", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()))),
                        Column.physical("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        Column.physical("n", DataTypes.BIGINT()),
                        Column.physical("flag", DataTypes.BOOLEAN()),
                        Column.physical("code", DataTypes.CHAR(3)),
                        Column.physical("raw", DataTypes.BINARY(2)),
                        Column.physical("bytes", DataTypes.BYTES()),
                        Column.physical("at", DataTypes.TIMESTAMP_LTZ(3)));

        for (int key : new int[] {0, 1, 2, 3}) {
            assertThatThrownBy(
                            () ->
                                    source(
                                                    typed,
                                                    "lookup.cache",
                                                    "FULL",
                                                    "lookup.full-cache.periodic-reload.interval",
                                                    "1 h")
                                            .getLookupRuntimeProvider(context(new int[] {key})))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("'lookup.cache' = 'FULL' requires equality predicates")
                    .hasMessageContaining("has unsupported equality type");
        }
        for (int[][] keys : new int[][][] {{}, {{2, 0}}}) {
            assertThatThrownBy(
                            () ->
                                    source(
                                                    typed,
                                                    "lookup.cache",
                                                    "FULL",
                                                    "lookup.full-cache.periodic-reload.interval",
                                                    "1 h")
                                            .getLookupRuntimeProvider(context(keys)))
                    .as("%s", (Object) keys)
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("'lookup.cache' = 'FULL' requires equality predicates");
        }
        for (int key = 4; key <= 9; key++) {
            assertThat(
                            source(
                                            typed,
                                            "lookup.cache",
                                            "FULL",
                                            "lookup.full-cache.periodic-reload.interval",
                                            "1 h")
                                    .getLookupRuntimeProvider(context(new int[] {key})))
                    .as("key %s", key)
                    .isInstanceOf(FullCachingLookupProvider.class);
        }
    }

    @Test
    void aFullCacheRefusesAsynchronousLookupsUnderBothKeys() {
        assertThatThrownBy(
                        () ->
                                source(
                                        KEYED,
                                        "lookup.cache",
                                        "FULL",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 h",
                                        "lookup.async",
                                        "true"))
                .hasStackTraceContaining(
                        "'lookup.async' cannot be true when 'lookup.cache' is FULL");
    }

    @Test
    void aFullCacheReloadOptionFlinkRefusesFailsPlanning() {
        // FactoryUtil echoes every option into its own message, so the phrase is Flink's trigger's.
        assertThatThrownBy(() -> source(KEYED, "lookup.cache", "FULL"))
                .hasStackTraceContaining("required to build Periodic cache reload trigger");
    }

    @Test
    void aNegativeRetryBudgetIsRefusedUnderItsKey() {
        assertThatThrownBy(() -> source(KEYED, "lookup.max-retries", "-1"))
                .hasStackTraceContaining("Option 'lookup.max-retries' must be zero or greater.");
    }

    @Test
    void aPartialCacheFlinkRefusesFailsPlanning() {
        // Flink requires an expiry or a row bound for a partial cache.
        assertThatThrownBy(() -> source(KEYED, "lookup.cache", "PARTIAL"))
                .hasStackTraceContaining("The cache will not have evictions");
    }

    @Test
    void aNegativeRowBoundIsRefusedWhenPlannedNotWhenTheCacheOpens() {
        assertThatThrownBy(
                        () ->
                                source(
                                        KEYED,
                                        "lookup.cache",
                                        "PARTIAL",
                                        "lookup.partial-cache.max-rows",
                                        "-1"))
                .hasStackTraceContaining(
                        "Option 'lookup.partial-cache.max-rows' must be zero or greater.");
    }

    @Test
    void copyKeepsTheLookupConfigAndEqualityTellsItApart() {
        FirestoreDynamicSource async = source(KEYED, "lookup.async", "true");

        assertThat(async.copy()).isEqualTo(async);
        assertThat(async).isNotEqualTo(source(KEYED));
        assertThat(source(KEYED, "lookup.max-retries", "1")).isNotEqualTo(source(KEYED));
        assertThat(source(KEYED, "lookup.max-retries", "1").hashCode())
                .isNotEqualTo(source(KEYED).hashCode());
        String[] hourly = {
            "lookup.cache", "FULL", "lookup.full-cache.periodic-reload.interval", "1 h"
        };
        String[] daily = {
            "lookup.cache", "FULL", "lookup.full-cache.periodic-reload.interval", "1 d"
        };
        assertThat(source(KEYED, hourly)).isEqualTo(source(KEYED, hourly));
        assertThat(source(KEYED, hourly)).isNotEqualTo(source(KEYED, daily));
        assertThat(source(KEYED, hourly).hashCode()).isNotEqualTo(source(KEYED, daily).hashCode());
    }

    @Test
    void theMaskDecodesEachPathAndNamesOnlyTheNameWhenThereIsNone() {
        assertThat(
                        FirestoreCollectionDocumentLookup.maskPaths(
                                new String[] {"`a.b`", "n", "r.`x.y`"}))
                .containsExactly(FieldPath.of("a.b"), FieldPath.of("n"), FieldPath.of("r", "x.y"));
        assertThat(FirestoreCollectionDocumentLookup.maskPaths(new String[0]))
                .containsExactly(FieldPath.documentId());
    }

    @Test
    void aMalformedEmulatorEndpointIsRefusedUnderItsKeyWhenTheClientIsBuilt() {
        FirestoreCollectionDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        DatabaseDestination.of("p"), "c", new String[0], "no-port", null);

        assertThatThrownBy(lookup::settings).hasMessageContaining("emulator-endpoint");
    }
}
