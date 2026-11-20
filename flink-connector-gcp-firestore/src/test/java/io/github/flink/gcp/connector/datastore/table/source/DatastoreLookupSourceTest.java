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

package io.github.flink.gcp.connector.datastore.table.source;

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
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupRequest;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.table.DatastoreDynamicTableFactory;
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
class DatastoreLookupSourceTest {

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
        options.put("connector", DatastoreDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("kind", "Order");
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

    private static DatastoreDynamicSource source(ResolvedSchema schema, String... pairs) {
        return (DatastoreDynamicSource) FactoryMocks.createTableSource(schema, options(pairs));
    }

    private static int maxRetries(LookupTableSource.LookupRuntimeProvider provider) {
        return provider instanceof LookupFunctionProvider
                ? ((DatastoreRowDataLookupFunction)
                                ((LookupFunctionProvider) provider).createLookupFunction())
                        .maxRetries()
                : ((DatastoreRowDataAsyncLookupFunction)
                                ((AsyncLookupFunctionProvider) provider)
                                        .createAsyncLookupFunction())
                        .maxRetries();
    }

    private static DatastoreKindEntityLookup entityLookup(
            LookupTableSource.LookupRuntimeProvider provider) {
        if (provider instanceof LookupFunctionProvider) {
            return (DatastoreKindEntityLookup)
                    ((DatastoreRowDataLookupFunction)
                                    ((LookupFunctionProvider) provider).createLookupFunction())
                            .entityLookup();
        }
        return (DatastoreKindEntityLookup)
                ((DatastoreRowDataAsyncLookupFunction)
                                ((AsyncLookupFunctionProvider) provider)
                                        .createAsyncLookupFunction())
                        .entityLookup();
    }

    @Test
    void aLookupIsSynchronousAndUncachedByDefaultAndReadsTheDeclaredProperties() {
        LookupTableSource.LookupRuntimeProvider provider =
                source(KEYED, "service-account-key-file", "/keys/sa.json")
                        .getLookupRuntimeProvider(context(new int[] {0}));

        assertThat(provider)
                .isInstanceOf(LookupFunctionProvider.class)
                .isNotInstanceOf(PartialCachingLookupProvider.class);
        DatastoreKindEntityLookup lookup = entityLookup(provider);
        assertThat(lookup.paths()).containsExactly("`name`", "`a.b`");
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
    void theRequestNamesTheDatabaseNamespaceAndKindAndReadsStrongly() {
        DatastoreDynamicSource source =
                source(
                        KEYED,
                        "database",
                        "db",
                        "namespace",
                        "tenant",
                        "service-account-key-file",
                        "/k",
                        "scan.read-time",
                        "2026-10-04T00:00:00Z");
        LookupTableSource.LookupRuntimeProvider provider =
                source.getLookupRuntimeProvider(context(new int[] {0}));
        // The function's own key builder, which the source configured.
        Key key =
                ((DatastoreRowDataLookupFunction)
                                ((LookupFunctionProvider) provider).createLookupFunction())
                        .keys()
                        .key(GenericRowData.of(StringData.fromString("o1")));

        LookupRequest request = entityLookup(provider).request(key);

        assertThat(request.getProjectId()).isEqualTo("my-project");
        assertThat(request.getDatabaseId()).isEqualTo("db");
        assertThat(request.getKeysList()).containsExactly(key);
        assertThat(key.getPartitionId().getProjectId()).isEqualTo("my-project");
        assertThat(key.getPartitionId().getDatabaseId()).isEqualTo("db");
        assertThat(key.getPartitionId().getNamespaceId()).isEqualTo("tenant");
        assertThat(key.getPath(0).getKind()).isEqualTo("Order");
        assertThat(key.getPath(0).getName()).isEqualTo("o1");
        assertThat(request.hasReadOptions())
                .as("strong, the default, whatever scan.read-time says")
                .isFalse();
        assertThat(request.getPropertyMask().getPathsList()).containsExactly("`name`", "`a.b`");
    }

    @Test
    void theEmulatorIsReachedWithoutCredentials() throws Exception {
        DatastoreKindEntityLookup emulator =
                entityLookup(
                        source(KEYED, "emulator-endpoint", "localhost:8080")
                                .getLookupRuntimeProvider(context(new int[] {0})));

        assertThat(emulator.settings().getCredentialsProvider())
                .isInstanceOf(NoCredentialsProvider.class);
        assertThat(emulator.serviceAccountKeyFile()).isNull();
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
    void aProjectedLookupReadsOnlyTheProjectedPropertiesAndFindsTheKeyByItsNewPosition() {
        DatastoreDynamicSource source = source(KEYED);
        source.applyProjection(
                new int[][] {{2}, {0}},
                DataTypes.ROW(
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.STRING().notNull())));

        assertThat(entityLookup(source.getLookupRuntimeProvider(context(new int[] {1}))).paths())
                .containsExactly("`a.b`");
        assertThatThrownBy(() -> source.getLookupRuntimeProvider(context(new int[] {0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires an equality predicate on the PRIMARY KEY column");
    }

    @Test
    void aLookupOfOnlyTheKeyAsksForTheKeyAlone() {
        DatastoreDynamicSource source = source(KEYED, "lookup.async", "true");
        source.applyProjection(
                new int[][] {{0}},
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING().notNull())));

        assertThat(entityLookup(source.getLookupRuntimeProvider(context(new int[] {0}))).paths())
                .containsExactly("__key__");
    }

    @Test
    void theAsynchronousFunctionBuildsBigintKeysAsIds() {
        ResolvedSchema idKeyed =
                new ResolvedSchema(
                        List.of(
                                Column.physical("id", DataTypes.BIGINT().notNull()),
                                Column.physical("name", DataTypes.STRING())),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("id")));

        Key key =
                ((DatastoreRowDataAsyncLookupFunction)
                                ((AsyncLookupFunctionProvider)
                                                source(idKeyed, "lookup.async", "true")
                                                        .getLookupRuntimeProvider(
                                                                context(new int[] {0})))
                                        .createAsyncLookupFunction())
                        .keys()
                        .key(GenericRowData.of(-5L));

        assertThat(key.getPath(0).getKind()).isEqualTo("Order");
        assertThat(key.getPath(0).getId()).isEqualTo(-5L);
        assertThat(key.getPartitionId().getNamespaceId()).isEmpty();
    }

    @Test
    void acceptsAdditionalPhysicalKeysInEitherOrder() {
        assertThat(source(KEYED).getLookupRuntimeProvider(context(new int[] {0}, new int[] {1})))
                .isInstanceOf(LookupFunctionProvider.class);
        assertThat(source(KEYED).getLookupRuntimeProvider(context(new int[] {1}, new int[] {0})))
                .isInstanceOf(LookupFunctionProvider.class);
    }

    @Test
    void aLookupWithoutTheKeyOrWithMalformedKeysIsRefused() {
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
        DatastoreDynamicSource source = source(KEYED);
        source.applyReadableMetadata(
                List.of("update-time"),
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("update_time", DataTypes.TIMESTAMP_LTZ(6).notNull())));

        assertThatThrownBy(
                        () ->
                                source.getLookupRuntimeProvider(
                                        context(new int[] {0}, new int[] {3})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("PRIMARY KEY column 'id'")
                .hasMessageContaining("metadata and nested key paths are unsupported");
    }

    private static DatastoreFullCacheInputFormat fullCacheLoader(
            LookupTableSource.LookupRuntimeProvider provider) {
        assertThat(provider).isInstanceOf(FullCachingLookupProvider.class);
        return (DatastoreFullCacheInputFormat)
                ((InputFormatProvider)
                                ((FullCachingLookupProvider) provider).getScanRuntimeProvider())
                        .createInputFormat();
    }

    private static DatastoreDynamicSource fullCacheSource(ResolvedSchema schema, String... pairs) {
        String[] all = new String[pairs.length + 4];
        all[0] = "lookup.cache";
        all[1] = "FULL";
        all[2] = "lookup.full-cache.periodic-reload.interval";
        all[3] = "1 h";
        System.arraycopy(pairs, 0, all, 4, pairs.length);
        return source(schema, all);
    }

    @Test
    void aFullCacheLoadsTheScansReadOfTheKindAsOneKeyRangeWithTheReloadTrigger() {
        LookupTableSource.LookupRuntimeProvider provider =
                source(
                                KEYED,
                                "lookup.cache",
                                "FULL",
                                "lookup.full-cache.periodic-reload.interval",
                                "10 min",
                                "namespace",
                                "tenant",
                                "scan.max-rows-per-fetch",
                                "7",
                                "scan.partition.max-partitions",
                                "4",
                                "service-account-key-file",
                                "/keys/sa.json")
                        .getLookupRuntimeProvider(context(new int[] {0}));

        DatastoreSourceConfig<RowData> config = fullCacheLoader(provider).getConfig();
        assertThat(config.getQuery().getKind(0).getName()).isEqualTo("Order");
        assertThat(config.getQuery().hasFilter()).isFalse();
        assertThat(config.getNamespace()).isEqualTo("tenant");
        assertThat(config.getPageSize()).isEqualTo(7);
        assertThat(config.getServiceAccountKeyFile()).isEqualTo("/keys/sa.json");
        // A load is one split read serially, so the kind is one key range whatever the scan asks.
        assertThat(config.getSplitCount()).isEqualTo(1);
        assertThat(((FullCachingLookupProvider) provider).getCacheReloadTrigger())
                .isInstanceOf(PeriodicCacheReloadTrigger.class);
    }

    @Test
    void aFullCacheLoadReadsAtItsOwnTimeAndWithoutALimit() {
        DatastoreDynamicSource source =
                fullCacheSource(KEYED, "scan.read-time", "2026-10-04T00:00:00Z");
        source.applyLimit(5);

        DatastoreSourceConfig<RowData> config =
                fullCacheLoader(source.getLookupRuntimeProvider(context(new int[] {0})))
                        .getConfig();
        assertThat(config.getReadTime()).isNull();
        assertThat(config.getQuery().hasLimit()).isFalse();
    }

    private static ResolvedExpression compare(
            BuiltInFunctionDefinition function, String column, int index, String value) {
        return CallExpression.permanent(
                function,
                List.of(
                        new FieldReferenceExpression(column, DataTypes.STRING(), 0, index),
                        new ValueLiteralExpression(value)),
                DataTypes.BOOLEAN());
    }

    @Test
    void aFullCacheLoadCarriesThePushedFiltersAsOneKeyRange() {
        // A key equality is pushed without the option and leaves the query splittable, so the
        // load still asks for one split.
        DatastoreDynamicSource keyed = fullCacheSource(KEYED);
        keyed.applyFilters(List.of(compare(BuiltInFunctionDefinitions.EQUALS, "id", 0, "k")));
        DatastoreSourceConfig<RowData> equality =
                fullCacheLoader(keyed.getLookupRuntimeProvider(context(new int[] {1}))).getConfig();
        assertThat(equality.getQuery().hasFilter()).isTrue();
        assertThat(equality.getSplitCount()).isEqualTo(1);

        // A range under the option makes the query one split already, which takes no count.
        DatastoreDynamicSource ranged =
                fullCacheSource(KEYED, "scan.filter-push-down.enabled", "true");
        ranged.applyFilters(
                List.of(compare(BuiltInFunctionDefinitions.GREATER_THAN, "name", 1, "a")));
        DatastoreSourceConfig<RowData> range =
                fullCacheLoader(ranged.getLookupRuntimeProvider(context(new int[] {0})))
                        .getConfig();
        assertThat(range.getQuery().hasFilter()).isTrue();
        assertThat(range.getSplitCount()).isNull();
    }

    @Test
    void everyFullCacheReloadOptionReachesTheTrigger() {
        LookupTableSource.LookupRuntimeProvider timed =
                source(
                                KEYED,
                                "lookup.cache",
                                "FULL",
                                "lookup.full-cache.reload-strategy",
                                "TIMED",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15:30",
                                "lookup.full-cache.timed-reload.interval-in-days",
                                "2")
                        .getLookupRuntimeProvider(context(new int[] {0}));
        assertThat(((FullCachingLookupProvider) timed).getCacheReloadTrigger())
                .isInstanceOf(TimedCacheReloadTrigger.class);

        // The factory accepts each option, and each one tells two tables apart.
        String[] daily = {
            "lookup.cache",
            "FULL",
            "lookup.full-cache.reload-strategy",
            "TIMED",
            "lookup.full-cache.timed-reload.iso-time",
            "10:15:30"
        };
        assertThat(source(KEYED, daily))
                .isNotEqualTo(
                        source(
                                KEYED,
                                "lookup.cache",
                                "FULL",
                                "lookup.full-cache.reload-strategy",
                                "TIMED",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15:30",
                                "lookup.full-cache.timed-reload.interval-in-days",
                                "2"));
        assertThat(fullCacheSource(KEYED))
                .isNotEqualTo(
                        fullCacheSource(
                                KEYED,
                                "lookup.full-cache.periodic-reload.schedule-mode",
                                "FIXED_RATE"));
    }

    @Test
    void aFullCacheServesAnyTopLevelKeyIncludingOnATableWithoutAPrimaryKey() {
        assertThat(fullCacheSource(KEYED).getLookupRuntimeProvider(context(new int[] {1})))
                .isInstanceOf(FullCachingLookupProvider.class);
        DatastoreDynamicSource keyless = fullCacheSource(UNKEYED);
        keyless.applyReadableMetadata(
                List.of("key-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD("key_id", DataTypes.BIGINT())));

        // A point lookup refuses a table without a PRIMARY KEY; the full cache joins it by any
        // column, the key's numeric id among them.
        assertThat(keyless.getLookupRuntimeProvider(context(new int[] {2})))
                .isInstanceOf(FullCachingLookupProvider.class);
    }

    @Test
    void aFullCacheRefusesAKeyWhoseJavaEqualityIsNotSqls() {
        ResolvedSchema typed =
                new ResolvedSchema(
                        List.of(
                                Column.physical("id", DataTypes.STRING().notNull()),
                                Column.physical("ratio", DataTypes.DOUBLE()),
                                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                                Column.physical(
                                        "r",
                                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()))),
                                Column.physical("n", DataTypes.BIGINT()),
                                Column.physical("flag", DataTypes.BOOLEAN()),
                                Column.physical("code", DataTypes.CHAR(3)),
                                Column.physical("raw", DataTypes.BINARY(2)),
                                Column.physical("bytes", DataTypes.BYTES()),
                                Column.physical("at", DataTypes.TIMESTAMP_LTZ(3))),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("id")));

        for (int key : new int[] {1, 2, 3}) {
            assertThatThrownBy(
                            () ->
                                    fullCacheSource(typed)
                                            .getLookupRuntimeProvider(context(new int[] {key})))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "A Datastore lookup with 'lookup.cache' = 'FULL' requires equality"
                                    + " predicates")
                    .hasMessageContaining("has unsupported equality type");
        }
        assertThatThrownBy(() -> fullCacheSource(typed).getLookupRuntimeProvider(context()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("a cast column is not a key");
        assertThatThrownBy(
                        () ->
                                fullCacheSource(typed)
                                        .getLookupRuntimeProvider(context(new int[] {3, 0})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("A nested key path is not supported");
        for (int key = 0; key <= 9; key++) {
            if (key >= 1 && key <= 3) {
                continue;
            }
            assertThat(fullCacheSource(typed).getLookupRuntimeProvider(context(new int[] {key})))
                    .as("key %s", key)
                    .isInstanceOf(FullCachingLookupProvider.class);
        }
    }

    @Test
    void aFullCacheRefusesAsynchronousLookups() {
        assertThatThrownBy(() -> fullCacheSource(KEYED, "lookup.async", "true"))
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
        DatastoreDynamicSource async = source(KEYED, "lookup.async", "true");

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
    void theMaskQuotesEachPropertyAndNamesOnlyTheKeyWhenThereIsNone() {
        assertThat(
                        DatastoreKindEntityLookup.maskPaths(
                                new String[] {"a.b", "n", "back`quote", "back\\slash"}))
                .containsExactly("`a.b`", "`n`", "`back\\`quote`", "`back\\\\slash`");
        assertThat(DatastoreKindEntityLookup.maskPaths(new String[0])).containsExactly("__key__");
    }

    @Test
    void aMalformedEmulatorEndpointIsRefusedUnderItsKeyWhenTheClientIsBuilt() {
        DatastoreKindEntityLookup lookup =
                new DatastoreKindEntityLookup(
                        DatabaseDestination.of("p"), new String[0], "no-port", null);

        assertThatThrownBy(lookup::settings).hasMessageContaining("emulator-endpoint");
    }
}
