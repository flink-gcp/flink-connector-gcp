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
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupRequest;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
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

    @Test
    void aFullCacheIsRefusedUnderItsKey() {
        assertThatThrownBy(() -> source(KEYED, "lookup.cache", "FULL"))
                .hasStackTraceContaining(
                        "Option 'lookup.cache' does not support FULL for the Datastore table"
                                + " source");
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
