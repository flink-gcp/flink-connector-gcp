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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.table.SpannerLookupConfig;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerLookupSourceTest {
    @TempDir Path tempDir;
    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("region", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("account", DataTypes.BIGINT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()));
    private static final SpannerTableSchemaConverter SCHEMA =
            SpannerTableSchemaConverter.of(
                    (RowType) PHYSICAL.getLogicalType(),
                    new int[] {0, 1},
                    Dialect.GOOGLE_STANDARD_SQL,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap());
    private static final SpannerTableSchemaConverter POSTGRESQL_DECIMAL_SCHEMA =
            SpannerTableSchemaConverter.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                            DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)))
                                    .getLogicalType(),
                    new int[] {0},
                    Dialect.POSTGRESQL,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap());
    private static final SpannerTableSchemaConverter UUID_SCHEMA =
            SpannerTableSchemaConverter.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    "external_id", DataTypes.STRING().notNull()),
                                            DataTypes.FIELD("tenant", DataTypes.BIGINT().notNull()),
                                            DataTypes.FIELD("name", DataTypes.STRING()))
                                    .getLogicalType(),
                    new int[] {0, 1},
                    Dialect.GOOGLE_STANDARD_SQL,
                    Collections.emptyList(),
                    Collections.singletonList("external_id"),
                    Collections.emptyMap(),
                    Collections.emptyMap());

    @Test
    void selectsSyncAsyncAndPartialCacheProviders() {
        assertThat(provider(config(), new int[][] {{0}, {1}}))
                .isInstanceOf(LookupFunctionProvider.class);
        assertThat(provider(config("lookup.async", "true"), new int[][] {{0}, {1}}))
                .isInstanceOf(AsyncLookupFunctionProvider.class);
        assertThat(
                        provider(
                                config(
                                        "lookup.cache",
                                        "partial",
                                        "lookup.partial-cache.max-rows",
                                        "10"),
                                new int[][] {{0}, {1}}))
                .isInstanceOf(PartialCachingLookupProvider.class);
        assertThat(
                        provider(
                                config(
                                        "lookup.async",
                                        "true",
                                        "lookup.cache",
                                        "partial",
                                        "lookup.partial-cache.max-rows",
                                        "10"),
                                new int[][] {{0}, {1}}))
                .isInstanceOf(PartialCachingAsyncLookupProvider.class);
    }

    @Test
    void passesTheQualifiedTableToSyncAndAsyncPointReads() {
        LookupFunctionProvider sync =
                (LookupFunctionProvider)
                        provider(config("schema", "analytics"), new int[][] {{0}, {1}});
        AsyncLookupFunctionProvider async =
                (AsyncLookupFunctionProvider)
                        provider(
                                config("schema", "analytics", "lookup.async", "true"),
                                new int[][] {{0}, {1}});

        assertThat(((SpannerRowDataLookupFunction) sync.createLookupFunction()).rowLookup())
                .isInstanceOfSatisfying(
                        SpannerDatabaseRowLookup.class,
                        lookup -> assertThat(lookup.table()).isEqualTo("analytics.people"));
        assertThat(
                        ((SpannerRowDataAsyncLookupFunction) async.createAsyncLookupFunction())
                                .rowLookup())
                .isInstanceOfSatisfying(
                        SpannerDatabaseRowLookup.class,
                        lookup -> assertThat(lookup.table()).isEqualTo("analytics.people"));
    }

    @Test
    void passesTheCredentialPathToSyncAndAsyncPointReads() {
        LookupFunctionProvider sync =
                (LookupFunctionProvider)
                        provider(
                                config("service-account-key-file", "/var/run/secrets/spanner.json"),
                                new int[][] {{0}, {1}});
        AsyncLookupFunctionProvider async =
                (AsyncLookupFunctionProvider)
                        provider(
                                config(
                                        "service-account-key-file",
                                        "/var/run/secrets/spanner.json",
                                        "lookup.async",
                                        "true"),
                                new int[][] {{0}, {1}});

        assertThat(((SpannerRowDataLookupFunction) sync.createLookupFunction()).rowLookup())
                .isInstanceOfSatisfying(
                        SpannerDatabaseRowLookup.class,
                        lookup ->
                                assertThat(lookup.serviceAccountKeyFile())
                                        .isEqualTo("/var/run/secrets/spanner.json"));
        assertThat(
                        ((SpannerRowDataAsyncLookupFunction) async.createAsyncLookupFunction())
                                .rowLookup())
                .isInstanceOfSatisfying(
                        SpannerDatabaseRowLookup.class,
                        lookup ->
                                assertThat(lookup.serviceAccountKeyFile())
                                        .isEqualTo("/var/run/secrets/spanner.json"));
    }

    @Test
    void lookupLoadsTheCredentialPathWhenTheFunctionOpens() {
        String path = "/missing/spanner-service-account.json";
        SpannerDatabaseRowLookup lookup =
                new SpannerDatabaseRowLookup(
                        SpannerDatabase.of("p", "i", "d"),
                        "people",
                        Collections.singletonList("id"),
                        null,
                        path);

        assertThatThrownBy(lookup::open)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
    }

    @Test
    void lookupInjectsRuntimeCredentialsIntoClientSettings() throws Exception {
        SpannerDatabaseRowLookup lookup =
                new SpannerDatabaseRowLookup(
                        SpannerDatabase.of("p", "i", "d"),
                        "people",
                        Collections.singletonList("id"),
                        null,
                        ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(lookup.settings().getCredentials() instanceof ServiceAccountCredentials)
                .isTrue();
    }

    @Test
    void requiresAllPrimaryKeyColumnsAndAcceptsPlannerKeyOrder() {
        assertThat(provider(config(), new int[][] {{1}, {0}}))
                .isInstanceOf(LookupFunctionProvider.class);
        assertThatThrownBy(() -> provider(config(), new int[][] {{0}}))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("every declared PRIMARY KEY");
        assertThatThrownBy(() -> provider(config(), new int[][] {{0}, {2}}))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("every declared PRIMARY KEY");
    }

    @Test
    void everyLookupKnobIsPartOfTheLookupIdentity() {
        SpannerLookupConfig base = configOf("lookup.partial-cache.max-rows", "10");

        assertThat(configOf("lookup.partial-cache.max-rows", "10"))
                .isEqualTo(base)
                .hasSameHashCodeAs(base);
        assertThat(base).isNotEqualTo("lookup");
        assertThat(configOf("lookup.partial-cache.max-rows", "11")).isNotEqualTo(base);
        assertThat(configOf("lookup.partial-cache.max-rows", "10", "lookup.cache", "partial"))
                .isNotEqualTo(base);
        assertThat(configOf("lookup.partial-cache.max-rows", "10", "lookup.async", "true"))
                .isNotEqualTo(base);
        assertThat(configOf("lookup.partial-cache.max-rows", "10", "lookup.max-retries", "5"))
                .isNotEqualTo(base);
        assertThat(
                        configOf(
                                "lookup.partial-cache.max-rows",
                                "10",
                                "lookup.partial-cache.cache-missing-key",
                                "false"))
                .isNotEqualTo(base);
        assertThat(
                        configOf(
                                "lookup.partial-cache.max-rows",
                                "10",
                                "lookup.partial-cache.expire-after-access",
                                "1 min"))
                .isNotEqualTo(base);
        assertThat(
                        configOf(
                                "lookup.partial-cache.max-rows",
                                "10",
                                "lookup.partial-cache.expire-after-write",
                                "1 min"))
                .isNotEqualTo(base);
    }

    @Test
    void rejectsNestedAndRepeatedLookupKeys() {
        assertThatThrownBy(() -> provider(config(), new int[][] {{0, 1}, {1}}))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("every declared PRIMARY KEY");
        assertThatThrownBy(() -> provider(config(), new int[][] {{0}, {0}}))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("every declared PRIMARY KEY");
    }

    @Test
    void lookupKeysConvertNumericBytesDateAndTimestampKeyParts() throws Exception {
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "amount",
                                                        DataTypes.DECIMAL(38, 9).notNull()),
                                                DataTypes.FIELD(
                                                        "payload", DataTypes.BYTES().notNull()),
                                                DataTypes.FIELD("day", DataTypes.DATE().notNull()),
                                                DataTypes.FIELD(
                                                        "at", DataTypes.TIMESTAMP_LTZ(9).notNull()),
                                                DataTypes.FIELD("name", DataTypes.STRING()))
                                        .getLogicalType(),
                        new int[] {0, 1, 2, 3},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        FakeLookup lookup = new FakeLookup(Struct.newBuilder().set("name").to("Ada").build());
        SpannerRowDataLookupFunction function =
                new SpannerRowDataLookupFunction(
                        schema, new int[] {4}, new int[] {0, 1, 2, 3}, 0, lookup);
        Instant instant = Instant.parse("2026-08-11T01:02:03.123456789Z");
        function.open(null);

        function.lookup(
                GenericRowData.of(
                        DecimalData.fromBigDecimal(new BigDecimal("12.340000000"), 38, 9),
                        new byte[] {1, 2, 3},
                        (int) LocalDate.parse("1969-12-31").toEpochDay(),
                        TimestampData.fromInstant(instant)));

        assertThat(lookup.keys)
                .containsExactly(
                        Key.of(
                                new BigDecimal("12.340000000"),
                                ByteArray.copyFrom(new byte[] {1, 2, 3}),
                                Date.fromYearMonthDay(1969, 12, 31),
                                Timestamp.ofTimeSecondsAndNanos(
                                        instant.getEpochSecond(), 123456789)));
        function.close();
    }

    @Test
    void rejectsFullCacheAndNegativeRetryBudgetsDuringPlanning() {
        assertThatThrownBy(() -> configOf("lookup.cache", "full"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not support FULL");
        assertThatThrownBy(() -> configOf("lookup.max-retries", "-1"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("zero or greater");
    }

    @Test
    void mapsEveryPartialCacheOptionIntoFlinksCache() {
        PartialCachingLookupProvider provider =
                (PartialCachingLookupProvider)
                        provider(
                                config(
                                        "lookup.cache",
                                        "partial",
                                        "lookup.partial-cache.expire-after-access",
                                        "1 min",
                                        "lookup.partial-cache.expire-after-write",
                                        "2 min",
                                        "lookup.partial-cache.cache-missing-key",
                                        "false",
                                        "lookup.partial-cache.max-rows",
                                        "10"),
                                new int[][] {{0}, {1}});

        assertThat(provider.getCache())
                .isEqualTo(
                        DefaultLookupCache.newBuilder()
                                .expireAfterAccess(Duration.ofMinutes(1))
                                .expireAfterWrite(Duration.ofMinutes(2))
                                .cacheMissingKey(false)
                                .maximumSize(10)
                                .build());
    }

    @Test
    void encodesCompositeKeysInDeclaredOrderAndConvertsHitsAndMisses() throws Exception {
        FakeLookup lookup = new FakeLookup(Struct.newBuilder().set("name").to("Ada").build());
        SpannerRowDataLookupFunction function =
                new SpannerRowDataLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {1, 0}, 0, lookup);
        function.open(null);

        Collection<RowData> rows =
                function.lookup(GenericRowData.of(7L, StringData.fromString("eu")));

        assertThat(lookup.keys).containsExactly(Key.of("eu", 7L));
        assertThat(rows).containsExactly(GenericRowData.of(StringData.fromString("Ada")));
        lookup.row = null;
        assertThat(function.lookup(GenericRowData.of(8L, StringData.fromString("us")))).isEmpty();
        assertThat(function.lookup(GenericRowData.of(null, StringData.fromString("us")))).isEmpty();
        function.close();
        assertThat(lookup.closed).isTrue();
    }

    @Test
    void encodesUuidCompositeKeysInDeclaredOrderAndRejectsShortenedText() throws Exception {
        FakeLookup lookup = new FakeLookup(Struct.newBuilder().set("name").to("Ada").build());
        SpannerRowDataLookupFunction function =
                new SpannerRowDataLookupFunction(
                        UUID_SCHEMA, new int[] {2}, new int[] {1, 0}, 0, lookup);
        function.open(null);

        function.lookup(
                GenericRowData.of(
                        7L, StringData.fromString("F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6")));

        assertThat(lookup.keys)
                .containsExactly(
                        Key.of(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"), 7L));
        assertThatThrownBy(
                        () ->
                                function.lookup(
                                        GenericRowData.of(7L, StringData.fromString("1-1-1-1-1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column 'external_id'");
        function.close();
    }

    @Test
    void syncAndAsyncLookupsUseExactPostgresqlDecimalConversion() {
        Struct exact = Struct.newBuilder().set("amount").to(Value.pgNumeric("12.34")).build();
        SpannerRowDataLookupFunction sync =
                new SpannerRowDataLookupFunction(
                        POSTGRESQL_DECIMAL_SCHEMA,
                        new int[] {1},
                        new int[] {0},
                        0,
                        new FakeLookup(exact));
        SpannerRowDataAsyncLookupFunction async =
                new SpannerRowDataAsyncLookupFunction(
                        POSTGRESQL_DECIMAL_SCHEMA,
                        new int[] {1},
                        new int[] {0},
                        0,
                        new FakeLookup(exact));

        assertThat(sync.lookup(GenericRowData.of(1L)))
                .singleElement()
                .satisfies(
                        row ->
                                assertThat(row.getDecimal(0, 5, 2).toBigDecimal())
                                        .isEqualByComparingTo("12.34"));
        assertThat(async.asyncLookup(GenericRowData.of(1L)).join())
                .singleElement()
                .satisfies(
                        row ->
                                assertThat(row.getDecimal(0, 5, 2).toBigDecimal())
                                        .isEqualByComparingTo("12.34"));

        Struct overflow = Struct.newBuilder().set("amount").to(Value.pgNumeric("1000.00")).build();
        SpannerRowDataLookupFunction overflowingSync =
                new SpannerRowDataLookupFunction(
                        POSTGRESQL_DECIMAL_SCHEMA,
                        new int[] {1},
                        new int[] {0},
                        0,
                        new FakeLookup(overflow));
        SpannerRowDataAsyncLookupFunction overflowingAsync =
                new SpannerRowDataAsyncLookupFunction(
                        POSTGRESQL_DECIMAL_SCHEMA,
                        new int[] {1},
                        new int[] {0},
                        0,
                        new FakeLookup(overflow));

        assertThatThrownBy(() -> overflowingSync.lookup(GenericRowData.of(1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column 'amount'")
                .hasMessageContaining("DECIMAL(5, 2)");
        assertThatThrownBy(() -> overflowingAsync.asyncLookup(GenericRowData.of(1L)).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Spanner column 'amount' cannot be represented exactly as Flink DECIMAL(5, 2). The value exceeds the declared precision.");
    }

    @Test
    void retriesOnlyTransientFailuresWithinTheSyncAndAsyncBudgets() throws Exception {
        RuntimeException transientFailure =
                SpannerExceptionFactory.newSpannerException(ErrorCode.UNAVAILABLE, "try again");
        RuntimeException permanentFailure =
                SpannerExceptionFactory.newSpannerException(ErrorCode.PERMISSION_DENIED, "denied");
        Struct hit = Struct.newBuilder().set("name").to("Ada").build();

        ScriptedLookup syncLookup = new ScriptedLookup().answer(transientFailure).answer(hit);
        SpannerRowDataLookupFunction sync =
                new SpannerRowDataLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 1, syncLookup);
        assertThat(sync.lookup(GenericRowData.of(StringData.fromString("eu"), 7L))).hasSize(1);
        assertThat(syncLookup.keys).hasSize(2);

        ScriptedLookup asyncLookup = new ScriptedLookup().answer(transientFailure).answer(hit);
        SpannerRowDataAsyncLookupFunction async =
                new SpannerRowDataAsyncLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 1, asyncLookup);
        assertThat(async.asyncLookup(GenericRowData.of(StringData.fromString("eu"), 7L)).join())
                .hasSize(1);
        assertThat(asyncLookup.keys).hasSize(2);

        ScriptedLookup permanentLookup = new ScriptedLookup().answer(permanentFailure).answer(hit);
        SpannerRowDataLookupFunction permanent =
                new SpannerRowDataLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 3, permanentLookup);
        assertThatThrownBy(
                        () -> permanent.lookup(GenericRowData.of(StringData.fromString("eu"), 7L)))
                .isSameAs(permanentFailure);
        assertThat(permanentLookup.keys).hasSize(1);

        ScriptedLookup permanentAsyncLookup =
                new ScriptedLookup().answer(permanentFailure).answer(hit);
        SpannerRowDataAsyncLookupFunction permanentAsync =
                new SpannerRowDataAsyncLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 3, permanentAsyncLookup);
        assertThatThrownBy(
                        () ->
                                permanentAsync
                                        .asyncLookup(
                                                GenericRowData.of(StringData.fromString("eu"), 7L))
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCause(permanentFailure);
        assertThat(permanentAsyncLookup.keys).hasSize(1);
    }

    @Test
    void asyncCancellationCancelsTheActiveSpannerRead() {
        SettableApiFuture<Struct> pending = SettableApiFuture.create();
        ScriptedLookup lookup = new ScriptedLookup().answer(pending);
        SpannerRowDataAsyncLookupFunction function =
                new SpannerRowDataAsyncLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 3, lookup);

        CompletableFuture<Collection<RowData>> result =
                function.asyncLookup(GenericRowData.of(StringData.fromString("eu"), 7L));
        assertThat(result).isNotDone();
        result.cancel(true);

        assertThat(pending.isCancelled()).isTrue();
        assertThat(lookup.keys).hasSize(1);
    }

    @Test
    void syncAndAsyncLookupsSkipKeysRejectedByAnExactPushedPredicate() {
        CallExpression regionIsEu =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        List.of(
                                new FieldReferenceExpression("region", DataTypes.STRING(), 0, 0),
                                new ValueLiteralExpression("eu")),
                        DataTypes.BOOLEAN());
        SpannerFilterPushDown.RuntimeState filters =
                SpannerFilterPushDown.translate(
                                SCHEMA, Collections.singletonList(regionIsEu), false)
                        .runtime();
        FakeLookup syncLookup = new FakeLookup(null);
        FakeLookup asyncLookup = new FakeLookup(null);
        SpannerRowDataLookupFunction sync =
                new SpannerRowDataLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 0, syncLookup, filters);
        SpannerRowDataAsyncLookupFunction async =
                new SpannerRowDataAsyncLookupFunction(
                        SCHEMA, new int[] {2}, new int[] {0, 1}, 0, asyncLookup, filters);
        GenericRowData rejected = GenericRowData.of(StringData.fromString("us"), 7L);

        assertThat(sync.lookup(rejected)).isEmpty();
        assertThat(async.asyncLookup(rejected).join()).isEmpty();
        assertThat(syncLookup.keys).isEmpty();
        assertThat(asyncLookup.keys).isEmpty();
    }

    @Test
    void scanIndexDoesNotDisablePrimaryKeyLookupGating() {
        CallExpression regionIsEu =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        List.of(
                                new FieldReferenceExpression("region", DataTypes.STRING(), 0, 0),
                                new ValueLiteralExpression("eu")),
                        DataTypes.BOOLEAN());
        SpannerFilterPushDown.RuntimeState filters =
                SpannerFilterPushDown.translate(SCHEMA, Collections.singletonList(regionIsEu), true)
                        .runtime();

        assertThat(filters.matchesPrimaryKey(Key.of("eu", 7L))).isTrue();
        assertThat(filters.matchesPrimaryKey(Key.of("us", 7L))).isFalse();
    }

    private static LookupRuntimeProvider provider(Configuration config, int[][] keys) {
        SpannerDynamicSource source =
                new SpannerDynamicSource(
                        SCHEMA, SpannerDatabase.of("p", "i", "d"), "people", PHYSICAL, config);
        return source.getLookupRuntimeProvider(context(keys));
    }

    private static Configuration config(String... values) {
        Configuration config = new Configuration();
        for (int i = 0; i < values.length; i += 2) {
            config.setString(values[i], values[i + 1]);
        }
        return config;
    }

    private static SpannerLookupConfig configOf(String... values) {
        return SpannerLookupConfig.from(config(values));
    }

    private static LookupContext context(int[][] keys) {
        return (LookupContext)
                Proxy.newProxyInstance(
                        LookupContext.class.getClassLoader(),
                        new Class<?>[] {LookupContext.class},
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("getKeys")) {
                                return keys;
                            }
                            if (method.getName().equals("createTypeInformation")) {
                                return ScanRuntimeProviderContext.INSTANCE.createTypeInformation(
                                        (DataType) arguments[0]);
                            }
                            if (method.getName().equals("preferCustomShuffle")) {
                                return false;
                            }
                            throw new UnsupportedOperationException(method.toString());
                        });
    }

    private static final class FakeLookup implements SpannerRowLookup {
        private static final long serialVersionUID = 1L;
        private final List<Key> keys = new ArrayList<>();
        @Nullable private Struct row;
        private boolean closed;

        private FakeLookup(@Nullable Struct row) {
            this.row = row;
        }

        @Override
        public void open() {}

        @Override
        public Struct read(Key key) {
            keys.add(key);
            return row;
        }

        @Override
        public ApiFuture<Struct> readAsync(Key key) {
            keys.add(key);
            return ApiFutures.immediateFuture(row);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class ScriptedLookup implements SpannerRowLookup {
        private static final long serialVersionUID = 1L;
        private final Deque<Object> answers = new ArrayDeque<>();
        private final List<Key> keys = new ArrayList<>();

        private ScriptedLookup answer(Object answer) {
            answers.add(answer);
            return this;
        }

        @Override
        public void open() {}

        @Override
        public Struct read(Key key) {
            keys.add(key);
            Object answer = answers.removeFirst();
            if (answer instanceof RuntimeException) {
                throw (RuntimeException) answer;
            }
            return (Struct) answer;
        }

        @Override
        public ApiFuture<Struct> readAsync(Key key) {
            keys.add(key);
            Object answer = answers.removeFirst();
            if (answer instanceof ApiFuture) {
                @SuppressWarnings("unchecked")
                ApiFuture<Struct> future = (ApiFuture<Struct>) answer;
                return future;
            }
            if (answer instanceof RuntimeException) {
                return ApiFutures.immediateFailedFuture((RuntimeException) answer);
            }
            return ApiFutures.immediateFuture((Struct) answer);
        }

        @Override
        public void close() {}
    }
}
