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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins the plan identity of the Spanner table source. */
class SpannerDynamicSourceTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("region", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("account", DataTypes.BIGINT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()));
    private static final SpannerTableSchemaConverter SCHEMA = schema(new int[] {0, 1});

    @Test
    void sourcesBuiltFromTheSameOptionsAreEqualAndDifferingOnesAreNot() {
        SpannerDynamicSource source = source();

        assertThat(source()).isEqualTo(source).hasSameHashCodeAs(source);
        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);
        assertThat(source).isNotEqualTo(SCHEMA);

        assertThat(source(config("schema", "analytics"))).isNotEqualTo(source);
        assertThat(source(config("dialect", "POSTGRESQL"))).isNotEqualTo(source);
        assertThat(source(config("scan.index", "by_name"))).isNotEqualTo(source);
        assertThat(source(config("scan.partition.max-partitions", "10"))).isNotEqualTo(source);
        assertThat(source(config("scan.partition.size", "1 mb"))).isNotEqualTo(source);
        assertThat(source(config("scan.data-boost-enabled", "true"))).isNotEqualTo(source);
        assertThat(source(config("scan.rpc-priority", "HIGH"))).isNotEqualTo(source);
        assertThat(source(config("scan.timestamp-bound.exact-staleness", "10s")))
                .isNotEqualTo(source);
        assertThat(source(config("emulator-endpoint", "localhost:9010"))).isNotEqualTo(source);
        assertThat(source(config("service-account-key-file", "/tmp/key.json")))
                .isNotEqualTo(source);
        assertThat(source(config("scan.parallelism", "4"))).isNotEqualTo(source);
        assertThat(source(config("lookup.async", "true"))).isNotEqualTo(source);
    }

    @Test
    void theSchemaTableDatabaseAndProducedTypeArePartOfTheIdentity() {
        SpannerDynamicSource source = source();

        assertThat(
                        new SpannerDynamicSource(
                                schema(new int[] {0}),
                                SpannerDatabase.of("p", "i", "d"),
                                "people",
                                PHYSICAL,
                                config()))
                .isNotEqualTo(source);
        assertThat(
                        new SpannerDynamicSource(
                                SCHEMA,
                                SpannerDatabase.of("p", "i", "other"),
                                "people",
                                PHYSICAL,
                                config()))
                .isNotEqualTo(source);
        assertThat(
                        new SpannerDynamicSource(
                                SCHEMA,
                                SpannerDatabase.of("p", "i", "d"),
                                "orders",
                                PHYSICAL,
                                config()))
                .isNotEqualTo(source);
        assertThat(
                        new SpannerDynamicSource(
                                SCHEMA,
                                SpannerDatabase.of("p", "i", "d"),
                                "people",
                                DataTypes.ROW(
                                        DataTypes.FIELD("region", DataTypes.STRING().notNull())),
                                config()))
                .isNotEqualTo(source);
    }

    @Test
    void anAppliedProjectionAndAnAppliedFilterArePartOfTheIdentity() {
        SpannerDynamicSource projected = sameProducedTypeFrom(new int[][] {{0}});
        SpannerDynamicSource filtered = source();
        filtered.applyFilters(Collections.singletonList(regionIs("eu")));
        SpannerDynamicSource otherFilter = source();
        otherFilter.applyFilters(Collections.singletonList(regionIs("us")));

        assertThat(projected).isNotEqualTo(source());
        assertThat(filtered).isNotEqualTo(source());
        assertThat(filtered).isNotEqualTo(otherFilter);
        assertThat(projected).isNotEqualTo(sameProducedTypeFrom(new int[][] {{2}}));
        assertThat(filtered.copy()).isEqualTo(filtered).hasSameHashCodeAs(filtered);
        assertThat(projected.copy()).isEqualTo(projected).hasSameHashCodeAs(projected);
    }

    /**
     * Applies one physical projection while keeping the produced type fixed, so that two sources
     * differ in nothing but the physical column each produced field reads.
     */
    private static SpannerDynamicSource sameProducedTypeFrom(int[][] projection) {
        SpannerDynamicSource source = source();
        source.applyProjection(
                projection, DataTypes.ROW(DataTypes.FIELD("projected", DataTypes.STRING())));
        return source;
    }

    private static ResolvedExpression regionIs(String value) {
        List<DataType> types = PHYSICAL.getChildren();
        FieldReferenceExpression region =
                new FieldReferenceExpression("region", types.get(0), 0, 0);
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                Arrays.asList(region, new ValueLiteralExpression(value)),
                DataTypes.BOOLEAN());
    }

    private static SpannerDynamicSource source() {
        return source(config());
    }

    private static SpannerDynamicSource source(Configuration config) {
        return new SpannerDynamicSource(
                SCHEMA, SpannerDatabase.of("p", "i", "d"), "people", PHYSICAL, config);
    }

    private static SpannerTableSchemaConverter schema(int[] primaryKey) {
        return SpannerTableSchemaConverter.of(
                (RowType) PHYSICAL.getLogicalType(),
                primaryKey,
                Dialect.GOOGLE_STANDARD_SQL,
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static Configuration config(String... values) {
        Configuration config = new Configuration();
        for (int i = 0; i < values.length; i += 2) {
            config.setString(values[i], values[i + 1]);
        }
        return config;
    }
}
