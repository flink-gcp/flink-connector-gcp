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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolution;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerTableReadResolverTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("tenant", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                    DataTypes.FIELD("score", DataTypes.BIGINT()),
                    DataTypes.FIELD("name", DataTypes.STRING()));
    private static final SpannerTableSchemaConverter SCHEMA =
            SpannerTableSchemaConverter.of(
                    (RowType) PHYSICAL.getLogicalType(),
                    new int[] {0, 1},
                    Dialect.GOOGLE_STANDARD_SQL,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap());

    @Test
    void resolvesPrimaryKeyMetadataAndAnExactPointRead() throws Exception {
        SpannerFilterPushDown.RuntimeState filters =
                runtime(
                        equals(field(0), new ValueLiteralExpression("eu")),
                        equals(field(1), new ValueLiteralExpression(BigDecimal.valueOf(7))));
        SpannerTableReadResolver resolver =
                resolver(null, Arrays.asList("tenant", "name"), false, filters);

        SpannerReadOperation operation = resolver.resolve(primaryMetadata());

        assertThat(operation.getTable()).isEqualTo("records");
        assertThat(operation.getIndex()).isNull();
        assertThat(operation.getColumns()).containsExactly("tenant", "name");
        assertThat(operation.getKeys()).isEqualTo(KeySet.singleKey(Key.of("eu", 7L)));
    }

    @Test
    void resolvesAReadableSecondaryIndexAndItsCarrierColumn() throws Exception {
        SpannerFilterPushDown.RuntimeState filters =
                runtime(equals(field(2), new ValueLiteralExpression(BigDecimal.valueOf(5))));
        SpannerTableReadResolver resolver =
                resolver("records_by_score", Collections.emptyList(), true, filters);

        SpannerReadOperation operation = resolver.resolve(secondaryMetadata(false));

        assertThat(operation.getIndex()).isEqualTo("records_by_score");
        assertThat(operation.getColumns()).containsExactly("score");
        assertThat(operation.getKeys().getRanges()).isNotEmpty();
    }

    @Test
    void keepsSameNamedAccessPathsInDifferentSchemasDistinct() throws Exception {
        SpannerTableReadResolver analytics = namedResolver("analytics");
        SpannerTableReadResolver archive = namedResolver("archive");

        SpannerReadOperation analyticsRead = analytics.resolve(secondaryMetadata(false));
        SpannerReadOperation archiveRead = archive.resolve(secondaryMetadata(false));

        assertThat(analyticsRead.getTable()).isEqualTo("analytics.records");
        assertThat(analyticsRead.getIndex()).isEqualTo("analytics.records_by_score");
        assertThat(archiveRead.getTable()).isEqualTo("archive.records");
        assertThat(archiveRead.getIndex()).isEqualTo("archive.records_by_score");
    }

    @Test
    void rejectsADeclaredPrimaryKeyThatDoesNotMatchLiveMetadata() {
        SpannerTableReadResolver resolver =
                resolver(null, Collections.singletonList("name"), false, emptyRuntime());
        SpannerTableReadResolver.IndexMetadata reversed =
                SpannerTableReadResolver.IndexMetadata.of(
                        null,
                        false,
                        Arrays.asList(
                                column("id", 1, "ASC", false), column("tenant", 2, "ASC", false)),
                        set("id", "tenant", "name"));

        assertThatThrownBy(() -> resolver.resolve(reversed))
                .hasMessageContaining("does not match the declared PRIMARY KEY")
                .hasMessageContaining("position 1");
    }

    @Test
    void rejectsMissingUnreadyAndUncoveredSecondaryIndexes() {
        SpannerTableReadResolver resolver =
                resolver("records_by_score", Arrays.asList("score", "name"), false, emptyRuntime());

        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        SpannerTableReadResolver.IndexMetadata.of(
                                                null,
                                                false,
                                                Collections.emptyList(),
                                                Collections.emptySet())))
                .hasMessageContaining("was not found");
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        SpannerTableReadResolver.IndexMetadata.of(
                                                "WRITE_ONLY",
                                                false,
                                                Collections.singletonList(
                                                        column("score", 1, "ASC", true)),
                                                set("score"))))
                .hasMessageContaining("not ready for reads")
                .hasMessageContaining("WRITE_ONLY");
        assertThatThrownBy(() -> resolver.resolve(secondaryMetadata(false)))
                .hasMessageContaining("cannot return columns")
                .hasMessageContaining("name");
    }

    @Test
    void distinguishesAMissingSchemaFromAMissingAccessPath() {
        SpannerTableName table =
                SpannerTableName.of("analytics", "records", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTableReadResolver resolver =
                new SpannerTableReadResolver(
                        SCHEMA,
                        table,
                        table.accessPath("records_by_score", "scan.index"),
                        Collections.singletonList("score"),
                        false,
                        Dialect.GOOGLE_STANDARD_SQL,
                        emptyRuntime());

        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        SpannerTableReadResolver.IndexMetadata.missingSchema()))
                .hasMessageContaining("Spanner schema")
                .hasMessageContaining("analytics")
                .hasMessageContaining("was not found");
    }

    @Test
    void parsesAnExistingSchemaWithAMissingAccessPathFromTheLeftJoinRow() {
        SpannerTableReadResolver.IndexMetadata metadata =
                SpannerTableReadResolver.IndexMetadata.read(
                        resultSet(new Object[] {"analytics", null, null, null, null, null, null}),
                        Dialect.GOOGLE_STANDARD_SQL);

        assertThatThrownBy(() -> namedResolver("analytics").resolve(metadata))
                .hasMessageContaining("access path")
                .hasMessageContaining("was not found")
                .hasMessageNotContaining("Spanner schema");
    }

    @Test
    void nullFilteredIndexesRequireFiltersThatExcludeNullKeyRows() throws Exception {
        SpannerTableReadResolver unsafe =
                resolver(
                        "records_by_score",
                        Collections.singletonList("score"),
                        false,
                        emptyRuntime());
        SpannerTableReadResolver safe =
                resolver(
                        "records_by_score",
                        Collections.singletonList("score"),
                        false,
                        runtime(equals(field(2), new ValueLiteralExpression(BigDecimal.ONE))));

        assertThatThrownBy(() -> unsafe.resolve(secondaryMetadata(true)))
                .hasMessageContaining("omits rows")
                .hasMessageContaining("score");
        assertThat(safe.resolve(secondaryMetadata(true)).getIndex()).isEqualTo("records_by_score");
    }

    @Test
    void metadataQueriesBindTheRequestedSchemaTableAndIndex() {
        Statement google =
                SpannerTableReadResolver.metadataQuery(
                        Dialect.GOOGLE_STANDARD_SQL, "analytics", "records", "PRIMARY_KEY");
        Statement defaultGoogle =
                SpannerTableReadResolver.metadataQuery(
                        Dialect.GOOGLE_STANDARD_SQL, "", "records", "PRIMARY_KEY");
        Statement postgres =
                SpannerTableReadResolver.metadataQuery(
                        Dialect.POSTGRESQL, "analytics", "records", "records_by_score");
        Statement defaultPostgres =
                SpannerTableReadResolver.metadataQuery(
                        Dialect.POSTGRESQL, "public", "records", "PRIMARY_KEY");

        assertThat(google.getSql())
                .contains("INFORMATION_SCHEMA.SCHEMATA")
                .contains("INFORMATION_SCHEMA.INDEXES")
                .contains("LOWER(s.SCHEMA_NAME) = LOWER(@schema_name)")
                .contains("@schema_name")
                .contains("@table_name")
                .contains("@index_name");
        assertThat(google.getParameters().get("schema_name").getString()).isEqualTo("analytics");
        assertThat(google.getParameters().get("table_name").getString()).isEqualTo("records");
        assertThat(google.getParameters().get("index_name").getString()).isEqualTo("PRIMARY_KEY");
        assertThat(postgres.getSql())
                .contains("information_schema.schemata")
                .contains("information_schema.indexes")
                .contains("WHERE s.schema_name = $1")
                .contains("$1")
                .contains("$2")
                .contains("$3");
        assertThat(postgres.getParameters().get("p1").getString()).isEqualTo("analytics");
        assertThat(postgres.getParameters().get("p2").getString()).isEqualTo("records");
        assertThat(postgres.getParameters().get("p3").getString()).isEqualTo("records_by_score");
        assertThat(defaultGoogle.getParameters().get("schema_name").getString()).isEmpty();
        assertThat(defaultGoogle.getParameters().get("table_name").getString())
                .isEqualTo("records");
        assertThat(defaultGoogle.getParameters().get("index_name").getString())
                .isEqualTo("PRIMARY_KEY");
        assertThat(defaultPostgres.getParameters().get("p1").getString()).isEqualTo("public");
        assertThat(defaultPostgres.getParameters().get("p2").getString()).isEqualTo("records");
        assertThat(defaultPostgres.getParameters().get("p3").getString()).isEqualTo("PRIMARY_KEY");
    }

    @Test
    void postgresqlMetadataParsingRecognizesNullFilteringAndIncludedColumns() throws Exception {
        SpannerTableReadResolver.IndexMetadata metadata =
                SpannerTableReadResolver.IndexMetadata.read(
                        resultSet(
                                new Object[] {
                                    "public", "READ_WRITE", "YES", "score", 1L, "ASC", "YES"
                                },
                                new Object[] {
                                    "public", "READ_WRITE", "YES", "name", null, null, "YES"
                                }),
                        Dialect.POSTGRESQL);
        SpannerTableReadResolver unsafe =
                resolver(
                        "records_by_score",
                        Arrays.asList("score", "name", "id"),
                        false,
                        emptyRuntime());
        SpannerTableReadResolver safe =
                resolver(
                        "records_by_score",
                        Arrays.asList("score", "name", "id"),
                        false,
                        runtime(equals(field(2), new ValueLiteralExpression(BigDecimal.ONE))));

        assertThatThrownBy(() -> unsafe.resolve(metadata)).hasMessageContaining("omits rows");
        assertThat(safe.resolve(metadata).getColumns()).containsExactly("score", "name", "id");
    }

    @Test
    void deferredReadOperationIsSerializable() throws Exception {
        SpannerTableReadResolver resolver =
                resolver(
                        "records_by_score",
                        Collections.singletonList("score"),
                        false,
                        runtime(equals(field(2), new ValueLiteralExpression(BigDecimal.ONE))));
        SpannerReadOperation deferred = SpannerReadOperationResolution.deferred(resolver);

        SpannerReadOperation copy = InstantiationUtil.clone(deferred);

        assertThat(copy).isEqualTo(deferred);
        assertThat(copy.toString()).contains("deferred read").contains("records_by_score");
    }

    private static SpannerTableReadResolver resolver(
            String index,
            List<String> columns,
            boolean zeroColumnProjection,
            SpannerFilterPushDown.RuntimeState filters) {
        return new SpannerTableReadResolver(
                SCHEMA,
                SpannerTableName.of(null, "records", Dialect.GOOGLE_STANDARD_SQL),
                index == null
                        ? null
                        : SpannerTableName.of(null, "records", Dialect.GOOGLE_STANDARD_SQL)
                                .accessPath(index, "scan.index"),
                columns,
                zeroColumnProjection,
                Dialect.GOOGLE_STANDARD_SQL,
                filters);
    }

    private static SpannerTableReadResolver namedResolver(String schema) {
        SpannerTableName table =
                SpannerTableName.of(schema, "records", Dialect.GOOGLE_STANDARD_SQL);
        return new SpannerTableReadResolver(
                SCHEMA,
                table,
                table.accessPath("records_by_score", "scan.index"),
                Collections.singletonList("score"),
                false,
                Dialect.GOOGLE_STANDARD_SQL,
                emptyRuntime());
    }

    private static SpannerTableReadResolver.IndexMetadata primaryMetadata() {
        return SpannerTableReadResolver.IndexMetadata.of(
                null,
                false,
                Arrays.asList(column("tenant", 1, "ASC", false), column("id", 2, "ASC", false)),
                set("tenant", "id", "score", "name"));
    }

    private static SpannerTableReadResolver.IndexMetadata secondaryMetadata(boolean nullFiltered) {
        return SpannerTableReadResolver.IndexMetadata.of(
                "READ_WRITE",
                nullFiltered,
                Arrays.asList(
                        column("score", 1, "ASC", true),
                        column("tenant", 2, "ASC", false),
                        column("id", 3, "ASC", false)),
                set("score", "tenant", "id"));
    }

    private static SpannerTableReadResolver.IndexColumn column(
            String name, long ordinal, String ordering, boolean nullable) {
        return new SpannerTableReadResolver.IndexColumn(name, ordinal, ordering, nullable);
    }

    private static LinkedHashSet<String> set(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static ResultSet resultSet(Object[]... rows) {
        AtomicInteger position = new AtomicInteger(-1);
        return (ResultSet)
                Proxy.newProxyInstance(
                        ResultSet.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, arguments) -> {
                            String name = method.getName();
                            if ("next".equals(name)) {
                                return position.incrementAndGet() < rows.length;
                            }
                            if ("close".equals(name)) {
                                return null;
                            }
                            int column = (Integer) arguments[0];
                            Object value = rows[position.get()][column];
                            switch (name) {
                                case "isNull":
                                    return value == null;
                                case "getString":
                                    return value;
                                case "getLong":
                                    return value;
                                case "getBoolean":
                                    return value;
                                default:
                                    throw new AssertionError(
                                            "Unexpected ResultSet method " + method);
                            }
                        });
    }

    private static SpannerFilterPushDown.RuntimeState emptyRuntime() {
        return SpannerFilterPushDown.State.empty().runtime();
    }

    private static SpannerFilterPushDown.RuntimeState runtime(ResolvedExpression... filters) {
        return SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(filters), true).runtime();
    }

    private static FieldReferenceExpression field(int index) {
        return new FieldReferenceExpression(
                ((RowType) PHYSICAL.getLogicalType()).getFieldNames().get(index),
                PHYSICAL.getChildren().get(index),
                index,
                index);
    }

    private static CallExpression equals(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS, Arrays.asList(left, right), DataTypes.BOOLEAN());
    }
}
