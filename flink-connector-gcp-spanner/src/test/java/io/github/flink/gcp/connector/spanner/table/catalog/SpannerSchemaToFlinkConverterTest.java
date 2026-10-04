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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.ColumnMetadata;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.NamedTypeKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.github.flink.gcp.connector.spanner.table.catalog.FakeSpannerCatalogClient.column;
import static io.github.flink.gcp.connector.spanner.table.catalog.FakeSpannerCatalogClient.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link SpannerSchemaToFlinkConverter}: the inverse of the connector page's type mapping. */
class SpannerSchemaToFlinkConverterTest {

    private static final Supplier<Map<String, NamedTypeKind>> NO_NAMED_TYPES =
            () -> {
                throw new AssertionError("proto types asked for a table that has none");
            };

    @Test
    void mapsEveryGoogleSqlTypeToTheFlinkTypeAHandWrittenTableDeclares() {
        Map<String, NamedTypeKind> kinds = new HashMap<>();
        kinds.put("example.events.Event", NamedTypeKind.PROTO);
        kinds.put("example.events.Status", NamedTypeKind.ENUM);

        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                key("id", "INT64", 1),
                                column("b", "BOOL"),
                                column("f32", "FLOAT32"),
                                column("f64", "FLOAT64"),
                                column("n", "NUMERIC"),
                                column("s", "STRING(10)"),
                                column("bytes", "BYTES(MAX)"),
                                column("d", "DATE"),
                                column("ts", "TIMESTAMP"),
                                column("j", "JSON"),
                                column("u", "UUID"),
                                column("ev", "`example.events.Event`"),
                                column("st", "`example.events.Status`"),
                                column("tags", "ARRAY<STRING(MAX)>"),
                                column("ids", "ARRAY<UUID>")),
                        Dialect.GOOGLE_STANDARD_SQL,
                        () -> kinds);

        assertThat(types(converted.schema()))
                .containsExactly(
                        entry("id", DataTypes.BIGINT().notNull()),
                        entry("b", DataTypes.BOOLEAN()),
                        entry("f32", DataTypes.FLOAT()),
                        entry("f64", DataTypes.DOUBLE()),
                        entry("n", DataTypes.DECIMAL(38, 9)),
                        entry("s", DataTypes.STRING()),
                        entry("bytes", DataTypes.BYTES()),
                        entry("d", DataTypes.DATE()),
                        entry("ts", DataTypes.TIMESTAMP_LTZ(9)),
                        entry("j", DataTypes.STRING()),
                        entry("u", DataTypes.STRING()),
                        entry("ev", DataTypes.BYTES()),
                        entry("st", DataTypes.BIGINT()),
                        entry("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        entry("ids", DataTypes.ARRAY(DataTypes.STRING())));
        assertThat(converted.markerOptions())
                .containsOnly(
                        Map.entry("json-field-paths", "j"),
                        Map.entry("uuid-field-paths", "u;ids"),
                        Map.entry("proto-type-names", "ev:example.events.Event"),
                        Map.entry("enum-type-names", "st:example.events.Status"));
    }

    @Test
    void mapsEveryPostgreSqlTypeAndMarksJsonbAndUuid() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                key("id", "bigint", 1),
                                column("n", "numeric"),
                                column("s", "character varying"),
                                column("ct", "spanner.commit_timestamp"),
                                column("j", "jsonb"),
                                column("u", "uuid"),
                                column("v", "real[] vector length 3")),
                        Dialect.POSTGRESQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema()))
                .containsExactly(
                        entry("id", DataTypes.BIGINT().notNull()),
                        entry("n", DataTypes.DECIMAL(38, 9)),
                        entry("s", DataTypes.STRING()),
                        entry("ct", DataTypes.TIMESTAMP_LTZ(9)),
                        entry("j", DataTypes.STRING()),
                        entry("u", DataTypes.STRING()),
                        entry("v", DataTypes.ARRAY(DataTypes.FLOAT())));
        assertThat(converted.markerOptions())
                .containsOnly(
                        Map.entry("json-field-paths", "j"), Map.entry("uuid-field-paths", "u"));
    }

    @Test
    void theKeyFollowsSpannerKeyOrderAndForcesNotNullOverANullableKeyColumn() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                new ColumnMetadata(
                                        "b", "STRING(MAX)", true, false, false, false, 2L),
                                column("v", "INT64"),
                                new ColumnMetadata("a", "INT64", false, false, false, false, 1L)),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(converted.schema().getPrimaryKey())
                .hasValueSatisfying(
                        key -> assertThat(key.getColumnNames()).containsExactly("a", "b"));
        assertThat(types(converted.schema()))
                .containsExactly(
                        entry("b", DataTypes.STRING().notNull()),
                        entry("v", DataTypes.BIGINT()),
                        entry("a", DataTypes.BIGINT().notNull()));
    }

    @Test
    void aGeneratedColumnIsKeptMarkedAndNullableUnlessItIsAKeyColumn() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                new ColumnMetadata("a", "INT64", false, false, false, false, null),
                                new ColumnMetadata("k", "INT64", false, true, true, false, 1L),
                                new ColumnMetadata(
                                        "total", "INT64", false, true, true, false, null)),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema()))
                .containsExactly(
                        entry("a", DataTypes.BIGINT().notNull()),
                        entry("k", DataTypes.BIGINT().notNull()),
                        entry("total", DataTypes.BIGINT()));
        assertThat(converted.markerOptions())
                .containsOnly(Map.entry("generated-columns", "k;total"));
    }

    @Test
    void aProtoOrEnumTheServiceNamesWithItsKindNeedsNoProtoBundle() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                key("Id", "INT64", 1),
                                column("Ev", "PROTO<example.events.Event>"),
                                column("Sts", "ARRAY<ENUM<example.events.Status>>")),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema()))
                .containsExactly(
                        entry("Id", DataTypes.BIGINT().notNull()),
                        entry("Ev", DataTypes.BYTES()),
                        entry("Sts", DataTypes.ARRAY(DataTypes.BIGINT())));
        assertThat(converted.markerOptions())
                .containsOnly(
                        Map.entry("proto-type-names", "Ev:example.events.Event"),
                        Map.entry("enum-type-names", "Sts:example.events.Status"));
    }

    /**
     * Spanner's read API refuses a generated column that is not stored (measured on the service;
     * the emulator returns it), so the catalog leaves one out rather than resolve a table no scan
     * reads.
     */
    @Test
    void aGeneratedColumnThatIsNotStoredIsLeftOut() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                key("id", "INT64", 1),
                                new ColumnMetadata(
                                        "stored", "INT64", true, true, true, false, null),
                                new ColumnMetadata(
                                        "virtual", "INT64", true, true, false, false, null)),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema())).containsOnlyKeys("id", "stored");
        assertThat(converted.markerOptions())
                .containsOnly(Map.entry("generated-columns", "stored"));
    }

    @Test
    void aHiddenColumnIsLeftOutAsSpannersOwnSelectStarLeavesItOut() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                key("id", "INT64", 1),
                                new ColumnMetadata(
                                        "tok", "TOKENLIST", true, true, true, true, null),
                                new ColumnMetadata(
                                        "h", "STRING(MAX)", true, false, false, true, null)),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema())).containsOnlyKeys("id");
        assertThat(converted.markerOptions()).isEmpty();
    }

    /**
     * A key column is kept even when hidden, since the primary key cannot name a missing column.
     */
    @Test
    void aHiddenKeyColumnIsKeptBecauseTheKeyNeedsIt() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(
                                new ColumnMetadata("id", "INT64", false, false, false, true, 1L),
                                column("v", "INT64")),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(types(converted.schema())).containsOnlyKeys("id", "v");
        assertThat(converted.schema().getPrimaryKey())
                .hasValueSatisfying(key -> assertThat(key.getColumnNames()).containsExactly("id"));
    }

    @Test
    void anUnsupportedTypeFailsNamingTheColumnAndTheSpannerSpelling() {
        assertThatThrownBy(
                        () ->
                                SpannerSchemaToFlinkConverter.convert(
                                        Arrays.asList(
                                                key("id", "INT64", 1), column("iv", "INTERVAL")),
                                        Dialect.GOOGLE_STANDARD_SQL,
                                        NO_NAMED_TYPES))
                .isInstanceOf(SpannerSchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessage(
                        "Column 'iv' has Spanner type INTERVAL, which has no Flink type the"
                                + " connector reads or writes: the connector's type mapping has no"
                                + " row for it.");
        assertThatThrownBy(
                        () ->
                                SpannerSchemaToFlinkConverter.convert(
                                        Collections.singletonList(
                                                column("ev", "`example.Missing`")),
                                        Dialect.GOOGLE_STANDARD_SQL,
                                        HashMap::new))
                .hasMessageContaining("'ev'")
                .hasMessageContaining("proto bundle declares no message or enum of that name");
    }

    @Test
    void aColumnNameFlinksOptionSyntaxSplitsOnIsQuotedInItsMarker() {
        SpannerSchemaToFlinkConverter.Converted converted =
                SpannerSchemaToFlinkConverter.convert(
                        Arrays.asList(column("col;semi", "JSON"), column("Col.Dot", "JSON")),
                        Dialect.GOOGLE_STANDARD_SQL,
                        NO_NAMED_TYPES);

        assertThat(converted.markerOptions())
                .containsOnly(Map.entry("json-field-paths", "'col;semi';Col.Dot"));
    }

    private static Map<String, Object> types(Schema schema) {
        Map<String, Object> types = new LinkedHashMap<>();
        List<UnresolvedColumn> columns = schema.getColumns();
        for (UnresolvedColumn column : columns) {
            types.put(column.getName(), ((UnresolvedPhysicalColumn) column).getDataType());
        }
        return types;
    }

    private static Map.Entry<String, Object> entry(String name, Object type) {
        return Map.entry(name, type);
    }
}
