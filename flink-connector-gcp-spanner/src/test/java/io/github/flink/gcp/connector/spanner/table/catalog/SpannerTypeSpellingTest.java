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

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerTypeSpelling.Kind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SpannerTypeSpelling}, fed the {@code SPANNER_TYPE} spellings the Spanner emulator reported
 * for a table declaring every type in each dialect (measured 2026-10-03 on emulator 1.5.57).
 */
class SpannerTypeSpellingTest {

    @Test
    void parsesEveryGoogleSqlSpellingTheMappingCovers() {
        assertGoogleSql("BOOL", Kind.BOOL, false);
        assertGoogleSql("INT64", Kind.INT64, false);
        assertGoogleSql("FLOAT32", Kind.FLOAT32, false);
        assertGoogleSql("FLOAT64", Kind.FLOAT64, false);
        assertGoogleSql("NUMERIC", Kind.NUMERIC, false);
        assertGoogleSql("STRING(MAX)", Kind.STRING, false);
        assertGoogleSql("STRING(10)", Kind.STRING, false);
        assertGoogleSql("BYTES(MAX)", Kind.BYTES, false);
        assertGoogleSql("BYTES(10)", Kind.BYTES, false);
        assertGoogleSql("DATE", Kind.DATE, false);
        assertGoogleSql("TIMESTAMP", Kind.TIMESTAMP, false);
        assertGoogleSql("JSON", Kind.JSON, false);
        assertGoogleSql("UUID", Kind.UUID, false);
        assertGoogleSql("ARRAY<INT64>", Kind.INT64, true);
        assertGoogleSql("ARRAY<STRING(10)>", Kind.STRING, true);
        assertGoogleSql("ARRAY<FLOAT32>", Kind.FLOAT32, true);
        assertGoogleSql("ARRAY<UUID>", Kind.UUID, true);
        assertGoogleSql("ARRAY<JSON>", Kind.JSON, true);
        assertGoogleSql("ARRAY<FLOAT32>(vector_length=>3)", Kind.FLOAT32, true);
    }

    /** Real Spanner's spelling, measured 2026-10-03 by SpannerCatalogRealGcpITCase. */
    @Test
    void theServiceSpellsAProtoOrEnumWithItsKind() {
        SpannerTypeSpelling proto =
                SpannerTypeSpelling.parse(
                        "PROTO<example.events.Event>", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTypeSpelling nested =
                SpannerTypeSpelling.parse(
                        "ENUM<example.events.Event.Kind>", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTypeSpelling array =
                SpannerTypeSpelling.parse(
                        "ARRAY<ENUM<example.events.Status>>", Dialect.GOOGLE_STANDARD_SQL);

        assertThat(proto.kind()).isEqualTo(Kind.PROTO);
        assertThat(proto.typeName()).isEqualTo("example.events.Event");
        assertThat(nested.kind()).isEqualTo(Kind.ENUM);
        assertThat(nested.typeName()).isEqualTo("example.events.Event.Kind");
        assertThat(array.kind()).isEqualTo(Kind.ENUM);
        assertThat(array.typeName()).isEqualTo("example.events.Status");
        assertThat(array.isArray()).isTrue();
    }

    /** The emulator's spelling, measured on 1.5.57. */
    @Test
    void aGoogleSqlProtoOrEnumIsANameInBackticksWithNothingSayingWhich() {
        SpannerTypeSpelling scalar =
                SpannerTypeSpelling.parse("`example.events.Status`", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTypeSpelling array =
                SpannerTypeSpelling.parse(
                        "ARRAY<`example.events.Event`>", Dialect.GOOGLE_STANDARD_SQL);

        assertThat(scalar.kind()).isEqualTo(Kind.NAMED);
        assertThat(scalar.typeName()).isEqualTo("example.events.Status");
        assertThat(scalar.isArray()).isFalse();
        assertThat(array.kind()).isEqualTo(Kind.NAMED);
        assertThat(array.typeName()).isEqualTo("example.events.Event");
        assertThat(array.isArray()).isTrue();
    }

    @Test
    void parsesEveryPostgreSqlSpellingTheMappingCovers() {
        assertPostgreSql("boolean", Kind.BOOL, false);
        assertPostgreSql("bigint", Kind.INT64, false);
        assertPostgreSql("real", Kind.FLOAT32, false);
        assertPostgreSql("double precision", Kind.FLOAT64, false);
        assertPostgreSql("numeric", Kind.NUMERIC, false);
        assertPostgreSql("character varying", Kind.STRING, false);
        assertPostgreSql("character varying(10)", Kind.STRING, false);
        assertPostgreSql("bytea", Kind.BYTES, false);
        assertPostgreSql("date", Kind.DATE, false);
        assertPostgreSql("timestamp with time zone", Kind.TIMESTAMP, false);
        assertPostgreSql("spanner.commit_timestamp", Kind.TIMESTAMP, false);
        assertPostgreSql("jsonb", Kind.JSON, false);
        assertPostgreSql("uuid", Kind.UUID, false);
        assertPostgreSql("bigint[]", Kind.INT64, true);
        assertPostgreSql("character varying(10)[]", Kind.STRING, true);
        assertPostgreSql("real[]", Kind.FLOAT32, true);
        assertPostgreSql("uuid[]", Kind.UUID, true);
        assertPostgreSql("jsonb[]", Kind.JSON, true);
        // The emulator reports a PostgreSQL vector column in the GoogleSQL spelling; the
        // documented PostgreSQL DDL spelling is accepted too.
        assertPostgreSql("ARRAY<FLOAT32>(vector_length=>3)", Kind.FLOAT32, true);
        assertPostgreSql("real[] vector length 3", Kind.FLOAT32, true);
    }

    @Test
    void typesTheMappingDoesNotCoverAreUnsupportedRatherThanErrors() {
        for (String type :
                new String[] {
                    "INTERVAL", "TOKENLIST", "STRUCT<a INT64>", "ARRAY<STRUCT<a INT64>>"
                }) {
            assertThat(SpannerTypeSpelling.parse(type, Dialect.GOOGLE_STANDARD_SQL).kind())
                    .as(type)
                    .isEqualTo(Kind.UNSUPPORTED);
        }
        for (String type : new String[] {"interval", "oid", "spanner.tokenlist", "interval[]"}) {
            assertThat(SpannerTypeSpelling.parse(type, Dialect.POSTGRESQL).kind())
                    .as(type)
                    .isEqualTo(Kind.UNSUPPORTED);
        }
    }

    private static void assertGoogleSql(String spelling, Kind kind, boolean array) {
        SpannerTypeSpelling type = SpannerTypeSpelling.parse(spelling, Dialect.GOOGLE_STANDARD_SQL);
        assertThat(type.kind()).as(spelling).isEqualTo(kind);
        assertThat(type.isArray()).as(spelling).isEqualTo(array);
    }

    private static void assertPostgreSql(String spelling, Kind kind, boolean array) {
        SpannerTypeSpelling type = SpannerTypeSpelling.parse(spelling, Dialect.POSTGRESQL);
        assertThat(type.kind()).as(spelling).isEqualTo(kind);
        assertThat(type.isArray()).as(spelling).isEqualTo(array);
    }
}
