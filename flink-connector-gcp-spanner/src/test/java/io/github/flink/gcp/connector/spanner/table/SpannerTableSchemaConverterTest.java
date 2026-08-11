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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerTableSchemaConverterTest {

    @Test
    void mapsTheNativeScalarAndCompositeTypes() {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 9)),
                                        DataTypes.FIELD("at", DataTypes.TIMESTAMP_LTZ(9)),
                                        DataTypes.FIELD(
                                                "labels", DataTypes.ARRAY(DataTypes.STRING())))
                                .getLogicalType();

        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        row,
                        new int[] {1},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        assertThat(schema.getColumns())
                .extracting(SpannerTableSchemaConverter.Column::getSpannerType)
                .containsExactly(
                        Type.bool(),
                        Type.int64(),
                        Type.numeric(),
                        Type.timestamp(),
                        Type.array(Type.string()));
        assertThat(schema.getPrimaryKeyIndexes()).containsExactly(1);
    }

    @Test
    void specialMarkersPreserveProtoEnumAndJsonIdentity() {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                                        DataTypes.FIELD("state", DataTypes.BIGINT()),
                                        DataTypes.FIELD("document", DataTypes.STRING()))
                                .getLogicalType();
        Map<String, String> proto = new HashMap<>();
        proto.put("payload", "example.Payload");
        Map<String, String> enumTypes = new HashMap<>();
        enumTypes.put("state", "example.State");

        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        row,
                        new int[0],
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.singletonList("document"),
                        proto,
                        enumTypes);

        assertThat(schema.getColumns())
                .extracting(SpannerTableSchemaConverter.Column::getSpannerType)
                .containsExactly(
                        Type.proto("example.Payload"),
                        Type.protoEnum("example.State"),
                        Type.json());
    }

    @Test
    void aPostgresqlJsonMarkerUsesJsonb() {
        RowType row =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("document", DataTypes.STRING()))
                                .getLogicalType();

        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        row,
                        new int[0],
                        Dialect.POSTGRESQL,
                        Collections.singletonList("document"),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        assertThat(schema.getColumns().get(0).getSpannerType()).isEqualTo(Type.pgJsonb());
    }

    @Test
    void postgresqlRejectsGoogleSqlNamedTypes() {
        RowType row =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.BYTES()))
                                .getLogicalType();

        assertThatThrownBy(
                        () ->
                                SpannerTableSchemaConverter.of(
                                        row,
                                        new int[0],
                                        Dialect.POSTGRESQL,
                                        Collections.emptyList(),
                                        Collections.singletonMap("payload", "example.Payload"),
                                        Collections.emptyMap()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("require the GOOGLE_STANDARD_SQL dialect");
    }

    @Test
    void rejectsAnUnknownMarkerPath() {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();

        assertThatThrownBy(
                        () ->
                                SpannerTableSchemaConverter.of(
                                        row,
                                        new int[0],
                                        Dialect.GOOGLE_STANDARD_SQL,
                                        Collections.singletonList("missing"),
                                        Collections.emptyMap(),
                                        Collections.emptyMap()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("unknown field paths")
                .hasMessageContaining("missing");
    }

    @Test
    void rejectsConflictingSpecialMarkersOnOneField() {
        RowType row =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.BYTES()))
                                .getLogicalType();
        Map<String, String> proto = Collections.singletonMap("payload", "example.Payload");

        assertThatThrownBy(
                        () ->
                                SpannerTableSchemaConverter.of(
                                        row,
                                        new int[0],
                                        Dialect.GOOGLE_STANDARD_SQL,
                                        Collections.singletonList("payload"),
                                        proto,
                                        Collections.emptyMap()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("more than one special Spanner type marker");
    }

    @Test
    void rejectsTypesThatWouldLoseInformation() {
        for (org.apache.flink.table.types.DataType unsupported :
                Arrays.asList(
                        DataTypes.INT(),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING())),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))) {
            RowType row =
                    (RowType) DataTypes.ROW(DataTypes.FIELD("value", unsupported)).getLogicalType();
            assertThatThrownBy(
                            () ->
                                    SpannerTableSchemaConverter.of(
                                            row,
                                            new int[0],
                                            Dialect.GOOGLE_STANDARD_SQL,
                                            Collections.emptyList(),
                                            Collections.emptyMap(),
                                            Collections.emptyMap()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("value")
                    .hasMessageContaining("unsupported");
        }
    }
}
