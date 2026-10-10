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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreTableSchemaTest {

    private static final DataType GEO =
            DataTypes.ROW(
                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                    DataTypes.FIELD("longitude", DataTypes.DOUBLE()));

    private static RowType row(DataTypes.Field... fields) {
        return (RowType) DataTypes.ROW(fields).getLogicalType();
    }

    private static FirestoreTableSchema schema(
            RowType rowType, int[] key, List<String> geoPoints, List<String> references) {
        return FirestoreTableSchema.of(rowType, key, geoPoints, references);
    }

    @Test
    void everyMappedTypeIsAcceptedAtEveryDepth() {
        RowType type =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("s", DataTypes.VARCHAR(10)),
                        DataTypes.FIELD("c", DataTypes.CHAR(2)),
                        DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD("d", DataTypes.DOUBLE()),
                        DataTypes.FIELD("bytes", DataTypes.BYTES()),
                        DataTypes.FIELD("bin", DataTypes.BINARY(4)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(9)),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                "matrix", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()))),
                        DataTypes.FIELD(
                                "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        DataTypes.FIELD(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "items",
                                                DataTypes.ARRAY(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "m",
                                                                        DataTypes.MAP(
                                                                                DataTypes.STRING(),
                                                                                DataTypes
                                                                                        .STRING()))))))));

        FirestoreTableSchema schema = schema(type, new int[] {0}, List.of(), List.of());

        assertThat(schema.hasPrimaryKey()).isTrue();
        assertThat(schema.getKeyIndex()).isZero();
    }

    static Stream<DataType> unmappedTypes() {
        return Stream.of(
                DataTypes.INT(),
                DataTypes.SMALLINT(),
                DataTypes.TINYINT(),
                DataTypes.FLOAT(),
                DataTypes.DECIMAL(10, 2),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIMESTAMP(3),
                DataTypes.MULTISET(DataTypes.STRING()),
                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
                DataTypes.ROW(DataTypes.FIELD("inner", DataTypes.INT())));
    }

    @ParameterizedTest
    @MethodSource("unmappedTypes")
    void aTypeWithoutAFirestoreFormIsRefusedNamingTheField(DataType type) {
        assertThatThrownBy(
                        () ->
                                schema(
                                        row(DataTypes.FIELD("f", type)),
                                        new int[0],
                                        List.of(),
                                        List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith("Field 'f");
    }

    @Test
    void aFieldNameFirestoreReservesIsRefusedAtAnyDepthButAMapKeyIsData() {
        assertThatThrownBy(
                        () ->
                                schema(
                                        row(DataTypes.FIELD("__meta__", DataTypes.STRING())),
                                        new int[0],
                                        List.of(),
                                        List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Field '__meta__' has a name Firestore reserves");
        assertThatThrownBy(
                        () ->
                                schema(
                                        row(
                                                DataTypes.FIELD(
                                                        "nested",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "____",
                                                                        DataTypes.STRING())))),
                                        new int[0],
                                        List.of(),
                                        List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Field 'nested.____' has a name Firestore reserves");

        // Names that only look close are ordinary fields, and a MAP's keys are data.
        schema(
                row(
                        DataTypes.FIELD("__a", DataTypes.STRING()),
                        DataTypes.FIELD("a__", DataTypes.STRING()),
                        DataTypes.FIELD("___", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))),
                new int[0],
                List.of(),
                List.of());
    }

    @Test
    void aMarkerOnAPathTwoFieldsShareIsRefused() {
        RowType type =
                row(
                        DataTypes.FIELD("a.b", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "a", DataTypes.ROW(DataTypes.FIELD("b", DataTypes.STRING()))));

        assertThatThrownBy(() -> schema(type, new int[0], List.of(), List.of("a.b")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("field paths that more than one field has: [a.b]");
        // Without a marker on it, the shared path is harmless: field names stay literal.
        schema(type, new int[0], List.of(), List.of());
    }

    @Test
    void thePrimaryKeyIsOneStringColumn() {
        RowType type =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("n", DataTypes.BIGINT().notNull()));

        assertThatThrownBy(() -> schema(type, new int[] {0, 1}, List.of(), List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("one STRING column, but this one has 2 columns");
        assertThatThrownBy(() -> schema(type, new int[] {1}, List.of(), List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'n' is the document id and must be STRING");
        assertThat(schema(type, new int[0], List.of(), List.of()).hasPrimaryKey()).isFalse();
    }

    @Test
    void markersReachThroughRowsArraysAndMapValues() {
        RowType type =
                row(
                        DataTypes.FIELD("location", GEO),
                        DataTypes.FIELD(
                                "stops",
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("position", GEO)))),
                        DataTypes.FIELD("author", DataTypes.STRING()),
                        DataTypes.FIELD("related", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                "byKind", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

        FirestoreTableSchema schema =
                schema(
                        type,
                        new int[0],
                        List.of("location", "stops.position"),
                        List.of("author", "related", "byKind.value"));

        assertThat(schema.isGeoPoint("location")).isTrue();
        assertThat(schema.isGeoPoint("stops.position")).isTrue();
        assertThat(schema.isReference("related")).isTrue();
        assertThat(schema.isReference("byKind.value")).isTrue();
        assertThat(schema.isReference("location")).isFalse();
    }

    @Test
    void aMarkerOnTheWrongTypeOrAnUnknownPathIsRefused() {
        RowType type =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD(
                                "swapped",
                                DataTypes.ROW(
                                        DataTypes.FIELD("longitude", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("latitude", DataTypes.DOUBLE()))));

        assertThatThrownBy(() -> schema(type, new int[0], List.of(), List.of("n")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("reference-field-paths names it, so it must be STRING");
        assertThatThrownBy(() -> schema(type, new int[0], List.of("swapped"), List.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("latitude and longitude, in that order");
        assertThatThrownBy(() -> schema(type, new int[0], List.of(), List.of("missing")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not declare: [missing]");
        assertThatThrownBy(() -> schema(type, new int[] {0}, List.of(), List.of("id")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("no schema marker can name it");
        assertThatThrownBy(() -> schema(type, new int[0], List.of("n"), List.of("n")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("both name [n]");
    }
}
