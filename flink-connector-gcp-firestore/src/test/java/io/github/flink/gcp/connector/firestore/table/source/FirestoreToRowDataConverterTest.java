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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.BsonObjectId;
import com.google.cloud.firestore.BsonTimestamp;
import com.google.cloud.firestore.Decimal128Value;
import com.google.cloud.firestore.FieldValue;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.Int32Value;
import com.google.cloud.firestore.MaxKey;
import com.google.cloud.firestore.MinKey;
import com.google.cloud.firestore.RegexValue;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreToRowDataConverterTest {

    private static final DataType GEO =
            DataTypes.ROW(
                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                    DataTypes.FIELD("longitude", DataTypes.DOUBLE()));

    private static FirestoreToRowDataConverter converter(
            DataType type, TypeMismatchPolicy policy, List<String> geo, List<String> refs) {
        RowType row = (RowType) DataTypes.ROW(DataTypes.FIELD("f", type)).getLogicalType();
        FirestoreTableSchema schema = FirestoreTableSchema.of(row, new int[0], geo, refs);
        return FirestoreToRowDataConverter.create(schema, "f", row.getTypeAt(0), policy);
    }

    private static FirestoreToRowDataConverter strict(DataType type) {
        return converter(type, TypeMismatchPolicy.FAIL, List.of(), List.of());
    }

    @Test
    void everyMappedValueReadsAsItsFlinkValue() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(strict(DataTypes.STRING()).convert("s"))
                    .isEqualTo(StringData.fromString("s"));
            assertThat(strict(DataTypes.BOOLEAN()).convert(true)).isEqualTo(true);
            assertThat(strict(DataTypes.BIGINT()).convert(5L)).isEqualTo(5L);
            assertThat(strict(DataTypes.BIGINT()).convert(new Int32Value(7))).isEqualTo(7L);
            assertThat(strict(DataTypes.DOUBLE()).convert(1.5d)).isEqualTo(1.5d);
            assertThat((byte[]) strict(DataTypes.BYTES()).convert(Blob.fromBytes(new byte[] {3})))
                    .containsExactly(3);
            assertThat(strict(DataTypes.ARRAY(DataTypes.BIGINT())).convert(Arrays.asList(1L, null)))
                    .isEqualTo(new GenericArrayData(new Object[] {1L, null}));
            assertThat(
                            strict(DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                                    .convert(Map.of("k", 1L)))
                    .isEqualTo(new GenericMapData(Map.of(StringData.fromString("k"), 1L)));
            assertThat(
                            strict(DataTypes.ROW(DataTypes.FIELD("a", DataTypes.BIGINT())))
                                    .convert(Map.of("a", 1L, "ignored", "x")))
                    .isEqualTo(GenericRowData.of(1L));
            assertThat(
                            converter(GEO, TypeMismatchPolicy.FAIL, List.of("f"), List.of())
                                    .convert(new GeoPoint(1.0, 2.0)))
                    .isEqualTo(GenericRowData.of(1.0, 2.0));
            assertThat(
                            converter(
                                            DataTypes.STRING(),
                                            TypeMismatchPolicy.FAIL,
                                            List.of(),
                                            List.of("f"))
                                    .convert(client.document("users/alice")))
                    .isEqualTo(StringData.fromString("users/alice"));
        }
    }

    @Test
    void anIntegerReadsIntoADoubleOnlyWhenItIsExact() throws Exception {
        FirestoreToRowDataConverter doubles = strict(DataTypes.DOUBLE());

        assertThat(doubles.convert(1L << 53)).isEqualTo((double) (1L << 53));
        assertThat(doubles.convert(-(1L << 53))).isEqualTo((double) -(1L << 53));
        assertThatThrownBy(() -> doubles.convert((1L << 53) + 1))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("which a DOUBLE cannot represent exactly");
        assertThatThrownBy(() -> doubles.convert(-(1L << 53) - 1))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("which a DOUBLE cannot represent exactly");
        // Math.abs(Long.MIN_VALUE) is negative, so the bound is two comparisons.
        assertThatThrownBy(() -> doubles.convert(Long.MIN_VALUE))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("which a DOUBLE cannot represent exactly");
    }

    @Test
    void aTimestampIsTruncatedToTheColumnsPrecisionOnBothSidesOfTheEpoch() throws Exception {
        assertThat(
                        strict(DataTypes.TIMESTAMP_LTZ(3))
                                .convert(Timestamp.ofTimeSecondsAndNanos(1, 123_456_789)))
                .isEqualTo(TimestampData.fromEpochMillis(1123));
        assertThat(
                        strict(DataTypes.TIMESTAMP_LTZ(6))
                                .convert(Timestamp.ofTimeSecondsAndNanos(-1, 123_456_789)))
                .isEqualTo(TimestampData.fromEpochMillis(-877, 456_000));
    }

    static Stream<Object[]> mismatches() throws Exception {
        return Stream.of(
                new Object[] {DataTypes.BIGINT(), "1"},
                new Object[] {DataTypes.BOOLEAN(), "true"},
                new Object[] {DataTypes.DOUBLE(), "1.5"},
                new Object[] {DataTypes.DOUBLE(), new Int32Value(1)},
                new Object[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()), List.of(1L)},
                new Object[] {DataTypes.BIGINT(), 1.5d},
                new Object[] {DataTypes.STRING(), 1L},
                new Object[] {DataTypes.BYTES(), Blob.createBsonBinary(128, new byte[] {1})},
                new Object[] {DataTypes.STRING(), new Decimal128Value("1.5")},
                new Object[] {DataTypes.STRING(), new BsonObjectId("507f1f77bcf86cd799439011")},
                new Object[] {DataTypes.STRING(), new RegexValue("^a", "i")},
                new Object[] {DataTypes.STRING(), MinKey.instance()},
                new Object[] {DataTypes.STRING(), MaxKey.instance()},
                new Object[] {DataTypes.TIMESTAMP_LTZ(6), new BsonTimestamp(1, 2)},
                new Object[] {
                    DataTypes.ARRAY(DataTypes.DOUBLE()), FieldValue.vector(new double[] {1})
                },
                new Object[] {GEO, Map.of("latitude", 1.0, "longitude", 2.0)},
                new Object[] {DataTypes.ROW(DataTypes.FIELD("a", DataTypes.BIGINT())), "x"});
    }

    @ParameterizedTest
    @MethodSource("mismatches")
    void aValueOutsideTheMappingFailsUnderFailAndReadsAsNullUnderNull(DataType type, Object value)
            throws Exception {
        List<String> geo = type.equals(GEO) ? List.of("f") : List.of();
        assertThatThrownBy(
                        () ->
                                converter(type, TypeMismatchPolicy.FAIL, geo, List.of())
                                        .convert(value))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageStartingWith("Field 'f' ");
        assertThat(converter(type, TypeMismatchPolicy.NULL, geo, List.of()).convert(value))
                .isNull();
    }

    @Test
    void aStringInAReferenceColumnIsAMismatch() {
        assertThatThrownBy(
                        () ->
                                converter(
                                                DataTypes.STRING(),
                                                TypeMismatchPolicy.FAIL,
                                                List.of(),
                                                List.of("f"))
                                        .convert("users/alice"))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("which is not a reference");
    }

    @Test
    void markersReachIntoArrayElementsAndMapValues() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            converter(
                                            DataTypes.ARRAY(DataTypes.STRING()),
                                            TypeMismatchPolicy.FAIL,
                                            List.of(),
                                            List.of("f"))
                                    .convert(List.of(client.document("c/a"))))
                    .isEqualTo(new GenericArrayData(new Object[] {StringData.fromString("c/a")}));
            assertThat(
                            converter(
                                            DataTypes.MAP(DataTypes.STRING(), GEO),
                                            TypeMismatchPolicy.FAIL,
                                            List.of("f.value"),
                                            List.of())
                                    .convert(Map.of("home", new GeoPoint(1.0, 2.0))))
                    .isEqualTo(
                            new GenericMapData(
                                    Map.of(
                                            StringData.fromString("home"),
                                            GenericRowData.of(1.0, 2.0))));
        }
    }

    @Test
    void underNullAMismatchedElementOrMapValueReadsAsNullAlone() throws Exception {
        assertThat(
                        converter(
                                        DataTypes.ARRAY(DataTypes.BIGINT()),
                                        TypeMismatchPolicy.NULL,
                                        List.of(),
                                        List.of())
                                .convert(Arrays.asList(1L, "x")))
                .isEqualTo(new GenericArrayData(new Object[] {1L, null}));
        assertThat(
                        converter(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                                        TypeMismatchPolicy.NULL,
                                        List.of(),
                                        List.of())
                                .convert(Map.of("k", "x")))
                .isEqualTo(
                        new GenericMapData(
                                Collections.singletonMap(StringData.fromString("k"), null)));
    }

    @Test
    void aReferenceInAnUnmarkedStringColumnIsAMismatch() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThatThrownBy(() -> strict(DataTypes.STRING()).convert(client.document("c/d")))
                    .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                    .hasMessageContaining("which is not a string");
        }
    }

    @Test
    void underNullTheInnermostNullableFieldReadsAsNullAndNotNullStillFails() throws Exception {
        DataType row =
                DataTypes.ROW(
                        DataTypes.FIELD("loose", DataTypes.BIGINT()),
                        DataTypes.FIELD("tight", DataTypes.BIGINT().notNull()));
        FirestoreToRowDataConverter lenient =
                converter(row, TypeMismatchPolicy.NULL, List.of(), List.of());

        assertThat(lenient.convert(Map.of("loose", "x", "tight", 1L)))
                .isEqualTo(GenericRowData.of(null, 1L));
        // A NOT NULL field cannot read as NULL, so the nullable row around it does.
        assertThat(lenient.convert(Map.of("loose", 1L, "tight", "x"))).isNull();
        assertThatThrownBy(
                        () ->
                                converter(
                                                DataTypes.BIGINT().notNull(),
                                                TypeMismatchPolicy.NULL,
                                                List.of(),
                                                List.of())
                                        .convert("x"))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class);
        assertThatThrownBy(
                        () ->
                                converter(
                                                DataTypes.BIGINT().notNull(),
                                                TypeMismatchPolicy.NULL,
                                                List.of(),
                                                List.of())
                                        .convert(null))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("a NOT NULL field holds null or is missing");
    }
}
