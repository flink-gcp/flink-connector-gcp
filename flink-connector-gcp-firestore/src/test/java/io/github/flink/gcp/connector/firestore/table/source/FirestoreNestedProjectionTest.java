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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.GeoPoint;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The field mask and the path converters of a nested projection ({@code docs/adr/0179}, the
 * revision for #1839): what is selected, and that a path reads as the same field inside the whole
 * column reads.
 */
class FirestoreNestedProjectionTest {

    private static final RowType TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD(
                                            "r",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("a", DataTypes.BIGINT()),
                                                    DataTypes.FIELD("b", DataTypes.STRING()),
                                                    DataTypes.FIELD(
                                                            "c",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "x",
                                                                            DataTypes.BIGINT()))),
                                                    DataTypes.FIELD("ref", DataTypes.STRING()),
                                                    DataTypes.FIELD(
                                                            "loc",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "latitude",
                                                                            DataTypes.DOUBLE()),
                                                                    DataTypes.FIELD(
                                                                            "longitude",
                                                                            DataTypes.DOUBLE()))))),
                                    DataTypes.FIELD(
                                            "n",
                                            DataTypes.ROW(DataTypes.FIELD("a", DataTypes.BIGINT()))
                                                    .notNull()),
                                    DataTypes.FIELD(
                                            "p.q",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("u.v", DataTypes.STRING()))),
                                    DataTypes.FIELD(
                                            "g",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                                                    DataTypes.FIELD(
                                                            "longitude", DataTypes.DOUBLE()))),
                                    DataTypes.FIELD(
                                            "k",
                                            DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "m",
                                                                    DataTypes.ROW(
                                                                                    DataTypes.FIELD(
                                                                                            "z",
                                                                                            DataTypes
                                                                                                    .BIGINT()))
                                                                            .notNull()))
                                                    .notNull()))
                            .getLogicalType();

    private static final FirestoreTableSchema SCHEMA =
            FirestoreTableSchema.of(TYPE, new int[] {0}, List.of("g", "r.loc"), List.of("r.ref"));

    private static String[] mask(int[]... columns) {
        return FirestoreDynamicSource.fieldMask(SCHEMA, columns);
    }

    private static Object read(int[] path, TypeMismatchPolicy policy, Object value)
            throws FirestoreToRowDataConverter.Mismatch {
        return FirestoreToRowDataConverter.createPath(SCHEMA, path, policy).convert(value);
    }

    @Test
    void aNullablePathIsSelectedWholeAndEachSegmentLiterally() {
        assertThat(mask(new int[] {1, 0})).containsExactly("r.a");
        assertThat(mask(new int[] {1, 2, 0})).containsExactly("r.c.x");
        assertThat(mask(new int[] {3, 0}))
                .containsExactly(FieldPath.of("p.q", "u.v").toString())
                .containsExactly("`p.q`.`u.v`");
    }

    @Test
    void thePathStopsAtANotNullFieldAndAtAGeographicalPoint() {
        assertThat(mask(new int[] {2, 0})).containsExactly("n");
        assertThat(mask(new int[] {5, 0, 0})).containsExactly("k");
        assertThat(mask(new int[] {4, 0}, new int[] {4, 1})).containsExactly("g");
        assertThat(mask(new int[] {1, 4, 0})).containsExactly("r.loc");
    }

    @Test
    void thePathStopsAtANotNullFieldBelowANullableOneToo() {
        // Such a table answers supportsNestedProjection() with false; the mask is right anyway.
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "r",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "c",
                                                                DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        "x",
                                                                                        DataTypes
                                                                                                .BIGINT()))
                                                                        .notNull()))))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(type, new int[0], List.of(), List.of());

        assertThat(FirestoreDynamicSource.fieldMask(schema, new int[][] {{0, 0, 0}}))
                .containsExactly("r");
    }

    @Test
    void anEqualPathIsSelectedOnceAndTheDocumentIdNotAtAll() {
        assertThat(mask(new int[] {2, 0}, new int[] {0}, new int[] {2, 0})).containsExactly("n");
        assertThat(mask(new int[] {1, 0}, new int[] {1, 1})).containsExactly("r.a", "r.b");
        assertThat(mask(new int[] {0})).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(TypeMismatchPolicy.class)
    void aNamedFieldReadsAsItDoesInsideTheWholeColumn(TypeMismatchPolicy policy) throws Exception {
        int[] ra = {1, 0};
        assertThat(read(ra, policy, Map.of("a", 5L))).isEqualTo(5L);
        assertThat(read(ra, policy, null)).isNull();
        assertThat(read(ra, policy, Map.of("b", "x"))).isNull();
        assertThat(read(new int[] {1, 2, 0}, policy, Map.of("c", Map.of("x", 3L)))).isEqualTo(3L);
        assertThat(read(new int[] {3, 0}, policy, Map.of("u.v", "s")))
                .isEqualTo(StringData.fromString("s"));
        assertThat(read(new int[] {4, 1}, policy, new GeoPoint(1.0, 2.0))).isEqualTo(2.0);
        assertThat(read(new int[] {1, 4, 1}, policy, Map.of("loc", new GeoPoint(3.0, 4.0))))
                .isEqualTo(4.0);
    }

    @Test
    void schemasOfEqualPathsAreEqualAndOfOtherPathsNot() {
        RowDataDeserializationSchema ra =
                new RowDataDeserializationSchema(
                        SCHEMA,
                        new int[][] {{1, 0}},
                        List.of(),
                        TypeMismatchPolicy.FAIL,
                        InternalTypeInfo.of(TYPE));

        assertThat(
                        new RowDataDeserializationSchema(
                                SCHEMA,
                                new int[][] {{1, 0}},
                                List.of(),
                                TypeMismatchPolicy.FAIL,
                                InternalTypeInfo.of(TYPE)))
                .isEqualTo(ra)
                .hasSameHashCodeAs(ra);
        assertThat(
                        new RowDataDeserializationSchema(
                                SCHEMA,
                                new int[][] {{1, 1}},
                                List.of(),
                                TypeMismatchPolicy.FAIL,
                                InternalTypeInfo.of(TYPE)))
                .isNotEqualTo(ra);
    }

    @ParameterizedTest
    @EnumSource(TypeMismatchPolicy.class)
    void aFieldTheQueryDoesNotNameIsNotChecked(TypeMismatchPolicy policy) throws Exception {
        assertThat(read(new int[] {1, 0}, policy, Map.of("a", 5L, "b", 7L))).isEqualTo(5L);
    }

    @Test
    void aMismatchOnThePathFollowsThePolicy() throws Exception {
        int[] ra = {1, 0};
        assertThat(read(ra, TypeMismatchPolicy.NULL, Map.of("a", "x"))).isNull();
        assertThat(read(ra, TypeMismatchPolicy.NULL, "hello")).isNull();
        assertThat(read(new int[] {4, 0}, TypeMismatchPolicy.NULL, "x")).isNull();
        assertThatThrownBy(() -> read(ra, TypeMismatchPolicy.FAIL, Map.of("a", "x")))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("'r.a'")
                .satisfies(
                        e ->
                                assertThat(
                                                ((FirestoreToRowDataConverter.Mismatch) e)
                                                        .readableAsNull())
                                        .isTrue());
        assertThatThrownBy(() -> read(ra, TypeMismatchPolicy.FAIL, "hello"))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("'r'")
                .hasMessageContaining("a map");
        assertThatThrownBy(() -> read(new int[] {4, 0}, TypeMismatchPolicy.FAIL, "x"))
                .hasMessageContaining("a geographical point");
    }

    @ParameterizedTest
    @EnumSource(TypeMismatchPolicy.class)
    void aMissingNotNullFieldOnThePathFailsUnderEitherPolicy(TypeMismatchPolicy policy) {
        assertThatThrownBy(() -> read(new int[] {5, 0, 0}, policy, null))
                .isInstanceOf(FirestoreToRowDataConverter.Mismatch.class)
                .hasMessageContaining("'k'")
                .satisfies(
                        e ->
                                assertThat(
                                                ((FirestoreToRowDataConverter.Mismatch) e)
                                                        .readableAsNull())
                                        .isFalse());
        assertThatThrownBy(() -> read(new int[] {5, 0, 0}, policy, Map.of()))
                .hasMessageContaining("'k.m'");
    }

    @Test
    void aMarkedReferenceOnANestedPathReadsAsItsPath() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            read(
                                    new int[] {1, 3},
                                    TypeMismatchPolicy.FAIL,
                                    Map.of("ref", client.document("users/bob"))))
                    .isEqualTo(StringData.fromString("users/bob"));
        }
    }
}
