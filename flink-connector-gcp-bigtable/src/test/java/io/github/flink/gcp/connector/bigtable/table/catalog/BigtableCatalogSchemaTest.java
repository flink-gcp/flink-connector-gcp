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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.bigtable.BigtableAdminProtos;
import io.github.flink.gcp.connector.bigtable.table.CatalogKeyType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link BigtableCatalogSchema}: the row key, one map per family, and the value type each family's
 * metadata decides. The aggregate types are built as a family is created with them ({@link
 * BigtableAdminProtos}), since the emulator cannot create an aggregate family.
 */
class BigtableCatalogSchemaTest {

    private static final Type RAW = Type.getDefaultInstance();

    private static String render(Schema schema) {
        return schema.getColumns().stream()
                .map(
                        column ->
                                column.getName()
                                        + " "
                                        + ((Schema.UnresolvedPhysicalColumn) column).getDataType())
                .collect(Collectors.joining(", "));
    }

    private static Map<String, Type> families(Object... familiesAndTypes) {
        Map<String, Type> families = new LinkedHashMap<>();
        for (int i = 0; i < familiesAndTypes.length; i += 2) {
            families.put((String) familiesAndTypes[i], (Type) familiesAndTypes[i + 1]);
        }
        return families;
    }

    @Test
    void aTableIsItsRowKeyThenOneMapPerFamilyInNameOrder() {
        Schema schema =
                BigtableCatalogSchema.of(families("b", RAW, "a", RAW), CatalogKeyType.BYTES);

        assertThat(render(schema))
                .isEqualTo("_key BYTES NOT NULL, a MAP<BYTES, BYTES>, b MAP<BYTES, BYTES>");
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getPrimaryKey().get().getColumnNames()).containsExactly("_key");
    }

    @Test
    void theStringKeyTypeAppliesToTheRowKeyAndEveryMapKeyButNoValue() {
        Schema schema =
                BigtableCatalogSchema.of(
                        families("cf", RAW, "totals", BigtableAdminProtos.int64Sum()),
                        CatalogKeyType.STRING);

        assertThat(render(schema))
                .isEqualTo(
                        "_key STRING NOT NULL, cf MAP<STRING, BYTES>, totals MAP<STRING, BIGINT>");
    }

    @Test
    void aTableWithNoFamilyIsItsRowKeyAlone() {
        assertThat(render(BigtableCatalogSchema.of(families(), CatalogKeyType.BYTES)))
                .isEqualTo("_key BYTES NOT NULL");
    }

    @Test
    void int64SumMinAndMaxFamiliesReadAsBigintAndHllAsBytes() {
        assertThat(BigtableCatalogSchema.valueType(BigtableAdminProtos.int64Sum()))
                .isEqualTo(DataTypes.BIGINT());
        assertThat(BigtableCatalogSchema.valueType(BigtableAdminProtos.int64Min()))
                .isEqualTo(DataTypes.BIGINT());
        assertThat(BigtableCatalogSchema.valueType(BigtableAdminProtos.int64Max()))
                .isEqualTo(DataTypes.BIGINT());
        assertThat(BigtableCatalogSchema.valueType(BigtableAdminProtos.int64Hll()))
                .isEqualTo(DataTypes.BYTES());
        assertThat(BigtableCatalogSchema.valueType(RAW)).isEqualTo(DataTypes.BYTES());
    }

    private static Type aggregate(Type.Aggregate.Builder aggregate) {
        return Type.newBuilder().setAggregateType(aggregate).build();
    }

    /**
     * An HLL family is BYTES even when the service reports an int64 state, which the admin API
     * names only as the sketch's count-estimate conversion: as BIGINT, the default trailing-bytes
     * policy would read each sketch's first eight bytes as a number.
     */
    @Test
    void anHllFamilyReadsAsBytesWhateverStateTypeItReports() {
        Type int64 = Type.newBuilder().setInt64Type(Type.Int64.getDefaultInstance()).build();

        assertThat(
                        BigtableCatalogSchema.valueType(
                                aggregate(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(int64)
                                                .setStateType(int64)
                                                .setHllppUniqueCount(
                                                        Type.Aggregate
                                                                .HyperLogLogPlusPlusUniqueCount
                                                                .getDefaultInstance()))))
                .isEqualTo(DataTypes.BYTES());
    }

    /** A sum, min or max family is BIGINT only while a reported state agrees with its input. */
    @Test
    void aReportedStateTypeThatIsNotABigEndianInt64ReadsAsBytes() {
        Type int64 = Type.newBuilder().setInt64Type(Type.Int64.getDefaultInstance()).build();
        Type bytes = Type.newBuilder().setBytesType(Type.Bytes.getDefaultInstance()).build();

        assertThat(
                        BigtableCatalogSchema.valueType(
                                aggregate(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(int64)
                                                .setStateType(bytes)
                                                .setSum(Type.Aggregate.Sum.getDefaultInstance()))))
                .isEqualTo(DataTypes.BYTES());
        assertThat(
                        BigtableCatalogSchema.valueType(
                                aggregate(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(int64)
                                                .setStateType(int64)
                                                .setMax(Type.Aggregate.Max.getDefaultInstance()))))
                .isEqualTo(DataTypes.BIGINT());
    }

    /**
     * An encoding newer than the pinned protobuf parses as unset with an unknown field; reading it
     * as big-endian could misread every cell, so it reads as BYTES.
     */
    @Test
    void anInt64EncodingTheProtobufDoesNotKnowReadsAsBytes() {
        Type.Int64.Encoding unknown =
                Type.Int64.Encoding.newBuilder()
                        .setUnknownFields(
                                com.google.protobuf.UnknownFieldSet.newBuilder()
                                        .addField(
                                                99,
                                                com.google.protobuf.UnknownFieldSet.Field
                                                        .newBuilder()
                                                        .addVarint(1)
                                                        .build())
                                        .build())
                        .build();
        Type input =
                Type.newBuilder()
                        .setInt64Type(Type.Int64.newBuilder().setEncoding(unknown))
                        .build();

        assertThat(unknown.getEncodingCase())
                .isEqualTo(Type.Int64.Encoding.EncodingCase.ENCODING_NOT_SET);
        assertThat(
                        BigtableCatalogSchema.valueType(
                                aggregate(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(input)
                                                .setStateType(input)
                                                .setSum(Type.Aggregate.Sum.getDefaultInstance()))))
                .isEqualTo(DataTypes.BYTES());
    }

    /** The client's builders set big-endian bytes, so the unset encoding needs its own case. */
    @Test
    void anInt64InputWithNoEncodingSetIsBigEndian() {
        assertThat(
                        BigtableCatalogSchema.valueType(
                                aggregate(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(
                                                        Type.newBuilder()
                                                                .setInt64Type(
                                                                        Type.Int64
                                                                                .getDefaultInstance()))
                                                .setSum(Type.Aggregate.Sum.getDefaultInstance()))))
                .isEqualTo(DataTypes.BIGINT());
    }

    /**
     * An ordered-code int64 is not the codec's BIGINT layout; BYTES reads it without misreading.
     */
    @Test
    void anOrderedCodeInt64OrAnUnknownTypeReadsAsBytes() {
        Type orderedCode =
                Type.newBuilder()
                        .setInt64Type(
                                Type.Int64.newBuilder()
                                        .setEncoding(
                                                Type.Int64.Encoding.newBuilder()
                                                        .setOrderedCodeBytes(
                                                                Type.Int64.Encoding.OrderedCodeBytes
                                                                        .getDefaultInstance())))
                        .build();
        Type orderedCodeSum =
                Type.newBuilder()
                        .setAggregateType(
                                Type.Aggregate.newBuilder()
                                        .setInputType(orderedCode)
                                        .setSum(Type.Aggregate.Sum.getDefaultInstance()))
                        .build();
        Type typedRaw = Type.newBuilder().setInt64Type(Type.Int64.getDefaultInstance()).build();
        Type unknownAggregator =
                Type.newBuilder()
                        .setAggregateType(
                                Type.Aggregate.newBuilder()
                                        .setInputType(
                                                Type.newBuilder()
                                                        .setInt64Type(
                                                                Type.Int64.getDefaultInstance())))
                        .build();

        assertThat(BigtableCatalogSchema.valueType(orderedCodeSum)).isEqualTo(DataTypes.BYTES());
        assertThat(BigtableCatalogSchema.valueType(typedRaw)).isEqualTo(DataTypes.BYTES());
        assertThat(BigtableCatalogSchema.valueType(unknownAggregator)).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void aFamilyNamedLikeTheRowKeyColumnIsRefused() {
        assertThatThrownBy(
                        () ->
                                BigtableCatalogSchema.of(
                                        families("_key", RAW, "cf", RAW), CatalogKeyType.BYTES))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column family '_key' has the name the catalog gives");
        // Flink column names are case-sensitive, so only the exact name collides.
        assertThat(render(BigtableCatalogSchema.of(families("_KEY", RAW), CatalogKeyType.BYTES)))
                .isEqualTo("_key BYTES NOT NULL, _KEY MAP<BYTES, BYTES>");
    }
}
