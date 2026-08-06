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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataToProtoConverter}: what each column type becomes on the wire. */
class RowDataToProtoConverterTest {

    /** The repeated field's values as a plain list, so AssertJ's element type is not a capture. */
    private static List<Object> items(Object converted) {
        List<Object> values = new ArrayList<>();
        ((Iterable<?>) converted).forEach(values::add);
        return values;
    }

    private static Object convertOne(DataType type, Object value) throws Exception {
        return convertOne(type, value, RowDataSchemaOptions.defaults());
    }

    private static Object convertOne(DataType type, Object value, RowDataSchemaOptions options)
            throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("v", type)).getLogicalType();
        TableSchema schema = RowTypeToTableSchemaConverter.convert(rowType, options);
        Descriptors.Descriptor descriptor =
                BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        RowDataToProtoConverter converter =
                new RowDataToProtoConverter(rowType, schema, descriptor);
        DynamicMessage message = converter.convert(GenericRowData.of(value));
        Descriptors.FieldDescriptor field = descriptor.getFields().get(0);
        if (field.isRepeated()) {
            return message.getField(field);
        }
        return message.hasField(field) ? message.getField(field) : null;
    }

    @Test
    void stringsBoolsAndNumbersTakeTheirPlainForms() throws Exception {
        assertThat(convertOne(DataTypes.STRING(), StringData.fromString("x"))).isEqualTo("x");
        assertThat(convertOne(DataTypes.BOOLEAN(), true)).isEqualTo(true);
        assertThat(convertOne(DataTypes.TINYINT(), (byte) 7)).isEqualTo(7L);
        assertThat(convertOne(DataTypes.SMALLINT(), (short) 7)).isEqualTo(7L);
        assertThat(convertOne(DataTypes.INT(), 7)).isEqualTo(7L);
        assertThat(convertOne(DataTypes.BIGINT(), 7L)).isEqualTo(7L);
        assertThat(convertOne(DataTypes.FLOAT(), 1.5f)).isEqualTo(1.5d);
        assertThat(convertOne(DataTypes.DOUBLE(), 1.5d)).isEqualTo(1.5d);
        assertThat(convertOne(DataTypes.BYTES(), new byte[] {1, 2}))
                .isEqualTo(ByteString.copyFrom(new byte[] {1, 2}));
    }

    @Test
    void aDateIsADayCount() throws Exception {
        assertThat(convertOne(DataTypes.DATE(), (int) LocalDate.of(2026, 8, 6).toEpochDay()))
                .isEqualTo((int) LocalDate.of(2026, 8, 6).toEpochDay());
    }

    @Test
    void timeAndDatetimeArePackedCivilValuesWhileATimestampIsEpochMicros() throws Exception {
        LocalTime time = LocalTime.of(12, 34, 56, 789_000_000);
        assertThat(convertOne(DataTypes.TIME(3), (int) (time.toNanoOfDay() / 1_000_000L)))
                .isEqualTo(CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(time));

        LocalDateTime wallClock = LocalDateTime.of(2026, 8, 6, 12, 34, 56, 789_000_000);
        assertThat(convertOne(DataTypes.TIMESTAMP(6), TimestampData.fromLocalDateTime(wallClock)))
                .isEqualTo(CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(wallClock));

        // The asymmetry that is easiest to get wrong: an instant is plain epoch microseconds.
        TimestampData instant = TimestampData.fromEpochMillis(1_500L, 123_000);
        assertThat(convertOne(DataTypes.TIMESTAMP_LTZ(6), instant)).isEqualTo(1_500_123L);
    }

    @Test
    void decimalsAreEncodedAtTheirDeclaredScale() throws Exception {
        DecimalData numeric = DecimalData.fromBigDecimal(new BigDecimal("1.25"), 10, 2);
        assertThat(convertOne(DataTypes.DECIMAL(10, 2), numeric))
                .isEqualTo(
                        BigDecimalByteStringEncoder.encodeToNumericByteString(
                                new BigDecimal("1.25")));

        DecimalData big = DecimalData.fromBigDecimal(new BigDecimal("1.25"), 38, 20);
        assertThat(convertOne(DataTypes.DECIMAL(38, 20), big))
                .isEqualTo(
                        BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                                new BigDecimal("1.25").setScale(20)));
    }

    @Test
    void aNullColumnIsLeftUnset() throws Exception {
        assertThat(convertOne(DataTypes.STRING(), null)).isNull();
    }

    @Test
    void anArrayBecomesARepeatedField() throws Exception {
        Object converted =
                convertOne(
                        DataTypes.ARRAY(DataTypes.STRING().notNull()),
                        new GenericArrayData(
                                new Object[] {
                                    StringData.fromString("a"), StringData.fromString("b")
                                }));
        assertThat(items(converted)).containsExactly("a", "b");
    }

    @Test
    void aNullElementFailsItsOwnRow() {
        assertThatThrownBy(
                        () ->
                                convertOne(
                                        DataTypes.ARRAY(DataTypes.STRING().notNull()),
                                        new GenericArrayData(new Object[] {null})))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cannot hold a null element");
    }

    @Test
    void aNullArrayIsAnEmptyRepeatedField() throws Exception {
        assertThat(items(convertOne(DataTypes.ARRAY(DataTypes.STRING().notNull()), null)))
                .isEmpty();
    }

    @Test
    void aMapBecomesRepeatedKeyValueMessages() throws Exception {
        Map<Object, Object> map = new HashMap<>();
        map.put(StringData.fromString("k"), 1);
        Object converted =
                convertOne(
                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT().notNull()),
                        new GenericMapData(map));
        assertThat(items(converted)).hasSize(1);
        DynamicMessage entry = (DynamicMessage) items(converted).get(0);
        assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("key")))
                .isEqualTo("k");
        assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("value")))
                .isEqualTo(1L);
    }

    @Test
    void aNestedRowBecomesANestedMessage() throws Exception {
        Object converted =
                convertOne(
                        DataTypes.ROW(DataTypes.FIELD("inner", DataTypes.INT())),
                        GenericRowData.of(3));
        DynamicMessage nested = (DynamicMessage) converted;
        assertThat(nested.getField(nested.getDescriptorForType().findFieldByName("inner")))
                .isEqualTo(3L);
    }

    @Test
    void columnsPairToDescriptorFieldsByPositionEvenUnderALocaleThatLowercasesDifferently()
            throws Exception {
        // BQTableSchemaToProtoDescriptor lowercases column names with the *default* locale, so
        // under tr_TR a column named ID becomes the proto field "ıd" (dotless i) that no
        // Locale.ROOT key matches. Pairing by position is what makes that a non-event — and this
        // is the only shape of test that can tell the two apart.
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            RowType rowType =
                    (RowType)
                            DataTypes.ROW(DataTypes.FIELD("ID", DataTypes.STRING()))
                                    .getLogicalType();
            TableSchema schema =
                    RowTypeToTableSchemaConverter.convert(rowType, RowDataSchemaOptions.defaults());
            Descriptors.Descriptor descriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
            DynamicMessage message =
                    new RowDataToProtoConverter(rowType, schema, descriptor)
                            .convert(GenericRowData.of(StringData.fromString("x")));
            assertThat(message.getField(descriptor.getFields().get(0))).isEqualTo("x");
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void aMarkedStringGoesThroughVerbatim() throws Exception {
        RowDataSchemaOptions options =
                RowDataSchemaOptions.builder()
                        .jsonFieldPaths(Collections.singletonList("v"))
                        .build();
        // Not validated, exactly as on every other write path: malformed JSON is BigQuery's to
        // reject, per row.
        assertThat(convertOne(DataTypes.STRING(), StringData.fromString("{not json"), options))
                .isEqualTo("{not json");
    }

    @Test
    void aMarkedRowIsRenderedAsJsonText() throws Exception {
        RowDataSchemaOptions options =
                RowDataSchemaOptions.builder()
                        .jsonFieldPaths(Collections.singletonList("v"))
                        .build();
        Object converted =
                convertOne(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING())),
                        GenericRowData.of(1, StringData.fromString("x")),
                        options);
        assertThat(converted).isEqualTo("{\"a\":1,\"b\":\"x\"}");
    }

    @Test
    void aMarkedGeographyStringGoesThroughVerbatim() throws Exception {
        RowDataSchemaOptions options =
                RowDataSchemaOptions.builder()
                        .geographyFieldPaths(Collections.singletonList("v"))
                        .build();
        assertThat(convertOne(DataTypes.STRING(), StringData.fromString("POINT(1 2)"), options))
                .isEqualTo("POINT(1 2)");
    }
}
