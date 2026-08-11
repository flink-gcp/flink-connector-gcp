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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class StructToRowDataConverterTest {

    @Test
    void convertsScalarsMarkersArraysAndNulls() {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                                        DataTypes.FIELD("ratio", DataTypes.FLOAT()),
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 9)),
                                        DataTypes.FIELD("document", DataTypes.STRING()),
                                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                                        DataTypes.FIELD("day", DataTypes.DATE()),
                                        DataTypes.FIELD("at", DataTypes.TIMESTAMP_LTZ(9)),
                                        DataTypes.FIELD(
                                                "labels", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD("missing", DataTypes.BIGINT()),
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("ratio64", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("state", DataTypes.BIGINT()),
                                        DataTypes.FIELD("raw", DataTypes.BYTES()),
                                        DataTypes.FIELD(
                                                "numbers", DataTypes.ARRAY(DataTypes.BIGINT())))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        type,
                        new int[0],
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.singletonList("document"),
                        Collections.singletonMap("payload", "example.Payload"),
                        Collections.singletonMap("state", "example.State"));
        Timestamp timestamp = Timestamp.parseTimestamp("2026-08-11T01:02:03.123456789Z");
        Struct struct =
                Struct.newBuilder()
                        .set("enabled")
                        .to(true)
                        .set("ratio")
                        .to(Value.float32(1.25F))
                        .set("amount")
                        .to(new BigDecimal("12.340000000"))
                        .set("document")
                        .to(Value.json("{\"ok\":true}"))
                        .set("payload")
                        .to(
                                Value.protoMessage(
                                        ByteArray.copyFrom(new byte[] {1, 2}), "example.Payload"))
                        .set("day")
                        .to(Date.parseDate("2026-08-11"))
                        .set("at")
                        .to(timestamp)
                        .set("labels")
                        .toStringArray(Arrays.asList("a", null, "b"))
                        .set("missing")
                        .to((Long) null)
                        .set("id")
                        .to(7L)
                        .set("ratio64")
                        .to(2.5D)
                        .set("state")
                        .to(Value.protoEnum(2L, "example.State"))
                        .set("raw")
                        .to(ByteArray.copyFrom(new byte[] {3, 4}))
                        .set("numbers")
                        .toInt64Array(Arrays.asList(5L, null, 8L))
                        .build();

        RowData row = new StructToRowDataConverter(schema, null).convert(struct);

        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getFloat(1)).isEqualTo(1.25F);
        assertThat(row.getDecimal(2, 38, 9))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("12.340000000"), 38, 9));
        assertThat(row.getString(3).toString()).isEqualTo("{\"ok\":true}");
        assertThat(row.getBinary(4)).containsExactly(1, 2);
        assertThat(row.getInt(5)).isEqualTo((int) LocalDate.parse("2026-08-11").toEpochDay());
        assertThat(row.getTimestamp(6, 9).toInstant())
                .isEqualTo(timestamp.toSqlTimestamp().toInstant());
        ArrayData labels = row.getArray(7);
        assertThat(labels.getString(0).toString()).isEqualTo("a");
        assertThat(labels.isNullAt(1)).isTrue();
        assertThat(labels.getString(2).toString()).isEqualTo("b");
        assertThat(row.isNullAt(8)).isTrue();
        assertThat(row.getLong(9)).isEqualTo(7L);
        assertThat(row.getDouble(10)).isEqualTo(2.5D);
        assertThat(row.getLong(11)).isEqualTo(2L);
        assertThat(row.getBinary(12)).containsExactly(3, 4);
        ArrayData numbers = row.getArray(13);
        assertThat(numbers.getLong(0)).isEqualTo(5L);
        assertThat(numbers.isNullAt(1)).isTrue();
        assertThat(numbers.getLong(2)).isEqualTo(8L);
    }

    @Test
    void projectionUsesRequestedOrderAndCanProduceAnEmptyRow() {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        type,
                        new int[0],
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        Struct struct = Struct.newBuilder().set("name").to("Ada").build();

        RowData projected = new StructToRowDataConverter(schema, new int[] {1}).convert(struct);
        RowData empty =
                new StructToRowDataConverter(schema, new int[0])
                        .convert(Struct.newBuilder().set("id").to(1L).build());

        assertThat(projected.getArity()).isEqualTo(1);
        assertThat(projected.getString(0).toString()).isEqualTo("Ada");
        assertThat(empty.getArity()).isZero();
    }
}
