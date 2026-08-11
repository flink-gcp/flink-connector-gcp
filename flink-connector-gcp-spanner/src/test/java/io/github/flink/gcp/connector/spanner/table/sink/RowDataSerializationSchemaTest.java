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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RowDataSerializationSchemaTest {

    private static final RowType ROW_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 9)),
                                    DataTypes.FIELD("at", DataTypes.TIMESTAMP_LTZ(9)),
                                    DataTypes.FIELD("payload", DataTypes.BYTES()),
                                    DataTypes.FIELD("labels", DataTypes.ARRAY(DataTypes.STRING())))
                            .getLogicalType();

    private static SpannerTableSchemaConverter schema(boolean primaryKey) {
        Map<String, String> proto = new HashMap<>();
        proto.put("payload", "example.Payload");
        return SpannerTableSchemaConverter.of(
                ROW_TYPE,
                primaryKey ? new int[] {0} : new int[0],
                Dialect.GOOGLE_STANDARD_SQL,
                Collections.emptyList(),
                proto,
                Collections.emptyMap());
    }

    private static GenericRowData row(RowKind kind) {
        GenericRowData row =
                GenericRowData.of(
                        7L,
                        StringData.fromString("Ada"),
                        DecimalData.fromBigDecimal(new BigDecimal("12.340000000"), 38, 9),
                        TimestampData.fromInstant(Instant.parse("2026-08-11T01:02:03.123456789Z")),
                        new byte[] {1, 2, 3},
                        new GenericArrayData(
                                new Object[] {
                                    StringData.fromString("a"), StringData.fromString("b")
                                }));
        row.setRowKind(kind);
        return row;
    }

    @Test
    void aPrimaryKeyMakesWritesIdempotentUpserts() throws Exception {
        Mutation mutation =
                new RowDataSerializationSchema(schema(true), "people")
                        .serialize(row(RowKind.UPDATE_AFTER), null);

        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
        assertThat(mutation.getTable()).isEqualTo("people");
        assertThat(mutation.asMap().get("name").getString()).isEqualTo("Ada");
        assertThat(mutation.asMap().get("amount").getNumeric())
                .isEqualByComparingTo("12.340000000");
        assertThat(mutation.asMap().get("at").getTimestamp().getNanos()).isEqualTo(123456789);
        assertThat(mutation.asMap().get("payload").getType())
                .isEqualTo(Type.proto("example.Payload"));
        assertThat(mutation.asMap().get("payload").getBytes())
                .isEqualTo(ByteArray.copyFrom(new byte[] {1, 2, 3}));
        assertThat(mutation.asMap().get("labels").getStringArray()).containsExactly("a", "b");
    }

    @Test
    void noPrimaryKeyMakesWritesAppendOnlyInserts() throws Exception {
        Mutation mutation =
                new RowDataSerializationSchema(schema(false), "people")
                        .serialize(row(RowKind.INSERT), null);

        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.INSERT);
    }

    @Test
    void convertsTheRemainingScalarMarkersAndNullArrays() throws Exception {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                                        DataTypes.FIELD("ratio32", DataTypes.FLOAT()),
                                        DataTypes.FIELD("ratio64", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("day", DataTypes.DATE()),
                                        DataTypes.FIELD("document", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "numbers", DataTypes.ARRAY(DataTypes.BIGINT())))
                                .getLogicalType();
        SpannerTableSchemaConverter tableSchema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[0],
                        Dialect.POSTGRESQL,
                        Collections.singletonList("document"),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        GenericRowData row =
                GenericRowData.of(
                        true,
                        1.25F,
                        2.5D,
                        (int) LocalDate.parse("2026-08-11").toEpochDay(),
                        StringData.fromString("{\"ok\":true}"),
                        null);

        Mutation mutation =
                new RowDataSerializationSchema(tableSchema, "values").serialize(row, null);

        assertThat(mutation.asMap().get("enabled").getBool()).isTrue();
        assertThat(mutation.asMap().get("ratio32").getFloat32()).isEqualTo(1.25F);
        assertThat(mutation.asMap().get("ratio64").getFloat64()).isEqualTo(2.5D);
        assertThat(mutation.asMap().get("day").getDate().toString()).isEqualTo("2026-08-11");
        assertThat(mutation.asMap().get("document").getType()).isEqualTo(Type.pgJsonb());
        assertThat(mutation.asMap().get("document").getPgJsonb()).isEqualTo("{\"ok\":true}");
        assertThat(mutation.asMap().get("numbers").isNull()).isTrue();
        assertThat(
                        RowDataToSpannerValueConverter.convert(
                                        GenericRowData.of(2L),
                                        0,
                                        DataTypes.BIGINT().getLogicalType(),
                                        Type.protoEnum("example.State"))
                                .getInt64())
                .isEqualTo(2L);
    }

    @Test
    void deleteCarriesOnlyTheDeclaredPrimaryKey() throws Exception {
        Mutation mutation =
                new RowDataSerializationSchema(schema(true), "people")
                        .serialize(row(RowKind.DELETE), null);

        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.DELETE);
        assertThat(mutation.getKeySet().getKeys()).containsExactly(Key.of(7L));
    }
}
