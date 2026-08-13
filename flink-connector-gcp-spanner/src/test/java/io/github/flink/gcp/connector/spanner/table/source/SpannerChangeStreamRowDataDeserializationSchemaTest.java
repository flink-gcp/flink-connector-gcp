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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.table.ChangeStreamChangelogMode;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamRowDataDeserializationSchemaTest {

    private static final List<DataChangeRecord.ColumnType> COLUMNS =
            Arrays.asList(
                    new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1),
                    new DataChangeRecord.ColumnType("name", "{\"code\":\"STRING\"}", false, 2));

    @Test
    void fullModeEmitsInsertAdjacentUpdatePairAndDelete() throws Exception {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(ChangeStreamChangelogMode.FULL, table(null, "people"));
        List<RowData> rows = new ArrayList<>();
        Collector<RowData> collector = collector(rows);

        deserializer.deserialize(
                record(
                        "people",
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        new Mod("{\"id\":\"1\"}", "{\"name\":\"Ada\"}", null)),
                collector);
        deserializer.deserialize(
                record(
                        "people",
                        ModType.UPDATE,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        new Mod("{\"id\":\"1\"}", "{\"name\":\"Grace\"}", "{\"name\":\"Ada\"}")),
                collector);
        deserializer.deserialize(
                record(
                        "people",
                        ModType.DELETE,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        new Mod("{\"id\":\"1\"}", null, "{\"name\":\"Grace\"}")),
                collector);

        assertThat(rows)
                .extracting(RowData::getRowKind)
                .containsExactly(
                        RowKind.INSERT,
                        RowKind.UPDATE_BEFORE,
                        RowKind.UPDATE_AFTER,
                        RowKind.DELETE);
        assertRow(rows.get(0), 1L, "Ada");
        assertRow(rows.get(1), 1L, "Ada");
        assertRow(rows.get(2), 1L, "Grace");
        assertRow(rows.get(3), 1L, "Grace");
    }

    @Test
    void appendsSelectedMetadataInPlannerOrderAndPreservesModIdentity() throws Exception {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(
                        ChangeStreamChangelogMode.FULL,
                        table(null, "people"),
                        ReadableMetadata.MOD_NUMBER,
                        ReadableMetadata.COMMIT_TIMESTAMP,
                        ReadableMetadata.SEQUENCE,
                        ReadableMetadata.SERVER_TRANSACTION_ID,
                        ReadableMetadata.IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION,
                        ReadableMetadata.TABLE,
                        ReadableMetadata.MOD_TYPE,
                        ReadableMetadata.VALUE_CAPTURE_TYPE,
                        ReadableMetadata.NUMBER_OF_RECORDS_IN_TRANSACTION,
                        ReadableMetadata.NUMBER_OF_PARTITIONS_IN_TRANSACTION,
                        ReadableMetadata.TRANSACTION_TAG,
                        ReadableMetadata.SYSTEM_TRANSACTION);
        List<RowData> rows = new ArrayList<>();

        deserializer.deserialize(
                record(
                        "people",
                        COLUMNS,
                        ModType.UPDATE,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        Arrays.asList(
                                new Mod(
                                        "{\"id\":\"1\"}",
                                        "{\"name\":\"Grace\"}",
                                        "{\"name\":\"Ada\"}"),
                                new Mod(
                                        "{\"id\":\"2\"}",
                                        "{\"name\":\"Lin\"}",
                                        "{\"name\":\"Lynn\"}"))),
                collector(rows));

        assertThat(rows)
                .extracting(RowData::getRowKind)
                .containsExactly(
                        RowKind.UPDATE_BEFORE,
                        RowKind.UPDATE_AFTER,
                        RowKind.UPDATE_BEFORE,
                        RowKind.UPDATE_AFTER);
        assertThat(rows).extracting(row -> row.getInt(2)).containsExactly(0, 0, 1, 1);
        RowData first = rows.get(0);
        assertThat(first.getTimestamp(3, 9).toInstant())
                .isEqualTo(Instant.parse("2026-08-13T00:00:00.123456789Z"));
        assertThat(first.getString(4).toString()).isEqualTo("0001");
        assertThat(first.getString(5).toString()).isEqualTo("tx-1");
        assertThat(first.getBoolean(6)).isTrue();
        assertThat(first.getString(7).toString()).isEqualTo("people");
        assertThat(first.getString(8).toString()).isEqualTo("UPDATE");
        assertThat(first.getString(9).toString()).isEqualTo("NEW_ROW_AND_OLD_VALUES");
        assertThat(first.getLong(10)).isEqualTo(1L);
        assertThat(first.getLong(11)).isEqualTo(1L);
        assertThat(first.getString(12).toString()).isEmpty();
        assertThat(first.getBoolean(13)).isFalse();
    }

    @Test
    void upsertModeEmitsKeyOnlyDeletesAndValidatesTheRuntimePrimaryKey() throws Exception {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(ChangeStreamChangelogMode.UPSERT, table(null, "people"));
        List<RowData> rows = new ArrayList<>();
        Collector<RowData> collector = collector(rows);

        deserializer.deserialize(
                record(
                        "people",
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW,
                        new Mod("{\"id\":\"1\"}", "{\"name\":\"Ada\"}", null)),
                collector);
        deserializer.deserialize(
                record(
                        "people",
                        ModType.UPDATE,
                        ValueCaptureType.NEW_ROW,
                        new Mod("{\"id\":\"2\"}", "{\"name\":null}", null)),
                collector);
        deserializer.deserialize(
                record(
                        "people",
                        ModType.DELETE,
                        ValueCaptureType.NEW_ROW,
                        new Mod("{\"id\":\"2\"}", null, null)),
                collector);

        assertThat(rows)
                .extracting(RowData::getRowKind)
                .containsExactly(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
        assertRow(rows.get(0), 1L, "Ada");
        assertRow(rows.get(1), 2L, null);
        assertRow(rows.get(2), 2L, null);

        List<DataChangeRecord.ColumnType> wrongKey =
                Arrays.asList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", false, 1),
                        new DataChangeRecord.ColumnType("name", "{\"code\":\"STRING\"}", true, 2));
        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                "people",
                                                wrongKey,
                                                ModType.INSERT,
                                                ValueCaptureType.NEW_ROW,
                                                Collections.singletonList(
                                                        new Mod(
                                                                "{\"id\":\"3\"}",
                                                                "{\"name\":\"bad\"}",
                                                                null))),
                                        collector))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("modIndex=-1");
    }

    @Test
    void validatesCompositePrimaryKeysAsASetNotByTableColumnPosition() throws Exception {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new BigIntType(false), new BigIntType(false), new VarCharType()
                        },
                        new String[] {"id", "tenant", "name"});
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {1, 0},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                new SpannerChangeStreamRowDataDeserializationSchema(
                        schema,
                        table(null, "people"),
                        ChangeStreamChangelogMode.UPSERT,
                        TypeInformation.of(RowData.class));
        List<DataChangeRecord.ColumnType> columns =
                Arrays.asList(
                        column("id", "{\"code\":\"INT64\"}", true, 1),
                        column("tenant", "{\"code\":\"INT64\"}", true, 2),
                        column("name", "{\"code\":\"STRING\"}", false, 3));
        List<RowData> rows = new ArrayList<>();

        deserializer.deserialize(
                record(
                        "people",
                        columns,
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW,
                        Collections.singletonList(
                                new Mod(
                                        "{\"id\":\"1\",\"tenant\":\"2\"}",
                                        "{\"name\":\"Ada\"}",
                                        null))),
                collector(rows));

        assertThat(rows).singleElement();
        assertThat(rows.get(0).getLong(0)).isEqualTo(1L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(2L);
    }

    @Test
    void stagesTheWholeRecordBeforeCollectingAndSanitizesFailures() {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(ChangeStreamChangelogMode.FULL, table(null, "people"));
        List<RowData> rows = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                "people",
                                                COLUMNS,
                                                ModType.INSERT,
                                                ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                                                Arrays.asList(
                                                        new Mod(
                                                                "{\"id\":\"1\"}",
                                                                "{\"name\":\"secret-first\"}",
                                                                null),
                                                        new Mod("{\"id\":\"2\"}", "{}", null))),
                                        collector(rows)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("table=people")
                .hasMessageContaining("transaction=tx-1")
                .hasMessageContaining("sequence=0001")
                .hasMessageContaining("modIndex=1")
                .hasMessageNotContaining("secret-first")
                .hasNoCause();
        assertThat(rows).isEmpty();
    }

    @Test
    void filtersByDialectAwareExactTableNameAndRejectsTypeOrCaptureMismatch() throws Exception {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(ChangeStreamChangelogMode.FULL, table("Analytics", "People"));
        List<RowData> rows = new ArrayList<>();

        deserializer.deserialize(
                record(
                        "analytics.people",
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        new Mod("{\"id\":\"1\"}", "{\"name\":\"Ada\"}", null)),
                collector(rows));
        deserializer.deserialize(
                record(
                        "analytics.people_archive",
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        new Mod("{\"id\":\"2\"}", "{\"name\":\"ignored\"}", null)),
                collector(rows));
        assertThat(rows).hasSize(1);

        List<DataChangeRecord.ColumnType> wrongType =
                Arrays.asList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"STRING\"}", true, 1),
                        COLUMNS.get(1));
        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                "analytics.people",
                                                wrongType,
                                                ModType.INSERT,
                                                ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                                                Collections.singletonList(
                                                        new Mod(
                                                                "{\"id\":\"1\"}",
                                                                "{\"name\":\"Ada\"}",
                                                                null))),
                                        collector(rows)))
                .isInstanceOf(IOException.class);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                "analytics.people",
                                                ModType.UPDATE,
                                                ValueCaptureType.NEW_VALUES,
                                                new Mod(
                                                        "{\"id\":\"1\"}",
                                                        "{\"name\":\"Ada\"}",
                                                        null)),
                                        collector(rows)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void ignoresExtraWatchedColumnsAndRejectsRecordsFromAnOlderMissingColumnSchema()
            throws Exception {
        SpannerChangeStreamRowDataDeserializationSchema deserializer =
                deserializer(ChangeStreamChangelogMode.FULL, table(null, "people"));
        List<DataChangeRecord.ColumnType> evolved = new ArrayList<>(COLUMNS);
        evolved.add(new DataChangeRecord.ColumnType("future", "{\"code\":\"STRING\"}", false, 3));
        List<RowData> rows = new ArrayList<>();

        deserializer.deserialize(
                record(
                        "people",
                        evolved,
                        ModType.INSERT,
                        ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                        Collections.singletonList(
                                new Mod(
                                        "{\"id\":\"1\"}",
                                        "{\"name\":\"Ada\",\"future\":\"ignored\"}",
                                        null))),
                collector(rows));
        assertThat(rows).hasSize(1);
        assertRow(rows.get(0), 1L, "Ada");

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                "people",
                                                Collections.singletonList(COLUMNS.get(0)),
                                                ModType.INSERT,
                                                ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                                                Collections.singletonList(
                                                        new Mod(
                                                                "{\"id\":\"2\"}",
                                                                "{\"name\":\"Grace\"}",
                                                                null))),
                                        collector(rows)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("modIndex=-1");
    }

    @Test
    void convertsEveryGoogleSqlPhysicalTypeFromItsChangeStreamJsonEncoding() throws Exception {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                                        DataTypes.FIELD("ratio32", DataTypes.FLOAT()),
                                        DataTypes.FIELD("ratio64", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 9)),
                                        DataTypes.FIELD("text", DataTypes.STRING()),
                                        DataTypes.FIELD("document", DataTypes.STRING()),
                                        DataTypes.FIELD("external_id", DataTypes.STRING()),
                                        DataTypes.FIELD("raw", DataTypes.BYTES()),
                                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                                        DataTypes.FIELD("state", DataTypes.BIGINT()),
                                        DataTypes.FIELD("day", DataTypes.DATE()),
                                        DataTypes.FIELD("at", DataTypes.TIMESTAMP_LTZ(9)),
                                        DataTypes.FIELD(
                                                "numbers", DataTypes.ARRAY(DataTypes.BIGINT())),
                                        DataTypes.FIELD("missing", DataTypes.STRING()))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {0},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.singletonList("document"),
                        Collections.singletonList("external_id"),
                        Collections.singletonMap("payload", "example.Payload"),
                        Collections.singletonMap("state", "example.State"));
        List<DataChangeRecord.ColumnType> columns =
                Arrays.asList(
                        column("id", "{\"code\":\"INT64\"}", true, 1),
                        column("enabled", "{\"code\":\"BOOL\"}", false, 2),
                        column("ratio32", "{\"code\":\"FLOAT32\"}", false, 3),
                        column("ratio64", "{\"code\":\"FLOAT64\"}", false, 4),
                        column("amount", "{\"code\":\"NUMERIC\"}", false, 5),
                        column("text", "{\"code\":\"STRING\"}", false, 6),
                        column("document", "{\"code\":\"JSON\"}", false, 7),
                        column("external_id", "{\"code\":\"UUID\"}", false, 8),
                        column("raw", "{\"code\":\"BYTES\"}", false, 9),
                        column(
                                "payload",
                                "{\"code\":\"PROTO\",\"proto_type_fqn\":\"example.Payload\"}",
                                false,
                                10),
                        column(
                                "state",
                                "{\"code\":\"ENUM\",\"proto_type_fqn\":\"example.State\"}",
                                false,
                                11),
                        column("day", "{\"code\":\"DATE\"}", false, 12),
                        column("at", "{\"code\":\"TIMESTAMP\"}", false, 13),
                        column(
                                "numbers",
                                "{\"code\":\"ARRAY\",\"array_element_type\":{\"code\":\"INT64\"}}",
                                false,
                                14),
                        column("missing", "{\"code\":\"STRING\"}", false, 15));
        String values =
                "{\"enabled\":true,\"ratio32\":\"1.25\",\"ratio64\":\"-Infinity\","
                        + "\"amount\":\"12.340000000\",\"text\":\"hello\","
                        + "\"document\":\"{\\\"ok\\\":true}\","
                        + "\"external_id\":\"f81d4fae-7dec-11d0-a765-00a0c91e6bf6\","
                        + "\"raw\":\"AQI=\",\"payload\":\"AwQ=\",\"state\":\"2\","
                        + "\"day\":\"2026-08-13\",\"at\":\"2026-08-13T01:02:03.123456789Z\","
                        + "\"numbers\":[\"5\",null,\"8\"],\"missing\":null}";
        List<RowData> rows = new ArrayList<>();

        new SpannerChangeStreamRowDataDeserializationSchema(
                        schema,
                        table(null, "people"),
                        ChangeStreamChangelogMode.UPSERT,
                        TypeInformation.of(RowData.class))
                .deserialize(
                        record(
                                "people",
                                columns,
                                ModType.INSERT,
                                ValueCaptureType.NEW_ROW,
                                Collections.singletonList(new Mod("{\"id\":\"7\"}", values, null))),
                        collector(rows));

        RowData row = rows.get(0);
        assertThat(row.getLong(0)).isEqualTo(7L);
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getFloat(2)).isEqualTo(1.25F);
        assertThat(row.getDouble(3)).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(row.getDecimal(4, 38, 9))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("12.340000000"), 38, 9));
        assertThat(row.getString(5).toString()).isEqualTo("hello");
        assertThat(row.getString(6).toString()).isEqualTo("{\"ok\":true}");
        assertThat(row.getString(7).toString()).isEqualTo("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
        assertThat(row.getBinary(8)).containsExactly(1, 2);
        assertThat(row.getBinary(9)).containsExactly(3, 4);
        assertThat(row.getLong(10)).isEqualTo(2L);
        assertThat(row.getInt(11)).isEqualTo((int) LocalDate.parse("2026-08-13").toEpochDay());
        assertThat(row.getTimestamp(12, 9).toInstant())
                .isEqualTo(java.time.Instant.parse("2026-08-13T01:02:03.123456789Z"));
        ArrayData numbers = row.getArray(13);
        assertThat(numbers.getLong(0)).isEqualTo(5L);
        assertThat(numbers.isNullAt(1)).isTrue();
        assertThat(numbers.getLong(2)).isEqualTo(8L);
        assertThat(row.isNullAt(14)).isTrue();
    }

    @Test
    void convertsPostgresqlAnnotatedNumericAndJsonbDescriptors() throws Exception {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)),
                                        DataTypes.FIELD("document", DataTypes.STRING()))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {0},
                        Dialect.POSTGRESQL,
                        Collections.singletonList("document"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        List<DataChangeRecord.ColumnType> columns =
                Arrays.asList(
                        column("id", "{\"code\":\"INT64\"}", true, 1),
                        column(
                                "amount",
                                "{\"code\":\"NUMERIC\",\"type_annotation\":\"PG_NUMERIC\"}",
                                false,
                                2),
                        column(
                                "document",
                                "{\"code\":\"JSON\",\"type_annotation\":\"PG_JSONB\"}",
                                false,
                                3));
        List<RowData> rows = new ArrayList<>();

        new SpannerChangeStreamRowDataDeserializationSchema(
                        schema,
                        SpannerTableName.of(null, "people", Dialect.POSTGRESQL),
                        ChangeStreamChangelogMode.UPSERT,
                        TypeInformation.of(RowData.class))
                .deserialize(
                        record(
                                "people",
                                columns,
                                ModType.INSERT,
                                ValueCaptureType.NEW_ROW,
                                Collections.singletonList(
                                        new Mod(
                                                "{\"id\":\"1\"}",
                                                "{\"amount\":\"12.30\",\"document\":\"{\\\"ok\\\":true}\"}",
                                                null))),
                        collector(rows));

        assertThat(rows.get(0).getDecimal(1, 5, 2).toBigDecimal()).isEqualByComparingTo("12.30");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("{\"ok\":true}");
    }

    @Test
    void convertsEveryGoogleSqlArrayEncodingIncludingNullArraysAndElements() throws Exception {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "bools", DataTypes.ARRAY(DataTypes.BOOLEAN())),
                                        DataTypes.FIELD(
                                                "ints", DataTypes.ARRAY(DataTypes.BIGINT())),
                                        DataTypes.FIELD(
                                                "floats", DataTypes.ARRAY(DataTypes.FLOAT())),
                                        DataTypes.FIELD(
                                                "doubles", DataTypes.ARRAY(DataTypes.DOUBLE())),
                                        DataTypes.FIELD(
                                                "numerics",
                                                DataTypes.ARRAY(DataTypes.DECIMAL(38, 9))),
                                        DataTypes.FIELD(
                                                "strings", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD(
                                                "jsons", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD(
                                                "uuids", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD(
                                                "bytes", DataTypes.ARRAY(DataTypes.BYTES())),
                                        DataTypes.FIELD(
                                                "protos", DataTypes.ARRAY(DataTypes.BYTES())),
                                        DataTypes.FIELD(
                                                "enums", DataTypes.ARRAY(DataTypes.BIGINT())),
                                        DataTypes.FIELD(
                                                "timestamps",
                                                DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(9))),
                                        DataTypes.FIELD("dates", DataTypes.ARRAY(DataTypes.DATE())))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {0},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.singletonList("jsons[]"),
                        Collections.singletonList("uuids[]"),
                        Collections.singletonMap("protos[]", "example.Payload"),
                        Collections.singletonMap("enums[]", "example.State"));
        List<DataChangeRecord.ColumnType> columns =
                Arrays.asList(
                        column("id", "{\"code\":\"INT64\"}", true, 1),
                        arrayColumn("bools", "{\"code\":\"BOOL\"}", 2),
                        arrayColumn("ints", "{\"code\":\"INT64\"}", 3),
                        arrayColumn("floats", "{\"code\":\"FLOAT32\"}", 4),
                        arrayColumn("doubles", "{\"code\":\"FLOAT64\"}", 5),
                        arrayColumn("numerics", "{\"code\":\"NUMERIC\"}", 6),
                        arrayColumn("strings", "{\"code\":\"STRING\"}", 7),
                        arrayColumn("jsons", "{\"code\":\"JSON\"}", 8),
                        arrayColumn("uuids", "{\"code\":\"UUID\"}", 9),
                        arrayColumn("bytes", "{\"code\":\"BYTES\"}", 10),
                        arrayColumn(
                                "protos",
                                "{\"code\":\"PROTO\",\"proto_type_fqn\":\"example.Payload\"}",
                                11),
                        arrayColumn(
                                "enums",
                                "{\"code\":\"ENUM\",\"proto_type_fqn\":\"example.State\"}",
                                12),
                        arrayColumn("timestamps", "{\"code\":\"TIMESTAMP\"}", 13),
                        arrayColumn("dates", "{\"code\":\"DATE\"}", 14));
        String populated =
                "{\"bools\":[true,null],\"ints\":[\"1\",null],"
                        + "\"floats\":[\"1.25\",null],\"doubles\":[\"-Infinity\",null],"
                        + "\"numerics\":[\"12.340000000\",null],\"strings\":[\"x\",null],"
                        + "\"jsons\":[\"{\\\"ok\\\":true}\",null],"
                        + "\"uuids\":[\"f81d4fae-7dec-11d0-a765-00a0c91e6bf6\",null],"
                        + "\"bytes\":[\"AQI=\",null],\"protos\":[\"AwQ=\",null],"
                        + "\"enums\":[\"2\",null],"
                        + "\"timestamps\":[\"2026-08-13T01:02:03.123456789Z\",null],"
                        + "\"dates\":[\"2026-08-13\",null]}";
        String allNull =
                "{\"bools\":null,\"ints\":null,\"floats\":null,\"doubles\":null,"
                        + "\"numerics\":null,\"strings\":null,\"jsons\":null,"
                        + "\"uuids\":null,\"bytes\":null,\"protos\":null,\"enums\":null,"
                        + "\"timestamps\":null,\"dates\":null}";
        List<RowData> rows = new ArrayList<>();

        new SpannerChangeStreamRowDataDeserializationSchema(
                        schema,
                        table(null, "people"),
                        ChangeStreamChangelogMode.UPSERT,
                        TypeInformation.of(RowData.class))
                .deserialize(
                        record(
                                "people",
                                columns,
                                ModType.INSERT,
                                ValueCaptureType.NEW_ROW,
                                Arrays.asList(
                                        new Mod("{\"id\":\"7\"}", populated, null),
                                        new Mod("{\"id\":\"8\"}", allNull, null))),
                        collector(rows));

        assertPopulatedArrays(rows.get(0), rowType);
        for (int index = 1; index < rowType.getFieldCount(); index++) {
            assertThat(rows.get(1).isNullAt(index)).as(rowType.getFieldNames().get(index)).isTrue();
        }
    }

    @Test
    void convertsPostgresqlArrayEncodingsIncludingNullArraysAndElements() throws Exception {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "numerics",
                                                DataTypes.ARRAY(DataTypes.DECIMAL(5, 2))),
                                        DataTypes.FIELD(
                                                "jsons", DataTypes.ARRAY(DataTypes.STRING())))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {0},
                        Dialect.POSTGRESQL,
                        Collections.singletonList("jsons[]"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        List<DataChangeRecord.ColumnType> columns =
                Arrays.asList(
                        column("id", "{\"code\":\"INT64\"}", true, 1),
                        arrayColumn(
                                "numerics",
                                "{\"code\":\"NUMERIC\",\"type_annotation\":\"PG_NUMERIC\"}",
                                2),
                        arrayColumn(
                                "jsons",
                                "{\"code\":\"JSON\",\"type_annotation\":\"PG_JSONB\"}",
                                3));
        List<RowData> rows = new ArrayList<>();

        new SpannerChangeStreamRowDataDeserializationSchema(
                        schema,
                        SpannerTableName.of(null, "people", Dialect.POSTGRESQL),
                        ChangeStreamChangelogMode.UPSERT,
                        TypeInformation.of(RowData.class))
                .deserialize(
                        record(
                                "people",
                                columns,
                                ModType.INSERT,
                                ValueCaptureType.NEW_ROW,
                                Arrays.asList(
                                        new Mod(
                                                "{\"id\":\"1\"}",
                                                "{\"numerics\":[\"12.30\",null],\"jsons\":[\"{\\\"ok\\\":true}\",null]}",
                                                null),
                                        new Mod(
                                                "{\"id\":\"2\"}",
                                                "{\"numerics\":null,\"jsons\":null}",
                                                null))),
                        collector(rows));

        assertPopulatedArrays(rows.get(0), rowType);
        assertThat(rows.get(1).isNullAt(1)).isTrue();
        assertThat(rows.get(1).isNullAt(2)).isTrue();
    }

    private static DataChangeRecord.ColumnType arrayColumn(
            String name, String elementDescriptor, long ordinal) {
        return column(
                name,
                "{\"code\":\"ARRAY\",\"array_element_type\":" + elementDescriptor + "}",
                false,
                ordinal);
    }

    private static void assertPopulatedArrays(RowData row, RowType rowType) {
        for (int index = 1; index < rowType.getFieldCount(); index++) {
            ArrayData array = row.getArray(index);
            assertThat(array.size()).as(rowType.getFieldNames().get(index)).isEqualTo(2);
            org.apache.flink.table.types.logical.LogicalType elementType =
                    ((ArrayType) rowType.getTypeAt(index)).getElementType();
            assertThat(ArrayData.createElementGetter(elementType).getElementOrNull(array, 0))
                    .as(rowType.getFieldNames().get(index))
                    .isNotNull();
            assertThat(array.isNullAt(1)).as(rowType.getFieldNames().get(index)).isTrue();
        }
    }

    private static DataChangeRecord.ColumnType column(
            String name, String descriptor, boolean key, long ordinal) {
        return new DataChangeRecord.ColumnType(name, descriptor, key, ordinal);
    }

    private static SpannerChangeStreamRowDataDeserializationSchema deserializer(
            ChangeStreamChangelogMode mode, SpannerTableName table) {
        return deserializer(mode, table, new ReadableMetadata[0]);
    }

    private static SpannerChangeStreamRowDataDeserializationSchema deserializer(
            ChangeStreamChangelogMode mode, SpannerTableName table, ReadableMetadata... metadata) {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new BigIntType(false), new VarCharType()
                        },
                        new String[] {"id", "name"});
        return new SpannerChangeStreamRowDataDeserializationSchema(
                SpannerTableSchemaConverter.of(
                        rowType,
                        new int[] {0},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap()),
                table,
                mode,
                metadata,
                TypeInformation.of(RowData.class));
    }

    private static SpannerTableName table(String schema, String table) {
        return SpannerTableName.of(schema, table, Dialect.GOOGLE_STANDARD_SQL);
    }

    private static DataChangeRecord record(
            String table, ModType type, ValueCaptureType capture, Mod mod) {
        return record(table, COLUMNS, type, capture, Collections.singletonList(mod));
    }

    private static DataChangeRecord record(
            String table,
            List<DataChangeRecord.ColumnType> columns,
            ModType type,
            ValueCaptureType capture,
            List<Mod> mods) {
        return new DataChangeRecord(
                Instant.parse("2026-08-13T00:00:00.123456789Z"),
                "0001",
                "tx-1",
                true,
                table,
                columns,
                mods,
                type,
                capture,
                1,
                1,
                "",
                false);
    }

    private static void assertRow(RowData row, long id, String name) {
        GenericRowData generic = (GenericRowData) row;
        assertThat(generic.getField(0)).isEqualTo(id);
        assertThat(generic.getField(1))
                .isEqualTo(name == null ? null : StringData.fromString(name));
    }

    private static Collector<RowData> collector(List<RowData> rows) {
        return new Collector<RowData>() {
            @Override
            public void collect(RowData record) {
                rows.add(record);
            }

            @Override
            public void close() {}
        };
    }
}
