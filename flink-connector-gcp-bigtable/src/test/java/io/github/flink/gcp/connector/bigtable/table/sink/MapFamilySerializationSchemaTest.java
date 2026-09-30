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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for writing a {@code MAP} column family: one cell per entry, merged or replaced. */
class MapFamilySerializationSchemaTest {

    /** A ROW family, a STRING-keyed map of strings, and a BYTES-keyed map of BIGINT values. */
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))),
                                            DataTypes.FIELD(
                                                    "m",
                                                    DataTypes.MAP(
                                                            DataTypes.STRING(),
                                                            DataTypes.STRING())),
                                            DataTypes.FIELD(
                                                    "n",
                                                    DataTypes.MAP(
                                                            DataTypes.BYTES(), DataTypes.BIGINT())))
                                    .getLogicalType());

    private static final WritableMetadata[] NO_METADATA = {};

    private static final WritableMetadata[] WITH_TIMESTAMP = {WritableMetadata.TIMESTAMP};

    private static RowDataSerializationSchema serializer(boolean keepLatest, boolean replace) {
        return new RowDataSerializationSchema(
                SCHEMA, "NULL", NO_METADATA, false, keepLatest, false, replace);
    }

    private static RowData row(Object cf, Object m, Object n) {
        GenericRowData row = GenericRowData.of(StringData.fromString("r1"), cf, m, n);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static GenericMapData strings(String... keysAndValues) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(
                    StringData.fromString(keysAndValues[i]),
                    keysAndValues[i + 1] == null
                            ? null
                            : StringData.fromString(keysAndValues[i + 1]));
        }
        return new GenericMapData(map);
    }

    /** Every mutation of the entry in order, as one readable line each. */
    private static List<String> mutations(MutateRowsRequest.Entry entry) {
        return entry.getMutationsList().stream()
                .map(MapFamilySerializationSchemaTest::describe)
                .collect(Collectors.toList());
    }

    private static String describe(Mutation mutation) {
        switch (mutation.getMutationCase()) {
            case SET_CELL:
                return "set "
                        + mutation.getSetCell().getFamilyName()
                        + ':'
                        + hex(mutation.getSetCell().getColumnQualifier())
                        + '='
                        + hex(mutation.getSetCell().getValue());
            case DELETE_FROM_COLUMN:
                return "delete-column "
                        + mutation.getDeleteFromColumn().getFamilyName()
                        + ':'
                        + hex(mutation.getDeleteFromColumn().getColumnQualifier());
            case DELETE_FROM_FAMILY:
                return "delete-family " + mutation.getDeleteFromFamily().getFamilyName();
            default:
                return mutation.getMutationCase().toString();
        }
    }

    private static String hex(ByteString bytes) {
        StringBuilder out = new StringBuilder();
        for (byte b : bytes.toByteArray()) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    private static String hex(String utf8) {
        return hex(ByteString.copyFromUtf8(utf8));
    }

    @Test
    void mergeWritesOneCellPerEntryAndNothingElse() throws Exception {
        Map<Object, Object> counts = new HashMap<>();
        counts.put(new byte[] {0x00, (byte) 0xff}, 5L);
        MutateRowsRequest.Entry entry =
                serializer(false, false)
                        .serialize(
                                row(null, strings("a", "x", "b", null), new GenericMapData(counts)),
                                null)
                        .toProto();

        // A key is the qualifier's bytes as they are; a value is encoded as a ROW qualifier of
        // the value type is — BIGINT as eight big-endian bytes, a null string as the literal.
        assertThat(mutations(entry))
                .containsExactly(
                        "set m:" + hex("a") + "=" + hex("x"),
                        "set m:" + hex("b") + "=" + hex("NULL"),
                        "set n:00ff=0000000000000005");
    }

    @Test
    void replaceDeletesTheFamilyBeforeWritingItsEntries() throws Exception {
        MutateRowsRequest.Entry entry =
                serializer(false, true)
                        .serialize(row(null, strings("a", "x"), null), null)
                        .toProto();

        // In this order and in one entry: a row entry's mutations apply in order and atomically,
        // so the family reads back as exactly the written map.
        assertThat(mutations(entry))
                .containsExactly("delete-family m", "set m:" + hex("a") + "=" + hex("x"));
    }

    @Test
    void replaceWithAnEmptyMapClearsTheFamily() throws Exception {
        MutateRowsRequest.Entry entry =
                serializer(false, true).serialize(row(null, strings(), null), null).toProto();

        assertThat(mutations(entry)).containsExactly("delete-family m");
    }

    @Test
    void mergeWithNothingButAnEmptyMapIsRejectedRatherThanSentEmpty() {
        assertThatThrownBy(
                        () -> serializer(false, false).serialize(row(null, strings(), null), null))
                .hasMessageContaining("is null or an empty map")
                .hasMessageContaining("would carry no cell");
    }

    @Test
    void aNullMapLeavesItsFamilyAloneEvenWhenReplacing() throws Exception {
        MutateRowsRequest.Entry entry =
                serializer(false, true)
                        .serialize(
                                row(GenericRowData.of(StringData.fromString("v")), null, null),
                                null)
                        .toProto();

        assertThat(mutations(entry)).containsExactly("set cf:" + hex("q") + "=" + hex("v"));
    }

    @Test
    void aNullKeyIsRejectedNamingTheFamilyAndTheRow() {
        Map<Object, Object> map = new HashMap<>();
        map.put(null, StringData.fromString("x"));

        assertThatThrownBy(
                        () ->
                                serializer(false, false)
                                        .serialize(row(null, new GenericMapData(map), null), null))
                .hasMessageContaining("MAP column family 'm' of the row with key 'r1'")
                .hasMessageContaining("null key");
    }

    @Test
    void keepLatestDeletesEachWrittenColumnUnderMerge() throws Exception {
        MutateRowsRequest.Entry entry =
                serializer(true, false)
                        .serialize(row(null, strings("a", "x"), null), null)
                        .toProto();

        assertThat(mutations(entry))
                .containsExactly(
                        "delete-column m:" + hex("a"), "set m:" + hex("a") + "=" + hex("x"));
    }

    @Test
    void keepLatestAddsNoColumnDeleteBehindAFamilyDelete() throws Exception {
        MutateRowsRequest.Entry entry =
                serializer(true, true)
                        .serialize(
                                row(
                                        GenericRowData.of(StringData.fromString("v")),
                                        strings("a", "x"),
                                        null),
                                null)
                        .toProto();

        // The ROW family keeps its keep-latest column delete; the replaced map needs none.
        assertThat(mutations(entry))
                .containsExactly(
                        "delete-column cf:" + hex("q"),
                        "set cf:" + hex("q") + "=" + hex("v"),
                        "delete-family m",
                        "set m:" + hex("a") + "=" + hex("x"));
    }

    @Test
    void everyEntryTakesTheRowsTimestamp() throws Exception {
        RowDataSerializationSchema withMetadata =
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, false);
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("r1"),
                        null,
                        strings("a", "x", "b", "y"),
                        null,
                        TimestampData.fromEpochMillis(1_234L));
        row.setRowKind(RowKind.INSERT);

        MutateRowsRequest.Entry entry = withMetadata.serialize(row, null).toProto();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsExactly(1_234_000L, 1_234_000L);
    }

    @Test
    void theWriterClockIsReadOncePerMapEntry() throws Exception {
        long[] next = {5_000L};
        RowDataSerializationSchema.CellClock clock =
                new RowDataSerializationSchema.CellClock() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public long micros() {
                        long value = next[0];
                        next[0] += 1_000L;
                        return value;
                    }
                };
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", NO_METADATA, false, false, clock);

        MutateRowsRequest.Entry entry =
                serializer.serialize(row(null, strings("a", "x", "b", "y"), null), null).toProto();

        // ADR-0149's per-cell stamping, as a ROW family's qualifiers take it.
        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsExactly(5_000L, 6_000L);
    }

    @Test
    void aStagedDeleteRemovesTheMapFamilyAmongTheDeclaredOnes() throws Exception {
        RowDataSerializationSchema staged =
                new RowDataSerializationSchema(
                        SCHEMA, "NULL", NO_METADATA, false, false, true, false);
        GenericRowData row = GenericRowData.of(StringData.fromString("r1"), null, null, null);
        row.setRowKind(RowKind.DELETE);

        assertThat(mutations(staged.serialize(row, null).toProto()))
                .containsExactly("delete-family cf", "delete-family m", "delete-family n");
    }

    @Test
    void aStagedReplaceDeletesTheFamilyBeforeWritingItsEntries() throws Exception {
        // The staged runtime accepts DeleteFromFamily on a data family (ADR-0163), so replace
        // takes the same shape there.
        RowDataSerializationSchema staged =
                new RowDataSerializationSchema(
                        SCHEMA, "NULL", NO_METADATA, false, false, true, true);

        assertThat(mutations(staged.serialize(row(null, strings("a", "x"), null), null).toProto()))
                .containsExactly("delete-family m", "set m:" + hex("a") + "=" + hex("x"));
    }

    @Test
    void insertIfAbsentWritesOneCellPerEntryAndNeverDeletesTheFamily() throws Exception {
        // The request's row is absent, so there is nothing to replace: this path never passes the
        // replace flag on, and a family delete here would throw.
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", NO_METADATA, false);

        assertThat(
                        serializer
                                .insertIfAbsent(row(null, strings("a", "x", "b", "y"), null))
                                .getOtherwiseMutations())
                .hasSize(2);
    }

    @Test
    void anEntryOverTheClientsMutationLimitFailsInsideSerialize() throws Exception {
        // A map makes the mutations per entry data-dependent. The client's Mutation refuses the
        // 100,001st as it is added — inside serialize(), which the writer routes as a
        // serialization failure — so an entry at the bound is the largest that builds, and its
        // proto still passes RowMutationEntry.toProto()'s own check.
        int limit = 100_000;
        assertThat(
                        serializer(false, false)
                                .serialize(row(null, numbered(limit), null), null)
                                .toProto()
                                .getMutationsCount())
                .isEqualTo(limit);

        assertThatThrownBy(
                        () ->
                                serializer(false, false)
                                        .serialize(row(null, numbered(limit + 1), null), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Too many mutations");
        // Replace spends one family delete and no column deletes, keep-latest or not.
        assertThat(
                        serializer(true, true)
                                .serialize(row(null, numbered(limit - 1), null), null)
                                .toProto()
                                .getMutationsCount())
                .isEqualTo(limit);
        // Under merge, keep-latest adds a column delete per entry, so half as many entries reach
        // the bound.
        assertThatThrownBy(
                        () ->
                                serializer(true, false)
                                        .serialize(row(null, numbered(limit / 2 + 1), null), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Too many mutations");
    }

    private static GenericMapData numbered(int entries) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < entries; i++) {
            map.put(StringData.fromString("q" + i), StringData.fromString("v"));
        }
        return new GenericMapData(map);
    }

    @Test
    void crossesTheJobGraphWithoutItsCodecLambdas() throws Exception {
        RowDataSerializationSchema serializer = serializer(false, true);
        RowData row = row(null, strings("a", "x"), null);

        byte[] serialized = InstantiationUtil.serializeObject(serializer);

        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataSerializationSchema restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(mutations(restored.serialize(row, null).toProto()))
                .isEqualTo(mutations(serializer.serialize(row, null).toProto()));
    }
}
