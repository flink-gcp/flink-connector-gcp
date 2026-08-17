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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for turning a changelog row into one Bigtable mutation. */
class RowDataSerializationSchemaTest {

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf1",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q1", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "q2", DataTypes.BIGINT()))),
                                            DataTypes.FIELD(
                                                    "cf2",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "flag", DataTypes.BOOLEAN()))))
                                    .getLogicalType());

    private static final RowDataSerializationSchema SERIALIZER =
            new RowDataSerializationSchema(SCHEMA, "NULL", false, false);

    private static RowData row(RowKind kind, Object key, Object cf1, Object cf2) {
        GenericRowData row = GenericRowData.of(key, cf1, cf2);
        row.setRowKind(kind);
        return row;
    }

    @Test
    void crossesTheJobGraphWithoutItsCodecLambdas() throws Exception {
        // This schema is what carries a CellValueCodec into the job graph, so the codec's own
        // guard is half the statement: a restored schema must still write the same mutation. A
        // lambda here would be rebound by a synthetic-method name the compiler picks, and the
        // measured consequence was a BIGINT cell silently written as the four bytes of an INT.
        RowData row =
                row(
                        RowKind.INSERT,
                        StringData.fromString("r1"),
                        GenericRowData.of(StringData.fromString("v"), 1L),
                        GenericRowData.of(true));

        byte[] serialized = InstantiationUtil.serializeObject(SERIALIZER);

        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataSerializationSchema restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        // Cell by cell, not whole protos: setCell stamps timestamp_micros from the clock, so two
        // calls differ by construction — the same reason cells() exists for the tests below.
        assertThat(cells(restored.serialize(row, null).toProto()))
                .isEqualTo(cells(SERIALIZER.serialize(row, null).toProto()));
    }

    private static MutateRowsRequest.Entry serialize(RowData row) throws Exception {
        RowMutationEntry entry = SERIALIZER.serialize(row, null);
        assertThat(entry).isNotNull();
        return entry.toProto();
    }

    private static RowData rowWithTimestamp(RowKind kind, Object timestamp) {
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("r1"),
                        GenericRowData.of(StringData.fromString("v"), 7L),
                        GenericRowData.of(true),
                        timestamp);
        row.setRowKind(kind);
        return row;
    }

    @Test
    void anInsertWritesEveryDeclaredQualifier() throws Exception {
        MutateRowsRequest.Entry entry =
                serialize(
                        row(
                                RowKind.INSERT,
                                StringData.fromString("r1"),
                                GenericRowData.of(StringData.fromString("v"), 7L),
                                GenericRowData.of(true)));

        assertThat(entry.getRowKey()).isEqualTo(ByteString.copyFromUtf8("r1"));
        List<Mutation> mutations = entry.getMutationsList();
        assertThat(mutations).hasSize(3);
        assertThat(mutations)
                .allSatisfy(mutation -> assertThat(mutation.hasSetCell()).isTrue())
                .extracting(m -> m.getSetCell().getFamilyName())
                .containsExactly("cf1", "cf1", "cf2");
        assertThat(mutations)
                .extracting(m -> m.getSetCell().getColumnQualifier().toStringUtf8())
                .containsExactly("q1", "q2", "flag");
        assertThat(mutations.get(0).getSetCell().getValue())
                .isEqualTo(ByteString.copyFromUtf8("v"));
        assertThat(mutations.get(1).getSetCell().getValue())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 7}));
        assertThat(mutations.get(2).getSetCell().getValue())
                .isEqualTo(ByteString.copyFrom(new byte[] {(byte) 0xff}));
    }

    @Test
    void anUpdateAfterWritesTheSameCellsAsAnInsert() throws Exception {
        MutateRowsRequest.Entry insert =
                serialize(
                        row(
                                RowKind.INSERT,
                                StringData.fromString("r1"),
                                GenericRowData.of(StringData.fromString("v"), 7L),
                                GenericRowData.of(true)));
        MutateRowsRequest.Entry update =
                serialize(
                        row(
                                RowKind.UPDATE_AFTER,
                                StringData.fromString("r1"),
                                GenericRowData.of(StringData.fromString("v"), 7L),
                                GenericRowData.of(true)));

        // Compared cell by cell rather than as whole protos: setCell stamps timestamp_micros
        // from the wall clock, so two serialize calls straddling a millisecond differ in a field
        // neither row kind controls. Measured as a real flake — 2 failures in 15 runs — before
        // this narrowed to what the test actually claims.
        assertThat(cells(update)).isEqualTo(cells(insert));
    }

    @Test
    void explicitTimestampMetadataIsAppliedToEveryCellAtMicrosecondPrecision() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", true, false);

        MutateRowsRequest.Entry entry =
                serializer
                        .serialize(
                                rowWithTimestamp(
                                        RowKind.INSERT,
                                        TimestampData.fromEpochMillis(1_700L, 123_456)),
                                null)
                        .toProto();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsOnly(1_700_123L)
                .hasSize(3);
    }

    @Test
    void truncationDropsOnlyTheSubMillisecondPartOfExplicitMetadata() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", true, true);

        MutateRowsRequest.Entry entry =
                serializer
                        .serialize(
                                rowWithTimestamp(
                                        RowKind.INSERT,
                                        TimestampData.fromEpochMillis(1_700L, 123_456)),
                                null)
                        .toProto();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsOnly(1_700_000L);
    }

    @Test
    void nullTimestampMetadataKeepsTheWriterClockPath() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", true, true);

        long beforeMillis = System.currentTimeMillis();
        MutateRowsRequest.Entry entry =
                serializer.serialize(rowWithTimestamp(RowKind.INSERT, null), null).toProto();
        long afterMillis = System.currentTimeMillis();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .allSatisfy(
                        timestamp ->
                                assertThat(timestamp)
                                        .isBetween(beforeMillis * 1_000L, afterMillis * 1_000L));
        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros() % 1_000L)
                .containsOnly(0L);
    }

    @Test
    void aDeleteIgnoresTimestampMetadata() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", true, false);

        MutateRowsRequest.Entry entry =
                serializer
                        .serialize(rowWithTimestamp(RowKind.DELETE, "not-a-timestamp"), null)
                        .toProto();

        assertThat(entry.getMutationsList())
                .singleElement()
                .satisfies(m -> assertThat(m.hasDeleteFromRow()).isTrue());
    }

    @Test
    void timestampMetadataOutsideEpochMicrosecondsIsRejected() {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", true, false);

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        rowWithTimestamp(
                                                RowKind.INSERT,
                                                TimestampData.fromEpochMillis(Long.MAX_VALUE)),
                                        null))
                .hasMessageContaining("'timestamp' metadata value")
                .hasMessageContaining("outside the range of epoch microseconds");
    }

    private static List<String> cells(MutateRowsRequest.Entry entry) {
        return entry.getMutationsList().stream()
                .map(Mutation::getSetCell)
                .map(
                        cell ->
                                cell.getFamilyName()
                                        + ':'
                                        + cell.getColumnQualifier().toStringUtf8()
                                        + '='
                                        + cell.getValue().toStringUtf8())
                .collect(Collectors.toList());
    }

    @Test
    void aNullCellIsWrittenRatherThanSkipped() throws Exception {
        // Written, because a qualifier left alone keeps whatever an earlier version of the row put
        // there — which is not what "this column is null now" means.
        MutateRowsRequest.Entry entry =
                serialize(
                        row(
                                RowKind.INSERT,
                                StringData.fromString("r1"),
                                GenericRowData.of(null, null),
                                GenericRowData.of((Object) null)));

        assertThat(entry.getMutationsList()).hasSize(3);
        assertThat(entry.getMutationsList().get(0).getSetCell().getValue())
                .as("a null character string takes the null-string-literal")
                .isEqualTo(ByteString.copyFromUtf8("NULL"));
        assertThat(entry.getMutationsList().get(1).getSetCell().getValue()).isEmpty();
        assertThat(entry.getMutationsList().get(2).getSetCell().getValue()).isEmpty();
    }

    @Test
    void aRowWhoseEveryFamilyIsNullIsRejectedRatherThanSentEmpty() {
        // Reached by an ordinary partial column list — INSERT INTO bt (rowkey) VALUES (...) — and
        // by a LEFT JOIN that produces no match. The entry would carry no mutation, which the
        // service answers with an INVALID_ARGUMENT naming neither the row nor the reason.
        assertThatThrownBy(
                        () ->
                                SERIALIZER.serialize(
                                        row(
                                                RowKind.INSERT,
                                                StringData.fromString("r1"),
                                                null,
                                                null),
                                        null))
                .hasMessageContaining("Every column family of the row with key 'r1' is null")
                .hasMessageContaining("would carry no cell");
    }

    @Test
    void aNullColumnFamilyWritesNoCellsForThatFamily() throws Exception {
        MutateRowsRequest.Entry entry =
                serialize(
                        row(
                                RowKind.INSERT,
                                StringData.fromString("r1"),
                                null,
                                GenericRowData.of(true)));

        assertThat(entry.getMutationsList())
                .extracting(m -> m.getSetCell().getFamilyName())
                .containsExactly("cf2");
    }

    @Test
    void aDeleteRemovesTheWholeRow() throws Exception {
        // Not the declared qualifiers one by one: the row key is the primary key, so a -D says the
        // key is gone, and deleting only the declared cells would leave the rest of the row behind.
        MutateRowsRequest.Entry entry =
                serialize(row(RowKind.DELETE, StringData.fromString("r1"), null, null));

        assertThat(entry.getRowKey()).isEqualTo(ByteString.copyFromUtf8("r1"));
        assertThat(entry.getMutationsList()).hasSize(1);
        assertThat(entry.getMutationsList().get(0).hasDeleteFromRow()).isTrue();
    }

    @Test
    void anUpdateBeforeIsRejectedRatherThanTreatedAsADelete() {
        // The declared upsert changelog means the planner never sends one. Falling through to the
        // delete branch — which is what the HBase connector's converter does — would erase the row
        // the following UPDATE_AFTER is about to rewrite.
        assertThatThrownBy(
                        () ->
                                SERIALIZER.serialize(
                                        row(
                                                RowKind.UPDATE_BEFORE,
                                                StringData.fromString("r1"),
                                                GenericRowData.of(StringData.fromString("v"), 7L),
                                                GenericRowData.of(true)),
                                        null))
                .hasMessageContaining("UPDATE_BEFORE")
                .hasMessageContaining("upsert changelog");
    }

    @Test
    void aNullRowKeyIsRejectedNamingTheColumn() {
        assertThatThrownBy(
                        () ->
                                SERIALIZER.serialize(
                                        row(
                                                RowKind.INSERT,
                                                null,
                                                GenericRowData.of(StringData.fromString("v"), 7L),
                                                GenericRowData.of(true)),
                                        null))
                .hasMessageContaining("'rowkey' is null");
    }

    @Test
    void anEmptyRowKeyIsRejectedRatherThanDropped() {
        // Bigtable has no row with an empty key; the emulator's acceptance of one is a measured
        // deviation. Flink's HBase connector drops such a record instead, which leaves an
        // incomplete table under a green job.
        assertThatThrownBy(
                        () ->
                                SERIALIZER.serialize(
                                        row(
                                                RowKind.INSERT,
                                                StringData.fromString(""),
                                                GenericRowData.of(StringData.fromString("v"), 7L),
                                                GenericRowData.of(true)),
                                        null))
                .hasMessageContaining("'rowkey' encodes to zero bytes");
    }

    @Test
    void aDeleteStillNeedsARowKey() {
        assertThatThrownBy(
                        () ->
                                SERIALIZER.serialize(
                                        row(RowKind.DELETE, StringData.fromString(""), null, null),
                                        null))
                .hasMessageContaining("encodes to zero bytes");
    }

    @Test
    void theRowKeyTakesTheDeclaredEncodingRatherThanText() throws Exception {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("k", DataTypes.BIGINT()),
                                                DataTypes.FIELD(
                                                        "cf",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q", DataTypes.STRING()))))
                                        .getLogicalType());
        GenericRowData row = GenericRowData.of(1L, GenericRowData.of(StringData.fromString("v")));

        RowMutationEntry entry =
                new RowDataSerializationSchema(schema, "NULL", false, false).serialize(row, null);

        assertThat(entry.toProto().getRowKey())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}));
    }
}
