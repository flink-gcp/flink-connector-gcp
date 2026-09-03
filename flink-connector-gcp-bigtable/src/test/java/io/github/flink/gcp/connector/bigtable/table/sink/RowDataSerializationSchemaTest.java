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

    /** What the planner hands a DDL that declares the {@code timestamp} metadata column. */
    private static final WritableMetadata[] WITH_TIMESTAMP = {WritableMetadata.TIMESTAMP};

    /** What it hands one that declares no metadata column at all. */
    private static final WritableMetadata[] NO_METADATA = {};

    private static final RowDataSerializationSchema SERIALIZER =
            new RowDataSerializationSchema(SCHEMA, "NULL", NO_METADATA, false);

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
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, false);

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
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, true);

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
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, true);

        long beforeMillis = System.currentTimeMillis();
        MutateRowsRequest.Entry entry =
                serializer.serialize(rowWithTimestamp(RowKind.INSERT, null), null).toProto();
        long afterMillis = System.currentTimeMillis();

        // Inside the bracket, and millisecond-aligned. Both are now this connector's own
        // guarantee rather than the client library's: the schema stamps the cell itself, so the
        // alignment does not move under a client upgrade. It did move once —
        // google-cloud-bigtable 2.81.0 stamped an aligned currentTimeMillis * 1000 and 2.82.0
        // switched to a microsecond Instant.now() — which is what this path stopped depending on
        // (ADR-0149). The upper bound needs no ceiling for the same reason: an aligned value
        // cannot land above the floor of the millisecond it was read in.
        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .hasSize(3)
                .allSatisfy(
                        timestamp ->
                                assertThat(timestamp)
                                        .isBetween(beforeMillis * 1_000L, afterMillis * 1_000L));
        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros() % 1_000L)
                .containsOnly(0L);
        // USER_SPECIFIED, because this connector specified it. The distinction is what the
        // service acts on: measured 2026-09-03, it truncates a CLIENT_AUTO_GENERATED timestamp to
        // the table's granularity and rejects a USER_SPECIFIED one whose precision does not match
        // (ADR-0044) — so stamping it here is only safe while the value is aligned, which the
        // assertion above holds.
        assertThat(entry.getMutationsList())
                .extracting(Mutation::getTimestampOrigin)
                .containsOnly(Mutation.TimestampOrigin.USER_SPECIFIED);
    }

    /** A clock returning a value no wall clock produces, advancing once per read. */
    private static final class FakeClock implements RowDataSerializationSchema.CellClock {

        private static final long serialVersionUID = 1L;

        private long next = 5_000L;

        @Override
        public long micros() {
            long value = next;
            next += 1_000L;
            return value;
        }
    }

    @Test
    void theConnectorStampsTheWriterClockCellRatherThanTheClientLibrary() throws Exception {
        // The only assertion that can tell the two apart. Against google-cloud-bigtable 2.81.0 the
        // client's own timestamp-less overload produces exactly currentTimeMillis() * 1000, so a
        // bracket-and-alignment check passes whoever stamped the cell — the value is identical by
        // construction, which is this change's point (ADR-0149) and would otherwise be its blind
        // spot. Feeding a clock no wall clock could produce is what pins the stamping here.
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        SCHEMA, "NULL", WITH_TIMESTAMP, true, new FakeClock());

        MutateRowsRequest.Entry entry =
                serializer.serialize(rowWithTimestamp(RowKind.INSERT, null), null).toProto();

        // Per cell, not per record: the client library read its clock once per setCell, and
        // matching that is what keeps this schema's mutation identical to the one it built before.
        // Reading it once would collapse a row's cells onto a single timestamp, which the three
        // distinct values here refuse.
        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsExactly(5_000L, 6_000L, 7_000L);
    }

    @Test
    void aRestoredSchemaStampsThroughTheProductionClock() throws Exception {
        // The clock is transient, so a job graph written before it existed carries no value for it
        // and a non-transient reference would restore as null — the first writer-clock record on a
        // last-state upgrade would then fail (ADR-0125 requires the upgrade path after 1.0.0).
        // Restoring on the production clock is also the right way round: a graph that captured a
        // test clock must not keep it.
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        SCHEMA, "NULL", WITH_TIMESTAMP, true, new FakeClock());

        RowDataSerializationSchema restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(serializer), getClass().getClassLoader());

        long beforeMillis = System.currentTimeMillis();
        MutateRowsRequest.Entry entry =
                restored.serialize(rowWithTimestamp(RowKind.INSERT, null), null).toProto();
        long afterMillis = System.currentTimeMillis();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .hasSize(3)
                .allSatisfy(
                        timestamp ->
                                assertThat(timestamp)
                                        .isBetween(beforeMillis * 1_000L, afterMillis * 1_000L));
    }

    @Test
    void aDdlDeclaringNoTimestampMetadataAlsoTakesTheWriterClock() throws Exception {
        // The most common DDL, and the one the writer-clock path exists for: no timestamp column
        // at all, rather than a declared-but-null one. It reaches the same arm, but by a different
        // route — timestampMetadataIndex is negative here instead of the value being null.
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", NO_METADATA, false, new FakeClock());

        MutateRowsRequest.Entry entry =
                serializer
                        .serialize(
                                row(
                                        RowKind.INSERT,
                                        StringData.fromString("r1"),
                                        GenericRowData.of(StringData.fromString("v"), 7L),
                                        GenericRowData.of(true)),
                                null)
                        .toProto();

        assertThat(entry.getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsExactly(5_000L, 6_000L, 7_000L);
        // The origin too, not only the value: both metadata shapes reach the same arm today, and
        // this is what would notice if the no-metadata case — the most common DDL — were ever
        // routed back to the client's timestamp-less overload, where it would silently become
        // CLIENT_AUTO_GENERATED.
        assertThat(entry.getMutationsList())
                .extracting(Mutation::getTimestampOrigin)
                .containsOnly(Mutation.TimestampOrigin.USER_SPECIFIED);
    }

    @Test
    void aDeleteIgnoresTimestampMetadata() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, false);

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
                new RowDataSerializationSchema(SCHEMA, "NULL", WITH_TIMESTAMP, false);

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
    void theRejectedRowNamesABinaryKeyEscaped() {
        // The row-key column need not be a character string. A BIGINT key encodes to eight raw
        // bytes, and toStringUtf8() put those into the message as control characters a terminal
        // swallows — so the one row this message exists to name arrived unreadable, in the very
        // message that tells a user which row to go and fix.
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

        assertThatThrownBy(
                        () ->
                                new RowDataSerializationSchema(schema, "NULL", NO_METADATA, false)
                                        .serialize(GenericRowData.of(1L, null), null))
                .hasMessageContaining("'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01'")
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
        // Bigtable has no row with an empty key. Flink's HBase connector drops such a record
        // instead, which leaves an incomplete table under a green job.
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
                new RowDataSerializationSchema(schema, "NULL", NO_METADATA, false)
                        .serialize(row, null);

        assertThat(entry.toProto().getRowKey())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}));
    }
}
