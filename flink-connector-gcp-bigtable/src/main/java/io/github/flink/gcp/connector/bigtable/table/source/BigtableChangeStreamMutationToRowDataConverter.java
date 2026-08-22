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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutationDispatcher;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Converts one connector-owned Change Streams mutation to the generic table envelope. */
@Internal
final class BigtableChangeStreamMutationToRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final StringData SET_CELL = stringData("SET_CELL");
    private static final StringData DELETE_CELLS = stringData("DELETE_CELLS");
    private static final StringData DELETE_FAMILY = stringData("DELETE_FAMILY");
    private static final StringData ADD_TO_CELL = stringData("ADD_TO_CELL");
    private static final StringData MERGE_TO_CELL = stringData("MERGE_TO_CELL");
    private static final StringData RAW_VALUE = stringData("RAW_VALUE");
    private static final StringData RAW_TIMESTAMP = stringData("RAW_TIMESTAMP");
    private static final StringData INT64 = stringData("INT64");
    private static final StringData OPEN = stringData("OPEN");
    private static final StringData CLOSED = stringData("CLOSED");
    private static final StringData UNBOUNDED = stringData("UNBOUNDED");

    private static final EntryConverter ENTRY_CONVERTER = new EntryConverter();
    private static final ValueConverter VALUE_CONVERTER = new ValueConverter();

    GenericRowData convert(BigtableChangeStreamMutation mutation) throws IOException {
        List<BigtableChangeStreamMutation.Entry> entries = mutation.getEntries();
        Object[] converted = new Object[entries.size()];
        for (int index = 0; index < entries.size(); index++) {
            converted[index] = convertEntry(index, entries.get(index));
        }
        return GenericRowData.of(
                mutation.getRowKey().toByteArray(), new GenericArrayData(converted));
    }

    private static StringData stringData(String value) {
        return StringData.fromBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    private static GenericRowData convertEntry(int index, BigtableChangeStreamMutation.Entry entry)
            throws IOException {
        return ChangeStreamMutationDispatcher.dispatchEntry(entry, ENTRY_CONVERTER, index);
    }

    /**
     * Renders each entry subtype as one envelope row.
     *
     * <p>The {@code kind} strings stay separate constants rather than deriving from {@link
     * BigtableChangeStreamMutation.EntryKind}: they are the envelope's SQL-visible output, and
     * binding them to the enum would let renaming a constant change what a query returns. Nothing
     * is lost by keeping them apart, because a new subtype cannot reach here without adding the
     * visitor method that names its string.
     *
     * <p>Stateless and shared; the index arrives as the argument. Boxing it costs nothing new — the
     * row it lands in already holds it as an {@code Object}.
     */
    private static final class EntryConverter
            implements ChangeStreamMutationDispatcher.EntryVisitor<GenericRowData, Integer> {

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.SetCellEntry entry, Integer index) {
            return entry(
                    index,
                    SET_CELL,
                    entry.getFamilyName(),
                    rawValue(entry.getQualifier().toByteArray()),
                    rawTimestamp(entry.getTimestampMicros()),
                    rawValue(entry.getValue().toByteArray()),
                    null);
        }

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.DeleteCellsEntry entry, Integer index) {
            return entry(
                    index,
                    DELETE_CELLS,
                    entry.getFamilyName(),
                    rawValue(entry.getQualifier().toByteArray()),
                    null,
                    null,
                    deleteRange(entry.getTimestampRange()));
        }

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.DeleteFamilyEntry entry, Integer index) {
            return entry(index, DELETE_FAMILY, entry.getFamilyName(), null, null, null, null);
        }

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.AddToCellEntry entry, Integer index)
                throws IOException {
            return entry(
                    index,
                    ADD_TO_CELL,
                    entry.getFamilyName(),
                    value(entry.getQualifier()),
                    value(entry.getTimestamp()),
                    value(entry.getInput()),
                    null);
        }

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.MergeToCellEntry entry, Integer index)
                throws IOException {
            return entry(
                    index,
                    MERGE_TO_CELL,
                    entry.getFamilyName(),
                    value(entry.getQualifier()),
                    value(entry.getTimestamp()),
                    value(entry.getInput()),
                    null);
        }
    }

    private static GenericRowData entry(
            int index,
            StringData kind,
            String family,
            GenericRowData qualifier,
            GenericRowData timestamp,
            GenericRowData value,
            GenericRowData deleteRange) {
        return GenericRowData.of(
                index,
                kind,
                StringData.fromString(family),
                qualifier,
                timestamp,
                value,
                deleteRange);
    }

    private static GenericRowData value(BigtableChangeStreamMutation.Value value)
            throws IOException {
        return ChangeStreamMutationDispatcher.dispatchValue(value, VALUE_CONVERTER, null);
    }

    /** Renders each aggregate value subtype as one envelope row; stateless, needing no argument. */
    private static final class ValueConverter
            implements ChangeStreamMutationDispatcher.ValueVisitor<GenericRowData, Void> {

        @Override
        public GenericRowData visit(BigtableChangeStreamMutation.RawValue value, Void argument) {
            return rawValue(value.getValue().toByteArray());
        }

        @Override
        public GenericRowData visit(
                BigtableChangeStreamMutation.RawTimestamp value, Void argument) {
            return rawTimestamp(value.getValue());
        }

        @Override
        public GenericRowData visit(BigtableChangeStreamMutation.Int64Value value, Void argument) {
            return GenericRowData.of(INT64, null, value.getValue());
        }
    }

    private static GenericRowData rawValue(byte[] value) {
        return GenericRowData.of(RAW_VALUE, value, null);
    }

    private static GenericRowData rawTimestamp(long micros) {
        return GenericRowData.of(RAW_TIMESTAMP, null, micros);
    }

    private static GenericRowData deleteRange(BigtableChangeStreamMutation.TimestampRange range) {
        return GenericRowData.of(
                bound(range.getStart().getType()),
                range.getStart().getTimestampMicros().isPresent()
                        ? range.getStart().getTimestampMicros().getAsLong()
                        : null,
                bound(range.getEnd().getType()),
                range.getEnd().getTimestampMicros().isPresent()
                        ? range.getEnd().getTimestampMicros().getAsLong()
                        : null);
    }

    private static StringData bound(BigtableChangeStreamMutation.BoundType bound) {
        switch (bound) {
            case OPEN:
                return OPEN;
            case CLOSED:
                return CLOSED;
            case UNBOUNDED:
                return UNBOUNDED;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Bigtable timestamp-range bound: " + bound + ".");
        }
    }
}
