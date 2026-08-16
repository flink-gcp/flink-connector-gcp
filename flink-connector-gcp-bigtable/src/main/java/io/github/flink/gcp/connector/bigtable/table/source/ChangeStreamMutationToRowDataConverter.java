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

import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Converts one connector-owned Change Streams mutation to the generic table envelope. */
@Internal
final class ChangeStreamMutationToRowDataConverter implements Serializable {

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

    GenericRowData convert(ChangeStreamMutation mutation) throws IOException {
        List<ChangeStreamMutation.Entry> entries = mutation.getEntries();
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

    private static GenericRowData convertEntry(int index, ChangeStreamMutation.Entry entry)
            throws IOException {
        if (entry instanceof ChangeStreamMutation.SetCellEntry) {
            ChangeStreamMutation.SetCellEntry set = (ChangeStreamMutation.SetCellEntry) entry;
            return entry(
                    index,
                    SET_CELL,
                    set.getFamilyName(),
                    rawValue(set.getQualifier().toByteArray()),
                    rawTimestamp(set.getTimestampMicros()),
                    rawValue(set.getValue().toByteArray()),
                    null);
        }
        if (entry instanceof ChangeStreamMutation.DeleteCellsEntry) {
            ChangeStreamMutation.DeleteCellsEntry delete =
                    (ChangeStreamMutation.DeleteCellsEntry) entry;
            return entry(
                    index,
                    DELETE_CELLS,
                    delete.getFamilyName(),
                    rawValue(delete.getQualifier().toByteArray()),
                    null,
                    null,
                    deleteRange(delete.getTimestampRange()));
        }
        if (entry instanceof ChangeStreamMutation.DeleteFamilyEntry) {
            ChangeStreamMutation.DeleteFamilyEntry delete =
                    (ChangeStreamMutation.DeleteFamilyEntry) entry;
            return entry(index, DELETE_FAMILY, delete.getFamilyName(), null, null, null, null);
        }
        if (entry instanceof ChangeStreamMutation.AddToCellEntry) {
            ChangeStreamMutation.AddToCellEntry add = (ChangeStreamMutation.AddToCellEntry) entry;
            return entry(
                    index,
                    ADD_TO_CELL,
                    add.getFamilyName(),
                    value(add.getQualifier()),
                    value(add.getTimestamp()),
                    value(add.getInput()),
                    null);
        }
        if (entry instanceof ChangeStreamMutation.MergeToCellEntry) {
            ChangeStreamMutation.MergeToCellEntry merge =
                    (ChangeStreamMutation.MergeToCellEntry) entry;
            return entry(
                    index,
                    MERGE_TO_CELL,
                    merge.getFamilyName(),
                    value(merge.getQualifier()),
                    value(merge.getTimestamp()),
                    value(merge.getInput()),
                    null);
        }
        throw new IOException(
                "Unsupported Bigtable Change Streams entry type: "
                        + entry.getClass().getName()
                        + ". Upgrade the table envelope converter before accepting this type.");
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

    private static GenericRowData value(ChangeStreamMutation.Value value) throws IOException {
        if (value instanceof ChangeStreamMutation.RawValue) {
            return rawValue(((ChangeStreamMutation.RawValue) value).getValue().toByteArray());
        }
        if (value instanceof ChangeStreamMutation.RawTimestamp) {
            return rawTimestamp(((ChangeStreamMutation.RawTimestamp) value).getValue());
        }
        if (value instanceof ChangeStreamMutation.Int64Value) {
            return GenericRowData.of(
                    INT64, null, ((ChangeStreamMutation.Int64Value) value).getValue());
        }
        throw new IOException(
                "Unsupported Bigtable Change Streams value type: "
                        + value.getClass().getName()
                        + ". Upgrade the table envelope converter before accepting this type.");
    }

    private static GenericRowData rawValue(byte[] value) {
        return GenericRowData.of(RAW_VALUE, value, null);
    }

    private static GenericRowData rawTimestamp(long micros) {
        return GenericRowData.of(RAW_TIMESTAMP, null, micros);
    }

    private static GenericRowData deleteRange(ChangeStreamMutation.TimestampRange range) {
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

    private static StringData bound(ChangeStreamMutation.BoundType bound) {
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
