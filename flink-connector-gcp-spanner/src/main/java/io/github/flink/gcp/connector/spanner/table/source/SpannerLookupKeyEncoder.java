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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Encodes a planner lookup-key row in declared Spanner primary-key order.
 *
 * <p>A key part declared {@code TIMESTAMP_LTZ(p < 9)} is coarse: rows carry the stored value
 * truncated to {@code p}, so a probe matches every stored value in its bucket. A key with a coarse
 * part is therefore read as a range rather than a point: the parts before the first coarse one pin
 * a prefix, the first coarse one ranges over its bucket, and the rows read are kept only when every
 * key part, truncated as a read row carries it, equals the probe's.
 */
@Internal
final class SpannerLookupKeyEncoder implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SpannerTableSchemaConverter schema;
    private final int[] keyPositions;

    /** Position in the primary key of the first coarse part, or {@code -1} when there is none. */
    private final int coarsePart;

    /** Bucket units per primary-key part; {@code 1} for a part that is not coarse. */
    private final int[] units;

    /** Converts a read row's primary-key columns, truncated as the row carries them. */
    private final StructToRowDataConverter keyConverter;

    SpannerLookupKeyEncoder(SpannerTableSchemaConverter schema, int[] keyPositions) {
        this.schema = schema;
        this.keyPositions = keyPositions;
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        this.units = new int[primaryKeyIndexes.length];
        int first = -1;
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            SpannerTableSchemaConverter.Column column =
                    schema.getColumns().get(primaryKeyIndexes[i]);
            units[i] =
                    column.getSpannerType().getCode() == Type.Code.TIMESTAMP
                            ? TimestampTruncation.unit(
                                    (LocalZonedTimestampType) column.getLogicalType())
                            : 1;
            if (units[i] > 1 && first < 0) {
                first = i;
            }
        }
        this.coarsePart = first;
        this.keyConverter = new StructToRowDataConverter(schema, primaryKeyIndexes);
    }

    Key encode(RowData row) {
        Key.Builder key = Key.newBuilder();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            SpannerTableSchemaConverter.Column column =
                    schema.getColumns().get(primaryKeyIndexes[i]);
            key.appendObject(SpannerKeyValueEncoder.rowValue(row, keyPositions[i], column));
        }
        return truncate(key.build());
    }

    /** Whether a key has a coarse part and must be read as a range. */
    boolean isCoarse() {
        return coarsePart >= 0;
    }

    /**
     * The first coarse primary-key column, whose live key order decides a range's endpoints, or
     * null when the key has no coarse part.
     */
    @Nullable
    static String coarseColumn(SpannerTableSchemaConverter schema) {
        for (int index : schema.getPrimaryKeyIndexes()) {
            SpannerTableSchemaConverter.Column column = schema.getColumns().get(index);
            if (column.getSpannerType().getCode() == Type.Code.TIMESTAMP
                    && TimestampTruncation.unit((LocalZonedTimestampType) column.getLogicalType())
                            > 1) {
                return column.getName();
            }
        }
        return null;
    }

    /**
     * The range of stored keys a probe can match: the prefix before the first coarse part, and that
     * part's bucket. Later parts are left open and checked by {@link #matches(Struct, Key)}.
     *
     * @param probe the probe as {@link #encode(RowData)} returns it
     * @param descending whether the coarse column sorts descending in the live primary key
     */
    KeySet range(Key probe, boolean descending) {
        List<Object> prefix = new ArrayList<>();
        Iterator<Object> parts = probe.getParts().iterator();
        for (int i = 0; i < coarsePart; i++) {
            prefix.add(parts.next());
        }
        Timestamp floor = (Timestamp) parts.next();
        @Nullable Timestamp next = TimestampTruncation.nextBucket(floor, units[coarsePart]);
        Key start = key(prefix, floor);
        Key prefixKey = key(prefix, null);
        if (next == null) {
            // The last bucket Spanner can store reaches the end of the prefix.
            return KeySet.range(
                    descending
                            ? KeyRange.closedClosed(prefixKey, start)
                            : KeyRange.closedClosed(start, prefixKey));
        }
        Key end = key(prefix, next);
        return KeySet.range(
                descending ? KeyRange.openClosed(end, start) : KeyRange.closedOpen(start, end));
    }

    /**
     * Converts the rows read from {@link #range(Key, boolean)} that carry the probe's key.
     *
     * @param rows the rows read
     * @param probe the probe as {@link #encode(RowData)} returns it
     * @param converter the converter to the projected row
     * @return the matching rows, converted
     */
    List<RowData> matching(List<Struct> rows, Key probe, StructToRowDataConverter converter) {
        List<RowData> matched = new ArrayList<>();
        for (Struct row : rows) {
            if (matches(row, probe)) {
                matched.add(converter.convert(row));
            }
        }
        return matched;
    }

    /** Whether a row read from {@link #range(Key, boolean)} carries the probe's key. */
    boolean matches(Struct row, Key probe) {
        RowData key = keyConverter.convert(row);
        Iterator<Object> expected = probe.getParts().iterator();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            Object stored =
                    SpannerKeyValueEncoder.rowValue(
                            key, i, schema.getColumns().get(primaryKeyIndexes[i]));
            Object wanted = expected.next();
            if (stored == null
                    || wanted == null
                    || SpannerKeyValueEncoder.compare(stored, wanted) != 0) {
                return false;
            }
        }
        return true;
    }

    /** Truncates each coarse part, so a probe carrying finer digits still names its bucket. */
    private Key truncate(Key key) {
        if (!isCoarse()) {
            return key;
        }
        Key.Builder truncated = Key.newBuilder();
        int i = 0;
        for (Object part : key.getParts()) {
            truncated.appendObject(
                    units[i] > 1 && part != null
                            ? TimestampTruncation.floor((Timestamp) part, units[i])
                            : part);
            i++;
        }
        return truncated.build();
    }

    private static Key key(List<Object> prefix, @Nullable Object last) {
        Key.Builder key = Key.newBuilder();
        for (Object part : prefix) {
            key.appendObject(part);
        }
        if (last != null) {
            key.appendObject(last);
        }
        return key.build();
    }
}
