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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Turns a changelog row into one Bigtable mutation, per the table's {@link BigtableTableSchema}.
 *
 * <p>An insert or an update writes every qualifier the DDL declares — including the ones whose
 * value is null, which become an empty cell, or the {@code null-string-literal} for a character
 * string. Writing them rather than skipping them is what makes the row read back as the DDL says it
 * should: a qualifier left unwritten would keep whatever an earlier version of the row put there. A
 * whole column family that is null writes no cells at all, since there is no value to encode. When
 * the DDL selects writable {@code timestamp} metadata, its non-null value is applied to every cell
 * this row writes; a null value keeps the client library's writer-clock path.
 *
 * <p>A delete removes the entire row, not the declared qualifiers one by one. The row key is the
 * primary key of an upsert sink, so "this key is gone" is what a {@code -D} means here; deleting
 * only the declared cells would leave a row behind made of whatever else was in it.
 *
 * <p>Three rejections, each of which fails the record through the sink's failure handler rather
 * than skipping it:
 *
 * <ul>
 *   <li>An {@code UPDATE_BEFORE} row, which the declared upsert changelog mode means the planner
 *       never sends. Treating it as a delete — which is what falling through to the {@code else}
 *       branch would do — would erase the row an update is about to rewrite.
 *   <li>An absent or empty row key. Bigtable has no such row, so failing here is what keeps the job
 *       honest about what it wrote. Flink's HBase connector drops the record instead, which leaves
 *       an incomplete table under a green job.
 *   <li>A row whose every column family is null, which would produce an entry carrying no mutation
 *       at all. The service answers that with an {@code INVALID_ARGUMENT} naming neither the row
 *       nor the reason, so it is refused here where both can be said. A partial column list in an
 *       {@code INSERT} is the ordinary way to reach it.
 * </ul>
 */
@Internal
final class RowDataSerializationSchema implements BigtableSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final int rowKeyIndex;
    private final String rowKeyName;
    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final Family[] families;
    private final int timestampMetadataIndex;
    private final boolean truncateCellTimestampToMillis;

    /**
     * Creates the schema from the table's DDL model.
     *
     * @param schema the parsed DDL model
     * @param nullStringLiteral the cell value that stands for a null character string
     * @param metadata the writable metadata the planner selected, in the order it laid the consumed
     *     row out in; the timestamp's index is read out of it here rather than assumed, and only
     *     that index is kept
     * @param truncateCellTimestampToMillis whether explicit timestamps lose their sub-millisecond
     *     part before being sent
     */
    RowDataSerializationSchema(
            BigtableTableSchema schema,
            String nullStringLiteral,
            WritableMetadata[] metadata,
            boolean truncateCellTimestampToMillis) {
        this.rowKeyIndex = schema.getRowKeyIndex();
        this.rowKeyName = schema.getRowKeyName();
        this.timestampMetadataIndex =
                WritableMetadata.TIMESTAMP.position(schema.getFieldCount(), metadata);
        this.truncateCellTimestampToMillis = truncateCellTimestampToMillis;
        // The plain encoder, not the nullable one: a null row key is rejected below rather than
        // encoded as an empty cell.
        this.rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        byte[] nullStringBytes = nullStringLiteral.getBytes(StandardCharsets.UTF_8);
        List<BigtableTableSchema.Family> declared = schema.getFamilies();
        this.families = new Family[declared.size()];
        for (int i = 0; i < declared.size(); i++) {
            families[i] = new Family(declared.get(i), nullStringBytes);
        }
    }

    @Override
    public RowMutationEntry serialize(RowData element, SinkWriter.Context context)
            throws IOException {
        RowKind kind = element.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            throw new IOException(
                    "An UPDATE_BEFORE row reached the Bigtable table sink, which declares an"
                            + " upsert changelog and so is never sent one. Writing it would delete"
                            + " the row the following UPDATE_AFTER rewrites.");
        }
        RowMutationEntry entry = RowMutationEntry.create(rowKey(element));
        if (kind == RowKind.DELETE) {
            return entry.deleteRow();
        }
        boolean hasExplicitTimestamp =
                timestampMetadataIndex >= 0 && !element.isNullAt(timestampMetadataIndex);
        long timestampMicros = hasExplicitTimestamp ? timestampMicros(element) : 0L;
        int written = 0;
        for (Family family : families) {
            if (element.isNullAt(family.index)) {
                continue;
            }
            RowData cells = element.getRow(family.index, family.qualifiers.length);
            for (int i = 0; i < family.qualifiers.length; i++) {
                ByteString value = ByteString.copyFrom(family.encoders[i].encode(cells, i));
                if (hasExplicitTimestamp) {
                    entry.setCell(family.name, family.qualifiers[i], timestampMicros, value);
                } else {
                    entry.setCell(family.name, family.qualifiers[i], value);
                }
            }
            written += family.qualifiers.length;
        }
        if (written == 0) {
            throw new IOException(
                    String.format(
                            "Every column family of the row with key '%s' is null, so the mutation"
                                    + " would carry no cell. Bigtable rejects such a request with"
                                    + " INVALID_ARGUMENT and a message that names neither the row"
                                    + " nor the reason, so it is refused here instead. A row with"
                                    + " nothing but a key is not a Bigtable row; write at least"
                                    + " one column family, or filter the record out upstream.",
                            RowRanges.format(entry.toProto().getRowKey())));
        }
        return entry;
    }

    private long timestampMicros(RowData element) throws IOException {
        TimestampData timestamp =
                element.getTimestamp(timestampMetadataIndex, WritableMetadata.TIMESTAMP_PRECISION);
        try {
            long millis = Math.multiplyExact(timestamp.getMillisecond(), 1_000L);
            if (truncateCellTimestampToMillis) {
                return millis;
            }
            return Math.addExact(millis, timestamp.getNanoOfMillisecond() / 1_000L);
        } catch (ArithmeticException e) {
            throw new IOException(
                    "The 'timestamp' metadata value is outside the range of epoch microseconds.",
                    e);
        }
    }

    private ByteString rowKey(RowData element) throws IOException {
        if (element.isNullAt(rowKeyIndex)) {
            throw new IOException(
                    String.format(
                            "The row-key column '%s' is null. Bigtable has no row without a key.",
                            rowKeyName));
        }
        ByteString key = ByteString.copyFrom(rowKeyEncoder.encode(element, rowKeyIndex));
        if (key.isEmpty()) {
            throw new IOException(
                    String.format(
                            "The row-key column '%s' encodes to zero bytes. Bigtable has no row"
                                    + " with an empty key.",
                            rowKeyName));
        }
        return key;
    }

    /** One column family's qualifiers and their encoders, resolved once. */
    private static final class Family implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final int index;
        private final ByteString[] qualifiers;
        private final CellValueCodec.FieldEncoder[] encoders;

        private Family(BigtableTableSchema.Family family, byte[] nullStringBytes) {
            this.name = family.getName();
            this.index = family.getIndex();
            List<BigtableTableSchema.Qualifier> declared = family.getQualifiers();
            this.qualifiers = new ByteString[declared.size()];
            this.encoders = new CellValueCodec.FieldEncoder[declared.size()];
            for (int i = 0; i < declared.size(); i++) {
                BigtableTableSchema.Qualifier qualifier = declared.get(i);
                // Encoded once here rather than per record: the SDK's String overload would
                // re-encode the same qualifier for every cell of every row.
                qualifiers[i] = ByteString.copyFromUtf8(qualifier.getName());
                encoders[i] = CellValueCodec.nullableEncoder(qualifier.getType(), nullStringBytes);
            }
        }
    }
}
