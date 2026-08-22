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

import org.apache.flink.table.data.GenericRowData;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a Bigtable row into one {@code RowData}, per the table's {@link BigtableTableSchema} and
 * the query's projection.
 *
 * <p>Output position {@code i} is physical column {@code projectedFields[i]} — the row key or one
 * column family — or column {@code i} itself when no projection was pushed. The projection is
 * top-level only, so a retained family always produces its full declared {@code ROW}; resolving it
 * here rather than re-deriving a narrowed schema is what lets a projection drop the row key, which
 * {@link BigtableTableSchema#of} would reject as a schema.
 *
 * <p>What a cell decodes into is decided by where it lands, not by what the read returned:
 *
 * <ul>
 *   <li>A cell whose family or qualifier the DDL does not declare is ignored. The family filter
 *       already prunes undeclared families server-side; undeclared qualifiers arrive and are
 *       skipped here.
 *   <li>Of a qualifier's versions, the latest wins: the service orders a row's cells by family,
 *       then qualifier, then timestamp <em>descending</em>, so that is the first cell seen.
 *   <li>A family none of whose declared qualifiers has a cell reads as a {@code null} field — the
 *       mirror of the sink, whose null family writes no cells. A family with at least one declared
 *       cell reads as a {@code ROW} whose absent qualifiers are null.
 *   <li>A declared qualifier's empty cell is a SQL {@code NULL} — or, for a character string, the
 *       {@code null-string-literal} is, and the empty cell is an empty string.
 * </ul>
 */
final class RowToRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int arity;

    /** The row key's output position, or -1 when the projection dropped it. */
    private final int rowKeySlot;

    @Nullable private final CellValueCodec.FieldDecoder rowKeyDecoder;
    private final FamilySlot[] familySlots;
    private final Map<String, Integer> familySlotsByName;

    /**
     * Creates the converter, resolving the schema into serializable state.
     *
     * @param schema the parsed DDL model of the whole table
     * @param projectedFields for each output position, the physical column it carries; null for the
     *     identity
     * @param nullStringLiteral the cell value that stands for a null character string
     */
    RowToRowDataConverter(
            BigtableTableSchema schema, @Nullable int[] projectedFields, String nullStringLiteral) {
        byte[] nullStringBytes = nullStringLiteral.getBytes(StandardCharsets.UTF_8);
        List<BigtableTableSchema.Family> declared = schema.getFamilies();
        this.arity = projectedFields == null ? 1 + declared.size() : projectedFields.length;
        int rowKeySlot = -1;
        FamilySlot[] slots = new FamilySlot[arity];
        int slotCount = 0;
        for (int i = 0; i < arity; i++) {
            int physical = projectedFields == null ? i : projectedFields[i];
            if (physical == schema.getRowKeyIndex()) {
                rowKeySlot = i;
                continue;
            }
            slots[slotCount++] = new FamilySlot(i, familyAt(declared, physical), nullStringBytes);
        }
        this.rowKeySlot = rowKeySlot;
        // The plain decoder: a Bigtable row cannot exist without a key, so there is no null to
        // read, whatever the column's declared nullability.
        this.rowKeyDecoder = rowKeySlot < 0 ? null : CellValueCodec.decoder(schema.getRowKeyType());
        this.familySlots = new FamilySlot[slotCount];
        System.arraycopy(slots, 0, this.familySlots, 0, slotCount);
        this.familySlotsByName = new HashMap<>();
        for (int s = 0; s < slotCount; s++) {
            familySlotsByName.put(this.familySlots[s].name, s);
        }
    }

    private static BigtableTableSchema.Family familyAt(
            List<BigtableTableSchema.Family> families, int physical) {
        for (BigtableTableSchema.Family family : families) {
            if (family.getIndex() == physical) {
                return family;
            }
        }
        // Unreachable: with nested projection unsupported, every projected index is a top-level
        // physical column, and every non-row-key column of this schema is a family.
        throw new IllegalStateException("No column family at physical index " + physical);
    }

    /**
     * Converts one row.
     *
     * @param row the row read from Bigtable
     * @return the produced row, always {@code INSERT}
     */
    GenericRowData convert(Row row) {
        GenericRowData out = new GenericRowData(arity);
        if (rowKeySlot >= 0) {
            byte[] key = row.getKey().toByteArray();
            try {
                out.setField(rowKeySlot, rowKeyDecoder.decode(key));
            } catch (RuntimeException e) {
                // The same guard the cell decode below carries, for the same reason: a row key is
                // as externally written as a cell — interop with HBase-written tables is what
                // CellValueCodec exists for — and a fixed-width decoder reading a shorter key
                // otherwise throws a bare ArrayIndexOutOfBoundsException naming nothing.
                // The key is escaped rather than decoded as UTF-8: this message asks whether the
                // row was written under a different encoding, so rendering the offending bytes
                // under the encoding the question doubts is how a reader loses them.
                throw new IllegalStateException(
                        String.format(
                                "The row key of the row '%s' holds %d byte(s), which the declared"
                                        + " row-key column type cannot decode. Was the row written"
                                        + " under a different encoding?",
                                RowRanges.format(row.getKey()), key.length),
                        e);
            }
        }
        GenericRowData[] familyRows = new GenericRowData[familySlots.length];
        // A per-row seen-marker rather than a contiguity assumption: the timestamp order within a
        // qualifier is what latest-wins leans on, but an interleave filter may emit duplicate or
        // regrouped cells, so "the same qualifier never reappears later" is not a guarantee worth
        // resting correctness on for two small allocations per row.
        boolean[][] taken = new boolean[familySlots.length][];
        for (RowCell cell : row.getCells()) {
            Integer s = familySlotsByName.get(cell.getFamily());
            if (s == null) {
                continue;
            }
            FamilySlot slot = familySlots[s];
            int q = slot.qualifierIndex(cell.getQualifier());
            if (q < 0) {
                continue;
            }
            if (familyRows[s] == null) {
                familyRows[s] = new GenericRowData(slot.qualifiers.length);
                taken[s] = new boolean[slot.qualifiers.length];
            }
            if (taken[s][q]) {
                continue;
            }
            taken[s][q] = true;
            byte[] value = cell.getValue().toByteArray();
            try {
                familyRows[s].setField(q, slot.decoders[q].decode(value));
            } catch (RuntimeException e) {
                // A malformed cell — one an external writer stored under a different layout —
                // otherwise surfaces as a bare ArrayIndexOutOfBoundsException naming nothing.
                // The row key is escaped and the qualifier is not, because they come from
                // different places: a qualifier is built from a DDL field name and so is valid
                // UTF-8 by construction, and escaping it would render a non-ASCII column as hex
                // the reader cannot match against the DDL they have to go and edit — while
                // printing it unlike the family beside it, which is the same identifier from the
                // same RowType. A row key is whatever an external writer stored.
                throw new IllegalStateException(
                        String.format(
                                "Cell %s:%s of the row with key '%s' holds %d byte(s), which the"
                                        + " declared column type cannot decode. Was the cell"
                                        + " written under a different encoding?",
                                slot.name,
                                slot.qualifiers[q].toStringUtf8(),
                                RowRanges.format(row.getKey()),
                                value.length),
                        e);
            }
        }
        for (int s = 0; s < familySlots.length; s++) {
            out.setField(familySlots[s].outputPosition, familyRows[s]);
        }
        return out;
    }

    /** One projected column family's qualifiers and their decoders, resolved once. */
    private static final class FamilySlot implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int outputPosition;
        private final String name;
        private final ByteString[] qualifiers;
        private final CellValueCodec.FieldDecoder[] decoders;

        private FamilySlot(
                int outputPosition, BigtableTableSchema.Family family, byte[] nullStringBytes) {
            this.outputPosition = outputPosition;
            this.name = family.getName();
            List<BigtableTableSchema.Qualifier> declared = family.getQualifiers();
            this.qualifiers = new ByteString[declared.size()];
            this.decoders = new CellValueCodec.FieldDecoder[declared.size()];
            for (int i = 0; i < declared.size(); i++) {
                BigtableTableSchema.Qualifier qualifier = declared.get(i);
                qualifiers[i] = ByteString.copyFromUtf8(qualifier.getName());
                decoders[i] = CellValueCodec.nullableDecoder(qualifier.getType(), nullStringBytes);
            }
        }

        private int qualifierIndex(ByteString qualifier) {
            for (int i = 0; i < qualifiers.length; i++) {
                if (qualifiers[i].equals(qualifier)) {
                    return i;
                }
            }
            return -1;
        }
    }
}
