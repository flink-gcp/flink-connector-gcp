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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRule;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteSerializationSchema;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts each nonnull input cell to one rule in DDL order.
 *
 * <p>A {@code MAP} family contributes one rule per entry whose value is non-null, addressed by the
 * entry's key, in the order the map presents its entries; a null key fails the record (ADR-0172).
 */
@Internal
final class RowDataReadModifyWriteSerializationSchema
        implements ReadModifyWriteSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final int rowKeyIndex;
    private final String rowKeyName;
    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final List<Family> families;
    private final boolean increment;

    RowDataReadModifyWriteSerializationSchema(BigtableTableSchema schema, WriteMode mode) {
        Preconditions.checkArgument(
                mode == WriteMode.APPEND || mode == WriteMode.INCREMENT,
                "A read-modify-write schema requires append or increment mode");
        ReadModifyWriteSchemaChecks.validate(schema, mode);
        rowKeyIndex = schema.getRowKeyIndex();
        rowKeyName = schema.getRowKeyName();
        rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        increment = mode == WriteMode.INCREMENT;
        families = new ArrayList<>();
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            families.add(new Family(family, !increment));
        }
    }

    @Override
    public ReadModifyWriteRequest serialize(RowData input, SinkWriter.Context context)
            throws IOException {
        if (input.getRowKind() != RowKind.INSERT) {
            throw new IOException(
                    "Bigtable append and increment require INSERT-only input; received "
                            + input.getRowKind());
        }
        if (input.isNullAt(rowKeyIndex)) {
            throw new IOException("The row-key column '" + rowKeyName + "' is null.");
        }
        ByteString key = ByteString.copyFrom(rowKeyEncoder.encode(input, rowKeyIndex));
        if (key.isEmpty()) {
            throw new IOException("The row-key column '" + rowKeyName + "' encodes to zero bytes.");
        }
        List<ReadModifyWriteRule> rules = new ArrayList<>();
        for (Family family : families) {
            if (input.isNullAt(family.index)) {
                continue;
            }
            if (family.map) {
                addMap(rules, input, key, family);
                continue;
            }
            RowData cells = input.getRow(family.index, family.qualifiers.length);
            for (int i = 0; i < family.qualifiers.length; i++) {
                if (cells.isNullAt(i)) {
                    continue;
                }
                ByteString qualifier = family.qualifiers[i];
                if (increment) {
                    rules.add(
                            ReadModifyWriteRule.increment(
                                    family.name, qualifier, cells.getLong(i)));
                } else {
                    ByteString value = ByteString.copyFrom(family.encoders[i].encode(cells, i));
                    if (value.isEmpty()) {
                        throw new IOException(
                                "Append value for column '"
                                        + family.name
                                        + "."
                                        + qualifier.toStringUtf8()
                                        + "' is empty.");
                    }
                    rules.add(ReadModifyWriteRule.append(family.name, qualifier, value));
                }
            }
        }
        if (rules.isEmpty()) {
            throw new IOException(
                    "Bigtable append and increment require at least one nonnull cell; all families"
                            + " or cells are null, and every map is empty or holds only null"
                            + " values.");
        }
        return ReadModifyWriteRequest.of(key, rules);
    }

    /** Adds one rule per non-null entry of a {@code MAP} family. */
    private void addMap(
            List<ReadModifyWriteRule> rules, RowData input, ByteString key, Family family)
            throws IOException {
        MapData map = input.getMap(family.index);
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            if (keys.isNullAt(i)) {
                throw RowDataSerializationSchema.nullMapKey(family.name, key);
            }
            if (values.isNullAt(i)) {
                continue;
            }
            ByteString qualifier = ByteString.copyFrom(family.keyEncoder.encode(keys, i));
            if (increment) {
                rules.add(ReadModifyWriteRule.increment(family.name, qualifier, values.getLong(i)));
                continue;
            }
            ByteString value = ByteString.copyFrom(family.valueEncoder.encode(values, i));
            if (value.isEmpty()) {
                // Escaped rather than decoded: a map key is data, not a DDL field name.
                throw new IOException(
                        "Append value for key '"
                                + RowRanges.format(qualifier)
                                + "' of MAP column family '"
                                + family.name
                                + "' is empty.");
            }
            rules.add(ReadModifyWriteRule.append(family.name, qualifier, value));
        }
    }

    /**
     * One family's qualifiers and their append encoders, or, for a {@code MAP} family, its key and
     * append value encoders.
     *
     * <p>The map fields were added after 1.0.0; a job graph written before them restores {@code
     * map} as {@code false}, which is the only kind of family such a graph could hold.
     */
    private static final class Family implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int index;
        private final ByteString[] qualifiers;
        private final CellValueCodec.FieldEncoder[] encoders;
        private final boolean map;
        // The plain encoders: a null key or value is handled before it is encoded.
        @Nullable private final CellValueCodec.ElementEncoder keyEncoder;
        @Nullable private final CellValueCodec.ElementEncoder valueEncoder;

        private Family(BigtableTableSchema.Family family, boolean append) {
            name = family.getName();
            index = family.getIndex();
            map = family.isMap();
            keyEncoder = map ? CellValueCodec.elementEncoder(family.getMapKeyType()) : null;
            valueEncoder =
                    map && append ? CellValueCodec.elementEncoder(family.getMapValueType()) : null;
            qualifiers = new ByteString[family.getQualifiers().size()];
            encoders = new CellValueCodec.FieldEncoder[append ? qualifiers.length : 0];
            for (int i = 0; i < qualifiers.length; i++) {
                BigtableTableSchema.Qualifier qualifier = family.getQualifiers().get(i);
                qualifiers[i] = ByteString.copyFromUtf8(qualifier.getName());
                if (append) {
                    encoders[i] = CellValueCodec.encoder(qualifier.getType());
                }
            }
        }
    }
}
