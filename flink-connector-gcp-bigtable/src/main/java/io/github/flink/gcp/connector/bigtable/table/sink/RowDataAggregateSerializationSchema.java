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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** Adds non-null integer contributions to aggregate cells in one atomic row entry. */
@Internal
final class RowDataAggregateSerializationSchema implements BigtableSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final RowDataSerializationSchema addresses;
    private final Family[] families;

    RowDataAggregateSerializationSchema(
            BigtableTableSchema schema, WritableMetadata[] metadata, boolean truncateToMillis) {
        this(schema, metadata, truncateToMillis, new RowDataSerializationSchema.WallClock());
    }

    RowDataAggregateSerializationSchema(
            BigtableTableSchema schema,
            WritableMetadata[] metadata,
            boolean truncateToMillis,
            RowDataSerializationSchema.CellClock clock) {
        addresses =
                new RowDataSerializationSchema(
                        schema, "", metadata, truncateToMillis, false, clock);
        families = new Family[schema.getFamilies().size()];
        for (int i = 0; i < families.length; i++) {
            families[i] = new Family(schema.getFamilies().get(i));
        }
    }

    @Override
    public RowMutationEntry serialize(RowData row, SinkWriter.Context context) throws IOException {
        if (row.getRowKind() != RowKind.INSERT) {
            throw new IOException(
                    "Bigtable 'sink.write-mode' = 'aggregate' requires INSERT-only input; found "
                            + row.getRowKind()
                            + ".");
        }
        ByteString key = addresses.rowKey(row);
        RowMutationEntry entry = RowMutationEntry.create(key);
        int written = 0;
        for (Family family : families) {
            if (row.isNullAt(family.index)) {
                continue;
            }
            RowData cells = row.getRow(family.index, family.qualifiers.length);
            for (int i = 0; i < family.qualifiers.length; i++) {
                if (!cells.isNullAt(i)) {
                    entry.addToCell(
                            family.name,
                            family.qualifiers[i],
                            addresses.cellTimestampMicros(row),
                            integer(cells, i, family.types[i]));
                    written++;
                }
            }
        }
        if (written == 0) {
            throw new IOException(
                    "Every aggregate cell of row '"
                            + RowRanges.format(key)
                            + "' is null; write at least one contribution or filter the record upstream.");
        }
        return entry;
    }

    private static long integer(RowData row, int index, LogicalTypeRoot type) {
        switch (type) {
            case TINYINT:
                return row.getByte(index);
            case SMALLINT:
                return row.getShort(index);
            case INTEGER:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            default:
                throw new IllegalStateException("Unsupported aggregate input type: " + type);
        }
    }

    private static final class Family implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int index;
        private final ByteString[] qualifiers;
        private final LogicalTypeRoot[] types;

        Family(BigtableTableSchema.Family family) {
            name = family.getName();
            index = family.getIndex();
            List<BigtableTableSchema.Qualifier> declared = family.getQualifiers();
            qualifiers = new ByteString[declared.size()];
            types = new LogicalTypeRoot[declared.size()];
            for (int i = 0; i < declared.size(); i++) {
                qualifiers[i] = ByteString.copyFromUtf8(declared.get(i).getName());
                types[i] = declared.get(i).getType().getTypeRoot();
            }
        }
    }
}
