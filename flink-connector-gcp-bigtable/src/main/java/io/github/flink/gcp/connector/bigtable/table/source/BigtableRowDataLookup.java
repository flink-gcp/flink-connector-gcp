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
import org.apache.flink.table.data.RowData;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A point lookup in the table layer's own terms: it takes the lookup key as {@link RowData},
 * encodes its single field into a row key, reads through the {@link BigtableRowLookup} seam, and
 * returns the projected {@code RowData} the table source produces — or no row at all.
 *
 * <p>It is the layer between that seam, which speaks the client's {@link Row}, and Flink's two
 * lookup functions, which speak {@code RowData} and differ only in how they retry: a loop on one
 * side, a stack-safe trampoline on the other. Those two extend different Flink superclasses and so
 * can share nothing by inheritance; each holds one of these instead.
 */
@Internal
final class BigtableRowDataLookup implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final RowToRowDataConverter converter;
    private final BigtableRowLookup lookup;

    /** Creates the support, reading through a client of its own. */
    BigtableRowDataLookup(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint) {
        this(
                schema,
                projectedFields,
                nullStringLiteral,
                new BigtableDataClientRowLookup(
                        destination,
                        filter,
                        ranges,
                        appProfileId,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }

    /** Creates the support around a given seam, which is how the module's tests reach it. */
    BigtableRowDataLookup(
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            BigtableRowLookup lookup) {
        this.rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        this.converter = new RowToRowDataConverter(schema, projectedFields, nullStringLiteral);
        this.lookup = lookup;
    }

    /** Opens the client resources the reads use. */
    void open() throws Exception {
        lookup.open();
    }

    /** Encodes the lookup key row's single field into a Bigtable row key. */
    ByteString rowKey(RowData keyRow) {
        return ByteString.copyFrom(rowKeyEncoder.encode(keyRow, 0));
    }

    /** Reads one row synchronously. */
    @Nullable
    Row read(ByteString rowKey) {
        return lookup.read(rowKey);
    }

    /** Reads one row asynchronously. */
    ApiFuture<Row> readAsync(ByteString rowKey) {
        return lookup.readAsync(rowKey);
    }

    /** Converts a read answer into the lookup's result: no row is no result, not a null row. */
    Collection<RowData> convert(@Nullable Row row) {
        return row == null
                ? Collections.emptyList()
                : Collections.singletonList(converter.convert(row));
    }

    /** Releases the client resources. */
    void close() throws Exception {
        lookup.close();
    }
}
