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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** A synchronous Bigtable row-key point lookup producing the table source's projected row. */
@Internal
public final class BigtableRowDataLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final RowToRowDataConverter converter;
    private final int maxRetries;
    private final BigtableRowLookup lookup;

    BigtableRowDataLookupFunction(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            int maxRetries) {
        this(
                schema,
                projectedFields,
                nullStringLiteral,
                maxRetries,
                new BigtableDataClientRowLookup(
                        destination,
                        filter,
                        ranges,
                        appProfileId,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }

    @VisibleForTesting
    BigtableRowDataLookupFunction(
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            int maxRetries,
            BigtableRowLookup lookup) {
        this.rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        this.converter = new RowToRowDataConverter(schema, projectedFields, nullStringLiteral);
        this.maxRetries = maxRetries;
        this.lookup = lookup;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        lookup.open();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        if (keyRow.isNullAt(0)) {
            return Collections.emptyList();
        }
        ByteString rowKey = ByteString.copyFrom(rowKeyEncoder.encode(keyRow, 0));
        for (int retry = 0; ; retry++) {
            try {
                return convert(lookup.read(rowKey));
            } catch (RuntimeException failure) {
                if (retry >= maxRetries || !BigtableLookupErrorClassifier.isTransient(failure)) {
                    throw failure;
                }
            }
        }
    }

    private Collection<RowData> convert(@Nullable Row row) {
        return row == null
                ? Collections.emptyList()
                : Collections.singletonList(converter.convert(row));
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
