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
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** A synchronous Bigtable row-key point lookup producing the table source's projected row. */
@Internal
public final class BigtableRowDataLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final BigtableRowDataLookup lookup;
    private final int maxRetries;

    BigtableRowDataLookupFunction(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            TrailingBytes trailingBytes,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            int maxRetries) {
        this.lookup =
                new BigtableRowDataLookup(
                        destination,
                        schema,
                        projectedFields,
                        nullStringLiteral,
                        trailingBytes,
                        filter,
                        ranges,
                        appProfileId,
                        serviceAccountKeyFile,
                        emulatorEndpoint);
        this.maxRetries = maxRetries;
    }

    @VisibleForTesting
    BigtableRowDataLookupFunction(
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            TrailingBytes trailingBytes,
            int maxRetries,
            BigtableRowLookup lookup) {
        this.lookup =
                new BigtableRowDataLookup(
                        schema, projectedFields, nullStringLiteral, trailingBytes, lookup);
        this.maxRetries = maxRetries;
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
        ByteString rowKey = lookup.rowKey(keyRow);
        for (int retry = 0; ; retry++) {
            try {
                return lookup.convert(lookup.read(rowKey));
            } catch (RuntimeException failure) {
                if (retry >= maxRetries || !BigtableLookupErrorClassifier.isTransient(failure)) {
                    throw failure;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
