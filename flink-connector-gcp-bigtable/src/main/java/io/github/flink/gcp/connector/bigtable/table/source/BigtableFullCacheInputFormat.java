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
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** The single-split bounded scan Flink's full lookup cache uses to load all projected rows. */
@Internal
final class BigtableFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private final TableDestination destination;
    private final Filters.Filter filter;
    private final List<ByteStringRange> ranges;
    @Nullable private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    private final RowToRowDataConverter converter;
    @Nullable private final RowStreamOpener rowStreamOpener;

    @Nullable private transient BigtableDataClient client;
    private transient Iterator<ByteStringRange> remainingRanges = Collections.emptyIterator();
    private transient Iterator<Row> rows = Collections.emptyIterator();

    BigtableFullCacheInputFormat(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable RowStreamOpener rowStreamOpener) {
        this.destination = destination;
        this.filter = filter;
        this.ranges = ranges;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.converter = new RowToRowDataConverter(schema, projectedFields, nullStringLiteral);
        this.rowStreamOpener = rowStreamOpener;
    }

    Filters.Filter getFilter() {
        return filter;
    }

    List<ByteStringRange> getRanges() {
        return RowRanges.copyAll(ranges);
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
        if (split.getSplitNumber() != 0) {
            rows = Collections.emptyIterator();
            return;
        }
        try {
            if (rowStreamOpener == null) {
                client = BigtableDataClient.create(settings());
            }
            remainingRanges = ranges.iterator();
            if (remainingRanges.hasNext()) {
                advanceRange();
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    private BigtableDataSettings settings() throws IOException {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint, BigtableConnectorOptions.EMULATOR_ENDPOINT.key());
        return BigtableDataClients.settings(
                        destination,
                        appProfileId,
                        endpoint,
                        BigtableCredentials.loadData(serviceAccountKeyFile))
                .build();
    }

    @Override
    public boolean reachedEnd() {
        while (!rows.hasNext() && remainingRanges.hasNext()) {
            advanceRange();
        }
        return !rows.hasNext();
    }

    @Override
    public RowData nextRecord(RowData reuse) {
        return converter.convert(rows.next());
    }

    private void advanceRange() {
        ByteStringRange range = remainingRanges.next();
        if (rowStreamOpener != null) {
            rows = rowStreamOpener.open(range);
            return;
        }
        Query query = Query.create(TableId.of(destination.getTable())).range(range).filter(filter);
        ServerStream<Row> stream = client.readRows(query);
        rows = stream.iterator();
    }

    @Override
    public void close() {
        rows = Collections.emptyIterator();
        remainingRanges = Collections.emptyIterator();
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @FunctionalInterface
    interface RowStreamOpener extends Serializable {

        Iterator<Row> open(ByteStringRange range);
    }
}
