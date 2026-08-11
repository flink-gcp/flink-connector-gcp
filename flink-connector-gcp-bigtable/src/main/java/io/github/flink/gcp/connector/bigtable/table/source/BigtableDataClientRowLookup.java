/*
 * Copyright 2026 laughingman7743
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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import javax.annotation.Nullable;

import java.util.List;

/** Point reads through a {@link BigtableDataClient}, owned by one lookup function instance. */
@Internal
final class BigtableDataClientRowLookup implements BigtableRowLookup {

    private static final long serialVersionUID = 1L;

    private final TableDestination destination;
    private final Filters.Filter filter;
    private final List<ByteStringRange> ranges;
    @Nullable private final String appProfileId;
    @Nullable private final String emulatorEndpoint;

    @Nullable private transient BigtableDataClient client;

    BigtableDataClientRowLookup(
            TableDestination destination,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String emulatorEndpoint) {
        this.destination = destination;
        this.filter = filter;
        this.ranges = ranges;
        this.appProfileId = appProfileId;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public void open() throws Exception {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null ? null : EmulatorEndpoint.parse(emulatorEndpoint);
        client =
                BigtableDataClient.create(
                        BigtableDataClients.settings(destination, appProfileId, endpoint).build());
    }

    @Override
    public Row read(ByteString rowKey) {
        if (!isInRange(rowKey)) {
            return null;
        }
        return client().readRow(TableId.of(destination.getTable()), rowKey, filter);
    }

    @Override
    public ApiFuture<Row> readAsync(ByteString rowKey) {
        if (!isInRange(rowKey)) {
            return ApiFutures.immediateFuture(null);
        }
        return client().readRowAsync(TableId.of(destination.getTable()), rowKey, filter);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private BigtableDataClient client() {
        if (client == null) {
            throw new IllegalStateException("The Bigtable row lookup has not been opened.");
        }
        return client;
    }

    private boolean isInRange(ByteString rowKey) {
        return ranges.stream().anyMatch(range -> RowRanges.contains(range, rowKey));
    }
}
