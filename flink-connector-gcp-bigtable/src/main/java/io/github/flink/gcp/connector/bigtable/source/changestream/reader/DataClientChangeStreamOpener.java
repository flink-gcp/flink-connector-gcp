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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import io.github.flink.gcp.connector.bigtable.LazyBigtableDataClient;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/** Opens change streams through a lazily created Bigtable data client. */
@Internal
public final class DataClientChangeStreamOpener implements ChangeStreamOpener {

    private static final long serialVersionUID = 1L;

    /**
     * The heartbeat duration asked of the service on every {@code ReadChangeStream} request, which
     * is also the service's own default. Two other things follow from it: a stream that no service
     * topology change closes is rotated out only at a heartbeat, so this is how often {@link
     * BigtableChangeStreamReader} can hand its slot to a queued partition, and {@code
     * missedHeartbeatIntervals} divides an elapsed time by it, so it is that gauge's unit. ADR-0103
     * records why it stays a constant rather than becoming a builder option.
     */
    static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);

    /**
     * The client this opener streams through, and everything around building it. Unlike the scan
     * source's two seams, every call here runs on the reader's task thread — the change-stream
     * reader has no split-fetcher pool — so the holder's thread guarding is inherited rather than
     * demanded by this seam.
     */
    private final LazyBigtableDataClient client;

    /**
     * Creates the opener.
     *
     * @param appProfileId the single-cluster application profile the change stream is read through
     */
    public DataClientChangeStreamOpener(String appProfileId) {
        // The change-stream source has no emulator option, so no endpoint ever reaches this seam.
        this.client = new LazyBigtableDataClient("change-stream opener", appProfileId, null);
    }

    @Override
    public void open(
            TableDestination table,
            ChangeStreamPartitionSplit split,
            @Nullable Instant boundedTimestamp,
            ResponseObserver<ChangeStreamRecord> observer)
            throws IOException {
        client.get(table).readChangeStreamAsync(query(table, split, boundedTimestamp), observer);
    }

    static ReadChangeStreamQuery query(
            TableDestination table,
            ChangeStreamPartitionSplit split,
            @Nullable Instant boundedTimestamp) {
        ReadChangeStreamQuery query =
                ReadChangeStreamQuery.create(table.getTable())
                        .streamPartition(ChangeStreamPartitions.sdkRange(split.getPartition()))
                        .heartbeatDuration(HEARTBEAT_INTERVAL);
        if (split.getContinuationTokens().isEmpty()) {
            query.startTime(split.getLowWatermark());
        } else {
            query.continuationTokens(split.getContinuationTokens());
        }
        if (boundedTimestamp != null) {
            query.endTime(boundedTimestamp);
        }
        return query;
    }

    /**
     * Builds the client settings. Visible to the module's tests because the mapping is otherwise
     * observable only through the client's behaviour: an application profile that never reaches the
     * client looks exactly like one that does.
     */
    @VisibleForTesting
    BigtableDataSettings settings(TableDestination table) throws IOException {
        return client.settings(table);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {
        client.useCredentials(credentials);
    }
}
