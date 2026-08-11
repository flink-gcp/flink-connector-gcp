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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

/** Opens change streams through a lazily created Bigtable data client. */
@Internal
public final class DataClientChangeStreamOpener implements ChangeStreamOpener {

    private static final long serialVersionUID = 1L;

    private final String appProfileId;
    @Nullable private transient volatile BigtableDataClient client;
    private transient volatile boolean closed;

    public DataClientChangeStreamOpener(String appProfileId) {
        this.appProfileId = appProfileId;
    }

    @Override
    public ChangeStream open(
            TableDestination table, ChangeStreamPartitionSplit split, @Nullable Instant endTime)
            throws IOException {
        ReadChangeStreamQuery query =
                ReadChangeStreamQuery.create(table.getTable())
                        .streamPartition(split.getPartition());
        if (split.getContinuationTokens().isEmpty()) {
            query.startTime(split.getLowWatermark());
        } else {
            query.continuationTokens(split.getContinuationTokens());
        }
        if (endTime != null) {
            query.endTime(endTime);
        }
        return new ServerChangeStream(client(table).readChangeStream(query));
    }

    private BigtableDataClient client(TableDestination table) throws IOException {
        BigtableDataClient existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new IOException("The Bigtable change-stream opener was already closed.");
            }
            if (client == null) {
                client =
                        BigtableDataClient.create(
                                BigtableDataClients.settings(table, appProfileId, null).build());
            }
            return client;
        }
    }

    @Override
    public void close() {
        BigtableDataClient toClose;
        synchronized (this) {
            closed = true;
            toClose = client;
            client = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    private static final class ServerChangeStream implements ChangeStream {
        private final ServerStream<ChangeStreamRecord> stream;
        private final Iterator<ChangeStreamRecord> iterator;
        private boolean ended;

        private ServerChangeStream(ServerStream<ChangeStreamRecord> stream) {
            this.stream = stream;
            this.iterator = stream.iterator();
        }

        @Override
        @Nullable
        public ChangeStreamRecord next() {
            if (!iterator.hasNext()) {
                ended = true;
                return null;
            }
            return iterator.next();
        }

        @Override
        public void cancel() {
            stream.cancel();
        }

        @Override
        public void close() {
            if (!ended) {
                stream.cancel();
            }
        }
    }
}
