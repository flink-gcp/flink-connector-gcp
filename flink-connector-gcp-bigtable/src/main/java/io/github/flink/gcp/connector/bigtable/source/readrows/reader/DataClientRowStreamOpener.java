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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.LazyBigtableDataClient;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Opens reads through a {@code google-cloud-bigtable} {@link BigtableDataClient}.
 *
 * <p>Named after the client its {@link #close()} releases, which is the connector's convention for
 * the real implementation of a seam.
 *
 * <p><b>The {@code Query} this opener builds does not outlive the call that builds it.</b> One is
 * built per open and thrown away, never stored and never checkpointed: a {@code Query} cannot be
 * read back — its target id accessor is internal, it exposes no row set, and its bound is the
 * minimal range enclosing everything it holds — so a split carrying one could not be logged,
 * truncated at the last emitted key, or compared. Checkpointed state also has to have a byte format
 * this connector owns rather than one a client upgrade can move.
 *
 * <p>No row limit is ever set. A limit is global to a query, so it cannot be partitioned across
 * splits without coordination; the client library says the same thing from the other side, by
 * refusing to shard a query that carries one.
 *
 * <p>The client's retry configuration is left alone: it resumes a broken {@code ReadRows} stream
 * from the last key it saw, transparently, which is why this connector owns no retry loop for a
 * read.
 */
@Internal
public final class DataClientRowStreamOpener implements RowStreamOpener {

    private static final long serialVersionUID = 1L;

    /**
     * The client this opener reads through, and everything around building it: a fetcher thread
     * opens a stream through it while {@link #close()} runs on the task thread once the fetchers
     * are down, and a fetcher generation can start while the previous one is still finishing.
     */
    private final LazyBigtableDataClient client;

    /**
     * Creates the opener.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DataClientRowStreamOpener(
            @Nullable String appProfileId, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(appProfileId, emulatorEndpoint, null);
    }

    public DataClientRowStreamOpener(
            @Nullable String appProfileId,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.client =
                new LazyBigtableDataClient(
                        "row stream opener", appProfileId, emulatorEndpoint, serviceAccountKeyFile);
    }

    @Override
    public RowStream open(
            TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter)
            throws IOException {
        Query query = Query.create(TableId.of(table.getTable())).range(range);
        if (filter != null) {
            query = query.filter(filter);
        }
        return new ServerRowStream(client.get(table).readRows(query));
    }

    @Override
    public void close() throws IOException {
        client.close();
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

    /** Loads credentials when a TaskManager creates the source reader. */
    public void loadCredentials() throws IOException {
        client.loadCredentials();
    }

    /** Supplies a runtime provider directly for settings-level tests. */
    @VisibleForTesting
    void setCredentialsOverride(@Nullable CredentialsProvider credentialsOverride) {
        client.setCredentialsOverride(credentialsOverride);
    }

    /**
     * A {@link RowStream} over the client's {@code ServerStream}.
     *
     * <p>The cancel on close is conditional because the client's contract is that a stream is
     * "either fully consumed or cancelled": once the iterator has reported the end, there is
     * nothing left to cancel, and asking anyway would be a call made about a call that has already
     * finished.
     *
     * <p>{@code exhausted} is volatile because the read that sets it runs on a fetcher thread while
     * a wake-up's close reads it on the task thread. A stale read there would cancel a stream that
     * had already ended — harmless against this client, but not a thing to leave to luck.
     */
    private static final class ServerRowStream implements RowStream {

        private final ServerStream<Row> stream;
        private final Iterator<Row> rows;

        private volatile boolean exhausted;

        private ServerRowStream(ServerStream<Row> stream) {
            this.stream = stream;
            this.rows = stream.iterator();
        }

        @Override
        @Nullable
        public Row next() {
            if (exhausted || !rows.hasNext()) {
                exhausted = true;
                return null;
            }
            return rows.next();
        }

        @Override
        public void close() {
            if (!exhausted) {
                stream.cancel();
            }
        }
    }
}
