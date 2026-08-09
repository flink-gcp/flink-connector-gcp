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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.source.BigQueryReadClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Opens {@code ReadRows} calls through a {@link BigQueryReadClient}.
 *
 * <p>Named after the SDK resource its {@link #close()} releases, as this repository's other client
 * wrappers are. One client serves every stream this reader opens: it is the connection pool, and
 * opening one per stream would cost a handshake per split.
 *
 * <p>Creation and release are guarded because they run on different threads — {@link #open} on a
 * split fetcher's, {@link #close} on the task thread once the fetchers are down — and a fetcher
 * generation can start while the previous one is still finishing. Unguarded, the two would leak a
 * client that nothing is left holding. The monitor is this object's own, since a lock field would
 * have to travel in the job graph and {@code Object} is not serializable.
 */
@Internal
public final class ReadClientRowStreamOpener implements RowStreamOpener {

    private static final long serialVersionUID = 1L;

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    private transient BigQueryReadClient client;
    private transient boolean closed;

    /**
     * Creates the opener.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public ReadClientRowStreamOpener(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public RowStream open(String streamName, long offset) throws IOException {
        BigQueryReadClient open;
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The BigQuery read stream opener was closed; the reader is shutting down.");
            }
            if (client == null) {
                client = BigQueryReadClients.create(emulatorEndpoint);
            }
            open = client;
        }
        ReadRowsRequest.Builder request = ReadRowsRequest.newBuilder().setReadStream(streamName);
        if (offset > 0) {
            // Left unset at zero so a fresh read and a resume from the start are the same request.
            request.setOffset(offset);
        }
        return new ServerRowStream(open.readRowsCallable().call(request.build()));
    }

    @Override
    public void close() {
        BigQueryReadClient open;
        synchronized (this) {
            closed = true;
            open = client;
            client = null;
        }
        if (open != null) {
            open.close();
        }
    }

    /** A {@link RowStream} over the SDK's server stream. */
    private static final class ServerRowStream implements RowStream {

        private final ServerStream<ReadRowsResponse> stream;
        private final Iterator<ReadRowsResponse> responses;

        private ServerRowStream(ServerStream<ReadRowsResponse> stream) {
            this.stream = stream;
            this.responses = stream.iterator();
        }

        @Override
        public ReadRowsResponse next() {
            return responses.hasNext() ? responses.next() : null;
        }

        @Override
        public void cancel() {
            stream.cancel();
        }

        @Override
        public void close() {
            // The SDK has no close on a server stream: cancelling is what releases it, and doing so
            // on an already-drained stream is a no-op.
            stream.cancel();
        }
    }
}
