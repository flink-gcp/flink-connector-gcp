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
import org.apache.flink.annotation.VisibleForTesting;

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

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private final int retryMaxAttempts;

    private transient BigQueryReadClient client;
    private transient boolean closed;

    /**
     * Volatile because it is written on the task thread by {@link #setRetryListener} and read on a
     * split fetcher's when the client is built. The write happens before any fetcher starts, so the
     * ordering is already established; the modifier says so rather than leaving it to be
     * re-derived.
     */
    @Nullable private transient volatile Runnable onRetry;

    /**
     * Creates the opener.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     * @param retryMaxAttempts the bound put on the client's own {@code ReadRows} retry
     */
    public ReadClientRowStreamOpener(
            @Nullable EmulatorEndpoint emulatorEndpoint, int retryMaxAttempts) {
        this(null, emulatorEndpoint, retryMaxAttempts);
    }

    /**
     * Creates the opener.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     * @param retryMaxAttempts the bound put on the client's own {@code ReadRows} retry
     */
    public ReadClientRowStreamOpener(
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            int retryMaxAttempts) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.retryMaxAttempts = retryMaxAttempts;
    }

    @Override
    public void setRetryListener(Runnable onRetry) {
        this.onRetry = onRetry;
    }

    /**
     * Returns the bound this opener puts on the client's own {@code ReadRows} retry.
     *
     * <p>{@code public} only because the builder's own tests live a package away and the value is
     * otherwise unobservable — a builder that accepted the knob and dropped it would look exactly
     * like one that did not. The same argument {@code BoundedShutdown#timeout()} is public under; a
     * third seam of this shape should cite one of them rather than widen by default.
     *
     * @return the bound
     */
    @VisibleForTesting
    public int retryMaxAttempts() {
        return retryMaxAttempts;
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
                client =
                        BigQueryReadClients.createForReads(
                                serviceAccountKeyFile, emulatorEndpoint, retryMaxAttempts, onRetry);
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
