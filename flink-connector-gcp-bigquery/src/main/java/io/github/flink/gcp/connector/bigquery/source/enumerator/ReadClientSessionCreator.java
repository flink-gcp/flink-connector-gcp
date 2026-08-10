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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.source.BigQueryReadClients;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Creates read sessions through a {@link BigQueryReadClient}.
 *
 * <p>Named after the SDK resource its {@link #close()} releases, as this repository's other client
 * wrappers are. The client is opened on first use rather than in the constructor: this object is
 * built where the job graph is, and a client built there would demand credentials on the submitting
 * machine.
 *
 * <p>Creation and release are guarded, and not for tidiness: {@link #create} runs on a coordinator
 * worker thread while {@link #close} runs on the scheduler thread, and the two race whenever a job
 * is cancelled during session creation. Unguarded, {@code close} reads a field the worker has not
 * yet written, closes nothing, and the client — a gRPC channel and its executor — is leaked in the
 * JobManager with nothing left to reach it. The lock is held for the client's construction only,
 * never for the call itself. The monitor is this object's own: a lock field would be one more thing
 * to serialize into the job graph, and {@code Object} is not serializable.
 */
@Internal
public final class ReadClientSessionCreator implements ReadSessionCreator {

    private static final long serialVersionUID = 1L;

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    private transient BigQueryReadClient client;
    private transient boolean closed;

    /**
     * Creates the session creator.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public ReadClientSessionCreator(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public ReadSession create(CreateReadSessionRequest request) throws IOException {
        BigQueryReadClient open;
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The BigQuery read session creator was closed; the source is shutting"
                                + " down.");
            }
            if (client == null) {
                client = BigQueryReadClients.createForSessions(emulatorEndpoint);
            }
            open = client;
        }
        return open.createReadSession(request);
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
}
