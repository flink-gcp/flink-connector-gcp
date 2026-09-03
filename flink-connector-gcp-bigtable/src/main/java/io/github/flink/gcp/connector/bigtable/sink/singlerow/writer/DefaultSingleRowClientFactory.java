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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableClientReaper;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The production {@link SingleRowClientFactory}: one {@link BigtableDataClient} per (project,
 * instance), shared by that instance's tables, with both request-response RPCs pinned to a single
 * attempt under the configured deadline.
 *
 * <p>The client pool has the shape of the batching sink's {@code DefaultMutationBatcherFactory},
 * and for the same reasons (ADR-0145): a client owns a channel pool and an executor, so one per
 * table would multiply both by the number of tables a job writes to; leases are counted per
 * instance so the client closes with its last table; permits come from a {@link
 * BigtableClientReaper}, which also closes released clients off the task thread and bounds the
 * open-or-closing count at {@code maxActiveInstances}.
 *
 * <h2>What the settings do and do not touch</h2>
 *
 * <p>{@link #settings(TableDestination)} takes everything the connector's data clients share from
 * {@link BigtableDataClients} and then applies exactly one thing of this family's own: {@code
 * requestTimeout} as the whole of a single attempt, on {@code checkAndMutateRowSettings()} and
 * {@code readModifyWriteRowSettings()}. The client's own defaults for both are an empty
 * retryable-code set and a 20-second total timeout (google-cloud-bigtable 2.81.0, measured
 * 2026-09-03 and pinned by {@code DefaultSingleRowClientFactoryTest}), so the setting changes the
 * number and not the shape: no retry is added, because neither RPC is idempotent and a retried
 * ambiguous failure could apply an increment twice (ADR-0148). The runtime adds no timer of its own
 * either — the client's deadline is the only timeout, and a request past it fails with the {@code
 * DEADLINE_EXCEEDED} the runtime reads as ambiguous.
 */
@Internal
public class DefaultSingleRowClientFactory implements SingleRowClientFactory {

    private static final long serialVersionUID = 1L;

    @Nullable private final String appProfileId;
    private final BigtableRequestOptions options;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final CredentialsProvider credentialsOverride;

    /**
     * The clients built so far, one per (project, instance), in creation order so a close reports
     * the first failure of a deterministic sequence. Transient and lazily created: the factory is
     * serialized into the job graph, and a client is runtime state each subtask builds for itself.
     * Touched only from the task thread.
     */
    @Nullable private transient Map<String, ClientState> clients;

    /** Lazily created with the runtime clients; never serialized into the job graph. */
    @Nullable private transient BigtableClientReaper clientReaper;

    /**
     * Creates the factory.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param options the runtime options, whose {@code requestTimeout} and {@code
     *     maxActiveInstances} this factory applies
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     * @param credentialsOverride the runtime-loaded service-account provider, or {@code null} to
     *     preserve application-default credentials
     */
    public DefaultSingleRowClientFactory(
            @Nullable String appProfileId,
            BigtableRequestOptions options,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        this.appProfileId = appProfileId;
        this.options = options;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
    }

    @Override
    public SingleRowClient create(TableDestination destination)
            throws IOException, InterruptedException {
        ClientState state = clientState(destination);
        state.liveTables++;
        return state.adapter;
    }

    /**
     * Returns the data client for the destination's instance, building it on first use.
     *
     * <p>Package-private so a test can assert the sharing directly: two tables of one instance must
     * get the same client, and a table of another instance must not. Nothing about a {@link
     * BigtableDataClient} reports which one it is, so identity is the only observable.
     */
    @VisibleForTesting
    BigtableDataClient client(TableDestination destination)
            throws IOException, InterruptedException {
        return clientState(destination).client;
    }

    private ClientState clientState(TableDestination destination)
            throws IOException, InterruptedException {
        if (clients == null) {
            clients = new LinkedHashMap<>();
        }
        String key = BigtableDataClients.instanceKey(destination);
        ClientState state = clients.get(key);
        if (state == null) {
            BigtableClientReaper reaper = clientReaper();
            reaper.acquireSlot();
            // Built, then put: a failure leaves no entry, so the next record retries the creation
            // rather than finding a half-built cache.
            try {
                state = new ClientState(BigtableDataClient.create(settings(destination)));
            } catch (IOException | RuntimeException | Error e) {
                reaper.releaseUnusedSlot();
                throw e;
            }
            clients.put(key, state);
        }
        return state;
    }

    @Override
    public void release(TableDestination destination) {
        if (clients == null) {
            throw new IllegalStateException(
                    "No Bigtable client exists while releasing table " + destination + ".");
        }
        String key = BigtableDataClients.instanceKey(destination);
        ClientState state = clients.get(key);
        if (state == null || state.liveTables <= 0) {
            throw new IllegalStateException(
                    "No Bigtable client lease exists while releasing table " + destination + ".");
        }
        state.liveTables--;
        if (state.liveTables != 0) {
            return;
        }
        clients.remove(key);
        clientReaper().closeEventually(state.client, "evicted Bigtable client " + key);
    }

    @VisibleForTesting
    int activeClientCount() {
        return clients == null ? 0 : clients.size();
    }

    @VisibleForTesting
    void awaitReleasedClients() throws InterruptedException {
        if (clientReaper != null) {
            clientReaper.awaitIdle();
        }
    }

    private BigtableClientReaper clientReaper() {
        if (clientReaper == null) {
            clientReaper = new BigtableClientReaper(options.getMaxActiveInstances());
        }
        return clientReaper;
    }

    /**
     * Closes every client this factory built. The reaper starts every remaining close before
     * waiting for any, so the clients' bounded final metric exports overlap; one client failing to
     * close cannot strand the rest, and the map is cleared whatever happens.
     */
    @Override
    public void close() throws Exception {
        if (clients == null && clientReaper == null) {
            return;
        }
        List<AutoCloseable> closeables = new ArrayList<>();
        if (clients != null) {
            for (ClientState state : clients.values()) {
                closeables.add(state.client);
            }
        }
        clients = null;
        BigtableClientReaper reaper = clientReaper;
        clientReaper = null;
        if (reaper != null) {
            reaper.closeAll(closeables);
        }
    }

    /**
     * Builds the client settings this factory connects to the destination's instance with. Visible
     * to the module's tests because the options-to-settings mapping is otherwise only observable
     * through the client's behaviour: a deadline that never reaches the client looks exactly like
     * one that does, until a request hangs for the client's default instead.
     */
    @VisibleForTesting
    BigtableDataSettings settings(TableDestination destination) {
        BigtableDataSettings.Builder settings =
                BigtableDataClients.settings(
                        destination, appProfileId, emulatorEndpoint, credentialsOverride);
        Duration requestTimeout = options.getRequestTimeout();
        // One attempt, whose total is the configured deadline: sets the total, initial and maximum
        // RPC timeouts to the same value and maxAttempts to 1, and clears the retryable codes —
        // which the client ships empty for both RPCs anyway; the test holds both.
        settings.stubSettings()
                .checkAndMutateRowSettings()
                .setSimpleTimeoutNoRetriesDuration(requestTimeout);
        settings.stubSettings()
                .readModifyWriteRowSettings()
                .setSimpleTimeoutNoRetriesDuration(requestTimeout);
        return settings.build();
    }

    private static final class ClientState {

        private final BigtableDataClient client;
        private final SingleRowClient adapter;
        private int liveTables;

        private ClientState(BigtableDataClient client) {
            this.client = client;
            this.adapter = new Adapter(client);
        }
    }

    /** The production {@link SingleRowClient}: the two async calls of the data client. */
    private static final class Adapter implements SingleRowClient {

        private final BigtableDataClient client;

        private Adapter(BigtableDataClient client) {
            this.client = client;
        }

        @Override
        public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation) {
            return client.checkAndMutateRowAsync(mutation);
        }

        @Override
        public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
            return client.readModifyWriteRowAsync(mutation);
        }
    }
}
