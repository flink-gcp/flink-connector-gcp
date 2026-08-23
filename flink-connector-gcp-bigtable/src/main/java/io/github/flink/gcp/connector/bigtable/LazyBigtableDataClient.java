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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * A {@link BigtableDataClient} built on first use and closed once, held by a seam implementation
 * that travels in the job graph.
 *
 * <p>The seams that read through the data client — the scan source's row-stream opener and row-key
 * sampler, and the change-stream source's opener — differ in the one call they make and share
 * everything around it: the settings they build, the provider their owner hands them, the lazy
 * construction and the close. That shared half lives here so that no seam owns a private copy of
 * client lifecycle code.
 *
 * <p>No key-file path travels here. The runtime component that owns the seam loads one provider for
 * every client family it owns and pushes it in, so this holder never loads a second one.
 *
 * <p>The client is {@code transient} because the reader-side holders, {@code
 * DataClientRowStreamOpener} and {@code DataClientChangeStreamOpener}, are serialized into the job
 * graph — the enumerator-side holder is not any more, since {@code docs/adr/0128} mints one sampler
 * per enumerator, so for that owner the marker is inert rather than load-bearing. It is {@code
 * volatile} because the thread that builds it may not be the thread that closes it, and built under
 * this object's monitor rather than a lock field, because a lock field would have to travel in the
 * job graph too. Which threads those are depends on the seam: a scan reader's split fetchers open
 * streams from their own threads while {@code close()} runs on the task thread once the fetchers
 * are down; an enumerator samples from the executor {@code SplitEnumeratorContext#callAsync} hands
 * the work to while {@code close()} runs on the coordinator thread; the change-stream reader has no
 * fetcher pool and opens and closes on its one task thread, inheriting the guarding rather than
 * needing it.
 *
 * <p>The holder does not lease a client returned by {@link #get(TableDestination)}. Synchronizing
 * the return would still let {@code close()} run before the caller starts its operation, while
 * holding the monitor for an operation would make coordinator teardown wait for a service round
 * trip. Each owner therefore supplies the operation lifecycle: the scan reader stops its fetchers
 * before closing the opener, the change-stream reader cancels its active reads first, and an
 * enumerator ignores a sampling completion that arrives after teardown.
 */
@Internal
public final class LazyBigtableDataClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String owner;
    @Nullable private final String appProfileId;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    @Nullable private transient volatile BigtableDataClient client;

    /**
     * What the owning component loaded and pushed in.
     *
     * <p>Production injection, client construction and clearing all take this object's monitor.
     * {@code volatile} also makes the value visible to the direct test-only {@link
     * #settings(TableDestination)} accessor, which deliberately does not acquire that monitor.
     */
    @Nullable private transient volatile CredentialsProvider credentials;

    private transient volatile boolean closed;

    /**
     * Creates the holder.
     *
     * @param owner how the holder names its seam in the closed-before-use failure, for example
     *     {@code "row stream opener"}
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public LazyBigtableDataClient(
            String owner,
            @Nullable String appProfileId,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.owner = Preconditions.checkNotNull(owner, "owner must not be null");
        this.appProfileId = appProfileId;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the client, building it on first use. */
    public BigtableDataClient get(TableDestination table) throws IOException {
        return get(table, BigtableDataClient::create);
    }

    /**
     * Returns the client through an injected creator that is neither retained nor serialized.
     *
     * <p>This seam lets the lifecycle test hold construction inside this object's monitor while a
     * concurrent close tries to acquire it.
     */
    @VisibleForTesting
    BigtableDataClient get(TableDestination table, DataClientCreator creator) throws IOException {
        BigtableDataClient existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The Bigtable "
                                + owner
                                + " for "
                                + table
                                + " was closed before it was"
                                + " used.");
            }
            if (client == null) {
                client = creator.create(settings(table));
            }
            return client;
        }
    }

    /**
     * Builds the client settings.
     *
     * <p>The method is public so module tests can inspect the mapping directly. That direct test
     * access deliberately performs no lifecycle check: holder tests use it after close to observe
     * credential clearing without constructing a client. Production operations reach the method
     * only through {@link #get(TableDestination)}, which refuses use after close.
     */
    @VisibleForTesting
    public BigtableDataSettings settings(TableDestination table) throws IOException {
        return BigtableDataClients.settings(table, appProfileId, emulatorEndpoint, credentials)
                .build();
    }

    /**
     * Takes the provider the seam's owner loaded, or {@code null} to keep ADC.
     *
     * <p>The owner supplies credentials before first use and before close. Injection waits for any
     * client construction already holding this object's monitor.
     *
     * @throws IllegalStateException if the holder is already closed
     */
    public synchronized void useCredentials(@Nullable CredentialsProvider credentials) {
        Preconditions.checkState(
                !closed, "The Bigtable %s was closed before credentials were supplied.", owner);
        this.credentials = credentials;
    }

    /** Closes the client if one was built, clears credentials, and refuses later use. */
    public void close() throws IOException {
        BigtableDataClient toClose;
        synchronized (this) {
            closed = true;
            toClose = client;
            client = null;
            credentials = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    /** Creates a data client from settings prepared by this holder. */
    @FunctionalInterface
    interface DataClientCreator {

        BigtableDataClient create(BigtableDataSettings settings) throws IOException;
    }
}
