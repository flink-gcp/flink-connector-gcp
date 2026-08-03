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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Creates a {@link MutationBatcher} backed by a {@code google-cloud-bigtable} {@link
 * BigtableDataClient}, connecting either to production Bigtable with application-default
 * credentials or to an emulator over a plaintext channel with no credentials.
 *
 * <p>The client's own retry configuration is left alone: it retries {@code MutateRows} per entry
 * for the transient codes already, which is why this sink owns no retry loop.
 */
@Internal
public class DefaultMutationBatcherFactory implements MutationBatcherFactory {

    private static final long serialVersionUID = 1L;

    private final TableDestination destination;
    @Nullable private final String appProfileId;
    private final BigtableWriterOptions writerOptions;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param destination the table every mutation is written to
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param writerOptions the writer tuning options, whose batch thresholds are applied to the
     *     client
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DefaultMutationBatcherFactory(
            TableDestination destination,
            @Nullable String appProfileId,
            BigtableWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.destination = destination;
        this.appProfileId = appProfileId;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public MutationBatcher create() throws IOException {
        BigtableDataClient client = BigtableDataClient.create(settings());
        try {
            // The TargetId overload, not the String one: that one is deprecated. TableId is the
            // TargetId a table has; authorized views are the other one and are out of scope here.
            return new BigtableBatcherAdapter(
                    client, client.newBulkMutationBatcher(TableId.of(destination.getTable())));
        } catch (RuntimeException e) {
            // The client is owned here until the adapter takes it over on success.
            client.close();
            throw e;
        }
    }

    /**
     * Builds the client settings this factory would connect with. Separate from {@link #create()},
     * and visible to the module's tests, because the options-to-settings mapping is otherwise only
     * observable through the client's behaviour: a threshold that never reaches the client looks
     * exactly like one that does. The same reasoning put every other connector's options mapping
     * under a unit test of its own.
     */
    @VisibleForTesting
    BigtableDataSettings settings() {
        BigtableDataSettings.Builder settings = newSettingsBuilder();
        settings.setProjectId(destination.getProject()).setInstanceId(destination.getInstance());
        if (appProfileId != null) {
            settings.setAppProfileId(appProfileId);
        }
        applyBatchThresholds(settings);
        return settings.build();
    }

    private BigtableDataSettings.Builder newSettingsBuilder() {
        if (emulatorEndpoint == null) {
            return BigtableDataSettings.newBuilder();
        }
        return BigtableDataSettings.newBuilderForEmulator(
                emulatorEndpoint.getHost(), emulatorEndpoint.getPort());
    }

    /**
     * Applies the configured batch thresholds, leaving the client's own default in place for each
     * one left unset — so a client upgrade that retunes it is inherited rather than overridden.
     */
    private void applyBatchThresholds(BigtableDataSettings.Builder settings) {
        Long elementCount = writerOptions.getBatchElementCount();
        Long byteSize = writerOptions.getBatchByteSize();
        if (elementCount == null && byteSize == null) {
            return;
        }
        BigtableBatchingCallSettings.Builder bulkMutateRows =
                settings.stubSettings().bulkMutateRowsSettings();
        BatchingSettings.Builder batching = bulkMutateRows.getBatchingSettings().toBuilder();
        if (elementCount != null) {
            batching.setElementCountThreshold(elementCount);
        }
        if (byteSize != null) {
            batching.setRequestByteThreshold(byteSize);
        }
        bulkMutateRows.setBatchingSettings(batching.build());
    }

    /** Adapts the client's bulk mutation {@link Batcher} to the writer-facing interface. */
    private static final class BigtableBatcherAdapter implements MutationBatcher {

        private final BigtableDataClient client;
        private final Batcher<RowMutationEntry, Void> batcher;

        private BigtableBatcherAdapter(
                BigtableDataClient client, Batcher<RowMutationEntry, Void> batcher) {
            this.client = client;
            this.batcher = batcher;
        }

        @Override
        public ApiFuture<Void> add(RowMutationEntry entry) {
            return batcher.add(entry);
        }

        @Override
        public void sendOutstanding() {
            batcher.sendOutstanding();
        }

        @Override
        public void close() throws Exception {
            try {
                // Sends what is buffered and waits for it. No timeout: a bounded one would abandon
                // mutations the service may still apply, and the writer's own flush has already
                // drained everything on the success path.
                batcher.close();
            } finally {
                client.close();
            }
        }
    }
}
