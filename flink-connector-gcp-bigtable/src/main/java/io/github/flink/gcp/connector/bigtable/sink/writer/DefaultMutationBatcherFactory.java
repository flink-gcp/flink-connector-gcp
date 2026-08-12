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
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Creates {@link MutationBatcher}s backed by {@code google-cloud-bigtable} {@link
 * BigtableDataClient}s, connecting either to production Bigtable with application-default
 * credentials or to an emulator over a plaintext channel with no credentials.
 *
 * <p>The client's own retry configuration is left alone: it retries {@code MutateRows} per entry
 * for the transient codes already, which is why this sink owns no retry loop.
 *
 * <h2>One batcher per table, one client per instance</h2>
 *
 * <p>A bulk mutation batcher is bound to one table, so a writer with per-record destinations needs
 * one per table. A {@link BigtableDataClient} is not: it is built for a (project, instance) pair
 * and hands out a batcher for any table in it, while holding a channel pool and a background
 * executor. So the clients are cached per instance and shared by the tables under it, and a
 * batcher's close releases <em>only</em> the batcher — {@link #close()} is what releases the
 * clients, once the writer has closed every batcher it created.
 *
 * <p>That is why {@code BigtableBatcherAdapter} holds no client, unlike the shape #324 left: a
 * batcher that closed the client it was built over would tear down every sibling batcher of the
 * same instance, and since {@link BigtableDataClient} reports nothing about having been closed, the
 * survivors would fail on their next send rather than at the close that broke them.
 */
@Internal
public class DefaultMutationBatcherFactory implements MutationBatcherFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMutationBatcherFactory.class);

    private static final long serialVersionUID = 1L;

    @Nullable private final String appProfileId;
    private final BigtableWriterOptions writerOptions;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final CredentialsProvider credentialsOverride;

    /**
     * The clients built so far, one per (project, instance), in creation order so a close reports
     * the first failure of a deterministic sequence.
     *
     * <p>Transient and lazily created: the factory is serialized into the job graph, and a client
     * is runtime state each subtask builds for itself. Touched only from the Flink task thread, as
     * the SPI requires.
     */
    @Nullable private transient Map<String, BigtableDataClient> clients;

    /**
     * Creates the factory.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param writerOptions the writer tuning options, whose batch thresholds are applied to the
     *     client
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DefaultMutationBatcherFactory(
            @Nullable String appProfileId,
            BigtableWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(appProfileId, writerOptions, emulatorEndpoint, null);
    }

    public DefaultMutationBatcherFactory(
            @Nullable String appProfileId,
            BigtableWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        this.appProfileId = appProfileId;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
    }

    @Override
    public MutationBatcher create(TableDestination destination) throws IOException {
        return create(destination, client(destination));
    }

    /**
     * Returns the client for the destination's instance, building it on first use.
     *
     * <p>Keyed by {@code project/instance} as a plain string, which is unambiguous because {@link
     * TableDestination} rejects a component containing {@code '/'}.
     *
     * <p>Package-private rather than private so a test can assert the sharing directly: two tables
     * of one instance must get the same client, and a table of another instance must not. Nothing
     * about a {@link BigtableDataClient} reports which one it is, so identity here is the only
     * observable — the alternative is the shape #232 exists to avoid, where the first batcher to
     * close takes its instance's siblings down and no test can see it.
     */
    @VisibleForTesting
    BigtableDataClient client(TableDestination destination) throws IOException {
        if (clients == null) {
            clients = new LinkedHashMap<>();
        }
        String key = destination.getProject() + "/" + destination.getInstance();
        BigtableDataClient client = clients.get(key);
        if (client == null) {
            // Built, then put: a failure leaves no entry, so the next record retries the creation
            // rather than finding a half-built cache.
            client = BigtableDataClient.create(settings(destination));
            clients.put(key, client);
        }
        return client;
    }

    /**
     * Wraps a batcher over a client this factory holds.
     *
     * <p>Package-private, and taking the client, so a test can hand in a client it keeps hold of.
     * Nothing else can: {@link BigtableDataClient} reports no closed state, so a factory whose
     * close released some other closeable would pass every test that injects its own — while
     * leaking a channel pool and an executor per instance in production.
     */
    @VisibleForTesting
    MutationBatcher create(TableDestination destination, BigtableDataClient client) {
        // The TargetId overload, not the String one: that one is deprecated. TableId is the
        // TargetId a table has; authorized views are the other one and are out of scope here.
        //
        // No guard closing the client on a failure here, unlike the shape this method had while it
        // built a client of its own: the client belongs to the cache and to sibling batchers, and
        // close() releases it whether or not this call succeeded.
        return new BigtableBatcherAdapter(
                destination, client.newBulkMutationBatcher(TableId.of(destination.getTable())));
    }

    /**
     * Closes every client this factory built. Called by the writer after every batcher has been
     * closed.
     *
     * <p>Through {@link Closers#closeAll} so one client failing to close cannot strand the rest,
     * and the map is cleared whatever happens: a factory that reported a failure must not hand a
     * closed client to a later {@link #create(TableDestination)}.
     */
    @Override
    public void close() throws Exception {
        if (clients == null) {
            return;
        }
        List<AutoCloseable> closeables = new ArrayList<>(clients.values());
        clients = null;
        Closers.closeAll(closeables);
    }

    /**
     * Builds the client settings this factory connects to the destination's instance with. Separate
     * from {@link #create(TableDestination)}, and visible to the module's tests, because the
     * options-to-settings mapping is otherwise only observable through the client's behaviour: a
     * threshold that never reaches the client looks exactly like one that does. The same reasoning
     * put every other connector's options mapping under a unit test of its own.
     *
     * <p>Everything both directions of this connector share — the emulator-versus-credentials
     * branch, the project and instance, the application profile — comes from {@link
     * BigtableDataClients}. Only the batch thresholds are the sink's own.
     */
    @VisibleForTesting
    BigtableDataSettings settings(TableDestination destination) {
        BigtableDataSettings.Builder settings =
                BigtableDataClients.settings(
                        destination, appProfileId, emulatorEndpoint, credentialsOverride);
        applyBatchThresholds(settings);
        return settings.build();
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

    /**
     * Shuts the batcher down, absorbing the one exception its shutdown reports by design.
     *
     * <p>gax's {@code BatcherImpl.close()} ends by throwing a {@code BatchingException} built from
     * {@code BatcherStats}, which accumulates <em>every entry failure of the batcher's
     * lifetime</em> and is never cleared by consuming an entry's future. The writer consumes all of
     * them — each one was classified and either handed to the failure handler or captured as fatal
     * when its own future completed — so the report re-states failures this sink has already
     * applied its policy to, and letting it out fails a job whose {@code logAndDrop} policy had
     * kept it running (#238). Nothing else is caught: an {@code InterruptedException} from the wait
     * and gax's own {@code IllegalStateException("unexpected error closing the batcher")} are
     * failures of the shutdown itself rather than a repeat of what the writer handled.
     *
     * <p>Absorbed rather than narrowed to the failures raised after the shutdown began, because
     * neither of the two ways to narrow it exists. gax's side is shut: {@code BatcherStats} is
     * package-private with no reset and no accessor, and {@code close(Duration)} rebuilds the
     * exception as {@code new BatchingException(cause.getMessage())}, so nothing per-entry survives
     * to recover from. This sink's side would mean draining its own in-flight set before shutting
     * down, and by then the mailbox those completions run on is quiesced — {@code
     * StreamTask.afterInvoke()} calls {@code prepareClose()} before {@code closeAllOperators()},
     * after which a {@code put} is rejected while a {@code take} still blocks, so the drain would
     * park the task thread forever rather than finish.
     *
     * <p>That same quiescing is what leaves this log line worth writing. A batch first sent from
     * inside the shutdown can still fail, and its completion callback can no longer reach the
     * mailbox, so the failure reaches neither the handler nor the writer's captured error: the
     * report absorbed here is its only record. At-least-once covers the mutation itself — a
     * shutdown carrying unsent work only happens on a path that is already ending the job.
     */
    @VisibleForTesting
    static void shutDownAbsorbingTheLifetimeFailureReport(
            TableDestination destination, ThrowingRunnable<Exception> shutdown) throws Exception {
        try {
            shutdown.run();
        } catch (BatchingException e) {
            LOG.warn(
                    "The Bigtable batcher for table {} reported its accumulated entry failures as"
                            + " it shut down. Each one was already classified and routed when its"
                            + " own future completed, so this is a repeat of what the sink acted on"
                            + " — unless it names a mutation sent by the shutdown itself, for which"
                            + " this line is the only report, since completions can no longer run"
                            + " once the task mailbox is quiesced.",
                    destination,
                    e);
        }
    }

    /**
     * Adapts the client's bulk mutation {@link Batcher} to the writer-facing interface.
     *
     * <p>The batcher's four operations are held as functional values, rather than as the SDK type,
     * <b>because that is the only seam a test can drive</b> (#324). {@code Batcher} is
     * {@code @InternalExtensionOnly} — the reason {@link MutationBatcher} exists as this module's
     * own SPI — so a fake must not implement it.
     *
     * <p>It holds no client (#232). The client is shared by every table of an instance and is the
     * factory's to release; an adapter that closed it would take its siblings down with it.
     */
    @VisibleForTesting
    static final class BigtableBatcherAdapter implements MutationBatcher {

        private final TableDestination destination;
        private final Function<RowMutationEntry, ApiFuture<Void>> batcherAdd;
        private final Runnable batcherSendOutstanding;
        private final Runnable batcherShutdown;
        private final ThrowingRunnable<Exception> batcherClose;

        /**
         * The production shape. Kept beside the injectable one, and delegating to it, so that the
         * four method references binding this adapter to one batcher live inside the class a test
         * can construct rather than at the {@link #create(TableDestination)} call site, where
         * nothing would reach them.
         *
         * <p>{@code closeAsync} is what {@link #shutdown()} binds to, and gax makes the pair
         * compose: {@code closeAsync()} sends what is buffered, refuses further mutations and
         * returns the one future it memoizes, and {@code close()} waits on that same future and
         * still raises the lifetime report the absorb above expects (measured against gax 2.82.0,
         * {@code BatcherImpl.closeAsync}/{@code close}). So calling both costs one wait, not two,
         * and calling only {@code close()} still works.
         */
        BigtableBatcherAdapter(
                TableDestination destination, Batcher<RowMutationEntry, Void> batcher) {
            this(
                    destination,
                    batcher::add,
                    batcher::sendOutstanding,
                    batcher::closeAsync,
                    batcher::close);
        }

        /** Package-private so a test can script a shutdown that throws; see the class javadoc. */
        @VisibleForTesting
        BigtableBatcherAdapter(
                TableDestination destination,
                Function<RowMutationEntry, ApiFuture<Void>> batcherAdd,
                Runnable batcherSendOutstanding,
                Runnable batcherShutdown,
                ThrowingRunnable<Exception> batcherClose) {
            this.destination = destination;
            this.batcherAdd = batcherAdd;
            this.batcherSendOutstanding = batcherSendOutstanding;
            this.batcherShutdown = batcherShutdown;
            this.batcherClose = batcherClose;
        }

        @Override
        public ApiFuture<Void> add(RowMutationEntry entry) {
            return batcherAdd.apply(entry);
        }

        @Override
        public void sendOutstanding() {
            batcherSendOutstanding.run();
        }

        @Override
        public void shutdown() {
            batcherShutdown.run();
        }

        @Override
        public void close() throws Exception {
            // The close sends what is buffered and waits for it. No timeout: a bounded one would
            // abandon mutations the service may still apply, and the writer's own flush has already
            // drained everything on the success path. shutdown() above is what keeps that unbounded
            // wait from being paid once per table.
            try {
                shutDownAbsorbingTheLifetimeFailureReport(destination, batcherClose);
            } catch (InterruptedException e) {
                // gax's wait clears the flag when it throws, and BigtableWriter.close() collects
                // this through its own Closers.closeAll and carries on to the next entry — the
                // failure handler's close, which is fully pluggable and may itself be a
                // BoundedShutdown that would then spend its whole budget instead of honouring the
                // cancellation. Same restore, same reason, as BoundedShutdown.close()'s.
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }
}
