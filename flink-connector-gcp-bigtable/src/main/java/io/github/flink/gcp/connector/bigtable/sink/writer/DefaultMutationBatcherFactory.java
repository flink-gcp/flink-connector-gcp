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
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;

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

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMutationBatcherFactory.class);

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
        return create(BigtableDataClient.create(settings()));
    }

    /**
     * Wraps a client this factory would otherwise have built itself, taking ownership of it.
     *
     * <p>Package-private so a test can keep hold of that client and assert the adapter released it.
     * Nothing else can: {@link BigtableDataClient} reports no closed state, so an adapter handed
     * some other closeable would pass every test that injects its own — while leaking a channel
     * pool and an executor per writer in production.
     */
    @VisibleForTesting
    MutationBatcher create(BigtableDataClient client) {
        try {
            // The TargetId overload, not the String one: that one is deprecated. TableId is the
            // TargetId a table has; authorized views are the other one and are out of scope here.
            return new BigtableBatcherAdapter(
                    destination,
                    client,
                    client.newBulkMutationBatcher(TableId.of(destination.getTable())));
        } catch (Throwable e) {
            // The client is owned here until the adapter takes it over on success.
            //
            // Throwable, not RuntimeException: a client's first classload can fail with a
            // NoClassDefFoundError — as can the lambda linkage the adapter's constructor performs —
            // which repeats on every restart attempt and would otherwise walk past this guard,
            // stranding a channel pool and an executor each time. The same guard, for the same
            // reason, as BigtableMutateRowsSink.createWriter's; precise rethrow keeps the declared
            // throws clause honest. Through Closers so a failing close is suppressed onto the
            // failure rather than replacing it — this method's own version of what close() does.
            Closers.closeAllSuppressing(e, client);
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
     * Adapts the client's bulk mutation {@link Batcher} to the writer-facing interface, and owns
     * the client's lifetime.
     *
     * <p>The batcher's three operations are held as functional values and the client as a plain
     * {@link AutoCloseable}, rather than as the two SDK types, <b>because that is the only seam a
     * test can drive</b> (#324). {@code Batcher} is {@code @InternalExtensionOnly} — the reason
     * {@link MutationBatcher} exists as this module's own SPI — so a fake must not implement it,
     * and {@link BigtableDataClient} reports nothing about having been closed and cannot be
     * subclassed to observe it, its only constructor being package-private. {@link #close()}
     * carries an invariant worth pinning and, before this shape, had no test at all.
     *
     * <p>Two vendor constraints, not one, which is why both halves are injected. The batcher's is
     * the annotation — a fake would be legal Java and an unsupported extension. The client's is
     * plain unextendability, the same shape as {@code BoundedShutdown}'s in the base module: {@code
     * Publisher} there is likewise a non-final class whose only constructor is private, so neither
     * client can be subclassed to observe its own teardown.
     */
    @VisibleForTesting
    static final class BigtableBatcherAdapter implements MutationBatcher {

        private final TableDestination destination;
        private final AutoCloseable client;
        private final Function<RowMutationEntry, ApiFuture<Void>> batcherAdd;
        private final Runnable batcherSendOutstanding;
        private final ThrowingRunnable<Exception> batcherShutdown;

        /**
         * The production shape. Kept beside the injectable one, and delegating to it, so that the
         * three method references binding this adapter to one batcher live inside the class a test
         * can construct rather than at the {@link #create()} call site, where nothing would reach
         * them.
         */
        BigtableBatcherAdapter(
                TableDestination destination,
                AutoCloseable client,
                Batcher<RowMutationEntry, Void> batcher) {
            this(destination, client, batcher::add, batcher::sendOutstanding, batcher::close);
        }

        /** Package-private so a test can script a shutdown that throws; see the class javadoc. */
        @VisibleForTesting
        BigtableBatcherAdapter(
                TableDestination destination,
                AutoCloseable client,
                Function<RowMutationEntry, ApiFuture<Void>> batcherAdd,
                Runnable batcherSendOutstanding,
                ThrowingRunnable<Exception> batcherShutdown) {
            this.destination = destination;
            this.client = client;
            this.batcherAdd = batcherAdd;
            this.batcherSendOutstanding = batcherSendOutstanding;
            this.batcherShutdown = batcherShutdown;
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
        public void close() throws Exception {
            // The shutdown sends what is buffered and waits for it. No timeout: a bounded one would
            // abandon mutations the service may still apply, and the writer's own flush has already
            // drained everything on the success path.
            //
            // Through Closers rather than a try/finally. Both close the client whichever way the
            // shutdown ends, and both propagate an Error unchanged; the difference is confined to
            // the case where *both* steps throw. A finally completing abruptly discards the try's
            // reason outright (JLS 14.20.2 — only try-with-resources suppresses), so the failure
            // that explains the teardown was lost in favour of the one that followed from it. The
            // client's close can throw: EnhancedBigtableStub reports a failing context close as an
            // IllegalStateException.
            //
            // Not the same defect as #276 — that one was later resources being abandoned, and it
            // replaced IOUtils.closeAll rather than any try/finally — but the same primitive, which
            // reports the first failure and suppresses the rest. What that costs is stated in
            // closeAll's own javadoc and is real here: the throwable Flink escalates on is the
            // shutdown's rather than the client's, so a JVM-fatal *client* close arrives suppressed
            // and unescalated. Accepted deliberately: the shutdown is where gax's own code runs.
            try {
                Closers.closeAll(
                        () ->
                                shutDownAbsorbingTheLifetimeFailureReport(
                                        destination, batcherShutdown),
                        client);
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
