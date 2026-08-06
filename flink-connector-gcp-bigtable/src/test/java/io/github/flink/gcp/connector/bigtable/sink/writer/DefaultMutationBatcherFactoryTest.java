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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings {@link DefaultMutationBatcherFactory} builds — the options-to-settings
 * mapping, which is otherwise invisible: a threshold that never reaches the client looks exactly
 * like one that does — for the one exception its batcher shutdown absorbs, and for the adapter it
 * wraps the client in, whose teardown no fake could reach before #324 made it injectable.
 *
 * <p>{@code @Timeout} because two tests build and close a real SDK client. It is JUnit's default
 * {@code SAME_THREAD} mode, so what it buys is precise: a teardown parked at an interruptible point
 * — {@code closeAsync().get()}, which is where a real one would park — fails the build instead of
 * hanging it, while one that ignores interruption still hangs. {@code SEPARATE_THREAD} would cover
 * both and is not used, matching {@code DefaultPublisherFactoryTest}.
 */
@Timeout(30)
class DefaultMutationBatcherFactoryTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    /** The message gax's {@code BatcherImpl} raises when its own shutdown goes wrong. */
    private static final String GAX_UNEXPECTED_CLOSE = "unexpected error closing the batcher";

    /**
     * Read-and-clear, unconditionally. One test here drives the adapter's interrupt restore, and a
     * flag left set would make every later test in this surefire fork see a cancellation that never
     * happened — #316's shared-state hazard in another shape, and one an assertion inside the test
     * would not cover, since an earlier assertion failing would skip it.
     */
    @AfterEach
    void clearAnyInterruptThisClassSet() {
        Thread.interrupted();
    }

    @Test
    void carriesTheDestinationAndTheApplicationProfile() {
        BigtableDataSettings settings =
                factory("batch-profile", BigtableWriterOptions.defaults(), "localhost:8086")
                        .settings();

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
        assertThat(settings.getAppProfileId()).isEqualTo("batch-profile");
    }

    @Test
    void appliesTheConfiguredBatchThresholds() {
        BatchingSettings batching =
                batchingSettings(
                        BigtableWriterOptions.builder()
                                .batchElementCount(7)
                                .batchByteSize(4096)
                                .build());

        assertThat(batching.getElementCountThreshold()).isEqualTo(7L);
        assertThat(batching.getRequestByteThreshold()).isEqualTo(4096L);
    }

    @Test
    void leavesEachUnsetThresholdAtTheClientsOwn() {
        BatchingSettings clientDefaults = batchingSettings(BigtableWriterOptions.defaults());
        // Only one of the two is set, so the other has to keep the client's value rather than be
        // reset alongside it.
        BatchingSettings batching =
                batchingSettings(BigtableWriterOptions.builder().batchElementCount(7).build());

        assertThat(batching.getElementCountThreshold()).isEqualTo(7L);
        assertThat(batching.getRequestByteThreshold())
                .isEqualTo(clientDefaults.getRequestByteThreshold());
        assertThat(batching.getDelayThresholdDuration())
                .isEqualTo(clientDefaults.getDelayThresholdDuration());
    }

    @Test
    void pointsTheClientAtTheEmulatorEndpoint() {
        BigtableDataSettings settings =
                factory(null, BigtableWriterOptions.defaults(), "bigtable.example:9035").settings();

        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
        // The emulator mode is the only one that must never present credentials.
        assertThat(settings.getStubSettings().getCredentialsProvider().getClass().getSimpleName())
                .isEqualTo("NoCredentialsProvider");
    }

    @Test
    void absorbsTheBatchersReportOfItsAccumulatedEntryFailures() throws Exception {
        // #238: gax's BatcherImpl.close() ends by throwing this, built from stats accumulating
        // every entry failure of the batcher's lifetime. The writer consumed each of those
        // failures through its own future and applied the sink's policy there, so letting the
        // report out failed a job that a logAndDrop policy had kept running.
        BatchingException report = lifetimeFailureReport();

        try (LogCapture capture = LogCapture.of(DefaultMutationBatcherFactory.class)) {
            assertThatCode(
                            () ->
                                    DefaultMutationBatcherFactory
                                            .shutDownAbsorbingTheLifetimeFailureReport(
                                                    TABLE,
                                                    () -> {
                                                        throw report;
                                                    }))
                    .doesNotThrowAnyException();

            // Absorbing the report is only defensible because it survives somewhere, and this
            // line is the whole of that: a mutation first sent from inside the shutdown reaches
            // neither the failure handler nor the writer's captured error, because completions
            // can no longer run once the task mailbox is quiesced (#238, #323).
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).contains(TABLE.toString());
                                assertThat(event.getThrowable()).isSameAs(report);
                            });
        }
    }

    @Test
    void letsEveryOtherShutdownFailureThrough() {
        // The absorb is for that one report and nothing else: gax throws an IllegalStateException
        // for an unexpected close error and an InterruptedException when the wait is cut short,
        // and both are failures of the shutdown itself rather than a repeat of what the writer
        // already handled.
        IllegalStateException unexpected = new IllegalStateException(GAX_UNEXPECTED_CLOSE);

        assertThatThrownBy(
                        () ->
                                DefaultMutationBatcherFactory
                                        .shutDownAbsorbingTheLifetimeFailureReport(
                                                TABLE,
                                                () -> {
                                                    throw unexpected;
                                                }))
                .isSameAs(unexpected);
    }

    @Test
    void shutsTheBatcherDownBeforeClosingTheClient() throws Exception {
        List<String> calls = new ArrayList<>();

        new DefaultMutationBatcherFactory.BigtableBatcherAdapter(
                        TABLE,
                        () -> calls.add("client"),
                        entry -> {
                            throw new AssertionError("add is not part of this test");
                        },
                        () -> calls.add("sendOutstanding"),
                        () -> calls.add("shutdown"))
                .close();

        // Order, not just occurrence: closing the client first would pull the channel out from
        // under the wait the shutdown performs.
        assertThat(calls).containsExactly("shutdown", "client");
    }

    @Test
    void closesTheClientEvenWhenTheBatcherShutdownThrows() {
        // The absorb takes the lifetime report and nothing else, so this is the shape that leaked:
        // an unexpected close error left the client holding its channel. gax raises this one from
        // BatcherImpl.close() itself.
        RecordingClient client = new RecordingClient();
        IllegalStateException unexpected = new IllegalStateException(GAX_UNEXPECTED_CLOSE);

        assertThatThrownBy(() -> adapter(client, throwing(unexpected)).close())
                .isSameAs(unexpected);

        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void closesTheClientAndRestoresTheInterruptWhenTheBatchersWaitIsCutShort() {
        // The other exception the absorb does not swallow, and the one a cancelling job produces:
        // Batcher.close() declares InterruptedException for the wait it performs. gax's wait clears
        // the flag when it throws, and BigtableWriter.close() carries on to the failure handler's
        // close afterwards, so the restore is what keeps that leg honouring the cancellation.
        RecordingClient client = new RecordingClient();
        InterruptedException interrupted =
                new InterruptedException("the batcher's wait was cut short");

        assertThatThrownBy(() -> adapter(client, throwing(interrupted)).close())
                .isSameAs(interrupted);

        assertThat(client.closes).isEqualTo(1);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void absorbsTheLifetimeFailureReportThroughTheAdaptersOwnClose() throws Exception {
        // The absorb (#238) is pinned above over the static seam alone, so nothing said close()
        // still routes through it — and binding the shutdown straight to the batcher compiles,
        // deleting that fix and failing at teardown a logAndDrop job it had kept running.
        //
        // Through LogCapture (#323, which landed while this was in review) rather than on the
        // swallow alone: a close() that caught the report itself would swallow it just as quietly,
        // and the warning is what tells the two apart — it is emitted by the shared helper, so
        // seeing it here is what makes this a routing assertion. Plus the half no other test
        // states: an absorbed report still releases the client.
        RecordingClient client = new RecordingClient();
        BatchingException report = lifetimeFailureReport();

        try (LogCapture capture = LogCapture.of(DefaultMutationBatcherFactory.class)) {
            assertThatCode(() -> adapter(client, throwing(report)).close())
                    .doesNotThrowAnyException();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(report));
        }
        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void closesTheClientEvenWhenTheBatcherShutdownThrowsAnError() {
        // This is a new call site of Closers, and #276's reason for it applies here: the
        // IOUtils.closeAll(..., Exception.class) form rethrows an Error from inside its own loop,
        // abandoning the client, and the Throwable.class form collects it as new Exception(e),
        // losing the type. (Flink halts the JVM only on its own fatal set, which this Error is
        // deliberately not in — the type still has to survive for the caller to act on.)
        RecordingClient client = new RecordingClient();
        NoClassDefFoundError blewUp = new NoClassDefFoundError("batcher shutdown blew up");

        assertThatThrownBy(() -> adapter(client, throwing(blewUp)).close()).isSameAs(blewUp);

        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void reportsTheShutdownFailureWithTheClientCloseFailureSuppressed() {
        // A try/finally reports the client's failure and discards the shutdown's outright, which is
        // the one that says why the teardown went wrong; the client's close then failing is the
        // consequence, not the cause. EnhancedBigtableStub really does report a failing context
        // close this way, so the pair is reachable. #276 fixed this shape at nine other call sites.
        RecordingClient client = new RecordingClient();
        client.failure = new IllegalStateException("failed to close client context");
        IllegalStateException unexpected = new IllegalStateException(GAX_UNEXPECTED_CLOSE);

        assertThatThrownBy(() -> adapter(client, throwing(unexpected)).close())
                .isSameAs(unexpected)
                .satisfies(
                        reported ->
                                assertThat(reported.getSuppressed())
                                        .containsExactly(client.failure));
    }

    @Test
    void sendsOutstandingThroughToTheBatcher() {
        // Deliberately not redundant with the emulator ITs, measured on #324: gax pushes a batch on
        // its own delay-threshold timer — 1 s for bulk mutations, set in ClientOperationSettings —
        // so a sendOutstanding reaching nothing still lets every row land one flush-wait later, and
        // every one of those tests passes. What this pins is the adapter's delegation; the
        // production constructor's binding of it to the batcher is pinned by nothing, and the cost
        // of that binding breaking is flush latency rather than lost data, since drainInFlight()
        // still waits and the timer still pushes.
        List<String> calls = new ArrayList<>();
        MutationBatcher adapter =
                new DefaultMutationBatcherFactory.BigtableBatcherAdapter(
                        TABLE,
                        new RecordingClient(),
                        entry -> {
                            throw new AssertionError("add is not part of this test");
                        },
                        () -> calls.add("sendOutstanding"),
                        () -> {});

        adapter.sendOutstanding();

        assertThat(calls).containsExactly("sendOutstanding");
    }

    @Test
    void addHandsTheEntryToTheBatcherAndReturnsItsFuture() {
        // The emulator ITs already fail if this stops delegating — they read the rows back — so
        // this is the adapter's contract stated in one place rather than new coverage.
        RowMutationEntry entry = entry("row-1");
        ApiFuture<Void> batcherFuture = ApiFutures.immediateFuture(null);
        List<RowMutationEntry> added = new ArrayList<>();
        MutationBatcher adapter =
                new DefaultMutationBatcherFactory.BigtableBatcherAdapter(
                        TABLE,
                        new RecordingClient(),
                        submitted -> {
                            added.add(submitted);
                            return batcherFuture;
                        },
                        () -> {
                            throw new AssertionError("sendOutstanding is not part of this test");
                        },
                        () -> {});

        // The identity matters: the writer registers its completion callback on what comes back,
        // so a future of the adapter's own would leave every mutation outstanding for good.
        assertThat(adapter.add(entry)).isSameAs(batcherFuture);
        assertThat(added).containsExactly(entry);
    }

    @Test
    void shutsDownTheBatcherTheFactoryItselfBuilt() throws Exception {
        // Through create(), which is the wiring the injectable constructor cannot cover: that the
        // adapter's shutdown reaches the batcher this factory handed it. Offline is safe — the
        // emulator settings carry no credentials, gRPC connects lazily, and an empty batcher's
        // close sends nothing.
        //
        // What this proves is that the batcher was *closed*, not that the shutdown *waited*:
        // BatcherImpl.add's precondition reads closeFuture, which closeAsync() sets just as
        // close() does. So binding the shutdown to closeAsync(), or to close(Duration.ZERO),
        // would survive this — the "no timeout" decision in the adapter is documented and unpinned.
        MutationBatcher batcher =
                factory(null, BigtableWriterOptions.defaults(), "localhost:1").create();

        batcher.close();

        assertThatThrownBy(() -> batcher.add(entry("row-1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed batcher");
    }

    @Test
    void closesTheClientTheFactoryBuiltTheBatcherOver() throws Exception {
        // The other half of that wiring, and the half that leaks in production: an adapter handed
        // any other closeable passes every test above, since those inject their own client. The
        // client's own close is unobservable — hence the seam — so the observable is a consequence
        // of it: BigtableClientContext.close() shuts down the background executor, and gax
        // schedules a batcher's delay-threshold push on exactly that executor. Measured, and SDK
        // internals rather than a documented contract, so a client upgrade may need this reread.
        DefaultMutationBatcherFactory factory =
                factory(null, BigtableWriterOptions.defaults(), "localhost:1");
        BigtableDataClient client = BigtableDataClient.create(factory.settings());

        factory.create(client).close();

        assertThatThrownBy(() -> client.newBulkMutationBatcher(TableId.of("orders")))
                .isInstanceOf(RejectedExecutionException.class);
    }

    /** Records the closes the adapter performs, and optionally fails them. */
    private static final class RecordingClient implements AutoCloseable {

        private int closes;
        @Nullable private RuntimeException failure;

        @Override
        public void close() {
            closes++;
            if (failure != null) {
                throw failure;
            }
        }
    }

    /** An adapter whose shutdown is scripted and whose other two operations are out of scope. */
    private static MutationBatcher adapter(
            AutoCloseable client, ThrowingRunnable<Exception> shutdown) {
        return new DefaultMutationBatcherFactory.BigtableBatcherAdapter(
                TABLE,
                client,
                entry -> {
                    throw new AssertionError("add is not part of this test");
                },
                () -> {
                    throw new AssertionError("sendOutstanding is not part of this test");
                },
                shutdown);
    }

    /**
     * A shutdown that fails with exactly the given throwable. Typed {@code Throwable} so a test can
     * script an {@code Error}, and {@code rethrowException} rather than {@code rethrow} because the
     * latter wraps a checked exception — which would quietly defeat the {@code isSameAs} on the
     * interrupted case.
     */
    private static ThrowingRunnable<Exception> throwing(Throwable failure) {
        return () -> ExceptionUtils.rethrowException(failure);
    }

    private static RowMutationEntry entry(String rowKey) {
        // A timestamp in whole milliseconds: Bigtable rejects anything finer, and this entry is
        // built the same way whether or not it ever reaches a service.
        return RowMutationEntry.create(rowKey).setCell("cf", "q", 1_000L, "v");
    }

    /**
     * Builds the exception gax raises from a batcher shutdown. Reflection is the only route: {@code
     * BatchingException} is final with a package-private constructor, so a change to that signature
     * fails this test loudly rather than leaving the absorb untested.
     */
    private static BatchingException lifetimeFailureReport() throws Exception {
        Constructor<BatchingException> constructor =
                BatchingException.class.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                "Batching finished with 1 batches failed to apply due to: 1 ApiException(1"
                        + " INVALID_ARGUMENT) and 0 partial failures.");
    }

    private static BatchingSettings batchingSettings(BigtableWriterOptions options) {
        return factory(null, options, "localhost:8086")
                .settings()
                .getStubSettings()
                .bulkMutateRowsSettings()
                .getBatchingSettings();
    }

    private static DefaultMutationBatcherFactory factory(
            String appProfileId, BigtableWriterOptions options, String emulatorEndpoint) {
        return new DefaultMutationBatcherFactory(
                TABLE, appProfileId, options, EmulatorEndpoint.parse(emulatorEndpoint));
    }
}
