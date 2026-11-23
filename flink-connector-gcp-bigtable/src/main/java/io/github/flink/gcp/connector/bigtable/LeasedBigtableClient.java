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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.StubSettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Bigtable client and the lease on the connector's gRPC event-loop group that covers its channels
 * (ADR-0208).
 *
 * <p>Base's {@link GrpcEventLoops.LeasedClient} pairs a lease with a gax {@code
 * BackgroundResource}, whose termination it can wait for. None of the three Bigtable clients is
 * one: {@code BigtableDataClient}, {@code BigtableTableAdminClient} and {@code
 * BigtableInstanceAdminClient} are plain {@link AutoCloseable}s (google-cloud-bigtable 2.88.0). The
 * data client's {@code close()} drains its session pool for up to six minutes, then shuts its
 * classic channels down and waits up to six minutes for them to terminate (gax 2.90.0 {@code
 * GrpcTransportChannel.close()}), without cancelling the classic calls still in flight; on an
 * interrupted thread, or one interrupted during that wait, it gives the wait up at once, consuming
 * the interrupt, and skips the rest of the client's cleanup. The admin clients' {@code close()}
 * only shuts their channels down (gax's {@code BackgroundResourceAggregation}). So closing this
 * holder closes the client and then releases the lease, restoring first an interrupt the client's
 * close consumed; the release of the group's last lease waits for the group to stop, which closes
 * any channel still open on it. A client is normally closed with nothing in flight: a writer has
 * flushed, a reader has stopped its reads, a full lookup cache load has cancelled its scan, and a
 * JobManager-side call has returned or been cancelled. A call the connector cannot cancel may still
 * be pending, such as a point read (a lookup's, or one a full lookup cache load makes for a range
 * whose endpoints are equal and closed, whose call can outlive the row it returned) or a mutation
 * left by a writer teardown that was interrupted; the client's close can then wait for it, or
 * cancel it if it was still queued on a session.
 *
 * <p>The lease reaches every channel the client builds from its settings' instantiating provider:
 * the data client's channel pool, its session channels, and a DirectPath data channel, which the
 * SDK builds from that provider's configurator too. It does not reach the channel of the data
 * client's built-in Cloud Monitoring exporter, which builds its own {@code MetricServiceClient}
 * from nothing but an endpoint and credentials, so that channel stays on gRPC's own group whenever
 * the export is on (ADR-0208).
 *
 * <p>The first {@link #close()} does the work; a concurrent or later one waits until it has
 * finished, unless it is interrupted while it waits, so a JobManager-side owner whose teardown
 * races the call it cancels never returns while the client is still closing.
 *
 * @param <C> the client type
 */
@Internal
public final class LeasedBigtableClient<C extends AutoCloseable> implements AutoCloseable {

    private final C client;
    private final GrpcEventLoops.Lease lease;

    private final AtomicBoolean closing = new AtomicBoolean();
    private final CountDownLatch closed = new CountDownLatch(1);

    private LeasedBigtableClient(C client, GrpcEventLoops.Lease lease) {
        this.client = client;
        this.lease = lease;
    }

    /**
     * Opens a lease and creates a client under it, releasing the lease if the creation fails.
     *
     * @param factory creates the client, applying the lease to its settings with {@link
     *     #applyTo(GrpcEventLoops.Lease, StubSettings.Builder)}
     * @param <C> the client type
     * @return the client paired with its lease
     * @throws IOException if the factory does
     */
    @VisibleForTesting
    static <C extends AutoCloseable> LeasedBigtableClient<C> open(Factory<C> factory)
            throws IOException {
        Preconditions.checkNotNull(factory, "factory must not be null");
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            return new LeasedBigtableClient<>(factory.create(lease), lease);
        } catch (IOException | RuntimeException | Error e) {
            lease.close();
            throw e;
        }
    }

    /**
     * Opens a data client on the group.
     *
     * <p>Takes the settings builder rather than built settings, so the lease is applied before
     * anything is built: a round trip through {@code toBuilder()} is not guaranteed to carry every
     * field a settings builder holds.
     *
     * @param settings the client's settings builder, which this applies the lease to
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static LeasedBigtableClient<BigtableDataClient> openData(
            BigtableDataSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return open(
                lease -> {
                    applyTo(lease, settings.stubSettings());
                    return BigtableDataClient.create(settings.build());
                });
    }

    /**
     * Opens a table admin client on the group.
     *
     * @param settings the client's settings builder, which this applies the lease to
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static LeasedBigtableClient<BigtableTableAdminClient> openTableAdmin(
            BigtableTableAdminSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return open(
                lease -> {
                    applyTo(lease, settings.stubSettings());
                    return BigtableTableAdminClient.create(settings.build());
                });
    }

    /**
     * Opens an instance admin client on the group.
     *
     * @param settings the client's settings builder, which this applies the lease to
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static LeasedBigtableClient<BigtableInstanceAdminClient> openInstanceAdmin(
            BigtableInstanceAdminSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return open(
                lease -> {
                    applyTo(lease, settings.stubSettings());
                    return BigtableInstanceAdminClient.create(settings.build());
                });
    }

    /**
     * Puts the channels a Bigtable client's stub settings build on the group, composed after any
     * configurator their provider already carries, such as the emulator builders' plaintext one.
     *
     * <p>The Bigtable settings wrap stub settings rather than extend gax's {@code ClientSettings},
     * so base's {@code Lease.applyTo(ClientSettings.Builder)} cannot take them; this is the same
     * step on the stub settings every Bigtable settings builder exposes through {@code
     * stubSettings()}.
     *
     * @param lease the lease to apply
     * @param stubSettings the client's stub settings builder
     * @param <B> the stub settings builder type
     * @return the same builder
     * @throws IllegalArgumentException if the provider does not build its own channels, which the
     *     group could then not reach
     */
    @VisibleForTesting
    static <B extends StubSettings.Builder<?, B>> B applyTo(
            GrpcEventLoops.Lease lease, B stubSettings) {
        Preconditions.checkNotNull(lease, "lease must not be null");
        Preconditions.checkNotNull(stubSettings, "stubSettings must not be null");
        TransportChannelProvider provider = stubSettings.getTransportChannelProvider();
        Preconditions.checkArgument(
                provider instanceof InstantiatingGrpcChannelProvider,
                "The event-loop group applies only to a provider that builds its own channels,"
                        + " but the settings carry %s",
                provider);
        return stubSettings.setTransportChannelProvider(
                lease.applyTo(((InstantiatingGrpcChannelProvider) provider).toBuilder()).build());
    }

    /**
     * Returns the client; close this holder, not the client.
     *
     * @return the client
     */
    public C client() {
        return client;
    }

    /**
     * Closes the client and releases its lease, or waits for the close already under way. The lease
     * is released even if the client's close fails, and that failure is then rethrown; an interrupt
     * ends a waiting caller's wait early and is restored.
     */
    @Override
    public void close() {
        if (closing.compareAndSet(false, true)) {
            try {
                // closeAll's contract: the lease is released whatever the client's close did, and
                // the first failure is the one reported. A data client's close consumes an
                // interrupt it finds set, so it is restored before the lease is released: inside
                // GrpcEventLoops.interruptibleReleases() that interrupt is what ends the release's
                // wait for the group.
                boolean interrupted = Thread.currentThread().isInterrupted();
                Closers.closeAll(() -> closeClient(interrupted), lease);
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            } finally {
                closed.countDown();
            }
            return;
        }
        try {
            closed.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the client, then restores an interrupt its close consumed: one already set when the
     * close began, or one that arrived during it, which gax reports as a close failure caused by an
     * {@link InterruptedException}.
     */
    private void closeClient(boolean interrupted) throws Exception {
        try {
            client.close();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Creates a client whose settings the given lease has been applied to. */
    @FunctionalInterface
    interface Factory<C extends AutoCloseable> {

        /**
         * Creates the client.
         *
         * @param lease the lease to apply to the client's settings
         * @return the client
         * @throws IOException if the client cannot be created
         */
        C create(GrpcEventLoops.Lease lease) throws IOException;
    }
}
