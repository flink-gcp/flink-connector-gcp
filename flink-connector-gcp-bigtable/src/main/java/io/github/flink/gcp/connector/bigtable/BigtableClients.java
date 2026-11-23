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

import com.google.api.gax.rpc.StubSettings;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import java.io.IOException;

/**
 * Opens the Bigtable clients on the connector's gRPC event-loop group, each paired with the lease
 * that covers its channels (ADR-0208).
 *
 * <p>None of the three Bigtable clients is a gax {@code BackgroundResource}: {@code
 * BigtableDataClient}, {@code BigtableTableAdminClient} and {@code BigtableInstanceAdminClient} are
 * plain {@link AutoCloseable}s (google-cloud-bigtable 2.89.0), so they are opened with {@link
 * GrpcEventLoops#openAutoCloseable}, whose holder closes the client with its own {@code close()}
 * and then releases the lease. The data client's {@code close()} drains its session pool for up to
 * six minutes, then shuts its classic channels down and waits up to six minutes for them to
 * terminate (gax 2.91.0 {@code GrpcTransportChannel.close()}), without cancelling the classic calls
 * still in flight; on an interrupted thread, or one interrupted during that wait, it gives the wait
 * up at once, consuming the interrupt, and skips the rest of the client's cleanup. The admin
 * clients' {@code close()} only shuts their channels down (gax's {@code
 * BackgroundResourceAggregation}). A client is normally closed with nothing in flight: a writer has
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
 * <p>Each method takes the settings builder rather than built settings, so the lease is applied
 * before anything is built: a round trip through {@code toBuilder()} is not guaranteed to carry
 * every field a settings builder holds.
 */
@Internal
public final class BigtableClients {

    private BigtableClients() {}

    /**
     * Opens a data client on the group.
     *
     * @param settings the client's settings builder, which this applies the lease to
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<BigtableDataClient> openData(
            BigtableDataSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return GrpcEventLoops.openAutoCloseable(
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
    public static GrpcEventLoops.LeasedClient<BigtableTableAdminClient> openTableAdmin(
            BigtableTableAdminSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return GrpcEventLoops.openAutoCloseable(
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
    public static GrpcEventLoops.LeasedClient<BigtableInstanceAdminClient> openInstanceAdmin(
            BigtableInstanceAdminSettings.Builder settings) throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return GrpcEventLoops.openAutoCloseable(
                lease -> {
                    applyTo(lease, settings.stubSettings());
                    return BigtableInstanceAdminClient.create(settings.build());
                });
    }

    /**
     * Puts the channels a Bigtable client's stub settings build on the group, composed after any
     * configurator their provider already carries, such as the emulator builders' plaintext one.
     * The Bigtable settings wrap stub settings rather than extend gax's {@code ClientSettings}, so
     * the lease is applied to the provider of the stub settings every Bigtable settings builder
     * exposes through {@code stubSettings()}.
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
        return stubSettings.setTransportChannelProvider(
                lease.applyTo(stubSettings.getTransportChannelProvider()));
    }
}
