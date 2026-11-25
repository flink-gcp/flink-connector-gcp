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

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ClientSettings;
import com.google.api.gax.rpc.StubSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClientV2;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClientV2;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Opens the Bigtable clients on the connector's gRPC event-loop group, each paired with the lease
 * that covers its channels (ADR-0208).
 *
 * <p>The data client, {@code BigtableDataClient}, is not a gax {@code BackgroundResource} but a
 * plain {@link AutoCloseable} (google-cloud-bigtable 2.89.0), so it is opened with {@link
 * GrpcEventLoops#openAutoCloseable}, whose holder closes the client with its own {@code close()}
 * and then releases the lease. That {@code close()} drains the client's session pool for up to six
 * minutes, then shuts its classic channels down and waits up to six minutes for them to terminate
 * (gax 2.91.0 {@code GrpcTransportChannel.close()}), without cancelling the classic calls still in
 * flight; on an interrupted thread, or one interrupted during that wait, it gives the wait up at
 * once, consuming the interrupt, and skips the rest of the client's cleanup.
 *
 * <p>The admin clients are opened as {@code BigtableTableAdminClientV2} and {@code
 * BigtableInstanceAdminClientV2}, which extend the generated {@code BaseBigtable*AdminClient}s and
 * are gax {@code BackgroundResource}s, so they are opened with {@link
 * GrpcEventLoops#open(GrpcEventLoops.ClientFactory)}: the holder shuts the client down, waits a
 * bounded time for it to terminate, cancels its calls if it has not, and then releases the lease.
 * The wrapper clients they replace, with their settings and factories, are {@code @ObsoleteApi}
 * since 2.89.0, and every admin call the connector makes is a {@code com.google.bigtable.admin.v2}
 * request that names its resources in full, so nothing the connector sends depends on the wrappers.
 *
 * <p>A client is normally closed with nothing in flight: a writer has flushed, a reader has stopped
 * its reads, a full lookup cache load has cancelled its scan, and a JobManager-side call has
 * returned or been cancelled. A call the connector cannot cancel may still be pending, such as a
 * point read (a lookup's, or one a full lookup cache load makes for a range whose endpoints are
 * equal and closed, whose call can outlive the row it returned) or a mutation left by a writer
 * teardown that was interrupted; the data client's close can then wait for it, or cancel it if it
 * was still queued on a session.
 *
 * <p>The lease reaches every channel the data client builds from its settings' instantiating
 * provider: its channel pool, its session channels, and a DirectPath data channel, which the SDK
 * builds from that provider's configurator too. It does not reach the channel of the data client's
 * built-in Cloud Monitoring exporter, which builds its own {@code MetricServiceClient} from nothing
 * but an endpoint and credentials, so that channel stays on gRPC's own group whenever the export is
 * on (ADR-0208). An emulator admin client runs on a plaintext channel opened with {@link
 * EmulatorChannels#openLeasedClient}, as the Pub/Sub, BigQuery and Cloud Tasks clients do; the
 * emulator data client keeps the SDK's {@code newBuilderForEmulator(host, port)}, whose plaintext
 * configurator the lease composes after.
 *
 * <p>{@link #openData} takes the settings builder rather than built settings, so the lease is
 * applied before anything is built: a round trip through {@code toBuilder()} is not guaranteed to
 * carry every field a settings builder holds. The admin clients' settings hold nothing but their
 * credentials and transport: {@link #tableAdminSettings} and {@link #instanceAdminSettings} build
 * them, the module's one emulator branch for the admin family, and the matching {@code open} method
 * chooses the transport, which on an emulator is a channel opened with the client.
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
     * Builds a table admin client's settings: the credentials, which are none for an emulator. The
     * transport is chosen when the client is opened with {@link #openTableAdmin}.
     *
     * @param emulatorEndpoint the emulator to connect to (plaintext, no credentials), or {@code
     *     null} for production Bigtable
     * @param credentials the runtime-loaded service-account provider, or {@code null} to preserve
     *     application-default credentials
     * @return the settings builder
     * @throws IllegalStateException if {@code BIGTABLE_EMULATOR_HOST} is set to a non-empty value
     *     and no emulator endpoint is given
     * @throws IllegalArgumentException if both an emulator endpoint and credentials are given
     */
    public static BaseBigtableTableAdminSettings.Builder tableAdminSettings(
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentials) {
        return adminSettings(
                BaseBigtableTableAdminSettings.newBuilder(), emulatorEndpoint, credentials);
    }

    /**
     * Builds an instance admin client's settings: the credentials, which are none for an emulator.
     * The transport is chosen when the client is opened with {@link #openInstanceAdmin}.
     *
     * @param emulatorEndpoint the emulator to connect to (plaintext, no credentials), or {@code
     *     null} for production Bigtable
     * @param credentials the runtime-loaded service-account provider, or {@code null} to preserve
     *     application-default credentials
     * @return the settings builder
     * @throws IllegalStateException if {@code BIGTABLE_EMULATOR_HOST} is set to a non-empty value
     *     and no emulator endpoint is given
     * @throws IllegalArgumentException if both an emulator endpoint and credentials are given
     */
    public static BaseBigtableInstanceAdminSettings.Builder instanceAdminSettings(
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentials) {
        return adminSettings(
                BaseBigtableInstanceAdminSettings.newBuilder(), emulatorEndpoint, credentials);
    }

    /**
     * Opens a table admin client on the group: in production on a channel its settings build, which
     * the lease reaches; on an emulator on a plaintext channel opened for it, with no credentials.
     *
     * @param settings the settings builder from {@link #tableAdminSettings}
     * @param emulatorEndpoint the emulator those settings were built for, or {@code null} for
     *     production Bigtable
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> openTableAdmin(
            BaseBigtableTableAdminSettings.Builder settings,
            @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return openAdmin(
                settings,
                emulatorEndpoint,
                BaseBigtableTableAdminSettings.defaultGrpcTransportProviderBuilder(),
                BigtableTableAdminClientV2::create);
    }

    /**
     * Opens an instance admin client on the group: in production on a channel its settings build,
     * which the lease reaches; on an emulator on a plaintext channel opened for it, with no
     * credentials.
     *
     * @param settings the settings builder from {@link #instanceAdminSettings}
     * @param emulatorEndpoint the emulator those settings were built for, or {@code null} for
     *     production Bigtable
     * @return the client paired with its lease
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<BigtableInstanceAdminClientV2> openInstanceAdmin(
            BaseBigtableInstanceAdminSettings.Builder settings,
            @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return openAdmin(
                settings,
                emulatorEndpoint,
                BaseBigtableInstanceAdminSettings.defaultGrpcTransportProviderBuilder(),
                BigtableInstanceAdminClientV2::create);
    }

    private static <S extends ClientSettings<S>, B extends ClientSettings.Builder<S, B>>
            B adminSettings(
                    B settings,
                    @Nullable EmulatorEndpoint emulatorEndpoint,
                    @Nullable CredentialsProvider credentials) {
        EmulatorHostVariableChecks.checkUnsetFor(emulatorEndpoint);
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentials == null,
                "credentials cannot be combined with an emulator endpoint");
        if (emulatorEndpoint != null) {
            settings.setCredentialsProvider(NoCredentialsProvider.create());
        } else if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        return settings;
    }

    private static <
                    S extends ClientSettings<S>,
                    B extends ClientSettings.Builder<S, B>,
                    C extends BackgroundResource>
            GrpcEventLoops.LeasedClient<C> openAdmin(
                    B settings,
                    @Nullable EmulatorEndpoint emulatorEndpoint,
                    InstantiatingGrpcChannelProvider.Builder transportDefaults,
                    AdminClientFactory<S, C> factory)
                    throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        if (emulatorEndpoint == null) {
            return GrpcEventLoops.open(lease -> factory.create(lease.applyTo(settings).build()));
        }
        return EmulatorChannels.openLeasedClient(
                transportDefaults,
                emulatorEndpoint,
                channel ->
                        factory.create(
                                settings.setCredentialsProvider(NoCredentialsProvider.create())
                                        .setTransportChannelProvider(channel)
                                        .build()));
    }

    /** Creates a V2 admin client from its built settings. */
    @FunctionalInterface
    private interface AdminClientFactory<S, C> {
        C create(S settings) throws IOException;
    }

    /**
     * Puts the channels a data client's stub settings build on the group, composed after any
     * configurator their provider already carries, such as the emulator builder's plaintext one.
     * The data settings wrap stub settings rather than extend gax's {@code ClientSettings}, so the
     * lease is applied to the provider of the stub settings the builder exposes through {@code
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
        return stubSettings.setTransportChannelProvider(
                lease.applyTo(stubSettings.getTransportChannelProvider()));
    }
}
