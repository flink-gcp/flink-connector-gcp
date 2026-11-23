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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFunction;
import com.google.auth.Credentials;
import com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Builds the Spanner service handle both directions of this connector open.
 *
 * <p>One helper rather than one copy per direction, because the emulator-versus-credentials branch
 * is exactly the code that reads green on a developer machine and red in CI: a handle built without
 * the emulator host reaches for application default credentials, which a workstation has and a
 * build agent does not.
 *
 * <p>Spanner's own {@code setEmulatorHost} switches the channel to plaintext and the credentials to
 * none in one call, so this connector never goes through the shared plaintext-channel helpers the
 * other connectors need.
 *
 * <p>The one other thing configured about the connection is where its channels run: on {@link
 * GrpcEventLoops}' group rather than gRPC's own, whose delayed teardown can outlive the job's class
 * loader (docs/adr/0208). The settings take a channel configurator, and {@code
 * SpannerOptions.Builder.build()} replaces whatever configurator was set with a plaintext one when
 * an emulator host is set (6.127.0), the {@code SPANNER_EMULATOR_HOST} environment variable's
 * included. The builder here therefore composes the group's configurator into every configurator
 * set on it, {@code build()}'s own among them, which keeps the client in its emulator mode. {@link
 * #open(String, SpannerOptions)} holds a lease while the handle lives.
 *
 * <p>The same call puts the client in its emulator mode, where it records no client metrics into a
 * provider ({@code SpannerOptions.isEmulatorEnabled()}, 6.124.0), so an emulator job reports its
 * {@code rpcMethod} series at zero. The emulator branch also turns the built-in Cloud Monitoring
 * export off. In emulator mode the client never starts it anyway, and saying so explicitly keeps
 * the test switch below from starting the process-wide exporter against an emulator.
 */
@Internal
public final class SpannerClients {

    /**
     * Whether the emulator branch leaves the client's emulator mode, so that the client records its
     * metrics against the emulator as it does against the service. Test-only: the integration tests
     * observe the client-metrics series through a Flink reporter this way. Read on every settings
     * build, so it reaches the components a job builds in this JVM.
     */
    private static volatile boolean emulatorModeBypassed;

    /**
     * Whether a component exports its client's own metrics to Flink when nothing chooses: every
     * builder starts from it, and a Table component without a builder resolves an unset option to
     * it, so the default lives in one place (ADR-0139).
     */
    public static final boolean DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK = false;

    /**
     * Whether a client takes part in its built-in Cloud Monitoring export when nothing chooses; one
     * place, as {@link #DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK} is.
     */
    public static final boolean DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING = true;

    private SpannerClients() {}

    /**
     * Makes every client built afterwards in this JVM reach an emulator outside the client's
     * emulator mode, or restores the default. The connection is the same, plaintext and without
     * credentials; what changes is that the client then records its metrics. It does so only if
     * {@code SPANNER_EMULATOR_HOST} is not set to the emulator, which the client also reads.
     *
     * @param bypassed whether to leave the emulator mode
     */
    @VisibleForTesting
    static void bypassEmulatorModeForTesting(boolean bypassed) {
        emulatorModeBypassed = bypassed;
    }

    /**
     * Builds the client settings for a database.
     *
     * <p>Separate from {@link #open(DatabaseDestination, SpannerOptions)} so that the mapping is
     * testable without opening a channel.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @return the settings
     */
    public static SpannerOptions settings(
            DatabaseDestination database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        return settings(database, emulatorEndpoint, null);
    }

    /**
     * Builds client settings with an optional runtime-loaded credential override.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @return the settings
     */
    public static SpannerOptions settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride) {
        return settings(database.getProject(), emulatorEndpoint, credentialsOverride);
    }

    /**
     * Builds client settings for a project, for a caller that reaches more than one database, as
     * the catalog does: the handle carries only the project, and each database is named per call.
     *
     * @param project the project the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @return the settings
     */
    public static SpannerOptions settings(
            String project,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride) {
        return builder(project, emulatorEndpoint, credentialsOverride).build();
    }

    private static SpannerOptions.Builder builder(
            String project,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        SpannerOptions.Builder settings = new EventLoopOptionsBuilder().setProjectId(project);
        if (emulatorEndpoint != null) {
            settings.setEmulatorHost(emulatorEndpoint.getTarget()).setBuiltInMetricsEnabled(false);
            if (emulatorModeBypassed) {
                // build() turns the emulator host into the host, a plaintext channel and no
                // credentials, and a rebuilt copy keeps all three; clearing the emulator host is
                // what leaves the emulator mode. The copy is a plain builder, but the configurator
                // it carries is the composed one, and without an emulator host its build() keeps
                // it.
                settings = settings.build().toBuilder().setEmulatorHost(null);
            }
        } else if (credentialsOverride != null) {
            settings.setCredentials(credentialsOverride);
        }
        return settings;
    }

    /**
     * Builds client settings for a runtime component, with its client-metrics choices.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param clientMetrics the client metrics the client records into, or {@code null} to export
     *     nothing to Flink
     * @param exportClientMetricsToCloudMonitoring whether the client keeps its built-in Cloud
     *     Monitoring export
     * @return the settings
     */
    public static SpannerOptions settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable ClientMetrics clientMetrics,
            boolean exportClientMetricsToCloudMonitoring) {
        return settingsBuilder(
                        database,
                        emulatorEndpoint,
                        credentialsOverride,
                        clientMetrics,
                        exportClientMetricsToCloudMonitoring)
                .build();
    }

    /**
     * Returns the builder {@link #settings(DatabaseDestination, EmulatorEndpoint, Credentials,
     * ClientMetrics, boolean)} builds, for a component that sets more on it.
     *
     * <p>Set it here rather than on a {@code toBuilder()} copy of built settings: the copy is a
     * plain builder, whose {@code build()} replaces the channel configurator when an emulator host
     * is set, and {@link #open(String, SpannerOptions)} refuses settings whose channels would not
     * run on the connector's event-loop group.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param clientMetrics the client metrics the client records into, or {@code null} to export
     *     nothing to Flink
     * @param exportClientMetricsToCloudMonitoring whether the client keeps its built-in Cloud
     *     Monitoring export
     * @return the settings builder
     */
    public static SpannerOptions.Builder settingsBuilder(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable ClientMetrics clientMetrics,
            boolean exportClientMetricsToCloudMonitoring) {
        SpannerOptions.Builder settings =
                builder(database.getProject(), emulatorEndpoint, credentialsOverride);
        applyClientMetrics(settings, clientMetrics, exportClientMetricsToCloudMonitoring);
        return settings;
    }

    /**
     * Applies the two client-metrics choices to settings.
     *
     * <p>They are independent in the client (6.124.0, {@code
     * SpannerOptions.createApiTracerFactory}): a {@code CustomOpenTelemetryMetricsProvider} adds a
     * tracer recording into the given instance beside the Cloud Monitoring one, and {@code
     * setBuiltInMetricsEnabled(false)} removes only the latter. The export is turned off and never
     * on, so that the client's own default, which {@code SPANNER_DISABLE_BUILTIN_METRICS} also
     * sets, stands when the choice is the default.
     *
     * @param settings the settings to apply them to
     * @param clientMetrics the client metrics to record into, or {@code null} to export nothing to
     *     Flink
     * @param exportToCloudMonitoring whether the client keeps its built-in Cloud Monitoring export
     */
    @VisibleForTesting
    static void applyClientMetrics(
            SpannerOptions.Builder settings,
            @Nullable ClientMetrics clientMetrics,
            boolean exportToCloudMonitoring) {
        if (clientMetrics != null) {
            settings.setClientMetricsProvider(
                    CustomOpenTelemetryMetricsProvider.create(clientMetrics.openTelemetry()));
        }
        if (!exportToCloudMonitoring) {
            settings.setBuiltInMetricsEnabled(false);
        }
    }

    /**
     * Opens a handle from settings already assembled by the owning runtime component.
     *
     * @param database the database to reach, for the failure message
     * @param settings the settings to open the handle from
     * @return the handle with its lease, which the caller owns and must close
     * @throws IOException if the handle cannot be created
     */
    public static GrpcEventLoops.LeasedClient<Spanner> open(
            DatabaseDestination database, SpannerOptions settings) throws IOException {
        return open(String.valueOf(database), settings);
    }

    /**
     * Opens a handle whose client records into client metrics the caller hands over with it,
     * closing the client metrics if the handle cannot be created: nothing else would.
     *
     * @param database the database to reach, for the failure message
     * @param settings the settings to open the handle from, carrying the client metrics
     * @param clientMetrics the client metrics the settings carry, or {@code null}
     * @return the handle with its lease, which the caller owns and closes with {@link
     *     #close(GrpcEventLoops.LeasedClient, ClientMetrics)}
     * @throws IOException if the handle cannot be created
     */
    public static GrpcEventLoops.LeasedClient<Spanner> open(
            DatabaseDestination database,
            SpannerOptions settings,
            @Nullable ClientMetrics clientMetrics)
            throws IOException {
        try {
            return open(database, settings);
        } catch (Throwable e) {
            Closers.closeAllSuppressing(e, clientMetrics);
            throw e;
        }
    }

    /**
     * Closes a handle and releases its lease, then closes the client metrics its client recorded
     * into, whatever the handle's close did. In this order because closed client metrics drop what
     * they are given, so a client still closing would lose whatever it recorded meanwhile.
     *
     * @param spanner the handle to close
     * @param clientMetrics the client metrics to close after it, or {@code null}
     */
    public static void close(
            GrpcEventLoops.LeasedClient<Spanner> spanner, @Nullable ClientMetrics clientMetrics) {
        try {
            spanner.close();
        } finally {
            if (clientMetrics != null) {
                clientMetrics.close();
            }
        }
    }

    /**
     * Opens a handle from settings, naming what it reaches in the failure message.
     *
     * <p>The lease is opened before the handle builds its channels and released if the handle
     * cannot be created.
     *
     * @param target what the handle reaches, for the failure message
     * @param settings the settings to open the handle from, built by this class
     * @return the handle with its lease, which the caller owns and must close
     * @throws IOException if the handle cannot be created
     * @throws IllegalArgumentException if the settings' channels would not run on the group, as
     *     after a {@code toBuilder()} copy rebuilt with an emulator host, or with a channel
     *     provider set, which the client uses in place of the configurator
     */
    public static GrpcEventLoops.LeasedClient<Spanner> open(String target, SpannerOptions settings)
            throws IOException {
        Preconditions.checkArgument(
                settings.getChannelConfigurator() instanceof EventLoopConfigurator
                        // A transport provider replaces the configurator altogether.
                        && settings.getChannelProvider() == null,
                "The Spanner settings for %s do not put their channels on the connector's"
                        + " event-loop group; build them from SpannerClients' builder, not a"
                        + " toBuilder() copy, and set no channel provider",
                target);
        // Spanner is not a gax BackgroundResource: the holder closes it with its own close(),
        // which waits for its channels to terminate (6.127.0 waits up to ten seconds for each of
        // its four stubs and its watchdog in turn), and then releases the lease. The settings
        // carry the group through their configurator, so the factory has nothing to apply.
        return GrpcEventLoops.openAutoCloseable(
                lease -> {
                    try {
                        return settings.getService();
                    } catch (RuntimeException e) {
                        throw new IOException(
                                "Failed to create the Spanner client for " + target + ".", e);
                    }
                });
    }

    /**
     * A settings builder that composes {@link GrpcEventLoops#channelConfigurator} into every
     * channel configurator set on it.
     *
     * <p>A subclass rather than a configurator set once, because {@code build()} sets its own
     * plaintext configurator when an emulator host is set and would otherwise drop the group's. It
     * relies on {@code build()} going through the overridable setter, which {@code
     * SpannerClientsTest} holds against the client version in use.
     */
    private static final class EventLoopOptionsBuilder extends SpannerOptions.Builder {

        EventLoopOptionsBuilder() {
            setChannelConfigurator(null);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public SpannerOptions.Builder setChannelConfigurator(
                @Nullable ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator) {
            return super.setChannelConfigurator(new EventLoopConfigurator(configurator));
        }
    }

    /**
     * {@link GrpcEventLoops#channelConfigurator} under a type of its own, so that {@link
     * #open(String, SpannerOptions)} can tell settings built here from settings whose configurator
     * a later {@code build()} replaced.
     */
    @SuppressWarnings("rawtypes")
    private static final class EventLoopConfigurator
            implements ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> {

        private final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> composed;

        EventLoopConfigurator(
                @Nullable ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> existing) {
            this.composed = GrpcEventLoops.channelConfigurator(existing);
        }

        @Override
        public ManagedChannelBuilder apply(ManagedChannelBuilder builder) {
            return composed.apply(builder);
        }
    }
}
