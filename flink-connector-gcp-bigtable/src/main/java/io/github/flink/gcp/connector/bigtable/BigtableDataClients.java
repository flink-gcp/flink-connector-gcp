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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.metrics.CustomOpenTelemetryMetricsProvider;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

/**
 * How this connector points a {@code BigtableDataClient} at an instance: production over
 * application-default credentials, production over a runtime-loaded service-account provider, or an
 * emulator over a plaintext channel with no credentials.
 *
 * <p>At the module root rather than beside either direction, because every data-client boundary of
 * this connector takes it — the sink, both sources and the table layer all build the same settings
 * from the same inputs. The admin clients do not come through here: {@code BigtableTableAdmin} and
 * the Change Streams coordinator build their own settings, and each carries its own copy of the
 * emulator branch. A second copy of this branch is exactly the thing that reads green on a machine
 * with credentials and red in CI — the failure ADR-0064 exists to describe.
 *
 * <p>Only what every path shares lives here. Per-caller tuning — the sink's batch thresholds, and
 * whatever a source may one day need — is applied by the caller on top of the builder this hands
 * back, which is why the return type is the builder rather than the settings.
 *
 * <p>Retry settings are deliberately untouched, on every path. The client retries {@code
 * MutateRows} per entry, resumes a broken {@code ReadRows} stream from the last key it saw, and
 * resumes a broken {@code ReadChangeStream} from its last continuation token. Only a failure that
 * outlives the client's own retry budget reaches the connector, and on the Change Streams path the
 * reader turns that into a terminal failure so Flink restarts from the checkpointed token. So
 * nothing here owns a retry loop, and nothing has a retry knob to map. The single-row request
 * family is the one caller that touches a call's settings on top of this builder — {@code
 * DefaultSingleRowClientFactory} pins {@code CheckAndMutateRow} and {@code ReadModifyWriteRow} to a
 * single attempt under its configured deadline — and it leaves the retryable-code sets as the
 * client ships them, which for those two RPCs is empty (ADR-0148).
 */
@Internal
public final class BigtableDataClients {

    private BigtableDataClients() {}

    /**
     * Returns the key under which every client pool shares one client per instance: the project and
     * instance a destination names, in the one spelling the pools and the per-instance bookkeeping
     * all use.
     *
     * @param destination the table whose instance the key names
     * @return the instance key
     */
    public static String instanceKey(TableDestination destination) {
        return destination.getProject() + "/" + destination.getInstance();
    }

    /**
     * Builds the settings for a client that talks to a destination's instance.
     *
     * @param destination the table whose project and instance the client is bound to; the table
     *     itself is named per call, not in the settings
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator to connect to (plaintext, no credentials), or {@code
     *     null} for production Bigtable
     * @param credentialsOverride the runtime-loaded service-account provider, or {@code null} to
     *     preserve application-default credentials
     * @return the settings builder, for the caller to tune and build
     */
    public static BigtableDataSettings.Builder settings(
            TableDestination destination,
            @Nullable String appProfileId,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        Preconditions.checkNotNull(destination, "destination must not be null");
        BigtableDataSettings.Builder settings =
                emulatorEndpoint == null
                        ? BigtableDataSettings.newBuilder()
                        : BigtableDataSettings.newBuilderForEmulator(
                                emulatorEndpoint.getHost(), emulatorEndpoint.getPort());
        settings.setProjectId(destination.getProject()).setInstanceId(destination.getInstance());
        if (credentialsOverride != null) {
            Preconditions.checkArgument(
                    emulatorEndpoint == null,
                    "credentialsOverride cannot be combined with an emulator endpoint");
            settings.setCredentialsProvider(credentialsOverride);
        }
        if (appProfileId != null) {
            settings.setAppProfileId(appProfileId);
        }
        return settings;
    }

    /**
     * Whether a component exports its client's own metrics to Flink when nothing chooses: every
     * builder starts from it, and a Table component without a builder resolves an unset option to
     * it, so the default lives in one place (ADR-0139).
     */
    public static final boolean DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK = false;

    /**
     * Whether a client keeps its built-in Cloud Monitoring export when nothing chooses; one place,
     * as {@link #DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK} is.
     */
    public static final boolean DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING = true;

    /**
     * Applies the two client-metrics choices to settings built by {@link #settings}.
     *
     * <p>On the classic stub path they are independent in the client (2.85.0, {@code
     * BigtableClientContext}). The built-in Cloud Monitoring exporter is created whenever internal
     * metrics are enabled, whatever the metrics provider; a {@code
     * CustomOpenTelemetryMetricsProvider} adds a second recorder beside it rather than replacing
     * it, and {@code NoopMetricsProvider} does not remove it. So exporting to Flink leaves the
     * Cloud Monitoring export running, and only {@code disableInternalMetrics()} stops it. The
     * session (vRPC) path that serves {@code ReadRow} and {@code MutateRow} is the exception:
     * {@code MetricsImpl.newTableTracer} returns a no-op tracer when internal metrics are off, so
     * there disabling the Cloud Monitoring export also silences the second recorder.
     *
     * <p>The provider is set on the emulator path too. The emulator builder sets {@code
     * NoopMetricsProvider}, and a later provider replaces it: the client then records against the
     * emulator like any other endpoint, which is what lets an emulator test observe the client's
     * series. The emulator builder has already disabled the Cloud Monitoring export.
     *
     * @param settings the settings to apply them to
     * @param clientMetrics the client metrics to record into, or {@code null} to export nothing to
     *     Flink
     * @param exportToCloudMonitoring whether the client keeps its built-in Cloud Monitoring export
     */
    public static void applyClientMetrics(
            BigtableDataSettings.Builder settings,
            @Nullable ClientMetrics clientMetrics,
            boolean exportToCloudMonitoring) {
        if (clientMetrics != null) {
            settings.setMetricsProvider(
                    CustomOpenTelemetryMetricsProvider.create(clientMetrics.openTelemetry()));
        }
        if (!exportToCloudMonitoring) {
            settings.disableInternalMetrics();
        }
    }
}
