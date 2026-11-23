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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.LeasedBigtableClient;
import io.github.flink.gcp.connector.bigtable.PerCallClientRunner;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Samples row keys through a {@code google-cloud-bigtable} {@link BigtableDataClient}.
 *
 * <p>Named after the client its {@link #close()} releases, which is the connector's convention for
 * the real implementation of a seam.
 *
 * <p>The sampler runs on the coordinator's side of the job, so its client lives only as long as the
 * one call it is opened for: {@link #sample(TableDestination)} opens a client, samples and closes
 * it, and {@link #close()} cancels a sample still in flight (ADR-0208, {@link
 * PerCallClientRunner}). The enumerator samples a table once and then has no further use for a
 * client, so nothing is lost by not keeping one.
 *
 * <p>That client never keeps the built-in Cloud Monitoring export, whatever the source's choice:
 * the export's own {@code MetricServiceClient} would put a channel on gRPC's event-loop group on
 * the JobManager, which the connector's group exists to avoid, and a client opened for one call
 * would report under a client id of its own. The source's choice governs the readers' clients.
 * Client metrics exported to Flink are unaffected.
 *
 * <p>The application profile is carried, and has to be: a Data Boost profile applies to {@code
 * SampleRowKeys} as well as to {@code ReadRows}, so a sampler that ignored it would plan a scan on
 * one kind of compute and then run it on another.
 *
 * <p>The client's retry configuration is left alone. It retries {@code SampleRowKeys} on the
 * transient codes under a total timeout of its own, so a failure that reaches the enumerator has
 * already exhausted the retry the client owns.
 *
 * <p>One sampler belongs to one enumerator: {@link DefaultRowKeySamplerFactory} mints it, and the
 * source mints one per {@code createEnumerator} and {@code restoreEnumerator}. That is what makes
 * the one-way closed state correct — it ends this object rather than poisoning one the next
 * enumerator will also be handed ({@code docs/adr/0128}).
 */
@Internal
public final class DataClientRowKeySampler implements RowKeySampler {

    @Nullable private final String appProfileId;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Runs each sample on a client of its own: {@link #sample(TableDestination)} runs on the
     * executor {@code SplitEnumeratorContext#callAsync} hands the work to, while {@link #close()}
     * runs on the coordinator thread.
     */
    private final PerCallClientRunner runner = new PerCallClientRunner("row key sampler");

    /** What the enumerator loaded and pushed in, or {@code null} for ADC; cleared on close. */
    @Nullable private volatile CredentialsProvider credentials;

    /** What the enumerator registered and pushed in; cleared on close. */
    @Nullable private volatile ClientMetrics clientMetrics;

    /**
     * Creates the sampler.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DataClientRowKeySampler(
            @Nullable String appProfileId, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.appProfileId = appProfileId;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public List<RowKeySample> sample(TableDestination table) throws IOException {
        // The TargetId overload, not the String one: that one is deprecated. TableId is the
        // TargetId a table has; authorized views are the other one and are out of scope here.
        List<KeyOffset> keyOffsets =
                runner.call(
                        () -> settingsBuilder(table),
                        LeasedBigtableClient::openData,
                        client -> client.sampleRowKeysAsync(TableId.of(table.getTable())));
        List<RowKeySample> samples = new ArrayList<>(keyOffsets.size());
        for (KeyOffset keyOffset : keyOffsets) {
            samples.add(RowKeySample.of(keyOffset.getKey(), keyOffset.getOffsetBytes()));
        }
        return samples;
    }

    @Override
    public void close() {
        // Closed first: a sample that has not opened its client by now is refused, so none can
        // build settings from the credentials cleared below and reach Bigtable as the process's
        // application default credentials.
        runner.close();
        credentials = null;
        clientMetrics = null;
    }

    /**
     * Builds the client settings. Visible to the module's tests because the mapping is otherwise
     * observable only through the client's behaviour: an application profile that never reaches the
     * client looks exactly like one that does.
     */
    @VisibleForTesting
    public BigtableDataSettings settings(TableDestination table) throws IOException {
        return settingsBuilder(table).build();
    }

    private BigtableDataSettings.Builder settingsBuilder(TableDestination table) {
        BigtableDataSettings.Builder settings =
                BigtableDataClients.settings(table, appProfileId, emulatorEndpoint, credentials);
        BigtableDataClients.applyClientMetrics(settings, clientMetrics, false);
        return settings;
    }

    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {
        this.credentials = credentials;
    }

    @Override
    public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
        this.clientMetrics = clientMetrics;
    }
}
