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

import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.ClientMetricsSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings {@link DataClientRowKeySampler} builds, and for its lifecycle.
 *
 * <p>The application profile has its own test here rather than only on {@link
 * io.github.flink.gcp.connector.bigtable.BigtableDataClients}: a sampler that simply never passed
 * the profile on would leave that shared test green while planning a Data Boost scan on ordinary
 * compute.
 */
@Timeout(30)
class DataClientRowKeySamplerTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void carriesTheApplicationProfileAndTheEmulatorEndpointToTheClient() throws Exception {
        BigtableDataSettings settings =
                new DataClientRowKeySampler(
                                "boost-profile",
                                EmulatorEndpoint.parse("bigtable.example:9035", "emulatorEndpoint"),
                                true)
                        .settings(TABLE);

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
        assertThat(settings.getAppProfileId()).isEqualTo("boost-profile");
        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
    }

    @Test
    void theFactoryCarriesTheClientMetricsChoicesToEverySamplersClient() throws Exception {
        DataClientRowKeySampler sampler =
                (DataClientRowKeySampler)
                        new DefaultRowKeySamplerFactory(null, null, false).create();
        ClientMetrics metrics = ClientMetrics.create("method");

        ClientMetricsSettings.assertExportsNothing(sampler.settings(TABLE));
        sampler.useClientMetrics(metrics);
        ClientMetricsSettings.assertRecordsOnlyInto(sampler.settings(TABLE), metrics);
    }

    /**
     * The <em>factory</em> travels; the sampler does not, and must not.
     *
     * <p>A sampler that could be serialized could be parked on the source configuration, which is
     * how one sampler came to be shared by every enumerator of a job and refused after the first
     * teardown ({@code docs/adr/0128}). The second assertion is the one that would notice that
     * coming back.
     */
    @Test
    void theFactoryTravelsInTheJobGraphAndTheSamplerDoesNot() throws Exception {
        DefaultRowKeySamplerFactory factory =
                new DefaultRowKeySamplerFactory(
                        "boost-profile",
                        EmulatorEndpoint.parse("bigtable.example:9035", "emulatorEndpoint"),
                        true);

        DefaultRowKeySamplerFactory back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        DataClientRowKeySampler sampler = (DataClientRowKeySampler) back.create();
        assertThat(sampler.settings(TABLE).getAppProfileId()).isEqualTo("boost-profile");
        assertThat(java.io.Serializable.class.isAssignableFrom(RowKeySampler.class))
                .as("a serializable sampler could be parked on the configuration again")
                .isFalse();
    }

    @Test
    void recordsIntoTheClientMetricsBesideTheCloudMonitoringExport() throws Exception {
        var seam =
                (DataClientRowKeySampler)
                        new DefaultRowKeySamplerFactory(null, null, true).create();
        ClientMetrics metrics = ClientMetrics.create("method");

        ClientMetricsSettings.assertAsShipped(seam.settings(TABLE));
        seam.useClientMetrics(metrics);
        ClientMetricsSettings.assertRecordsInto(seam.settings(TABLE), metrics);
    }

    @Test
    void theBuilderCarriesTheCloudMonitoringChoiceIntoThisSeam() throws Exception {
        // Production settings: the emulator builder would disable the export whatever the choice.
        ClientMetricsSettings.assertExportsNothing(
                ((DataClientRowKeySampler)
                                TestSources.scanWithoutEmulator(
                                                b -> b.exportClientMetricsToCloudMonitoring(false))
                                        .getConfig()
                                        .getSamplerFactory()
                                        .create())
                        .settings(TABLE));
        ClientMetricsSettings.assertAsShipped(
                ((DataClientRowKeySampler)
                                TestSources.scanWithoutEmulator(b -> b)
                                        .getConfig()
                                        .getSamplerFactory()
                                        .create())
                        .settings(TABLE));
    }

    @Test
    void theBuilderWiresTheApplicationProfileIntoThisSeam() throws Exception {
        // Constructing a sampler by hand, as the test above does, cannot see the builder's wiring:
        // a build() that passed null here would leave every other unit test green and be caught
        // only by a gated run against a billed instance.
        DataClientRowKeySampler sampler =
                (DataClientRowKeySampler)
                        TestSources.config(builder -> builder.appProfileId("boost-profile"))
                                .getSamplerFactory()
                                .create();

        assertThat(sampler.settings(TABLE).getAppProfileId()).isEqualTo("boost-profile");
    }

    @Test
    void injectsTheRuntimeCredentialProvider() throws Exception {
        DataClientRowKeySampler sampler = new DataClientRowKeySampler(null, null, true);
        NoCredentialsProvider provider = NoCredentialsProvider.create();
        sampler.useCredentials(provider);

        assertThat(sampler.settings(TABLE).getStubSettings().getCredentialsProvider())
                .isSameAs(provider);
    }

    @Test
    void closingWithoutHavingSampledReleasesNothingAndFailsNothing() {
        assertThatCode(() -> new DataClientRowKeySampler(null, null, true).close())
                .doesNotThrowAnyException();
    }

    @Test
    void refusesToBuildAClientAfterItHasBeenClosed() throws IOException {
        DataClientRowKeySampler sampler =
                new DataClientRowKeySampler(
                        null, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"), true);
        sampler.close();

        assertThatThrownBy(() -> sampler.sample(TABLE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Bigtable row key sampler")
                .hasMessageContaining("was closed before it was used");
    }
}
