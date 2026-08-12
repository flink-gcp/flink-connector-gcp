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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.stub.readrows.ReadRowsResumptionStrategy;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.ServiceAccountKeyFiles;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryReadClients}. */
class BigQueryReadClientsTest {

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsReachTheStorageReadSettings() throws Exception {
        BigQueryReadSettings settings =
                BigQueryReadClients.readSettings(
                        ServiceAccountKeyFiles.create(tempDir).toString(), null, 7, null);

        Credentials credentials = settings.getCredentialsProvider().getCredentials();
        assertThat(credentials instanceof ServiceAccountCredentials).isTrue();
        assertThat(((ServiceAccountCredentials) credentials).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    @Test
    void theEmulatorSettingsKeepTheStorageReadApisInboundMessageSize() throws IOException {
        // Storage Read responses carry whole row batches, and the API's own transport builder
        // raises the limit to Integer.MAX_VALUE for that reason. A provider built from a bare
        // InstantiatingGrpcChannelProvider.newBuilder() drops back to gRPC's 4 MiB default, which
        // fails only on the emulator and only once a batch grows past it.
        BigQueryReadSettings settings =
                BigQueryReadClients.emulatorSettings(EmulatorEndpoint.parse("localhost:9060"));

        assertThat(provider(settings).toBuilder().getMaxInboundMessageSize())
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void theEmulatorSettingsDialTheEmulatorWithoutCredentials() throws IOException {
        BigQueryReadSettings settings =
                BigQueryReadClients.emulatorSettings(EmulatorEndpoint.parse("localhost:9060"));

        assertThat(provider(settings).getEndpoint()).isEqualTo("localhost:9060");
        assertThat(provider(settings).toBuilder().getChannelConfigurator()).isNotNull();
        assertThat(settings.getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
    }

    @Test
    void theReadingSettingsBoundTheClientsOwnRetry() throws IOException {
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, null, 7, null);

        assertThat(settings.readRowsSettings().getRetrySettings().getMaxAttempts()).isEqualTo(7);
    }

    @Test
    void theReadingSettingsChangeNothingAboutTheRetryButItsBound() throws IOException {
        // The backoff sequence is the SDK's, and staying out of it is the decision: a schedule
        // rewritten here would be one more thing to keep in step with a client that already tunes
        // it for this API.
        RetrySettings sdk =
                BigQueryReadSettings.newBuilder().build().readRowsSettings().getRetrySettings();

        RetrySettings ours =
                BigQueryReadClients.readSettings(null, null, 7, null)
                        .readRowsSettings()
                        .getRetrySettings();

        assertThat(ours).isEqualTo(sdk.toBuilder().setMaxAttempts(7).build());
    }

    @Test
    void theSdkLeavesReadRowsUnboundedWhichIsWhyTheKnobExists() throws IOException {
        // The premise the retryMaxAttempts knob rests on, measured against
        // google-cloud-bigquerystorage 3.30.0 and pinned here so a BOM bump that changes it fails
        // rather than quietly making the knob pointless — or, worse, doubly bounding the read.
        RetrySettings sdk =
                BigQueryReadSettings.newBuilder().build().readRowsSettings().getRetrySettings();

        assertThat(sdk.getMaxAttempts()).isZero();
        assertThat(sdk.getTotalTimeoutDuration()).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void theClientResumesABrokenReadItself() throws IOException {
        // The other half of that premise, and the reason this connector runs no retry loop of its
        // own: the client resumes a broken ReadRows at originalOffset + rowsProcessed. Were this
        // strategy to disappear, a retried attempt would re-read the stream from the top and every
        // row already handed downstream would be emitted twice.
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, null, 7, null);

        assertThat(settings.readRowsSettings().getResumptionStrategy())
                .isInstanceOf(ReadRowsResumptionStrategy.class);
    }

    @Test
    void theClientRetriesOnlyWhatItsOwnClassificationAllows() throws IOException {
        // Deliberately not widened. UNAVAILABLE is the configured code; the client additionally
        // resumes a handful of INTERNAL messages and a RESOURCE_EXHAUSTED carrying RetryInfo,
        // through its own algorithm rather than through this set. A bump that adds a code here is
        // worth reading before it ships.
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, null, 7, null);

        assertThat(settings.readRowsSettings().getRetryableCodes())
                .containsExactly(StatusCode.Code.UNAVAILABLE);
    }

    @Test
    void theRetryListenerIsWiredWhenOneIsGiven() throws IOException {
        AtomicInteger retries = new AtomicInteger();

        BigQueryReadSettings settings =
                BigQueryReadClients.readSettings(null, null, 7, retries::incrementAndGet);

        settings.getReadRowsRetryAttemptListener()
                .onRetryAttempt(Status.UNAVAILABLE, new Metadata());
        assertThat(retries).hasValue(1);
    }

    @Test
    void noRetryListenerIsWiredWhenNoneIsGiven() throws IOException {
        assertThat(
                        BigQueryReadClients.readSettings(null, null, 7, null)
                                .getReadRowsRetryAttemptListener())
                .isNull();
    }

    private static InstantiatingGrpcChannelProvider provider(BigQueryReadSettings settings) {
        // The cast is safe by construction: the settings come from EmulatorChannels, whose
        // plaintextProvider returns this type.
        return (InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider();
    }
}
