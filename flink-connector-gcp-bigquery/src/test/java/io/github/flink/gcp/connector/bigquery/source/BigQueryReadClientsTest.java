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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.StreamResumptionStrategy;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.stub.readrows.ReadRowsResumptionStrategy;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryReadClients}. */
class BigQueryReadClientsTest {

    /** The event-loop lease every settings build here applies; released after each test. */
    private GrpcEventLoops.Lease lease;

    @BeforeEach
    void openLease() {
        lease = GrpcEventLoops.acquire();
    }

    @AfterEach
    void releaseLease() {
        lease.close();
    }

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsReachTheStorageReadSettings() throws Exception {
        BigQueryReadSettings settings =
                BigQueryReadClients.readSettings(
                        ServiceAccountKeyFiles.create(tempDir).toString(), 7, null, lease);

        Credentials credentials = settings.getCredentialsProvider().getCredentials();
        assertThat(credentials instanceof ServiceAccountCredentials).isTrue();
        assertThat(((ServiceAccountCredentials) credentials).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    @Test
    void theReadingSettingsBoundTheClientsOwnRetry() throws IOException {
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, 7, null, lease);

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
                BigQueryReadClients.readSettings(null, 7, null, lease)
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
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, 7, null, lease);

        assertThat(settings.readRowsSettings().getResumptionStrategy())
                .isInstanceOf(ReadRowsResumptionStrategy.class);
    }

    @Test
    void theClientRetriesOnlyWhatItsOwnClassificationAllows() throws IOException {
        // Deliberately not widened. UNAVAILABLE is the configured code; the client additionally
        // resumes a handful of INTERNAL messages and a RESOURCE_EXHAUSTED whose RetryInfo carries a
        // delay, through its own algorithm rather than through this set. A bump that adds a code
        // here is worth reading before it ships.
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, 7, null, lease);

        assertThat(settings.readRowsSettings().getRetryableCodes())
                .containsExactly(StatusCode.Code.UNAVAILABLE);
    }

    @Test
    void aResumeRequestIsCountedAndStillResumesWhereTheBrokenCallStopped() throws IOException {
        AtomicInteger retries = new AtomicInteger();
        StreamResumptionStrategy<ReadRowsRequest, ReadRowsResponse> strategy =
                BigQueryReadClients.readSettings(null, 7, retries::incrementAndGet, lease)
                        .readRowsSettings()
                        .getResumptionStrategy()
                        .createNew();

        strategy.processResponse(ReadRowsResponse.newBuilder().setRowCount(3).build());
        assertThat(retries).as("a delivered response is not a retry").hasValue(0);

        ReadRowsRequest resume =
                strategy.getResumeRequest(
                        ReadRowsRequest.newBuilder().setReadStream("s").setOffset(10).build());
        assertThat(resume.getOffset()).isEqualTo(13);
        assertThat(retries).hasValue(1);
    }

    @Test
    void eachCallResumesFromItsOwnRowsNotAnotherCallsOnes() throws IOException {
        // The client mints a strategy per call, and one reading client serves every stream a reader
        // opens; a delegate shared between calls would resume a stream past rows another delivered.
        StreamResumptionStrategy<ReadRowsRequest, ReadRowsResponse> prototype =
                BigQueryReadClients.readSettings(null, 7, () -> {}, lease)
                        .readRowsSettings()
                        .getResumptionStrategy();
        StreamResumptionStrategy<ReadRowsRequest, ReadRowsResponse> first = prototype.createNew();
        StreamResumptionStrategy<ReadRowsRequest, ReadRowsResponse> second = prototype.createNew();

        first.processResponse(ReadRowsResponse.newBuilder().setRowCount(3).build());

        assertThat(
                        second.getResumeRequest(
                                        ReadRowsRequest.newBuilder()
                                                .setReadStream("other")
                                                .setOffset(10)
                                                .build())
                                .getOffset())
                .isEqualTo(10);
    }

    @Test
    void theSdkRetryListenerIsNeverSet() throws IOException {
        // It misses a plain UNAVAILABLE retry and counts the attempt maxAttempts refuses (#2301);
        // set beside the resumption count, it would also count the retries it does see twice.
        assertThat(
                        BigQueryReadClients.readSettings(null, 7, () -> {}, lease)
                                .getReadRowsRetryAttemptListener())
                .isNull();
    }

    /** The reading client's channels run on the connector's event-loop group (#2237). */
    @Test
    void theReadingSettingsRunOnTheEventLoopGroup() throws IOException {
        BigQueryReadSettings settings = BigQueryReadClients.readSettings(null, 7, () -> {}, lease);

        // The SDK's default provider carries no configurator, so one here is the group's.
        assertThat(
                        ((InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider())
                                .toBuilder().getChannelConfigurator())
                .isNotNull();
    }

    /** The session client's channels run on the connector's event-loop group too (#2237). */
    @Test
    void theSessionSettingsRunOnTheEventLoopGroup() throws IOException {
        BigQueryReadSettings settings = BigQueryReadClients.sessionSettings(null, lease);

        // The SDK's default provider carries no configurator, so one here is the group's.
        assertThat(
                        ((InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider())
                                .toBuilder().getChannelConfigurator())
                .isNotNull();
    }
}
