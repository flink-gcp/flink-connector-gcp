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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultSpannerDatabaseAccessFactory}. */
class DefaultSpannerDatabaseAccessFactoryTest {

    private static final DatabaseDestination DATABASE =
            DatabaseDestination.of("my-project", "my-instance", "my-db");

    @Test
    void unsetKnobsAddNoTransactionOptionAtAll() {
        // Leaving the service's own handling in place, rather than restating today's defaults and
        // freezing them into every job.
        assertThat(
                        DefaultSpannerDatabaseAccessFactory.transactionOptions(
                                SpannerWriterOptions.defaults()))
                .isEmpty();
    }

    @Test
    void carriesTheCommitDelayAndPriorityWhenTheyAreSet() {
        Options.TransactionOption[] transactionOptions =
                DefaultSpannerDatabaseAccessFactory.transactionOptions(
                        SpannerWriterOptions.builder()
                                .maxCommitDelay(Duration.ofMillis(20))
                                .rpcPriority(SpannerRpcPriority.LOW)
                                .build());

        // Asserted by kind rather than by value, because the client library reads a transaction
        // option only from inside its own package: the accessors that would say what these carry
        // are package-private, as is the aggregate they fold into. Comparing against what the
        // library's own factories produce keeps the assertion off any private class name.
        assertThat(transactionOptions)
                .hasSize(2)
                .anyMatch(option -> option.getClass() == commitDelayKind())
                .anyMatch(option -> option.getClass() == priorityKind());
    }

    @Test
    void injectsWriterRuntimeCredentialsIntoClientSettings() {
        GoogleCredentials credentials =
                GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));
        DefaultSpannerDatabaseAccessFactory factory =
                new DefaultSpannerDatabaseAccessFactory(
                        DATABASE, SpannerWriterOptions.defaults(), null, credentials);

        assertThat(factory.settings().getCredentials() == credentials).isTrue();
    }

    @Test
    void changesOnlyBatchWriteAndKeepsSdkRetriesDisabled() {
        SpannerOptions baseline = SpannerClients.settings(DATABASE, null, null);
        SpannerOptions actual =
                new DefaultSpannerDatabaseAccessFactory(
                                DATABASE,
                                SpannerWriterOptions.builder()
                                        .batchWriteTimeout(Duration.ofSeconds(17))
                                        .build(),
                                null)
                        .settings();

        RetrySettings retry =
                actual.getSpannerStubSettings().batchWriteSettings().getRetrySettings();
        assertThat(actual.getSpannerStubSettings().batchWriteSettings().getRetryableCodes())
                .isEmpty();
        assertThat(retry.getInitialRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(17));
        assertThat(retry.getMaxRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(17));
        assertThat(retry.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(17));
        assertThat(retry.getMaxAttempts()).isEqualTo(1);

        assertThat(nonBatchWriteSettings(actual.getSpannerStubSettings()))
                .containsExactlyElementsOf(
                        nonBatchWriteSettings(baseline.getSpannerStubSettings()));
        assertThat(actual.getInstanceAdminStubSettings().getInstanceSettings())
                .isEqualTo(baseline.getInstanceAdminStubSettings().getInstanceSettings());
        assertThat(actual.getDatabaseAdminStubSettings().getDatabaseSettings())
                .isEqualTo(baseline.getDatabaseAdminStubSettings().getDatabaseSettings());
    }

    @Test
    void revalidatesTheTimeoutAtTheTaskManagerBoundary() throws Exception {
        SpannerWriterOptions forged = SpannerWriterOptions.builder().build();
        Field timeout = SpannerWriterOptions.class.getDeclaredField("batchWriteTimeout");
        timeout.setAccessible(true);
        timeout.set(forged, Duration.ofNanos(999_999));

        assertThatThrownBy(
                        () ->
                                new DefaultSpannerDatabaseAccessFactory(DATABASE, forged, null)
                                        .settings())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchWriteTimeout")
                .hasMessageContaining("at least 1 millisecond");
        assertThat(SpannerWriterOptions.defaults().getBatchWriteTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @ParameterizedTest
    @EnumSource(SpannerRpcPriority.class)
    void mapsEveryPriorityThisConnectorOffers(SpannerRpcPriority priority) {
        // A value added to either enum has to fail somewhere; the mapping is a switch with no
        // default fall-through, so this pins that every one of ours reaches it.
        assertThat(
                        DefaultSpannerDatabaseAccessFactory.transactionOptions(
                                SpannerWriterOptions.builder().rpcPriority(priority).build()))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());
    }

    private static Class<?> commitDelayKind() {
        return Options.maxCommitDelay(Duration.ofMillis(1)).getClass();
    }

    private static Class<?> priorityKind() {
        return Options.priority(Options.RpcPriority.LOW).getClass();
    }

    private static List<String> nonBatchWriteSettings(SpannerStubSettings settings) {
        return Arrays.asList(
                        settings.createSessionSettings(),
                        settings.batchCreateSessionsSettings(),
                        settings.getSessionSettings(),
                        settings.listSessionsSettings(),
                        settings.deleteSessionSettings(),
                        settings.executeSqlSettings(),
                        settings.executeStreamingSqlSettings(),
                        settings.executeBatchDmlSettings(),
                        settings.readSettings(),
                        settings.streamingReadSettings(),
                        settings.beginTransactionSettings(),
                        settings.commitSettings(),
                        settings.rollbackSettings(),
                        settings.partitionQuerySettings(),
                        settings.partitionReadSettings(),
                        settings.fetchCacheUpdateSettings())
                .stream()
                // ServerStreamingCallSettings does not implement structural equality; its string
                // form contains the retry codes, retry settings, idle timeout and wait timeout.
                .map(Object::toString)
                .toList();
    }

    @Test
    void releasesTheServiceHandleWhenTheDatabaseClientCannotBeOpened() {
        // The handle is the factory's until the adapter takes it over, and nothing downstream
        // would ever close it — a job restarting on a bad DatabaseId would otherwise leak one
        // channel pool per attempt.
        FakeSpanner spanner = new FakeSpanner(new IllegalStateException("no such database"));

        assertThatThrownBy(() -> factory("localhost:9010").create(spanner))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no such database");

        assertThat(spanner.closes()).isEqualTo(1);
    }

    private static DefaultSpannerDatabaseAccessFactory factory(String emulatorEndpoint) {
        return new DefaultSpannerDatabaseAccessFactory(
                DATABASE,
                SpannerWriterOptions.defaults(),
                EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint"));
    }
}
