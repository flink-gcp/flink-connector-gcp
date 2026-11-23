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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import com.google.api.gax.core.ExecutorProvider;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.spanner.ClientMetricsProbe;
import io.github.flink.gcp.connector.spanner.ClosedEmulator;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.StallingSpannerServer;
import io.github.flink.gcp.connector.spanner.TestLeasedSpanners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultSpannerChangeStreamCoordinatorClientFactoryTest {

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a client left its lease open").isFalse();
    }

    @Test
    @Timeout(60)
    void aCloseDuringInitializationCancelsTheReadAndReleasesTheLease() throws Exception {
        // The server answers the session the client creates in the background, which no context
        // of the caller's reaches; the read on it is the call in flight.
        try (StallingSpannerServer server = StallingSpannerServer.startAnsweringSessions()) {
            SpannerChangeStreamCoordinatorClient client =
                    new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                    DatabaseDestination.of("project", "instance", "database"),
                                    "orders",
                                    server.endpoint())
                            .create((ClientMetrics) null);
            CompletableFuture<Duration> initializing =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return client.initialize();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            long started;
            try {
                assertThat(server.awaitStalledCall(30)).as("a read reached the server").isTrue();
                started = System.nanoTime();
            } finally {
                client.close();
            }
            // The read is cancelled rather than waited out: the client's close alone waits for
            // its channels, up to 10 s per stub, which a read that is never answered keeps open.
            assertThatThrownBy(() -> initializing.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .satisfies(StallingSpannerServer::assertCancelled);
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
                    .as("milliseconds from close to initialization's end; calls %s", server.calls())
                    .isLessThan(3_000);
        }
    }

    @Test
    void usesSevenDaysWhenNoAbsentRetentionFallbackIsConfigured() {
        DatabaseDestination database = DatabaseDestination.of("project", "instance", "database");

        DefaultSpannerChangeStreamCoordinatorClientFactory factoryWithDefault =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(database, "orders", null);
        DefaultSpannerChangeStreamCoordinatorClientFactory factoryWithExplicitFallback =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                        database, "orders", Duration.ofDays(7), null);

        assertThat(factoryWithDefault)
                .usingRecursiveComparison()
                .isEqualTo(factoryWithExplicitFallback);
    }

    @Test
    void closesTheServiceHandleWhenTheDatabaseClientCannotBeOpened() {
        IllegalStateException failure = new IllegalStateException("no such database");
        FailingSpanner spanner = new FailingSpanner(failure);
        DefaultSpannerChangeStreamCoordinatorClientFactory factory =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                        DatabaseDestination.of("project", "instance", "database"),
                        "orders",
                        Duration.ofDays(7),
                        null);

        assertThatThrownBy(() -> factory.create(TestLeasedSpanners.leased(spanner)))
                .isSameAs(failure);
        assertThat(spanner.closeCalls).hasValue(1);
        assertThat(GrpcEventLoops.isRunning()).as("the handle's lease was released").isFalse();
    }

    private static final class FailingSpanner implements Spanner {

        private final RuntimeException failure;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private FailingSpanner(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public DatabaseClient getDatabaseClient(DatabaseId databaseId) {
            throw failure;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        @Override
        public boolean isClosed() {
            return closeCalls.get() > 0;
        }

        @Override
        public SpannerOptions getOptions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DatabaseAdminClient getDatabaseAdminClient() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InstanceAdminClient getInstanceAdminClient() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BatchClient getBatchClient(DatabaseId databaseId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutorProvider getAsyncExecutorProvider() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void carriesTheClientMetricsChoicesIntoTheClientSettings() throws Exception {
        // No emulator: its branch turns the Cloud Monitoring export off by itself, which would
        // hide whether the factory's own choice reaches the settings.
        ClientMetrics metrics = ClientMetricsProbe.create();

        SpannerOptions off = factory(false).settings(metrics);
        SpannerOptions on = factory(true).settings(null);

        assertThat(off.getClientMetricsProvider())
                .isInstanceOfSatisfying(
                        CustomOpenTelemetryMetricsProvider.class,
                        provider ->
                                assertThat(provider.getOpenTelemetry())
                                        .isSameAs(metrics.openTelemetry()));
        assertThat(off.isEnableBuiltInMetrics()).isFalse();
        assertThat(on.getClientMetricsProvider())
                .isNotInstanceOf(CustomOpenTelemetryMetricsProvider.class);
    }

    @Test
    void aFailedOpenLeavesTheSourcesClientMetricsOpen() {
        // They belong to the source and are shared by every enumerator it builds; the client
        // takes no ownership of them, so not even a failed open may close them.
        ClientMetrics metrics = ClientMetricsProbe.create();

        assertThatThrownBy(
                        () ->
                                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                                DatabaseDestination.of(
                                                        "project", "instance", "database"),
                                                "orders",
                                                Duration.ofDays(7),
                                                ClosedEmulator.endpoint(),
                                                null,
                                                true)
                                        .create(metrics))
                .isInstanceOf(IOException.class);

        assertThat(ClientMetricsProbe.isOpen(metrics)).isTrue();
    }

    private static DefaultSpannerChangeStreamCoordinatorClientFactory factory(
            boolean exportToCloudMonitoring) {
        return new DefaultSpannerChangeStreamCoordinatorClientFactory(
                DatabaseDestination.of("project", "instance", "database"),
                "orders",
                Duration.ofDays(7),
                null,
                null,
                exportToCloudMonitoring);
    }
}
