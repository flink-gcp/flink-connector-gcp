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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.ClientMetricsProbe;
import io.github.flink.gcp.connector.spanner.ClosedEmulator;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultSpannerChangeStreamQueryClientFactoryTest {

    @Test
    void reentrantPublicationRunsOnlyAfterTheCallbackReturns() {
        Deque<Runnable> delegated = new ArrayDeque<>();
        Executor delegate = delegated::addLast;
        DefaultSpannerChangeStreamQueryClientFactory.SerializingExecutor executor =
                new DefaultSpannerChangeStreamQueryClientFactory.SerializingExecutor(delegate);
        List<String> order = new ArrayList<>();

        executor.execute(
                () -> {
                    order.add("callback");
                    assertThat(
                                    DefaultSpannerChangeStreamQueryClientFactory.DefaultHandle
                                            .pauseBeforePublishing(
                                                    new SpannerChangeStreamRecord.Heartbeat(
                                                            java.time.Instant.EPOCH),
                                                    executor,
                                                    ignored -> order.add("mailbox-visible")))
                            .isEqualTo(AsyncResultSet.CallbackResponse.PAUSE);
                    order.add("callback-returned");
                });

        delegated.removeFirst().run();
        assertThat(order).containsExactly("callback", "callback-returned");
        delegated.removeFirst().run();
        assertThat(order)
                .containsExactlyElementsOf(
                        Arrays.asList("callback", "callback-returned", "mailbox-visible"));
    }

    @Test
    void carriesTheClientMetricsChoicesIntoTheClientSettings() throws Exception {
        // No emulator: its branch turns the Cloud Monitoring export off by itself, which would
        // hide whether the factory's own choice reaches the settings.
        ClientMetrics metrics = ClientMetricsProbe.create();

        SpannerOptions off = factory(null, false).settings(metrics);

        assertThat(off.getClientMetricsProvider())
                .isInstanceOfSatisfying(
                        CustomOpenTelemetryMetricsProvider.class,
                        provider ->
                                assertThat(provider.getOpenTelemetry())
                                        .isSameAs(metrics.openTelemetry()));
        assertThat(off.isEnableBuiltInMetrics()).isFalse();
    }

    @Test
    void closesTheClientMetricsWhenTheServiceHandleCannotBeOpened() {
        ClientMetrics metrics = ClientMetricsProbe.create();

        assertThatThrownBy(() -> factory(ClosedEmulator.endpoint(), true).create(metrics))
                .isInstanceOf(IOException.class);

        assertThat(ClientMetricsProbe.isOpen(metrics)).isFalse();
    }

    private static DefaultSpannerChangeStreamQueryClientFactory factory(
            @Nullable EmulatorEndpoint emulatorEndpoint, boolean exportToCloudMonitoring) {
        return new DefaultSpannerChangeStreamQueryClientFactory(
                DatabaseDestination.of("project", "instance", "database"),
                "orders",
                SpannerRpcPriority.HIGH,
                1,
                emulatorEndpoint,
                null,
                exportToCloudMonitoring);
    }
}
