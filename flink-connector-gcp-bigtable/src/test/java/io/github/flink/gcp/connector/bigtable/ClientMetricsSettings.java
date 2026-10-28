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

import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.metrics.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.bigtable.data.v2.stub.metrics.DefaultMetricsProvider;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads the two client-metrics choices back from built client settings, the one place they are
 * observable without a client: every component that owns a data client applies them through {@link
 * BigtableDataClients#applyClientMetrics}, and each component's test asserts its own settings with
 * these.
 */
public final class ClientMetricsSettings {

    private ClientMetricsSettings() {}

    /** Asserts the client records into the given client metrics and keeps its built-in export. */
    public static void assertRecordsInto(BigtableDataSettings settings, ClientMetrics metrics) {
        assertProviderIs(settings, metrics);
        assertThat(settings.areInternalMetricsEnabled()).isTrue();
    }

    /** Asserts the client records into the given client metrics and has no built-in export. */
    public static void assertRecordsOnlyInto(BigtableDataSettings settings, ClientMetrics metrics) {
        assertProviderIs(settings, metrics);
        assertThat(settings.areInternalMetricsEnabled()).isFalse();
    }

    /**
     * Records one operation of the given method through the metrics provider the settings carry, as
     * the client would, so a test can read which registered series it reached.
     *
     * @param settings settings carrying a {@code CustomOpenTelemetryMetricsProvider}
     * @param method the method without its {@code Bigtable.} prefix
     */
    public static void recordOneOperation(BigtableDataSettings settings, String method) {
        recordOneOperation(
                ((CustomOpenTelemetryMetricsProvider) settings.getMetricsProvider())
                        .getOpenTelemetry(),
                method);
    }

    /**
     * Records one operation of the given method into an OpenTelemetry instance, as the client
     * would.
     *
     * @param openTelemetry the instance to record into
     * @param method the method without its {@code Bigtable.} prefix
     */
    public static void recordOneOperation(OpenTelemetry openTelemetry, String method) {
        openTelemetry
                .getMeterProvider()
                .get("bigtable")
                .histogramBuilder("bigtable.googleapis.com/internal/client/operation_latencies")
                .setUnit("ms")
                .build()
                .record(1.0, Attributes.of(AttributeKey.stringKey("method"), "Bigtable." + method));
    }

    private static void assertProviderIs(BigtableDataSettings settings, ClientMetrics metrics) {
        assertThat(settings.getMetricsProvider())
                .isInstanceOfSatisfying(
                        CustomOpenTelemetryMetricsProvider.class,
                        provider ->
                                assertThat(provider.getOpenTelemetry())
                                        .isSameAs(metrics.openTelemetry()));
    }

    /** Asserts the client exports nothing to Flink and has no Cloud Monitoring exporter. */
    public static void assertExportsNothing(BigtableDataSettings settings) {
        assertThat(settings.getMetricsProvider()).isSameAs(DefaultMetricsProvider.INSTANCE);
        assertThat(settings.areInternalMetricsEnabled()).isFalse();
    }

    /** Asserts the client's metrics are left as it ships them. */
    public static void assertAsShipped(BigtableDataSettings settings) {
        assertThat(settings.getMetricsProvider()).isSameAs(DefaultMetricsProvider.INSTANCE);
        assertThat(settings.areInternalMetricsEnabled()).isTrue();
    }
}
