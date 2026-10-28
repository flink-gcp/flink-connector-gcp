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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.spanner.SpannerClients;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The two client-metrics options of a Spanner table, as a table declared them.
 *
 * <p>One value rather than two fields on every table source and sink, because all of them carry
 * both to every component that builds a client: the sink writer, the scan's enumerator and readers
 * through the DataStream builders, and the lookup functions directly. A key the table leaves out
 * stays unset, so a builder keeps its own default.
 */
@Internal
public final class SpannerClientMetricsConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Boolean exportToFlink;
    @Nullable private final Boolean exportToCloudMonitoring;

    private SpannerClientMetricsConfig(
            @Nullable Boolean exportToFlink, @Nullable Boolean exportToCloudMonitoring) {
        this.exportToFlink = exportToFlink;
        this.exportToCloudMonitoring = exportToCloudMonitoring;
    }

    /** Returns the options of a table that declares neither, so every builder keeps its default. */
    public static SpannerClientMetricsConfig unset() {
        return new SpannerClientMetricsConfig(null, null);
    }

    /**
     * Reads both options from a table's configuration.
     *
     * @param config the table's options
     * @return the options as declared
     */
    public static SpannerClientMetricsConfig from(ReadableConfig config) {
        return new SpannerClientMetricsConfig(
                config.getOptional(SpannerConnectorOptions.CLIENT_METRICS_EXPORT_TO_FLINK)
                        .orElse(null),
                config.getOptional(
                                SpannerConnectorOptions.CLIENT_METRICS_EXPORT_TO_CLOUD_MONITORING)
                        .orElse(null));
    }

    /**
     * Hands each declared option to a builder's setter, under its key if the setter refuses it.
     *
     * @param toFlink the builder's {@code exportClientMetricsToFlink}
     * @param toCloudMonitoring the builder's {@code exportClientMetricsToCloudMonitoring}
     */
    public void applyTo(Consumer<Boolean> toFlink, Consumer<Boolean> toCloudMonitoring) {
        OptionSetters.accept(
                SpannerConnectorOptions.CLIENT_METRICS_EXPORT_TO_FLINK.key(),
                exportToFlink,
                toFlink);
        OptionSetters.accept(
                SpannerConnectorOptions.CLIENT_METRICS_EXPORT_TO_CLOUD_MONITORING.key(),
                exportToCloudMonitoring,
                toCloudMonitoring);
    }

    /** Returns whether the client metrics are exported to Flink, the default when unset. */
    public boolean exportToFlink() {
        return exportToFlink != null
                ? exportToFlink
                : SpannerClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK;
    }

    /**
     * Returns whether the client takes part in the Cloud Monitoring export, the default when unset.
     */
    public boolean exportToCloudMonitoring() {
        return exportToCloudMonitoring != null
                ? exportToCloudMonitoring
                : SpannerClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerClientMetricsConfig)) {
            return false;
        }
        SpannerClientMetricsConfig that = (SpannerClientMetricsConfig) other;
        return Objects.equals(exportToFlink, that.exportToFlink)
                && Objects.equals(exportToCloudMonitoring, that.exportToCloudMonitoring);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exportToFlink, exportToCloudMonitoring);
    }
}
