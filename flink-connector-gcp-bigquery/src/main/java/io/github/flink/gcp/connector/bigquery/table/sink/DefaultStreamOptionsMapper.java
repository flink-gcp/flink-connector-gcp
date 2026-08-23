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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import io.github.flink.gcp.connector.bigquery.table.OptionSetters;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Maps the {@code sink.default-stream.*} options onto {@link DefaultStreamOptions}.
 *
 * <p>Adds no defaults of its own and owns no bounds: an option left out of the DDL leaves its
 * setter uncalled through {@link OptionSetters}, the builder's own {@code Preconditions} are the
 * one place a bad value is rejected, and the rejection is renamed to the option key the SQL caller
 * wrote (issue #1030).
 *
 * <p>Returns {@code null} when no key of the family is set, which is the state the builder reads as
 * "this write method's options were not configured" — the same shape {@code
 * defaultStreamOptions(...)} has on the DataStream API, where it is the one write-method options
 * object that is optional.
 */
@Internal
public final class DefaultStreamOptionsMapper {

    /** Every key of the family, for the "is any of these set?" scan. */
    private static final List<ConfigOption<?>> FAMILY =
            Arrays.asList(
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_APPEND_REQUEST_BYTES,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_INITIAL_DELAY,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_DELAY_MULTIPLIER,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DELAY,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DURATION,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_REQUESTS,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_BYTES,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MIN_CONNECTIONS_PER_REGION,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_CONNECTIONS_PER_REGION,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_DESTINATION_IDLE_TIMEOUT,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_FLUSH_INTERVAL,
                    BigQueryConnectorOptions.SINK_DEFAULT_STREAM_METRICS_PER_DESTINATION);

    private DefaultStreamOptionsMapper() {}

    /** Returns the keys of the family that the given configuration sets, in declaration order. */
    public static List<String> presentKeys(ReadableConfig config) {
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : FAMILY) {
            if (config.getOptional(option).isPresent()) {
                present.add(option.key());
            }
        }
        return present;
    }

    /**
     * Builds the options, or returns {@code null} when the configuration sets none of the family.
     *
     * @param config the table's options
     * @return the options, or {@code null}
     */
    @Nullable
    public static DefaultStreamOptions map(ReadableConfig config) {
        if (presentKeys(config).isEmpty()) {
            return null;
        }
        DefaultStreamOptions.Builder builder = DefaultStreamOptions.builder();

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_APPEND_REQUEST_BYTES,
                size -> builder.maxAppendRequestBytes(size.getBytes()));

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_INITIAL_BACKOFF,
                builder::recoveryInitialBackoff);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_BACKOFF,
                builder::recoveryMaxBackoff);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_ATTEMPTS,
                builder::recoveryMaxAttempts);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_INITIAL_DELAY,
                builder::retryInitialDelay);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_DELAY_MULTIPLIER,
                builder::retryDelayMultiplier);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DELAY,
                builder::retryMaxDelay);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_ATTEMPTS,
                builder::retryMaxAttempts);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DURATION,
                builder::maxRetryDuration);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_REQUESTS,
                builder::maxInflightRequests);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_BYTES,
                size -> builder.maxInflightBytes(size.getBytes()));
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MIN_CONNECTIONS_PER_REGION,
                builder::minConnectionsPerRegion);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_CONNECTIONS_PER_REGION,
                builder::maxConnectionsPerRegion);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_DESTINATION_IDLE_TIMEOUT,
                builder::destinationIdleTimeout);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_FLUSH_INTERVAL,
                builder::flushInterval);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_METRICS_PER_DESTINATION,
                builder::perDestinationMetrics);

        return builder.build();
    }
}
