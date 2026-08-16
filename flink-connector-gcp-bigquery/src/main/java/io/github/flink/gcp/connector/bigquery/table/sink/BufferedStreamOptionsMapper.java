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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Maps the {@code sink.buffered-stream.*} options onto {@link BufferedStreamOptions}.
 *
 * <p>Under the same contract as {@code DefaultStreamOptionsMapper}: every knob is applied through
 * the builder, no default is introduced, and value validation stays with that builder so a SQL user
 * gets the message a DataStream user gets.
 *
 * <p><b>Unlike that mapper, this one always builds</b>, and the difference is not a stylistic one.
 * {@code defaultStreamOptions(...)} is optional on {@code BigQuerySinkBuilder}; {@code
 * bufferedStreamOptions(...)} is <em>required</em> for {@code STORAGE_API_EXACTLY_ONCE}. So the
 * decision of whether an options object is wanted belongs to the write method, which the factory
 * knows, and not to key presence — a DDL selecting exactly-once and tuning nothing would otherwise
 * be told that {@code bufferedStreamOptions(...) is required}, a builder method it never called and
 * cannot call. Every knob here is defaulted, so an untuned {@code builder().build()} is exactly
 * what that DDL means.
 *
 * <p>{@link #presentKeys(ReadableConfig)} survives for the factory's wrong-family check, which is
 * the other half: these keys under any other write method are rejected naming the keys themselves.
 */
@Internal
public final class BufferedStreamOptionsMapper {

    /** Every key of the family, for the "is any of these set?" scan. */
    private static final List<ConfigOption<?>> FAMILY =
            Arrays.asList(
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_MAX_APPEND_REQUEST_BYTES,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_DESTINATION_IDLE_TIMEOUT,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_INITIAL_DELAY,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_DELAY_MULTIPLIER,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DELAY,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DURATION);

    private BufferedStreamOptionsMapper() {}

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
     * Builds the options, leaving every knob the configuration does not set at its default.
     *
     * @param config the table's options
     * @return the options, never {@code null}
     */
    public static BufferedStreamOptions map(ReadableConfig config) {
        BufferedStreamOptions.Builder builder = BufferedStreamOptions.builder();

        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_MAX_APPEND_REQUEST_BYTES)
                .map(MemorySize::getBytes)
                .ifPresent(builder::maxAppendRequestBytes);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_DESTINATION_IDLE_TIMEOUT)
                .ifPresent(builder::destinationIdleTimeout);

        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_INITIAL_BACKOFF)
                .ifPresent(builder::recoveryInitialBackoff);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_BACKOFF)
                .ifPresent(builder::recoveryMaxBackoff);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_ATTEMPTS)
                .ifPresent(builder::recoveryMaxAttempts);

        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_INITIAL_DELAY)
                .ifPresent(builder::retryInitialDelay);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_DELAY_MULTIPLIER)
                .ifPresent(builder::retryDelayMultiplier);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DELAY)
                .ifPresent(builder::retryMaxDelay);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_ATTEMPTS)
                .ifPresent(builder::retryMaxAttempts);
        config.getOptional(BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DURATION)
                .ifPresent(builder::maxRetryDuration);

        return builder.build();
    }
}
