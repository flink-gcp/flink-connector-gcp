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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

/**
 * Builds {@link PubSubPublisherOptions} from the table options.
 *
 * <p>Every knob is applied with {@code getOptional(...).ifPresent(...)}, so an option absent from
 * the DDL leaves the builder at its own default and an empty configuration produces exactly {@link
 * PubSubPublisherOptions#defaults()}. That is the whole contract of this class: it adds no defaults
 * of its own, and it performs no validation — the builder's own {@code Preconditions} are the one
 * place a bad value is rejected, so a SQL user gets the same message a DataStream user does.
 */
@Internal
public final class PublisherOptionsMapper {

    private PublisherOptionsMapper() {}

    /**
     * Maps the table options onto publisher options.
     *
     * @param config the table options
     * @return the publisher options; exactly {@link PubSubPublisherOptions#defaults()} when the
     *     configuration sets none of them
     */
    public static PubSubPublisherOptions map(ReadableConfig config) {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();

        config.getOptional(PubSubConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD)
                .ifPresent(builder::batchElementCountThreshold);
        config.getOptional(PubSubConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD)
                .map(MemorySize::getBytes)
                .ifPresent(builder::batchRequestByteThreshold);
        config.getOptional(PubSubConnectorOptions.SINK_BATCHING_DELAY_THRESHOLD)
                .ifPresent(builder::batchDelayThreshold);

        config.getOptional(PubSubConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT)
                .ifPresent(builder::retryTotalTimeout);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_INITIAL_DELAY)
                .ifPresent(builder::retryInitialDelay);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER)
                .ifPresent(builder::retryDelayMultiplier);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_MAX_DELAY)
                .ifPresent(builder::retryMaxDelay);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT)
                .ifPresent(builder::retryInitialRpcTimeout);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER)
                .ifPresent(builder::retryRpcTimeoutMultiplier);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT)
                .ifPresent(builder::retryMaxRpcTimeout);
        config.getOptional(PubSubConnectorOptions.SINK_RETRY_MAX_ATTEMPTS)
                .ifPresent(builder::retryMaxAttempts);

        config.getOptional(PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED)
                .ifPresent(builder::enableMessageOrdering);
        config.getOptional(PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_MESSAGES)
                .ifPresent(builder::maxInFlightMessages);
        config.getOptional(PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES)
                .map(MemorySize::getBytes)
                .ifPresent(builder::maxInFlightBytes);

        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF)
                .ifPresent(builder::recoveryInitialBackoff);
        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF)
                .ifPresent(builder::recoveryMaxBackoff);
        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS)
                .ifPresent(builder::recoveryMaxAttempts);

        return builder.build();
    }
}
