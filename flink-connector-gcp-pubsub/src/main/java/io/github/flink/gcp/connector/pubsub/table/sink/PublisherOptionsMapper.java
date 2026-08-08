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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds {@link PubSubPublisherOptions} from the table options.
 *
 * <p>Every knob is applied with {@code getOptional(...).ifPresent(...)}, so an option absent from
 * the DDL leaves the builder at its own default and an empty configuration produces exactly {@link
 * PubSubPublisherOptions#defaults()}. That is the whole contract of this class: it adds no defaults
 * of its own, and a bad <em>value</em> is rejected by the builder's own {@code Preconditions}, so a
 * SQL user gets the same message a DataStream user does.
 *
 * <p><b>One cross-check is restated here in DDL vocabulary</b>, and that is deliberate rather than
 * duplication. A {@code Preconditions} failure inside {@code createDynamicTableSink} is wrapped by
 * Flink's {@code FactoryUtil} into a {@code ValidationException} whose own message says only
 * "Unable to create a sink for writing table ...", leaving the actionable sentence in the cause —
 * and that sentence would name {@code retryTotalTimeout(...)}, which appears nowhere in the user's
 * {@code WITH} clause. {@code TopicCreateOptionsMapper} restates its builder's create-disposition
 * check for the same reason. A check whose message needs no translation stays in the builder alone.
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
        rejectBoundedRetriesWithMessageOrdering(config);
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
        config.getOptional(PubSubConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS)
                .ifPresent(builder::maxConsecutiveRejections);

        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF)
                .ifPresent(builder::recoveryInitialBackoff);
        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF)
                .ifPresent(builder::recoveryMaxBackoff);
        config.getOptional(PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS)
                .ifPresent(builder::recoveryMaxAttempts);

        config.getOptional(PubSubConnectorOptions.SINK_PUBLISH_PROGRESS_TIMEOUT)
                .ifPresent(builder::publishProgressTimeout);
        config.getOptional(PubSubConnectorOptions.SINK_SHUTDOWN_TIMEOUT)
                .ifPresent(builder::shutdownTimeout);

        config.getOptional(PubSubConnectorOptions.SINK_METRICS_PER_DESTINATION)
                .ifPresent(builder::perDestinationMetrics);

        return builder.build();
    }

    /**
     * The builder's ordering-versus-retry-budget check, in the keys the DDL actually spells. The
     * builder still owns it — a DataStream user meets it there — so this fires first only to name
     * {@code sink.retry.*} rather than {@code retryTotalTimeout(...)}.
     */
    private static void rejectBoundedRetriesWithMessageOrdering(ReadableConfig config) {
        if (!config.getOptional(PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED)
                .orElse(false)) {
            return;
        }
        List<String> bounded = new ArrayList<>(2);
        for (ConfigOption<?> option :
                new ConfigOption<?>[] {
                    PubSubConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT,
                    PubSubConnectorOptions.SINK_RETRY_MAX_ATTEMPTS
                }) {
            if (config.getOptional(option).isPresent()) {
                bounded.add("'" + option.key() + "'");
            }
        }
        if (!bounded.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%s cannot be combined with '%s' = 'true': an ordering-enabled publisher"
                                    + " retries without limit, so neither an attempt cap nor a total"
                                    + " timeout can bound a publish there. Remove %s, or disable message"
                                    + " ordering. The other six 'sink.retry.*' options are unaffected.",
                            String.join(" and ", bounded),
                            PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED.key(),
                            String.join(" and ", bounded)));
        }
    }
}
