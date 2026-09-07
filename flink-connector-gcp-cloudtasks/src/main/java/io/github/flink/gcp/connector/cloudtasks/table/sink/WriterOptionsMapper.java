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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import io.github.flink.gcp.connector.cloudtasks.table.OptionSetters;

import javax.annotation.Nullable;

import java.util.List;

/** Maps Table API options onto the shared delivery mode, writer and staging settings. */
@Internal
public final class WriterOptionsMapper {

    private WriterOptionsMapper() {}

    /** Resolves the delivery mode using the sink builder's default. */
    public static CloudTasksDeliveryGuarantee deliveryGuarantee(ReadableConfig config) {
        return config.getOptional(CloudTasksConnectorOptions.SINK_DELIVERY_GUARANTEE)
                .orElse(CloudTasksDeliveryGuarantee.AT_LEAST_ONCE);
    }

    /** Maps staging settings, rejecting explicit settings for the eager mode. */
    @Nullable
    public static CloudTasksStagedOptions mapStaged(ReadableConfig config) {
        List<ConfigOption<?>> stagedOptions =
                List.of(
                        CloudTasksConnectorOptions.SINK_STAGED_NAME_RETENTION,
                        CloudTasksConnectorOptions.SINK_STAGED_CLOCK_SKEW_ALLOWANCE,
                        CloudTasksConnectorOptions.SINK_STAGED_REQUEST_TIMEOUT,
                        CloudTasksConnectorOptions.SINK_STAGED_MAX_TASKS,
                        CloudTasksConnectorOptions.SINK_STAGED_MAX_BYTES,
                        CloudTasksConnectorOptions.SINK_STAGED_VERIFY_QUEUE_RETENTION,
                        CloudTasksConnectorOptions.SINK_STAGED_EXPIRED_ENVELOPE_POLICY);
        if (deliveryGuarantee(config) != CloudTasksDeliveryGuarantee.EXACTLY_ONCE) {
            for (ConfigOption<?> option : stagedOptions) {
                if (config.getOptional(option).isPresent()) {
                    throw new ValidationException(
                            "Option '"
                                    + option.key()
                                    + "' requires 'sink.delivery-guarantee' = 'exactly-once'.");
                }
            }
            return null;
        }
        CloudTasksStagedOptions.Builder builder = CloudTasksStagedOptions.builder();
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_STAGED_NAME_RETENTION,
                builder::nameRetention);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_STAGED_CLOCK_SKEW_ALLOWANCE,
                builder::clockSkewAllowance);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_STAGED_REQUEST_TIMEOUT,
                builder::requestTimeout);
        OptionSetters.apply(
                config, CloudTasksConnectorOptions.SINK_STAGED_MAX_TASKS, builder::maxStagedTasks);
        OptionSetters.apply(
                config, CloudTasksConnectorOptions.SINK_STAGED_MAX_BYTES, builder::maxStagedBytes);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_STAGED_VERIFY_QUEUE_RETENTION,
                builder::verifyQueueRetention);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_STAGED_EXPIRED_ENVELOPE_POLICY,
                builder::expiredEnvelopePolicy);
        try {
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw new ValidationException(
                    "Options 'sink.staged.name-retention', 'sink.staged.clock-skew-allowance' and"
                            + " 'sink.staged.request-timeout' must leave at least one whole millisecond"
                            + " for send authorization.",
                    e);
        }
    }

    public static CloudTasksWriterOptions map(ReadableConfig config) {
        CloudTasksWriterOptions.Builder builder = CloudTasksWriterOptions.builder();
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_IN_FLIGHT_MAX_TASKS,
                builder::maxInFlightTasks);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_CHANNEL_POOL_SIZE,
                builder::channelPoolSize);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                builder::recoveryInitialBackoff);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                builder::recoveryMaxBackoff);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                builder::recoveryMaxAttempts);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_NOT_FOUND_INITIAL_BACKOFF,
                builder::notFoundRecoveryInitialBackoff);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_NOT_FOUND_MAX_BACKOFF,
                builder::notFoundRecoveryMaxBackoff);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_RECOVERY_NOT_FOUND_MAX_ATTEMPTS,
                builder::notFoundRecoveryMaxAttempts);
        OptionSetters.apply(
                config,
                CloudTasksConnectorOptions.SINK_METRICS_PER_DESTINATION,
                builder::perDestinationMetrics);
        return builder.build();
    }
}
