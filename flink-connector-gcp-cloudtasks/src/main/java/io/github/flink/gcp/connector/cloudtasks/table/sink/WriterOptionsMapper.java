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
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import io.github.flink.gcp.connector.cloudtasks.table.OptionSetters;

/** Maps Table API options onto the DataStream writer options. */
@Internal
public final class WriterOptionsMapper {

    private WriterOptionsMapper() {}

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
