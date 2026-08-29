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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

/**
 * Builds {@link BigtableWriterOptions} from the table options.
 *
 * <p>Every knob is applied through {@link OptionSetters}, so an option absent from the DDL leaves
 * the builder at its own default and an empty configuration produces exactly {@link
 * BigtableWriterOptions#defaults()}. That is the whole contract of this class: it adds no defaults
 * of its own, each bound stays in the builder's own checks, and a single value the builder rejects
 * is renamed to the option key the SQL caller wrote (issue #1030).
 *
 * <p>No cross-check is restated here. The writer's one cross-check compares two knobs whose DDL
 * keys are spelled the same way as their setters ({@code sink.recovery.max-backoff} against {@code
 * sink.recovery.initial-backoff}), so the builder's message is readable from a {@code WITH} clause
 * as it stands. The Pub/Sub mapper restates one because its does not.
 */
@Internal
public final class WriterOptionsMapper {

    private WriterOptionsMapper() {}

    /**
     * Maps the table options onto writer options.
     *
     * @param config the table options
     * @return the writer options; exactly {@link BigtableWriterOptions#defaults()} when the
     *     configuration sets none of them
     */
    public static BigtableWriterOptions map(ReadableConfig config) {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD,
                builder::batchElementCountThreshold);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD,
                size -> builder.batchRequestByteThreshold(size.getBytes()));

        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_ENTRIES,
                builder::maxInFlightEntries);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                size -> builder.maxInFlightBytes(size.getBytes()));
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS,
                builder::maxConsecutiveRejections);

        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                builder::recoveryInitialBackoff);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                builder::recoveryMaxBackoff);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                builder::recoveryMaxAttempts);

        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_DESTINATION_IDLE_TIMEOUT,
                builder::destinationIdleTimeout);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_MAX_ACTIVE_INSTANCES,
                builder::maxActiveInstances);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_METRICS_PER_DESTINATION,
                builder::perDestinationMetrics);

        return builder.build();
    }
}
