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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;

/**
 * Builds {@link BigtableWriterOptions} from the table options.
 *
 * <p>Every knob is applied with {@code getOptional(...).ifPresent(...)}, so an option absent from
 * the DDL leaves the builder at its own default and an empty configuration produces exactly {@link
 * BigtableWriterOptions#defaults()}. That is the whole contract of this class: it adds no defaults
 * of its own, and a bad <em>value</em> is rejected by the builder's own checks, so a SQL user gets
 * the same message a DataStream user does.
 *
 * <p>No cross-check is restated here. Each of the writer's own is either about a single value (the
 * batch ceilings, which name the number rather than a setter) or compares two knobs whose DDL keys
 * are spelled the same way as their setters ({@code sink.recovery.max-backoff} against {@code
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

        config.getOptional(BigtableConnectorOptions.SINK_BATCHING_ELEMENT_COUNT)
                .ifPresent(builder::batchElementCount);
        config.getOptional(BigtableConnectorOptions.SINK_BATCHING_BYTE_SIZE)
                .map(MemorySize::getBytes)
                .ifPresent(builder::batchByteSize);

        config.getOptional(BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_ENTRIES)
                .ifPresent(builder::maxInFlightEntries);
        config.getOptional(BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES)
                .map(MemorySize::getBytes)
                .ifPresent(builder::maxInFlightBytes);
        config.getOptional(BigtableConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS)
                .ifPresent(builder::maxConsecutiveRejections);

        config.getOptional(BigtableConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF)
                .ifPresent(builder::recoveryInitialBackoff);
        config.getOptional(BigtableConnectorOptions.SINK_RECOVERY_MAX_BACKOFF)
                .ifPresent(builder::recoveryMaxBackoff);
        config.getOptional(BigtableConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS)
                .ifPresent(builder::recoveryMaxAttempts);

        config.getOptional(BigtableConnectorOptions.SINK_DESTINATION_IDLE_TIMEOUT)
                .ifPresent(builder::destinationIdleTimeout);
        config.getOptional(BigtableConnectorOptions.SINK_METRICS_PER_DESTINATION)
                .ifPresent(builder::perDestinationMetrics);

        return builder.build();
    }
}
