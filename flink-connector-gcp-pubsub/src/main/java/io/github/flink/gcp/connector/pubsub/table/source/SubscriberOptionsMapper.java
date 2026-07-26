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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

/**
 * Builds {@link PubSubSubscriberOptions} from the table options.
 *
 * <p>The source-side counterpart of {@code PublisherOptionsMapper}, under the same contract: every
 * knob is applied with {@code getOptional(...).ifPresent(...)}, so an empty configuration produces
 * exactly {@link PubSubSubscriberOptions#defaults()}, no default is introduced here, and validation
 * is left to the builder so a SQL user gets the same message a DataStream user does.
 */
@Internal
public final class SubscriberOptionsMapper {

    private SubscriberOptionsMapper() {}

    /**
     * Maps the table options onto subscriber options.
     *
     * @param config the table options
     * @return the subscriber options; exactly {@link PubSubSubscriberOptions#defaults()} when the
     *     configuration sets none of them
     */
    public static PubSubSubscriberOptions map(ReadableConfig config) {
        PubSubSubscriberOptions.Builder builder = PubSubSubscriberOptions.builder();

        config.getOptional(PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT)
                .ifPresent(builder::flowControlMaxOutstandingElementCount);
        config.getOptional(PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES)
                .map(MemorySize::getBytes)
                .ifPresent(builder::flowControlMaxOutstandingRequestBytes);
        config.getOptional(PubSubConnectorOptions.SCAN_PARALLEL_PULL_COUNT)
                .ifPresent(builder::parallelPullCount);

        config.getOptional(PubSubConnectorOptions.SCAN_ACK_MAX_EXTENSION_PERIOD)
                .ifPresent(builder::maxAckExtensionPeriod);
        config.getOptional(PubSubConnectorOptions.SCAN_ACK_MIN_DURATION_PER_EXTENSION)
                .ifPresent(builder::minDurationPerAckExtension);
        config.getOptional(PubSubConnectorOptions.SCAN_ACK_MAX_DURATION_PER_EXTENSION)
                .ifPresent(builder::maxDurationPerAckExtension);
        config.getOptional(PubSubConnectorOptions.SCAN_ACK_AWAIT_CONFIRMATION)
                .ifPresent(builder::awaitAckConfirmation);

        config.getOptional(PubSubConnectorOptions.SCAN_SHUTDOWN_TIMEOUT)
                .ifPresent(builder::shutdownTimeout);
        config.getOptional(PubSubConnectorOptions.SCAN_MAX_RECORDS_PER_FETCH)
                .ifPresent(builder::maxRecordsPerFetch);
        config.getOptional(PubSubConnectorOptions.SCAN_FIRST_CHECKPOINT_TIMEOUT)
                .ifPresent(builder::firstCheckpointTimeout);

        return builder.build();
    }
}
