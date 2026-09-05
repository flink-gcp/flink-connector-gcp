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

import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

/** Maps conditional runtime options, leaving absent values at their builder defaults. */
@Internal
public final class RequestOptionsMapper {
    private RequestOptionsMapper() {}

    /**
     * Maps SQL options with diagnostics in the option-key vocabulary.
     *
     * @param config the table configuration
     * @return the request options
     */
    public static BigtableRequestOptions map(ReadableConfig config) {
        BigtableRequestOptions.Builder builder = BigtableRequestOptions.builder();
        OptionSetters.apply(
                config, BigtableConnectorOptions.SINK_REQUEST_TIMEOUT, builder::requestTimeout);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_REQUESTS,
                builder::maxInFlightRequests);
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
