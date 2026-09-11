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
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

import javax.annotation.Nullable;

/** Maps checkpoint-owned delivery options without performing remote service validation. */
@Internal
public final class StagedOptionsMapper {
    private StagedOptionsMapper() {}

    /**
     * Requires explicit replay-marker and transactional-profile configuration for staged writes.
     */
    @Nullable
    public static BigtableStagedOptions map(ReadableConfig config, boolean staged) {
        if (!staged) {
            for (var option :
                    java.util.List.of(
                            BigtableConnectorOptions.SINK_STAGED_MARKER_FAMILY,
                            BigtableConnectorOptions.SINK_STAGED_MAX_ENTRIES,
                            BigtableConnectorOptions.SINK_STAGED_MAX_BYTES)) {
                if (config.getOptional(option).isPresent()) {
                    throw new ValidationException(
                            "Option '"
                                    + option.key()
                                    + "' requires 'sink.delivery-guarantee' = 'exactly-once'.");
                }
            }
            return null;
        }
        String profile =
                config.getOptional(BigtableConnectorOptions.SINK_APP_PROFILE_ID)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Option 'sink.app-profile-id' is required for 'sink.delivery-guarantee' = 'exactly-once'."));
        OptionSetters.accept(
                BigtableConnectorOptions.SINK_APP_PROFILE_ID.key(),
                profile,
                value -> ResourceNames.checkComponent(value, "appProfileId"));
        if (config.getOptional(BigtableConnectorOptions.SINK_CREATE_DISPOSITION)
                        .orElse(CreateDisposition.CREATE_NEVER)
                != CreateDisposition.CREATE_NEVER) {
            throw new ValidationException(
                    "Option 'sink.create-disposition' must be 'create-never' for 'sink.delivery-guarantee' = 'exactly-once'; provision the data and marker families first.");
        }
        if (config.getOptional(BigtableConnectorOptions.SINK_STAGED_MARKER_FAMILY).isEmpty()) {
            throw new ValidationException(
                    "Option 'sink.staged.marker-family' is required for 'sink.delivery-guarantee' = 'exactly-once'.");
        }
        BigtableStagedOptions.Builder builder =
                BigtableStagedOptions.builder().requestOptions(RequestOptionsMapper.map(config));
        OptionSetters.apply(
                config, BigtableConnectorOptions.SINK_STAGED_MARKER_FAMILY, builder::markerFamily);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_STAGED_MAX_ENTRIES,
                builder::maxStagedEntries);
        OptionSetters.apply(
                config,
                BigtableConnectorOptions.SINK_STAGED_MAX_BYTES,
                value -> builder.maxStagedBytes(value.getBytes()));
        return builder.build();
    }
}
