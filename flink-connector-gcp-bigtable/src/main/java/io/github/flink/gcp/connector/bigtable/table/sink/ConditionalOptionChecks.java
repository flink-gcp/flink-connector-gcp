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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;

import java.util.List;
import java.util.Map;

/** Refuses explicit options that the selected write operation cannot use. */
@Internal
public final class ConditionalOptionChecks {
    private ConditionalOptionChecks() {}

    /**
     * Validates only explicit keys, including explicit values equal to an option's default.
     *
     * @param options the catalog table's options
     * @param mode the selected write mode
     */
    public static void validate(Map<String, String> options, WriteMode mode) {
        List<ConfigOption<?>> rejected =
                mode == WriteMode.INSERT_IF_ABSENT
                        ? List.of(
                                BigtableConnectorOptions.SINK_CREATE_DISPOSITION,
                                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS,
                                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE,
                                BigtableConnectorOptions.SINK_INSERT_ONLY_INPUT_MODE,
                                BigtableConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD,
                                BigtableConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD,
                                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_ENTRIES,
                                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                                BigtableConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS,
                                BigtableConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                                BigtableConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                                BigtableConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS)
                        : List.of(
                                BigtableConnectorOptions.SINK_CONDITIONAL_EMPTY_BRANCH_POLICY,
                                BigtableConnectorOptions.SINK_REQUEST_TIMEOUT,
                                BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_REQUESTS);
        for (ConfigOption<?> option : rejected) {
            if (options.containsKey(option.key())) {
                throw new ValidationException(
                        "Option '"
                                + option.key()
                                + "' cannot be used with 'sink.write-mode' = '"
                                + mode
                                + "'.");
            }
        }
    }
}
