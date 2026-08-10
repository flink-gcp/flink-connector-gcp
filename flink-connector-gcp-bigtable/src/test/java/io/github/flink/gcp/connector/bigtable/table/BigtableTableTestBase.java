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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared harness for the table integration tests: the emulator container and its helpers from the
 * sink harness, plus the two things a SQL test needs on top — a {@link TableEnvironment} and a
 * {@code WITH} clause carrying the emulator endpoint.
 *
 * <p>Endpoint injection is plain interpolation into the DDL, so the tests exercise the production
 * factory and its {@code emulator-endpoint} option rather than a test-only factory.
 */
abstract class BigtableTableTestBase extends AbstractBigtableEmulatorITCase {

    static TableEnvironment streamingTableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    /**
     * Renders a {@code WITH} clause for the {@code bigtable} connector: the connector, the
     * destination and the emulator endpoint are always present, and the given pairs are added on
     * top.
     *
     * @param tableId the Bigtable table the SQL table names
     * @param keysAndValues alternating option keys and values
     * @return the rendered clause, including the {@code WITH} keyword
     */
    static String withOptions(String tableId, String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put("project", PROJECT);
        options.put("instance", INSTANCE);
        options.put("table", tableId);
        options.put("emulator-endpoint", emulatorEndpoint());
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }
}
