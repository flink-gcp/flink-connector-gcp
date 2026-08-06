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

package io.github.flink.gcp.connector.bigquery.table;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders the {@code WITH} clause the table tests hand to the planner.
 *
 * <p>The rendering only — each test class keeps its own wrapper, because what a table is configured
 * with differs by class (the emulator's two endpoints, a real project and dataset, a staging path)
 * and so do the gating annotations the shape of a shared base class would drag along. What is here
 * is the part that can drift silently: the quoting and the separator.
 */
final class TableDdl {

    private TableDdl() {}

    /**
     * Renders {@code WITH (...)} over the connector, the destination and the given alternating keys
     * and values.
     *
     * @param project the destination project
     * @param dataset the destination dataset
     * @param table the destination table
     * @param keysAndValues alternating option keys and values
     * @return the clause, ready to append to a {@code CREATE TABLE (...)}
     */
    static String withOptions(
            String project, String dataset, String table, String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigQueryDynamicTableFactory.IDENTIFIER);
        options.put("project", project);
        options.put("dataset", dataset);
        options.put("table", table);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }

    /**
     * Joins two alternating key/value arrays, for a wrapper that fixes some options of its own.
     *
     * @param fixed the wrapper's own keys and values, applied first
     * @param keysAndValues the caller's, which may override them
     * @return the concatenation
     */
    static String[] concat(String[] fixed, String... keysAndValues) {
        String[] all = Arrays.copyOf(fixed, fixed.length + keysAndValues.length);
        System.arraycopy(keysAndValues, 0, all, fixed.length, keysAndValues.length);
        return all;
    }
}
