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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.storage.writer.AbstractBigQueryEmulatorITCase;

/**
 * Harness for the SQL integration tests: the emulator container from the writer suite, a streaming
 * {@link TableEnvironment}, and a {@code WITH} clause carrying the emulator's two endpoints.
 *
 * <p>The endpoints are interpolated into the DDL rather than injected, so these tests drive the
 * production factory through the planner — which is the whole point of the exercise, and the one
 * thing the writer suite's {@code @VisibleForTesting} seam cannot cover.
 */
abstract class BigQueryTableTestBase extends AbstractBigQueryEmulatorITCase {

    static TableEnvironment streamingTableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    /**
     * Renders a {@code WITH} clause over the destination and the two emulator endpoints, plus the
     * given alternating keys and values.
     */
    static String withOptions(String table, String... keysAndValues) {
        String[] endpoints = {
            "emulator-endpoint", grpcEndpoint(),
            "emulator-rest-endpoint", restEndpoint()
        };
        return TableDdl.withOptions(
                PROJECT, DATASET, table, TableDdl.concat(endpoints, keysAndValues));
    }
}
