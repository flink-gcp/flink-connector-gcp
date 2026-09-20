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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.bigquery.BigQueryEmulatorContainers;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises default-stream RPC wiring; buffered-stream offsets require real-service validation. */
@Testcontainers
@Timeout(180)
@Execution(ExecutionMode.SAME_THREAD)
class RecoveryJobITCase {
    @Container
    static final GenericContainer<?> EMULATOR =
            BigQueryEmulatorContainers.newContainer(
                    RecoveryOptions.PROJECT, RecoveryOptions.DATASET);

    @ParameterizedTest
    @CsvSource({"ALO,10", "ALO,50"})
    void productionWriterWritesEveryRowToItsExpectedDestination(String mode, int destinations)
            throws Exception {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--run-id",
                                "it-"
                                        + mode.toLowerCase(java.util.Locale.ROOT)
                                        + "-"
                                        + destinations,
                                "--mode",
                                mode,
                                "--destinations",
                                Integer.toString(destinations),
                                "--records",
                                Integer.toString(2 * destinations)));
        var client = BigQueryEmulatorContainers.restClient(EMULATOR, RecoveryOptions.PROJECT);
        var schema =
                Schema.of(
                        Field.of("run_id", StandardSQLTypeName.STRING),
                        Field.of("sequence", StandardSQLTypeName.INT64),
                        Field.of("destination", StandardSQLTypeName.INT64),
                        Field.of("payload", StandardSQLTypeName.BYTES));
        for (int destination = 0; destination < destinations; destination++) {
            var table = options.table(destination);
            client.create(
                    TableInfo.of(
                            TableId.of(table.getProject(), table.getDataset(), table.getTable()),
                            StandardTableDefinition.of(schema)));
        }
        var sink =
                BigQueryRecoveryJob.sink(
                        options,
                        BigQueryEmulatorContainers.grpcEndpoint(EMULATOR),
                        BigQueryEmulatorContainers.restEndpoint(EMULATOR));
        try (var writer = sink.createWriter(new StubWriterInitContext(0))) {
            SinkWriter.Context context =
                    new SinkWriter.Context() {
                        @Override
                        public long currentWatermark() {
                            return Long.MIN_VALUE;
                        }

                        @Override
                        public Long timestamp() {
                            return null;
                        }
                    };
            // Concurrent appends lock the emulator's SQLite database. Exercise each destination's
            // production RPC path sequentially; RecoveryRoutingITCase measures the parallel graph.
            // Each stream gets one append, avoiding the emulator's follow-up stream-binding bug.
            for (long destination = 0; destination < destinations; destination++) {
                writer.write(destination, context);
                writer.write(destination + destinations, context);
                writer.flush(false);
            }
        }
        Set<Long> seen = new HashSet<>();
        for (int destination = 0; destination < destinations; destination++) {
            var table = options.table(destination);
            var rows =
                    client.query(
                            QueryJobConfiguration.newBuilder(
                                            "SELECT run_id, sequence, destination FROM `"
                                                    + table.getProject()
                                                    + "."
                                                    + table.getDataset()
                                                    + "."
                                                    + table.getTable()
                                                    + "`")
                                    .setUseLegacySql(false)
                                    .build());
            assertThat(rows.getTotalRows()).isEqualTo(2);
            for (var row : rows.iterateAll()) {
                long sequence = row.get("sequence").getLongValue();
                assertThat(row.get("run_id").getStringValue()).isEqualTo(options.runId);
                assertThat(sequence % destinations).isEqualTo(destination);
                assertThat(row.get("destination").getLongValue()).isEqualTo(destination);
                assertThat(seen.add(sequence)).isTrue();
            }
        }
        assertThat(seen)
                .containsExactlyInAnyOrderElementsOf(
                        java.util.stream.LongStream.range(0, options.records).boxed().toList());
    }
}
