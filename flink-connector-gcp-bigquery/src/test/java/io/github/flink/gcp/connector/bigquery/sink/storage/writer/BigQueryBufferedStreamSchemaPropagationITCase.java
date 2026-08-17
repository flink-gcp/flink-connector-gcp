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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.FieldValueList;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.committer.BufferedStreamCommitter;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Manual real-service probe for connector-driven buffered-stream schema propagation.
 *
 * <p>This class outlives the default integration-test fork ceiling (issue #959), which is 90
 * minutes against the three hours declared below. Run it with the ceiling raised, or surefire kills
 * the fork mid-probe and reports a timeout that says nothing about the measurement:
 *
 * <pre>{@code
 * mvn -pl flink-connector-gcp-bigquery surefire:test@integration-tests \
 *     -Dtest.excluded.groups= -Dit.fork.timeout.seconds=14400 -Dtest=BigQueryBufferedStreamSchemaPropagationITCase
 * }</pre>
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_BUFFERED_SCHEMA_EVOLUTION", matches = ".+")
@Timeout(10800)
class BigQueryBufferedStreamSchemaPropagationITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryBufferedStreamSchemaPropagationITCase.class);
    private static final String TABLE = "buffered_schema_propagation_it_" + TestNames.runId();

    @AfterAll
    static void cleanUp() {
        if (RealBigQuery.project() != null && RealBigQuery.dataset() != null) {
            RealBigQuery.deleteTables(TABLE);
        }
    }

    @Test
    void connectorDrivenEvolutionPropagatesWithoutReplacingTheStream() throws Exception {
        String projectKey = "BQ_IT_" + "PROJECT";
        String datasetKey = "BQ_IT_" + "DATASET";
        assertThat(RealBigQuery.project()).as(projectKey + " is set").isNotBlank();
        assertThat(RealBigQuery.dataset()).as(datasetKey + " is set").isNotBlank();

        RealBigQuery.createTable(TABLE, BigQueryBufferedStreamSchemaEvolutionITCase.V1);
        TableDestination destination = RealBigQuery.destination(TABLE);
        EvolvingSerializer serializer =
                new EvolvingSerializer(BigQueryBufferedStreamSchemaEvolutionITCase.V1);
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(500))
                        .recoveryMaxBackoff(Duration.ofSeconds(10))
                        .recoveryMaxAttempts(10)
                        .build();
        BigQuerySinkConfig<String> config =
                BigQueryBufferedStreamSchemaEvolutionITCase.config(
                        destination,
                        serializer,
                        options,
                        SchemaUpdateOptions.builder().allowNewFields().build());
        WriteClientBufferedStreamServiceFactory serviceFactory =
                new WriteClientBufferedStreamServiceFactory();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        BigQueryBufferedStreamWriter<String> writer =
                new BigQueryBufferedStreamWriter<>(
                        config,
                        options,
                        serviceFactory,
                        new BigQueryTableAdmin(),
                        metrics,
                        0,
                        List.of());
        BufferedStreamCommitter committer =
                new BufferedStreamCommitter(
                        serviceFactory, null, options, CreateDisposition.CREATE_NEVER);
        Instant started = Instant.now();
        try {
            writer.write("alice", TestContexts.NO_OP);
            writer.flush(false);
            var beforeCommit = writer.prepareCommit();
            assertThat(beforeCommit).hasSize(1);
            BufferedStreamWriterState before = writer.snapshotState(1).get(0);
            BufferedStreamCommitTestUtils.commit(committer, beforeCommit);

            serializer.evolveTo(BigQueryBufferedStreamSchemaEvolutionITCase.V2);
            writer.write("bob:hello", TestContexts.NO_OP);
            writer.flush(false);
            var afterCommit = writer.prepareCommit();
            assertThat(afterCommit).hasSize(1);
            BufferedStreamWriterState after = writer.snapshotState(2).get(0);
            BufferedStreamCommitTestUtils.commit(committer, afterCommit);

            assertThat(after.getStreamName()).isEqualTo(before.getStreamName());
            assertThat(metrics.counterValue("schemaReconciliations")).isEqualTo(1);
        } finally {
            LOG.info(
                    "Buffered schema propagation probe ran for {}",
                    Duration.between(started, Instant.now()));
            writer.close();
            committer.close();
        }

        assertThat(RealBigQuery.tableFields(TABLE).get("note")).isNotNull();
        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT name, note FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " ORDER BY name");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get(0).getStringValue()).isEqualTo("alice");
        assertThat(rows.get(0).get(1).isNull()).isTrue();
        assertThat(rows.get(1).get(0).getStringValue()).isEqualTo("bob");
        assertThat(rows.get(1).get(1).getStringValue()).isEqualTo("hello");
    }
}
