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
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Fast real-service acceptance test for mid-stream buffered-writer schema refresh. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(180)
class BigQueryBufferedStreamSchemaEvolutionITCase {

    private static final String TABLE = "buffered_schema_evolution_it_" + TestNames.runId();
    static final TableSchema V1 =
            TableSchema.newBuilder().addFields(nullableString("name")).build();
    static final TableSchema V2 = V1.toBuilder().addFields(nullableString("note")).build();

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void externallyWidenedTableKeepsTheSameBufferedStream() throws Exception {
        RealBigQuery.createTable(TABLE, V2);
        TableDestination destination = RealBigQuery.destination(TABLE);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BufferedStreamOptions options = BufferedStreamOptions.builder().build();
        BigQuerySinkConfig<String> config =
                config(destination, serializer, options, SchemaUpdateOptions.defaults());
        WriteClientBufferedStreamServiceFactory serviceFactory =
                new WriteClientBufferedStreamServiceFactory();
        BigQueryBufferedStreamWriter<String> writer =
                new BigQueryBufferedStreamWriter<>(
                        config,
                        options,
                        serviceFactory,
                        new BigQueryTableAdmin(),
                        TestSinkWriterMetricGroup.create(),
                        0,
                        List.of());
        BufferedStreamCommitter committer =
                new BufferedStreamCommitter(
                        serviceFactory, null, options, CreateDisposition.CREATE_NEVER);
        try {
            writer.write("alice", TestContexts.NO_OP);
            writer.flush(false);
            var beforeCommit = writer.prepareCommit();
            assertThat(beforeCommit).hasSize(1);
            BufferedStreamWriterState before = writer.snapshotState(1).get(0);
            BufferedStreamCommitTestUtils.commit(committer, beforeCommit);

            serializer.evolveTo(V2);
            writer.write("bob:hello", TestContexts.NO_OP);
            writer.flush(false);
            var afterCommit = writer.prepareCommit();
            assertThat(afterCommit).hasSize(1);
            BufferedStreamWriterState after = writer.snapshotState(2).get(0);
            BufferedStreamCommitTestUtils.commit(committer, afterCommit);

            assertThat(after.getStreamName()).isEqualTo(before.getStreamName());
            assertThat(before.getNextOffset()).isEqualTo(1);
            assertThat(after.getNextOffset()).isEqualTo(2);
        } finally {
            writer.close();
            committer.close();
        }

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

    static BigQuerySinkConfig<String> config(
            TableDestination destination,
            EvolvingSerializer serializer,
            BufferedStreamOptions options,
            SchemaUpdateOptions schemaUpdateOptions) {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .destination(destination)
                                .serializer(serializer)
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .schemaUpdateOptions(schemaUpdateOptions)
                                .bufferedStreamOptions(options)
                                .build();
        return sink.getConfig();
    }

    private static TableFieldSchema nullableString(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }
}
