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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for table auto-creation against the BigQuery emulator
 * (goccy/bigquery-emulator): the at-least-once writer writing to a table that does not exist,
 * end-to-end through the Storage Write API gRPC endpoint and the REST table-creation path.
 */
class BigQueryTableAutoCreationITCase extends AbstractBigQueryEmulatorITCase {
    private static BigQueryDefaultStreamWriter<String> writer(
            TableDestination destination, CreateDisposition disposition) {
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destination(destination)
                                        .serializer(new NameColumnSerializer())
                                        .createDisposition(disposition)
                                        .build())
                        .getConfig();
        return new BigQueryDefaultStreamWriter<>(
                config,
                new EmulatorAppenderFactory(grpcEndpoint()),
                new BigQueryTableAdmin(restClient),
                TestSinkWriterMetricGroup.create(),
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                new RetrySchedule(100, 1_000, 30, 0),
                new RetrySchedule(100, 1_000, 30, 0));
    }

    @Test
    void createIfNeededCreatesMissingTableAndWritesEndToEnd() throws Exception {
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "auto_created");
        BigQueryDefaultStreamWriter<String> writer =
                writer(destination, CreateDisposition.CREATE_IF_NEEDED);
        try {
            writer.write("alice", CONTEXT);
            writer.write("bob", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(restClient.getTable(TableId.of(DATASET, "auto_created"))).isNotNull();
        assertThat(queryNames("auto_created")).containsExactly("alice", "bob");
    }

    @Test
    void createNeverFailsFastOnMissingTable() throws Exception {
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "never_created");
        BigQueryDefaultStreamWriter<String> writer =
                writer(destination, CreateDisposition.CREATE_NEVER);
        try {
            assertThatThrownBy(
                            () -> {
                                writer.write("alice", CONTEXT);
                                writer.flush(false);
                            })
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CREATE_NEVER");
        } finally {
            writer.close();
        }

        assertThat(restClient.getTable(TableId.of(DATASET, "never_created"))).isNull();
    }
}
