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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.bigquery.sink.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for per-record dynamic destinations against the BigQuery emulator
 * (goccy/bigquery-emulator): one facade-built writer fanning records out to multiple tables through
 * a {@code destinationResolver}.
 */
class BigQueryDynamicDestinationsITCase extends AbstractBigQueryEmulatorITCase {

    @Test
    void dynamicDestinationsFanOutToMultipleTables() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        createTable("fanout_even", serializer.getTableSchema(null));
        createTable("fanout_odd", serializer.getTableSchema(null));
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        PROJECT,
                                                        DATASET,
                                                        element.length() % 2 == 0
                                                                ? "fanout_even"
                                                                : "fanout_odd"))
                                .serializer(serializer)
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            writer.write("dave", CONTEXT);
            writer.write("eve", CONTEXT);
            writer.write("mallory", CONTEXT);
            writer.flush(true);
            assertThat(queryNames("fanout_even")).containsExactly("dave");
            assertThat(queryNames("fanout_odd")).containsExactly("eve", "mallory");
        } finally {
            writer.close();
        }
    }
}
