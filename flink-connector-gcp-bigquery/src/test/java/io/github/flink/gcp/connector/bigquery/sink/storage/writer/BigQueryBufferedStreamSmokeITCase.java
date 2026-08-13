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

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test of the production {@link WriteClientBufferedStreamService} wiring against the BigQuery
 * emulator: create a BUFFERED stream, append two offset batches, flush once, and read the rows back
 * over REST.
 *
 * <p>Deliberately a single-flush smoke test only: the emulator (0.8.1) keeps no flush cursor —
 * every {@code FlushRows} call re-inserts all rows up to the requested offset, so a second flush
 * would duplicate previously flushed rows, and buffered appends neither honor the request offset
 * nor raise {@code OFFSET_ALREADY_EXISTS} (reported upstream as goccy/bigquery-emulator#505). The
 * exactly-once semantics (idempotent re-flush, the restore probe) are therefore covered by unit
 * tests and the real-GCP IT, not here.
 */
class BigQueryBufferedStreamSmokeITCase extends AbstractBigQueryEmulatorITCase {

    @Test
    void createsAppendsFlushesAndReadsBack() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "buffered_smoke");
        createTable("buffered_smoke", serializer.getTableSchema(destination));

        try (BufferedStreamService service =
                new WriteClientBufferedStreamService(
                        null,
                        BufferedStreamOptions.builder().build(),
                        EmulatorEndpoint.parse(grpcEndpoint()))) {
            String streamName = service.createBufferedStream(destination);
            assertThat(streamName).contains("buffered_smoke");

            try (OffsetRowAppender appender =
                    service.openAppender(streamName, serializer.getDescriptor(destination))) {
                appender.append(
                                ProtoRows.newBuilder()
                                        .addSerializedRows(serializer.serialize("alice"))
                                        .addSerializedRows(serializer.serialize("bob"))
                                        .build(),
                                0)
                        .get(30, TimeUnit.SECONDS);
                appender.append(
                                ProtoRows.newBuilder()
                                        .addSerializedRows(serializer.serialize("carol"))
                                        .build(),
                                2)
                        .get(30, TimeUnit.SECONDS);
            }

            service.flushRows(streamName, 2);
        }

        assertThat(queryNames("buffered_smoke")).containsExactly("alice", "bob", "carol");
    }

    @Test
    void serviceCreatesAndAppendsToTwoDestinations() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        TableDestination first = TableDestination.of(PROJECT, DATASET, "buffered_dynamic_first");
        TableDestination second = TableDestination.of(PROJECT, DATASET, "buffered_dynamic_second");
        createTable("buffered_dynamic_first", serializer.getTableSchema(first));
        createTable("buffered_dynamic_second", serializer.getTableSchema(second));
        try (BufferedStreamService service =
                new WriteClientBufferedStreamService(
                        null,
                        BufferedStreamOptions.builder().build(),
                        EmulatorEndpoint.parse(grpcEndpoint()))) {
            String firstStream = service.createBufferedStream(first);
            String secondStream = service.createBufferedStream(second);
            assertThat(firstStream).contains("buffered_dynamic_first");
            assertThat(secondStream).contains("buffered_dynamic_second");

            long firstOffset = appendOne(service, serializer, first, firstStream, "first-alice");
            long secondOffset = appendOne(service, serializer, second, secondStream, "second-bob");
            // The emulator assigns offsets globally across buffered streams and can hang while
            // flushing more than one of them. The writer's per-destination offsets and commits are
            // therefore covered by fakes and real GCP; this smoke test pins the production
            // service's two-destination create/open/append wiring only.
            assertThat(firstOffset).isNotNegative();
            assertThat(secondOffset).isNotNegative();
        }
    }

    private static long appendOne(
            BufferedStreamService service,
            NameColumnSerializer serializer,
            TableDestination destination,
            String streamName,
            String value)
            throws Exception {
        try (OffsetRowAppender appender =
                service.openAppender(streamName, serializer.getDescriptor(destination))) {
            AppendRowsResponse response =
                    appender.append(
                                    ProtoRows.newBuilder()
                                            .addSerializedRows(serializer.serialize(value))
                                            .build(),
                                    0)
                            .get(30, TimeUnit.SECONDS);
            return response.getAppendResult().getOffset().getValue();
        }
    }
}
