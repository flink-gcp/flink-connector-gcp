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

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Storage Write API clients run on the connector's own gRPC event-loop group and stop it when
 * they close (#2237).
 *
 * <p>gRPC's shared group is destroyed a second after its last channel closes, which for a job-jar
 * deployment is after Flink has closed the job's class loader; under {@code
 * cluster.uncaught-exception-handling: FAIL} that exits the TaskManager. What makes the connector
 * immune is that its channels never use that group and that its own group has stopped by the time
 * {@code close()} returns, which is what these tests observe through the group's thread names. The
 * emulator path is the one a test can reach; the production default-stream path builds its
 * transport provider in {@code StreamWriterRowAppenderFactory.productionChannelProvider}, which
 * {@code StreamWriterRowAppenderFactoryTest} covers.
 */
class BigQueryWriteEventLoopITCase extends AbstractBigQueryEmulatorITCase {

    private static final String THREAD_PREFIX = "flink-gcp-grpc-elg";

    @Test
    void theDefaultStreamAppenderRunsOnTheGroupAndStopsItOnClose() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "event_loop_default");
        createTable("event_loop_default", serializer.getTableSchema(destination));

        try (RowAppender appender =
                emulatorAppenderFactory()
                        .create(destination, serializer.getDescriptor(destination), null)) {
            appender.append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(serializer.serialize("alice"))
                                    .build())
                    .get(30, TimeUnit.SECONDS);

            assertThat(groupThreadAlive())
                    .as("a %s* thread while appending", THREAD_PREFIX)
                    .isTrue();
        }

        assertThat(awaitNoGroupThread())
                .as("no %s* thread once the appender closed", THREAD_PREFIX)
                .isTrue();
    }

    @Test
    void theBufferedStreamServiceRunsOnTheGroupAndStopsItOnClose() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "event_loop_buffered");
        createTable("event_loop_buffered", serializer.getTableSchema(destination));

        try (BufferedStreamService service =
                new WriteClientBufferedStreamService(
                        null,
                        BufferedStreamOptions.builder().build(),
                        EmulatorEndpoint.parse(grpcEndpoint(), "emulatorEndpoint"))) {
            String streamName = service.createBufferedStream(destination);
            try (OffsetRowAppender appender =
                    service.openAppender(streamName, serializer.getDescriptor(destination))) {
                appender.append(
                                ProtoRows.newBuilder()
                                        .addSerializedRows(serializer.serialize("bob"))
                                        .build(),
                                0)
                        .get(30, TimeUnit.SECONDS);
            }

            assertThat(groupThreadAlive())
                    .as("a %s* thread while the service is open", THREAD_PREFIX)
                    .isTrue();
        }

        assertThat(awaitNoGroupThread())
                .as("no %s* thread once the service closed", THREAD_PREFIX)
                .isTrue();
    }

    /** Waits up to ten seconds for the group's threads to finish exiting. */
    private static boolean awaitNoGroupThread() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (groupThreadAlive()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    private static boolean groupThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().startsWith(THREAD_PREFIX));
    }
}
