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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Storage Read API clients — the enumerator's session creator and the reader's stream opener —
 * run on the connector's own gRPC event-loop group and stop it when they close (#2237).
 *
 * <p>See {@code BigQueryWriteEventLoopITCase} for why that is what keeps a job-jar deployment's
 * TaskManager alive under {@code cluster.uncaught-exception-handling: FAIL}. The session creator
 * runs on the JobManager, whose class loader for the job closes the same way. The two clients are
 * tested apart, so that one of them leaving the group is not hidden by the other's threads; that
 * the session client's settings carry the group is {@code BigQueryReadClientsTest}'s to show, since
 * the creator closes its client before a test could look.
 */
class BigQueryReadEventLoopITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String TABLE = "event_loop";
    private static final String THREAD_PREFIX = "flink-gcp-grpc-elg";

    @BeforeAll
    static void seed() throws Exception {
        createTable(
                TABLE,
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        insert(TABLE, "id", "(0), (1)");
    }

    /**
     * The session creator closes its client, and with it the group, as soon as the session exists —
     * before the creator itself is closed. On the JobManager Flink closes the enumerator only after
     * releasing the job's class loader, so a client still open then would tear the group down on a
     * closed loader.
     */
    @Test
    void theSessionCreatorLeavesNoGroupOnceTheSessionExists() throws Exception {
        try (ReadClientSessionCreator creator = new ReadClientSessionCreator(endpoint())) {
            assertThat(creator.create(request()).getStreamsCount()).isPositive();

            // Checked the moment create() returns, not polled: a client closed a moment later
            // would already be too late on a JobManager.
            assertThat(GrpcEventLoops.isRunning())
                    .as("the session's client was still holding the group when create() returned")
                    .isFalse();
            assertThat(awaitNoGroupThread())
                    .as("a %s* thread outlived the session's creation", THREAD_PREFIX)
                    .isTrue();
        }
    }

    @Test
    void theStreamOpenerRunsOnTheGroupAndStopsItOnClose() throws Exception {
        String stream;
        try (ReadClientSessionCreator creator = new ReadClientSessionCreator(endpoint())) {
            stream = creator.create(request()).getStreams(0).getName();
        }
        // The creator's group is gone, so a thread from here on is the opener's.
        assertThat(awaitNoGroupThread()).isTrue();

        try (ReadClientRowStreamOpener opener =
                new ReadClientRowStreamOpener(
                        endpoint(), BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS)) {
            try (RowStream rows = opener.open(stream, 0)) {
                while (rows.next() != null) {
                    // Drained, so the stream is done before the opener closes.
                }
            }

            assertThat(groupThreadAlive())
                    .as("a %s* thread while the stream opener is open", THREAD_PREFIX)
                    .isTrue();
        }

        assertThat(awaitNoGroupThread())
                .as("no %s* thread once the stream opener closed", THREAD_PREFIX)
                .isTrue();
    }

    private static EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse(grpcEndpoint(), "emulatorEndpoint");
    }

    private static CreateReadSessionRequest request() {
        return CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + PROJECT)
                .setReadSession(
                        ReadSession.newBuilder()
                                .setTable(destination(TABLE).toTablePath())
                                .setDataFormat(DataFormat.AVRO)
                                .build())
                .build();
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
