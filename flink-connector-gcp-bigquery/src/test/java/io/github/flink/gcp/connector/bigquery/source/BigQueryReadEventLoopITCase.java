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
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Storage Read API clients — the enumerator's session creator and the reader's stream opener —
 * run on the connector's own gRPC event-loop group and stop it when they close (#2237).
 *
 * <p>See {@code BigQueryWriteEventLoopITCase} for why that is what keeps a job-jar deployment's
 * TaskManager alive under {@code cluster.uncaught-exception-handling: FAIL}. The session creator
 * runs on the JobManager, whose class loader for the job closes the same way. The two clients are
 * tested apart, so that one of them leaving the group is not hidden by the other's threads. The
 * emulator arms are the ones a test can reach; that the production session client's settings carry
 * the group is {@code BigQueryReadClientsTest}'s to show.
 */
class BigQueryReadEventLoopITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String TABLE = "event_loop";

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
     * closed loader. The creation must show a group thread while a lease is open, since finding no
     * group afterwards alone would pass for a client whose channels ran on gRPC's group too ({@link
     * EventLoopObserver}).
     */
    @Test
    void theSessionCreatorRunsOnTheGroupAndLeavesNoGroupOnceTheSessionExists() throws Exception {
        assertThat(
                        EventLoopObserver.awaitNoGroup(
                                GrpcEventLoops::isRunning, Duration.ofSeconds(10)))
                .as(
                        "no lease and no %s* thread before the creator opens",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
        try (ReadClientSessionCreator creator = new ReadClientSessionCreator(endpoint())) {
            AtomicBoolean heldAtReturn = new AtomicBoolean();
            assertThat(
                            EventLoopObserver.sawGroupDuring(
                                    GrpcEventLoops::isRunning,
                                    () -> {
                                        ReadSession session = creator.create(request());
                                        // Read the moment create() returns, not polled: a client
                                        // closed a moment later would already be too late on a
                                        // JobManager.
                                        heldAtReturn.set(GrpcEventLoops.isRunning());
                                        assertThat(session.getStreamsCount()).isPositive();
                                    }))
                    .as("a %s* thread while the session was created", GrpcEventLoopThreads.PREFIX)
                    .isTrue();

            assertThat(heldAtReturn.get())
                    .as("the session's client was still holding the group when create() returned")
                    .isFalse();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("a %s* thread outlived the session's creation", GrpcEventLoopThreads.PREFIX)
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
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();

        try (ReadClientRowStreamOpener opener =
                new ReadClientRowStreamOpener(
                        endpoint(), BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS)) {
            try (RowStream rows = opener.open(stream, 0)) {
                while (rows.next() != null) {
                    // Drained, so the stream is done before the opener closes.
                }
            }

            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread while the stream opener is open", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the stream opener closed", GrpcEventLoopThreads.PREFIX)
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
}
