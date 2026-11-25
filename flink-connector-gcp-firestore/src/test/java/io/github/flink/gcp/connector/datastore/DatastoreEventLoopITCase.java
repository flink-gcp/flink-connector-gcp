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

package io.github.flink.gcp.connector.datastore;

import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.datastore.sink.writer.DatastoreDatabaseAccess;
import io.github.flink.gcp.connector.datastore.sink.writer.DefaultDatastoreDatabaseAccessFactory;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ClientQueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlan;
import io.github.flink.gcp.connector.datastore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Datastore-mode clients run on the connector's own event-loop group and stop it when they
 * close (docs/adr/0208, #2329), and reach an emulator whose endpoint carries a scheme.
 *
 * <p>Each test runs against the emulator's {@code host:port}, against the same endpoint written as
 * {@code dns:///host:port}, which the client library's {@code Datastore} could not read, and as
 * {@code http://host:port}, which it could and which gRPC alone cannot. The class starts an
 * emulator of its own and opens no client outside the connector's, so a {@code
 * grpc-default-worker-ELG} thread, which gRPC's shared group runs, can only come from a connector
 * client that bypassed the group.
 */
@Testcontainers
@Timeout(300)
class DatastoreEventLoopITCase {

    /** gRPC's shared worker group's thread-name prefix. */
    private static final String SHARED_GROUP_PREFIX = "grpc-default-worker-ELG";

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, and gRPC destroys its
     * shared group a second after its last channel closes.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("it-project");

    @Container
    static final FirestoreEmulatorContainer EMULATOR =
            FirestoreEmulatorContainers.newContainer().withFlags("--database-mode=datastore-mode");

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(EventLoopObserver.awaitNoGroup(GrpcEventLoops::isRunning, LEFTOVER_TEARDOWN))
                .as(
                        "no lease and no %s* thread before the test: an earlier class in this fork"
                                + " left the group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(awaitNoSharedGroupThread(LEFTOVER_TEARDOWN))
                .as(
                        "no %s* thread before the test: an earlier class in this fork left a"
                                + " channel open on gRPC's shared group",
                        SHARED_GROUP_PREFIX)
                .isTrue();
    }

    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void theSinksClientRunsOnTheGroupAndStopsItOnClose(String prefix) throws Exception {
        String kind = TestNames.unique("K");
        DatastoreDatabaseAccess access =
                new DefaultDatastoreDatabaseAccessFactory(
                                DATABASE, Duration.ofSeconds(30), endpoint(prefix), null)
                        .create();
        try {
            assertThat(access.lookup(Key.newBuilder(DATABASE.getProject(), kind, "a").build()))
                    .isNull();
            List<Key> allocated =
                    access.allocateIds(
                            List.of(IncompleteKey.newBuilder(DATABASE.getProject(), kind).build()));
            assertThat(allocated)
                    .singleElement()
                    .satisfies(key -> assertThat(key.hasId()).isTrue());

            assertOnTheGroup("the sink's client");
        } finally {
            access.close();
        }

        assertGroupStopped("the sink's client");
    }

    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void thePageReadersClientRunsOnTheGroupAndStopsItOnClose(String prefix) throws Exception {
        ClientQueryPageReader reader = new ClientQueryPageReader(endpoint(prefix));
        try {
            QueryResultBatch batch = reader.read(DATABASE, request(TestNames.unique("K")));
            assertThat(batch.getEntityResultsList()).isEmpty();

            assertOnTheGroup("the page reader's client");
        } finally {
            reader.close();
        }

        assertGroupStopped("the page reader's client");
    }

    /**
     * The planner runs on the JobManager, so its client closes when the plan returns, before the
     * enumerator that owns the planner closes it. The plan must show a group thread while a lease
     * is open, and hold no lease the moment it returns ({@link EventLoopObserver}).
     */
    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void thePlannerClosesItsClientWhenThePlanReturns(String prefix) throws Exception {
        DatastoreSourceConfig<String> config =
                ((DatastoreBatchSource<String>)
                                DatastoreSource.<String>builder()
                                        .database(DATABASE)
                                        .deserializer(new TestSources.KeyNameDeserializer())
                                        .emulatorEndpoint(target(prefix))
                                        .kind(TestNames.unique("K"))
                                        .splitCount(1)
                                        .build())
                        .getConfig();
        AtomicReference<QueryPlan> planned = new AtomicReference<>();
        AtomicBoolean heldAtReturn = new AtomicBoolean();
        try (ClientQueryPlanner planner = new ClientQueryPlanner(endpoint(prefix))) {
            assertThat(
                            EventLoopObserver.sawGroupDuring(
                                    GrpcEventLoops::isRunning,
                                    () -> {
                                        QueryPlan plan = planner.plan(config, 1);
                                        heldAtReturn.set(GrpcEventLoops.isRunning());
                                        planned.set(plan);
                                    }))
                    .as("a %s* thread while the plan ran", GrpcEventLoopThreads.PREFIX)
                    .isTrue();

            assertThat(planned.get().getRequests()).hasSize(1);
            // Nothing else holds a lease, so the plan's own close of its client stopped the group,
            // before the planner closed.
            assertThat(heldAtReturn.get())
                    .as("the planner's client was still holding the group when the plan returned")
                    .isFalse();
            assertGroupStopped("the planner's client, once the plan returned");
        }
    }

    private static RunQueryRequest request(String kind) {
        PartitionId partition =
                PartitionId.newBuilder()
                        .setProjectId(DATABASE.getProject())
                        .setDatabaseId(DATABASE.getDatabaseId())
                        .build();
        return RunQueryRequest.newBuilder()
                .setProjectId(DATABASE.getProject())
                .setDatabaseId(DATABASE.getDatabaseId())
                .setPartitionId(partition)
                .setQuery(SplittableQueries.ofKind(kind))
                .build();
    }

    private static void assertOnTheGroup(String client) {
        assertThat(GrpcEventLoopThreads.alive())
                .as("a %s* thread while %s is open", GrpcEventLoopThreads.PREFIX, client)
                .isTrue();
        assertThat(sharedGroupThreadAlive())
                .as("a %s* thread while %s is open", SHARED_GROUP_PREFIX, client)
                .isFalse();
    }

    private static void assertGroupStopped(String client) throws InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once %s closed", GrpcEventLoopThreads.PREFIX, client)
                .isTrue();
        assertThat(sharedGroupThreadAlive())
                .as("a %s* thread after %s closed", SHARED_GROUP_PREFIX, client)
                .isFalse();
    }

    private static String target(String prefix) {
        return prefix + EMULATOR.getEmulatorEndpoint();
    }

    private static EmulatorEndpoint endpoint(String prefix) {
        return EmulatorEndpoint.parse(target(prefix), "emulatorEndpoint");
    }

    private static boolean sharedGroupThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().startsWith(SHARED_GROUP_PREFIX));
    }

    private static boolean awaitNoSharedGroupThread(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (sharedGroupThreadAlive()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(50);
        }
        return true;
    }
}
