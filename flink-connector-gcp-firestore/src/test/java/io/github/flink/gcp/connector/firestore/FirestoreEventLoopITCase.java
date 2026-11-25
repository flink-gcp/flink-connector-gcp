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

package io.github.flink.gcp.connector.firestore;

import com.google.api.core.ApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.sink.writer.DefaultFirestoreDatabaseAccessFactory;
import io.github.flink.gcp.connector.firestore.sink.writer.FirestoreDatabaseAccess;
import io.github.flink.gcp.connector.firestore.sink.writer.TestRetryPolicies;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.ClientQueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlan;
import io.github.flink.gcp.connector.firestore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.firestore.table.source.TestDocumentLookups;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Firestore Native clients run on the connector's own event-loop group and stop it when they
 * close (docs/adr/0208, #2330).
 *
 * <p>The class starts an emulator of its own and opens no client outside the connector's, so a
 * thread of gRPC's shared group, such as {@code grpc-default-worker-ELG}, can only come from a
 * connector client that bypassed the group, as the library's own {@code setEmulatorHost} does.
 */
@Testcontainers
@Timeout(300)
class FirestoreEventLoopITCase {

    /**
     * The thread-name prefixes of gRPC's shared worker group: with the epoll transport, and with
     * NIO where epoll does not load.
     */
    private static final List<String> SHARED_GROUP_PREFIXES =
            List.of("grpc-default-worker-ELG", "grpc-nio-worker-ELG");

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, and gRPC destroys its
     * shared group a second after its last channel closes.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    @Container
    static final FirestoreEmulatorContainer EMULATOR = FirestoreEmulatorContainers.newContainer();

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        // The library reads the variable whenever no emulator host was set, and a client it
        // builds from it runs off the group (FirestoreClients).
        assertThat(System.getenv("FIRESTORE_EMULATOR_HOST"))
                .as("FIRESTORE_EMULATOR_HOST is set; unset it to run this test")
                .isNull();
        assertThat(GrpcEventLoopThreads.awaitNone(LEFTOVER_TEARDOWN))
                .as(
                        "no %s* thread before the test: an earlier class in this fork left the"
                                + " group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(awaitNoSharedGroupThread(LEFTOVER_TEARDOWN))
                .as(
                        "no %s thread before the test: an earlier class in this fork left a"
                                + " channel open on gRPC's shared group",
                        SHARED_GROUP_PREFIXES)
                .isTrue();
        assertThat(GrpcEventLoops.isRunning())
                .as("a lease open before the test: an earlier class in this fork left it open")
                .isFalse();
    }

    @Test
    void theSinksClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        String collection = uniqueCollection();
        FirestoreDatabaseAccess access =
                new DefaultFirestoreDatabaseAccessFactory(
                                TestSources.DATABASE,
                                FirestoreWriterOptions.builder().build(),
                                endpoint(),
                                null)
                        .create(TestRetryPolicies.singleAttempt());
        try {
            ApiFuture<WriteResult> written =
                    access.submit(FirestoreWrite.set(collection + "/a", Map.of("n", 1L)));
            access.sendOutstanding();
            // The write's result arrives once its batch was sent and answered.
            written.get(30, TimeUnit.SECONDS);

            assertOnTheGroup("the sink's client");
        } finally {
            access.close();
        }

        assertGroupStopped("the sink's client");
    }

    @Test
    void thePageReadersClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        ClientQueryPageReader reader = new ClientQueryPageReader(endpoint());
        try {
            Query page = reader.query(TestSources.DATABASE, scan(uniqueCollection()));
            // A second back, so the emulator, on this machine's clock, never sees a future time.
            Timestamp readTime =
                    Timestamp.ofTimeMicroseconds(
                            TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - 1000));
            assertThat(reader.read(page, readTime)).isEmpty();

            assertOnTheGroup("the page reader's client");
        } finally {
            reader.close();
        }

        assertGroupStopped("the page reader's client");
    }

    @Test
    void theLookupsClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        TestDocumentLookups.OpenLookup lookup =
                TestDocumentLookups.open(TestSources.DATABASE, uniqueCollection(), target());
        try {
            assertThat(lookup.read("absent").exists()).isFalse();

            assertOnTheGroup("the lookup's client");
        } finally {
            lookup.close();
        }

        assertGroupStopped("the lookup's client");
    }

    /**
     * The planner runs on the JobManager, so its client closes when the plan returns, before the
     * enumerator that owns the planner closes it.
     */
    @Test
    void thePlannerClosesItsClientWhenThePlanReturns() throws Exception {
        FirestoreSourceConfig<String> config =
                TestSources.source(
                                builder ->
                                        builder.collectionGroup(uniqueCollection())
                                                .partitionCount(1)
                                                .emulatorEndpoint(target()))
                        .getConfig();
        try (ClientQueryPlanner planner = new ClientQueryPlanner(endpoint())) {
            QueryPlan plan = planner.plan(config, 1);

            assertThat(plan.getQueries()).hasSize(1);
            // Nothing else holds a lease, so the plan's own close of its client stopped the group,
            // before the planner closed.
            assertGroupStopped("the planner's client, once the plan returned");
        }
    }

    /** A query with an offset takes one more call, which resolves it into a cursor. */
    @Test
    void aPlanThatResolvesAnOffsetClosesItsClientWhenItReturns() throws Exception {
        String collection = uniqueCollection();
        FirestoreSourceConfig<String> config =
                TestSources.source(
                                builder ->
                                        builder.query(
                                                        firestore ->
                                                                firestore
                                                                        .collection(collection)
                                                                        .offset(2))
                                                .emulatorEndpoint(target()))
                        .getConfig();
        try (ClientQueryPlanner planner = new ClientQueryPlanner(endpoint())) {
            QueryPlan plan = planner.plan(config, 1);

            assertThat(plan.getQueries())
                    .singleElement()
                    .satisfies(
                            query -> assertThat(query.getStructuredQuery().getOffset()).isZero());
            assertGroupStopped("the planner's client, once the plan returned");
        }
    }

    private static com.google.firestore.v1.RunQueryRequest scan(String collection) {
        return com.google.firestore.v1.RunQueryRequest.newBuilder()
                .setParent(
                        "projects/"
                                + TestSources.DATABASE.getProject()
                                + "/databases/"
                                + TestSources.DATABASE.getDatabaseId()
                                + "/documents")
                .setStructuredQuery(
                        com.google.firestore.v1.StructuredQuery.newBuilder()
                                .addFrom(
                                        com.google.firestore.v1.StructuredQuery.CollectionSelector
                                                .newBuilder()
                                                .setCollectionId(collection)))
                .build();
    }

    private static String uniqueCollection() {
        return TestNames.unique("c").toLowerCase(Locale.ROOT);
    }

    private static void assertOnTheGroup(String client) {
        assertThat(GrpcEventLoopThreads.alive())
                .as("a %s* thread while %s is open", GrpcEventLoopThreads.PREFIX, client)
                .isTrue();
        assertThat(sharedGroupThreadAlive())
                .as("a %s thread while %s is open", SHARED_GROUP_PREFIXES, client)
                .isFalse();
    }

    private static void assertGroupStopped(String client) throws InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once %s closed", GrpcEventLoopThreads.PREFIX, client)
                .isTrue();
        assertThat(sharedGroupThreadAlive())
                .as("a %s thread after %s closed", SHARED_GROUP_PREFIXES, client)
                .isFalse();
    }

    private static String target() {
        return EMULATOR.getEmulatorEndpoint();
    }

    private static EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse(target(), "emulatorEndpoint");
    }

    private static boolean sharedGroupThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(
                        t ->
                                t.isAlive()
                                        && SHARED_GROUP_PREFIXES.stream()
                                                .anyMatch(t.getName()::startsWith));
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
