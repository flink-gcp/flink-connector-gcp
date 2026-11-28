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
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Firestore;
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
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Firestore Native clients run on the connector's own event-loop group and stop it when they
 * close (docs/adr/0208, #2330), and reach an emulator whose endpoint carries a scheme, or that
 * {@code FIRESTORE_EMULATOR_HOST} names, on that group too (#2343).
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
        // FirestoreClients reads the variable when no emulator is configured, and a client then
        // dials what it names; the test that models it passes the value itself.
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

    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void thePageReadersClientRunsOnTheGroupAndStopsItOnClose(String prefix) throws Exception {
        ClientQueryPageReader reader = new ClientQueryPageReader(endpoint(prefix));
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
     * enumerator that owns the planner closes it. The plan must start one of the group's event
     * loops while the group is running, and hold no lease the moment it returns ({@link
     * EventLoopObserver}).
     */
    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void thePlannerClosesItsClientWhenThePlanReturns(String prefix) throws Exception {
        FirestoreSourceConfig<String> config =
                TestSources.source(
                                builder ->
                                        builder.collectionGroup(uniqueCollection())
                                                .partitionCount(1)
                                                .emulatorEndpoint(prefix + target()))
                        .getConfig();
        try (ClientQueryPlanner planner = new ClientQueryPlanner(endpoint(prefix))) {
            QueryPlan plan = planOnTheGroup(planner, config);

            assertThat(plan.getQueries()).hasSize(1);
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
            QueryPlan plan = planOnTheGroup(planner, config);

            assertThat(plan.getQueries())
                    .singleElement()
                    .satisfies(
                            query -> assertThat(query.getStructuredQuery().getOffset()).isZero());
            assertGroupStopped("the planner's client, once the plan returned");
        }
    }

    /**
     * Without a configured emulator, a client reaches the emulator {@code FIRESTORE_EMULATOR_HOST}
     * names, as the client library would send it there, but on the connector's group, whatever
     * spelling of the endpoint gRPC accepts.
     */
    @ParameterizedTest(name = "variable prefix \"{0}\"")
    @ValueSource(strings = {"", "dns:///", "http://"})
    void aClientOfTheEmulatorTheVariableNamesRunsOnTheGroup(String prefix) throws Exception {
        String variable = prefix + target();
        GrpcEventLoops.LeasedClient<Firestore> client =
                FirestoreClients.open(
                        TestSources.DATABASE,
                        null,
                        // A stand-in token rather than Application Default Credentials, so a client
                        // that missed the variable could reach no project with real credentials.
                        FirestoreClients.settings(
                                TestSources.DATABASE,
                                null,
                                GoogleCredentials.create(new AccessToken("t", null)),
                                null),
                        name ->
                                name.equals(FirestoreClients.EMULATOR_HOST_VARIABLE)
                                        ? variable
                                        : null);
        try {
            assertThat(
                            client.client()
                                    .collection(uniqueCollection())
                                    .document("absent")
                                    .get()
                                    .get(30, TimeUnit.SECONDS)
                                    .exists())
                    .isFalse();

            assertOnTheGroup("the client of the variable's emulator");
        } finally {
            client.close();
        }

        assertGroupStopped("the client of the variable's emulator");
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

    /**
     * Plans the source, with a default partition count of one, and checks that the plan started one
     * of the group's event loops while the group was running and that its client no longer held a
     * lease when it returned. Nothing else holds a lease, so a lease still open at that moment is
     * the plan's own.
     */
    private static QueryPlan planOnTheGroup(
            ClientQueryPlanner planner, FirestoreSourceConfig<String> config) throws Exception {
        AtomicReference<QueryPlan> planned = new AtomicReference<>();
        AtomicBoolean heldAtReturn = new AtomicBoolean();
        assertThat(
                        EventLoopObserver.sawGroupDuring(
                                GrpcEventLoops::isRunning,
                                GrpcEventLoops::loopsStartedWhileRunning,
                                () -> {
                                    QueryPlan plan = planner.plan(config, 1);
                                    heldAtReturn.set(GrpcEventLoops.isRunning());
                                    planned.set(plan);
                                }))
                .as("a group event loop started while the plan ran")
                .isTrue();
        assertThat(heldAtReturn.get())
                .as("the planner's client was still holding the group when the plan returned")
                .isFalse();
        return planned.get();
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
        return endpoint("");
    }

    private static EmulatorEndpoint endpoint(String prefix) {
        return EmulatorEndpoint.parse(prefix + target(), "emulatorEndpoint");
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
