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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.grpc.GcpManagedChannel;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.spanner.SpannerEmulatorContainers;
import io.github.flink.gcp.connector.testutils.spanner.SpannerTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Once the connector's Spanner client has closed, no thread of the client library's two static
 * executors stays to hold the job's class loader (docs/adr/0210, #2296).
 *
 * <p>Not an {@link AbstractSpannerEmulatorITCase}: that harness keeps a client of its own open for
 * the whole class, and an open client's periodic tasks rightly keep one thread of each executor.
 * The harness client here only creates the instance and the database and is closed before the test.
 * It does not go through {@link SpannerClients}, so its threads stay under the client library's own
 * policy until the test's {@code SpannerClients.open} applies the connector's: without that, they
 * never exit, and the last assertion fails.
 */
@Testcontainers
@Timeout(300)
class SpannerStaticExecutorsITCase {

    private static final String PROJECT = "it-project";
    private static final String INSTANCE = "it-instance";
    private static final String DATABASE = "executors";

    /** The two executors' thread-name prefixes (grpc-gcp 1.20.0, google-cloud-spanner 6.127.0). */
    private static final String GRPC_GCP_THREADS = "gcp-mc-bg-";

    private static final String MAINTAINER_THREADS = "multiplexed-session-maintainer-";

    /**
     * How long the threads get to exit: the shortened keep-alive, plus whatever a client an earlier
     * class in this reused fork is still closing needs.
     */
    private static final Duration EXIT_BUDGET = Duration.ofSeconds(90);

    @Container
    static final SpannerEmulatorContainer EMULATOR = SpannerEmulatorContainers.newContainer();

    @BeforeAll
    static void createDatabaseAndCloseTheHarnessClient() throws Exception {
        SpannerStaticExecutors.keepAliveForTesting(Duration.ofSeconds(1));
        Spanner harness =
                SpannerTestClients.forEmulator(EMULATOR.getEmulatorGrpcEndpoint(), PROJECT);
        try {
            harness.getInstanceAdminClient()
                    .createInstance(
                            InstanceInfo.newBuilder(InstanceId.of(PROJECT, INSTANCE))
                                    .setInstanceConfigId(
                                            InstanceConfigId.of(PROJECT, "emulator-config"))
                                    .setNodeCount(1)
                                    .setDisplayName("integration tests")
                                    .build())
                    .get();
            harness.getDatabaseAdminClient()
                    .createDatabase(
                            INSTANCE,
                            Dialect.GOOGLE_STANDARD_SQL.createDatabaseStatementFor(DATABASE),
                            Dialect.GOOGLE_STANDARD_SQL,
                            Collections.emptyList())
                    .get();
        } finally {
            harness.close();
        }
    }

    @AfterAll
    static void restoreTheKeepAlive() {
        SpannerStaticExecutors.keepAliveForTesting(null);
        SpannerStaticExecutors.allowIdleThreadsToTimeOut();
    }

    @Test
    void theExecutorsThreadsExitOnceTheClientHasClosed() throws Exception {
        ScheduledThreadPoolExecutor grpcGcp =
                read(GcpManagedChannel.class, "SHARED_BACKGROUND_SERVICE");
        ScheduledThreadPoolExecutor maintainer =
                read(
                        Class.forName("com.google.cloud.spanner.MultiplexedSessionDatabaseClient"),
                        "MAINTAINER_SERVICE");
        // getTaskCount() includes the queue, and the open below purges tasks earlier stock
        // clients in this fork cancelled; purge them first so the baseline does not count them.
        grpcGcp.purge();
        maintainer.purge();
        long grpcGcpTasks = grpcGcp.getTaskCount();
        long maintainerTasks = maintainer.getTaskCount();
        DatabaseDestination database = DatabaseDestination.of(PROJECT, INSTANCE, DATABASE);
        GrpcEventLoops.LeasedClient<Spanner> leased =
                SpannerClients.open(
                        database,
                        SpannerClients.settings(
                                database,
                                EmulatorEndpoint.parse(
                                        EMULATOR.getEmulatorGrpcEndpoint(), "emulatorEndpoint")));
        try {
            // This open applied the connector's policy: the shortened keep-alive is distinctive,
            // so a value an earlier client in this fork left behind cannot pass for it.
            assertThat(grpcGcp.getKeepAliveTime(TimeUnit.SECONDS))
                    .as("grpc-gcp's keep-alive after this open")
                    .isOne();
            assertThat(maintainer.getKeepAliveTime(TimeUnit.SECONDS))
                    .as("the maintainer's keep-alive after this open")
                    .isOne();
            try (ResultSet rows =
                    leased.client()
                            .getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE))
                            .singleUse()
                            .executeQuery(Statement.of("SELECT 1"))) {
                assertThat(rows.next()).isTrue();
            }
            // This client scheduled work on both executors, so the check below can fail; threads
            // an earlier class left would not show that.
            assertThat(grpcGcp.getTaskCount())
                    .as("tasks this client scheduled on grpc-gcp's executor")
                    .isGreaterThan(grpcGcpTasks);
            assertThat(maintainer.getTaskCount())
                    .as("tasks this client scheduled on the maintainer's executor")
                    .isGreaterThan(maintainerTasks);
            assertThat(threads())
                    .as("the client library's executor threads while the client is open")
                    .containsKeys(GRPC_GCP_THREADS, MAINTAINER_THREADS);
        } finally {
            leased.close();
        }

        long deadline = System.nanoTime() + EXIT_BUDGET.toNanos();
        Map<String, Integer> left = threads();
        while (!left.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(100);
            left = threads();
        }
        assertThat(left)
                .as(
                        "the client library's executor threads %s after the client closed, with"
                                + " %s and %s tasks queued (grpc-gcp's, the maintainer's); a"
                                + " client an earlier class in this fork left open would keep one",
                        EXIT_BUDGET, grpcGcp.getQueue().size(), maintainer.getQueue().size())
                .isEmpty();
    }

    private static ScheduledThreadPoolExecutor read(Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return (ScheduledThreadPoolExecutor) field.get(null);
    }

    /** Counts the live threads of the two executors by name prefix. */
    private static Map<String, Integer> threads() {
        Map<String, Integer> counts = new TreeMap<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (!thread.isAlive()) {
                continue;
            }
            for (String prefix : new String[] {GRPC_GCP_THREADS, MAINTAINER_THREADS}) {
                if (thread.getName().startsWith(prefix)) {
                    counts.merge(prefix, 1, Integer::sum);
                }
            }
        }
        return counts;
    }
}
