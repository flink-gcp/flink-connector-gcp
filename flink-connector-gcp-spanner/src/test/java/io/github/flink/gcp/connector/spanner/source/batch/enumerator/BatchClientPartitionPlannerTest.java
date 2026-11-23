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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.StallingSpannerServer;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the planner's lifecycle, which is all of it that can be driven without a service.
 *
 * <p>The endpoint is an emulator one throughout, and never connected to: without it the client
 * reaches for application default credentials, which a workstation has and a build agent does not.
 * What it costs a test to reach the service is why the planning call itself is covered by the
 * module's emulator integration tests instead.
 */
class BatchClientPartitionPlannerTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "i", "db");

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a planner left its lease open").isFalse();
    }

    @Test
    @Timeout(60)
    void aCloseDuringAPlanningCallCancelsTheCallAndReleasesTheLease() throws Exception {
        try (StallingSpannerServer server = StallingSpannerServer.start()) {
            BatchClientPartitionPlanner planner =
                    new BatchClientPartitionPlanner(DATABASE, server.endpoint());
            CompletableFuture<PartitionPlan> planning =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return planner.plan(
                                            SpannerReadOperation.query(Statement.of("SELECT 1")),
                                            TimestampBound.strong(),
                                            PartitionOptions.getDefaultInstance(),
                                            false,
                                            null);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            long started;
            try {
                assertThat(server.awaitStalledCall(30))
                        .as("the planning call reached the server")
                        .isTrue();
                assertThat(GrpcEventLoops.isRunning()).as("the call runs under a lease").isTrue();
                started = System.nanoTime();
            } finally {
                planner.close();
            }
            // close() returns once the call has ended and released its handle's lease, whichever
            // thread closed the handle.
            assertThat(GrpcEventLoops.isRunning())
                    .as("the call's lease was released before close returned")
                    .isFalse();
            // The call is cancelled rather than waited out: the client's close alone waits for its
            // channels, up to 10 s per stub, which a call that is never answered keeps open.
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
                    .as("milliseconds from close to its return; calls %s", server.calls())
                    .isLessThan(3_000);
            assertThatThrownBy(() -> planning.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .satisfies(StallingSpannerServer::assertCancelled);
        }
    }

    @Test
    @Timeout(60)
    void aPlanningFailureIsNotReplacedByTheClientsCloseFailure() throws Exception {
        // Interrupting the planning thread fails the call, and the client's close then fails on
        // the same interrupt; the planning failure, which for a query the service will not
        // partition carries the service's reason, is the one reported.
        try (StallingSpannerServer server = StallingSpannerServer.start()) {
            BatchClientPartitionPlanner planner =
                    new BatchClientPartitionPlanner(DATABASE, server.endpoint());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread planning =
                    new Thread(
                            () -> {
                                try {
                                    planner.plan(
                                            SpannerReadOperation.query(Statement.of("SELECT 1")),
                                            TimestampBound.strong(),
                                            PartitionOptions.getDefaultInstance(),
                                            false,
                                            null);
                                } catch (Throwable e) {
                                    failure.set(e);
                                }
                            },
                            "planning");
            try {
                planning.start();
                assertThat(server.awaitStalledCall(30)).isTrue();
                planning.interrupt();
                planning.join(TimeUnit.SECONDS.toMillis(20));
                assertThat(planning.isAlive()).as("the interrupt ended the plan").isFalse();
            } finally {
                planner.close();
                planning.join(TimeUnit.SECONDS.toMillis(20));
            }

            Throwable reported = failure.get();
            assertThat(reported).as("the plan failed").isNotNull();
            assertThat(closedTheClient(reported))
                    .as(
                            "the reported failure is the planning call's, not the close's: %s",
                            reported)
                    .isFalse();
            assertThat(reported.getSuppressed())
                    .as("the client's close failure rides on the planning failure: %s", reported)
                    .anyMatch(BatchClientPartitionPlannerTest::closedTheClient);
        }
    }

    /** Whether a failure was thrown from the client's own close. */
    private static boolean closedTheClient(Throwable failure) {
        for (StackTraceElement frame : failure.getStackTrace()) {
            if (frame.getClassName().equals("com.google.cloud.spanner.SpannerImpl")
                    && frame.getMethodName().equals("close")) {
                return true;
            }
        }
        return false;
    }

    @Test
    void planningAfterCloseIsRefusedByName() throws Exception {
        BatchClientPartitionPlanner planner = planner();
        planner.close();

        // Refused before any client is built, which is what keeps a completion arriving after
        // teardown from opening a client on the JobManager after the enumerator has gone.
        assertThatThrownBy(this::plan)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed before it was used");

        assertThatThrownBy(
                        () ->
                                planner.plan(
                                        SpannerReadOperation.query(Statement.of("SELECT 1")),
                                        TimestampBound.strong(),
                                        PartitionOptions.getDefaultInstance(),
                                        false,
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(DATABASE.toString());
    }

    @Test
    void closingTwiceIsHarmless() throws Exception {
        BatchClientPartitionPlanner planner = planner();

        planner.close();

        // The enumerator closes it once, but a teardown may close it again — so the second close
        // has to be a no-op rather than a failure the job is told about.
        assertThatCode(planner::close).doesNotThrowAnyException();
    }

    @Test
    void theOptionsCarryOnlyWhatWasAskedFor() {
        // Asserted by kind rather than by value, because the client library reads an option only
        // from inside its own package. What the *value* maps to is SpannerRpcPriorityTest's.
        assertThat(BatchClientPartitionPlanner.queryOptions(false, null)).isEmpty();
        assertThat(BatchClientPartitionPlanner.readOptions(false, null)).isEmpty();

        assertThat(BatchClientPartitionPlanner.queryOptions(false, SpannerRpcPriority.LOW))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());
        assertThat(BatchClientPartitionPlanner.readOptions(false, SpannerRpcPriority.LOW))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());

        assertThat(BatchClientPartitionPlanner.queryOptions(true, null))
                .singleElement()
                .matches(option -> option.getClass() == dataBoostKind());
        assertThat(BatchClientPartitionPlanner.readOptions(true, null))
                .singleElement()
                .matches(option -> option.getClass() == dataBoostKind());
    }

    @Test
    void bothOptionsTravelTogether() {
        // The two families are assembled separately because the client gives their values no
        // common supertype; asserting one family would leave the other free to lose an option.
        assertThat(BatchClientPartitionPlanner.queryOptions(true, SpannerRpcPriority.MEDIUM))
                .hasSize(2)
                .anyMatch(option -> option.getClass() == dataBoostKind())
                .anyMatch(option -> option.getClass() == priorityKind());
        assertThat(BatchClientPartitionPlanner.readOptions(true, SpannerRpcPriority.MEDIUM))
                .hasSize(2)
                .anyMatch(option -> option.getClass() == dataBoostKind())
                .anyMatch(option -> option.getClass() == priorityKind());
    }

    /**
     * The class {@code dataBoostEnabled} answers with.
     *
     * <p>Read through the public interface it implements, because the class itself is
     * package-private in the client library — calling {@code getClass()} on the expression does not
     * compile from here.
     */
    private static Class<?> dataBoostKind() {
        Options.ReadAndQueryOption option = Options.dataBoostEnabled(true);
        return option.getClass();
    }

    /** The class {@code priority} answers with. */
    private static Class<?> priorityKind() {
        return Options.priority(Options.RpcPriority.LOW).getClass();
    }

    private void plan() throws IOException {
        BatchClientPartitionPlanner planner = planner();
        planner.close();
        planner.plan(
                SpannerReadOperation.query(Statement.of("SELECT 1")),
                TimestampBound.strong(),
                PartitionOptions.getDefaultInstance(),
                false,
                null);
    }

    private static BatchClientPartitionPlanner planner() {
        return new BatchClientPartitionPlanner(
                DATABASE, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));
    }

    /**
     * The <em>factory</em> travels; the planner does not, and must not.
     *
     * <p>A planner that could be serialized could be parked on the source configuration, which is
     * how one planner came to be shared by every enumerator of a job and refused after the first
     * teardown ({@code docs/adr/0128}). The second assertion is the one that would notice that
     * coming back.
     */
    @Test
    void theFactoryTravelsInTheJobGraphAndThePlannerDoesNot() throws Exception {
        DefaultPartitionPlannerFactory factory =
                new DefaultPartitionPlannerFactory(
                        DATABASE, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        DefaultPartitionPlannerFactory back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        assertThat(back.create()).isInstanceOf(BatchClientPartitionPlanner.class);
        assertThat(java.io.Serializable.class.isAssignableFrom(PartitionPlanner.class))
                .as("a serializable planner could be parked on the configuration again")
                .isFalse();
    }
}
