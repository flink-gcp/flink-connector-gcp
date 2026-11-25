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
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    void aCloseWhileTheClientIsBeingBuiltWaitsForTheBuildRatherThanCancellingIt() throws Exception {
        // Before this change the build ran in the call's context, so the close cancelled the
        // client's emulator check, which the client rethrows without closing the stubs it has
        // already built (#2306). The held check stays inside the client's own 10 s timeout for it.
        try (StallingSpannerServer server = StallingSpannerServer.startHoldingTheEmulatorCheck()) {
            BatchClientPartitionPlanner planner =
                    new BatchClientPartitionPlanner(DATABASE, server.endpoint());
            AtomicReference<Throwable> planFailure = new AtomicReference<>();
            Thread planning = planningThread(planner, planFailure);
            AtomicBoolean checkAnswered = new AtomicBoolean();
            AtomicReference<Boolean> answeredWhenCloseReturned = new AtomicReference<>();
            AtomicReference<Boolean> leaseHeldWhenCloseReturned = new AtomicReference<>();
            Thread closing =
                    new Thread(
                            () -> {
                                planner.close();
                                answeredWhenCloseReturned.set(checkAnswered.get());
                                leaseHeldWhenCloseReturned.set(GrpcEventLoops.isRunning());
                            },
                            "close");
            try {
                planning.start();
                assertThat(server.awaitEmulatorCheck(30))
                        .as("the client's build reached its emulator check")
                        .isTrue();
                closing.start();
                // A cancellation, had the close made one, has reached the check by the time the
                // close parks: the context runs the call's cancellation on the cancelling thread.
                assertThat(awaitParkedOrEnded(closing))
                        .as("the close parked or ended")
                        .isIn(Thread.State.WAITING, Thread.State.TERMINATED);
            } finally {
                checkAnswered.set(true);
                server.answerEmulatorCheck();
                closing.join(TimeUnit.SECONDS.toMillis(20));
                planning.join(TimeUnit.SECONDS.toMillis(20));
            }

            assertThat(closing.isAlive()).as("the close ended").isFalse();
            assertThat(planning.isAlive()).as("the plan ended").isFalse();
            // The discriminator: a cancelled check fails the build instead.
            assertThat(planFailure.get())
                    .as("the plan was refused once the client was built, not cancelled")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("was closed before it was used");
            assertThat(answeredWhenCloseReturned.get())
                    .as("the check had been answered when the close returned")
                    .isTrue();
            assertThat(leaseHeldWhenCloseReturned.get())
                    .as("the built client's lease was released before the close returned")
                    .isFalse();
            assertThat(server.calls()).as("no call was started on the client").isEmpty();
        }
    }

    @Test
    @Timeout(60)
    void anInterruptEndsACloseWaitingForTheClientsBuild() throws Exception {
        // Flink interrupts a closing coordinator that overruns its close timeout.
        try (StallingSpannerServer server = StallingSpannerServer.startHoldingTheEmulatorCheck()) {
            BatchClientPartitionPlanner planner =
                    new BatchClientPartitionPlanner(DATABASE, server.endpoint());
            AtomicReference<Throwable> planFailure = new AtomicReference<>();
            Thread planning = planningThread(planner, planFailure);
            AtomicBoolean checkAnswered = new AtomicBoolean();
            AtomicReference<Boolean> answeredWhenCloseReturned = new AtomicReference<>();
            AtomicReference<Boolean> interruptedWhenCloseReturned = new AtomicReference<>();
            Thread closing =
                    new Thread(
                            () -> {
                                planner.close();
                                answeredWhenCloseReturned.set(checkAnswered.get());
                                interruptedWhenCloseReturned.set(
                                        Thread.currentThread().isInterrupted());
                            },
                            "close");
            try {
                planning.start();
                assertThat(server.awaitEmulatorCheck(30)).isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the close waits for the build")
                        .isEqualTo(Thread.State.WAITING);
                closing.interrupt();
                closing.join(TimeUnit.SECONDS.toMillis(5));
                assertThat(closing.isAlive()).as("the interrupt ended the close").isFalse();
            } finally {
                checkAnswered.set(true);
                server.answerEmulatorCheck();
                closing.join(TimeUnit.SECONDS.toMillis(20));
                planning.join(TimeUnit.SECONDS.toMillis(20));
            }

            assertThat(answeredWhenCloseReturned.get())
                    .as("the close returned before the build ended")
                    .isFalse();
            assertThat(interruptedWhenCloseReturned.get()).as("the interrupt is restored").isTrue();
            assertThat(planning.isAlive()).as("the plan ended").isFalse();
            assertThat(planFailure.get())
                    .as("the client built after the close was closed unused")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("was closed before it was used");
        }
    }

    @Test
    @Timeout(60)
    void aLateClientsCloseFailureIsLogged() throws Exception {
        // The closed batch enumerator drops what the plan throws. The client's close fails on an
        // interrupt the build's uninterruptible wait for the emulator check left set.
        try (StallingSpannerServer server = StallingSpannerServer.startHoldingTheEmulatorCheck();
                LogCapture capture = LogCapture.of(BatchClientPartitionPlanner.class)) {
            BatchClientPartitionPlanner planner =
                    new BatchClientPartitionPlanner(DATABASE, server.endpoint());
            AtomicReference<Throwable> planFailure = new AtomicReference<>();
            Thread planning = planningThread(planner, planFailure);
            Thread closing = new Thread(planner::close, "close");
            try {
                planning.start();
                assertThat(server.awaitEmulatorCheck(30)).isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the close waits for the build")
                        .isEqualTo(Thread.State.WAITING);
                planning.interrupt();
            } finally {
                server.answerEmulatorCheck();
                closing.join(TimeUnit.SECONDS.toMillis(20));
                planning.join(TimeUnit.SECONDS.toMillis(20));
            }

            assertThat(planning.isAlive()).as("the plan ended").isFalse();
            assertThat(closing.isAlive()).as("the close ended").isFalse();
            assertThat(planFailure.get())
                    .as("the plan was refused once the client was built")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("was closed before it was used");
            assertThat(capture.getEvents())
                    .as("the late client's close failure is logged")
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("WARN ");
                                assertThat(event.getMessage()).contains(DATABASE.toString());
                                assertThat(event.getThrowable()).isNotNull();
                            });
        }
    }

    /** A thread that plans one query on the planner and records how it failed. */
    private static Thread planningThread(
            BatchClientPartitionPlanner planner, AtomicReference<Throwable> failure) {
        return new Thread(
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
    }

    /**
     * Waits until a thread parks waiting, is blocked on a monitor, or has ended, and says which.
     */
    private static Thread.State awaitParkedOrEnded(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING
                    || state == Thread.State.BLOCKED
                    || state == Thread.State.TERMINATED) {
                return state;
            }
            assertThat(System.nanoTime())
                    .as("%s parked or ended in time", thread.getName())
                    .isLessThan(deadline);
            Thread.onSpinWait();
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
