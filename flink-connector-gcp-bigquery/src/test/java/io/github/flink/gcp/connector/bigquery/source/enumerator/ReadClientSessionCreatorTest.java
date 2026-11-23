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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sentence a read of a view earns.
 *
 * <p>What BigQuery answers is measured rather than assumed: on 2026-08-10 a logical view and a
 * materialized view both answered {@code CreateReadSession} with {@code INVALID_ARGUMENT: request
 * failed: non-table entities cannot be read with the storage API}. The cases below are the same
 * failure with each half of that changed, because the hint has to stay off every other {@code
 * INVALID_ARGUMENT} the same call produces — a bad projection, an unparsable row restriction, a
 * snapshot outside the time-travel window.
 */
class ReadClientSessionCreatorTest {

    private static final String VIEW_MESSAGE =
            "request failed: non-table entities cannot be read with the storage API";

    @Test
    void namesTheQueryKnobWhenBigQueryRefusedANonTable() {
        String hint = ReadClientSessionCreator.viewHint(invalidArgument(VIEW_MESSAGE));

        assertThat(hint).contains("query(...)").contains("view");
    }

    @Test
    void recognizesAViewRefusalWhetherBareOrInsideTheHintCreateThrows() {
        ApiException refusal = invalidArgument(VIEW_MESSAGE);

        assertThat(ReadClientSessionCreator.refusedAsView(refusal)).isTrue();
        assertThat(
                        ReadClientSessionCreator.refusedAsView(
                                new IOException(
                                        ReadClientSessionCreator.viewHint(refusal), refusal)))
                .isTrue();
        assertThat(
                        ReadClientSessionCreator.refusedAsView(
                                new IOException(invalidArgument("Invalid row restriction"))))
                .isFalse();
        assertThat(ReadClientSessionCreator.refusedAsView(new IOException(VIEW_MESSAGE))).isFalse();
    }

    @Test
    void staysOffAnInvalidArgumentThatIsNotAboutANonTable() {
        assertThat(ReadClientSessionCreator.viewHint(invalidArgument("Invalid row restriction")))
                .isNull();
    }

    @Test
    void staysOffAFailureWithAnotherStatusCode() {
        // PERMISSION_DENIED is what a missing table can answer with (ADR-0030), and it is not a
        // view. Keying on the message alone would claim it was one.
        assertThat(
                        ReadClientSessionCreator.viewHint(
                                ApiExceptionFactory.createException(
                                        new IllegalStateException(VIEW_MESSAGE),
                                        GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                                        false)))
                .isNull();
    }

    @Test
    void staysOffAFailureCarryingNoMessageAtAll() {
        // gax promises no message text; keying the hint on it must survive one that is absent.
        assertThat(
                        ReadClientSessionCreator.viewHint(
                                ApiExceptionFactory.createException(
                                        null,
                                        GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                                        false)))
                .isNull();
    }

    @Test
    void refusesToCreateAfterItWasClosed() {
        // The race close() guards against: a cancellation lands while the coordinator worker is
        // still planning. A creator that built a client here would leak it in the JobManager,
        // because the close that should release it has already run.
        //
        // The passing test does no I/O: the guard throws before any client exists. With the
        // guard gone, a real client is built against localhost:1 and the test fails only after
        // gax's ~10-minute retry budget (599.7 s, measured under exactly that mutation) —
        // deliberately not decorated with @Timeout, which was measured against the same
        // mutation: the same-thread interrupt fires at the deadline, gax's retry loop does not
        // abort on it, and the wall clock does not move.
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));
        creator.close();

        assertThatThrownBy(() -> creator.create(CreateReadSessionRequest.getDefaultInstance()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed");
    }

    private static ApiException invalidArgument(String message) {
        return ApiExceptionFactory.createException(
                new IllegalStateException(message),
                GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                false);
    }

    /**
     * The <em>factory</em> travels; the creator does not, and must not.
     *
     * <p>A creator that could be serialized could be parked on the source configuration, which is
     * how one creator came to be shared by every enumerator of a job and refused after the first
     * teardown ({@code docs/adr/0128}). The second assertion is the one that would notice that
     * coming back.
     */
    /**
     * A creator closed while its session is still being created cancels the call rather than
     * waiting for it, and leaves no event-loop thread behind (#2237). On the JobManager the close
     * runs after the job's class loader has been released, so waiting out the call there would tear
     * the group down on a closed loader.
     *
     * <p>The endpoint is a gRPC server that accepts the call and never answers it, so a close that
     * waited for the call would wait out its own bound instead of cancelling.
     */
    @Test
    @Timeout(60)
    void closingDuringCreationCancelsTheCallAndLeavesNoGroup() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicLong cancelledAt = new AtomicLong();
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        Server silent =
                NettyServerBuilder.forAddress(new InetSocketAddress(loopback, 0))
                        .addService(
                                new BigQueryReadGrpc.BigQueryReadImplBase() {
                                    @Override
                                    public void createReadSession(
                                            CreateReadSessionRequest request,
                                            StreamObserver<ReadSession> response) {
                                        // Never answered; the time the client gives up on it
                                        // is the measurement.
                                        ((ServerCallStreamObserver<ReadSession>) response)
                                                .setOnCancelHandler(
                                                        () -> {
                                                            cancelledAt.set(System.nanoTime());
                                                            cancelled.countDown();
                                                        });
                                        received.countDown();
                                    }
                                })
                        .build()
                        .start();
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(
                        EmulatorEndpoint.parse(
                                "127.0.0.1:" + silent.getPort(), "emulatorEndpoint"));
        AtomicLong failedAt = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                creator.create(CreateReadSessionRequest.getDefaultInstance());
                            } catch (Throwable t) {
                                failedAt.set(System.nanoTime());
                                failure.set(t);
                            }
                        },
                        "session-creation");
        try {
            worker.start();
            assertThat(received.await(30, TimeUnit.SECONDS))
                    .as("the call never reached the server")
                    .isTrue();
            assertThat(groupThreads()).as("the call did not run on the group").isNotEmpty();

            long closing = System.nanoTime();
            creator.close();
            worker.join(TimeUnit.SECONDS.toMillis(30));

            assertThat(worker.isAlive()).as("the creation outlived the close").isFalse();
            // An IOException that says so, not the CancellationException the future's get()
            // throws, which create() does not declare (#2282).
            assertThat(failure.get())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("was closed while a call was in flight")
                    .hasCauseInstanceOf(CancellationException.class);
            // When the server saw the call cancelled, not when the teardown after it finished: a
            // close that waited for the call would leave it open until the client gave up on it.
            assertThat(cancelled.await(30, TimeUnit.SECONDS))
                    .as("the server never saw the call cancelled")
                    .isTrue();
            assertThat(Duration.ofNanos(cancelledAt.get() - closing))
                    .as("close waited for the call instead of cancelling it")
                    .isLessThan(Duration.ofSeconds(4));
            assertThat(awaitNoGroupThread()).as("a group thread outlived the close").isTrue();
        } finally {
            creator.close();
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(30));
            silent.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * The same, while gax keeps retrying a call the server refuses: closing the creator and then
     * interrupting its thread, as Flink does, ends the creation rather than leaving it to wait out
     * the call's ten-minute retry budget or a retry nothing will run.
     */
    @Test
    @Timeout(60)
    void closingWhileARetryIsScheduledCancelsIt() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Server refusing =
                NettyServerBuilder.forAddress(
                                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
                        .addService(
                                new BigQueryReadGrpc.BigQueryReadImplBase() {
                                    @Override
                                    public void createReadSession(
                                            CreateReadSessionRequest request,
                                            StreamObserver<ReadSession> response) {
                                        attempts.incrementAndGet();
                                        response.onError(Status.UNAVAILABLE.asRuntimeException());
                                    }
                                })
                        .build()
                        .start();
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(
                        EmulatorEndpoint.parse(
                                "127.0.0.1:" + refusing.getPort(), "emulatorEndpoint"));
        AtomicLong failedAt = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                creator.create(CreateReadSessionRequest.getDefaultInstance());
                            } catch (Throwable t) {
                                failedAt.set(System.nanoTime());
                                failure.set(t);
                            }
                        },
                        "session-creation");
        try {
            worker.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (attempts.get() < 3 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(attempts.get()).as("gax never retried the refused call").isGreaterThan(2);
            assertThat(worker.isAlive()).as("the creation ended before the close").isTrue();

            creator.close();
            // What Flink does next when it closes the source; a cancellation that missed a retry
            // gax was starting leaves the creation to this.
            long interrupting = System.nanoTime();
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(30));

            assertThat(worker.isAlive()).as("the creation outlived the close").isFalse();
            assertThat(failure.get()).isNotNull();
            // Measured from the interrupt, after the close has returned, so the close's own
            // bounded waits are not charged to the creation; an ended one makes this negative.
            assertThat(Duration.ofNanos(failedAt.get() - interrupting))
                    .as("the creation ignored the interrupt")
                    .isLessThan(Duration.ofSeconds(20));
            assertThat(awaitNoGroupThread()).as("a group thread outlived the close").isTrue();
        } finally {
            creator.close();
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(30));
            refusing.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static boolean awaitNoGroupThread() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!groupThreads().isEmpty()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    private static List<Thread> groupThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().startsWith("flink-gcp-grpc-elg"))
                .collect(Collectors.toList());
    }

    /**
     * Flink interrupts the coordinator's worker when it closes the source; a creation waiting on
     * its call ends then, keeps the interrupt, and releases its client and lease, even if nothing
     * cancelled the call.
     */
    @Test
    @Timeout(60)
    void anInterruptEndsTheWaitAndReleasesTheClient() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        Server silent =
                NettyServerBuilder.forAddress(
                                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
                        .addService(
                                new BigQueryReadGrpc.BigQueryReadImplBase() {
                                    @Override
                                    public void createReadSession(
                                            CreateReadSessionRequest request,
                                            StreamObserver<ReadSession> response) {
                                        received.countDown();
                                    }
                                })
                        .build()
                        .start();
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(
                        EmulatorEndpoint.parse(
                                "127.0.0.1:" + silent.getPort(), "emulatorEndpoint"));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean stillInterrupted = new AtomicBoolean();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                creator.create(CreateReadSessionRequest.getDefaultInstance());
                            } catch (Throwable t) {
                                stillInterrupted.set(Thread.currentThread().isInterrupted());
                                failure.set(t);
                            }
                        },
                        "session-creation");
        try {
            worker.start();
            assertThat(received.await(30, TimeUnit.SECONDS))
                    .as("the call never reached the server")
                    .isTrue();

            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(20));

            assertThat(worker.isAlive()).as("the interrupt did not end the wait").isFalse();
            assertThat(failure.get()).isInstanceOf(InterruptedIOException.class);
            assertThat(stillInterrupted).as("the interrupt was swallowed").isTrue();
            assertThat(GrpcEventLoops.isRunning()).as("the lease outlived the creation").isFalse();
        } finally {
            creator.close();
            worker.join(TimeUnit.SECONDS.toMillis(30));
            silent.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** A call the SDK refuses before it starts still releases the client and its lease. */
    @Test
    void aCallRefusedBeforeItStartsReleasesTheClient() {
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        assertThatThrownBy(() -> creator.create(null)).isInstanceOf(NullPointerException.class);
        assertThat(GrpcEventLoops.isRunning()).as("the refused call's lease was kept").isFalse();
        creator.close();
    }

    @Test
    void theFactoryTravelsInTheJobGraphAndTheCreatorDoesNot() throws Exception {
        DefaultReadSessionCreatorFactory factory =
                new DefaultReadSessionCreatorFactory(
                        null, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        DefaultReadSessionCreatorFactory back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        assertThat(back.create()).isInstanceOf(ReadClientSessionCreator.class);
        assertThat(java.io.Serializable.class.isAssignableFrom(ReadSessionCreator.class))
                .as("a serializable creator could be parked on the configuration again")
                .isFalse();
    }
}
