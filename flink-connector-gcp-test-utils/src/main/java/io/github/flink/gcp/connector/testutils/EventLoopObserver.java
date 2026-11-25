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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.ThrowingRunnable;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Observes the connectors' own gRPC event-loop group around an operation that opens its client and
 * closes it before returning, such as an enumerator's or an admin's call.
 *
 * <p>A check that only finds no group thread afterwards passes as well when the operation never
 * used the group, so {@link #sawGroupDuring(BooleanSupplier, ThrowingRunnable)} also records
 * whether a group thread was alive while a lease was open. Both halves matter: the group's threads
 * start when a channel registers, but the release of the last lease also starts any that never ran,
 * to shut them down ({@link GrpcEventLoopThreads}), so a lease opened and released around a client
 * whose channels ran elsewhere still shows threads for a moment. Those appear only after the
 * release has detached the group, when no lease is open, which is why a thread seen alive counts
 * only if a lease is still open right after.
 *
 * <p>Whether a lease is open is {@code base.rpc.GrpcEventLoops.isRunning()}, which every caller
 * passes in: base depends on this module, not the other way round. It also reads true while the
 * group is pinned to a job's class loader with no lease open, and a pinned group keeps its threads,
 * so under a pin {@link #sawGroupDuring(BooleanSupplier, ThrowingRunnable)} reads an earlier
 * channel's thread as the operation's. What rules a pin out is where these tests run: on the
 * application class path the pin is a no-op. {@link #awaitNoGroup(BooleanSupplier, Duration)} waits
 * for a pinned group's release as it does for a lease's, but a pin registered before any lease
 * opened holds no group yet, and passes it.
 */
@Internal
public final class EventLoopObserver {

    private EventLoopObserver() {}

    /**
     * Runs the operation while polling for a live group thread. The operation starts only once the
     * poller thread is running, so a fast operation does not finish before the poller was ever
     * scheduled; that a poll lands while the operation's lease is open is still a matter of timing.
     *
     * @param leaseOpen whether a lease is open: {@code GrpcEventLoops::isRunning}
     * @param operation the operation, which opens and closes its own client
     * @return whether a group thread was alive, with a lease open, at some point while it ran;
     *     meaningful only while no pin holds the group
     * @throws Exception if the operation does
     */
    public static boolean sawGroupDuring(
            BooleanSupplier leaseOpen, ThrowingRunnable<? extends Exception> operation)
            throws Exception {
        AtomicBoolean seen = new AtomicBoolean();
        AtomicBoolean done = new AtomicBoolean();
        CountDownLatch polling = new CountDownLatch(1);
        Thread poller =
                new Thread(
                        () -> {
                            polling.countDown();
                            while (!done.get() && !seen.get()) {
                                // Threads first, then the lease: a thread the shutdown started
                                // is alive only once no lease is open.
                                if (GrpcEventLoopThreads.alive() && leaseOpen.getAsBoolean()) {
                                    seen.set(true);
                                }
                            }
                        },
                        "event-loop-observer");
        poller.setDaemon(true);
        poller.start();
        try {
            uninterruptibly(polling::await);
            operation.run();
        } finally {
            done.set(true);
            uninterruptibly(poller::join);
        }
        return seen.get();
    }

    /**
     * Waits for the poller to start or to stop without letting an interrupt skip the operation or
     * replace what it threw: a client runner restores the interrupt flag before it throws, and the
     * poller starts at once and stops on its own once the operation is done. An interrupt received
     * meanwhile is restored afterwards.
     */
    private static void uninterruptibly(ThrowingRunnable<InterruptedException> wait) {
        boolean interrupted = false;
        while (true) {
            try {
                wait.run();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Waits until no lease is open and no group thread is alive, so that a group an earlier class
     * in a reused fork left behind is ruled out before a test relies on its absence. A lease whose
     * client never registered a channel starts no thread, which is why the lease is checked too. It
     * cannot rule out a group none of whose event loops ever ran, caught between its release and
     * its shutdown, which for a moment has neither ({@link GrpcEventLoopThreads}).
     *
     * @param leaseOpen whether a lease is open: {@code GrpcEventLoops::isRunning}
     * @param timeout how long to wait
     * @return whether both were gone in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNoGroup(BooleanSupplier leaseOpen, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (leaseOpen.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return GrpcEventLoopThreads.awaitNone(
                Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
    }
}
