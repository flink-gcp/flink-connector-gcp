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
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Observes the connectors' own gRPC event-loop group around an operation that opens its client and
 * closes it before returning, such as an enumerator's or an admin's call.
 *
 * <p>A check that only finds no group thread afterwards passes as well when the operation never
 * used the group, so {@link #sawGroupDuring(BooleanSupplier, LongSupplier, ThrowingRunnable)} also
 * asks whether a group the operation opened gave one of its event loops a first task while it was
 * running. A live group thread cannot answer that: the release of the last lease starts every event
 * loop that never ran, to shut it down ({@link GrpcEventLoopThreads}), and a group still shutting
 * down from before the operation shows threads while the operation's own lease is open. So the
 * answer comes from base instead: {@code base.rpc.GrpcEventLoops.loopsStartedWhileRunning()} counts
 * the event-loop threads that started while their own group was the running one, and leaves out
 * both kinds.
 *
 * <p>Base's state reaches this class as suppliers the caller passes in, {@code
 * GrpcEventLoops::isRunning} and {@code GrpcEventLoops::loopsStartedWhileRunning}, because base
 * depends on this module, not the other way round.
 *
 * <p>{@code isRunning()} also reads true while the group is pinned to a job's class loader with no
 * lease open. What rules a pin out is where these tests run: on the application class path the pin
 * is a no-op. {@link #awaitNoGroup(BooleanSupplier, Duration)} waits for a pinned group's release
 * as it does for a lease's, but a pin registered before any lease opened holds no group yet, and
 * passes it.
 */
@Internal
public final class EventLoopObserver {

    private EventLoopObserver() {}

    /**
     * Runs the operation and returns whether Netty asked a group it opened for an event loop's
     * thread while that group was running. An event loop's thread starts once the loop has a first
     * task, such as a channel's registration, so a client whose channels run on the group starts
     * one, and a client whose channels run elsewhere starts none while its lease is open. The count
     * is taken just before Netty starts the thread, so a start that fails counts too. The answer is
     * read after the operation, so no sample has to land while its lease is open.
     *
     * <p>The operation must start with no group running: a task on a loop that has already started
     * starts no thread, so the count cannot show an operation's use of a group that was already
     * running. It must also be the only user of the group while it runs, since the count does not
     * say which client started a loop.
     *
     * @param running whether a group is running: {@code GrpcEventLoops::isRunning}
     * @param loopsStarted the count of event-loop threads started while their group was running:
     *     {@code GrpcEventLoops::loopsStartedWhileRunning}
     * @param operation the operation, which opens and closes its own client
     * @return whether the count grew while the operation ran
     * @throws IllegalStateException if a group is already running when the operation would start
     * @throws Exception if the operation does
     */
    public static boolean sawGroupDuring(
            BooleanSupplier running,
            LongSupplier loopsStarted,
            ThrowingRunnable<? extends Exception> operation)
            throws Exception {
        if (running.getAsBoolean()) {
            throw new IllegalStateException(
                    "A gRPC event-loop group is already running, so the operation's own use of one"
                            + " cannot be told apart");
        }
        long before = loopsStarted.getAsLong();
        operation.run();
        return loopsStarted.getAsLong() > before;
    }

    /**
     * Waits until no group is running and no group thread is alive, so that a group an earlier
     * class in a reused fork left behind is ruled out before a test relies on its absence. A lease
     * whose client never registered a channel starts no thread, which is why the group is checked
     * too. It cannot rule out a group none of whose event loops ever ran, caught between its
     * release and its shutdown, which for a moment has neither ({@link GrpcEventLoopThreads});
     * {@link #sawGroupDuring(BooleanSupplier, LongSupplier, ThrowingRunnable)} does not count that
     * group's threads.
     *
     * @param running whether a group is running: {@code GrpcEventLoops::isRunning}
     * @param timeout how long to wait
     * @return whether both were gone in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNoGroup(BooleanSupplier running, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (running.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return GrpcEventLoopThreads.awaitNone(
                Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
    }
}
