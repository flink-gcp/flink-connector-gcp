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
 * closes it before returning, such as an enumerator's or an admin's call, and around a client a
 * test keeps open, such as a writer's or a reader's.
 *
 * <p>A check that only finds no group thread afterwards passes as well when the operation never
 * used the group, so {@link #sawGroupDuring(BooleanSupplier, LongSupplier, ThrowingRunnable)} also
 * asks whether a group the operation opened gave one of its event loops a first task while it was
 * running. A live group thread cannot answer that: the release of the last lease starts every event
 * loop that never ran, to shut it down ({@link GrpcEventLoopThreads}), and a group still shutting
 * down from before the operation shows threads while the operation's own lease is open. So the
 * answer comes from base instead: {@code base.rpc.GrpcEventLoops.loopsStartedWhileRunning()} counts
 * the event-loop threads that started while their own group was the running one, and leaves out
 * both kinds. A client the test keeps open is observed through the same count, by {@link
 * #loopStartsFromNow(BooleanSupplier, LongSupplier)} before it opens and {@link
 * LoopStarts#anyStarted()} while it is open, since a live group thread there can as well be one of
 * an earlier group that is still shutting down.
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
        LoopStarts starts = loopStartsFromNow(running, loopsStarted);
        operation.run();
        return starts.anyStarted();
    }

    /**
     * Reads the count of event-loop threads started while their group was running, before a test
     * opens a client it keeps open, so that {@link LoopStarts#anyStarted()} can say while the
     * client is open whether a group the client opened gave one of its event loops a first task. A
     * live group thread cannot say that: it can as well belong to an earlier group that is still
     * shutting down ({@link GrpcEventLoopThreads}).
     *
     * <p>The same conditions as for {@link #sawGroupDuring(BooleanSupplier, LongSupplier,
     * ThrowingRunnable)} hold: no group may be running when the count is read, and the client must
     * be the only user of the group until it is read again, since the count does not say which
     * client started a loop.
     *
     * @param running whether a group is running: {@code GrpcEventLoops::isRunning}
     * @param loopsStarted the count of event-loop threads started while their group was running:
     *     {@code GrpcEventLoops::loopsStartedWhileRunning}
     * @return a baseline that later readings of the count are compared against
     * @throws IllegalStateException if a group is already running
     */
    public static LoopStarts loopStartsFromNow(BooleanSupplier running, LongSupplier loopsStarted) {
        if (running.getAsBoolean()) {
            throw new IllegalStateException(
                    "A gRPC event-loop group is already running, so what follows cannot be told"
                            + " apart from what holds it: a lease is still open, perhaps left by an"
                            + " earlier class in this fork, or a pin holds the group");
        }
        return new LoopStarts(loopsStarted, loopsStarted.getAsLong());
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

    /**
     * A baseline of the count of event-loop threads started while their group was running, taken by
     * {@link #loopStartsFromNow(BooleanSupplier, LongSupplier)} with no group running. Later
     * readings of the count are compared against it.
     */
    public static final class LoopStarts {

        private static final Duration TIMEOUT = Duration.ofSeconds(10);

        private final LongSupplier loopsStarted;
        private final long before;

        private LoopStarts(LongSupplier loopsStarted, long before) {
            this.loopsStarted = loopsStarted;
            this.before = before;
        }

        /**
         * Returns whether Netty has asked for an event-loop thread since the count was read, while
         * the thread's own group was running. An event loop's thread starts once the loop has a
         * first task, such as a channel's registration, so a client that registers a channel on a
         * group it opened starts one, and a client whose channels run elsewhere starts none. The
         * count is taken just before Netty starts the thread, so a start that fails counts too. It
         * never goes down, so it does not say that the group is still running; {@code
         * GrpcEventLoops.isRunning()} does, for any lease or pin, not for a particular one.
         *
         * @return whether Netty has
         */
        public boolean anyStarted() {
            return loopsStarted.getAsLong() > before;
        }

        /**
         * Waits up to ten seconds for {@link #anyStarted()}, for a client whose channels register
         * on an event loop after the call that opened them has returned.
         *
         * @return whether one started in time
         * @throws InterruptedException if interrupted while waiting
         */
        public boolean awaitAnyStarted() throws InterruptedException {
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (!anyStarted()) {
                if (System.nanoTime() >= deadline) {
                    return false;
                }
                Thread.sleep(20);
            }
            return true;
        }
    }
}
