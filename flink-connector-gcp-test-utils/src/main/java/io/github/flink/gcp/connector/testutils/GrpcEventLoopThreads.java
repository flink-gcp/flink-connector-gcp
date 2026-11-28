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

import java.time.Duration;

/**
 * Observes the threads of the connectors' own gRPC event-loop group ({@code
 * base.rpc.GrpcEventLoops}, ADR-0208) by name.
 *
 * <p>Finding no thread after a client closed shows that no thread of the group was alive when the
 * threads were looked at, not that the close released its lease: a lease whose client never
 * registered a channel has no thread. A live thread does not show that the client's channels run on
 * the group: a name does not say which group a thread belongs to. A release detaches its group
 * before shutting it down, the shutdown starts every event loop that never ran in order to stop it,
 * and a loop busy with a task keeps its thread until the task ends, so an earlier group's threads
 * can read as those of a group a later lease opened, and a lease opened and released around a
 * client whose channels ran on gRPC's shared group still shows threads for a moment. Whether a
 * client's channels run on the group is therefore observed through {@link EventLoopObserver}, which
 * reads base's count of the event-loop threads that Netty asked for while their own group was
 * running rather than thread names, both for an operation that opens and closes its own client and
 * for a client a test keeps open. The readings here assume that no other lease is open in the JVM
 * and that no pin holds the group (a pinned group keeps its threads with no lease open). Surefire
 * runs one test class at a time in a fork, but it reuses forks, so a class that leaked a lease
 * would leave the group running into the next one: a test checks {@link
 * EventLoopObserver#awaitNoGroup(java.util.function.BooleanSupplier, Duration)} before it opens a
 * client, so that such a leak fails there rather than at the close it would otherwise be blamed on.
 * {@link #awaitNone()} alone misses a leaked lease whose client never registered a channel, since
 * that lease has no thread.
 */
@Internal
public final class GrpcEventLoopThreads {

    /**
     * The group's thread-name prefix. It restates {@code GrpcEventLoops.THREAD_NAME_PREFIX} because
     * base depends on this module, not the other way round.
     */
    public static final String PREFIX = "flink-gcp-grpc-elg";

    private static final long TIMEOUT_SECONDS = 10;

    private GrpcEventLoopThreads() {}

    /**
     * Returns whether a thread of the group is alive. The thread can be one of an earlier group
     * that is still shutting down, so whether a client's channels run on the group is read from
     * {@link EventLoopObserver} instead.
     *
     * @return whether one is
     */
    public static boolean alive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().startsWith(PREFIX));
    }

    /**
     * Waits up to ten seconds for every thread of the group to finish.
     *
     * @return whether they had finished in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNone() throws InterruptedException {
        return awaitNone(Duration.ofSeconds(TIMEOUT_SECONDS));
    }

    /**
     * Waits up to the given time for every thread of the group to finish. For a check before a test
     * opens a client: a MiniCluster job an earlier class in the fork cancelled tears its clients
     * down after that class has returned, within the clients' own shutdown budgets.
     *
     * @param timeout how long to wait
     * @return whether they had finished in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNone(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (alive()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }
}
