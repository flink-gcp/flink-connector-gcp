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
 * <p>The group starts a thread when a channel registers on it, so a live thread while a client is
 * open shows that a channel of the client's runs on the group rather than on gRPC's shared one (not
 * that every one does), and no thread after it closed shows that its close stopped the group. The
 * release of the last lease starts threads too: the group's shutdown starts every event loop that
 * never ran in order to stop it, so a lease opened and released around a client whose channels ran
 * on gRPC's group still shows threads for a moment, and finding none afterwards does not show the
 * client used the group. An operation that opens and closes its own client is therefore observed
 * through {@link EventLoopObserver}, which counts a thread only while a lease is open. These
 * readings assume that no other lease is open in the JVM, that no pin holds the group (a pinned
 * group keeps its threads with no lease open), and that no earlier group is still shutting down:
 * its release detaches it first, so a group none of whose event loops ever ran has, for a moment,
 * neither a lease nor a thread, and the threads its shutdown then starts would read as the next
 * lease's. Surefire runs one test class at a time in a fork, but it reuses forks, so a class that
 * leaked a lease would leave the group running into the next one: a test checks {@link
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
     * Returns whether a thread of the group is alive.
     *
     * @return whether one is
     */
    public static boolean alive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().startsWith(PREFIX));
    }

    /**
     * Waits up to ten seconds for a thread of the group to be alive.
     *
     * @return whether one became alive in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitAlive() throws InterruptedException {
        return await(true);
    }

    /**
     * Waits up to ten seconds for every thread of the group to finish.
     *
     * @return whether they had finished in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNone() throws InterruptedException {
        return await(false, Duration.ofSeconds(TIMEOUT_SECONDS));
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
        return await(false, timeout);
    }

    private static boolean await(boolean alive) throws InterruptedException {
        return await(alive, Duration.ofSeconds(TIMEOUT_SECONDS));
    }

    private static boolean await(boolean alive, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (alive() != alive) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }
}
