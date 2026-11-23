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

import java.util.concurrent.TimeUnit;

/**
 * Observes the threads of the connectors' own gRPC event-loop group ({@code
 * base.rpc.GrpcEventLoops}, ADR-0208) by name.
 *
 * <p>The group starts a thread only when a channel registers on it, so a live thread while a client
 * is open shows that the client's channels run on the group rather than on gRPC's shared one, and
 * no thread after it closed shows that its close stopped the group. Both readings assume that no
 * other lease is open in the JVM. Surefire runs one test class at a time in a fork, but it reuses
 * forks, so a class that leaked a lease would leave the group running into the next one: a test
 * checks {@link #awaitNone()} before it opens a client, so that such a leak fails there rather than
 * at the close it would otherwise be blamed on.
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
        return await(false);
    }

    private static boolean await(boolean alive) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (alive() != alive) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }
}
