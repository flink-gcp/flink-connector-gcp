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

package io.github.flink.gcp.connector.bigtable;

import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Observes the connector's gRPC event-loop group around an operation that opens its client and
 * closes it before returning, such as an enumerator's call.
 *
 * <p>A check that only finds no group thread afterwards passes as well when the operation never
 * used the group, so {@link #sawGroupDuring(Callable)} also records whether a group thread was
 * alive while a lease was open. Both halves matter: the group's threads start when a channel
 * registers, but the release of the last lease also starts any that never ran, to shut them down,
 * so a lease opened and released around a client whose channels ran elsewhere still shows threads
 * for a moment. Those appear only after the release has detached the group, when no lease is open,
 * which is why a thread seen alive counts only if a lease is still open right after.
 */
public final class EventLoopObserver {

    private EventLoopObserver() {}

    /**
     * Runs the operation while polling for a live group thread.
     *
     * @param operation the operation, which opens and closes its own client
     * @return whether a group thread was alive, with a lease open, at some point while it ran
     * @throws Exception if the operation does
     */
    public static boolean sawGroupDuring(Callable<?> operation) throws Exception {
        AtomicBoolean seen = new AtomicBoolean();
        AtomicBoolean done = new AtomicBoolean();
        Thread poller =
                new Thread(
                        () -> {
                            while (!done.get() && !seen.get()) {
                                // Threads first, then the lease: a thread the shutdown started
                                // is alive only once no lease is open.
                                if (GrpcEventLoopThreads.alive() && GrpcEventLoops.isRunning()) {
                                    seen.set(true);
                                }
                            }
                        },
                        "event-loop-observer");
        poller.setDaemon(true);
        poller.start();
        try {
            operation.call();
        } finally {
            done.set(true);
            poller.join();
        }
        return seen.get();
    }

    /**
     * Waits until no lease is open and no group thread is alive, so that a group an earlier class
     * in a reused fork left behind is ruled out before a test relies on its absence. A lease whose
     * client never registered a channel starts no thread, which is why the lease is checked too.
     *
     * @param timeout how long to wait
     * @return whether both were gone in time
     * @throws InterruptedException if interrupted while waiting
     */
    public static boolean awaitNoGroup(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (GrpcEventLoops.isRunning()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return GrpcEventLoopThreads.awaitNone(
                Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
    }
}
