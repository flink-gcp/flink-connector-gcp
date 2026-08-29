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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;

import java.time.Duration;

/** Child-JVM probe for the production Bigtable client lifecycle. */
public final class BigtableInstanceClientLifecycleProbe {

    private static final long MAX_RETAINED_HEAP_GROWTH = 8L * 1024 * 1024;
    private static final int MAX_RETAINED_THREAD_GROWTH = 16;
    private static final Duration RECLAMATION_TIMEOUT = Duration.ofSeconds(15);

    private BigtableInstanceClientLifecycleProbe() {}

    public static void main(String[] args) throws Exception {
        DefaultMutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        null,
                        BigtableWriterOptions.defaults(),
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));
        try {
            exercise(factory, 0, 32);
            quiesce();
            int baselineThreads = Thread.getAllStackTraces().size();
            long baselineHeap = usedHeap();

            exercise(factory, 32, 512);
            ResourceSnapshot finalResources = awaitReclaimed(baselineThreads, baselineHeap);
            int finalThreads = finalResources.threads;
            long finalHeap = finalResources.heap;

            if (factory.activeClientCount() != 0) {
                throw new AssertionError(
                        "The production factory retained "
                                + factory.activeClientCount()
                                + " instance client(s).");
            }
            if (finalThreads > baselineThreads + MAX_RETAINED_THREAD_GROWTH) {
                throw new AssertionError(
                        "Thread count grew from "
                                + baselineThreads
                                + " to "
                                + finalThreads
                                + " after historical instances were released.");
            }
            long retainedGrowth = finalHeap - baselineHeap;
            if (retainedGrowth > MAX_RETAINED_HEAP_GROWTH) {
                throw new AssertionError(
                        "Retained heap grew by "
                                + retainedGrowth
                                + " bytes after historical instances were released.");
            }
            System.out.println(
                    "OK baselineThreads="
                            + baselineThreads
                            + " finalThreads="
                            + finalThreads
                            + " retainedHeapGrowth="
                            + retainedGrowth);
        } finally {
            factory.close();
        }
    }

    private static void exercise(DefaultMutationBatcherFactory factory, int first, int count)
            throws Exception {
        for (int i = first; i < first + count; i++) {
            TableDestination destination = TableDestination.of("p", "instance-" + i, "orders");
            MutationBatcher batcher = factory.create(destination);
            batcher.close();
            factory.release(destination);
        }
    }

    private static void quiesce() throws InterruptedException {
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(50);
        }
    }

    private static ResourceSnapshot awaitReclaimed(int baselineThreads, long baselineHeap)
            throws InterruptedException {
        long deadline = System.nanoTime() + RECLAMATION_TIMEOUT.toNanos();
        ResourceSnapshot snapshot;
        do {
            System.gc();
            Thread.sleep(50);
            snapshot = new ResourceSnapshot(Thread.getAllStackTraces().size(), usedHeap());
        } while ((snapshot.threads > baselineThreads + MAX_RETAINED_THREAD_GROWTH
                        || snapshot.heap - baselineHeap > MAX_RETAINED_HEAP_GROWTH)
                && System.nanoTime() < deadline);
        return snapshot;
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static final class ResourceSnapshot {
        private final int threads;
        private final long heap;

        private ResourceSnapshot(int threads, long heap) {
            this.threads = threads;
            this.heap = heap;
        }
    }
}
