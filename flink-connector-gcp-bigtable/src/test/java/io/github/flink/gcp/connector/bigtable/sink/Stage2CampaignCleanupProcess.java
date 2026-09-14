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

package io.github.flink.gcp.connector.bigtable.sink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;

/** Child JVM used to hold real checkpoint deletion across terminal cleanup. */
final class Stage2CampaignCleanupProcess {
    private Stage2CampaignCleanupProcess() {}

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        String mode = args[1];
        if (mode.equals("forced")) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        try {
                                            Files.writeString(
                                                    directory.resolve("terminating"),
                                                    "shutdown hook entered");
                                            new CountDownLatch(1).await();
                                        } catch (Exception failure) {
                                            throw new IllegalStateException(failure);
                                        }
                                    }));
        }
        Stage2CampaignJournal journal = new Stage2CampaignJournal(directory);
        journal.start("a".repeat(32), Long.parseLong(args[2]), args[3], System.currentTimeMillis());
        journal.prepareCell(0);
        journal.tablesReady();
        for (int i = 0; i < 6; i++) {
            String table = Stage2AssessmentPlan.runs().get(i).table();
            var lease = journal.claim(table, 999_999_990L + i, "worker-" + i);
            Path checkpoint =
                    Files.createDirectories(lease.workDirectory().resolve("checkpoint/state"));
            Files.writeString(checkpoint.resolve("first"), "checkpoint data");
            Files.writeString(checkpoint.resolve("second"), "checkpoint data");
            journal.finish(table, 999_999_990L + i, "worker-" + i, true);
        }
        if (mode.equals("before")) {
            Files.writeString(directory.resolve("barrier"), "before cell cleanup");
            await(directory.resolve("release"));
            try {
                journal.cellCleaned("f".repeat(64));
                throw new AssertionError("Stopped controller entered cell cleanup");
            } catch (IllegalStateException expected) {
                if (!expected.getMessage().contains("stopped")) {
                    throw expected;
                }
                Files.writeString(directory.resolve("refused"), expected.getMessage());
            }
            return;
        }
        journal.cellCleaned(
                "f".repeat(64),
                work -> {
                    if (!mode.equals("after")) {
                        try (var paths = Files.walk(work)) {
                            var pending =
                                    paths.sorted(Comparator.reverseOrder())
                                            .collect(java.util.stream.Collectors.toList());
                            Files.delete(pending.get(0));
                            Files.writeString(
                                    directory.resolve("barrier"),
                                    "checkpoint deletion in progress");
                            await(directory.resolve("release"));
                            for (Path path : pending.subList(1, pending.size())) {
                                Files.delete(path);
                            }
                        }
                    } else {
                        Stage2Lease.removeWork(work);
                    }
                });
        Files.writeString(directory.resolve("barrier"), "cell cleanup completed");
        await(directory.resolve("release"));
    }

    private static void await(Path signal) throws java.io.IOException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(45);
        while (!Files.exists(signal)) {
            if (System.nanoTime() >= deadline) {
                throw new java.io.IOException("Controller fixture signal timed out");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(failure);
            }
        }
    }
}
