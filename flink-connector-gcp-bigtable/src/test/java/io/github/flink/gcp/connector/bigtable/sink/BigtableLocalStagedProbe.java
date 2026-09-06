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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.stream.Stream;

/** Standalone fixed-input local calibration, never a Bigtable service-capacity benchmark. */
public final class BigtableLocalStagedProbe {
    private BigtableLocalStagedProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 8
                || !(args[0].equals("staged") || args[0].equals("bulk"))
                || !(args[7].equals("hot") || args[7].equals("even"))) {
            throw new IllegalArgumentException(
                    "arm(staged|bulk) count payloadBytes parallelism inFlight checkpointMillis delayMillis keys(hot|even)");
        }
        int count = Integer.parseInt(args[1]);
        int bytes = Integer.parseInt(args[2]);
        int parallelism = Integer.parseInt(args[3]);
        int inFlight = Integer.parseInt(args[4]);
        long interval = Long.parseLong(args[5]);
        long delay = Long.parseLong(args[6]);
        if (count < 1
                || count > 100_000
                || (bytes != 1024 && bytes != 65536)
                || parallelism < 1
                || parallelism > 16
                || inFlight < 1
                || inFlight > 16
                || interval < 1
                || interval > 60_000
                || delay < 0
                || delay > 1000) {
            throw new IllegalArgumentException(
                    "Local probe limits: count 1..100000, payload 1024|65536, parallelism/inFlight 1..16, checkpoint 1..60000 ms, delay 0..1000 ms");
        }
        if ((long) count * bytes > 32L * 1024 * 1024) {
            throw new IllegalArgumentException(
                    "Local probe retained payload limit: count * payloadBytes must not exceed 32 MiB; reduce count or payloadBytes");
        }
        Path directory = Files.createTempDirectory("bigtable-local-staged-");
        System.out.println("LOCAL_DIRECTORY " + directory);
        try (LocalStagedHarness run =
                new LocalStagedHarness(bytes, args[7].equals("hot"), false, inFlight)) {
            run.delayMillis = delay;
            long gcBefore = gcMillis();
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run,
                            directory,
                            args[0].equals("staged"),
                            false,
                            parallelism,
                            count,
                            interval,
                            false,
                            null,
                            false)) {
                job.finish();
                long elapsed = System.nanoTime() - job.started;
                long gc = gcMillis() - gcBefore;
                if (run.acknowledgements.size() != count
                        || !run.acknowledgements.keySet().equals(run.admissions.keySet())) {
                    throw new IllegalStateException("Input acknowledgement inventory mismatch");
                }
                System.out.println("CHECKPOINT_STATS " + job.checkpointStats());
                var completions = new java.util.ArrayList<>(run.clientCompletionNanos);
                java.util.Collections.sort(completions);
                System.out.printf(
                        Locale.ROOT,
                        "CLIENT_COMPLETION,%d,%d,%d,%d%n",
                        completions.size(),
                        completions.get((int) Math.ceil(completions.size() * 0.50) - 1),
                        completions.get((int) Math.ceil(completions.size() * 0.95) - 1),
                        completions.get((int) Math.ceil(completions.size() * 0.99) - 1));
                System.gc();
                long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
                System.out.printf(
                        Locale.ROOT,
                        "LOCAL,%s,%d,%d,%d,%d,%d,%d,%s,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                        args[0],
                        count,
                        bytes,
                        parallelism,
                        inFlight,
                        interval,
                        delay,
                        args[7],
                        run.throughput(),
                        run.percentileNanos(0.50),
                        run.percentileNanos(0.95),
                        run.percentileNanos(0.99),
                        run.peakWriterEntries.get(),
                        run.peakWriterBytes.get(),
                        run.peakActive.get(),
                        heap,
                        gc,
                        elapsed);
            }
        } finally {
            try (Stream<Path> paths = Files.walk(directory)) {
                for (Path path :
                        paths.sorted(Comparator.reverseOrder())
                                .collect(java.util.stream.Collectors.toList())) {
                    Files.delete(path);
                }
            }
            if (Files.exists(directory)) {
                throw new IllegalStateException(
                        "Local checkpoint directory was not removed: " + directory);
            }
        }
        System.out.println("LOCAL_CLEANUP absent");
    }

    private static long gcMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> Math.max(0, bean.getCollectionTime()))
                .sum();
    }
}
