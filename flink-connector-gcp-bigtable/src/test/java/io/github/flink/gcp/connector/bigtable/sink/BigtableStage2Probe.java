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

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Standalone Stage 2 entry point; service commands require a separately created lease manifest. */
public final class BigtableStage2Probe {
    private BigtableStage2Probe() {}

    public static void main(String[] args) throws Exception {
        try {
            execute(args);
        } catch (Exception failure) {
            if (args.length >= 2
                    && (args[0].equals("create")
                            || args[0].equals("service")
                            || args[0].equals("preflight"))) {
                Path manifest = Path.of(args[1]).toAbsolutePath();
                if (Files.isRegularFile(manifest)) {
                    try {
                        Files.writeString(
                                manifest.resolveSibling("stop"),
                                "Experiment failed; clean exact owned resources");
                    } catch (IOException cleanupSignal) {
                        failure.addSuppressed(cleanupSignal);
                    }
                }
            }
            throw failure;
        }
    }

    private static void execute(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("plan")) {
            Stage2Lease lease = Stage2Lease.plan(Path.of(args[1]));
            System.out.println(
                    "STAGE2_PLAN "
                            + lease.instance
                            + " project=flink-gcp zone=us-central1-b nodes=1"
                            + " leaseMinutes=60 maxWriteAttempts=250000 maxWriteBytes=1073741824 maxReadBytes=1073741824");
            return;
        }
        if (args.length == 2) {
            Stage2Lease lease = new Stage2Lease(Path.of(args[1]));
            switch (args[0]) {
                case "create":
                    lease.create();
                    return;
                case "cleanup":
                    lease.cleanup();
                    return;
                case "supervise":
                    lease.supervise();
                    return;
                case "monitor":
                    Stage2Monitoring.capture(lease);
                    return;
                case "preflight":
                    preflight(lease);
                    return;
                default:
                    throw new IllegalArgumentException("Unknown lease command");
            }
        }
        if (args.length == 3 && args[0].equals("service")) {
            Stage2Lease lease = new Stage2Lease(Path.of(args[1]));
            String table = args[2];
            lease.claim(table);
            if (table.startsWith("recovery-")) {
                recovery(lease, table);
            } else if (table.equals("hot")
                    || table.equals("serialized")
                    || table.matches("(bulk|staged)-r[123]")) {
                boolean staged = !table.startsWith("bulk");
                boolean hot = table.equals("hot");
                int capacity = hot ? 35_000 : table.equals("serialized") ? 256 : 4000;
                long measure = hot ? 300_000 : table.equals("serialized") ? 10_000 : 30_000;
                timed(
                        lease,
                        table,
                        lease.work.resolve(table),
                        staged,
                        false,
                        "127.0.0.1:1",
                        1024,
                        1,
                        table.equals("serialized") ? 1 : 4,
                        1000,
                        10_000,
                        measure,
                        capacity,
                        0,
                        hot);
            } else {
                throw new IllegalArgumentException("Table is not an authorized experiment");
            }
            return;
        }
        if (args.length == 12 && args[0].equals("local")) {
            if (!(args[2].equals("staged") || args[2].equals("bulk"))
                    || !(args[11].equals("hot") || args[11].equals("even"))) {
                throw new IllegalArgumentException(
                        "Local arm must be bulk|staged and keys even|hot");
            }
            timed(
                    null,
                    "local-table",
                    Path.of(args[1]),
                    args[2].equals("staged"),
                    false,
                    "127.0.0.1:1",
                    Integer.parseInt(args[3]),
                    Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]),
                    Long.parseLong(args[6]),
                    Long.parseLong(args[7]),
                    Long.parseLong(args[8]),
                    Integer.parseInt(args[9]),
                    Long.parseLong(args[10]),
                    args[11].equals("hot"));
            return;
        }
        throw new IllegalArgumentException(
                "Commands: plan|create|cleanup|supervise|preflight manifest; "
                        + "service manifest table; local directory arm bytes parallelism inFlight checkpointMillis "
                        + "warmupMillis measureMillis capacity delayMillis keys");
    }

    static void timed(
            Stage2Lease lease,
            String table,
            Path directory,
            boolean staged,
            boolean emulator,
            String endpoint,
            int bytes,
            int parallelism,
            int inFlight,
            long intervalMillis,
            long warmupMillis,
            long measurementMillis,
            int capacity,
            long delayMillis,
            boolean hot)
            throws Exception {
        if ((bytes != 1024 && bytes != 65536)
                || parallelism < 1
                || parallelism > 16
                || inFlight < 1
                || inFlight > 16
                || intervalMillis < 1
                || intervalMillis > 60_000
                || warmupMillis < 0
                || warmupMillis > 60_000
                || measurementMillis < 1
                || measurementMillis > 300_000
                || delayMillis < 0
                || delayMillis > 1000
                || capacity < 1
                || capacity > 1_000_000) {
            throw new IllegalArgumentException(
                    "Stage 2 local calibration configuration is outside its limits");
        }
        Stage2Harness.checkDistribution();
        Files.createDirectories(directory.getParent());
        Files.createDirectory(directory);
        TableDestination destination =
                lease == null
                        ? TableDestination.of("local-project", "local-instance", table)
                        : lease.table(table);
        try (Stage2Harness run =
                        new Stage2Harness(
                                destination,
                                endpoint,
                                directory.resolve("inventory.bin"),
                                capacity,
                                bytes,
                                hot,
                                false,
                                inFlight,
                                true,
                                lease);
                LocalStagedJob job =
                        new LocalStagedJob(
                                run,
                                directory,
                                staged,
                                lease != null || emulator,
                                parallelism,
                                capacity,
                                intervalMillis,
                                true,
                                null,
                                false);
                PrintWriter samples =
                        new PrintWriter(
                                Files.newBufferedWriter(directory.resolve("samples.jsonl")))) {
            run.delayMillis = delayMillis;
            await(
                    "Stage 2 readers",
                    Duration.ofSeconds(30),
                    () -> {
                        if (job.result.isDone()) {
                            job.result.join();
                        }
                        return run.readersStarted.get() == parallelism;
                    },
                    () -> "started=" + run.readersStarted.get());
            Stage2Sampler sampler = new Stage2Sampler(job);
            String initialSample =
                    "{\"phase\":\"before-admission\",\"checkpoints\":"
                            + job.checkpointStats()
                            + "}";
            long sampleBytes = initialSample.length() * 2L;
            if (sampleBytes > 8L * 1024 * 1024) {
                throw new IOException("Stage 2 checkpoint/measurement storage cap exhausted");
            }
            samples.println(initialSample);
            if (samples.checkError()) {
                throw new IOException("Stage 2 measurement write failed");
            }
            System.out.println(
                    "STAGE2_CONFIG bytes="
                            + bytes
                            + " parallelism="
                            + parallelism
                            + " inFlight="
                            + inFlight
                            + " checkpointMillis="
                            + intervalMillis
                            + " capacity="
                            + capacity
                            + " hot="
                            + hot
                            + " delayMillis="
                            + delayMillis
                            + " jvmFlags="
                            + ManagementFactory.getRuntimeMXBean().getInputArguments());
            if (lease != null
                    && System.currentTimeMillis() + warmupMillis + measurementMillis
                            > lease.startedAt() + 45 * 60_000L) {
                throw new IOException(
                        "Full observation no longer fits the admission lease; leave it unmeasured");
            }
            long gcBefore = gcMillis();
            long started = System.nanoTime();
            run.startWindow(
                    started,
                    TimeUnit.MILLISECONDS.toNanos(warmupMillis),
                    TimeUnit.MILLISECONDS.toNanos(measurementMillis));
            System.out.println(
                    "STAGE2_WINDOW start="
                            + java.time.Instant.now()
                            + " warmupMillis="
                            + warmupMillis
                            + " measurementMillis="
                            + measurementMillis);
            long deadline =
                    started
                            + TimeUnit.MILLISECONDS.toNanos(
                                    warmupMillis + measurementMillis + 120_000);
            long peakHeap = 0;
            long nextSample = started;
            while (!job.result.isDone()) {
                long now = System.nanoTime();
                if (now >= deadline) {
                    throw new IOException("Stage 2 drain deadline expired");
                }
                if (lease != null) {
                    lease.requireLive();
                }
                if (now >= nextSample) {
                    String sample = job.checkpointStats();
                    String runtime = sampler.sample();
                    sampleBytes += (sample.length() + runtime.length()) * 2L;
                    if (sampleBytes > 8L * 1024 * 1024
                            || directorySize(lease == null ? directory : lease.work)
                                    > 2L * 1024 * 1024 * 1024) {
                        throw new IOException(
                                "Stage 2 checkpoint/measurement storage cap exhausted");
                    }
                    peakHeap =
                            Math.max(
                                    peakHeap,
                                    ManagementFactory.getMemoryMXBean()
                                            .getHeapMemoryUsage()
                                            .getUsed());
                    samples.println(
                            "{\"elapsedNanos\":"
                                    + (now - started)
                                    + ",\"stagedEntries\":"
                                    + run.stagedTotal(0)
                                    + ",\"stagedBytes\":"
                                    + run.stagedTotal(1)
                                    + ",\"activeRequests\":"
                                    + run.active.get()
                                    + ",\"runtime\":"
                                    + runtime
                                    + ",\"checkpoints\":"
                                    + sample
                                    + "}");
                    if (samples.checkError()) {
                        throw new IOException("Stage 2 measurement write failed");
                    }
                    nextSample = now + TimeUnit.SECONDS.toNanos(1);
                }
                try {
                    job.result.get(100, TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException waiting) {
                    /* Supervisor checks deadlines between polls. */
                }
            }
            job.result.get();
            long gc = gcMillis() - gcBefore;
            if (run.ledger.measuredCount() == 0 && run.censored.get()) {
                if (run.ledger.admittedCount() != run.ledger.acknowledgedCount()) {
                    throw new IOException("Censored inventory was not drained");
                }
                System.out.println(
                        "STAGE2_CENSORED no measured inputs; capacity exhausted in warmup");
            } else {
                Stage2Ledger.Summary result = run.ledger.summary();
                System.gc();
                long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
                System.out.printf(
                        Locale.ROOT,
                        "STAGE2,%s,%s,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                        staged ? "staged" : "bulk",
                        run.censored.get() ? "CENSORED" : "OBSERVATION",
                        result.count,
                        result.throughput,
                        result.p50,
                        result.p95,
                        result.p99,
                        result.drainNanos,
                        run.clientQuantileUpperBound(.95),
                        run.peakWriterEntries.get(),
                        run.peakWriterBytes.get(),
                        run.peakActive.get(),
                        heap,
                        peakHeap,
                        gc,
                        run.serializationAllocatedBytes.get(),
                        run.restoreAllocatedBytes.get(),
                        run.sourcePollNanos.get(),
                        run.committerWaitNanos.get(),
                        run.sourceStarvations.get());
            }
            System.out.println("STAGE2_CHECKPOINT_FINAL " + job.checkpointStats());
            if (lease != null || emulator) {
                run.readback(staged, emulator);
            }
            if (!run.attempts.isEmpty()
                    || !run.originalFutures.isEmpty()
                    || !run.admissions.isEmpty()
                    || !run.acknowledgements.isEmpty()
                    || !run.clientCompletionNanos.isEmpty()
                    || !run.store.sent.isEmpty()
                    || !run.store.cells.isEmpty()) {
                throw new IOException(
                        "Sustained instrumentation retained the finite probe's tracing");
            }
            run.ledger.sync();
            samples.flush();
            preserveSamples(lease, directory, false);
        } catch (Exception failure) {
            boolean workloadLimit = false;
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                String message = cause.getMessage();
                if (message != null
                        && (message.contains("Staging capacity exceeded")
                                || message.contains("budget exhausted")
                                || message.contains("storage cap exhausted"))) {
                    workloadLimit = true;
                }
            }
            System.out.println(
                    "STAGE2_TERMINAL " + (workloadLimit ? "CENSORED_WORKLOAD_LIMIT" : "FAILED"));
            try {
                preserveSamples(lease, directory, true);
            } catch (IOException evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            throw failure;
        } finally {
            if (lease == null) {
                Stage2Lease.removeWork(directory);
                System.out.println("STAGE2_LOCAL_CLEANUP ABSENT " + directory);
            }
        }
    }

    private static void preserveSamples(Stage2Lease lease, Path directory, boolean failed)
            throws IOException {
        Path source = directory.resolve("samples.jsonl");
        if (!Files.isRegularFile(source, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            if (failed) {
                return;
            }
            throw new IOException("Stage 2 measurement file is missing");
        }
        if (Files.size(source) > 8L * 1024 * 1024) {
            throw new IOException("Stage 2 sample evidence exceeds its storage cap");
        }
        Path evidence =
                (lease == null ? directory.getParent() : lease.manifest.getParent())
                        .resolve(
                                directory.getFileName()
                                        + (failed ? "-failed" : "")
                                        + "-samples.jsonl");
        Files.copy(source, evidence);
        System.out.println("STAGE2_SAMPLES " + evidence);
    }

    static long directorySize(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            throw new IOException("Owned work directory disappeared");
        }
        java.util.concurrent.atomic.AtomicLong total = new java.util.concurrent.atomic.AtomicLong();
        Files.walkFileTree(
                directory,
                new java.nio.file.SimpleFileVisitor<>() {
                    @Override
                    public java.nio.file.FileVisitResult visitFile(
                            Path file, java.nio.file.attribute.BasicFileAttributes attributes) {
                        total.addAndGet(attributes.size());
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }

                    @Override
                    public java.nio.file.FileVisitResult visitFileFailed(
                            Path file, IOException failure) throws IOException {
                        if (failure instanceof java.nio.file.NoSuchFileException) {
                            return java.nio.file.FileVisitResult.CONTINUE;
                        }
                        throw failure;
                    }
                });
        return total.get();
    }

    private static long gcMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> Math.max(0, bean.getCollectionTime()))
                .sum();
    }

    private static void preflight(Stage2Lease lease) throws Exception {
        Stage2Preflight.read(lease.table("staged-r1"), LocalStagedHarness.PROFILE);
        for (String profile : new String[] {"no-tx", "multi-cluster"}) {
            boolean rejected = false;
            try {
                Stage2Preflight.read(lease.table("staged-r1"), profile);
            } catch (IOException expected) {
                if (!expected.getMessage().contains("requires single-cluster")) {
                    throw expected;
                }
                rejected = true;
                System.out.println(
                        "STAGE2_REJECT_PROFILE " + profile + " " + expected.getMessage());
            }
            if (!rejected) {
                throw new IOException("Invalid profile was accepted");
            }
        }
        for (String table : new String[] {"marker-missing", "marker-gc", "marker-typed"}) {
            boolean rejected = false;
            try {
                Stage2Preflight.read(lease.table(table), LocalStagedHarness.PROFILE);
            } catch (IOException expected) {
                rejected = true;
                if (!expected.getMessage().contains("requires an existing raw marker")) {
                    throw expected;
                }
                System.out.println("STAGE2_REJECT_FAMILY " + table + " " + expected.getMessage());
            }
            if (!rejected) {
                throw new IOException("Invalid marker family was accepted");
            }
        }
    }

    private static void recovery(Stage2Lease lease, String table) throws Exception {
        Path directory = lease.work.resolve(table);
        Files.createDirectories(directory);
        try (Stage2Harness run =
                new Stage2Harness(
                        lease.table(table),
                        "127.0.0.1:1",
                        directory.resolve("inventory.bin"),
                        1024,
                        1024,
                        true,
                        true,
                        4,
                        false,
                        lease)) {
            recovery(run, directory, true);
        }
    }

    static void recovery(Stage2Harness run, Path directory, boolean service) throws Exception {
        run.startWindow(System.nanoTime(), 0, TimeUnit.HOURS.toNanos(1));
        run.loseAnswerAt = 8;
        String checkpoint;
        try (LocalStagedJob job =
                new LocalStagedJob(
                        run,
                        directory.resolve("initial"),
                        true,
                        service,
                        2,
                        128,
                        60_000,
                        true,
                        null,
                        true)) {
            await(
                    "Stage 2 recovery admissions",
                    Duration.ofSeconds(30),
                    () -> {
                        if (job.result.isDone()) {
                            job.result.join();
                        }
                        return run.ledger.admittedCount() == 128;
                    },
                    () -> "admitted=" + run.ledger.admittedCount());
            checkpoint = job.checkpoint();
            await(
                    "Stage 2 recovered acknowledgements",
                    Duration.ofSeconds(90),
                    () -> {
                        if (job.result.isDone()) {
                            job.result.join();
                        }
                        return run.ledger.acknowledgedCount() == 128;
                    },
                    () -> "ack=" + run.ledger.acknowledgedCount());
            if (run.deduplicated.get() == 0) {
                throw new IOException("Response-loss recovery did not replay persisted envelopes");
            }
        }
        if (service) {
            run.readback(true, false);
        } else {
            run.verifyFakeSums();
        }
        for (int parallelism : new int[] {1, 3}) {
            int deduplicatedBefore = run.deduplicated.get();
            try (LocalStagedJob restored =
                    new LocalStagedJob(
                            run,
                            directory.resolve("restore-" + parallelism),
                            true,
                            service,
                            parallelism,
                            128,
                            60_000,
                            true,
                            checkpoint,
                            false)) {
                await(
                        "Restored committer closed loop",
                        Duration.ofSeconds(90),
                        () -> {
                            if (restored.result.isDone()) {
                                restored.result.join();
                            }
                            return run.active.get() == 0
                                    && run.deduplicated.get() >= deduplicatedBefore + 128;
                        },
                        () -> "deduplicated=" + run.deduplicated.get());
                restored.savepoint(directory.resolve("stop-" + parallelism), true);
            }
            if (service) {
                run.readback(true, false);
            } else {
                run.verifyFakeSums();
            }
        }
        System.out.println(
                "STAGE2_SUM_RECOVERY PASS acknowledgements="
                        + run.acknowledgedCount()
                        + " deduplicated="
                        + run.deduplicated.get());
    }
}
