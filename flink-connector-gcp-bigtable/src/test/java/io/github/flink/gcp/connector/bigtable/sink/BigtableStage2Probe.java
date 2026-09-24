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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Standalone Stage 2 entry point; service commands require a separately created lease manifest. */
public final class BigtableStage2Probe {
    static final long ORDINARY_SAMPLE_BYTES = 8L * 1024 * 1024;
    static final long SUSTAINED_SAMPLE_BYTES = 64L * 1024 * 1024;

    /**
     * Admits the bulk writer's default of 1,000 in-flight entries; the staged committer's default
     * of 100 lies inside it (issue #1464).
     */
    static final int MAX_IN_FLIGHT = 1000;

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
            if (lease.productionRecovery
                    && !(args[0].equals("cleanup") || args[0].equals("supervise"))) {
                throw new IllegalArgumentException(
                        "Production recovery requires its own entry point");
            }
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
                case "preflight":
                    preflight(lease);
                    return;
                default:
                    throw new IllegalArgumentException("Unknown lease command");
            }
        }
        if (args.length == 3 && args[0].equals("service")) {
            Stage2Lease lease = new Stage2Lease(Path.of(args[1]));
            if (lease.productionRecovery) {
                throw new IllegalArgumentException(
                        "Experimental workers cannot use a production recovery lease");
            }
            String table = args[2];
            if (table.startsWith("recovery-")) {
                throw new IllegalArgumentException(
                        "Recovery runs use BigtableProductionRecoveryProbe");
            }
            lease.claim(table);
            if (table.equals("hot")
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
        timed(
                lease,
                table,
                directory,
                staged,
                emulator,
                endpoint,
                bytes,
                parallelism,
                inFlight,
                intervalMillis,
                warmupMillis,
                measurementMillis,
                capacity,
                delayMillis,
                hot,
                Stage2RunLimits.historical(capacity));
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
            boolean hot,
            Stage2RunLimits limits)
            throws Exception {
        timedRun(
                Stage2RunLease.legacy(lease),
                table,
                directory,
                staged,
                emulator,
                endpoint,
                bytes,
                parallelism,
                inFlight,
                intervalMillis,
                warmupMillis,
                measurementMillis,
                capacity,
                delayMillis,
                hot,
                limits);
    }

    static boolean timedRun(
            Stage2RunLease lease,
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
            boolean hot,
            Stage2RunLimits limits)
            throws Exception {
        return timedRun(
                lease,
                table,
                directory,
                staged,
                emulator,
                endpoint,
                bytes,
                parallelism,
                inFlight,
                intervalMillis,
                warmupMillis,
                measurementMillis,
                capacity,
                delayMillis,
                hot,
                limits,
                300_000L,
                ORDINARY_SAMPLE_BYTES);
    }

    /** Runs a separately reserved hot-row phase through the same production instrument. */
    static boolean sustainedHotRun(
            Stage2RunLease lease,
            String table,
            Path directory,
            int bytes,
            int parallelism,
            int inFlight,
            long intervalMillis,
            long warmupMillis,
            long measurementMillis,
            Stage2RunLimits limits)
            throws Exception {
        return timedRun(
                lease,
                table,
                directory,
                true,
                false,
                lease == null ? "127.0.0.1:1" : "",
                bytes,
                parallelism,
                inFlight,
                intervalMillis,
                warmupMillis,
                measurementMillis,
                limits.inventoryEntries,
                0,
                true,
                limits,
                3_600_000L,
                SUSTAINED_SAMPLE_BYTES);
    }

    private static boolean timedRun(
            Stage2RunLease lease,
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
            boolean hot,
            Stage2RunLimits limits,
            long maximumMeasurementMillis,
            long maximumSampleBytes)
            throws Exception {
        if ((bytes != 1024 && bytes != 65536)
                || parallelism < 1
                || parallelism > 16
                || inFlight < 1
                || inFlight > MAX_IN_FLIGHT
                || intervalMillis < 1
                || intervalMillis > 60_000
                || warmupMillis < 0
                || warmupMillis > 60_000
                || measurementMillis < 1
                || measurementMillis > maximumMeasurementMillis
                || delayMillis < 0
                || delayMillis > 1000
                || capacity < 1
                || capacity != limits.inventoryEntries) {
            throw new IllegalArgumentException(
                    "Stage 2 local calibration configuration is outside its limits");
        }
        Math.multiplyExact(
                Math.addExact(Math.addExact(warmupMillis, measurementMillis), limits.drainMillis),
                1_000_000L);
        boolean light = lightInstrument(lease != null || emulator);
        long admitPerSecond = admitPerSecond(parallelism);
        Stage2CommitProgress.enableWaitAccounting();
        Stage2Harness.checkDistribution();
        Files.createDirectories(directory.getParent());
        Files.createDirectory(directory);
        TableDestination destination =
                lease == null
                        ? TableDestination.of("local-project", "local-instance", table)
                        : lease.table(table);
        // Read by the failure path, so a failed or censored run still reports its commit records.
        Stage2Harness observedRun = null;
        boolean recordsPrinted = false;
        try (Stage2Harness run =
                        instrumented(
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
                                        lease,
                                        limits),
                                light,
                                admitPerSecond);
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
            observedRun = run;
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
                    "{\"phase\":\"before-admission\",\"admissionMode\":\""
                            + (run.admitPerSecond == 0 ? "unrestricted" : "paced")
                            + "\",\"admitPerSecond\":"
                            + run.admitPerSecond
                            + ",\"instrument\":\""
                            + (run.light ? "light" : "full")
                            + "\",\"checkpoints\":"
                            + job.checkpointStats()
                            + "}";
            long sampleBytes =
                    (initialSample + System.lineSeparator())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
                            .length;
            if (sampleBytes > maximumSampleBytes) {
                throw new IOException("Stage 2 checkpoint/measurement storage cap exhausted");
            }
            samples.println(initialSample);
            if (samples.checkError()) {
                throw new IOException("Stage 2 measurement write failed");
            }
            System.out.println(
                    "STAGE2_CONFIG instrument="
                            + (run.light ? "production-light" : "production-v1")
                            + " admitPerSecond="
                            + run.admitPerSecond
                            + " bytes="
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
                            + " "
                            + limits.describe()
                            + " sampleBytes="
                            + maximumSampleBytes
                            + " jvmFlags="
                            + ManagementFactory.getRuntimeMXBean().getInputArguments());
            if (lease != null) {
                lease.requireWindow(Math.addExact(warmupMillis, measurementMillis));
            }
            long gcBefore = gcMillis();
            long cpuBefore = processCpuNanos();
            Map<Long, ThreadReading> threadsBefore = new java.util.HashMap<>();
            observeThreads(threadsBefore);
            Map<Long, ThreadReading> threadsSeen = new java.util.HashMap<>();
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
                                    warmupMillis + measurementMillis + limits.drainMillis);
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
                    observeThreads(threadsSeen);
                    String sample = job.checkpointStats();
                    String runtime = sampler.sample();
                    String progress = run.commits.sample(System.nanoTime());
                    String record =
                            "{\"elapsedNanos\":"
                                    + (now - started)
                                    + ",\"stagedEntries\":"
                                    + run.stagedTotal(0)
                                    + ",\"stagedBytes\":"
                                    + run.stagedTotal(1)
                                    + ",\"activeRequests\":"
                                    + run.active.get()
                                    + ",\"admittedInputs\":"
                                    + run.ledger.admittedCount()
                                    + ",\"acknowledgedInputs\":"
                                    + run.ledger.acknowledgedCount()
                                    + ",\"inputProgress\":"
                                    + run.ledger.sample()
                                    + ",\"commitProgress\":"
                                    + progress
                                    + ",\"notificationProgress\":"
                                    + run.notifications.sample(System.nanoTime())
                                    + ",\"runtime\":"
                                    + runtime
                                    + ",\"checkpoints\":"
                                    + sample
                                    + "}";
                    sampleBytes +=
                            (record + System.lineSeparator())
                                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)
                                    .length;
                    requireWorkWithinLimit(lease, directory, limits.workBytes);
                    if (sampleBytes > maximumSampleBytes) {
                        throw new IOException(
                                "Stage 2 checkpoint/measurement storage cap exhausted");
                    }
                    peakHeap =
                            Math.max(
                                    peakHeap,
                                    ManagementFactory.getMemoryMXBean()
                                            .getHeapMemoryUsage()
                                            .getUsed());
                    samples.println(record);
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
            long finished = System.nanoTime();
            long cpuAfter = processCpuNanos();
            observeThreads(threadsSeen);
            requireWorkWithinLimit(lease, directory, limits.workBytes);
            long gc = gcMillis() - gcBefore;
            System.out.println(
                    "STAGE2_PROCESS_CPU {\"elapsedNanos\":"
                            + (finished - started)
                            + ",\"processCpuNanos\":"
                            + (cpuBefore < 0 || cpuAfter < 0 ? -1 : cpuAfter - cpuBefore)
                            + ",\"processors\":"
                            + Runtime.getRuntime().availableProcessors()
                            + ",\"threadGroups\":"
                            + threadGroupTotals(
                                    threadsBefore, threadsSeen, reading -> reading.cpuNanos)
                            + ",\"committableSerializations\":"
                            + run.committableSerializations.get()
                            + ",\"committableDeserializations\":"
                            + run.committableDeserializations.get()
                            + ",\"threadGroupAllocatedBytes\":"
                            + threadGroupTotals(
                                    threadsBefore, threadsSeen, reading -> reading.allocatedBytes)
                            + "}");
            if (run.light) {
                System.out.println(
                        "STAGE2_LIGHT {\"acknowledged\":"
                                + run.lightAcknowledged.get()
                                + ",\"deduplicated\":"
                                + run.deduplicated.get()
                                + ",\"peakActive\":"
                                + run.peakActive.get()
                                + ",\"committerWaitNanos\":"
                                + run.committerWaitNanos.get()
                                + ",\"gcMillis\":"
                                + gc
                                + ",\"censored\":"
                                + run.censored.get()
                                + "}");
                System.out.println("STAGE2_CHECKPOINT_FINAL " + job.checkpointStats());
                System.out.println("STAGE2_COMMIT_FINAL " + run.commits.sample(System.nanoTime()));
                printCommitRecords(run);
                recordsPrinted = true;
                samples.flush();
                preserveSamples(lease, directory, false, maximumSampleBytes);
                // A match means the row already held this envelope's marker, so nothing was
                // written.
                return !run.censored.get()
                        && run.lightAcknowledged.get() > 0
                        && run.deduplicated.get() == 0;
            }
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
                        run.clientNanos.quantileUpperBound(.95),
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
            System.out.println("STAGE2_INPUT_FINAL " + run.ledger.sample());
            System.out.println("STAGE2_COMMIT_FINAL " + run.commits.sample(System.nanoTime()));
            printCommitRecords(run);
            recordsPrinted = true;
            System.out.println(
                    "STAGE2_NOTIFICATION_FINAL " + run.notifications.sample(System.nanoTime()));
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
            preserveSamples(lease, directory, false, maximumSampleBytes);
            return !run.censored.get() && run.ledger.measuredCount() > 0;
        } catch (Exception failure) {
            if (observedRun != null && !recordsPrinted) {
                // Includes a commit still running when the run failed, such as the one a checkpoint
                // expiry waited on, which the finished records cannot show.
                System.out.println(
                        "STAGE2_COMMIT_FINAL " + observedRun.commits.sample(System.nanoTime()));
                printCommitRecords(observedRun);
            }
            boolean workloadLimit = false;
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                String message = cause.getMessage();
                if (message != null
                        && (message.contains("Staging capacity exceeded")
                                || message.contains("Bigtable staging capacity exceeded")
                                || message.contains("budget exhausted")
                                || message.contains("storage cap exhausted"))) {
                    workloadLimit = true;
                }
            }
            System.out.println(
                    "STAGE2_TERMINAL " + (workloadLimit ? "CENSORED_WORKLOAD_LIMIT" : "FAILED"));
            try {
                preserveSamples(lease, directory, true, maximumSampleBytes);
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

    /**
     * Whether {@code -Dstage2.instrument} selects the lightweight instrument; {@code full} or unset
     * selects the full one and anything else fails. The lightweight send path needs a Bigtable
     * transport, so it is refused for the fake one.
     */
    static boolean lightInstrument(boolean transport) {
        String mode = System.getProperty("stage2.instrument", "full");
        if (!mode.equals("full") && !mode.equals("light")) {
            throw new IllegalArgumentException("stage2.instrument must be full or light: " + mode);
        }
        if (mode.equals("light") && !transport) {
            throw new IllegalArgumentException(
                    "stage2.instrument=light needs the emulator or a service transport");
        }
        return mode.equals("light");
    }

    /**
     * The average admission rate across readers from {@code -Dstage2.admitPerSecond}, 0 for
     * unpaced; a paced run gives every reader at least one input per second.
     */
    static long admitPerSecond(int parallelism) {
        String value = System.getProperty("stage2.admitPerSecond", "0");
        long rate;
        try {
            rate = Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "stage2.admitPerSecond must be a whole number: " + value, failure);
        }
        if (rate < 0 || (rate > 0 && rate < parallelism)) {
            throw new IllegalArgumentException(
                    "stage2.admitPerSecond must be 0 or at least the parallelism: " + rate);
        }
        return rate;
    }

    /** Sets the run's instrument mode and admission rate before any reader or writer exists. */
    private static Stage2Harness instrumented(
            Stage2Harness run, boolean light, long admitPerSecond) {
        run.light = light;
        run.admitPerSecond = admitPerSecond;
        return run;
    }

    /**
     * One thread's name, CPU time and allocated bytes, each -1 where the JVM does not measure it.
     */
    static final class ThreadReading {
        final String name;
        final long cpuNanos;
        final long allocatedBytes;

        ThreadReading(String name, long cpuNanos, long allocatedBytes) {
            this.name = name;
            this.cpuNanos = cpuNanos;
            this.allocatedBytes = allocatedBytes;
        }
    }

    /**
     * Reads every live thread into {@code seen}, replacing an earlier reading of the same thread,
     * so a thread that has ended keeps its last one.
     */
    static void observeThreads(Map<Long, ThreadReading> seen) {
        var bean = ManagementFactory.getThreadMXBean();
        var allocation =
                bean instanceof com.sun.management.ThreadMXBean
                                && ((com.sun.management.ThreadMXBean) bean)
                                        .isThreadAllocatedMemoryEnabled()
                        ? (com.sun.management.ThreadMXBean) bean
                        : null;
        boolean cpu = bean.isThreadCpuTimeSupported() && bean.isThreadCpuTimeEnabled();
        for (java.lang.management.ThreadInfo info : bean.getThreadInfo(bean.getAllThreadIds())) {
            if (info == null) {
                continue;
            }
            long id = info.getThreadId();
            seen.put(
                    id,
                    new ThreadReading(
                            info.getThreadName(),
                            cpu ? bean.getThreadCpuTime(id) : -1,
                            allocation == null ? -1 : allocation.getThreadAllocatedBytes(id)));
        }
    }

    /**
     * The growth of one per-thread value since {@code before} for every thread {@code seen}, summed
     * by thread name with digits folded, as a JSON object. A thread that ended counts up to its
     * last reading, so a group can understate its share by up to one sampling interval; a thread
     * without the value is left out.
     */
    static String threadGroupTotals(
            Map<Long, ThreadReading> before,
            Map<Long, ThreadReading> seen,
            java.util.function.ToLongFunction<ThreadReading> value) {
        Map<String, Long> groups = new java.util.TreeMap<>();
        seen.forEach(
                (id, reading) -> {
                    long now = value.applyAsLong(reading);
                    if (now < 0) {
                        return;
                    }
                    ThreadReading earlier = before.get(id);
                    long then = earlier == null ? 0 : Math.max(0, value.applyAsLong(earlier));
                    String group =
                            reading.name
                                    .replaceAll("[0-9]+", "N")
                                    .replaceAll("[^A-Za-z0-9 ._:#()>/-]", "_");
                    groups.merge(group, now - then, Long::sum);
                });
        StringBuilder json = new StringBuilder("{");
        groups.forEach(
                (name, total) -> {
                    if (json.length() > 1) {
                        json.append(',');
                    }
                    json.append('"').append(name).append("\":").append(total);
                });
        return json.append('}').toString();
    }

    /** This JVM's CPU time, or -1 where the platform does not report it. */
    static long processCpuNanos() {
        var bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) bean).getProcessCpuTime();
        }
        return -1;
    }

    /**
     * Prints one record per finished commit invocation and the run's latency distributions. The
     * client round trips cover the measurement window only; the completion hold covers every
     * successful completion of the run; a failed RPC records none. The peak of outstanding
     * conditional writes is here as well as on the {@code STAGE2} line, which a failed or censored
     * run does not print.
     */
    private static void printCommitRecords(Stage2Harness run) {
        for (String batch : run.commits.finishedRecords()) {
            System.out.println("STAGE2_COMMIT_BATCH " + batch);
        }
        System.out.println(
                "STAGE2_CLIENT_FINAL {\"droppedBatchRecords\":"
                        + run.commits.droppedRecords()
                        + ",\"peakActive\":"
                        + run.peakActive.get()
                        + ",\"measuredClientNanos\":"
                        + run.clientNanos.json()
                        + ",\"completionHoldNanos\":"
                        + run.completionHoldNanos.json()
                        + "}");
    }

    static void preserveSamples(
            Stage2RunLease lease, Path directory, boolean failed, long maximumSampleBytes)
            throws IOException {
        Path source = directory.resolve("samples.jsonl");
        if (!Files.isRegularFile(source, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            if (failed) {
                return;
            }
            throw new IOException("Stage 2 measurement file is missing");
        }
        if (Files.size(source) > maximumSampleBytes) {
            throw new IOException("Stage 2 sample evidence exceeds its storage cap");
        }
        Path evidence =
                (lease == null ? directory.getParent() : lease.evidenceDirectory())
                        .resolve(
                                directory.getFileName()
                                        + (failed ? "-failed" : "")
                                        + "-samples.jsonl");
        Files.copy(source, evidence);
        System.out.println("STAGE2_SAMPLES " + evidence);
    }

    static void requireWorkWithinLimit(Stage2RunLease lease, Path directory, long bytes)
            throws IOException {
        if (directorySize(lease == null ? directory : lease.workRoot()) > bytes) {
            throw new IOException("Stage 2 checkpoint/measurement storage cap exhausted");
        }
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
        Stage2Harness.readMetadata(lease.table("staged-r1"), LocalStagedHarness.PROFILE);
        for (String profile : new String[] {"no-tx", "multi-cluster"}) {
            boolean rejected = false;
            try {
                Stage2Harness.readMetadata(lease.table("staged-r1"), profile);
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
                Stage2Harness.readMetadata(lease.table(table), LocalStagedHarness.PROFILE);
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
}
