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

import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;

/** One fresh-JVM formal observation on an already supervised retained trial instance. */
final class Stage2CampaignWorker {
    private Stage2CampaignWorker() {}

    static void execute(Path directory, String table) throws Exception {
        Stage2CampaignJournal journal = new Stage2CampaignJournal(directory);
        execute(
                journal,
                table,
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments()),
                Stage2CampaignWorker::observe);
    }

    interface Observation {
        boolean run(Stage2RunLease lease, Stage2AssessmentPlan.Run run, Stage2RunLimits limits)
                throws Exception;
    }

    static void execute(
            Stage2CampaignJournal journal,
            String table,
            long pid,
            String started,
            String jvmFlags,
            Observation observation)
            throws Exception {
        Stage2AssessmentPlan.Run run = Stage2AssessmentPlan.run(table);
        if (!journal.inputs.getProperty("jvmFlags").equals(jvmFlags)) {
            throw new IOException("Worker JVM flags differ from the frozen campaign inputs");
        }
        Stage2RunLease lease = journal.claim(table, pid, started);
        boolean observed;
        try {
            observed = observation.run(lease, run, journal.limits);
        } catch (InterruptedException interruption) {
            stopOnInterruption(journal, interruption);
            throw interruption;
        } catch (Exception | Error failure) {
            boolean recorded =
                    recordOrStop(
                            journal, () -> journal.finish(table, pid, started, false), failure);
            System.out.println(
                    "STAGE2_CAMPAIGN_WORKER " + table + (recorded ? " FAILED" : " UNRECORDED"));
            throw failure;
        }
        recordOrStop(journal, () -> journal.finish(table, pid, started, observed), null);
        if (!observed) {
            System.out.println("STAGE2_CAMPAIGN_WORKER " + table + " FAILED");
            throw new IOException(
                    "Formal observation was empty or censored; recorded as FAILED for " + table);
        }
        System.out.println("STAGE2_CAMPAIGN_WORKER " + table + " OBSERVED");
    }

    /**
     * An interrupted observation is not a measured failure of the workload: the protocol stops the
     * campaign on an interruption rather than recording the run and moving on.
     */
    private static void stopOnInterruption(
            Stage2CampaignJournal journal, InterruptedException interruption) {
        // Publish the stop before restoring the interrupt flag: the journal's file channels are
        // interruptible and would refuse the publication on an already interrupted thread.
        try {
            journal.stop();
        } catch (IOException | RuntimeException stopFailure) {
            interruption.addSuppressed(stopFailure);
        }
        Thread.currentThread().interrupt();
    }

    /**
     * Records the run's outcome and returns whether it was recorded. A recorded failure lets the
     * campaign proceed; a failure to record the outcome stops the campaign, because an unrecorded
     * run cannot be told apart from a lost one. With a {@code failure} the recording error is
     * attached to it as suppressed; without one the recording error is thrown after the stop.
     */
    private static boolean recordOrStop(
            Stage2CampaignJournal journal,
            ThrowingRunnable<IOException> recording,
            Throwable failure)
            throws IOException {
        try {
            recording.run();
            return true;
        } catch (IOException | RuntimeException | Error recordingFailure) {
            try {
                journal.stop();
            } catch (IOException | RuntimeException stopFailure) {
                recordingFailure.addSuppressed(stopFailure);
            }
            if (failure == null) {
                throw recordingFailure;
            }
            failure.addSuppressed(recordingFailure);
            return false;
        }
    }

    interface AuxiliaryObservation {
        boolean run(Stage2RunLease lease, Stage2AuxiliaryPlan.Phase phase) throws Exception;
    }

    static void executeAuxiliary(Path directory, String name) throws Exception {
        executeAuxiliary(
                new Stage2CampaignJournal(directory),
                name,
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments()),
                (lease, phase) -> observeAuxiliary(lease, phase, lease.workDirectory()));
    }

    static boolean observeAuxiliary(
            Stage2RunLease lease, Stage2AuxiliaryPlan.Phase phase, Path work) throws Exception {
        if (phase.name.equals("sustained")) {
            return BigtableStage2Probe.sustainedHotRun(
                    lease,
                    phase.table(),
                    work,
                    1024,
                    1,
                    4,
                    1000,
                    Stage2AuxiliaryPlan.WARMUP_SECONDS * 1000,
                    phase.measurementSeconds * 1000,
                    phase.limits);
        }
        return BigtableStage2Probe.timedRun(
                lease,
                phase.table(),
                work,
                true,
                false,
                lease == null ? "127.0.0.1:1" : "",
                1024,
                1,
                1,
                1000,
                Stage2AuxiliaryPlan.WARMUP_SECONDS * 1000,
                phase.measurementSeconds * 1000,
                phase.limits.inventoryEntries,
                0,
                false,
                phase.limits);
    }

    static void executeAuxiliary(
            Stage2CampaignJournal journal,
            String name,
            long pid,
            String started,
            String jvmFlags,
            AuxiliaryObservation observation)
            throws Exception {
        if (!journal.inputs.getProperty("jvmFlags").equals(jvmFlags)) {
            throw new IOException("Worker JVM flags differ from the frozen campaign inputs");
        }
        var phase = journal.auxiliaryPhase(name);
        Stage2RunLease lease = journal.claimAuxiliary(name, pid, started);
        boolean observed;
        try {
            observed = observation.run(lease, phase);
        } catch (InterruptedException interruption) {
            stopOnInterruption(journal, interruption);
            throw interruption;
        } catch (Exception | Error failure) {
            boolean recorded =
                    recordOrStop(
                            journal,
                            () -> journal.finishAuxiliary(name, pid, started, false),
                            failure);
            System.out.println(
                    "STAGE2_AUXILIARY_WORKER " + name + (recorded ? " FAILED" : " UNRECORDED"));
            throw failure;
        }
        recordOrStop(journal, () -> journal.finishAuxiliary(name, pid, started, observed), null);
        if (!observed) {
            System.out.println("STAGE2_AUXILIARY_WORKER " + name + " FAILED");
            throw new IOException(
                    "Auxiliary observation was empty or censored; recorded as FAILED for " + name);
        }
        System.out.println("STAGE2_AUXILIARY_WORKER " + name + " OBSERVED");
    }

    private static boolean observe(
            Stage2RunLease lease, Stage2AssessmentPlan.Run run, Stage2RunLimits limits)
            throws Exception {
        var cell = run.cell;
        return BigtableStage2Probe.timedRun(
                lease,
                run.table(),
                lease.workDirectory(),
                run.staged,
                false,
                "",
                cell.payloadBytes,
                cell.parallelism,
                cell.inFlight,
                cell.checkpointSeconds * 1000L,
                cell.warmupSeconds() * 1000L,
                cell.measurementSeconds() * 1000L,
                limits.inventoryEntries,
                0,
                cell.hot,
                limits);
    }
}
