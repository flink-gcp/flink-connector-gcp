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
        try {
            boolean observed = observation.run(lease, run, journal.limits);
            journal.finish(table, pid, started, observed);
            if (!observed) {
                throw new IOException("Formal observation was empty or censored; campaign stopped");
            }
            System.out.println("STAGE2_CAMPAIGN_WORKER " + table + " OBSERVED");
        } catch (Exception | Error failure) {
            try {
                journal.stop();
            } catch (IOException | RuntimeException stopFailure) {
                failure.addSuppressed(stopFailure);
            }
            throw failure;
        }
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
