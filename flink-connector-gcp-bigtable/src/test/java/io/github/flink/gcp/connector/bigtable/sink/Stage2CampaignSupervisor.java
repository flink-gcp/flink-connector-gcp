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
import java.util.Properties;

/** Independent process supervision; cloud adapters must verify ownership again when deleting. */
final class Stage2CampaignSupervisor {
    interface Resources {
        void verifyOwned() throws Exception;

        void deleteOwnedAndVerifyAbsent() throws Exception;
    }

    interface Workers {
        void stopAndVerifyGone(long pid, String started) throws Exception;
    }

    private final Stage2CampaignJournal journal;
    private final Resources resources;
    private final Workers workers;

    Stage2CampaignSupervisor(Stage2CampaignJournal journal, Resources resources, Workers workers) {
        this.journal = journal;
        this.resources = resources;
        this.workers = workers;
    }

    /** Poll only after start returns; failed setup uses cleanup explicitly. */
    boolean tick() throws Exception {
        if (!Files.exists(journal.directory.resolve("activation.started"))) {
            throw new IllegalStateException("Supervisor polling requires campaign activation");
        }
        try {
            Properties state = journal.read();
            if ("ABSENT".equals(state.getProperty("phase"))) {
                return false;
            }
            resources.verifyOwned();
            journal.heartbeat(
                    ProcessHandle.current().pid(),
                    ProcessHandle.current().info().startInstant().orElseThrow().toString());
            return true;
        } catch (Exception failure) {
            try {
                cleanup();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    void cleanup() throws Exception {
        // Publish the stop condition before signalling any worker or touching cloud resources.
        if (!Files.exists(journal.directory.resolve("stop"))) {
            Files.writeString(
                    journal.directory.resolve("stop"),
                    "Campaign cleanup requested",
                    java.nio.file.StandardOpenOption.CREATE_NEW);
        }
        journal.stop();
        Properties state = journal.read();
        long pid = Long.parseLong(state.getProperty("workerPid", "0"));
        if (pid > 0) {
            workers.stopAndVerifyGone(pid, state.getProperty("workerStart"));
        }
        resources.deleteOwnedAndVerifyAbsent();
        Stage2Lease.removeWork(journal.directory.resolve("work"));
        journal.absent();
    }

    static void stopProcess(long pid, String started) throws Exception {
        if (!Stage2CampaignJournal.sameProcessAlive(pid, started)) {
            return;
        }
        if (pid == ProcessHandle.current().pid()) {
            throw new IllegalStateException("Supervisor refuses to terminate itself");
        }
        ProcessHandle process = ProcessHandle.of(pid).orElseThrow();
        Stage2Lease.stopWorker(
                started,
                new Stage2Lease.Worker() {
                    @Override
                    public String started() {
                        return process.info().startInstant().map(Object::toString).orElse("");
                    }

                    @Override
                    public void destroy(boolean forcibly) {
                        if (!Stage2CampaignJournal.sameProcessAlive(pid, started)) {
                            return;
                        }
                        if (forcibly) {
                            process.destroyForcibly();
                        } else {
                            process.destroy();
                        }
                    }

                    @Override
                    public void awaitExit(long seconds) throws Exception {
                        process.onExit().get(seconds, java.util.concurrent.TimeUnit.SECONDS);
                    }
                });
        if (Stage2CampaignJournal.sameProcessAlive(pid, started)) {
            throw new IllegalStateException("Original campaign worker is still alive");
        }
    }
}
