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
import java.io.StringReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.List;
import java.util.Properties;

/** Durable single-host admission journal; resource ownership is established by the supervisor. */
final class Stage2CampaignJournal {
    private static final long HEARTBEAT_MILLIS = 20_000;
    final Path directory;
    final Properties inputs;
    final Stage2RunLimits limits;
    final Stage2AuxiliaryPlan auxiliary;
    private final Properties plan;
    private final Clock clock;
    private final String planSha256;

    private interface Mutation {
        void accept(Properties state) throws IOException;
    }

    Stage2CampaignJournal(Path directory) throws IOException {
        this(directory, Clock.systemUTC());
    }

    Stage2CampaignJournal(Path directory, Clock clock) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();
        this.clock = clock;
        byte[] planBytes = Files.readAllBytes(this.directory.resolve("campaign.properties"));
        plan = properties(planBytes);
        planSha256 = Stage2CampaignPlan.sha256(planBytes);
        java.util.Map<String, byte[]> snapshots = new java.util.HashMap<>();
        for (String name :
                List.of("inputs.properties", "limits.properties", "matrix.csv", "leases.csv")) {
            byte[] snapshot = Files.readAllBytes(this.directory.resolve(name));
            snapshots.put(name, snapshot);
            if (!Stage2CampaignPlan.sha256(snapshot).equals(plan.getProperty(name + ".sha256"))) {
                throw new IOException("Campaign input digest mismatch: " + name);
            }
        }
        inputs = Stage2CampaignPlan.read(snapshots.get("inputs.properties"));
        limits = Stage2RunLimits.read(snapshots.get("limits.properties"));
        if (!"PREPARATION_ONLY".equals(plan.getProperty("state"))
                || !"VERIFIED_BIGTABLE_FREE_TRIAL_REQUIRED"
                        .equals(plan.getProperty("pricingCondition"))
                || !"648".equals(plan.getProperty("runs"))) {
            throw new IOException("Not a retained trial campaign plan");
        }
        auxiliary = Stage2AuxiliaryPlan.read(this.directory, inputs);
        validateCapacity();
    }

    private void validateCapacity() throws IOException {
        long entries = limits.inventoryEntries;
        long largestPayload =
                Stage2AssessmentPlan.runs().stream()
                        .mapToLong(run -> run.cell.payloadBytes)
                        .max()
                        .orElseThrow();
        long readBytes = Math.multiplyExact(entries, largestPayload + 1024);
        if (number(inputs, "maxWriteAttemptsPerRun") < Math.multiplyExact(entries, 4)
                || number(inputs, "maxWriteBytesPerRun") < Math.multiplyExact(readBytes, 4)
                || number(inputs, "maxReadBytesPerRun") < readBytes) {
            throw new IOException("Per-run reservations do not cover the full matrix inventory");
        }
        if (limits.workBytes < Math.multiplyExact(entries, 6 * 64L)) {
            throw new IOException("Campaign work cap cannot retain the six cell inventories");
        }
    }

    // Called only after an independent supervisor verifies the exact instance, trial and host.
    void start(String owner, long supervisorPid, String supervisorStart, long hostStartedAt)
            throws IOException {
        if (!owner.matches("[a-f0-9]{32}") || supervisorPid <= 0 || supervisorStart.isBlank()) {
            throw new IllegalArgumentException("Missing campaign owner or supervisor identity");
        }
        if (supervisorPid == ProcessHandle.current().pid()) {
            throw new IllegalArgumentException("Campaign supervisor must be a different process");
        }
        validatePlan();
        long now = clock.millis();
        long deadline = Math.addExact(hostStartedAt, Math.multiplyExact(hostBoundSeconds(), 1000));
        if (hostStartedAt <= 0 || hostStartedAt > now || now >= deadline) {
            throw new IOException("Host creation time is invalid or its reservation has expired");
        }
        Properties state = new Properties();
        state.setProperty("phase", "READY");
        state.setProperty("owner", owner);
        state.setProperty("supervisorPid", Long.toString(supervisorPid));
        state.setProperty("supervisorStart", supervisorStart);
        state.setProperty("controllerPid", Long.toString(ProcessHandle.current().pid()));
        state.setProperty(
                "controllerStart",
                ProcessHandle.current().info().startInstant().orElseThrow().toString());
        state.setProperty("startedAt", Long.toString(hostStartedAt));
        state.setProperty("activatedAt", Long.toString(now));
        state.setProperty("deadline", Long.toString(deadline));
        state.setProperty("heartbeat", Long.toString(now));
        state.setProperty("nextRun", "0");
        state.setProperty("nextAuxiliary", "0");
        state.setProperty("auxiliarySha256", auxiliary == null ? "" : auxiliary.sha256);
        state.setProperty("cell", "-1");
        state.setProperty("planSha256", planSha256);
        try (FileChannel channel =
                        FileChannel.open(
                                directory.resolve("state.lock"),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            if (Files.exists(directory.resolve("state.properties"))
                    || Files.exists(directory.resolve("stop"))) {
                throw new java.nio.file.FileAlreadyExistsException(
                        "Campaign already activated or stopped");
            }
            // The durable marker prevents retry even if the initial snapshot is interrupted.
            Files.writeString(
                    directory.resolve("activation.started"),
                    planSha256,
                    StandardOpenOption.CREATE_NEW);
            force(directory.resolve("activation.started"));
            force(directory);
            writeNew(directory.resolve("state.initial"), state);
            Files.move(
                    directory.resolve("state.initial"),
                    directory.resolve("state.properties"),
                    StandardCopyOption.ATOMIC_MOVE);
            force(directory);
        }
    }

    long hostBoundSeconds() {
        return number(plan, "hostBoundSeconds");
    }

    private void validatePlan() throws IOException {
        Path scratch = Files.createTempDirectory("stage2-plan-validation-");
        Path generated = scratch.resolve("plan");
        try {
            Stage2CampaignPlan.write(
                    directory.resolve("inputs.properties"),
                    directory.resolve("limits.properties"),
                    generated);
            Properties expected =
                    properties(Files.readAllBytes(generated.resolve("campaign.properties")));
            if (!plan.equals(expected)) {
                throw new IOException(
                        "Campaign reservation differs from the complete validated plan");
            }
        } finally {
            if (Files.isDirectory(generated)) {
                for (String name :
                        List.of(
                                "inputs.properties",
                                "limits.properties",
                                "matrix.csv",
                                "leases.csv",
                                "campaign.properties")) {
                    Files.deleteIfExists(generated.resolve(name));
                }
                Files.delete(generated);
            }
            Files.delete(scratch);
        }
    }

    Properties read() throws IOException {
        Properties state = properties(Files.readAllBytes(directory.resolve("state.properties")));
        if (!planSha256.equals(state.getProperty("planSha256"))) {
            throw new IOException("Campaign plan changed after activation");
        }
        if (!(auxiliary == null ? "" : auxiliary.sha256)
                .equals(state.getProperty("auxiliarySha256", ""))) {
            throw new IOException("Auxiliary plan changed after activation");
        }
        return state;
    }

    private void update(Mutation mutation) throws IOException {
        update(mutation, false);
    }

    private synchronized void update(Mutation mutation, boolean stopping) throws IOException {
        try (FileChannel channel =
                        FileChannel.open(
                                directory.resolve("state.lock"),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            Properties state;
            if (stopping && !Files.exists(directory.resolve("state.properties"))) {
                // Atomic initial publication means no worker could claim this campaign yet.
                state = new Properties();
                state.setProperty("planSha256", planSha256);
                state.setProperty("auxiliarySha256", auxiliary == null ? "" : auxiliary.sha256);
                archiveInterrupted(directory.resolve("state.initial"));
            } else {
                state = read();
            }
            mutation.accept(state);
            Path next = directory.resolve("state.next");
            // An interrupted publication cannot resume admission. Cleanup preserves it separately.
            if (stopping) {
                archiveInterrupted(next);
            }
            writeNew(next, state);
            Files.move(
                    next,
                    directory.resolve("state.properties"),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            force(directory);
        }
    }

    private void archiveInterrupted(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.move(
                    path,
                    directory.resolve(
                            "state.interrupted-" + java.util.UUID.randomUUID() + ".properties"),
                    StandardCopyOption.ATOMIC_MOVE);
            force(directory);
        }
    }

    private void record(String name, Properties state) throws IOException {
        writeNew(directory.resolve(name), state);
        force(directory);
    }

    private void live(Properties state) {
        long now = clock.millis();
        if (!List.of("READY", "PREPARING", "RUNNING", "RETAINING", "AUXILIARY_READY")
                        .contains(state.getProperty("phase"))
                || !sameProcessAlive(
                        number(state, "supervisorPid"), state.getProperty("supervisorStart"))
                || Files.exists(directory.resolve("stop"))
                || now >= number(state, "deadline")
                || (List.of("PREPARING", "RUNNING", "RETAINING")
                                .contains(state.getProperty("phase"))
                        && now >= number(state, "cellDeadline"))
                || now < number(state, "heartbeat")
                || now - number(state, "heartbeat") >= HEARTBEAT_MILLIS) {
            throw new IllegalStateException("Campaign stopped, expired or lost supervision");
        }
    }

    void heartbeat(long supervisorPid, String supervisorStart) throws IOException {
        // Recheck the frozen plan outside measured submissions. Workers bind their validated
        // constructor snapshot to state; a changed plan cannot be admitted by a new worker.
        if (!planSha256.equals(
                Stage2CampaignPlan.sha256(
                        Files.readAllBytes(directory.resolve("campaign.properties"))))) {
            throw new IOException("Campaign plan changed after activation");
        }
        byte[] auxiliarySnapshot = Stage2AuxiliaryPlan.snapshot(directory);
        String actualAuxiliary =
                auxiliarySnapshot == null ? "" : Stage2CampaignPlan.sha256(auxiliarySnapshot);
        if (!(auxiliary == null ? "" : auxiliary.sha256).equals(actualAuxiliary)) {
            throw new IOException("Auxiliary plan changed after activation");
        }
        update(
                state -> {
                    live(state);
                    if (supervisorPid != number(state, "supervisorPid")
                            || !supervisorStart.equals(state.getProperty("supervisorStart"))) {
                        throw new IllegalStateException("Different campaign supervisor");
                    }
                    if (!sameProcessAlive(
                            number(state, "controllerPid"), state.getProperty("controllerStart"))) {
                        throw new IllegalStateException("Campaign controller disappeared");
                    }
                    if ("STARTED".equals(state.getProperty("runStatus"))
                            && (!sameProcessAlive(
                                            number(state, "workerPid"),
                                            state.getProperty("workerStart"))
                                    || clock.millis() >= number(state, "workerDeadline"))) {
                        throw new IllegalStateException("Campaign worker disappeared or expired");
                    }
                    state.setProperty("heartbeat", Long.toString(clock.millis()));
                });
    }

    /** Records the cell before creating any of its tables; it cannot be retried after a crash. */
    void prepareCell(int cell) throws IOException {
        update(
                state -> {
                    live(state);
                    requireController(state);
                    if (!state.getProperty("phase").equals("READY")
                            || cell < 0
                            || cell >= 108
                            || number(state, "nextRun") != cell * 6L
                            || number(state, "cell") != cell - 1L) {
                        throw new IllegalStateException("Cell is not the next unclaimed cell");
                    }
                    var run = Stage2AssessmentPlan.runs().get(cell * 6);
                    long seconds =
                            Math.addExact(
                                    6L * (run.cell.warmupSeconds() + run.cell.measurementSeconds()),
                                    Math.addExact(
                                            Math.multiplyExact(
                                                    6, number(inputs, "runOverheadSeconds")),
                                            number(inputs, "leaseOverheadSeconds")));
                    long end =
                            Math.addExact(
                                    clock.millis(),
                                    Math.multiplyExact(Math.max(60, seconds), 1000));
                    if (end > number(state, "deadline")
                            || seconds > number(inputs, "leaseLimitSeconds")) {
                        throw new IllegalStateException(
                                "Full cell does not fit the reserved lease");
                    }
                    state.setProperty("cell", Integer.toString(cell));
                    state.setProperty("cellDeadline", Long.toString(end));
                    state.setProperty("phase", "PREPARING");
                });
    }

    void tablesReady() throws IOException {
        update(
                state -> {
                    live(state);
                    requireController(state);
                    if (!state.getProperty("phase").equals("PREPARING")) {
                        throw new IllegalStateException("No table creation is pending");
                    }
                    state.setProperty("phase", "RUNNING");
                });
    }

    Stage2AuxiliaryPlan.Phase auxiliaryPhase(String name) {
        if (auxiliary == null) {
            throw new IllegalStateException("No auxiliary phase reservation is frozen");
        }
        return auxiliary.phases.stream()
                .filter(phase -> phase.name.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown auxiliary phase"));
    }

    void prepareAuxiliary(String name) throws IOException {
        var phase = auxiliaryPhase(name);
        update(
                state -> {
                    live(state);
                    requireController(state);
                    int order = Math.toIntExact(number(state, "nextAuxiliary"));
                    if (!"AUXILIARY_READY".equals(state.getProperty("phase"))
                            || number(state, "nextRun") != 648
                            || number(state, "cell") != 107
                            || order >= 2
                            || !auxiliary.phases.get(order).name.equals(name)) {
                        throw new IllegalStateException(
                                "Auxiliary phase is repeated or out of order");
                    }
                    long end =
                            Math.addExact(
                                    clock.millis(), Math.multiplyExact(phase.boundSeconds, 1000));
                    if (end > number(state, "deadline")) {
                        throw new IllegalStateException(
                                "Auxiliary phase does not fit the host reservation");
                    }
                    state.setProperty("activeAuxiliary", name);
                    state.setProperty("cellDeadline", Long.toString(end));
                    state.setProperty("phase", "PREPARING");
                });
    }

    Stage2RunLease claimAuxiliary(String name, long pid, String processStart) throws IOException {
        var phase = auxiliaryPhase(name);
        update(
                state -> {
                    live(state);
                    if (!"RUNNING".equals(state.getProperty("phase"))
                            || !name.equals(state.getProperty("activeAuxiliary"))
                            || "STARTED".equals(state.getProperty("runStatus"))
                            || pid <= 0
                            || pid == number(state, "supervisorPid")
                            || pid == number(state, "controllerPid")
                            || processStart.isBlank()
                            || (state.containsKey("workerPid")
                                    && sameProcessAlive(
                                            number(state, "workerPid"),
                                            state.getProperty("workerStart")))) {
                        throw new IllegalStateException(
                                "Auxiliary worker is repeated or has a foreign process identity");
                    }
                    long end =
                            Math.addExact(
                                    clock.millis(), Math.multiplyExact(phase.runSeconds, 1000));
                    if (end > number(state, "cellDeadline")) {
                        throw new IllegalStateException(
                                "Full auxiliary run does not fit its phase reservation");
                    }
                    state.setProperty("runStatus", "STARTED");
                    state.setProperty("workerPid", Long.toString(pid));
                    state.setProperty("workerStart", processStart);
                    state.setProperty("workerDeadline", Long.toString(end));
                    state.setProperty("workerTable", phase.table());
                    state.setProperty("remainingAttempts", Long.toString(phase.writeAttempts));
                    state.setProperty("remainingWriteBytes", Long.toString(phase.writeBytes));
                    state.setProperty("remainingReadBytes", Long.toString(phase.readBytes));
                });
        return worker(phase.table(), pid, processStart, phase.limits);
    }

    void finishAuxiliary(String name, long pid, String processStart, boolean successful)
            throws IOException {
        var phase = auxiliaryPhase(name);
        update(
                state -> {
                    workerLive(state, phase.table(), pid, processStart);
                    if (!name.equals(state.getProperty("activeAuxiliary"))) {
                        throw new IllegalStateException("Different auxiliary completion");
                    }
                    state.setProperty("runStatus", successful ? "OBSERVED" : "FAILED");
                    record("auxiliary-" + name + "-run.properties", state);
                    state.setProperty("phase", successful ? "RETAINING" : "STOPPED");
                });
    }

    void auxiliaryCleaned(String name, String evidenceSha256) throws IOException {
        auxiliaryCleaned(name, evidenceSha256, Stage2Lease::removeWork);
    }

    void auxiliaryCleaned(String name, String evidenceSha256, WorkCleaner cleaner)
            throws IOException {
        var phase = auxiliaryPhase(name);
        Properties retained = read();
        requireRetainedCell(retained, evidenceSha256);
        if (!name.equals(retained.getProperty("activeAuxiliary"))) {
            throw new IllegalStateException("Different retained auxiliary phase");
        }
        // The recorded controller remains responsible for this unlocked deletion too.
        cleaner.remove(directory.resolve("work").resolve(phase.table()));
        update(
                state -> {
                    requireRetainedCell(state, evidenceSha256);
                    if (!name.equals(state.getProperty("activeAuxiliary"))) {
                        throw new IllegalStateException(
                                "Retained auxiliary phase changed during cleanup");
                    }
                    var evidence = new Properties();
                    evidence.setProperty("evidenceSha256", evidenceSha256);
                    evidence.setProperty("phaseName", name);
                    record("auxiliary-" + name + "-evidence.properties", evidence);
                    state.setProperty(
                            "nextAuxiliary", Long.toString(number(state, "nextAuxiliary") + 1));
                    state.remove("activeAuxiliary");
                    state.setProperty(
                            "phase",
                            number(state, "nextAuxiliary") == 2
                                    ? "CAMPAIGN_COMPLETE"
                                    : "AUXILIARY_READY");
                });
    }

    Stage2RunLease claim(String table, long pid, String processStart) throws IOException {
        Stage2AssessmentPlan.run(table);
        int order = -1;
        // Run descriptors are rebuilt, so compare their stable table names.
        List<Stage2AssessmentPlan.Run> runs = Stage2AssessmentPlan.runs();
        for (int index = 0; index < runs.size(); index++) {
            if (runs.get(index).table().equals(table)) {
                order = index;
                break;
            }
        }
        final int selected = order;
        update(
                state -> {
                    live(state);
                    if (!state.getProperty("phase").equals("RUNNING")
                            || selected != number(state, "nextRun")
                            || selected / 6 != number(state, "cell")
                            || "STARTED".equals(state.getProperty("runStatus"))
                            || pid <= 0
                            || pid == number(state, "supervisorPid")
                            || processStart.isBlank()) {
                        throw new IllegalStateException(
                                "Run is repeated, out of order or outside the cell");
                    }
                    if (state.containsKey("workerPid")
                            && sameProcessAlive(
                                    number(state, "workerPid"), state.getProperty("workerStart"))) {
                        throw new IllegalStateException("Previous worker is still alive");
                    }
                    var cell = runs.get(selected).cell;
                    long seconds =
                            Math.addExact(
                                    cell.warmupSeconds() + cell.measurementSeconds(),
                                    number(inputs, "runOverheadSeconds"));
                    long end = Math.addExact(clock.millis(), Math.multiplyExact(seconds, 1000));
                    if (end > number(state, "cellDeadline")) {
                        throw new IllegalStateException("Full run does not fit the reserved cell");
                    }
                    state.setProperty("runStatus", "STARTED");
                    state.setProperty("workerPid", Long.toString(pid));
                    state.setProperty("workerStart", processStart);
                    state.setProperty("workerDeadline", Long.toString(end));
                    state.setProperty("workerTable", table);
                    state.setProperty(
                            "remainingAttempts", inputs.getProperty("maxWriteAttemptsPerRun"));
                    state.setProperty(
                            "remainingWriteBytes", inputs.getProperty("maxWriteBytesPerRun"));
                    state.setProperty(
                            "remainingReadBytes", inputs.getProperty("maxReadBytesPerRun"));
                });
        return worker(table, pid, processStart);
    }

    private void workerLive(Properties state, String table, long pid, String processStart) {
        live(state);
        if (!state.getProperty("phase").equals("RUNNING")
                || !"STARTED".equals(state.getProperty("runStatus"))
                || !table.equals(state.getProperty("workerTable"))
                || pid != number(state, "workerPid")
                || !processStart.equals(state.getProperty("workerStart"))
                || clock.millis() >= number(state, "workerDeadline")
                || clock.millis() >= number(state, "cellDeadline")) {
            throw new IllegalStateException(
                    "Campaign worker is not active or its deadline expired");
        }
    }

    private Stage2RunLease worker(String name, long pid, String processStart) {
        return worker(name, pid, processStart, limits);
    }

    private Stage2RunLease worker(
            String name, long pid, String processStart, Stage2RunLimits workerLimits) {
        TableDestination target =
                TableDestination.of(
                        inputs.getProperty("project"), inputs.getProperty("instance"), name);
        return new Stage2RunLease() {
            @Override
            public void requireLive() throws IOException {
                try {
                    workerLive(read(), name, pid, processStart);
                } catch (IllegalStateException failure) {
                    throw new IOException(failure.getMessage(), failure);
                }
            }

            @Override
            public void requireTarget(TableDestination destination) throws IOException {
                requireLive();
                if (!target.equals(destination)) {
                    throw new IOException("Destination is not owned by this campaign worker");
                }
            }

            @Override
            public long reserve(long attempts, long bytes) throws IOException {
                update(
                        state -> {
                            workerLive(state, name, pid, processStart);
                            consume(state, "remainingAttempts", attempts);
                            consume(state, "remainingWriteBytes", bytes);
                        });
                return attempts;
            }

            @Override
            public void reserveRead(long bytes) throws IOException {
                update(
                        state -> {
                            workerLive(state, name, pid, processStart);
                            consume(state, "remainingReadBytes", bytes);
                        });
            }

            @Override
            public TableDestination table(String requested) throws IOException {
                TableDestination destination =
                        TableDestination.of(target.getProject(), target.getInstance(), requested);
                requireTarget(destination);
                return destination;
            }

            @Override
            public Path workDirectory() {
                return directory.resolve("work").resolve(name);
            }

            @Override
            public Path workRoot() {
                return directory.resolve("work");
            }

            @Override
            public Path evidenceDirectory() {
                return directory;
            }

            @Override
            public void requireWindow(long observationMillis) throws IOException {
                requireLive();
                long end =
                        Math.addExact(
                                clock.millis(),
                                Math.addExact(observationMillis, workerLimits.drainMillis));
                if (end > number(read(), "workerDeadline")) {
                    throw new IOException(
                            "Full observation and drain no longer fit the campaign worker");
                }
            }
        };
    }

    void finish(String table, long pid, String processStart, boolean successful)
            throws IOException {
        update(
                state -> {
                    workerLive(state, table, pid, processStart);
                    if (state.containsKey("activeAuxiliary")) {
                        throw new IllegalStateException(
                                "Auxiliary observation requires its separate outcome");
                    }
                    long order = number(state, "nextRun");
                    state.setProperty("runStatus", successful ? "OBSERVED" : "FAILED");
                    // Persist outcomes separately so measured liveness checks remain fixed-size.
                    record("run-" + order + ".properties", state);
                    if (!successful) {
                        state.setProperty("phase", "STOPPED");
                    } else {
                        state.setProperty("nextRun", Long.toString(order + 1));
                        if ((order + 1) % 6 == 0) {
                            state.setProperty("phase", "RETAINING");
                        }
                    }
                });
    }

    @FunctionalInterface
    interface WorkCleaner {
        void remove(Path work) throws IOException;
    }

    /** Called after retained metrics are saved and the exact six tables are verified absent. */
    void cellCleaned(String evidenceSha256) throws IOException {
        cellCleaned(evidenceSha256, Stage2Lease::removeWork);
    }

    void cellCleaned(String evidenceSha256, WorkCleaner cleaner) throws IOException {
        Properties retained = read();
        requireRetainedCell(retained, evidenceSha256);
        if (retained.containsKey("activeAuxiliary")) {
            throw new IllegalStateException("Auxiliary evidence requires its separate completion");
        }
        long cell = number(retained, "cell");
        int first = Math.toIntExact(cell * 6);
        // Keep checkpoint-tree I/O outside the publication lock so supervision can continue.
        for (var run : Stage2AssessmentPlan.runs().subList(first, first + 6)) {
            cleaner.remove(directory.resolve("work").resolve(run.table()));
        }
        update(
                state -> {
                    requireRetainedCell(state, evidenceSha256);
                    if (number(state, "cell") != cell) {
                        throw new IllegalStateException("Retained cell changed during cleanup");
                    }
                    Properties evidence = new Properties();
                    evidence.setProperty("evidenceSha256", evidenceSha256);
                    evidence.setProperty("cell", Long.toString(cell));
                    record("cell-" + cell + ".properties", evidence);
                    state.setProperty(
                            "phase",
                            number(state, "nextRun") == 648
                                    ? (auxiliary == null ? "MATRIX_COMPLETE" : "AUXILIARY_READY")
                                    : "READY");
                });
    }

    private void requireRetainedCell(Properties state, String evidenceSha256) {
        live(state);
        requireController(state);
        if (!state.getProperty("phase").equals("RETAINING")
                || !evidenceSha256.matches("[a-f0-9]{64}")
                || sameProcessAlive(number(state, "workerPid"), state.getProperty("workerStart"))) {
            throw new IllegalStateException("Cell evidence or worker termination is incomplete");
        }
    }

    private static void requireController(Properties state) {
        ProcessHandle current = ProcessHandle.current();
        if (current.pid() != number(state, "controllerPid")
                || !current.info()
                        .startInstant()
                        .orElseThrow()
                        .toString()
                        .equals(state.getProperty("controllerStart"))) {
            throw new IllegalStateException("Different campaign controller");
        }
    }

    void absent() throws IOException {
        update(
                state -> {
                    if (!"STOPPED".equals(state.getProperty("phase"))) {
                        throw new IllegalStateException(
                                "Workers must be stopped before recording absence");
                    }
                    state.setProperty("phase", "ABSENT");
                    state.setProperty("cleanedAt", clock.instant().toString());
                });
    }

    void stop() throws IOException {
        update(state -> state.setProperty("phase", "STOPPED"), true);
    }

    private static void consume(Properties state, String key, long amount) {
        long remaining = number(state, key);
        if (amount <= 0 || amount > remaining) {
            throw new IllegalStateException("Campaign worker budget exhausted: " + key);
        }
        state.setProperty(key, Long.toString(remaining - amount));
    }

    static boolean sameProcessAlive(long pid, String start) {
        return ProcessHandle.of(pid)
                .filter(ProcessHandle::isAlive)
                .flatMap(process -> process.info().startInstant())
                .map(instant -> instant.toString().equals(start))
                .orElse(false);
    }

    private static long number(Properties values, String key) {
        return Long.parseLong(values.getProperty(key));
    }

    private static Properties properties(byte[] bytes) throws IOException {
        Properties result = new Properties();
        result.load(new StringReader(new String(bytes, StandardCharsets.UTF_8)));
        return result;
    }

    private static void writeNew(Path path, Properties properties) throws IOException {
        try (var stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
            properties.store(stream, "Stage 2 campaign state");
        }
        force(path);
    }

    static void force(Path path) throws IOException {
        try (FileChannel channel =
                FileChannel.open(
                        path,
                        Files.isDirectory(path)
                                ? StandardOpenOption.READ
                                : StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}
