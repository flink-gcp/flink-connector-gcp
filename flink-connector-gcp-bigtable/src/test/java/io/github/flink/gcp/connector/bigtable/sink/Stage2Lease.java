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

import com.google.api.gax.rpc.NotFoundException;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.CreateAppProfileRequest;
import com.google.bigtable.admin.v2.CreateInstanceRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Instance;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.ListInstancesResponse;
import com.google.bigtable.admin.v2.LocationName;
import com.google.bigtable.admin.v2.ProjectName;
import com.google.bigtable.admin.v2.StorageType;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.github.flink.gcp.connector.bigtable.BigtableAdminProtos;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Exact-resource lease journal. The parent process supervises it independently of experiment JVMs.
 */
final class Stage2Lease {
    private static final String PROJECT = "flink-gcp";

    static final List<String> TABLES =
            List.of(
                    "bulk-r1",
                    "staged-r1",
                    "staged-r2",
                    "bulk-r2",
                    "bulk-r3",
                    "staged-r3",
                    "hot",
                    "recovery-flink1",
                    "recovery-flink2",
                    "serialized",
                    "marker-missing",
                    "marker-gc",
                    "marker-typed");
    static final List<String> PRODUCTION_RECOVERY_TABLES =
            List.of(
                    "recovery-datastream-flink1",
                    "recovery-table-flink1",
                    "recovery-datastream-flink2",
                    "recovery-table-flink2",
                    "marker-missing",
                    "marker-gc",
                    "marker-typed");
    final List<String> tables;
    final boolean productionRecovery;
    final Path manifest;
    final String instance;
    final String token;
    final Path work;

    Stage2Lease(Path manifest) throws IOException {
        this.manifest = manifest.toAbsolutePath().normalize();
        Properties properties = read();
        instance = properties.getProperty("instance", "");
        token = properties.getProperty("owner", "");
        if (!instance.matches("flink-s2-[0-9]{10}-[a-f0-9]{6}")
                || !token.matches("[a-f0-9]{32}")
                || !properties.getProperty("project", "").equals(PROJECT)
                || !properties.getProperty("zone", "").equals("us-central1-b")) {
            throw new IOException("Invalid or unsupported Stage 2 authorization manifest");
        }
        String profile = properties.getProperty("profile", "bounded-experiment");
        productionRecovery = profile.equals("production-recovery");
        boolean retiredDiagnostic = profile.equals("diagnostic-seven-cell-v1");
        if (!productionRecovery && !retiredDiagnostic && !profile.equals("bounded-experiment")) {
            throw new IOException("Unknown Stage 2 lease profile");
        }
        // Retired manifests authorize no targets; cleanup uses only the exact instance and owner.
        tables =
                retiredDiagnostic
                        ? List.of()
                        : productionRecovery ? PRODUCTION_RECOVERY_TABLES : TABLES;
        if (!retiredDiagnostic
                && !properties.getProperty("tables", "").equals(String.join(",", tables))) {
            throw new IOException("Lease targets differ from the fixed profile");
        }
        work = this.manifest.getParent().resolve("work");
    }

    static Stage2Lease plan(Path manifest) throws IOException {
        return plan(manifest, false);
    }

    static Stage2Lease plan(Path manifest, boolean productionRecovery) throws IOException {
        Files.createDirectories(manifest.toAbsolutePath().getParent());
        Properties properties = new Properties();
        String owner = UUID.randomUUID().toString().replace("-", "");
        properties.setProperty("owner", owner);
        properties.setProperty(
                "instance",
                "flink-s2-" + Instant.now().getEpochSecond() + "-" + owner.substring(0, 6));
        properties.setProperty("project", PROJECT);
        properties.setProperty("zone", "us-central1-b");
        properties.setProperty("phase", "PLANNED");
        properties.setProperty("remainingAttempts", "250000");
        properties.setProperty("remainingWriteBytes", Long.toString(1L << 30));
        properties.setProperty("remainingReadBytes", Long.toString(1L << 30));
        properties.setProperty(
                "profile", productionRecovery ? "production-recovery" : "bounded-experiment");
        properties.setProperty(
                "tables",
                String.join(",", productionRecovery ? PRODUCTION_RECOVERY_TABLES : TABLES));
        if (productionRecovery) {
            properties.setProperty("remainingAttempts", "8192");
            properties.setProperty("remainingWriteBytes", "16777216");
            properties.setProperty("remainingReadBytes", "67108864");
        }
        try (var output = Files.newOutputStream(manifest, StandardOpenOption.CREATE_NEW)) {
            properties.store(output, "Stage 2 exact-resource authorization; no credentials");
        }
        forceFile(manifest);
        forceDirectory(manifest.toAbsolutePath().getParent());
        return new Stage2Lease(manifest);
    }

    private Properties read() throws IOException {
        Properties properties = new Properties();
        try (var input = Files.newInputStream(manifest)) {
            properties.load(input);
        }
        return properties;
    }

    void update(Consumer<Properties> mutation) throws IOException {
        try (FileChannel channel =
                        FileChannel.open(
                                manifest.resolveSibling("lease.lock"),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            Properties properties = read();
            mutation.accept(properties);
            Path next = manifest.resolveSibling("lease.next");
            try (var output = Files.newOutputStream(next)) {
                properties.store(output, "Stage 2 lease");
            }
            try (FileChannel output = FileChannel.open(next, StandardOpenOption.WRITE)) {
                output.force(true);
            }
            Files.move(
                    next,
                    manifest,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(manifest.getParent());
        }
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    long reserve(long attempts, long bytes) throws IOException {
        requireLive();
        update(
                properties -> {
                    long remaining = Long.parseLong(properties.getProperty("remainingAttempts"));
                    long remainingBytes =
                            Long.parseLong(properties.getProperty("remainingWriteBytes"));
                    if (attempts <= 0
                            || bytes <= 0
                            || attempts > remaining
                            || bytes > remainingBytes) {
                        throw new IllegalStateException(
                                "Aggregate Stage 2 operation/storage budget exhausted");
                    }
                    properties.setProperty(
                            "remainingAttempts", Long.toString(remaining - attempts));
                    properties.setProperty(
                            "remainingWriteBytes", Long.toString(remainingBytes - bytes));
                });
        return attempts;
    }

    void reserveRead(long bytes) throws IOException {
        update(
                properties -> {
                    long remaining = Long.parseLong(properties.getProperty("remainingReadBytes"));
                    if (bytes < 0 || bytes > remaining) {
                        throw new IllegalStateException("Aggregate readback cap exhausted");
                    }
                    properties.setProperty("remainingReadBytes", Long.toString(remaining - bytes));
                });
    }

    long startedAt() throws IOException {
        String started = read().getProperty("startedAt");
        if (started == null) {
            throw new IOException("Stage 2 lease has never started creation");
        }
        return Long.parseLong(started);
    }

    void requireLive() throws IOException {
        Properties properties = read();
        if (!properties.getProperty("phase").equals("ACTIVE")
                || System.currentTimeMillis()
                        >= Long.parseLong(properties.getProperty("startedAt", "0"))
                                + 50 * 60_000L) {
            throw new IOException("Stage 2 lease is not active or its execution deadline expired");
        }
    }

    void requireTarget(TableDestination table) throws IOException {
        requireLive();
        if (!table.getProject().equals(PROJECT)
                || !table.getInstance().equals(instance)
                || !tables.contains(table.getTable())) {
            throw new IOException("Destination is not owned by this Stage 2 lease");
        }
    }

    TableDestination table(String name) throws IOException {
        TableDestination table = TableDestination.of(PROJECT, instance, name);
        requireTarget(table);
        return table;
    }

    private void rejectRetiredDiagnostic() throws IOException {
        if (tables.isEmpty()
                || read().getProperty("profile", "").equals("diagnostic-seven-cell-v1")) {
            throw new IOException(
                    "The seven-cell diagnostic profile is retired; cleanup remains available");
        }
    }

    void claim(String table) throws IOException {
        rejectRetiredDiagnostic();
        requireTarget(TableDestination.of(PROJECT, instance, table));
        if (System.currentTimeMillis() >= startedAt() + 45 * 60_000L) {
            throw new IOException("Stage 2 admission deadline expired");
        }
        update(
                properties -> {
                    String key = "run." + table;
                    if (properties.containsKey(key)) {
                        throw new IllegalStateException("Repetition already claimed: " + table);
                    }
                    if (productionRecovery) {
                        int cell = tables.indexOf(table);
                        if (cell < 0 || cell >= 4) {
                            throw new IllegalStateException(
                                    "Only the four recovery cells may claim workers");
                        }
                        for (int earlier = 0; earlier < cell; earlier++) {
                            if (!properties
                                    .getProperty("run." + tables.get(earlier), "")
                                    .equals("PASS")) {
                                throw new IllegalStateException(
                                        "Previous production recovery cell has not passed");
                            }
                        }
                        if (cell > 0
                                && ProcessHandle.of(
                                                Long.parseLong(
                                                        properties.getProperty("workerPid", "0")))
                                        .filter(
                                                process ->
                                                        process.info()
                                                                .startInstant()
                                                                .map(Object::toString)
                                                                .orElse("")
                                                                .equals(
                                                                        properties.getProperty(
                                                                                "workerStarted",
                                                                                "")))
                                        .isPresent()) {
                            throw new IllegalStateException(
                                    "Previous production worker is still running");
                        }
                    }
                    properties.setProperty(key, "STARTED");
                    properties.setProperty(
                            "workerPid", Long.toString(ProcessHandle.current().pid()));
                    properties.setProperty(
                            "workerStarted",
                            ProcessHandle.current().info().startInstant().orElseThrow().toString());
                });
    }

    void create() throws Exception {
        rejectRetiredDiagnostic();
        if (!read().getProperty("phase").equals("PLANNED")) {
            throw new IOException("Lease has already attempted creation");
        }
        if (productionRecovery) {
            Properties properties = read();
            long supervisor = Long.parseLong(properties.getProperty("supervisorPid", "0"));
            if (supervisor <= 0
                    || supervisor == ProcessHandle.current().pid()
                    || ProcessHandle.of(supervisor)
                            .filter(
                                    process ->
                                            process.info()
                                                    .startInstant()
                                                    .map(Object::toString)
                                                    .orElse("")
                                                    .equals(
                                                            properties.getProperty(
                                                                    "supervisorStarted", "")))
                            .isEmpty()) {
                throw new IOException("Independent production lease supervisor is not running");
            }
        }
        try (BigtableInstanceAdminClient admin = BigtableInstanceAdminClient.create(PROJECT)) {
            try {
                admin.getBaseClient().getInstance(instanceName());
                throw new IOException(
                        "Pre-existing instance collision; it must not be reused or deleted");
            } catch (NotFoundException absent) {
                update(
                        properties -> {
                            properties.setProperty("phase", "CREATING");
                            properties.setProperty(
                                    "workerPid", Long.toString(ProcessHandle.current().pid()));
                            properties.setProperty(
                                    "workerStarted",
                                    ProcessHandle.current()
                                            .info()
                                            .startInstant()
                                            .orElseThrow()
                                            .toString());
                            properties.setProperty(
                                    "startedAt", Long.toString(System.currentTimeMillis()));
                        });
            }
            var creation =
                    admin.getBaseClient()
                            .createInstanceAsync(
                                    CreateInstanceRequest.newBuilder()
                                            .setParent(ProjectName.of(PROJECT).toString())
                                            .setInstanceId(instance)
                                            .setInstance(
                                                    Instance.newBuilder()
                                                            .setDisplayName(
                                                                    "Flink Stage 2 experiment")
                                                            .setType(Instance.Type.PRODUCTION)
                                                            .putLabels("stage2-owner", token))
                                            .putClusters(
                                                    "stage2-c1",
                                                    Cluster.newBuilder()
                                                            .setLocation(
                                                                    LocationName.of(
                                                                                    PROJECT,
                                                                                    "us-central1-b")
                                                                            .toString())
                                                            .setServeNodes(1)
                                                            .setDefaultStorageType(StorageType.SSD)
                                                            .build())
                                            .build());
            String operation = creation.getName();
            update(properties -> properties.setProperty("operation", operation));
            creation.get(10, java.util.concurrent.TimeUnit.MINUTES);
            update(properties -> properties.setProperty("creationFinished", "true"));
            requireCreating();
            createAppProfile(admin, LocalStagedHarness.PROFILE, singleCluster(true));
            createAppProfile(admin, "no-tx", singleCluster(false));
            createAppProfile(
                    admin,
                    "multi-cluster",
                    AppProfile.newBuilder()
                            .setMultiClusterRoutingUseAny(
                                    AppProfile.MultiClusterRoutingUseAny.getDefaultInstance()));
        }
        try (BigtableTableAdminClient admin = BigtableTableAdminClient.create(PROJECT, instance)) {
            for (String name : tables) {
                requireCreating();
                Table.Builder table =
                        Table.newBuilder().putColumnFamilies("cf", BigtableAdminProtos.rawFamily());
                if (name.equals("marker-gc")) {
                    table.putColumnFamilies(
                            StagedMutationTestSink.MARKER_FAMILY,
                            BigtableAdminProtos.maxVersionsFamily(1));
                } else if (name.equals("marker-typed")) {
                    table.putColumnFamilies(
                            StagedMutationTestSink.MARKER_FAMILY,
                            BigtableAdminProtos.typedFamily(BigtableAdminProtos.int64Sum()));
                } else if (!name.equals("marker-missing")) {
                    table.putColumnFamilies(
                            StagedMutationTestSink.MARKER_FAMILY, BigtableAdminProtos.rawFamily());
                }
                if (name.startsWith("recovery-")) {
                    table.putColumnFamilies(
                            "agg", BigtableAdminProtos.typedFamily(BigtableAdminProtos.int64Sum()));
                }
                admin.getBaseClient()
                        .createTable(
                                CreateTableRequest.newBuilder()
                                        .setParent(instanceName().toString())
                                        .setTableId(name)
                                        .setTable(table)
                                        .build());
            }
        }
        requireCreating();
        update(
                properties -> {
                    if (!properties.getProperty("phase").equals("CREATING")) {
                        throw new IllegalStateException("Lease stopped during setup");
                    }
                    properties.setProperty("phase", "ACTIVE");
                });
        System.out.println("STAGE2_CREATED " + instance);
    }

    private void requireCreating() throws IOException {
        Properties properties = read();
        if (!properties.getProperty("phase").equals("CREATING")
                || System.currentTimeMillis() >= startedAt() + 45 * 60_000L) {
            throw new IOException("Resource setup exceeded its lease");
        }
    }

    private void createAppProfile(
            BigtableInstanceAdminClient admin, String profile, AppProfile.Builder routing) {
        admin.getBaseClient()
                .createAppProfile(
                        CreateAppProfileRequest.newBuilder()
                                .setParent(instanceName().toString())
                                .setAppProfileId(profile)
                                // What the client's model request defaulted the description to.
                                .setAppProfile(routing.setDescription(profile))
                                .build());
    }

    private InstanceName instanceName() {
        return InstanceName.of(PROJECT, instance);
    }

    private static AppProfile.Builder singleCluster(boolean allowTransactionalWrites) {
        return AppProfile.newBuilder()
                .setSingleClusterRouting(
                        AppProfile.SingleClusterRouting.newBuilder()
                                .setClusterId("stage2-c1")
                                .setAllowTransactionalWrites(allowTransactionalWrites));
    }

    interface OwnedInstance {
        String owner() throws Exception;

        boolean creationFinished() throws Exception;

        default boolean ready() throws Exception {
            return false;
        }

        void delete() throws Exception;

        boolean listed() throws Exception;
    }

    static void removeOwned(String token, OwnedInstance remote) throws Exception {
        String owner = remote.owner();
        if (owner != null && !owner.equals(token)) {
            throw new IOException("Ownership mismatch; refusing deletion");
        }
        if (!(owner != null && remote.ready()) && !remote.creationFinished()) {
            throw new IOException("Creation is still unresolved; absence is not cleanup");
        }
        if (owner != null) {
            remote.delete();
        }
        if (remote.owner() != null || remote.listed()) {
            throw new IOException("Owned instance remains after deletion");
        }
    }

    static com.google.longrunning.Operation ownedCreation(
            String instance, String token, Iterable<com.google.longrunning.Operation> operations)
            throws IOException {
        com.google.longrunning.Operation completed = null;
        int inspected = 0;
        for (com.google.longrunning.Operation operation : operations) {
            if (++inspected > 1000) {
                throw new IOException("Creation reconciliation operation cap exceeded");
            }
            if (!operation
                    .getMetadata()
                    .is(com.google.bigtable.admin.v2.CreateInstanceMetadata.class)) {
                continue;
            }
            var request =
                    operation
                            .getMetadata()
                            .unpack(com.google.bigtable.admin.v2.CreateInstanceMetadata.class)
                            .getOriginalRequest();
            if (!request.getParent().equals("projects/flink-gcp")
                    || !request.getInstanceId().equals(instance)
                    || !token.equals(request.getInstance().getLabelsMap().get("stage2-owner"))) {
                continue;
            }
            if (!operation.getDone()) {
                return operation;
            }
            completed = operation;
        }
        return completed;
    }

    void cleanup() throws Exception {
        Properties properties = read();
        if (properties.getProperty("phase").equals("PLANNED")) {
            System.out.println("STAGE2_CLEANUP no creation attempted");
            return;
        }
        update(p -> p.setProperty("phase", "CLEANING"));
        terminateWorker(properties);
        try (BigtableInstanceAdminClient admin = BigtableInstanceAdminClient.create(PROJECT)) {
            removeOwned(
                    token,
                    new OwnedInstance() {

                        @Override
                        public String owner() {
                            try {
                                return admin.getBaseClient()
                                        .getInstance(instanceName())
                                        .getLabelsMap()
                                        .getOrDefault("stage2-owner", "");
                            } catch (NotFoundException absent) {
                                return null;
                            }
                        }

                        @Override
                        public boolean creationFinished() throws IOException {
                            if (properties
                                    .getProperty("creationFinished", "false")
                                    .equals("true")) {
                                return true;
                            }
                            String operation = properties.getProperty("operation");
                            if (operation == null) {
                                var recovered =
                                        ownedCreation(
                                                instance,
                                                token,
                                                admin.getBaseClient()
                                                        .getOperationsClient()
                                                        .listOperations(
                                                                "operations/projects/flink-gcp/instances/"
                                                                        + instance,
                                                                "")
                                                        .iterateAll());
                                if (recovered == null) {
                                    return false;
                                }
                                update(p -> p.setProperty("operation", recovered.getName()));
                                return recovered.getDone();
                            }
                            return admin.getBaseClient()
                                    .getOperationsClient()
                                    .getOperation(operation)
                                    .getDone();
                        }

                        @Override
                        public boolean ready() throws IOException {
                            try {
                                Instance current =
                                        admin.getBaseClient().getInstance(instanceName());
                                boolean ready =
                                        token.equals(current.getLabelsMap().get("stage2-owner"))
                                                && current.getState() == Instance.State.READY;
                                if (ready) {
                                    update(p -> p.setProperty("creationFinished", "true"));
                                }
                                return ready;
                            } catch (NotFoundException absent) {
                                return false;
                            }
                        }

                        @Override
                        public void delete() {
                            admin.getBaseClient().deleteInstance(instanceName());
                        }

                        @Override
                        public boolean listed() {
                            ListInstancesResponse listing =
                                    admin.getBaseClient().listInstances(ProjectName.of(PROJECT));
                            BigtableAdminProtos.checkListedEverywhere(
                                    listing.getFailedLocationsList(), "instances");
                            return listing.getInstancesList().stream()
                                    .anyMatch(
                                            value ->
                                                    value.getName()
                                                            .equals(instanceName().toString()));
                        }
                    });
        }
        removeWork(work);
        update(
                p -> {
                    p.setProperty("phase", "ABSENT");
                    p.setProperty("cleanedAt", Instant.now().toString());
                });
        System.out.println("STAGE2_CLEANUP ABSENT " + instance);
    }

    private static void terminateWorker(Properties properties) throws Exception {
        long pid = Long.parseLong(properties.getProperty("workerPid", "0"));
        if (pid == ProcessHandle.current().pid()) {
            return;
        }
        var optional = ProcessHandle.of(pid);
        if (optional.isEmpty() || !optional.get().isAlive()) {
            return;
        }
        ProcessHandle process = optional.get();
        stopWorker(
                properties.getProperty("workerStarted"),
                new Worker() {
                    @Override
                    public String started() {
                        return process.info().startInstant().map(Instant::toString).orElse("");
                    }

                    @Override
                    public void destroy(boolean forcibly) {
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
    }

    interface Worker {
        String started();

        void destroy(boolean forcibly);

        void awaitExit(long seconds) throws Exception;
    }

    static void stopWorker(String expectedStart, Worker worker) throws Exception {
        if (!worker.started().equals(expectedStart)) {
            System.out.println("STAGE2_CLEANUP original worker gone; preserving reused PID");
            return;
        }
        worker.destroy(false);
        try {
            worker.awaitExit(10);
        } catch (java.util.concurrent.TimeoutException timeout) {
            worker.destroy(true);
            worker.awaitExit(20);
        }
    }

    static void removeWork(Path work) throws IOException {
        if (!Files.exists(work, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (Files.isSymbolicLink(work)) {
            throw new IOException("Owned work root is a symlink");
        }
        try (var paths = Files.walk(work)) {
            for (Path path :
                    paths.sorted(Comparator.reverseOrder())
                            .collect(java.util.stream.Collectors.toList())) {
                Files.delete(path);
            }
        }
        if (Files.exists(work)) {
            throw new IOException("Owned checkpoint directory remains");
        }
    }

    void supervise() throws Exception {
        if (productionRecovery) {
            update(
                    properties -> {
                        if (properties.containsKey("supervisorPid")
                                || !properties.getProperty("phase").equals("PLANNED")) {
                            throw new IllegalStateException(
                                    "Production supervisor requires a fresh planned lease");
                        }
                        properties.setProperty(
                                "supervisorPid", Long.toString(ProcessHandle.current().pid()));
                        properties.setProperty(
                                "supervisorStarted",
                                ProcessHandle.current()
                                        .info()
                                        .startInstant()
                                        .orElseThrow()
                                        .toString());
                    });
        }
        System.out.println("STAGE2_SUPERVISOR " + instance);
        while (true) {
            Properties properties = read();
            String phase = properties.getProperty("phase");
            if (phase.equals("ABSENT")) {
                return;
            }
            long start = Long.parseLong(properties.getProperty("startedAt", "0"));
            boolean stop = Files.exists(manifest.resolveSibling("stop"));
            if ((start != 0 && System.currentTimeMillis() >= start + 50 * 60_000L) || stop) {
                try {
                    cleanup();
                    return;
                } catch (Exception failure) {
                    System.err.println("STAGE2_CLEANUP_INCOMPLETE " + instance + " " + failure);
                    if (start == 0 || System.currentTimeMillis() >= start + 60 * 60_000L) {
                        throw failure;
                    }
                    Thread.sleep(10_000);
                }
            }
            Thread.sleep(1000);
        }
    }
}
