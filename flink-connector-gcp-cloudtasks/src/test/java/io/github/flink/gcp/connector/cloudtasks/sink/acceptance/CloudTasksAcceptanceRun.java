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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.GetTaskRequest;
import com.google.cloud.tasks.v2.ListTasksRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Duration;
import io.grpc.CallOptions;
import io.grpc.Deadline;
import io.grpc.Status;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** One serial, explicitly selected phase of the separately approved acceptance manifest. */
final class CloudTasksAcceptanceRun implements AutoCloseable, RecordingTaskProxy.Service {
    private static final int LIST_PAGE_SIZE = 10;

    final Properties manifest;
    final String phase;
    final Path directory;
    final AcceptanceEvidence evidence;
    final TaskCreationLedger ledger = new TaskCreationLedger();
    private final Set<String> owned = new LinkedHashSet<>();
    private final CloudTasksClient client;
    private final com.google.cloud.tasks.v2beta3.CloudTasksClient admin;
    private final long endMillis;
    private int operations;
    private int creates;
    private volatile boolean admissionStopped;

    CloudTasksAcceptanceRun(String selectedPhase) throws Exception {
        phase = selectedPhase;
        Path path = Path.of(requiredProperty("cloudtasks.acceptance.manifest"));
        byte[] bytes = Files.readAllBytes(path);
        String digest =
                org.apache.flink.util.StringUtils.byteToHexString(
                        MessageDigest.getInstance("SHA-256").digest(bytes));
        if (!digest.equals(requiredProperty("cloudtasks.acceptance.sha256"))) {
            throw new IllegalArgumentException("Acceptance manifest hash mismatch");
        }
        manifest = new Properties();
        try (var input = Files.newInputStream(path)) {
            manifest.load(input);
        }
        if (!Set.of("120", "221", "slow").contains(phase)) {
            throw new IllegalArgumentException("Unknown acceptance phase");
        }
        directory = path.toAbsolutePath().getParent();
        long start =
                Long.parseLong(Files.readString(directory.resolve("started-at-millis")).trim());
        endMillis = Math.addExact(start, TimeUnit.HOURS.toMillis(4));
        if (start > System.currentTimeMillis() || System.currentTimeMillis() >= endMillis) {
            throw new IllegalStateException("Acceptance lifetime is not active");
        }
        if (!value("location").equals("us-central1")
                || !value("run.id").matches("[a-z0-9]{8,20}")) {
            throw new IllegalArgumentException("Unexpected acceptance location or run identity");
        }
        for (String arm : List.of("http", "ae")) {
            String expected = "flink-ct-r1245-" + value("run.id") + "-" + phase + "-" + arm;
            if (!value("queue." + phase + "." + arm).equals(expected)) {
                throw new IllegalArgumentException("Queue is outside the frozen naming plan");
            }
        }
        evidence = new AcceptanceEvidence(directory.resolve(phase + ".jsonl"));
        CloudTasksClient openedClient = null;
        try {
            evidence.record(
                    "phase-start",
                    Map.of(
                            "phase",
                            phase,
                            "manifestSha256",
                            digest,
                            "java",
                            System.getProperty("java.version"),
                            "deadline",
                            endMillis));
            var taskSettings = com.google.cloud.tasks.v2.CloudTasksSettings.newBuilder();
            taskSettings.getTaskSettings().setRetryableCodes();
            taskSettings.listTasksSettings().setRetryableCodes();
            openedClient = CloudTasksClient.create(taskSettings.build());
            var queueSettings = com.google.cloud.tasks.v2beta3.CloudTasksSettings.newBuilder();
            queueSettings.getQueueSettings().setRetryableCodes();
            admin = com.google.cloud.tasks.v2beta3.CloudTasksClient.create(queueSettings.build());
            client = openedClient;
        } catch (Throwable failure) {
            try {
                AcceptanceCleanup.closeAll(openedClient, evidence);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    String value(String key) {
        String value = manifest.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing manifest field " + key);
        }
        return value;
    }

    String queue(boolean appEngine) {
        return "projects/"
                + value("project")
                + "/locations/"
                + value("location")
                + "/queues/"
                + value("queue." + phase + "." + (appEngine ? "ae" : "http"));
    }

    void provision() throws Exception {
        for (boolean appEngine : new boolean[] {false, true}) {
            String name = queue(appEngine);
            reserve(1, false);
            try {
                admin.getQueue(name);
                throw new IllegalStateException("Pre-existing queue; no ownership: " + name);
            } catch (com.google.api.gax.rpc.NotFoundException expected) {
                // Only NOT_FOUND permits a create attempt.
            }
            var configuration =
                    com.google.cloud.tasks.v2beta3.Queue.newBuilder()
                            .setName(name)
                            .setTombstoneTtl(Duration.newBuilder().setSeconds(3_600))
                            .setRateLimits(
                                    com.google.cloud.tasks.v2beta3.RateLimits.newBuilder()
                                            .setMaxDispatchesPerSecond(1)
                                            .setMaxConcurrentDispatches(1))
                            .setRetryConfig(
                                    com.google.cloud.tasks.v2beta3.RetryConfig.newBuilder()
                                            .setMaxAttempts(phase.equals("slow") ? -1 : 2)
                                            .setMaxRetryDuration(
                                                    Duration.newBuilder()
                                                            .setSeconds(
                                                                    phase.equals("slow") ? 0 : 2))
                                            .setMinBackoff(Duration.newBuilder().setSeconds(5))
                                            .setMaxBackoff(Duration.newBuilder().setSeconds(5)));
            reserve(1, false);
            owned.add(name);
            evidence.record(
                    "queue-create-attempt",
                    Map.of(
                            "name",
                            name,
                            "configurationBase64",
                            encode(configuration.build().toByteArray())));
            try {
                admin.createQueue(
                        "projects/" + value("project") + "/locations/" + value("location"),
                        configuration.build());
            } catch (com.google.api.gax.rpc.AlreadyExistsException collision) {
                owned.remove(name);
                evidence.record("queue-create-collision", Map.of("name", name));
                throw collision;
            }
            evidence.record("queue-created", Map.of("name", name));
            pause(name);
        }
    }

    void pause(String name) {
        reserve(1, false);
        admin.pauseQueue(name);
        verifyPaused(name);
    }

    void verifyPaused(String name) {
        reserve(1, false);
        var queue = admin.getQueue(name);
        evidence.record(
                "queue-readback", Map.of("name", name, "queueBase64", encode(queue.toByteArray())));
        if (queue.getState() != com.google.cloud.tasks.v2beta3.Queue.State.PAUSED
                || !queue.hasTombstoneTtl()
                || queue.getTombstoneTtl().getSeconds() != 3_600
                || queue.getTombstoneTtl().getNanos() != 0
                || queue.hasPurgeTime()) {
            throw new AssertionError("Queue does not match the paused, unpurged one-hour protocol");
        }
    }

    void configureExhaustion(String name) {
        verifyPaused(name);
        reserve(1, false);
        var updated =
                admin.updateQueue(
                        com.google.cloud.tasks.v2beta3.UpdateQueueRequest.newBuilder()
                                .setQueue(
                                        com.google.cloud.tasks.v2beta3.Queue.newBuilder()
                                                .setName(name)
                                                .setRetryConfig(
                                                        com.google.cloud.tasks.v2beta3.RetryConfig
                                                                .newBuilder()
                                                                .setMaxAttempts(2)
                                                                .setMaxRetryDuration(
                                                                        Duration.newBuilder()
                                                                                .setSeconds(2))
                                                                .setMinBackoff(
                                                                        Duration.newBuilder()
                                                                                .setSeconds(5))
                                                                .setMaxBackoff(
                                                                        Duration.newBuilder()
                                                                                .setSeconds(5))))
                                .setUpdateMask(
                                        com.google.protobuf.FieldMask.newBuilder()
                                                .addPaths("retry_config"))
                                .build());
        evidence.record(
                "retry-config-updated", Map.of("queueBase64", encode(updated.toByteArray())));
        verifyPaused(name);
        verifyRetries(name, true);
    }

    void verifyRetries(String name, boolean bounded) {
        reserve(1, false);
        var observed = admin.getQueue(name);
        evidence.record(
                "retry-config-readback", Map.of("queueBase64", encode(observed.toByteArray())));
        var retry = observed.getRetryConfig();
        if (retry.getMaxAttempts() != (bounded ? 2 : -1)
                || retry.getMaxRetryDuration().getSeconds() != (bounded ? 2 : 0)
                || retry.getMaxRetryDuration().getNanos() != 0) {
            throw new AssertionError("Removal phase has unexpected retry limits");
        }
    }

    void resume(String name) {
        reserve(1, false);
        admin.resumeQueue(name);
        evidence.record("queue-resumed", Map.of("name", name));
    }

    @Override
    public Task create(CreateTaskRequest request, Deadline deadline) throws Exception {
        if (!owned.contains(request.getParent())) {
            throw new IllegalArgumentException("Request outside owned queues");
        }
        reserve(1, true);
        evidence.record(
                "outgoing-create",
                Map.of(
                        "requestBase64",
                        encode(request.toByteArray()),
                        "remainingNanos",
                        deadline.timeRemaining(TimeUnit.NANOSECONDS)));
        var response =
                client.createTaskCallable()
                        .futureCall(
                                request,
                                GrpcCallContext.createDefault()
                                        .withCallOptions(
                                                CallOptions.DEFAULT.withDeadline(deadline)));
        try {
            Task task =
                    response.get(
                            Math.max(1, deadline.timeRemaining(TimeUnit.NANOSECONDS)),
                            TimeUnit.NANOSECONDS);
            evidence.record(
                    "service-create-response",
                    Map.of(
                            "taskBase64",
                            encode(task.toByteArray()),
                            "name",
                            task.getName(),
                            "createTime",
                            task.getCreateTime().toString()));
            ledger.created(task);
            return task;
        } catch (java.util.concurrent.TimeoutException | InterruptedException error) {
            response.cancel(true);
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw error;
        } catch (java.util.concurrent.ExecutionException error) {
            Throwable cause = error.getCause();
            evidence.record(
                    "service-create-error",
                    Map.of(
                            "name",
                            request.getTask().getName(),
                            "status",
                            Status.fromThrowable(cause).getCode().name()));
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw error;
        }
    }

    Task get(String name) {
        reserve(1, false);
        try {
            Task task =
                    client.getTask(
                            GetTaskRequest.newBuilder()
                                    .setName(name)
                                    .setResponseView(Task.View.FULL)
                                    .build());
            evidence.record("get-task", Map.of("taskBase64", encode(task.toByteArray())));
            return task;
        } catch (com.google.api.gax.rpc.NotFoundException missing) {
            evidence.record("get-task-not-found", Map.of("name", name));
            return null;
        }
    }

    List<Task> list(String queue) {
        List<Task> tasks = new ArrayList<>();
        String token = "";
        do {
            // List billing counts returned tasks. Reserve the full page before making the call.
            reserve(LIST_PAGE_SIZE, false);
            var page =
                    client.listTasksCallable()
                            .call(
                                    ListTasksRequest.newBuilder()
                                            .setParent(queue)
                                            .setResponseView(Task.View.FULL)
                                            .setPageSize(LIST_PAGE_SIZE)
                                            .setPageToken(token)
                                            .build());
            evidence.record(
                    "list-tasks", Map.of("queue", queue, "pageBase64", encode(page.toByteArray())));
            tasks.addAll(page.getTasksList());
            token = page.getNextPageToken();
        } while (!token.isEmpty());
        return tasks;
    }

    void observe(Task original) {
        observeAll(List.of(original));
    }

    void observeAll(List<Task> originals) {
        if (originals.isEmpty()) {
            throw new AssertionError("No captured creations to observe");
        }
        Task original = originals.get(0);
        String queue = original.getName().substring(0, original.getName().lastIndexOf("/tasks/"));
        List<Task> listed = list(queue);
        for (Task expected : originals) {
            Task current = get(expected.getName());
            if (current == null) {
                throw new AssertionError("Original task disappeared before live observation");
            }
            ledger.observed(current, false);
            ledger.observed(
                    listed.stream()
                            .filter(task -> task.getName().equals(expected.getName()))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Task not found in ListTasks")),
                    true);
        }
    }

    void delete(String name) {
        reserve(1, false);
        client.deleteTask(name);
        evidence.record("delete-task", Map.of("name", name));
    }

    long remainingMillis() {
        return endMillis - System.currentTimeMillis();
    }

    void stopAdmission() {
        admissionStopped = true;
    }

    void requireActive() {
        if (admissionStopped) {
            throw new IllegalStateException(
                    "An earlier case failed; no further admissions are authorized");
        }
    }

    private synchronized void reserve(int units, boolean create) {
        requireActive();
        if (System.currentTimeMillis() >= endMillis - TimeUnit.MINUTES.toMillis(30)
                || operations + units > 30_000
                || create && creates >= 1_500) {
            throw new IllegalStateException(
                    "Acceptance budget exhausted; stop and preserve partial evidence");
        }
        operations += units;
        if (create) {
            creates++;
        }
    }

    @Override
    public void close() throws Exception {
        List<String> unresolved = new ArrayList<>();
        RuntimeException clientCloseFailure = null;
        try {
            try {
                client.close();
            } catch (RuntimeException failure) {
                clientCloseFailure = failure;
                evidence.record("client-close-error", Map.of("type", failure.getClass().getName()));
            }
            for (String queue : owned) {
                boolean absent = false;
                for (int attempt = 0; attempt < 3 && !absent; attempt++) {
                    try {
                        admin.deleteQueue(queue);
                    } catch (com.google.api.gax.rpc.NotFoundException expected) {
                        // Read independently below, including after an ambiguous create response.
                    } catch (RuntimeException error) {
                        evidence.record(
                                "cleanup-delete-error", Map.of("queue", queue, "attempt", attempt));
                    }
                    try {
                        admin.getQueue(queue);
                    } catch (com.google.api.gax.rpc.NotFoundException expected) {
                        absent = true;
                    } catch (RuntimeException error) {
                        evidence.record(
                                "cleanup-read-error", Map.of("queue", queue, "attempt", attempt));
                    }
                }
                evidence.record("queue-cleanup", Map.of("name", queue, "verifiedAbsent", absent));
                if (!absent) {
                    unresolved.add(queue);
                }
            }
            evidence.record(
                    "phase-cleanup",
                    Map.of(
                            "unresolved",
                            unresolved,
                            "reservedUnits",
                            operations,
                            "createAttempts",
                            creates,
                            "finishedAt",
                            Instant.now().toString()));
        } finally {
            AcceptanceCleanup.closeAll(admin, evidence);
        }
        if (clientCloseFailure != null) {
            throw clientCloseFailure;
        }
        if (!unresolved.isEmpty()) {
            throw new AssertionError("Unresolved owned queues: " + unresolved);
        }
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing system property " + key);
        }
        return value;
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
