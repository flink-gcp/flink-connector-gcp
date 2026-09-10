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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.Awaits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The same recovery experiment runs locally against an assumed model and, separately, on GCP. */
@Timeout(180)
abstract class StagedRecoveryAcceptance {
    private static final Map<String, CheckpointGate> CHECKPOINT_GATES =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final InMemoryReporter REPORTER = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(REPORTER.addToConfiguration(new Configuration()))
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(6)
                            .build());

    static final class Shape {
        final boolean table;
        final boolean appEngine;
        final boolean stable;

        Shape(boolean table, boolean appEngine, boolean stable) {
            this.table = table;
            this.appEngine = appEngine;
            this.stable = stable;
        }

        @Override
        public String toString() {
            return (table ? "table" : "datastream")
                    + "-"
                    + (appEngine ? "ae" : "http")
                    + "-"
                    + (stable ? "stable" : "random");
        }
    }

    @TempDir Path temporary;
    final List<JobClient> jobs = new ArrayList<>();
    final String caseId = UUID.randomUUID().toString().replace("-", "");
    AcceptanceEvidence evidence;
    RecordingTaskProxy proxy;
    private long scheduledSeconds;
    private String caseName;
    String currentQueue;

    abstract RecordingTaskProxy.Service service();

    abstract String queue(boolean appEngine);

    abstract void observe(Task task);

    abstract AcceptanceEvidence openEvidence() throws Exception;

    abstract void closeEvidence() throws Exception;

    static Stream<Shape> shapes() {
        return Stream.of(false, true)
                .flatMap(
                        table ->
                                Stream.of(false, true)
                                        .flatMap(
                                                appEngine ->
                                                        Stream.of(false, true)
                                                                .map(
                                                                        stable ->
                                                                                new Shape(
                                                                                        table,
                                                                                        appEngine,
                                                                                        stable))));
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> rescalings() {
        return shapes().flatMap(
                        shape ->
                                Stream.of(1, 3)
                                        .map(
                                                parallelism ->
                                                        org.junit.jupiter.params.provider.Arguments
                                                                .of(shape, parallelism)));
    }

    @BeforeEach
    void prepareCase(TestInfo info) throws Exception {
        evidence = openEvidence();
        caseName = info.getDisplayName();
        evidence.record("case-start", Map.of("case", caseName, "caseId", caseId));
        scheduledSeconds = System.currentTimeMillis() / 1000 + 43_200;
        proxy = new RecordingTaskProxy(service(), evidence);
    }

    @AfterEach
    void stopCase() throws Exception {
        CheckpointGate pending = CHECKPOINT_GATES.remove(caseId);
        if (pending != null) {
            pending.release.countDown();
        }
        List<AutoCloseable> resources = new ArrayList<>();
        for (JobClient job : jobs) {
            resources.add(() -> stop(job));
        }
        resources.add(proxy);
        resources.add(this::closeEvidence);
        AcceptanceCleanup.closeAll(resources.toArray(new AutoCloseable[0]));
    }

    @ParameterizedTest(name = "lost-response {0}")
    @MethodSource("shapes")
    void responseLossRecoversTheOriginalRequests(
            Shape shape, @InjectMiniCluster MiniCluster cluster) throws Exception {
        proxy.inject(RecordingTaskProxy.Fault.LOSE_RESPONSE, 1);
        JobClient initial = start(shape, null, 1, false, "fail", false);
        staged(initial, 4);
        assertThat(proxy.requests()).isEmpty();
        String checkpoint = checkpoint(cluster, initial);
        failed(initial, "UNAVAILABLE");
        assertReadable(checkpoint);
        assertThat(proxy.responses()).hasSize(1);
        observe(proxy.responses().get(0));
        var original = proxy.requests().get(0);
        proxy.nextIncarnation();
        JobClient recovered = start(shape, checkpoint, 1, false, "fail", false);
        committed(recovered, 4);
        assertThat(proxy.requests().get(1)).isEqualTo(original);
        assertThat(proxy.requests().stream().map(request -> request.getTask().getName()).distinct())
                .hasSize(4);
        assertThat(counter(recovered, "tasksDeduplicated")).isEqualTo(1);
        observeAll();
        passed();
    }

    @ParameterizedTest(name = "partial-commit-rescale {0} 2-to-{1}")
    @MethodSource("rescalings")
    void partialCommitAndLostDurableAckSurviveRescaling(
            Shape shape, int restoredParallelism, @InjectMiniCluster MiniCluster cluster)
            throws Exception {
        proxy.inject(RecordingTaskProxy.Fault.HOLD_RESPONSE, 2);
        JobClient initial = start(shape, null, 2, false, "fail", false);
        staged(initial, 4);
        assertThat(proxy.requests()).isEmpty();
        String checkpoint = checkpoint(cluster, initial);
        proxy.awaitHeld();
        stop(initial);
        assertReadable(checkpoint);
        var originals = proxy.requests();
        assertThat(originals).isNotEmpty();
        observeAll();
        proxy.nextIncarnation();
        JobClient one = start(shape, checkpoint, restoredParallelism, false, "fail", false);
        committed(one, 4);
        assertThat(proxy.requests()).containsAll(originals);
        checkpoint(cluster, one);
        assertThat(proxy.requests().stream().map(request -> request.getTask().getName()).distinct())
                .hasSize(4);
        for (var request : originals) {
            assertThat(
                            proxy.requests().stream()
                                    .filter(
                                            replay ->
                                                    replay.getTask()
                                                            .getName()
                                                            .equals(request.getTask().getName())))
                    .allMatch(request::equals);
        }
        observeAll();
        passed();
    }

    @ParameterizedTest(name = "expiry-retained-state {0}")
    @MethodSource("shapes")
    void terminalExpiryRetainsStateAndBoundedRestartsSendNothing(
            Shape shape, @InjectMiniCluster MiniCluster cluster) throws Exception {
        proxy.inject(RecordingTaskProxy.Fault.HOLD_RESPONSE, 1);
        JobClient initial = start(shape, null, 1, false, "fail", false);
        staged(initial, 4);
        String checkpoint = checkpoint(cluster, initial);
        proxy.awaitHeld();
        observeAll();
        stop(initial);
        int before = proxy.requests().size();
        // Tighten current options without changing the persisted origin or refreshing its deadline.
        long after = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        Awaits.await(
                "the original staging instant to be older than the tightened window",
                Duration.ofSeconds(5),
                () -> System.nanoTime() >= after);
        proxy.nextIncarnation();
        JobClient expired = start(shape, checkpoint, 1, true, "fail", false);
        failed(expired, "envelope expired");
        assertReadable(checkpoint);
        assertThat(proxy.requests()).hasSize(before);
        JobClient canceled = start(shape, checkpoint, 1, true, "drop", false);
        committed(canceled, 4);
        assertThat(counter(canceled, "expiredEnvelopesDropped")).isEqualTo(4);
        checkpoint(cluster, canceled);
        stop(canceled);
        assertReadable(checkpoint);
        assertThat(proxy.requests()).hasSize(before);
        passed();
    }

    @ParameterizedTest(name = "finished-savepoint {0}")
    @MethodSource("shapes")
    void finishedSavepointResumesWithinTheWindow(
            Shape shape, @InjectMiniCluster MiniCluster cluster) throws Exception {
        JobClient initial = start(shape, null, 1, false, "fail", false);
        staged(initial, 4);
        String savepoint =
                initial.stopWithSavepoint(
                                false,
                                temporary.resolve("savepoints").toUri().toString(),
                                SavepointFormatType.CANONICAL)
                        .get(45, TimeUnit.SECONDS);
        initial.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        assertThat(initial.getJobStatus().get()).isEqualTo(JobStatus.FINISHED);
        assertReadable(savepoint);
        var originals = proxy.requests();
        assertThat(originals).hasSize(4);
        observeAll();
        proxy.nextIncarnation();
        JobClient restored = start(shape, savepoint, 1, false, "fail", false);
        committed(restored, 4);
        assertThat(proxy.requests().subList(4, 8)).containsExactlyElementsOf(originals);
        observeAll();
        passed();
    }

    @ParameterizedTest(name = "old-incarnation {0}")
    @MethodSource("shapes")
    void anOldIncarnationCanCreateWithoutInformingTheReplacement(
            Shape shape, @InjectMiniCluster MiniCluster cluster) throws Exception {
        proxy.inject(RecordingTaskProxy.Fault.HOLD_BEFORE_FORWARD, 1);
        JobClient initial = start(shape, null, 1, false, "fail", false);
        staged(initial, 4);
        String checkpoint = checkpoint(cluster, initial);
        proxy.awaitHeld();
        stop(initial);
        proxy.nextIncarnation();
        proxy.releaseOldRequest();
        Awaits.await(
                "old-incarnation create response recorded outside the current client",
                Duration.ofSeconds(15),
                () -> proxy.responses().size() == 1);
        observeAll();
        JobClient replacement = start(shape, checkpoint, 1, false, "fail", false);
        committed(replacement, 4);
        assertThat(counter(replacement, "tasksDeduplicated")).isEqualTo(1);
        assertThat(proxy.requests().get(1)).isEqualTo(proxy.requests().get(0));
        observeAll();
        passed();
    }

    JobClient start(
            Shape shape,
            String restore,
            int parallelism,
            boolean shortWindow,
            String policy,
            boolean direct)
            throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, shortWindow ? "fixed-delay" : "none");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (restore != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
        }
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        // The first periodic trigger may otherwise be randomized down to zero.
        // Explicit checkpoints/savepoints are not subject to the periodic minimum pause.
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.HOURS.toMillis(1));
        env.disableOperatorChaining();
        String salt = caseId;
        var source =
                new DataGeneratorSource<Long>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(20),
                        Types.LONG);
        var records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                        .uid("records")
                        .setParallelism(1)
                        .filter(value -> value < 4)
                        .uid("select-four")
                        .setParallelism(1)
                        .map(value -> salt + "-" + value)
                        .returns(Types.STRING)
                        .uid("payload")
                        .setParallelism(1);
        records.map(new CheckpointBlocker(caseId))
                .returns(Types.STRING)
                .uid("checkpoint-blocker")
                .setParallelism(1)
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .uid("checkpoint-probe")
                .setParallelism(1);
        currentQueue = queue(shape.appEngine);
        String[] parts = currentQueue.split("/");
        long schedule = scheduledSeconds;
        if (shape.table) {
            var table = StreamTableEnvironment.create(env);
            table.createTemporaryView("records", table.fromDataStream(records).as("payload"));
            String target =
                    shape.appEngine
                            ? "'target.type'='app-engine','app-engine.relative-uri'='/accepted','app-engine.service'='default','app-engine.version'='flink-e2e',"
                            : "'http.url'='https://example.invalid/accepted',";
            table.executeSql(
                    "CREATE TABLE tasks (payload STRING, scheduled TIMESTAMP_LTZ(3) METADATA FROM 'schedule-time'"
                            + (shape.stable ? ", task_key STRING METADATA FROM 'task-id'" : "")
                            + ") WITH ('connector'='cloud-tasks','project'='"
                            + parts[1]
                            + "','location'='"
                            + parts[3]
                            + "','queue'='"
                            + parts[5]
                            + "','format'='json',"
                            + target
                            + (direct ? "" : "'emulator-endpoint'='" + proxy.endpoint() + "',")
                            + "'sink.delivery-guarantee'='exactly-once','sink.in-flight.max-tasks'='1','sink.recovery.max-attempts'='1',"
                            + "'sink.staged.clock-skew-allowance'='0 ms','sink.staged.request-timeout'='20 s',"
                            + "'sink.staged.name-retention'='"
                            + (shortWindow ? "21 s" : "1 h")
                            + "',"
                            + "'sink.staged.expired-envelope-policy'='"
                            + policy
                            + "')");
            var statement = table.createStatementSet();
            statement.addInsertSql(
                    "INSERT INTO tasks SELECT payload, TO_TIMESTAMP_LTZ("
                            + (schedule * 1000)
                            + ", 3)"
                            + (shape.stable ? ", payload" : "")
                            + " FROM records");
            statement.attachAsDataStream();
            JobClient job = env.executeAsync("cloudtasks-table-recovery-acceptance");
            jobs.add(job);
            return job;
        }
        boolean appEngine = shape.appEngine;
        var sink =
                CloudTasksSink.<String>builder()
                        .queue(QueueDestination.of(parts[1], parts[3], parts[5]))
                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                        .writerOptions(
                                CloudTasksWriterOptions.builder()
                                        .maxInFlightTasks(1)
                                        .recoveryMaxAttempts(1)
                                        .build())
                        .stagedOptions(
                                CloudTasksStagedOptions.builder()
                                        .clockSkewAllowance(Duration.ZERO)
                                        .requestTimeout(Duration.ofSeconds(20))
                                        .nameRetention(
                                                shortWindow
                                                        ? Duration.ofSeconds(21)
                                                        : Duration.ofHours(1))
                                        .expiredEnvelopePolicy(
                                                CloudTasksStagedOptions.ExpiredEnvelopePolicy
                                                        .valueOf(
                                                                policy.toUpperCase(
                                                                        java.util.Locale.ROOT)))
                                        .build())
                        .serializer(value -> task(value, appEngine, schedule));
        if (!direct) {
            sink.emulatorEndpoint(proxy.endpoint());
        }
        if (shape.stable) {
            sink.taskIdExtractor(value -> value);
        }
        records.sinkTo(sink.build()).uid("tasks");
        JobClient job = env.executeAsync("cloudtasks-recovery-acceptance");
        jobs.add(job);
        return job;
    }

    static Task task(String body, boolean appEngine, long schedule) {
        var task = Task.newBuilder().setScheduleTime(Timestamp.newBuilder().setSeconds(schedule));
        if (appEngine) {
            task.setAppEngineHttpRequest(
                    AppEngineHttpRequest.newBuilder()
                            .setHttpMethod(HttpMethod.POST)
                            .setRelativeUri("/accepted")
                            .setAppEngineRouting(
                                    AppEngineRouting.newBuilder()
                                            .setService("default")
                                            .setVersion("flink-e2e"))
                            .setBody(ByteString.copyFromUtf8(body)));
        } else {
            task.setHttpRequest(
                    HttpRequest.newBuilder()
                            .setHttpMethod(HttpMethod.POST)
                            .setUrl("https://example.invalid/accepted")
                            .setBody(ByteString.copyFromUtf8(body)));
        }
        return task.build();
    }

    void observeAll() {
        proxy.responses().stream()
                .collect(
                        java.util.stream.Collectors.toMap(
                                Task::getName, task -> task, (first, second) -> first))
                .values()
                .forEach(this::observe);
    }

    void passed() {
        evidence.record("case-pass", Map.of("case", caseName, "caseId", caseId));
    }

    static void staged(JobClient job, long count) throws InterruptedException {
        Awaits.await(
                "staged tasks",
                Duration.ofSeconds(30),
                () -> {
                    assertHealthy(job);
                    return REPORTER
                                    .findMetrics(job.getJobID(), ".*\\.stagedTasks$")
                                    .values()
                                    .stream()
                                    .map(Gauge.class::cast)
                                    .map(Gauge::getValue)
                                    .mapToLong(value -> ((Number) value).longValue())
                                    .sum()
                            == count;
                });
    }

    private static void assertHealthy(JobClient job) {
        try {
            if (job.getJobStatus().get(5, TimeUnit.SECONDS).isGloballyTerminalState()) {
                job.getJobExecutionResult().get(5, TimeUnit.SECONDS);
                throw new AssertionError("Job terminated before the expected observation");
            }
        } catch (Exception error) {
            throw new AssertionError(
                    "Job failed before the expected observation: " + job.getJobID(), error);
        }
    }

    static void committed(JobClient job, long count) throws InterruptedException {
        Awaits.await(
                "all restored committables finalized",
                Duration.ofSeconds(30),
                () ->
                        counter(job, "successfulCommittables")
                                        + counter(job, "alreadyCommittedCommittables")
                                == count);
    }

    static long counter(JobClient job, String name) {
        return REPORTER.findMetrics(job.getJobID(), ".*\\." + name + "$").values().stream()
                .map(Counter.class::cast)
                .mapToLong(Counter::getCount)
                .sum();
    }

    static void awaitRunning(MiniCluster cluster, JobClient job) throws InterruptedException {
        Awaits.await(
                "all checkpoint participants initialized",
                Duration.ofSeconds(30),
                () -> {
                    try {
                        assertHealthy(job);
                        if (job.getJobStatus().get(5, TimeUnit.SECONDS) != JobStatus.RUNNING) {
                            return false;
                        }
                        for (var vertex :
                                cluster.getExecutionGraph(job.getJobID())
                                        .get(5, TimeUnit.SECONDS)
                                        .getAllExecutionVertices()) {
                            var state = vertex.getExecutionState();
                            if (state != org.apache.flink.runtime.execution.ExecutionState.RUNNING
                                    && state
                                            != org.apache.flink.runtime.execution.ExecutionState
                                                    .FINISHED) {
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception error) {
                        throw new AssertionError(error);
                    }
                });
    }

    String checkpoint(MiniCluster cluster, JobClient job) throws Exception {
        awaitRunning(cluster, job);
        int received = proxy.requests().size();
        List<String> liveBefore = liveTaskNames();
        var gate = new CheckpointGate();
        CHECKPOINT_GATES.put(caseId, gate);
        java.util.concurrent.CompletableFuture<String> checkpoint;
        try {
            checkpoint = cluster.triggerCheckpoint(job.getJobID());
            java.util.concurrent.CompletableFuture.anyOf(checkpoint, gate.entered)
                    .get(30, TimeUnit.SECONDS);
            staged(job, 0);
            assertThat(checkpoint).isNotDone();
            assertThat(proxy.requests()).hasSize(received);
            assertThat(liveTaskNames()).containsExactlyInAnyOrderElementsOf(liveBefore);
            evidence.record("checkpoint-pending-no-creation", Map.of("caseId", caseId));
        } finally {
            gate.release.countDown();
            CHECKPOINT_GATES.remove(caseId, gate);
        }
        String pointer = checkpoint.get(30, TimeUnit.SECONDS);
        Path path = Path.of(java.net.URI.create(pointer));
        Path metadata = Files.isDirectory(path) ? path.resolve("_metadata") : path;
        evidence.record(
                "checkpoint-completed",
                Map.of(
                        "caseId",
                        caseId,
                        "jobId",
                        job.getJobID().toHexString(),
                        "metadataSha256",
                        org.apache.flink.util.StringUtils.byteToHexString(
                                java.security.MessageDigest.getInstance("SHA-256")
                                        .digest(Files.readAllBytes(metadata)))));
        return pointer;
    }

    List<String> liveTaskNames() {
        return List.of();
    }

    private static final class CheckpointGate {
        final java.util.concurrent.CompletableFuture<Void> entered =
                new java.util.concurrent.CompletableFuture<>();
        final java.util.concurrent.CountDownLatch release =
                new java.util.concurrent.CountDownLatch(1);
    }

    private static final class CheckpointBlocker
            implements org.apache.flink.api.common.functions.MapFunction<String, String>,
                    org.apache.flink.streaming.api.checkpoint.CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private final String id;

        CheckpointBlocker(String id) {
            this.id = id;
        }

        @Override
        public String map(String value) {
            return value;
        }

        @Override
        public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context)
                throws Exception {
            CheckpointGate gate = CHECKPOINT_GATES.get(id);
            if (gate != null) {
                gate.entered.complete(null);
                if (!gate.release.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("Checkpoint observation gate was not released");
                }
            }
        }

        @Override
        public void initializeState(
                org.apache.flink.runtime.state.FunctionInitializationContext context) {}
    }

    static void failed(JobClient job, String message) {
        assertThatThrownBy(() -> job.getJobExecutionResult().get(45, TimeUnit.SECONDS))
                .hasStackTraceContaining(message);
    }

    static void assertReadable(String pointer) {
        Path path = Path.of(java.net.URI.create(pointer));
        assertThat(Files.isReadable(Files.isDirectory(path) ? path.resolve("_metadata") : path))
                .isTrue();
    }

    static void stop(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            job.cancel().get(10, TimeUnit.SECONDS);
        }
        try {
            job.getJobExecutionResult().get(15, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException expected) {
            // Failed/canceled jobs are deliberate; timeout still fails teardown.
        }
    }
}
