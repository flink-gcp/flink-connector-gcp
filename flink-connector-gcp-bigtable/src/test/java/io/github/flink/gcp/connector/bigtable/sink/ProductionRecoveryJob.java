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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Shared local/service correctness scenario through the two production API factories. */
final class ProductionRecoveryJob {
    /**
     * The periodic interval of the restored jobs, unchanged by the held initial job: the injected
     * response loss is spent before they start, so a periodic commit there replays harmlessly.
     */
    private static final long RESTORED_INTERVAL_MILLIS = 60_000;

    private ProductionRecoveryJob() {}

    static LocalStagedHarness newRun(
            io.github.flink.gcp.connector.bigtable.TableDestination destination) {
        return newRun(destination, 90_000);
    }

    /**
     * A run with the plan's fixed input shape and a caller-chosen bound on checkpoint and savepoint
     * control futures; the recovery lease keeps 90 seconds, the native gated class allows more
     * after its initial stop exceeded 90 seconds on both entry points on 2026-09-14.
     */
    static LocalStagedHarness newRun(
            io.github.flink.gcp.connector.bigtable.TableDestination destination,
            long controlTimeoutMillis) {
        return new LocalStagedHarness(destination, "127.0.0.1:1", 32, true, true, 1) {
            @Override
            long checkpointTimeoutMillis() {
                return 60_000;
            }

            @Override
            long checkpointOperationTimeoutMillis() {
                return controlTimeoutMillis;
            }
        };
    }

    /** One line per outcome with its count, for a diagnostic log of the commit observations. */
    static String summarizeCommits(LocalStagedHarness run, int from) {
        var counts =
                new java.util.EnumMap<CommitObservation.Outcome, Integer>(
                        CommitObservation.Outcome.class);
        for (CommitObservation observation : phase(run.productionCommits, from)) {
            counts.merge(observation.outcome, 1, Integer::sum);
        }
        return "commitObservations=" + counts;
    }

    interface Readback {
        void verify() throws Exception;
    }

    /** One commit request seen at the delegating committer boundary, with how it ended. */
    static final class CommitObservation {
        enum Outcome {
            /** Recorded but never handed to the production committer, or not yet returned. */
            UNSENT,
            /** The production committer returned without a signal: the service applied it. */
            APPLIED,
            /** The production committer signalled that the marker already existed. */
            ALREADY_COMMITTED,
            FAILED,
            RETRY
        }

        final ByteString identity;
        final ByteString row;
        volatile Outcome outcome = Outcome.UNSENT;

        CommitObservation(BigtableCommittable committable) {
            var request = committable.getRequest();
            var mutations = request.getFalseMutationsList();
            this.identity = mutations.get(mutations.size() - 1).getSetCell().getColumnQualifier();
            this.row = request.getRowKey();
        }
    }

    /**
     * Requires the observations after {@code from} to be exactly {@code count} first applications
     * with distinct identities, and returns the identity-to-row inventory they established.
     */
    static Map<ByteString, ByteString> requireCommitted(
            List<CommitObservation> observations, int from, int count) throws IOException {
        Map<ByteString, ByteString> inventory = new HashMap<>();
        List<CommitObservation> phase = phase(observations, from);
        for (CommitObservation observation : phase) {
            if (observation.outcome != CommitObservation.Outcome.APPLIED
                    || inventory.put(observation.identity, observation.row) != null) {
                throw new IOException(
                        "Initial commit did not apply a distinct envelope once: "
                                + describe(phase));
            }
        }
        if (inventory.size() != count) {
            throw new IOException(
                    "Initial commit applied " + inventory.size() + " envelopes, expected " + count);
        }
        return inventory;
    }

    /**
     * Requires the observations after {@code from} to replay exactly the inventory's identities on
     * their original rows, each absorbed by its retained marker. A restore that commits nothing, or
     * commits under fresh identities, fails here even though the readback would be unchanged.
     */
    static void requireReplayed(
            List<CommitObservation> observations, int from, Map<ByteString, ByteString> inventory)
            throws IOException {
        Map<ByteString, ByteString> replayed = new HashMap<>();
        List<CommitObservation> phase = phase(observations, from);
        for (CommitObservation observation : phase) {
            if (observation.outcome != CommitObservation.Outcome.ALREADY_COMMITTED
                    || !observation.row.equals(inventory.get(observation.identity))
                    || replayed.put(observation.identity, observation.row) != null) {
                throw new IOException(
                        "Restored commit was not a deduplicated replay of the original envelope: "
                                + describe(phase));
            }
        }
        if (!replayed.keySet().equals(inventory.keySet())) {
            throw new IOException(
                    "Restore replayed "
                            + replayed.size()
                            + " of "
                            + inventory.size()
                            + " restored envelopes: "
                            + describe(phase));
        }
    }

    private static List<CommitObservation> phase(List<CommitObservation> observations, int from) {
        synchronized (observations) {
            return List.copyOf(observations.subList(from, observations.size()));
        }
    }

    private static String describe(List<CommitObservation> phase) {
        StringBuilder text = new StringBuilder(phase.size() + " observations");
        for (CommitObservation observation : phase) {
            text.append(' ')
                    .append(observation.row.toStringUtf8())
                    .append('/')
                    .append(observation.identity.toStringUtf8())
                    .append('=')
                    .append(observation.outcome);
        }
        return text.toString();
    }

    /**
     * Waits for the job to terminate and returns every failure text it left behind: the job status,
     * the caller's direct exception, the job result's failure and each execution's own failure
     * info. A stop-with-savepoint that fails during stopping reports a {@code
     * StopWithSavepointStoppingException} on both the operation and the job result; among what the
     * archived execution graph exposes, the task's cause survives only on the failed execution's
     * failure info, so a rejection message must be read from there. Call it before the cluster
     * closes; a job that does not terminate fails the assertion naming its state.
     */
    static String failureText(LocalStagedJob job, @Nullable Throwable direct) throws Exception {
        Throwable terminal;
        try {
            terminal =
                    job.result
                            .handle((result, failure) -> failure)
                            .get(
                                    job.run.checkpointOperationTimeoutMillis(),
                                    java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException timeout) {
            throw new AssertionError(
                    "The job did not terminate: status="
                            + job.client.getJobStatus().join()
                            + " admissions="
                            + job.run.admissions.size()
                            + " acknowledgements="
                            + job.run.acknowledgements.size(),
                    timeout);
        }
        StringBuilder text =
                new StringBuilder("jobStatus=" + job.client.getJobStatus().join()).append('\n');
        if (direct != null) {
            text.append(ExceptionUtils.stringifyException(direct)).append('\n');
        }
        if (terminal != null) {
            text.append(ExceptionUtils.stringifyException(terminal)).append('\n');
        }
        var graph = job.cluster.getExecutionGraph(job.client.getJobID()).join();
        if (graph.getFailureInfo() != null) {
            text.append(graph.getFailureInfo().getExceptionAsString()).append('\n');
        }
        for (var vertex : graph.getAllExecutionVertices()) {
            vertex.getCurrentExecutionAttempt()
                    .getFailureInfo()
                    .ifPresent(info -> text.append(info.getExceptionAsString()).append('\n'));
        }
        return text.toString();
    }

    /** Distinct contributions per row for the first {@code inputs} sequence numbers of a run. */
    static Map<ByteString, Long> expectedContributions(LocalStagedHarness run, int inputs) {
        Map<ByteString, Long> expected = new HashMap<>();
        for (long number = 0; number < inputs; number++) {
            expected.merge(run.input(number).row(), 1L, Long::sum);
        }
        return expected;
    }

    static void run(
            LocalStagedHarness run,
            ProductionRecoveryProxy proxy,
            Path directory,
            boolean tableApi,
            Readback readback)
            throws Exception {
        var sink = sink(run, proxy.endpoint(), tableApi);
        String checkpoint;
        // The initial job must not complete a periodic checkpoint before the explicit one: its
        // commit meets the injected response loss, and the failover that follows aborts the
        // explicit trigger while it is still pending or queued.
        try (var job =
                job(
                        run,
                        directory.resolve("initial"),
                        sink,
                        2,
                        LocalStagedJob.HELD_INTERVAL_MILLIS,
                        null,
                        true)) {
            job.awaitAdmissions(128);
            checkpoint = job.checkpoint();
            System.out.println(
                    "PRODUCTION_PHASE completedCheckpoint="
                            + checkpoint
                            + " at="
                            + java.time.Instant.now());
            waitFor(job, proxy, 128, 1);
            readback.verify();
            System.out.println("PRODUCTION_CHECKPOINTS " + job.checkpointStats());
        }
        if (run.staged.get() != 128) {
            throw new IOException(
                    "Automatic recovery serialized fresh source inputs after the completed checkpoint");
        }
        int staged = run.staged.get();
        String stop = null;
        for (int parallelism : new int[] {1, 3}) {
            int duplicates;
            synchronized (proxy) {
                duplicates = proxy.duplicates;
            }
            try (var job =
                    job(
                            run,
                            directory.resolve("rescale-" + parallelism),
                            sink,
                            parallelism,
                            RESTORED_INTERVAL_MILLIS,
                            checkpoint,
                            false)) {
                waitFor(job, proxy, 128, duplicates + 128);
                stop = job.savepoint(directory.resolve("stop-" + parallelism), true);
            }
            if (run.staged.get() != staged) {
                throw new IOException(
                        "Completed-checkpoint replay serialized fresh input identities");
            }
            readback.verify();
        }
        try (var job =
                job(
                        run,
                        directory.resolve("resume-stop"),
                        sink,
                        1,
                        RESTORED_INTERVAL_MILLIS,
                        stop,
                        false)) {
            job.finish();
        }
        proxy.requireHealthy();
        if (run.staged.get() != staged || proxy.discarded != 1) {
            throw new IOException("Successful-stop resume or response-loss calibration failed");
        }
        readback.verify();
        System.out.println(
                "PRODUCTION_RECOVERY PASS api="
                        + (tableApi ? "table" : "datastream")
                        + " inputs=128 identities="
                        + proxy.envelopes.size()
                        + " wireAttempts="
                        + proxy.attempts
                        + " discarded="
                        + proxy.discarded
                        + " duplicates="
                        + proxy.duplicates);
    }

    private static LocalStagedJob job(
            LocalStagedHarness run,
            Path directory,
            MappedSink<?> sink,
            int parallelism,
            long interval,
            String restore,
            boolean restart)
            throws Exception {
        return new LocalStagedJob(
                run,
                directory,
                true,
                false,
                parallelism,
                128,
                interval,
                true,
                restore,
                restart,
                sink);
    }

    private static void waitFor(
            LocalStagedJob job, ProductionRecoveryProxy proxy, int acknowledged, int duplicates)
            throws Exception {
        await(
                "production recovery acknowledgements",
                Duration.ofSeconds(90),
                () -> {
                    if (!job.allRunning()) {
                        return false;
                    }
                    synchronized (proxy) {
                        if (proxy.fatal != null) {
                            throw new IllegalStateException("Proxy failed", proxy.fatal);
                        }
                        return proxy.acknowledged.size() == acknowledged
                                && proxy.duplicates >= duplicates;
                    }
                },
                () -> "wireAttempts=" + proxy.attempts + " duplicates=" + proxy.duplicates);
    }

    /** Builds the production sink against the loopback proxy under the transactional profile. */
    static MappedSink<?> sink(LocalStagedHarness run, String endpoint, boolean tableApi) {
        return sink(run, endpoint, tableApi, LocalStagedHarness.PROFILE);
    }

    /**
     * Builds one of the two production API entry points for the shared scenario.
     *
     * @param endpoint the loopback emulator endpoint the connector dials, or {@code null} to leave
     *     the builder and Table options without one so the connector takes its native TLS and
     *     application-default-credentials branch against the real service
     * @param profile the application profile the sink declares; the recovery plan uses the
     *     transactional one, the native acceptance also passes a rejected shape
     */
    @SuppressWarnings("unchecked")
    static MappedSink<?> sink(
            LocalStagedHarness run, @Nullable String endpoint, boolean tableApi, String profile) {
        if (!tableApi) {
            String runId = run.id;
            var builder =
                    BigtableSink.<Long>builder()
                            .table(run.table)
                            .appProfileId(profile)
                            .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE);
            if (endpoint != null) {
                builder.emulatorEndpoint(endpoint);
            }
            var sink =
                    (BigtableStagedSink<Long>)
                            builder.stagedOptions(
                                            BigtableStagedOptions.builder()
                                                    .markerFamily(
                                                            StagedMutationTestSink.MARKER_FAMILY)
                                                    .requestOptions(
                                                            BigtableRequestOptions.builder()
                                                                    .maxInFlightRequests(1)
                                                                    .build())
                                                    .build())
                                    .serializer(
                                            (number, context) -> {
                                                var input =
                                                        LocalStagedHarness.run(runId).input(number);
                                                return RowMutationEntry.createFromMutationUnsafe(
                                                        input.row(),
                                                        com.google.cloud.bigtable.data.v2.models
                                                                .Mutation.fromProtoUnsafe(
                                                                List.of(input.mutations().get(1))));
                                            })
                                    .build();
            return new MappedSink<>(sink, run.id, false);
        }
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", run.table.getProject());
        options.put("instance", run.table.getInstance());
        options.put("table", run.table.getTable());
        if (endpoint != null) {
            options.put("emulator-endpoint", endpoint);
        }
        options.put("sink.app-profile-id", profile);
        options.put("sink.write-mode", "aggregate");
        options.put("sink.aggregate.column-family-types", "agg:int64-sum");
        options.put("sink.delivery-guarantee", "exactly-once");
        options.put("sink.staged.marker-family", StagedMutationTestSink.MARKER_FAMILY);
        options.put("sink.in-flight.max-requests", "1");
        var schema =
                ResolvedSchema.of(
                        Column.physical("key", DataTypes.STRING()),
                        Column.physical(
                                "agg",
                                DataTypes.ROW(DataTypes.FIELD("count", DataTypes.BIGINT()))));
        var table = FactoryMocks.createTableSink(schema, options);
        ((SupportsWritingMetadata) table)
                .applyWritableMetadata(
                        List.of("timestamp"),
                        DataTypes.ROW(
                                DataTypes.FIELD("key", DataTypes.STRING()),
                                DataTypes.FIELD(
                                        "agg",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("count", DataTypes.BIGINT()))),
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(6))));
        var sink =
                (BigtableStagedSink<RowData>)
                        ((SinkV2Provider)
                                        table.getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                                .createSink();
        return new MappedSink<>(sink, run.id, true);
    }

    /** Adapts only input representation; all committer and topology methods delegate unchanged. */
    static final class MappedSink<T>
            implements CrossVersionSink<Long>,
                    SupportsCommitter<BigtableCommittable>,
                    SupportsPreCommitTopology<BigtableCommittable, BigtableCommittable> {
        private static final long serialVersionUID = 1L;
        private final BigtableStagedSink<T> delegate;
        private final String runId;
        private final boolean tableApi;
        private final boolean suppressCommits;

        MappedSink(BigtableStagedSink<T> delegate, String runId, boolean tableApi) {
            this(delegate, runId, tableApi, false);
        }

        private MappedSink(
                BigtableStagedSink<T> delegate,
                String runId,
                boolean tableApi,
                boolean suppressCommits) {
            this.delegate = delegate;
            this.runId = runId;
            this.tableApi = tableApi;
            this.suppressCommits = suppressCommits;
        }

        /**
         * A copy whose committer records every request but sends none, so the replay assertion can
         * be shown to fail when a restore commits nothing. Only the local control uses it; it sends
         * nothing to any service.
         */
        MappedSink<T> suppressingCommits() {
            return new MappedSink<>(delegate, runId, tableApi, true);
        }

        /** The configuration the production sink was built with, for asserting its transport. */
        BigtableSinkConfig<T> config() {
            return delegate.getConfig();
        }

        @Override
        public CommittingSinkWriter<Long, BigtableCommittable> createWriter(
                WriterInitContext context) throws IOException {
            var writer = delegate.createWriter(context);
            return new CommittingSinkWriter<>() {
                @Override
                @SuppressWarnings("unchecked")
                public void write(Long number, Context context)
                        throws IOException, InterruptedException {
                    var run = LocalStagedHarness.run(runId);
                    T value =
                            (T)
                                    (tableApi
                                            ? GenericRowData.of(
                                                    StringData.fromString(
                                                            run.input(number).row().toStringUtf8()),
                                                    GenericRowData.of(1L),
                                                    TimestampData.fromEpochMillis(1))
                                            : number);
                    writer.write(value, context);
                    run.admitted(number);
                    run.staged.incrementAndGet();
                }

                @Override
                public void flush(boolean endOfInput) throws IOException, InterruptedException {
                    writer.flush(endOfInput);
                }

                @Override
                public Collection<BigtableCommittable> prepareCommit()
                        throws IOException, InterruptedException {
                    return writer.prepareCommit();
                }

                @Override
                public void close() throws Exception {
                    writer.close();
                }
            };
        }

        @Override
        public Committer<BigtableCommittable> createCommitter(CommitterInitContext context)
                throws IOException {
            var committer = delegate.createCommitter(context);
            String runId = this.runId;
            boolean suppress = suppressCommits;
            return new Committer<>() {
                @Override
                public void commit(Collection<CommitRequest<BigtableCommittable>> requests)
                        throws IOException, InterruptedException {
                    var run = LocalStagedHarness.run(runId);
                    List<ObservedRequest> observed = new ArrayList<>();
                    for (var request : requests) {
                        observed.add(new ObservedRequest(request));
                    }
                    // Published before the production committer runs, so a stalled commit stage
                    // is visible as UNSENT observations rather than as an empty phase.
                    for (var request : observed) {
                        run.productionCommits.add(request.observation);
                    }
                    try {
                        if (!suppress) {
                            committer.commit(new ArrayList<>(observed));
                            for (var request : observed) {
                                if (request.observation.outcome
                                        == CommitObservation.Outcome.UNSENT) {
                                    request.observation.outcome = CommitObservation.Outcome.APPLIED;
                                }
                            }
                        }
                    } catch (IOException | InterruptedException | RuntimeException failure) {
                        for (var request : observed) {
                            if (request.observation.outcome == CommitObservation.Outcome.UNSENT) {
                                request.observation.outcome = CommitObservation.Outcome.FAILED;
                            }
                        }
                        throw failure;
                    }
                }

                @Override
                public void close() throws Exception {
                    committer.close();
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<BigtableCommittable> getCommittableSerializer() {
            return delegate.getCommittableSerializer();
        }

        @Override
        public SimpleVersionedSerializer<BigtableCommittable> getWriteResultSerializer() {
            return getCommittableSerializer();
        }

        @Override
        public DataStream<CommittableMessage<BigtableCommittable>> addPreCommitTopology(
                DataStream<CommittableMessage<BigtableCommittable>> stream) {
            return delegate.addPreCommitTopology(stream);
        }
    }

    /** Forwards every signal to Flink's request and records which one the committer chose. */
    private static final class ObservedRequest
            implements Committer.CommitRequest<BigtableCommittable> {
        private final Committer.CommitRequest<BigtableCommittable> delegate;
        final CommitObservation observation;

        ObservedRequest(Committer.CommitRequest<BigtableCommittable> delegate) {
            this.delegate = delegate;
            this.observation = new CommitObservation(delegate.getCommittable());
        }

        @Override
        public BigtableCommittable getCommittable() {
            return delegate.getCommittable();
        }

        @Override
        public int getNumberOfRetries() {
            return delegate.getNumberOfRetries();
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            observation.outcome = CommitObservation.Outcome.FAILED;
            delegate.signalFailedWithKnownReason(t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            observation.outcome = CommitObservation.Outcome.FAILED;
            delegate.signalFailedWithUnknownReason(t);
        }

        @Override
        public void retryLater() {
            observation.outcome = CommitObservation.Outcome.RETRY;
            delegate.retryLater();
        }

        @Override
        public void updateAndRetryLater(BigtableCommittable committable) {
            observation.outcome = CommitObservation.Outcome.RETRY;
            delegate.updateAndRetryLater(committable);
        }

        @Override
        public void signalAlreadyCommitted() {
            observation.outcome = CommitObservation.Outcome.ALREADY_COMMITTED;
            delegate.signalAlreadyCommitted();
        }
    }
}
