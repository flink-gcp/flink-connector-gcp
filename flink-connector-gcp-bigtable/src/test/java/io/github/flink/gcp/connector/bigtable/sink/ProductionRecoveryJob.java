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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Shared local/service correctness scenario through the two production API factories. */
final class ProductionRecoveryJob {
    private ProductionRecoveryJob() {}

    static LocalStagedHarness newRun(
            io.github.flink.gcp.connector.bigtable.TableDestination destination) {
        return new LocalStagedHarness(destination, "127.0.0.1:1", 32, true, true, 1) {
            @Override
            long checkpointTimeoutMillis() {
                return 60_000;
            }

            @Override
            long checkpointOperationTimeoutMillis() {
                return 90_000;
            }
        };
    }

    interface Readback {
        void verify() throws Exception;
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
        try (var job = job(run, directory.resolve("initial"), sink, 2, null, true)) {
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
        try (var job = job(run, directory.resolve("resume-stop"), sink, 1, stop, false)) {
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
                60_000,
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
                    if (job.result.isDone()) {
                        job.result.join();
                    }
                    for (var vertex :
                            job.cluster
                                    .getExecutionGraph(job.client.getJobID())
                                    .join()
                                    .getAllExecutionVertices()) {
                        if (vertex.getExecutionState()
                                != org.apache.flink.runtime.execution.ExecutionState.RUNNING) {
                            return false;
                        }
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

    @SuppressWarnings("unchecked")
    static MappedSink<?> sink(LocalStagedHarness run, String endpoint, boolean tableApi) {
        if (!tableApi) {
            String runId = run.id;
            var sink =
                    (BigtableStagedSink<Long>)
                            BigtableSink.<Long>builder()
                                    .table(run.table)
                                    .emulatorEndpoint(endpoint)
                                    .appProfileId(LocalStagedHarness.PROFILE)
                                    .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                                    .stagedOptions(
                                            BigtableStagedOptions.builder()
                                                    .markerFamily("flink_commit")
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
        options.put("emulator-endpoint", endpoint);
        options.put("sink.app-profile-id", LocalStagedHarness.PROFILE);
        options.put("sink.write-mode", "aggregate");
        options.put("sink.aggregate.column-family-types", "agg:int64-sum");
        options.put("sink.delivery-guarantee", "exactly-once");
        options.put("sink.staged.marker-family", "flink_commit");
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
    private static final class MappedSink<T>
            implements CrossVersionSink<Long>,
                    SupportsCommitter<BigtableCommittable>,
                    SupportsPreCommitTopology<BigtableCommittable, BigtableCommittable> {
        private static final long serialVersionUID = 1L;
        private final BigtableStagedSink<T> delegate;
        private final String runId;
        private final boolean tableApi;

        MappedSink(BigtableStagedSink<T> delegate, String runId, boolean tableApi) {
            this.delegate = delegate;
            this.runId = runId;
            this.tableApi = tableApi;
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
            return delegate.createCommitter(context);
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
}
