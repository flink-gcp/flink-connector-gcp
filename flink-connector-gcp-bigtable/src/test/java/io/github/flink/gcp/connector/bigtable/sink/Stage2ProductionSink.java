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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableStagedTableAdmin;
import io.github.flink.gcp.connector.bigtable.sink.tables.StagedTableAdmin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.stream.Collectors;

/** Measures the production sink without implementing its staging, validation or commit loop. */
final class Stage2ProductionSink
        implements CrossVersionSink<Long>,
                SupportsCommitter<BigtableCommittable>,
                SupportsPreCommitTopology<BigtableCommittable, BigtableCommittable> {
    private static final long serialVersionUID = 1L;
    private final String runId;
    private final boolean transport;
    private final BigtableStagedSink<Long> delegate;

    @SuppressWarnings("unchecked")
    Stage2ProductionSink(Stage2Harness run, boolean transport) {
        this.runId = run.id;
        this.transport = transport;
        String id = run.id;
        var builder =
                BigtableSink.<Long>builder()
                        .table(run.table)
                        .appProfileId(LocalStagedHarness.PROFILE)
                        .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                        .stagedOptions(
                                BigtableStagedOptions.builder()
                                        .markerFamily(StagedMutationTestSink.MARKER_FAMILY)
                                        .maxStagedEntries(run.maxEntries)
                                        .maxStagedBytes(run.maxBytes)
                                        .requestOptions(
                                                BigtableRequestOptions.builder()
                                                        .maxInFlightRequests(run.inFlight)
                                                        .build())
                                        .build())
                        .serializer(
                                (value, context) -> {
                                    LocalStagedHarness current = LocalStagedHarness.run(id);
                                    current.admitted(value);
                                    var input = current.input(value);
                                    return RowMutationEntry.createFromMutationUnsafe(
                                            input.row(),
                                            com.google.cloud.bigtable.data.v2.models.Mutation
                                                    .fromProtoUnsafe(input.mutations()));
                                });
        if (run.lease == null) {
            builder.emulatorEndpoint(run.endpoint);
        }
        delegate = (BigtableStagedSink<Long>) builder.build();
    }

    private Stage2Harness run() {
        return (Stage2Harness) LocalStagedHarness.run(runId);
    }

    @Override
    public CommittingSinkWriter<Long, BigtableCommittable> createWriter(WriterInitContext context)
            throws IOException {
        Stage2Harness run = run();
        Stage2WriterMetrics metrics = new Stage2WriterMetrics();
        var writer = delegate.createWriter(metrics.observe(context));
        metrics.sample(run, writer);
        return new CommittingSinkWriter<Long, BigtableCommittable>() {
            @Override
            public void write(Long value, Context context)
                    throws IOException, InterruptedException {
                try {
                    writer.write(value, context);
                    run.staged.incrementAndGet();
                } finally {
                    metrics.sample(run, writer);
                }
            }

            @Override
            public void flush(boolean endOfInput) throws IOException, InterruptedException {
                writer.flush(endOfInput);
            }

            @Override
            public Collection<BigtableCommittable> prepareCommit()
                    throws IOException, InterruptedException {
                Collection<BigtableCommittable> values = writer.prepareCommit();
                metrics.sample(run, writer);
                run.prepared(
                        values.stream()
                                .map(BigtableCommittable::getRequest)
                                .collect(Collectors.toList()));
                return values;
            }

            @Override
            public void close() throws Exception {
                try {
                    writer.close();
                } finally {
                    metrics.sample(run, writer);
                }
                if (run.failWriterClose.compareAndSet(true, false)) {
                    throw new IOException("Injected writer close failure after stop");
                }
            }
        };
    }

    @Override
    public Committer<BigtableCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        Stage2Harness run = run();
        return createCommitter(
                context,
                run.lease == null ? run.localTableAdmin : new BigtableStagedTableAdmin(null, null));
    }

    Committer<BigtableCommittable> createCommitter(
            CommitterInitContext context, StagedTableAdmin metadata) throws IOException {
        Stage2Harness run = run();
        StagedTableAdmin guardedMetadata =
                (destination, profile, marker, families) -> {
                    if (run.lease != null) {
                        run.lease.requireTarget(destination);
                    }
                    metadata.validate(destination, profile, marker, families);
                };
        var committer =
                delegate.createCommitter(
                        context,
                        guardedMetadata,
                        profile -> new ObservedClients(run, profile, transport));
        return new Committer<BigtableCommittable>() {
            @Override
            public void commit(Collection<CommitRequest<BigtableCommittable>> requests)
                    throws IOException, InterruptedException {
                run.commitStarted(this, requests.size());
                boolean successful = false;
                try {
                    committer.commit(requests);
                    successful = true;
                } finally {
                    run.commitFinished(this, successful);
                }
            }

            @Override
            public void close() throws Exception {
                try {
                    committer.close();
                } finally {
                    run.committersClosed.incrementAndGet();
                }
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<BigtableCommittable> getCommittableSerializer() {
        var serializer = delegate.getCommittableSerializer();
        return new SimpleVersionedSerializer<BigtableCommittable>() {
            @Override
            public int getVersion() {
                return serializer.getVersion();
            }

            @Override
            public byte[] serialize(BigtableCommittable value) throws IOException {
                long before = Stage2Harness.allocatedBytes();
                try {
                    return serializer.serialize(value);
                } finally {
                    Stage2Harness.allocationDelta(run().serializationAllocatedBytes, before);
                }
            }

            @Override
            public BigtableCommittable deserialize(int version, byte[] value) throws IOException {
                long before = Stage2Harness.allocatedBytes();
                try {
                    return serializer.deserialize(version, value);
                } finally {
                    Stage2Harness.allocationDelta(run().restoreAllocatedBytes, before);
                }
            }
        };
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

    private static final class ObservedClients implements SingleRowClientFactory {
        private static final long serialVersionUID = 1L;
        private final transient Stage2Harness run;
        private final transient SingleRowClientFactory delegate;
        private final String profile;

        ObservedClients(Stage2Harness run, String profile, boolean transport) throws IOException {
            this.run = run;
            this.profile = profile;
            this.delegate =
                    transport
                            ? new DefaultSingleRowClientFactory(
                                    profile,
                                    BigtableRequestOptions.builder()
                                            .maxInFlightRequests(run.inFlight)
                                            .build(),
                                    run.lease == null
                                            ? io.github.flink.gcp.connector.base.rpc
                                                    .EmulatorEndpoint.parse(
                                                    run.endpoint, "endpoint")
                                            : null,
                                    null)
                            : null;
        }

        @Override
        public SingleRowClient create(TableDestination destination)
                throws IOException, InterruptedException {
            if (!destination.equals(run.table) || !profile.equals(LocalStagedHarness.PROFILE)) {
                throw new IOException("Restored destination/profile differs from the Stage 2 run");
            }
            SingleRowClient client = delegate == null ? null : delegate.create(destination);
            return new SingleRowClient() {
                @Override
                public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation) {
                    var wire =
                            mutation.toProto(
                                    RequestContext.create(
                                            destination.getProject(),
                                            destination.getInstance(),
                                            profile));
                    try {
                        run.beforeProductionSend(wire.getSerializedSize());
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                    long started = System.nanoTime();
                    ApiFuture<Boolean> original =
                            client == null ? run.fake(wire) : client.checkAndMutateRow(mutation);
                    return new Stage2ObservedFuture<>(
                            original,
                            run,
                            (matched, now) -> {
                                run.clientCompleted(
                                        LocalStagedHarness.sequence(wire.getFalseMutationsList()),
                                        now - started,
                                        now);
                                run.commitAcknowledged(wire, now);
                                if (matched) {
                                    run.deduplicated.incrementAndGet();
                                }
                            });
                }

                @Override
                public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
                    throw new UnsupportedOperationException("Stage 2 uses conditional commits");
                }
            };
        }

        @Override
        public void release(TableDestination destination) throws Exception {
            if (delegate != null) {
                delegate.release(destination);
            }
        }

        @Override
        public void close() throws Exception {
            if (delegate != null) {
                delegate.close();
            }
        }
    }
}
