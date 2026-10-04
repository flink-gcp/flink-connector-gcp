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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Holds the table adapters to their contract: runtime creation, source restoration and serializers
 * reach the delegate unchanged, and the vertex carries the table's catalog name over all of its
 * physical resources.
 */
class TableLineageAdaptersTest {
    private static final ResourceIdentifier A = LineageIdentifiers.bigQueryTable("p", "d", "a");
    private static final ResourceIdentifier B = LineageIdentifiers.bigQueryTable("p", "d", "b");

    @Test
    void theTableSourceDelegatesRuntimeCreationAndSerializers() throws Exception {
        RecordingSource delegate = new RecordingSource();
        Source<String, SourceSplit, String> wrapped =
                TableLineageSource.of(delegate, Types.STRING, "t", "bigquery", List.of(A));
        SourceReaderContext readerContext = new FakeSourceReaderContext(null);
        SplitEnumeratorContext<SourceSplit> enumeratorContext = new FakeSplitEnumeratorContext<>(1);
        String checkpoint = "state";

        assertThat(wrapped.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(wrapped.createReader(readerContext)).isSameAs(RecordingSource.READER);
        assertThat(delegate.readerContext).isSameAs(readerContext);
        assertThat(wrapped.createEnumerator(enumeratorContext)).isSameAs(RecordingSource.PLANNED);
        assertThat(delegate.enumeratorContext).isSameAs(enumeratorContext);
        assertThat(wrapped.restoreEnumerator(enumeratorContext, checkpoint))
                .isSameAs(RecordingSource.RESTORED);
        assertThat(delegate.enumeratorContext).isSameAs(enumeratorContext);
        assertThat(delegate.checkpoint).isSameAs(checkpoint);
        assertThat(wrapped.getSplitSerializer()).isSameAs(RecordingSource.SPLITS);
        assertThat(wrapped.getEnumeratorCheckpointSerializer()).isSameAs(RecordingSource.STATE);
        assertThat(((ResultTypeQueryable<?>) wrapped).getProducedType()).isEqualTo(Types.STRING);
        assertThat(delegate.calls)
                .containsExactly(
                        "boundedness", "createReader", "createEnumerator", "restore:state");
    }

    @ParameterizedTest
    @EnumSource(Boundedness.class)
    void theTableSourceReportsTheCatalogNameWithTheDelegatesBoundedness(Boundedness boundedness) {
        TableLineageSource<String, SourceSplit, String> wrapped =
                TableLineageSource.of(
                        new RecordingSource(boundedness),
                        Types.STRING,
                        "c.db.t",
                        "bigquery",
                        List.of(B, A));

        SourceLineageVertex vertex = wrapped.getLineageVertex();

        assertThat(vertex.boundedness()).isEqualTo(boundedness);
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(dataset -> assertDataset(dataset, "c.db.t", A, B));
        assertThat(wrapped.delegate()).isInstanceOf(RecordingSource.class);
    }

    @Test
    void theTableSinkDelegatesWriterCreationAndReportsTheCatalogName() throws Exception {
        RecordingSink delegate = new RecordingSink();
        TableLineageSink<String> wrapped =
                TableLineageSink.of(delegate, "c.db.out", "bigquery", List.of(A, B));
        WriterInitContext context = new StubWriterInitContext(0);

        assertThat(wrapped.createWriter(context)).isSameAs(RecordingSink.WRITER);
        assertThat(delegate.context).isSameAs(context);
        LineageVertex vertex = wrapped.getLineageVertex();

        assertThat(delegate.writers).isEqualTo(1);
        assertThat(vertex).isNotInstanceOf(SourceLineageVertex.class);
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(dataset -> assertDataset(dataset, "c.db.out", A, B));
        assertThat(wrapped.delegate()).isSameAs(delegate);
    }

    @Test
    void runtimeCreationFailuresReachTheCallerUnchanged() {
        IOException failure = new IOException("delegate failed");
        RecordingSource source = new RecordingSource();
        source.failure = failure;
        TableLineageSource<String, SourceSplit, String> wrappedSource =
                TableLineageSource.of(source, Types.STRING, "t", "bigquery", List.of(A));

        assertThatThrownBy(() -> wrappedSource.createReader(new FakeSourceReaderContext(null)))
                .isSameAs(failure);
        assertThatThrownBy(
                        () -> wrappedSource.createEnumerator(new FakeSplitEnumeratorContext<>(1)))
                .isSameAs(failure);
        assertThatThrownBy(
                        () ->
                                wrappedSource.restoreEnumerator(
                                        new FakeSplitEnumeratorContext<>(1), "state"))
                .isSameAs(failure);

        RecordingSink sink = new RecordingSink();
        sink.failure = failure;
        TableLineageSink<String> wrappedSink =
                TableLineageSink.of(sink, "t", "bigquery", List.of(A));
        assertThatThrownBy(() -> wrappedSink.createWriter(new StubWriterInitContext(0)))
                .isSameAs(failure);
    }

    @Test
    void resourceSnapshotsAndConfigurationSurviveSerialization() throws Exception {
        List<ResourceIdentifier> resources = new ArrayList<>(List.of(B, A));
        TableLineageSource<String, SourceSplit, String> source =
                TableLineageSource.of(
                        new RecordingSource(Boundedness.CONTINUOUS_UNBOUNDED),
                        Types.STRING,
                        "c.db.t",
                        "bigquery",
                        resources);
        TableLineageSink<String> sink =
                TableLineageSink.of(new RecordingSink(), "c.db.out", "bigquery", resources);
        resources.clear();

        for (TableLineageSource<String, SourceSplit, String> candidate :
                List.of(source, roundTrip(source))) {
            assertThat(candidate.getProducedType()).isEqualTo(Types.STRING);
            assertThat(candidate.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
            assertDataset(candidate.getLineageVertex().datasets().get(0), "c.db.t", A, B);
            assertThat(candidate.createReader(new FakeSourceReaderContext(null)))
                    .isSameAs(RecordingSource.READER);
        }
        for (TableLineageSink<String> candidate : List.of(sink, roundTrip(sink))) {
            assertDataset(candidate.getLineageVertex().datasets().get(0), "c.db.out", A, B);
            assertThat(candidate.createWriter(new StubWriterInitContext(0)))
                    .isSameAs(RecordingSink.WRITER);
        }
    }

    @Test
    void unknownResourcesRetainOneLogicalDataset() {
        TableLineageSource<String, SourceSplit, String> source =
                TableLineageSource.of(
                        new RecordingSource(), Types.STRING, "c.db.t", "bigquery", List.of());
        TableLineageSink<String> sink =
                TableLineageSink.of(new RecordingSink(), "c.db.out", "bigquery", List.of());

        assertThat(source.getLineageVertex().datasets())
                .singleElement()
                .satisfies(dataset -> assertDataset(dataset, "c.db.t"));
        assertThat(sink.getLineageVertex().datasets())
                .singleElement()
                .satisfies(dataset -> assertDataset(dataset, "c.db.out"));
    }

    @Test
    void theTableSinkRefusesADelegateWithWriterState() {
        assertThatThrownBy(
                        () -> TableLineageSink.of(new StatefulSink(), "t", "bigquery", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        StatefulSink.class.getName()
                                + " keeps writer state, commits or changes the pre-write topology, which TableLineageSink would"
                                + " not forward");
    }

    @Test
    void theTableSinkRefusesADelegateThatCommits() {
        assertThatThrownBy(
                        () -> TableLineageSink.of(new CommittingSink(), "t", "bigquery", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(CommittingSink.class.getName());
    }

    @Test
    void theTableSinkRefusesADelegateWithAPreWriteTopology() {
        assertThatThrownBy(
                        () -> TableLineageSink.of(new PreWriteSink(), "t", "bigquery", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(PreWriteSink.class.getName())
                .hasMessageContaining("pre-write topology");
    }

    private static void assertDataset(
            LineageDataset dataset, String name, ResourceIdentifier... resources) {
        assertThat(dataset.name()).isEqualTo(name);
        assertThat(dataset.namespace()).isEqualTo("bigquery");
        assertThat(((PhysicalResourceFacet) dataset.facets().get("gcp")).resources())
                .containsExactly(resources);
    }

    private static final class RecordingSource implements Source<String, SourceSplit, String> {
        static final SimpleVersionedSerializer<SourceSplit> SPLITS = serializer();
        static final SimpleVersionedSerializer<String> STATE = serializer();
        static final SourceReader<String, SourceSplit> READER = new FakeReader();
        static final SplitEnumerator<SourceSplit, String> PLANNED = new FakeEnumerator();
        static final SplitEnumerator<SourceSplit, String> RESTORED = new FakeEnumerator();
        final List<String> calls = new ArrayList<>();
        final Boundedness boundedness;
        transient SourceReaderContext readerContext;
        transient SplitEnumeratorContext<SourceSplit> enumeratorContext;
        String checkpoint;
        IOException failure;

        RecordingSource() {
            this(Boundedness.BOUNDED);
        }

        RecordingSource(Boundedness boundedness) {
            this.boundedness = boundedness;
        }

        @Override
        public Boundedness getBoundedness() {
            calls.add("boundedness");
            return boundedness;
        }

        @Override
        public SourceReader<String, SourceSplit> createReader(SourceReaderContext context)
                throws IOException {
            calls.add("createReader");
            readerContext = context;
            if (failure != null) {
                throw failure;
            }
            return READER;
        }

        @Override
        public SplitEnumerator<SourceSplit, String> createEnumerator(
                SplitEnumeratorContext<SourceSplit> context) throws IOException {
            calls.add("createEnumerator");
            enumeratorContext = context;
            if (failure != null) {
                throw failure;
            }
            return PLANNED;
        }

        @Override
        public SplitEnumerator<SourceSplit, String> restoreEnumerator(
                SplitEnumeratorContext<SourceSplit> context, String checkpoint) throws IOException {
            calls.add("restore:" + checkpoint);
            enumeratorContext = context;
            this.checkpoint = checkpoint;
            if (failure != null) {
                throw failure;
            }
            return RESTORED;
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            return SPLITS;
        }

        @Override
        public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() {
            return STATE;
        }
    }

    private static class RecordingSink implements CrossVersionSink<String> {
        static final SinkWriter<String> WRITER =
                new SinkWriter<String>() {
                    @Override
                    public void write(String element, Context context) {}

                    @Override
                    public void flush(boolean endOfInput) {}

                    @Override
                    public void close() {}
                };
        int writers;
        transient WriterInitContext context;
        IOException failure;

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            writers++;
            this.context = context;
            if (failure != null) {
                throw failure;
            }
            return WRITER;
        }
    }

    private static final class FakeReader implements SourceReader<String, SourceSplit> {
        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<String> output) {
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<SourceSplit> snapshotState(long checkpointId) {
            return List.of();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<SourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }

    private static final class FakeEnumerator implements SplitEnumerator<SourceSplit, String> {
        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {}

        @Override
        public void addSplitsBack(List<SourceSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public String snapshotState(long checkpointId) {
            return "state";
        }

        @Override
        public void close() {}
    }

    private static final class StatefulSink extends RecordingSink
            implements SupportsWriterState<String, String> {
        @Override
        public StatefulSinkWriter<String, String> restoreWriter(
                WriterInitContext context, Collection<String> recoveredState) {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<String> getWriterStateSerializer() {
            return serializer();
        }
    }

    private static final class PreWriteSink extends RecordingSink
            implements SupportsPreWriteTopology<String> {
        @Override
        public DataStream<String> addPreWriteTopology(DataStream<String> inputDataStream) {
            return inputDataStream;
        }
    }

    private static final class CommittingSink extends RecordingSink
            implements SupportsCommitter<String> {
        @Override
        public Committer<String> createCommitter(CommitterInitContext context) {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<String> getCommittableSerializer() {
            return serializer();
        }
    }

    private static <T> SimpleVersionedSerializer<T> serializer() {
        return new SimpleVersionedSerializer<T>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(T obj) {
                return new byte[0];
            }

            @Override
            public T deserialize(int version, byte[] serialized) {
                return null;
            }
        };
    }

    private static <T extends Serializable> T roundTrip(T value) throws Exception {
        return InstantiationUtil.deserializeObject(
                InstantiationUtil.serializeObject(value),
                TableLineageAdaptersTest.class.getClassLoader());
    }
}
