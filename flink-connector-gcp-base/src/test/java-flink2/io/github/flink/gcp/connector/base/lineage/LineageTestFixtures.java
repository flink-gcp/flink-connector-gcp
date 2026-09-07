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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.base.lineage.internal.Lineage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** Empty local runtimes: metadata inspection and runtime creation have separate counters. */
final class LineageTestFixtures {
    static final AtomicInteger READERS = new AtomicInteger();
    static final AtomicInteger ENUMERATORS = new AtomicInteger();
    static final AtomicInteger WRITERS = new AtomicInteger();

    private LineageTestFixtures() {}

    static final class FakeSource
            implements Source<RowData, SourceSplit, Void>, LineageVertexProvider {
        private static final long serialVersionUID = 1L;
        private final List<ResourceIdentifier> resources;
        private final Boundedness boundedness;
        private final boolean table;

        FakeSource(List<ResourceIdentifier> resources, Boundedness boundedness, boolean table) {
            this.resources = List.copyOf(resources);
            this.boundedness = boundedness;
            this.table = table;
        }

        @Override
        public SourceLineageVertex getLineageVertex() {
            return table
                    ? Lineage.tableSource("configured.input", "bigquery", boundedness, resources)
                    : Lineage.source(getBoundedness(), resources);
        }

        @Override
        public Boundedness getBoundedness() {
            return boundedness;
        }

        @Override
        public SourceReader<RowData, SourceSplit> createReader(SourceReaderContext context) {
            READERS.incrementAndGet();
            return new SourceReader<>() {
                private final CompletableFuture<Void> available = new CompletableFuture<>();
                private boolean noMoreSplits;

                @Override
                public void start() {}

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output) {
                    return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public List<SourceSplit> snapshotState(long checkpointId) {
                    return List.of();
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return available;
                }

                @Override
                public void addSplits(List<SourceSplit> splits) {}

                @Override
                public void notifyNoMoreSplits() {
                    noMoreSplits = true;
                    available.complete(null);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> createEnumerator(
                SplitEnumeratorContext<SourceSplit> context) {
            ENUMERATORS.incrementAndGet();
            return new SplitEnumerator<>() {
                @Override
                public void start() {}

                @Override
                public void handleSplitRequest(int subtask, String hostname) {}

                @Override
                public void addSplitsBack(List<SourceSplit> splits, int subtask) {}

                @Override
                public void addReader(int subtask) {
                    // Registration is the only completion signal; readers do not request splits.
                    context.signalNoMoreSplits(subtask);
                }

                @Override
                public Void snapshotState(long checkpointId) {
                    return null;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<SourceSplit> context, Void state) {
            return createEnumerator(context);
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            return new EmptySerializer<>();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return new EmptySerializer<>();
        }
    }

    static final class FakeSink implements Sink<RowData>, LineageVertexProvider {
        private static final long serialVersionUID = 1L;
        private final List<ResourceIdentifier> resources;
        private final boolean table;

        FakeSink(List<ResourceIdentifier> resources, boolean table) {
            this.resources = List.copyOf(resources);
            this.table = table;
        }

        @Override
        public LineageVertex getLineageVertex() {
            return table
                    ? Lineage.tableSink("configured.output", "bigquery", resources)
                    : Lineage.sink(resources);
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            WRITERS.incrementAndGet();
            return new SinkWriter<>() {
                @Override
                public void write(RowData value, Context context) {}

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }

    private static final class EmptySerializer<T> implements SimpleVersionedSerializer<T> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(T value) {
            return new byte[0];
        }

        @Override
        public T deserialize(int version, byte[] bytes) {
            return null;
        }
    }
}
