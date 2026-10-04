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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;

import java.io.IOException;
import java.util.List;

/**
 * A Table sink's runtime {@link Sink}, reporting {@link Lineage#tableSink(String, String, List)}
 * under the table's catalog name (ADR-0160). Writer creation goes to the wrapped sink unchanged.
 *
 * <p>This adapter forwards {@code createWriter} only. Construction therefore refuses a delegate
 * implementing {@link SupportsWriterState}, {@link SupportsCommitter} or {@link
 * SupportsPreWriteTopology}, whose state, commits or pre-write topology it would otherwise drop
 * from the job without an error.
 *
 * @param <T> the sink's input type
 */
@Internal
public final class TableLineageSink<T> implements CrossVersionSink<T>, LineageVertexProvider {
    private static final long serialVersionUID = 1L;

    private final Sink<T> delegate;
    private final LineageMetadata lineage;
    private final String namespace;
    private final List<ResourceIdentifier> resources;

    private TableLineageSink(
            Sink<T> delegate,
            String logicalName,
            String namespace,
            List<ResourceIdentifier> resources) {
        this.delegate = delegate;
        Preconditions.checkArgument(
                !(delegate instanceof SupportsWriterState)
                        && !(delegate instanceof SupportsCommitter)
                        && !(delegate instanceof SupportsPreWriteTopology),
                "%s keeps writer state, commits or changes the pre-write topology, which %s would not forward",
                delegate.getClass().getName(),
                TableLineageSink.class.getSimpleName());
        this.lineage = LineageMetadata.of(logicalName);
        this.namespace = namespace;
        this.resources = List.copyOf(resources);
    }

    /**
     * Wraps a sink that needs only writer creation so that it reports the table's lineage.
     *
     * @param delegate the sink writer creation goes to
     * @param logicalName the table's catalog identifier
     * @param namespace the connector's canonical namespace
     * @param resources the physical resources the table writes
     * @param <T> the sink's input type
     * @return the wrapped sink
     * @throws IllegalArgumentException if the delegate keeps writer state, commits or supplies a
     *     pre-write topology
     */
    public static <T> TableLineageSink<T> of(
            Sink<T> delegate,
            String logicalName,
            String namespace,
            List<ResourceIdentifier> resources) {
        return new TableLineageSink<>(delegate, logicalName, namespace, resources);
    }

    /**
     * Returns the wrapped sink, for a connector test that inspects what its factory built.
     *
     * @return the wrapped sink
     */
    @VisibleForTesting
    public Sink<T> delegate() {
        return delegate;
    }

    @Override
    public LineageVertex getLineageVertex() {
        return lineage.sink(namespace, resources);
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return delegate.createWriter(context);
    }
}
